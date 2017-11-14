package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/codahale/hdrhistogram"
	_ "github.com/lib/pq"
)

var dbName = "roachbench"

var concurrency = flag.Int("concurrency", 2*runtime.NumCPU(), "Number of concurrent writers inserting blocks")
var duration = flag.Duration("duration", 0, "The duration to run. If 0, run forever.")

// For postgres, use something like "postgres://$(whoami)@locahost:5432/template1?sslmode=disable
var dbURL = flag.String("url", "", "Database URL.")

// Data loading and dumping flags.
var drop = flag.Bool("drop", false, "Drop and recreate the database.")
var load = flag.Bool("load", false, "Generate fresh data from --seed (deterministic). Use with --drop.")
var dumpfile = flag.String("dump-file", "", "If specified, will dump DB contents to this file.")
var loadfile = flag.String("load-file", "", "Filepath of file generated from --dump to load into database.")
var seed = flag.Int64("seed", 42, "Pseudo-random seed used to generate data.")
var dbms = flag.String("dbms", "cockroach", "DBMS used (values: 'cockroach' or 'postgres'). Relevant for --load and --dump.")
var databases = map[string]bool{
	"cockroach": true,
	"postgres":  true,
}
var variant = flag.String("variant", "normal", "How tables are created (values: 'normal' or 'interleaved'). Use with --load/--load-file.")
var variants = map[string]bool{
	"normal":      true,
	"interleaved": true,
}

var nMerchants = flag.Int("merchants", 10000, "Number of rows in table <merchant> to generate. Use with --load.")
var nProducts = flag.Int("products", 1000000, "number of rows in table <product> to generate. Use with --load.")
var nVariants = flag.Int("variants", 10000000, "number of rows in table <variant> to generate. Use with --load.")
var nStores = flag.Int("stores", 50000, "Number of rows in table <store> to generate. Use with --load.")

var schemaVar schemaVariant
var tables []tableName

func init() {
	flag.Parse()

	// Schema DDL statements initialization.
	initDDL()

	if !databases[*dbms] {
		log.Fatalf("--database must either be 'cockroach' or 'postgres'")
	}
	if !variants[*variant] {
		log.Fatalf("--variant must either be 'normal' or 'interleaved'")
	}

	if *dbms == "postgres" && *variant != "normal" {
		log.Fatalf("--database=postgres only works with --variant=normal")
	}

	schemaVar = schemaVariant(*variant)
	tables = variantTables[schemaVar]

	dbName = dbName + "_" + *variant

	if *dbURL == "" {
		// Need to connect to template DB to perform drops and loads.
		if *dbms == "postgres" && (*drop || *load || *loadfile != "") {
			*dbURL = defaultURL("template1")
		} else {
			*dbURL = defaultURL(dbName)
		}

	}
}

var stderr = log.New(os.Stderr, "", 0)

func defaultURL(name string) string {
	if *dbms == "postgres" {
		return fmt.Sprintf("postgres://richardwu@localhost:5432/%s?sslmode=disable", name)
	}
	return fmt.Sprintf("postgres://root@localhost:26257/%s?sslmode=disable", name)
}

func dropDB(db *sql.DB) {
	stderr.Printf("Dropping database %s\n", dbName)
	if _, err := db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s CASCADE", dbName)); err != nil {
		log.Fatalf("could not drop database: %v", err)
	}
	stderr.Println("Dropping database complete.")
}

func createDB(db *sql.DB) {
	stderr.Printf("Creating database %s\n", dbName)
	if _, err := db.Exec(fmt.Sprintf("CREATE DATABASE %s", dbName)); err != nil {
		log.Fatalf("could not create database: %v", err)
	}
	stderr.Println("Creating database complete.")
}

func connectDB() *sql.DB {
	stderr.Printf("connecting to db: %s\n", *dbURL)

	// Open connection to DB.
	parsedURL, err := url.Parse(*dbURL)
	if err != nil {
		log.Fatal(err)
	}
	db, err := sql.Open("postgres", parsedURL.String())
	if err != nil {
		log.Fatal(err)
	}
	// Set concurrency to number of tables in schema + 1.
	db.SetMaxOpenConns(len(variantTables[schemaVar]) + 1)
	db.SetMaxIdleConns(len(variantTables[schemaVar]) + 1)

	return db
}

func main() {
	if Product == ProductInterleaved {
		panic("oh no")
	}

	db := connectDB()
	defer db.Close()

	if *drop {
		dropDB(db)
	}

	if *drop || *loadfile != "" || *load {
		createDB(db)
	}

	// Connect to the newly created DB for postgres.
	if *dbms == "postgres" && (*drop || *load || *loadfile != "") {
		*dbURL = defaultURL(dbName)
		db = connectDB()
	}

	if *loadfile != "" {
		// Load data from a dumpfile.
		stderr.Printf("Loading from dump file %s...\n", *loadfile)
		var cmd *exec.Cmd
		if *dbms == "cockroach" {
			cmd = exec.Command("cockroach", "sql", "--insecure", "--database="+dbName)
		} else if *dbms == "postgres" {
			cmd = exec.Command("psql", dbName)
		}

		// Pipe the file content of the dump file.
		in, err := cmd.StdinPipe()
		if err != nil {
			log.Fatal(err)
		}
		stderr.Println("reading sql dump contents")
		content, err := ioutil.ReadFile(*loadfile)
		if err != nil {
			log.Fatal(err)
		}
		go func() {
			defer in.Close()
			stderr.Println("piping sql dump to command")
			if _, err := in.Write(content); err != nil {
				log.Fatal(err)
			}
		}()

		stderr.Println("running load command")
		output, err := cmd.CombinedOutput()
		if err != nil {
			log.Printf("%s\n", string(output))
			log.Fatal(err)
		}

		stderr.Printf("Loading from file %s complete!\n", *loadfile)
	} else if *load {
		// Create tables.
		if err := loadSchema(db); err != nil {
			log.Fatal(err)
		}
		if err := generateData(db); err != nil {
			log.Fatal(err)
		}
		if err := applyConstraints(db); err != nil {
			log.Fatal(err)
		}
	}

	// Dump all data to the dumpfile.
	if *dumpfile != "" {
		stderr.Printf("Dumping to file %s...\n", *dumpfile)
		var cmd *exec.Cmd
		if *dbms == "cockroach" {
			cmd = exec.Command("cockroach", "dump", "--insecure", dbName)
		} else if *dbms == "postgres" {
			cmd = exec.Command("pg_dump", dbName)
		}

		output, err := cmd.Output()
		if err != nil {
			log.Fatal(err)
		}
		if err = ioutil.WriteFile(*dumpfile, output, 0644); err != nil {
			log.Fatal(err)
		}

		stderr.Printf("Dump to %s complete!\n", *dumpfile)
	}

	// Run queries.

	start := time.Now()
	var wg sync.WaitGroup
	workers := make([]*worker, *concurrency)
	for i := range workers {
		workers[i] = newWorker(db, &wg)
		go workers[i].run(&wg)
	}

	tick := time.Tick(time.Second)
	done := make(chan os.Signal, 3)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		wg.Wait()
		done <- syscall.Signal(0)
	}()

	if *duration > 0 {
		go func() {
			time.Sleep(*duration)
			done <- syscall.Signal(0)
		}()
	}

	defer func() {
		// Output results that mimic Go's built-in benchmark format.
		elapsed := time.Since(start)
		ops := atomic.LoadUint64(&numOps)
		fmt.Printf("%s\t%12.1f ns/op\n",
			"roach-bench", float64(elapsed.Nanoseconds())/float64(ops))
	}()

	cumLatency := hdrhistogram.New(minLatency.Nanoseconds(), maxLatency.Nanoseconds(), 1)

	lastNow := time.Now()
	var lastOps uint64
	for i := 0; ; {
		select {
		case <-tick:
			var h *hdrhistogram.Histogram
			for _, w := range workers {
				w.latency.Lock()
				m := w.latency.Merge()
				w.latency.Rotate()
				w.latency.Unlock()
				if h == nil {
					h = m
				} else {
					h.Merge(m)
				}
			}

			cumLatency.Merge(h)
			now := time.Now()
			elapsed := now.Sub(lastNow)
			ops := numOps
			if i%20 == 0 {
				fmt.Println("_time______ops/s(inst)__ops/s(cum)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
			}
			i++
			totalTime := time.Duration(time.Since(start).Seconds()+0.5) * time.Second
			fmt.Printf("%5s %12.1f %11.1f %8.1f %8.1f %8.1f %8.1f\n",
				totalTime,
				float64(ops-lastOps)/elapsed.Seconds(),
				float64(ops)/time.Since(start).Seconds(),
				time.Duration(h.ValueAtQuantile(50)).Seconds()*1000,
				time.Duration(h.ValueAtQuantile(95)).Seconds()*1000,
				time.Duration(h.ValueAtQuantile(99)).Seconds()*1000,
				time.Duration(h.ValueAtQuantile(100)).Seconds()*1000)

			lastOps = ops
			lastNow = now

		case <-done:
			for _, w := range workers {
				w.latency.Lock()
				m := w.latency.Merge()
				w.latency.Rotate()
				w.latency.Unlock()
				cumLatency.Merge(m)
			}

			ops := atomic.LoadUint64(&numOps)
			elapsed := time.Since(start).Seconds()
			fmt.Println("\n_elapsed___________ops_____ops/s(cum)__avg(ms)__p50(ms)__p95(ms)__p99(ms)__pMax(ms)")
			fmt.Printf("%7.1fs %11d %15.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n\n",
				time.Since(start).Seconds(),
				ops,
				float64(ops)/elapsed,
				time.Duration(cumLatency.Mean()).Seconds()*1000,
				time.Duration(cumLatency.ValueAtQuantile(50)).Seconds()*1000,
				time.Duration(cumLatency.ValueAtQuantile(95)).Seconds()*1000,
				time.Duration(cumLatency.ValueAtQuantile(99)).Seconds()*1000,
				time.Duration(cumLatency.ValueAtQuantile(100)).Seconds()*1000)
			return
		}
	}
}
