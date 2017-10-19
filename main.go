package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"

	_ "github.com/lib/pq"
)

const dbName = "roachbench"

var dbURL = flag.String("url", fmt.Sprintf("postgres://root@localhost:26257/%s?sslmode=disable", dbName), "Database URL.")
var drop = flag.Bool("drop", false, "Drop and recreate the database.")
var load = flag.Bool("load", false, "Generate fresh data from --seed (deterministic). Use with --drop.")
var dump = flag.Bool("dump", false, "Generate a dump for the schema. Use with --load.")
var dumpfile = flag.String("dump-file", "dumps/dump-roach.sql", "Dump filepath. Use with --dump.")
var seed = flag.Int64("seed", 42, "Pseudo-random seed used to generate data.")
var dbms = flag.String("database", "cockroach", "DBMS used (values: 'cockroach' or 'postgres'). Relevant for --load and --dump.")
var databases = map[string]bool{
	"cockroach": true,
	"postgres":  true,
}
var variant = flag.String("variant", "normal", "How tables are created (values: 'normal' or 'interleaved'). Use with --load.")
var variants = map[string]bool{
	"normal":      true,
	"interleaved": true,
}

var nMerchants = flag.Int("merchants", 1000, "Number of rows in table <merchant> to generate. Use with --load.")
var nProducts = flag.Int("products", 100000, "number of rows in table <product> to generate. Use with --load.")
var nVariants = flag.Int("variants", 200000, "number of rows in table <variant> to generate. Use with --load.")
var nStores = flag.Int("stores", 3000, "Number of rows in table <store> to generate. Use with --load.")

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
}

var stderr = log.New(os.Stderr, "", 0)

func main() {
	if Product == ProductInterleaved {
		panic("oh no")
	}
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

	if *drop {
		stderr.Println("Deleting database %s\n", dbName)
		if _, err := db.Exec(fmt.Sprintf("DROP DATABASE %s", dbName)); err != nil {
			log.Fatalf("could not drop database: %v", err)
		}
		stderr.Printf("Recreating database %s...\n", dbName)
		if _, err := db.Exec(fmt.Sprintf("CREATE DATABASE %s", dbName)); err != nil {
			log.Fatalf("could not recreate database: %v", err)
		}
	}

	if *load {
		// Create tables.
		if err := loadSchema(db); err != nil {
			log.Fatal(err)
		}
		if err := generateData(db); err != nil {
			log.Fatal(err)
		}
		return
	}
}
