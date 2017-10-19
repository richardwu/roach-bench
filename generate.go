package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os/exec"
	"strconv"
	"sync"
)

const (
	textChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
	batchSize = 500
	textLen   = 42
	intMax    = 42
	decMax    = 42
)

func generateData(db *sql.DB) error {
	// Insert pseudo-random data into tables.
	stderr.Println("Inserting into tables, this may take a while...")
	var wg sync.WaitGroup
	for _, table := range tables {
		writer := newWriter(db, table, &wg)
		go writer.insertData(&wg)
	}

	wg.Wait()

	// Dump all data to the dumpfile.
	stderr.Printf("Data insertion done. Dumping to file %s...\n", *dumpfile)
	var cmd *exec.Cmd
	if *dbms == "cockroach" {
		cmd = exec.Command("cockroach", "dump", "--insecure", dbName)
	} else if *dbms == "postgres" {
		cmd = exec.Command("pg_dump", dbName)
	}

	output, err := cmd.Output()
	if err != nil {
		return err
	}
	if err = ioutil.WriteFile(*dumpfile, output, 0644); err != nil {
		return err
	}

	stderr.Printf("Dump to %s complete!\n", *dumpfile)

	return nil
}

type tableWriter struct {
	pkey  int
	db    *sql.DB
	table tableName
	rnd   *rand.Rand

	columnTypes []columnType

	// Updated per insertion.
	// Scratch space for each individual value.
	scratch []byte
	// Buffer to hold the values generated to append to insert statements.
	valuesBuf []byte
}

func newWriter(db *sql.DB, table tableName, wg *sync.WaitGroup) tableWriter {
	wg.Add(1)
	w := tableWriter{
		db:    db,
		table: table,
	}

	// Generate from the same pseudo-random seed for each table to ensure
	// deterministic results even with concurrency.
	w.rnd = rand.New(rand.NewSource(*seed))
	// Primary key starts at 1.
	w.pkey = 1
	w.scratch = make([]byte, 0, textLen+2)
	w.columnTypes = tableTypes[w.table]
	return w
}

// Meant to be concurrently executed.
func (tw *tableWriter) insertData(wg *sync.WaitGroup) {
	remainingRows := tableRows[tw.table]
	curBatch := batchSize
	for remainingRows > 0 {
		if remainingRows < curBatch {
			curBatch = remainingRows
		}

		// Generate a batch of values to insert.
		tw.valuesBuf = tw.genValues(curBatch)
		// Form insert statement and execute.
		if _, err := tw.db.Exec(fmt.Sprintf("%s %s;", insertStmts[tw.table], string(tw.valuesBuf))); err != nil {
			log.Fatal(err)
		}

		remainingRows -= curBatch
	}

	stderr.Printf("Inserting into table <%s> complete.\n", tw.table)
	wg.Add(-1)
}

func (tw *tableWriter) genValues(nTuples int) []byte {
	values := tw.valuesBuf[:0]
	commaTuples := false
	for i := 0; i < nTuples; i++ {
		if commaTuples {
			values = append(values, ',')
		}
		values = append(values, '(')
		commaValues := false
		for i, c := range tw.columnTypes {
			if commaValues {
				values = append(values, ',')
			}

			var temp []byte
			switch c {
			case PkeyInt:
				temp = []byte(strconv.Itoa(tw.pkey))
				tw.pkey++
			case FkeyInt:
				temp = tw.randFkeyInt(i)
			case Int:
				temp = tw.randInt()
			case Text:
				temp = tw.randText()
			case Dec:
				temp = tw.randDec()
			default:
				panic("undefined column type")
			}
			values = append(values, temp...)
			commaValues = true
		}
		values = append(values, ')')
		commaTuples = true
	}
	return values
}

func (tw *tableWriter) randFkeyInt(cidx int) []byte {
	var temp int
	switch tw.table {
	case Product, ProductInterleaved, Store, StoreInterleaved:
		// Foreign key (merchant id) must be between 1 and nMerchants.
		temp = tw.rnd.Intn(*nMerchants) + 1
	case Variant, VariantInterleaved:
		if cidx == 0 {
			// Foreign key (merchant id) must be between 1 and nMerchants.
			temp = tw.rnd.Intn(*nMerchants) + 1
		} else if cidx == 1 {
			// Foreign key (product id) must be between 1 and nProducts.
			temp = tw.rnd.Intn(*nProducts) + 1
		} else {
			panic("invalid fkey column index")
		}
	default:
		panic("unsupported table for fkey generation")
	}

	return []byte(strconv.Itoa(temp))
}

func (tw *tableWriter) randInt() []byte {
	return []byte(strconv.Itoa(tw.rnd.Intn(intMax)))
}

func (tw *tableWriter) randText() []byte {
	scratch := tw.scratch[:0]
	scratch = append(scratch, '\'')
	for i := 0; i < textLen; i++ {
		scratch = append(scratch, textChars[tw.rnd.Intn(len(textChars))])
	}
	scratch = append(scratch, '\'')
	return scratch
}

func (tw *tableWriter) randDec() []byte {
	return []byte(fmt.Sprintf("%.2f", tw.rnd.Float64()*decMax))
}
