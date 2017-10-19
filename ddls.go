package main

import (
	"database/sql"
	"fmt"

	"github.com/pkg/errors"
)

// schemaVariant is the type of schema we want to benchmark on. Tables are
// either interleaved (Interleaved) or not (Normal).
type schemaVariant string

const (
	Normal      schemaVariant = "normal"
	Interleaved               = "interleaved"
)

type tableName string

const (
	Merchant           tableName = "merchant"
	Product                      = "product"
	Variant                      = "variant"
	Store                        = "store"
	ProductInterleaved           = "product (interleaved)"
	VariantInterleaved           = "variant (interleaved)"
	StoreInterleaved             = "store (interleaved)"
)

var interleaveName = map[tableName]tableName{
	Product: ProductInterleaved,
	Variant: VariantInterleaved,
	Store:   StoreInterleaved,
}

// Map of all DDL statements that are run when loadSchema is invoked.
var ddlStmts = map[tableName]string{
	Merchant: `
	create table merchant (
	  m_id		integer	      not null,
	  m_name	text,
	  m_address	text,
	  primary key (m_id)
	)`,

	Product: `
	create table product (
	  p_m_id	integer	      not null,
	  p_id		integer	      not null,
	  p_name	text,
	  p_desc	text,
	  primary key (p_m_id, p_id)
	)`,
	// foreign key (p_m_id) references merchant (m_id)

	Variant: `
	create table variant (
	  v_m_id	integer	      not null,
	  v_p_id	integer	      not null,
	  v_id		integer	      not null,
	  v_name	text,
	  v_qty		integer,
	  v_price	decimal,
	  primary key (v_m_id, v_p_id, v_id)
	)`,
	// foreign key (v_m_id, v_p_id) references product (p_m_id, p_id)

	Store: `
	create table store (
	  s_m_id	integer	      not null,
	  s_id		integer	      not null,
	  s_name	text,
	  s_address	text,
	  primary key (s_m_id, s_id)
	)`,
	// foreign key(s_m_id) references merchant (m_id)
}

var insertStmts = map[tableName]string{
	Merchant: `
  insert into merchant
  (m_id, m_name, m_address)
  values
  `,
	Product: `
  insert into product
  (p_m_id, p_id, p_name, p_desc)
  values
  `,
	Variant: `
  insert into variant
  (v_m_id, v_p_id, v_id, v_name, v_qty, v_price)
  values
  `,
	Store: `
  insert into store
  (s_m_id, s_id, s_name, s_address)
  values
  `,
}

type columnType int

const (
	Text columnType = iota
	Int
	Dec
	PkeyInt
	FkeyInt
)

var tableTypes = map[tableName][]columnType{
	Merchant: []columnType{PkeyInt, Text, Text},
	Product:  []columnType{FkeyInt, PkeyInt, Text, Text},
	Variant:  []columnType{FkeyInt, FkeyInt, PkeyInt, Text, Int, Dec},
	Store:    []columnType{FkeyInt, PkeyInt, Text, Text},
}

type interleaveInfo struct {
	name           tableName
	interleaveStmt string
}

var toInterleave = []interleaveInfo{
	{
		name:           Product,
		interleaveStmt: " interleave in parent merchant (p_m_id)",
	},
	{
		name:           Variant,
		interleaveStmt: " interleave in parent product (v_m_id, v_p_id)",
	},
	{
		name:           Store,
		interleaveStmt: " interleave in parent merchant (s_m_id)",
	},
}

var variantTables = map[schemaVariant][]tableName{
	Normal: []tableName{
		Merchant,
		Product,
		Variant,
		Store,
	},
	Interleaved: []tableName{
		Merchant,
		ProductInterleaved,
		VariantInterleaved,
		StoreInterleaved,
	},
}

var tableRows = make(map[tableName]int)

func initDDL() {
	// Initialize interleave DDL statements.
	for _, info := range toInterleave {
		ddlStmts[interleaveName[info.name]] = fmt.Sprintf("%s %s", ddlStmts[info.name], info.interleaveStmt)
	}

	// Interleave tables have same column types.
	for name, types := range tableTypes {
		tableTypes[interleaveName[name]] = types
	}

	// Interleave tables have the same insert statements.
	for name, stmt := range insertStmts {
		insertStmts[interleaveName[name]] = stmt
	}

	tableRows[Merchant] = *nMerchants
	tableRows[Product] = *nProducts
	tableRows[Variant] = *nVariants
	tableRows[Store] = *nStores

	// Interleave tables have the same number of rows.
	for name, n := range tableRows {
		tableRows[interleaveName[name]] = n
	}

	// Append semicolons to DDL statements.
	for name, stmt := range ddlStmts {
		// Append semicolon to all DDL statements.
		ddlStmts[name] = fmt.Sprintf("%s;", stmt)
	}
}

func loadSchema(db *sql.DB) error {
	stderr.Println("Creating tables with default schema")
	for _, stmtName := range variantTables[schemaVar] {
		if _, err := db.Exec(ddlStmts[stmtName]); err != nil {
			return errors.Wrap(err, "loading schema failed")
		}
	}

	return nil
}
