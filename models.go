package main

import (
	"database/sql"
)

// typedef for config items
type Config struct {
	Server string
	Instance string
	Username string
	Password string
	Database string
	Crypto string
	Mode string
	Whitelist []string
	Blacklist []string
	Type2 []string
	Timestamps []string
}

// Typedef for tables
type Table struct {
	name string
	rowCount int
	folder string
	where string
	timestamp string
	type2 bool
	columns []Column
}

// Typedef for columns
type Column struct {
	name sql.NullString
	dataType sql.NullString
	maxLength sql.NullString
	precision sql.NullString
	scale sql.NullString
	nullable sql.NullString
	ordinalPosition sql.NullString
	collationName sql.NullString
	primaryKey sql.NullString
}