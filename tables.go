package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"
)

func getTables(database string, blacklist []string, dbConnection* sql.DB) ([]string) {

	// Change the query with the blacklist
	var blacklistString string
	if len(blacklist) > 0 {
		blacklistString += "AND TABLE_NAME NOT IN ("
		for index, item := range blacklist {
			if index == 0 {
				blacklistString += "'" + item + "'"
			} else {
				blacklistString += ",'" + item + "'"
			}
		}
		blacklistString += ")"
	} else {
		blacklistString = ""
	}

	queryString := fmt.Sprintf(`
		SELECT
			TABLE_NAME
		FROM
			INFORMATION_SCHEMA.TABLES
		WHERE
			TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG = '%s'
			%s
	`, database, blacklistString)

	// Open the database query and get ready to read results.
	query, err := dbConnection.Query(queryString)
	if err != nil {
		log.Fatal(err)
	}
	defer query.Close()

	// Generate the slice of table names.
	tables := make([]string, 0)

	// Go through the results and create an array of results.
	for query.Next() {
		var column string
		query.Scan(&column)
		tables = append(tables, column)
	}

	return tables
}

func getTableMetadata(tableName string, dbConnection* sql.DB) (Table) {

	queryString := fmt.Sprintf(`
		SELECT DISTINCT
		    c.name 'Column Name',
		    t.Name 'Data Type',
		    c.max_length 'Max Length',
		    c.precision 'Precision',
		    c.scale 'Scale',
		    c.is_nullable 'Is Nullable',
		    c.column_id 'Ordinal Position',
		    c.collation_name 'Collation Name',
		    ISNULL(i.is_primary_key, 0) 'Primary Key'
		FROM    
		    sys.columns c
		INNER JOIN 
		    sys.types t ON c.user_type_id = t.user_type_id
		LEFT OUTER JOIN 
		    sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
		LEFT OUTER JOIN 
		    sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
		WHERE
		    c.object_id = OBJECT_ID('%s')
	`, tableName)

	// Open the database query and get ready to read results.
	query, err := dbConnection.Query(queryString)
	if err != nil {
		log.Fatal(err)
	}
	defer query.Close()

	var table Table
	table.name = tableName
	table.type2 = false

	// Go through the results and create an array of results.
	for query.Next() {
		var metadata Column
		query.Scan(
			&metadata.name,
			&metadata.dataType,
			&metadata.maxLength,
			&metadata.precision,
			&metadata.scale,
			&metadata.nullable,
			&metadata.ordinalPosition,
			&metadata.collationName,
			&metadata.primaryKey,
		)
		table.columns = append(table.columns, metadata)
	}

	return table
}

func getRowCount(tableName string, dbConnection* sql.DB) (int) {

	queryString := fmt.Sprintf("SELECT COUNT(*) AS 'count' FROM %s", tableName)

	// Open the database query and get ready to read results.
	query, err := dbConnection.Query(queryString)
	if err != nil {
		log.Fatal(err)
	}
	defer query.Close()

	// Row count
	var count int

	// Go through the results and create an array of results.
	for query.Next() {
		query.Scan(&count)
	}

	return count
}

func getMaxTimestamp(tableName string, timestampName string, dbConnection* sql.DB) (time.Time) {

	queryString := fmt.Sprintf("SELECT MAX(%s) AS '%s' FROM %s", timestampName, timestampName, tableName)

	// Open the database query and get ready to read results.
	query, err := dbConnection.Query(queryString)
	if err != nil {
		log.Fatal(err)
	}
	defer query.Close()

	// Row count
	var timestamp time.Time

	// Go through the results and create an array of results.
	for query.Next() {
		query.Scan(&timestamp)
	}

	return timestamp
}