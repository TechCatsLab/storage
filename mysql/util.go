package mysql

import (
	"database/sql"
	"strings"
)

func parseTableSchema(db *sql.DB, schema string) (database string, table string, err error) {
	schemaSlice := strings.SplitN(schema, ".", 2)
	if len(schemaSlice) == 2 {
		database, table = schemaSlice[0], schemaSlice[1]
		database = strings.TrimLeft(database, " ")
		database = strings.TrimRight(database, " ")
		table = strings.TrimLeft(table, " ")
		table = strings.TrimRight(table, " ")
		if database == "" {
			database, err = getDatabaseName(db)
		}
		return
	}
	database, err = getDatabaseName(db)
	table = schemaSlice[0]
	return
}

func getDatabaseName(db *sql.DB) (database string, err error) {
	r := db.QueryRow(
		"SELECT SCHEMA_NAME " +
			"FROM information_schema.SCHEMATA " +
			"WHERE SCHEMA_NAME = DATABASE();",
	)
	err = r.Scan(&database)

	// not select a database, database name is regard as blank
	if err == sql.ErrNoRows {
		err = nil
	}
	return
}

func exist(r *sql.Row) bool {
	var dest string
	err := r.Scan(&dest)
	switch err {
	case sql.ErrNoRows:
		return false
	case nil:
		return true
	default:
		panic(err)
	}
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
