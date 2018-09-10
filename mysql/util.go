package mysql

import (
	"database/sql"
	"strings"
)

func parseTableSchema(db *sql.DB, schema string) (database string, table string) {
	schemaSlice := strings.SplitN(schema, ".", 2)
	if len(schemaSlice) == 2 {
		database, table = strings.Trim(schemaSlice[0], " "), strings.Trim(schemaSlice[1], " ")
		if table == "" {
			panic(errEmptyParamTable)
		}
		if database == "" {
			database = getDatabaseName(db)
		}
		return
	}
	table = strings.Trim(schemaSlice[0], " ")
	if table == "" {
		panic(errEmptyParamTable)
	}
	database = getDatabaseName(db)
	return
}

// getDatabaseName gets the name of the currently selected database
// Causing panic if there is no selected database
func getDatabaseName(db *sql.DB) string {
	var database string
	r := db.QueryRow(
		"SELECT SCHEMA_NAME " +
			"FROM information_schema.SCHEMATA " +
			"WHERE SCHEMA_NAME = DATABASE();",
	)
	err := r.Scan(&database)

	// no selected database
	if err == sql.ErrNoRows {
		err = errNoSelectedDatabase
	}
	if err != nil {
		panic(err)
	}
	return database
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
