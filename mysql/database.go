package mysql

import (
	"database/sql"
	"strings"
)

func CreateDatabaseIfNotExist(db *sql.DB, database string) error {
	_, err := db.Exec("CREATE DATABASE IF NOT EXISTS " + database)
	return err
}

func DatabaseExist(db *sql.DB, database string) bool {
	database = strings.Trim(database, " ")
	if database == "" {
		r := db.QueryRow(
			"SELECT SCHEMA_NAME " +
				"FROM information_schema.SCHEMATA " +
				"WHERE SCHEMA_NAME = DATABASE();",
		)
		return exist(r)
	}
	r := db.QueryRow(
		"SELECT SCHEMA_NAME "+
			"FROM information_schema.SCHEMATA "+
			"WHERE SCHEMA_NAME = ?;", database,
	)
	return exist(r)
}
