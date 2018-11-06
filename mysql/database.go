package mysql

import (
	"database/sql"
	"strings"
)

// DatabaseExist check whether a database exists
func DatabaseExist(db *sql.DB, database string) (bool, error) {
	database = strings.Trim(database, " ")
	if database == "" {
		return false, errEmptyParamDatabase
	}
	r := db.QueryRow(
		"SELECT SCHEMA_NAME "+
			"FROM information_schema.SCHEMATA "+
			"WHERE SCHEMA_NAME = ?;", database,
	)
	return exist(r)
}

// CreateDatabaseIfNotExist create a database if not exists
func CreateDatabaseIfNotExist(db *sql.DB, database string) error {
	database = strings.Trim(database, " ")
	if database == "" {
		return errEmptyParamDatabase
	}
	_, err := db.Exec("CREATE DATABASE IF NOT EXISTS " + database)
	return err
}

// DropDatabaseIfExist drop a databse if exists
// Drop the current database if param database is empty
func DropDatabaseIfExist(db *sql.DB, database string) error {
	database = strings.Trim(database, " ")
	if database == "" {
		database, err := getDatabaseName(db)
		if err != nil {
			return err
		}
		_, err = db.Exec("DROP DATABASE " + database)
		return err
	}
	_, err := db.Exec("DROP DATABASE IF EXISTS " + database)
	return err
}
