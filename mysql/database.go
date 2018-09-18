package mysql

import (
	"database/sql"
	"strings"
)

// DatabaseExist check whether a database exists
// Return errEmptyParamDatabase if database is blank or a string of spaces
func DatabaseExist(db *sql.DB, database string) bool {
	database = strings.Trim(database, " ")
	if database == "" {
		panic(errEmptyParamDatabase)
	}
	r := db.QueryRow(
		"SELECT SCHEMA_NAME "+
			"FROM information_schema.SCHEMATA "+
			"WHERE SCHEMA_NAME = ?;", database,
	)
	return exist(r)
}

// CreateDatabase create a database, return errDatabaseAlreadyExist if the database is already exist
// Return errEmptyParamDatabase if database is blank or a string of spaces
func CreateDatabase(db *sql.DB, database string) error {
	database = strings.Trim(database, " ")
	if database == "" {
		return errEmptyParamDatabase
	}
	if DatabaseExist(db, database) {
		return errDatabaseAlreadyExist
	}
	_, err := db.Exec("CREATE DATABASE " + database)
	return err
}

// CreateDatabaseIfNotExist create a database if not exists
// Return errEmptyParamDatabase if database is blank or a string of spaces
func CreateDatabaseIfNotExist(db *sql.DB, database string) error {
	database = strings.Trim(database, " ")
	if database == "" {
		return errEmptyParamDatabase
	}
	_, err := db.Exec("CREATE DATABASE IF NOT EXISTS " + database)
	return err
}

// DropDatabase drop a database
// Drop the currently selected database if param database is blank or a string of spaces
// Return errDropedDatabaseNotExist if database not exists
func DropDatabase(db *sql.DB, database string) error {
	database = strings.Trim(database, " ")
	if database == "" {
		database = getDatabaseName(db)
		_, err := db.Exec("DROP DATABASE " + database)
		return err
	}
	if !DatabaseExist(db, database) {
		return errDropedDatabaseNotExist
	}
	_, err := db.Exec("DROP DATABASE " + database)
	return err
}

// DropDatabaseIfExist drop a databse if exists
// Drop the currently selected database if param database is blank or a string of spaces
func DropDatabaseIfExist(db *sql.DB, database string) error {
	database = strings.Trim(database, " ")
	if database == "" {
		database = getDatabaseName(db)
		_, err := db.Exec("DROP DATABASE " + database)
		return err
	}
	_, err := db.Exec("DROP DATABASE IF EXISTS " + database)
	return err
}
