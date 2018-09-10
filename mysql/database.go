package mysql

import (
	"database/sql"
	"strings"
)

// CreateDatabase create a database, return a errDatabaseAlreadyExist if the database is already exist
// database should not be empty, or a string of spaces
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
// database should not be empty, or a string of spaces
func CreateDatabaseIfNotExist(db *sql.DB, database string) error {
	database = strings.Trim(database, " ")
	if database == "" {
		return errEmptyParamDatabase
	}
	_, err := db.Exec("CREATE DATABASE IF NOT EXISTS " + database)
	return err
}

// DatabaseExist check whether a database exists
// database should not be empty, or a string of spaces
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

// DropDatabase drop a database
// If param database is blank or a string of spaces, drop the currently selected database
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

// DropDatabaseIfExist drop a databse if exist
// If param database is blank or a string of spaces, drop the currently selected database
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
