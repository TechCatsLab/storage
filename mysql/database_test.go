package mysql

import (
	"database/sql"
	"testing"

	"github.com/storage/mysql/constant"
)

var dbInstance = "db1"

func Test_DatabaseExist(t *testing.T) {
	db, err := sql.Open("mysql", constant.Dsn)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = CreateDatabaseIfNotExist(db, dbInstance)
	if err != nil {
		panic(err)
	}
	if !DatabaseExist(db, dbInstance) {
		panic("test err ")
	}
	if !DatabaseExist(db, " "+dbInstance+"  ") {
		panic("test err ")
	}
	_, err = db.Exec("drop database " + dbInstance)
	if err != nil {
		panic(err)
	}
	if DatabaseExist(db, dbInstance) {
		panic("test err ")
	}

	dbInstance = " "
	if !DatabaseExist(db, dbInstance) {
		panic("test err ")
	}
}

func Test_CreateDatabase(t *testing.T) {
	db, err := sql.Open("mysql", constant.Dsn)
	if err != nil {
		panic(err)
	}
	err = CreateDatabaseIfNotExist(db, "db1")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("drop database db1;")
	if err != nil {
		panic(err)
	}
}
