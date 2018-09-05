package check

import (
	"database/sql"

	"github.com/go-gorp/gorp"
	_ "github.com/go-sql-driver/mysql"
)

const dsn = "root:123456@tcp(127.0.0.1:3306)/"

func IsDbExist(dbName string) bool {
	db, err := sql.Open("mysql", dsn)
	checkErr(err)
	dbmp := &gorp.DbMap{Db: db, Dialect: gorp.MySQLDialect{Engine: "InnoDB", Encoding: "UTF8"}}
	var count int64
	count, err = dbmp.SelectInt("SELECT COUNT(0) "+
		"FROM information_schema.SCHEMATA "+
		"WHERE SCHEMA_NAME = ?;", dbName)
	checkErr(err)
	return count > 0
}

func IsTableExist(dbName, tableName string) bool {
	db, err := sql.Open("mysql", dsn)
	checkErr(err)
	dbmp := &gorp.DbMap{Db: db, Dialect: gorp.MySQLDialect{Engine: "InnoDB", Encoding: "UTF8"}}
	var count int64
	count, err = dbmp.SelectInt("SELECT COUNT(0) "+
		"FROM information_schema.TABLES "+
		"WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?;", dbName, tableName)
	checkErr(err)
	return count > 0
}

func IsColExist(dbName, tableName, ColName string) bool {
	db, err := sql.Open("mysql", dsn)
	checkErr(err)
	dbmp := &gorp.DbMap{Db: db, Dialect: gorp.MySQLDialect{Engine: "InnoDB", Encoding: "UTF8"}}
	var count int64
	count, err = dbmp.SelectInt("SELECT COUNT(0) "+
		"FROM information_schema.COLUMNS "+
		"WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?;", dbName, tableName, ColName)
	checkErr(err)
	return count > 0
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
