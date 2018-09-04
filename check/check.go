package check

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/go-gorp/gorp"
	_ "github.com/go-sql-driver/mysql"
)

const dsnFormat = "root:123456@tcp(127.0.0.1:3306)/%s"

func IsDbExist(dbName string) bool { // todo: split to 2 part
	db := getDb(dbName)
	return isDbExist(db)
}

func isDbExist(db *sql.DB) bool {
	err := db.Ping()
	if err != nil {
		if strings.Contains(err.Error(), "Unknown database") {
			return false
		}
		panic(err)
	}
	return true
}

func IsTableExist(dbName, tableName string) bool {
	db := getDb(dbName)
	if !isDbExist(db) {
		return false
	}
	dbmp := &gorp.DbMap{Db: db, Dialect: gorp.MySQLDialect{Engine: "InnoDB", Encoding: "UTF8"}}
	count, err := dbmp.SelectInt("SELECT COUNT(0) "+
		"FROM information_schema.TABLES "+
		"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?;", tableName)
	if err != nil {
		panic(err)
	} else {
		return count > 0
	}
}

func IsColExist(dbName, tableName, ColName string) bool { // todo:id db not exist, return false
	db := getDb(dbName)
	if !isDbExist(db) {
		return false
	}
	dbmp := &gorp.DbMap{Db: db, Dialect: gorp.MySQLDialect{Engine: "InnoDB", Encoding: "UTF8"}}
	count, err := dbmp.SelectInt("SELECT COUNT(0) "+
		"FROM information_schema.COLUMNS "+
		"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?;", tableName, ColName)
	if err != nil {
		panic(err)
	} else {
		return count > 0
	}
}

func getDb(dbName string) *sql.DB {
	dsn := fmt.Sprintf(dsnFormat, dbName)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	return db
}
