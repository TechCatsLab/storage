package mysql

import (
	"database/sql"
	"strings"
)

func ColumnExist(db *sql.DB, schema, column string) bool {
	database, table, err := parseTableSchema(db, schema)
	checkErr(err)
	column = strings.Trim(column, " ")
	r := db.QueryRow(
		"SELECT COLUMN_NAME "+
			"FROM information_schema.COLUMNS "+
			"WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?;", database, table, column,
	)
	return exist(r)
}

func AddColumn(db *sql.DB, schema, column, mysqlColType string) {

}
