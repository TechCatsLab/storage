package mysql

import (
	"database/sql"
	"strings"
)

// ColumnExist check whether a column exists, use the currently selected database if schema does not
// assign a database
// lacking of database, table, or column leads to panic
func ColumnExist(db *sql.DB, schema, column string) bool {
	database, table := parseTableSchema(db, schema)
	column = strings.Trim(column, " ")
	if column == "" {
		panic(errEmptyParamColumn)
	}
	r := db.QueryRow(
		"SELECT COLUMN_NAME "+
			"FROM information_schema.COLUMNS "+
			"WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?;", database, table, column,
	)
	return exist(r)
}

// CreateColumn create a column, if the column has been existed, return errColumnAlreadyExist
// lacking of database, table, column or columnType leads to panic
func CreateColumn(db *sql.DB, schema, column, columnType string) error {
	if ColumnExist(db, schema, column) {
		return errColumnAlreadyExist
	}
	if strings.Trim(columnType, " ") == "" {
		return errEmptyParamColType
	}
	database, table := parseTableSchema(db, schema)
	_, err := db.Exec("ALTER TABLE " + database + "." + table + " ADD " + column + " " + columnType)
	return err
}

// CreateColumnWithConstraint create a column with constraint
// lacking of needed param leads to panic
func CreateColumnWithConstraint(db *sql.DB, schema, column, columnType, defaul string, isPK, isUniq, isAutoIncr, isNotNull bool) error {
	if ColumnExist(db, schema, column) {
		return errColumnAlreadyExist
	}
	var constraint string
	if isPK {
		constraint = " PRIMARY KEY"
	}
	if isUniq {
		constraint += " UNIQUE"
	}
	if isAutoIncr {
		constraint += " AUTO_INCREMENT"
	}
	if isNotNull {
		constraint += " NOT NULL"
	}
	defaul = strings.Trim(defaul, " ")
	if defaul != "" {
		constraint += " DEFAULT '" + defaul + "'"
	}
	database, table := parseTableSchema(db, schema)
	_, err := db.Exec("ALTER TABLE " + database + "." + table + " ADD " + column + " " + columnType + constraint)
	return err
}

func DropColumn(db *sql.DB, schema, column string) error {
	column = strings.Trim(column, " ")
	if column == "" {
		panic(errEmptyParamColumn)
	}
	database, table := parseTableSchema(db, schema)
	schema = database + "." + table
	if !ColumnExist(db, schema, column) {
		return errDropedColumnNotExist
	}
	_, err := db.Exec("ALTER TABLE " + schema + " DROP COLUMN " + column)
	return err
}

func DropColumnIfExist(db *sql.DB, schema, column string) error {
	column = strings.Trim(column, " ")
	if column == "" {
		panic(errEmptyParamColumn)
	}
	database, table := parseTableSchema(db, schema)
	schema = database + "." + table
	if !ColumnExist(db, schema, column) {
		return nil
	}
	_, err := db.Exec("ALTER TABLE " + schema + " DROP COLUMN " + column)
	return err
}
