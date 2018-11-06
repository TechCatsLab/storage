package mysql

import (
	"database/sql"
	"strings"
)

// ColumnExist check whether a column exists.
// Use the currently selected database if schema does not contain one
// Empty table or empty column leads to panic
func ColumnExist(db *sql.DB, schema, column string) (bool, error) {
	database, table, err := parseTableSchema(db, schema)
	if err != nil {
		return false, err
	}
	column = strings.Trim(column, " ")
	if column == "" {
		return false, errEmptyParamColumn
	}
	r := db.QueryRow(
		`SELECT COLUMN_NAME 
			FROM information_schema.COLUMNS 
			WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?`, database, table, column,
	)
	return exist(r)
}

// CreateColumnIfNotExist create a column if not exist.
func CreateColumnIfNotExist(db *sql.DB, schema, column, columnType string) error {
	database, table, err := parseTableSchema(db, schema)
	if err != nil {
		return err
	}
	var colexist bool
	colexist, err = ColumnExist(db, database+"."+table, column)
	if err != nil {
		return err
	}
	if colexist {
		return nil
	}
	if strings.Trim(columnType, " ") == "" {
		return errEmptyParamColType
	}
	_, err = db.Exec("ALTER TABLE " + database + "." + table + " ADD " + column + " " + columnType)
	return err
}

// CreateColumnWithConstraint create a column with constraint if not exist.
// Empty param leads to panic.
func CreateColumnWithConstraint(db *sql.DB, schema, column, columnType, deflt string, isPK, isUniq, isAutoIncr, isNotNull bool) error {
	database, table, err := parseTableSchema(db, schema)
	if err != nil {
		return err
	}
	var colexist bool
	colexist, err = ColumnExist(db, database+"."+table, column)
	if err != nil {
		return err
	}
	if colexist {
		return nil
	}
	columnType = strings.Trim(columnType, " ")
	if columnType == "" {
		return errEmptyParamColType
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
	deflt = parseDefault(columnType, deflt)
	if deflt != "" {
		constraint += " DEFAULT " + deflt
	}
	_, err = db.Exec("ALTER TABLE " + database + "." + table + " ADD " + column + " " + columnType + constraint)
	return err
}

// parseDefault add single quote to deflt when colType is VARCHAR
func parseDefault(colType, deflt string) string {
	deflt = strings.Trim(deflt, " ")
	if deflt == "" {
		return ""
	}
	if strings.Contains(strings.ToLower(colType), "varchar") {
		return "'" + deflt + "'"
	}
	return deflt
}

// DropColumnIfExist drop a specific cloumn if exists.
// Empty param leads to panic.
func DropColumnIfExist(db *sql.DB, schema, column string) error {
	column = strings.Trim(column, " ")
	if column == "" {
		return errEmptyParamColumn
	}
	database, table, err := parseTableSchema(db, schema)
	if err != nil {
		return err
	}
	schema = database + "." + table
	var isexist bool
	isexist, err = ColumnExist(db, schema, column)
	if err != nil {
		return err
	}
	if !isexist {
		return nil
	}
	_, err = db.Exec("ALTER TABLE " + schema + " DROP COLUMN " + column)
	return err
}
