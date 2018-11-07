package mysql

import (
	"database/sql"
	"strings"
)

type DescColumn struct {
	// The name of the catalog to which the table containing the column belongs. This value is always def.
	Catalog              string
	Schema               string
	Table                string
	Name                 string
	Position             uint
	Default              *sql.NullString
	Nullable             bool
	DataType             string         // the type name only with no other information
	MaxVarcharLen        *sql.NullInt64 // For string columns, the maximum length in characters
	MaxByteLen           *sql.NullInt64 // For string columns, the maximum length in bytes
	NumPrecision         *sql.NullInt64 // For numeric columns, the numeric precision
	NumScale             *sql.NullInt64 // For numeric columns, the numeric scale
	DatetimePrecision    *sql.NullInt64 // For temporal columns, the fractional seconds precision
	Charset              *sql.NullString
	Collation            *sql.NullString
	ColumnType           string // The column data type
	ColumnKey            string // PRI | UNI | MUL, see https://dev.mysql.com/doc/refman/5.7/en/columns-table.html#COLUMN_KEY
	Extra                string
	Privileges           string // The privileges you have for the column.
	Comment              string // Any comment included in the column definition.
	GenerationExpression string
}

// DescribeColumn get the detail information of column
func DescribeColumn(db *sql.DB, schema, column string) (*DescColumn, error) {
	database, table, err := parseTableSchema(db, schema)
	if err != nil {
		return nil, err
	}
	column = strings.Trim(column, " ")
	if column == "" {
		return nil, errEmptyParamColumn
	}
	dc := &DescColumn{
		Default:           &sql.NullString{},
		MaxVarcharLen:     &sql.NullInt64{},
		MaxByteLen:        &sql.NullInt64{},
		NumPrecision:      &sql.NullInt64{},
		NumScale:          &sql.NullInt64{},
		DatetimePrecision: &sql.NullInt64{},
		Charset:           &sql.NullString{},
		Collation:         &sql.NullString{},
	}
	var nullable string
	err = db.QueryRow(
		`SELECT * 
			FROM information_schema.COLUMNS 
			WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?`, database, table, column,
	).Scan(&dc.Catalog, &dc.Schema, &dc.Table, &dc.Name, &dc.Position, dc.Default, &nullable, &dc.DataType,
		dc.MaxVarcharLen, dc.MaxByteLen, dc.NumPrecision, dc.NumScale, dc.DatetimePrecision, dc.Charset, dc.Collation,
		&dc.ColumnType, &dc.ColumnKey, &dc.Extra, &dc.Privileges, &dc.Comment, &dc.GenerationExpression,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errColumnNotExist
		}
		return nil, err
	}

	if nullable == "YES" {
		dc.Nullable = true
	} else if nullable == "NO" {
		dc.Nullable = false
	} else {
		panic("unknown isNullable value: " + nullable)
	}

	return dc, nil
}

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
