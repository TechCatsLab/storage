package mysql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

// TableExist check whether a table exists
// Causing panic if lack of param
// Spaces at two sides will be removed when query
func TableExist(db *sql.DB, schema string) bool {
	database, table := parseTableSchema(db, schema)
	r := db.QueryRow(
		"SELECT TABLE_NAME "+
			"FROM information_schema.TABLES "+
			"WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?;", database, table,
	)
	return exist(r)
}

// CreateTable create a table, return errTableAlreadyExist if the table is already exist
func CreateTable(db *sql.DB, i interface{}) error {
	t := reflect.TypeOf(i)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	table := t.Name()
	return CreateTableWithName(db, i, table)
}

// CreateTableWithName create a table with specific name, return errTableAlreadyExist if the table is already exist
func CreateTableWithName(db *sql.DB, i interface{}, table string) error {
	if TableExist(db, table) {
		return errTableAlreadyExist
	}
	return CreateTableWithNameIfNotExist(db, i, table)
}

// CreateTableIfNotExist creates a table if it's not exist
// example:
// type CreateTableInstance struct {
// 	id        int32      `mysql:"_id, primarykey, autoincrement, notnull"`
// 	Name      string     `mysql:",unique, default:zhanghow, notnull, size:20"`
// 	CreatedAt *time.Time `mysql:"created_at, notnull"`
// }
//
// err := CreateTableIfNotExist(db, CreateTableInstance{})
// it is equal to :
// err := CreateTableWithNameIfNotExist(db, CreateTableInstance{}, "CreateTableInstance")
func CreateTableIfNotExist(db *sql.DB, i interface{}) error {
	t := reflect.TypeOf(i)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return CreateTableWithNameIfNotExist(db, i, t.Name())
}

// CreateTableWithNameIfNotExist creates a table with the specific name
func CreateTableWithNameIfNotExist(db *sql.DB, i interface{}, name string) error {
	name = strings.Trim(name, " ")
	if name == "" {
		panic(errEmptyParamTable)
	}
	t := reflect.TypeOf(i)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		panic(fmt.Sprintf("%s is not a struct type", t.String()))
	}
	if t.NumField() == 0 {
		panic("struct has zero field")
	}
	sqlTable := getTableSQL(name, t)
	_, err := db.Exec(sqlTable)
	return err
}

// getTableSQL get the SQL for create a table
func getTableSQL(name string, t reflect.Type) string {
	sqlColumns := getColumnsSQL(t)
	sqlTable := "CREATE TABLE IF NOT EXISTS " + name + "("
	for i, c := range sqlColumns {
		if i == 0 {
			sqlTable = sqlTable + c
		} else {
			sqlTable = sqlTable + "," + c
		}
	}
	return sqlTable + ");"
}

// getColumnsSQL create the columns part of SQL for create a table
func getColumnsSQL(t reflect.Type) (sqlColumns []string) {
	n := t.NumField()
	for i := 0; i < n; i++ {
		field := t.Field(i)
		if field.Anonymous && field.Type.Kind() == reflect.Struct {
			sqlSubColumns := getColumnsSQL(field.Type)
			sqlColumns = append(sqlColumns, sqlSubColumns...)
		} else {
			var (
				sqlColumn       string
				columnName      string
				columnType      string
				isPrimaryKey    bool
				isAutoIncrement bool
				isUnique        bool
				isNotNull       bool
				columnDefault   string
			)
			args := strings.Split(strings.Replace(field.Tag.Get("mysql"), " ", "", -1), ",")
			columnName = args[0]
			if columnName == "" {
				columnName = strings.ToLower(field.Name)
			}
			var fieldType reflect.Type
			if field.Type.Kind() == reflect.Ptr {
				fieldType = field.Type.Elem()
			} else {
				fieldType = field.Type
			}

			switch fieldType.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				columnType = "INT"
			case reflect.Float32:
				columnType = "FLOAT"
			case reflect.Float64:
				columnType = "DOUBLE"
			case reflect.String:
				columnType = "VARCHAR"
			default:
				ft := fieldType.String()
				if ft == "time.Time" {
					columnType = "DATE"
				} else {
					panic(fmt.Sprintf("unsuported type for mysql %s", ft))
				}
			}
			for _, arg := range args[1:] {
				argSplited := strings.SplitN(arg, ":", 2)
				switch argSplited[0] {
				case "size", "default":
					if len(argSplited) == 1 {
						panic(fmt.Sprintf("missing option value for option %v on field %v", argSplited[0], field.Name))
					}
				default:
					if len(argSplited) == 2 {
						panic(fmt.Sprintf("unexpected option value for option %v on field %v", argSplited[0], field.Name))
					}
				}

				switch argSplited[0] {
				case "size":
					columnType = columnType + "(" + argSplited[1] + ")"
				case "default":
					columnDefault = "DEFAULT '" + argSplited[1] + "'"
				case "primarykey":
					isPrimaryKey = true
				case "autoincrement":
					isAutoIncrement = true
				case "unique":
					isUnique = true
				case "notnull":
					isNotNull = true
				default:
					panic(fmt.Sprintf("Unrecognized tag option for field %v: %v", field.Name, argSplited[0]))
				}
			}
			sqlColumn = columnName + " " + columnType
			if isPrimaryKey {
				sqlColumn = sqlColumn + " PRIMARY KEY"
			}
			if isAutoIncrement {
				sqlColumn = sqlColumn + " AUTO_INCREMENT"
			}
			if isUnique {
				sqlColumn = sqlColumn + " UNIQUE"
			}
			if isNotNull {
				sqlColumn = sqlColumn + " NOT NULL"
			}
			if columnDefault != "" {
				sqlColumn = sqlColumn + " " + columnDefault
			}
			sqlColumns = append(sqlColumns, sqlColumn)
		}
	}
	return
}

// DropTable panic if schema does not assign a table, or there is no currently selected database
// and schema does schema does not assign a database
// Return errDropTableNotExist when table not exists
func DropTable(db *sql.DB, schema string) error {
	database, table := parseTableSchema(db, schema)
	if !TableExist(db, database+"."+table) {
		return errDropedTableNotExist
	}
	_, err := db.Exec("DROP TABLE " + database + "." + table)
	return err
}

// DropTableIfExist panic if schema does not assign a table, or there is no currently selected database
// and schema does schema does not assign a database
func DropTableIfExist(db *sql.DB, schema string) error {
	database, table := parseTableSchema(db, schema)
	_, err := db.Exec("DROP TABLE IF EXISTS " + database + "." + table)
	return err
}
