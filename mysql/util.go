package mysql

import (
	"database/sql"
	"reflect"
	"strings"
)

// parseTableSchema parse database(equal to schema in mysql) name and table name in param schema.
// For instance:
// 		1. parseTableSchema(db, "mydb.mytable") return "mydb" and "mytable"
// 		2. parseTableSchema(db, ".mytable") return current database and "mytable"
// 		3. parseTableSchema(db, "mytable") equal to instance 2
// In other case, err != nil.
func parseTableSchema(db *sql.DB, schema string) (database, table string, err error) {
	schemaSlice := strings.SplitN(schema, ".", 2)
	if len(schemaSlice) == 2 {
		database, table = strings.Trim(schemaSlice[0], " "), strings.Trim(schemaSlice[1], " ")
		if table == "" {
			return "", "", errEmptyParamTable
		}
		if database == "" {
			database, err = getDatabaseName(db)
			if err != nil {
				return "", "", err
			}
		}
		return
	}
	table = strings.Trim(schemaSlice[0], " ")
	if table == "" {
		return "", "", errEmptyParamTable
	}
	database, err = getDatabaseName(db)
	if err != nil {
		return "", "", err
	}
	return
}

// parseTableSchemaDefault parse database(equal to schema in mysql) name and table name in param schema.
// For instance(similar to parseTableSchema, but return name of i when table is empty):
// 		1. parseTableSchemaDefault(db, "mydb.mytable") return "mydb" and "mytable"
// 		2. parseTableSchemaDefault(db, ".mytable") return current database and "mytable"
// 		3. parseTableSchemaDefault(db, "mytable") equal to instance 2
//		4. parseTableSchemaDefault(db, ".") return current database and name of i
// 		5. parseTableSchemaDefault(db, "") equal to instance 4
// 		6. parseTableSchemaDefault(db, "mydb.") return "mydb" and name of i
// In other case, err != nil.
func parseTableSchemaDefault(db *sql.DB, i interface{}, schema string) (database, table string, err error) {
	schemaSlice := strings.SplitN(schema, ".", 2)
	if len(schemaSlice) == 2 {
		database, table = strings.Trim(schemaSlice[0], " "), strings.Trim(schemaSlice[1], " ")
		if table == "" {
			table = getInterfaceName(i)
		}
		if database == "" {
			database, err = getDatabaseName(db)
			if err != nil {
				return "", "", err
			}
		}
		return
	}
	table = strings.Trim(schemaSlice[0], " ")
	if table == "" {
		table = getInterfaceName(i)
	}
	database, err = getDatabaseName(db)
	if err != nil {
		return "", "", err
	}
	return
}

// getDatabaseName gets the name of the current database
func getDatabaseName(db *sql.DB) (database string, err error) {
	err = db.QueryRow(
		"SELECT SCHEMA_NAME " +
			"FROM information_schema.SCHEMATA " +
			"WHERE SCHEMA_NAME = DATABASE();",
	).Scan(&database)

	if err != nil {
		// no currently selected database
		if err == sql.ErrNoRows {
			return "", errNoSelectedDatabase
		}

		panic(err)
	}
	return
}

// getInterfaceName get the name of interface, get the name of element if i is a pointer type
func getInterfaceName(i interface{}) string {
	t := reflect.TypeOf(i)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.Name()
}

func exist(r *sql.Row) (bool, error) {
	var dest string
	err := r.Scan(&dest)
	switch err {
	case sql.ErrNoRows:
		return false, nil
	case nil:
		return true, nil
	default:
		return false, err
	}
}
