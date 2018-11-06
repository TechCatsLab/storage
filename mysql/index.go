package mysql

import (
	"database/sql"
	"errors"
	"strings"
)

func IndexExist(db *sql.DB, schema, index string) (bool, error) {
	index = strings.Trim(index, " ")
	if index == "" {
		return false, errEmptyParamIndex
	}
	database, table, err := parseTableSchema(db, schema)
	if err != nil {
		return false, err
	}
	r := db.QueryRow(
		"SELECT INDEX_NAME "+
			"FROM information_schema.statistics "+
			"WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND INDEX_NAME=?", database, table, index,
	)
	return exist(r)
}

// func CreateIndex(db *sql.DB, schema, index string, columns []string, unique, fulltext bool) error {
// 	if index = strings.Trim(index, " "); index == "" {
// 		panic(errEmptyParamIndex)
// 	}
// 	database, table := parseTableSchema(db, schema)
// 	schema = database + "." + table
// 	if IndexExist(db, schema, index) {
// 		return errIndexAlreadyExist
// 	}
// 	uniqueStr := ""
// 	if unique {
// 		uniqueStr = " UNIQUE"
// 	}
// 	fulltextStr := ""
// 	if fulltext {
// 		fulltextStr = " FULLTEXT"
// 	}
// 	query := "CREATE" + uniqueStr + fulltextStr + " INDEX " + index + " ON " + schema + "(" + strings.Join(columns, ",") + ")"
// 	_, err := db.Exec(query)
// 	return err
// }

func CreateIndexIfNotExist(db *sql.DB, schema, index string, columns []string, unique, fulltext bool) error {
	if index = strings.Trim(index, " "); index == "" {
		panic(errEmptyParamIndex)
	}
	database, table, err := parseTableSchema(db, schema)
	if err != nil {
		return err
	}
	schema = database + "." + table
	var isexist bool
	isexist, err = IndexExist(db, schema, index)
	if err != nil {
		return err
	}
	if isexist {
		return nil
	}
	if len(columns) == 0 {
		return errors.New("error empty param columns")
	}
	var uniqueStr string
	if unique {
		uniqueStr = " UNIQUE"
	}
	var fulltextStr string
	if fulltext {
		fulltextStr = " FULLTEXT"
	}
	query := "CREATE" + uniqueStr + fulltextStr + " INDEX " + index + " ON " + schema + "(" + strings.Join(columns, ",") + ")"
	_, err = db.Exec(query)
	return err
}

// func DropIndex(db *sql.DB, schema, index string) error {
// 	if index = strings.Trim(index, " "); index == "" {
// 		panic(errEmptyParamIndex)
// 	}
// 	database, table := parseTableSchema(db, schema)
// 	schema = database + "." + table
// 	if !IndexExist(db, schema, index) {
// 		return errDropedIndexNotExist
// 	}
// 	_, err := db.Exec("DROP INDEX " + index + " ON " + schema)
// 	return err
// }

func DropIndexIfExist(db *sql.DB, schema, index string) error {
	index = strings.Trim(index, " ")
	if index == "" {
		return errEmptyParamIndex
	}
	database, table, err := parseTableSchema(db, schema)
	if err != nil {
		return err
	}
	schema = database + "." + table
	var isexist bool
	isexist, err = IndexExist(db, schema, index)
	if err != nil {
		return err
	}
	if !isexist {
		return nil
	}
	_, err = db.Exec("DROP INDEX " + index + " ON " + schema)
	return err
}
