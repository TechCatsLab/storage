package mysql

import (
	"errors"
)

var (
	errDatabaseAlreadyExist = errors.New("database already exist")
	errTableAlreadyExist    = errors.New("table already exist")
	errColumnAlreadyExist   = errors.New("column already exist")

	errNoSelectedDatabase = errors.New("no selected database")
	errEmptyParamDatabase = errors.New("param database is empty")
	errEmptyParamTable    = errors.New("param table is empty")
	errEmptyParamColumn   = errors.New("param column is empty")
	errEmptyParamColType  = errors.New("param columnType is empty")

	errDropedDatabaseNotExist = errors.New("drop a database that does not exist")
	errDropedTableNotExist    = errors.New("drop a table that does not exist")
	errDropedColumnNotExist   = errors.New("drop a column that does not exist")
)
