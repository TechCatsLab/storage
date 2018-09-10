package mysql

import (
	"database/sql"
	"testing"

	"github.com/storage/mysql/constant"
)

func Test_column(t *testing.T) {
	db, err := sql.Open("mysql", constant.Dsn)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	if err := CreateDatabase(db, dbInstance); err != nil {
		t.Error(err)
	}
	if _, err = db.Exec("USE " + dbInstance); err != nil {
		t.Error(err)
	}
	if err = CreateTableWithName(db, testCreateTable{}, "table1"); err != nil {
		t.Error(err)
	}
	if err = CreateColumn(db, "table1", "column1", "VARCHAR(10)"); err != nil {
		t.Error(err)
	}
	if !ColumnExist(db, "table1", " column1 ") {
		t.Error(errTestFaild)
	}
	if err = CreateColumnWithConstraint(db, dbInstance+".table1", "column2", "DATE", "2018-09-11", false, true, false, true); err != nil {
		t.Error(err)
	}
	if err = DropColumn(db, dbInstance+".table1", "column1"); err != nil {
		t.Error(err)
	}
	if err = DropColumnIfExist(db, dbInstance+".table1", "column2"); err != nil {
		t.Error(err)
	}
	if ColumnExist(db, "table1", " column2 ") {
		t.Error(errTestFaild)
	}
	if err = DropColumnIfExist(db, dbInstance+".table1", "column2"); err != nil {
		t.Error(err)
	}
	if err = DropDatabase(db, ""); err != nil {
		t.Error(err)
	}
}
