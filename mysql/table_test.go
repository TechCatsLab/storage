package mysql

import (
	"database/sql"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/storage/mysql/constant"
)

type testCreateTable struct {
	id        int32      `mysql:"_id, primarykey, autoincrement, notnull"`
	Name      string     `mysql:",unique, default:zhanghow, notnull, size:20"`
	CreatedAt *time.Time `mysql:"created_at, notnull"`
}

func Test_table(t *testing.T) {
	db, err := sql.Open("mysql", constant.Dsn)
	if err != nil {
		t.Error(err)
	}

	if err = CreateDatabaseIfNotExist(db, dbInstance); err != nil {
		t.Error(err)
	}
	if err = CreateDatabase(db, dbInstance); err != errDatabaseAlreadyExist {
		t.Error(errTestFaild)
	}
	if _, err = db.Exec("USE " + dbInstance); err != nil {
		t.Error(err)
	}
	if err = CreateTable(db, testCreateTable{}); err != nil {
		t.Error(err)
	}
	if !TableExist(db, " testCreateTable ") {
		t.Error(errTestFaild)
	}
	if err = CreateTable(db, &testCreateTable{}); err != errTableAlreadyExist {
		t.Error(err)
	}
	if err = CreateTableIfNotExist(db, testCreateTable{}); err != nil {
		t.Error(err)
	}
	if err = CreateTableWithSchema(db, testCreateTable{}, " table1 "); err != nil {
		t.Error(err)
	}
	if !TableExist(db, dbInstance+" . "+" table1 ") {
		t.Error(errTestFaild)
	}
	if err = CreateTableWithSchemaIfNotExist(db, testCreateTable{}, "table1"); err != nil {
		t.Error(err)
	}
	if err = CreateTableWithSchemaIfNotExist(db, testCreateTable{}, "table2"); err != nil {
		t.Error(err)
	}
	if !TableExist(db, " "+dbInstance+" . "+" table2 ") {
		t.Error(errTestFaild)
	}
	if err = DropTable(db, dbInstance+"."+" table1 "); err != nil {
		t.Error(err)
	}
	if TableExist(db, " "+dbInstance+" . "+" table1 ") {
		t.Error(errTestFaild)
	}
	if err = DropTableIfExist(db, dbInstance+"."+"table1"); err != nil {
		t.Error(err)
	}
	if err = DropTableIfExist(db, dbInstance+"."+"table2"); err != nil {
		t.Error(err)
	}
	if TableExist(db, dbInstance+"."+"table2") {
		t.Error(errTestFaild)
	}
	if err = DropDatabase(db, dbInstance); err != nil {
		t.Error(err)
	}
}
