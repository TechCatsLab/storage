package mysql

import (
	"database/sql"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/storage/mysql/constant"
)

type testCreateTablePass struct {
	id        int32      `mysql:"_id, primarykey, autoincrement, notnull"`
	Name      string     `mysql:",unique, default:zhanghow, notnull, size:20"`
	CreatedAt *time.Time `mysql:"created_at, notnull"`
}

func Test_CreateTableIfNotExists(t *testing.T) {
	db, err := sql.Open("mysql", constant.Dsn)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = CreateTableIfNotExists(db, testCreateTablePass{})
	if err != nil {
		panic(err)
	}

	err = CreateTableIfNotExists(db, &testCreateTablePass{})
	if err != nil {
		panic(err)
	}
	db.Exec("drop table testCreateTablePass;")
}

func Test_TableExist(t *testing.T) {
	db, err := sql.Open("mysql", constant.Dsn)
	if err != nil {
		panic(err)
	}

	err = CreateDatabaseIfNotExist(db, "db1")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("use db1;")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS table1(id VARCHAR(3) PRIMARY KEY);")
	if err != nil {
		panic(err)
	}

	if !TableExist(db, "db1.table1") {
		panic("table not exist")
	}
	if !TableExist(db, "  db1 . table1  ") {
		panic("table not exist")
	}
	if !TableExist(db, "   . table1  ") {
		panic("table not exist")
	}
	if !TableExist(db, ". table1  ") {
		panic("table not exist")
	}
	if TableExist(db, " ytugihhug4567986cg  ") {
		panic("err test table exist")
	}
	if TableExist(db, ".") {
		panic("err test table exist")
	}
	if TableExist(db, "") {
		panic("err test table exist")
	}
	_, err = db.Exec("drop database db1")
	if err != nil {
		panic(err)
	}
}
