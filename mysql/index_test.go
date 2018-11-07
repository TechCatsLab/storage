package mysql

import (
	"database/sql"
	"testing"

	"github.com/TechCatsLab/storage/mysql/constant"
)

func Test_index(t *testing.T) {
	db, err := sql.Open("mysql", constant.Dsn)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	// if err := CreateDatabase(db, dbInstance); err != nil {
	// 	t.Error(err)
	// }
	// if _, err = db.Exec("USE " + dbInstance); err != nil {
	// 	t.Error(err)
	// }
	// if err = CreateTable(db, testCreateTable{}); err != nil {
	// 	t.Error(err)
	// }
	// schema := dbInstance + ".testCreateTable"
	// if err = CreateColumn(db, schema, "column1", "INT"); err != nil {
	// 	t.Error(err)
	// }
	// if err = CreateIndex(db, schema, "myindex", []string{"column1"}, true, false); err != nil {
	// 	t.Error(err)
	// }
	// if !IndexExist(db, schema, "myindex") {
	// 	t.Error(errTestFaild)
	// }
	// if err = DropIndexIfExist(db, schema, "myindex"); err != nil {
	// 	t.Error(err)
	// }
	// if IndexExist(db, schema, "myindex") {
	// 	t.Error(errTestFaild)
	// }
	// if err = DropDatabase(db, dbInstance); err != nil {
	// 	t.Error(err)
	// }
}
