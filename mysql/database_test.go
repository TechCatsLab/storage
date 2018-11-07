package mysql

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/TechCatsLab/storage/mysql/constant"
)

var dbInstance = "testDatabase1014"
var errTestFaild = errors.New("test faild")

func Test_database(t *testing.T) {
	db, err := sql.Open("mysql", constant.Dsn)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	// if err = CreateDatabase(db, dbInstance); err != nil {
	// 	t.Error(err)
	// }
	// if !DatabaseExist(db, dbInstance) {
	// 	t.Error(errTestFaild)
	// }
	// if err = DropDatabase(db, " "+dbInstance+"  "); err != nil {
	// 	t.Error(err)
	// }
	// if err = DropDatabaseIfExist(db, "  "+dbInstance+" "); err != nil {
	// 	t.Error(err)
	// }
	// if DatabaseExist(db, dbInstance) {
	// 	t.Error(errTestFaild)
	// }
}
