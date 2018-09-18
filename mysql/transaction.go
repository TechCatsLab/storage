package mysql

import (
	"database/sql"
)

func NumTransaction(db *sql.DB) error {
	_, err := db.Exec("SELECT * FROM information_schema.INNODB_TRX")
	return err
}
