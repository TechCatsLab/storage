package mysql

import (
	"database/sql"
)

// NumTransaction return the number of transaction
func NumTransaction(db *sql.DB) (num int, err error) {
	err = db.QueryRow("SELECT COUNT(0) FROM information_schema.INNODB_TRX").Scan(&num)
	return
}
