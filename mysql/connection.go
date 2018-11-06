package mysql

import (
	"database/sql"
)

// NumProcess return the number of transaction
func NumProcess(db *sql.DB) (num int, err error) {
	err = db.QueryRow("SELECT COUNT(0) FROM information_schema.PROCESSLIST").Scan(&num)
	return
}
