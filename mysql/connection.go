package mysql

import (
	"database/sql"
)

func NumProcess(db *sql.DB) (num int32, err error) {
	r := db.QueryRow("SELECT COUNT(0) FROM information_schema.PROCESSLIST;")
	err = r.Scan(&num)
	return
}
