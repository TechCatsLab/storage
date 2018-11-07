package mysql

import (
	"database/sql"
	"testing"

	"github.com/TechCatsLab/storage/mysql/constant"
)

func Test_connection(t *testing.T) {
	db, err := sql.Open("mysql", constant.Dsn)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	// var rs *sql.Rows
	// rs, err = db.Query("SELECT * FROM information_schema.PROCESSLIST")
	// if err != nil {
	// 	t.Error(err)
	// }
	// defer rs.Close()

	// var (
	// 	id      int
	// 	user    string
	// 	host    string
	// 	DB      sql.NullString
	// 	command string
	// 	TIME    string
	// 	state   string
	// 	info    sql.NullString
	// )
	// for rs.Next() {
	// 	err = rs.Scan(&id, &user, &host, &DB, &command, &TIME, &state, &info)
	// 	if err != nil {
	// 		t.Error(err)
	// 	}
	// 	fmt.Println(id, user, host, DB, command, TIME, state, info)
	// }
	// var numProcess int32
	// numProcess, err = NumProcess(db)
	// if err != nil {
	// 	t.Error(err)
	// }
	// fmt.Println("numProcess:", numProcess)
	// time.Sleep(5 * time.Second)
}
