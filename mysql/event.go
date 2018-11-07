package mysql

import (
	"database/sql"
	"strings"
)

const (
	EventStatusEnable           = "ENABLE"
	EventStatusDisable          = "DISABLE"
	EventStatusSlavesideDisable = "SLAVESIDE_DISABLED"
)

var (
	eventSQLs = []string{
		`SELECT STATUS 
			FROM INFORMATION_SCHEMA.EVENTS 
			WHERE EVENT_SCHEMA=? AND EVENT_NAME=?`,

		`SELECT COUNT(0)
			FROM INFORMATION_SCHEMA.EVENTS`,

		`SELECT COUNT(0)
			FROM INFORMATION_SCHEMA.EVENTS
			WHERE STATUS=?`,

		`SELECT COUNT(0)
			FROM INFORMATION_SCHEMA.EVENTS
			WHERE EVENT_SCHEMA=? AND STATUS=?`,
	}
)

// EventExist check wheather database.event exist. Using current database when param database is empty.
func EventExist(db *sql.DB, database, event string) (bool, error) {
	database = strings.Trim(database, " ")
	var err error
	if database == "" {
		database, err = getDatabaseName(db)
		if err != nil {
			return false, err
		}
	}
	event = strings.Trim(event, " ")
	r := db.QueryRow(eventSQLs[0], database, event)
	return exist(r)
}

// EventStatus get the event status. Using current database when param database is empty.
func EventStatus(db *sql.DB, database, event string) (string, error) {
	database = strings.Trim(database, " ")
	var err error
	if database == "" {
		database, err = getDatabaseName(db)
		if err != nil {
			return "", err
		}
	}
	event = strings.Trim(event, " ")
	var status string
	err = db.QueryRow(eventSQLs[0], database, event).Scan(&status)
	return status, err
}

// NumAllEvent get amount of event in all datababe.
func NumAllEvent(db *sql.DB) (uint, error) {
	var num uint
	err := db.QueryRow(eventSQLs[1]).Scan(&num)
	return num, err
}

// NumAllEventWithStatus get amount of event with specific status in all datababe.
func NumAllEventWithStatus(db *sql.DB, status string) (uint, error) {
	var num uint
	err := db.QueryRow(eventSQLs[2], status).Scan(&num)
	return num, err
}

// NumEventWithDBAndStatus get amount of event with specific status in specific datababe.
func NumEventWithDBAndStatus(db *sql.DB, database, status string) (uint, error) {
	database = strings.Trim(database, " ")
	var err error
	if database == "" {
		database, err = getDatabaseName(db)
		if err != nil {
			return 0, err
		}
	}
	var num uint
	err = db.QueryRow(eventSQLs[3], database, status).Scan(&num)
	return num, err
}
