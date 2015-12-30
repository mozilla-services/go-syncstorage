package storage

import "database/sql"

type DB struct {
	uid string
	db  *sql.DB
}
