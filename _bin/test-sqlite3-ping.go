package main

import (
	"database/sql"
	"log"

	_ "github.com/mattn/go-sqlite3"
)

func main() {

	db, err := sql.Open("sqlite3", "/tmp/mydb.db")

	if err != nil {
		log.Fatal(err)
	}

	err = db.Close()
	if err != nil {
		log.Fatal("close ", err)
	}

	err = db.Ping() // this does not reopen the db file
	if err != nil {
		log.Fatal("ping ", err)
	}

}
