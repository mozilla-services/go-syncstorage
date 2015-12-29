package main

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
)

func createDB(filename string) (err error) {

	db, err := sql.Open("sqlite3", filename+"?_busy_timeout=500")
	if err != nil {
		return
	}

	tx, err := db.Begin()
	if err != nil {
		return
	}

	_, err = tx.Exec(`
	CREATE TABLE IF NOT EXISTS testTable (
		id VARCHAR(1),
		data VARCHAR(64)
	);`)

	if err != nil {
		tx.Rollback()
		return
	}

	err = tx.Commit()
	return
}

func startTx(filename string, id string) (tx *sql.Tx, err error) {
	db, err := sql.Open("sqlite3", filename+"?_busy_timeout=500")
	if err != nil {
		return
	}

	tx, err = db.Begin()
	if err != nil {
		return
	}

	_, err = tx.Exec(`INSERT INTO testTable (id, data) VALUES (?,? )`,
		id, strconv.FormatInt(time.Now().Unix(), 16))

	return
}

func main() {

	filename := os.TempDir() + strconv.FormatInt(time.Now().Unix(), 16) + ".db"
	fmt.Println("File: ", filename)
	err := createDB(filename)
	if err != nil {
		fmt.Println(err)
	} else {
		//defer os.Remove(filename)
	}

	step := make(chan struct{})
	next := struct{}{}

	var done sync.WaitGroup

	done.Add(2)

	go func() {
		defer done.Done()
		tx, err := startTx(filename, "1")
		if err != nil {
			panic(err)
		}

		step <- next

		fmt.Println("go#1: block for 1500ms before committing. Blocks go#2...")
		<-time.After(1500 * time.Millisecond)
		fmt.Println("go#1: Committing.")
		tx.Commit()
		<-step
		fmt.Println("go#1: Finished.")
		step <- next
	}()

	//
	go func() {
		defer done.Done()

		fmt.Println("go#2: waiting for go#1 to start...")
		<-step

		// try w/ backoff to create a new transaction
		for _, sleep := range []int{100, 250, 500, 1000, 2000, 4000} {
			tx, err := startTx(filename, "2")

			if err != nil {
				if e, ok := err.(sqlite3.Error); ok {
					if e.Code == sqlite3.ErrBusy {
						fmt.Println("go#2: DB is BUSY, waiting ", sleep, "ms for go#1 to finish")
						<-time.After(time.Duration(sleep) * time.Millisecond)
						continue
					}
				} else {
					panic(fmt.Sprintf("go#2: Got unknown error: %s", err))
				}
			} else {
				fmt.Println("go#2: Committing.")
				tx.Commit()
				break
			}
		}
		step <- next
		<-step
		fmt.Println("go#2: Finished.")
	}()

	done.Wait()
	fmt.Println("Finished OK")
}
