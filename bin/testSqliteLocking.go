package main

/*
 having multiple go routines open up the same file and them
 trying to write to it at the same time will result in
 "Database locked" errors from sqlite.

 That is normal however it slows down the whole system
 as something(?) blocks before it errors. It's much better
 use a single syncserver.DB and a RWMutex lock.

 see: testSqliteLocking2.go
*/

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/mostlygeek/go-syncstorage/syncstorage"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func RandStringBytesRmndr(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

// test multiple writers / readers
func main() {

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(userId string) {
			db, err := syncstorage.NewDB("./tmp/"+userId+".db", userId)
			defer db.Close()
			defer wg.Done()

			if err != nil {
				fmt.Println("db open error", userId, err)
				return
			}

			putErr := db.PutObject(1, &syncstorage.BSO{
				Id:      RandStringBytesRmndr(12),
				Payload: "boom.",
			})

			if putErr != nil {
				fmt.Println("put error", userId, putErr)
				return
			}
		}("benson")
	}

	wg.Wait()
	fmt.Println("Done")

}
