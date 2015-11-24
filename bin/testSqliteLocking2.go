package main

/*
 this tests if the sync.RWMutex on helps with a single DB writer and
 multiple go-routines does not error out when writing to the same
 user's db

 results: yes, with a RWMutex on syncstorage.DB multiple go-routines
 can write to it without any problems
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

	userId := RandStringBytesRmndr(12)

	db, err := syncstorage.NewDB("./tmp/"+userId+".db", userId)
	defer db.Close()
	if err != nil {
		fmt.Println("db open error", userId, err)
		return
	}

	start := time.Now()
	goRoutines := 20
	perGoRoutine := 100
	for i := 0; i < goRoutines; i++ { // num go-routines
		wg.Add(1)
		go func(db *syncstorage.DB) {
			defer wg.Done()

			for w := 0; w < perGoRoutine; w++ {
				putErr := db.PutObject(1, &syncstorage.BSO{
					Id:      RandStringBytesRmndr(12),
					Payload: "boom.",
				})

				if putErr != nil {
					fmt.Println("put error", putErr)
					return
				}
			}
		}(db)
	}
	wg.Wait()

	took := time.Now().Sub(start)
	total := goRoutines * perGoRoutine
	per := float64(total) / took.Seconds()
	fmt.Println("Done, took: ", took, "for", total, "records, ", "about ~", per, "second")

}
