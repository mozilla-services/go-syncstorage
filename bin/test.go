package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/mostlygeek/go-syncstorage/syncstorage"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()_ "

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

func main() {

	db, err := syncstorage.NewDB("/tmp/mydb.db", "benson")
	defer db.Close()

	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}

	// insert 1000 objects
	bookmarkId, err := db.GetCollection("bookmarks")

	objects := 1000

	fmt.Println("Putting 20K BSOs @ 500 bytes")
	start := time.Now()
	for i := 0; i < objects; i++ {
		if err != nil {
			fmt.Printf("Error no bookmarks collection: %v\n", err)
			os.Exit(1)
		}

		putErr := db.PutObject(bookmarkId, &syncstorage.BSO{
			Id:      RandStringBytesRmndr(12),
			Payload: RandStringBytesRmndr(500),
		})

		if putErr != nil {
			fmt.Printf("put error :( %v", putErr)
			os.Exit(1)
		}

		if i%100 == 0 {
			fmt.Print(".")
		}
	}
	took := time.Now().Sub(start)
	fmt.Printf("\n%d INSERTS Took: %v, ~%f/second \n", objects, took, (float64(objects) / took.Seconds()))

}
