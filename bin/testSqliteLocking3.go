package main

/*
 This tests out if there are any errors with multiple reader/writers on the database.
 It also help choose the right locking mutex for syncstorage.DB.

 There should be no errors with the multiple readers/writers modifying the database
 as only a single reader or writer can be operating on it a time.
*/

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/mostlygeek/go-syncstorage/syncstorage"
)

const OUTPUT_ON = true

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

func WriterPrint(s string) {
	if OUTPUT_ON {
		fmt.Printf("\033[%dm%s\033[0m", 31, s)
	}
}

func ReaderPrint(s string) {
	if OUTPUT_ON {
		fmt.Printf("\033[%dm%s\033[0m", 33, s)
	}

}

func RunTest(dbFile, readers, writers string, numPutsPerWriter int) {

	var writerWG sync.WaitGroup
	done := make(chan bool)

	db, err := syncstorage.NewDB(dbFile, "none")
	defer db.Close()

	if err != nil {
		fmt.Println("db open error", err)
		return
	}

	// test multiple database files

	// run a bunch of readers against the database
	var readerWG sync.WaitGroup
	for _, id := range strings.Split(readers, "") {
		readerWG.Add(1)
		go func(id string) {
			defer readerWG.Done()

			for {
				select {
				case <-time.After(time.Duration(rand.Intn(50)) * time.Millisecond):
					_, _, err := db.StorageUsed()

					if err != nil {
						fmt.Println("storage used err: ", err)
						return
					} else {
						ReaderPrint(id)
					}

				case <-done:
					return
				}

			}
		}(id)
	}

	// run a bunch of writers against the database
	for _, id := range strings.Split(writers, "") {
		writerWG.Add(1)

		go func(workerId string) {
			defer writerWG.Done()

			for j := 0; j < numPutsPerWriter; j++ {
				putErr := db.PutObject(1, &syncstorage.BSO{
					Id:      RandStringBytesRmndr(12),
					Payload: RandStringBytesRmndr(100 + rand.Intn(200)),
				})

				if putErr != nil {
					fmt.Println("put error", putErr)
					return
				}

				WriterPrint(workerId)
				time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
			}

		}(id)
	}

	writerWG.Wait()

	// stop all writers
	close(done)

	readerWG.Wait()
}

// test multiple writers / readers
func main() {

	WriterPrint("DB Writers are RED\n")
	ReaderPrint("DB Readers are YELLOW\n")

	var testWG sync.WaitGroup

	concurrent := 200

	fmt.Printf("Running %d concurrent tests\n\n", concurrent)

	for t := 0; t < concurrent; t++ {
		testWG.Add(1)
		go func(t int) {
			defer testWG.Done()
			file := fmt.Sprintf("./db_work/testSqliteLocking3-%03d", t)
			RunTest(file, "abcdefghij", "0123456789", 100)
		}(t)
	}

	testWG.Wait()

	fmt.Println()

	//count, storage, _ := db.StorageUsed()
	//fmt.Printf("\n\nDone. %d BSOs using %d bytes\n", count, storage)

}
