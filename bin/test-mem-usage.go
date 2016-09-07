package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"

	"github.com/mozilla-services/go-syncstorage/syncstorage"
	"github.com/mozilla-services/go-syncstorage/web"
)

/* This will open up $arg2 number of files to see memory usage.
   On the server each opened sqlite3 db seems to take about a MB
   of RAM. This seems really high... not sure if it is in the handlers
   or in sqlite3 directly

   Results from testing:

   100  handlers = 65.5MB
   1000 handlers = 605.0 MB
   2000 handlers = 1170 MB

   Give or take a bit, it is about 600KB of RAM per active sync handler

*/

func main() {
	tmpdir := os.Getenv("TMPDIR")

	if _, err := os.Stat(tmpdir); os.IsNotExist(err) {
		log.Fatal("Config Error: could not use " + tmpdir)
	}

	cleanedDir := filepath.Clean(tmpdir)
	testfile := cleanedDir + string(os.PathSeparator) + "test.writable"
	f, err := os.Create(testfile)
	if err != nil {
		log.Fatal("Config Error: tmpdir is not writable")
	} else {
		f.Close()
		os.Remove(testfile)
	}

	if len(os.Args) < 2 {
		log.Fatal("Usage: " + os.Args[0] + " <num dbs>")
	}

	numDBs, err := strconv.ParseInt(os.Args[1], 10, 16)
	if err != nil {
		log.Fatal(err)
	}

	dbList := make([]*web.SyncUserHandler, numDBs, numDBs)
	defaultConfig := web.NewDefaultSyncUserHandlerConfig()

	fmt.Println("Creating %d Handlers", numDBs)
	for i := int64(0); i < numDBs; i++ {
		dbName := fmt.Sprintf("%s/memtest_%d.db", cleanedDir, i)
		_ = dbName

		db, err := syncstorage.NewDB(dbName)
		if err != nil {
			log.Fatal(err)
		}

		dbList[i] = web.NewSyncUserHandler(fmt.Sprintf("u%d", i), db, defaultConfig)
	}

	// wait for a CTL+C signal
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		done <- true
	}()

	// The program will wait here until it gets the
	// expected signal (as indicated by the goroutine
	// above sending a value on `done`) and then exit.
	fmt.Println("awaiting signal")
	<-done
	fmt.Println()
	fmt.Println("Cleaning Up")

	// clean up
	for i, handler := range dbList {
		handler.StopHTTP()
		dbName := fmt.Sprintf("%s/memtest_%d.db", cleanedDir, i)
		os.Remove(dbName)
	}
}
