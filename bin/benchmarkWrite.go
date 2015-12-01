package main

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/codegangsta/cli"
	"github.com/mostlygeek/go-syncstorage/syncstorage"
)

func FillUserDBPut(path string, numBSOs, bsoSize int) error {

	db, err := syncstorage.New(path)
	if err != nil {
		return err
	}
	defer db.Close()

	cId := 1
	payload := syncstorage.String(strings.Repeat("b", bsoSize))
	sortIndex := syncstorage.Int(1)
	TTL := syncstorage.Int(syncstorage.DEFAULT_BSO_TTL)

	for i := 0; i < numBSOs; i++ {
		bId := "b" + strconv.Itoa(i)
		_, err := db.PutBSO(cId, bId, payload, sortIndex, TTL)
		if err != nil {
			return fmt.Errorf("Err on PUT BSO # %d, %s", i, err.Error())
		}
	}

	return nil
}

func FillUserDBPost(path string, numBSOs, bsoSize int) error {
	db, err := syncstorage.New(path)
	if err != nil {
		return err
	}
	defer db.Close()

	cId := 1
	payload := syncstorage.String(strings.Repeat("b", bsoSize))
	sortIndex := syncstorage.Int(1)
	TTL := syncstorage.Int(syncstorage.DEFAULT_BSO_TTL)

	var create syncstorage.PostBSOInput

	for i := 0; i < numBSOs; i++ {
		// only do 100 at a time
		if i == 0 {
			create = syncstorage.PostBSOInput{}
		}

		bId := "b" + strconv.Itoa(i)
		create[bId] = syncstorage.NewPutBSOInput(payload, sortIndex, TTL)

		if (i % 100) == 0 {
			_, err := db.PostBSOs(cId, create)
			if err != nil {
				return fmt.Errorf("Err on POST BSO #%d", i)
			}
		}
	}

	if len(create) > 0 {
		_, err := db.PostBSOs(cId, create)
		if err != nil {
			return fmt.Errorf("Err on POST BSO last %d bsos", len(create))
		}
	}

	return nil
}

// RecordStatistics writes run stats into a sqlite3 table so we can
// do some data analysis over it
func RecordStatistic(method string, users, concurrency, bsos, size, failed int, took time.Duration) error {

	db, err := sql.Open("sqlite3", "./benchmarkWrite_stats.db")
	if err != nil {
		return err
	}
	defer db.Close()

	var name string
	q := "SELECT name from sqlite_master WHERE type='table' AND name='stats'"
	if err := db.QueryRow(q).Scan(&name); err == sql.ErrNoRows {
		create := `CREATE TABLE stats (
			time DATETIME,
			method STRING,
			users NUMBER,
			concurrency NUMBER,
			bsos NUMBER,
			size NUMBER,
			failures NUMBER,
			took NUMBER
		)`

		if _, err := db.Exec(create); err != nil {
			return fmt.Errorf("stats create err: %s", err.Error())
		}
	}

	dml := `INSERT INTO stats (time, method, users, concurrency, bsos, size, failures, took)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`

	// in milliseconds
	tookMS := took.Nanoseconds() / 1000 / 1000

	if _, err := db.Exec(dml, time.Now(), method, users, concurrency, bsos, size, failed, tookMS); err != nil {
		return fmt.Errorf("stats insert err: %s", err.Error())
	}

	return nil

}

// This will benchmark how long it will take to create U number of users
// with R records with C concurrent access
func main() {

	app := cli.NewApp()
	app.Name = "benchmark-write"
	app.Usage = "Benchmark how long it takes to create a number of users with BSOs concurrently"

	app.Flags = []cli.Flag{

		cli.StringFlag{
			Name:  "workdir, w",
			Value: "/tmp",
			Usage: "Where to put temp sqlite files",
		},
		cli.StringFlag{
			Name:  "method, m",
			Value: "PUT",
			Usage: "Use the PUT (one bso/transaction) | POST (multiple bsos/transaction) to create data",
		},
		cli.IntFlag{
			Name:  "users, u",
			Value: 10,
			Usage: "Number of users",
		},
		cli.IntFlag{
			Name:  "bsos, b",
			Value: 1000,
			Usage: "BSOs to create per user",
		},
		cli.IntFlag{
			Name:  "size, s",
			Value: 500,
			Usage: "bytes per bso payload",
		},
		cli.IntFlag{
			Name:  "concurrency, c",
			Value: 1,
			Usage: "Number of users to create in parallel",
		},
	}

	app.Action = func(c *cli.Context) {

		fmt.Println("Note: Stats are written to benchmarkWrite_stats.db in the current directory. Use the sqlite3 cli to inspect the stats table inside.")

		workDir := c.String("workdir")
		numUsers := c.Int("users")
		numBSOs := c.Int("bsos")
		bsoSize := c.Int("size")
		concurrency := c.Int("concurrency")

		var method string
		if c.String("method") == "POST" {
			method = "POST"
		} else {
			method = "PUT"
		}

		// check that workdir is writable
		if stat, err := os.Stat(workDir); err == nil {
			if stat.IsDir() != true {
				fmt.Printf("Error: %s not a directory\n", workDir)
				os.Exit(1)
			}
		} else {
			fmt.Printf("Error: %s does not exist\n", workDir)
			os.Exit(1)
		}

		// Generate User Id's for workers
		userIdChan := make(chan int)
		go func(numUsers int) {
			for i := 0; i < numUsers; i++ {
				userIdChan <- i
			}
			close(userIdChan)
		}(numUsers)

		// Count total errors
		errCount := 0
		errChan := make(chan int)
		go func() {
			for c := range errChan {
				errCount += c
			}
		}()

		start := time.Now()

		// workers ... will pull a
		var wg sync.WaitGroup
		for i := 0; i < concurrency; i++ {
			wg.Add(1)

			go func(workerId int, workDir string, method string, numBSOs, bsoSize int) {
				defer wg.Done()
				for userId := range userIdChan {
					dbFile := fmt.Sprintf("%s/user_%d.db", workDir, userId)

					var err error

					if method == "PUT" {
						fmt.Printf("worker #%d = PUT => %s\n", workerId, dbFile)
						err = FillUserDBPut(dbFile, numBSOs, bsoSize)
					} else {
						fmt.Printf("worker #%d = POST => %s\n", workerId, dbFile)
						err = FillUserDBPost(dbFile, numBSOs, bsoSize)
					}

					if err != nil {
						fmt.Printf("Worker %d Error: %s\n", workerId, err.Error())
						errChan <- 1
					}
				}
			}(i, workDir, method, numBSOs, bsoSize)
		}

		// wait for all writer goroutines to finish
		wg.Wait()
		close(errChan)

		took := time.Now().Sub(start)

		err := RecordStatistic(method, numUsers, concurrency, numBSOs, bsoSize, errCount, took)
		if err != nil {
			fmt.Println("Record stats error: ", err)
		}
	}

	app.Run(os.Args)
}
