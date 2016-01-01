package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/codegangsta/cli"
	. "github.com/mostlygeek/go-debug"
	"github.com/mostlygeek/go-syncstorage/syncstorage"
)

var (
	debug = Debug("bench:dispatch")
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func RandStringBytesRmndr(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}

type task struct {
	i   int
	uid string
}

func main() {

	app := cli.NewApp()
	app.Name = "benchmark-dispatch"
	app.Usage = "Benchmark how dispatch works under various scenarios"

	app.Flags = []cli.Flag{

		cli.StringFlag{
			Name:  "basedir, b",
			Value: os.TempDir() + "dispatch_benchmark",
			Usage: "Where to put temp sqlite files",
		},
		cli.StringFlag{
			Name:  "statsfile, t",
			Value: "./dispatch-benchmark-stats.db",
			Usage: "sqlite3 database to write stats to",
		},
		cli.IntFlag{
			Name:  "users, u",
			Value: 100,
			Usage: "Unique number of users",
		},

		cli.IntFlag{
			Name:  "writers, w",
			Value: 1,
			Usage: "How many goroutines to use dispatch",
		},

		cli.IntFlag{
			Name:  "requests, r",
			Value: 100,
			Usage: "number of PUT requests to generate",
		},

		cli.IntFlag{
			Name:  "pools, p",
			Value: 4,
			Usage: "Number of pools in the dispatcher",
		},

		cli.IntFlag{
			Name:  "cachesize, c",
			Value: 32,
			Usage: "Number of open sqlite files per pool",
		},
	}

	app.Action = func(c *cli.Context) {

		basedir := c.String("basedir")
		users := c.Int("users")
		writers := c.Int("writers")
		requests := c.Int("requests")
		pools := c.Int("pools")
		cachesize := c.Int("cachesize")

		fmt.Println("Using basedir: ", basedir)

		var rLimit syscall.Rlimit
		err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
		if err != nil {
			fmt.Println("Error Getting Rlimit ", err)
		}
		fmt.Println("Open File limit: ", rLimit.Cur)
		if uint64(pools*cachesize) > rLimit.Cur {
			fmt.Println("WARNING! pools*cachesize > file limit")
		} else {
			fmt.Println("Total file handler cache size: ", pools*cachesize)
		}

		var wg sync.WaitGroup
		todoCh := make(chan *task)

		// when a task is complete a message is sent to this channel
		compCh := make(chan *task)
		done := make(chan struct{})

		// generate uids
		go func() {
			uids := make([]string, users)
			for u := 0; u < len(uids); u++ {
				uids[u] = strconv.Itoa(100000000 + u)
			}

			numUids := len(uids)
			for i := 0; i < requests; i++ {
				index := rand.Intn(numUids)
				todoCh <- &task{i: i, uid: uids[index]}
			}
		}()

		// record stats and stop everything when done
		go func() {
			count := 0

			var start time.Time
			var jobId string

			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-compCh:
					count += 1
					if count == 1 {
						// initialize things
						start = time.Now()
						jobId = start.Format("jan-02-2006-03:04:05.000")
						InitStats(c.String("statsfile"), jobId, users, writers, requests, pools, cachesize)
					}
					if count == requests {

						if err := UpdateStats(c.String("statsfile"), jobId, count, start); err != nil {
							debug("ERROR: %v", err)
						} else {
							debug("hmm")
						}
						close(done) // close this to trigger all workers to stop
						return
					}
				case <-ticker.C:
					// update stats
					if err := UpdateStats(c.String("statsfile"), jobId, count, start); err != nil {
						debug("ERROR: %v", err)
					}
				}
			}
		}()

		dispatch, err := syncstorage.NewDispatch(uint16(pools), basedir, syncstorage.TwoLevelPath, cachesize)
		if err != nil {
			panic(err)
		}

		start := time.Now()
		for i := 0; i < writers; i++ {
			wg.Add(1)
			go func(workerId int) {
				defer wg.Done()

				for {
					select {
					case task := <-todoCh:
						debug("%06d. %d => %s", task.i, workerId, task.uid)

						bId := RandStringBytesRmndr(8)
						payload := RandStringBytesRmndr(200 + rand.Intn(300))
						_, err := dispatch.PutBSO(task.uid, 1, bId, &payload, nil, nil)
						debug("%06d. %d - done", task.i, workerId)

						if err != nil {
							fmt.Printf("ERROR in worker %d for %s: %v\n", workerId, task.uid, err)
							panic(":(")
						}

						compCh <- task

						break
					case <-done:
						return
					}
				}
			}(i)
		}

		wg.Wait()
		took := time.Now().Sub(start)
		fmt.Printf("PUT %d in %v\n", requests, took)
	}

	app.Run(os.Args)
}

// RecordStatistics writes run stats into a sqlite3 table so we can
// do some data analysis over it
func InitStats(
	statsfile,
	id string,
	users,
	writers,
	requests,
	pools,
	cachesize int) error {

	db, err := sql.Open("sqlite3", statsfile)
	if err != nil {
		return err
	}
	defer db.Close()

	create := `CREATE TABLE IF NOT EXISTS stats (
		Id VARCHAR,

		users NUMBER,
		writers NUMBER,

		pools NUMBER,
		cachesize NUMBER,

		requests NUMBER,

		-- records progress for very long running jobs
		completed NUMBER,

		-- total time taken, updated as completed is updated
		took NUMBER,

		PRIMARY KEY(Id)
	)`

	if _, err := db.Exec(create); err != nil {
		return fmt.Errorf("stats create err: %s", err.Error())
	}

	dml := `INSERT INTO stats
			(id, users, writers, requests, pools, cachesize)
			VALUES (?, ?, ?, ?, ?, ?)`

	if _, err := db.Exec(dml, id, users, writers, requests, pools, cachesize); err != nil {
		return fmt.Errorf("stats insert err: %s", err.Error())
	}

	return nil
}

func UpdateStats(
	statsfile,
	id string,
	completed int,
	start time.Time) error {
	db, err := sql.Open("sqlite3", statsfile)
	if err != nil {
		return err
	}
	defer db.Close()

	took := time.Now().Sub(start).Nanoseconds() / 1000 / 1000
	dml := "UPDATE stats SET completed=?, took=? WHERE Id=?"
	if _, err := db.Exec(dml, completed, took, id); err != nil {
		return fmt.Errorf("stats update err: %s", err.Error())
	}

	return nil
}
