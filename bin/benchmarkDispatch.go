package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/mostlygeek/go-syncstorage/syncstorage"
	. "github.com/tj/go-debug"
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

type work struct {
	i   int
	uid string
}

func main() {

	tmpdir := os.TempDir() + "dispatch_test"
	fmt.Println("Using basedir: ", tmpdir)

	var wg sync.WaitGroup

	uidCh := make(chan *work, 16)
	done := make(chan struct{})

	numRecordsToWrite := 2500

	// generate uids
	go func() {

		// generate a list of 250K users
		uids := make([]string, 250000)
		for u := 0; u < len(uids); u++ {
			uids[u] = strconv.Itoa(100000000 + u)
		}

		numUids := len(uids)
		for i := 0; i < numRecordsToWrite; i++ {
			index := rand.Intn(numUids)
			uidCh <- &work{i: i, uid: uids[index]}
		}

		close(done)
	}()

	dispatch, err := syncstorage.NewDispatch(4, tmpdir, syncstorage.TwoLevelPath, 32)
	if err != nil {
		panic(err)
	}

	var rLimit syscall.Rlimit
	err = syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		fmt.Println("Error Getting Rlimit ", err)
	}
	fmt.Println("Open File limit: ", rLimit.Cur)

	start := time.Now()
	for i := 0; i < 1; i++ {
		wg.Add(1)
		go func(workerId int) {
			defer wg.Done()

			for {
				select {
				case work := <-uidCh:
					debug("%06d. %d => %s", work.i, workerId, work.uid)
					bId := RandStringBytesRmndr(8)
					payload := RandStringBytesRmndr(200 + rand.Intn(300))
					_, err := dispatch.PutBSO(work.uid, 1, bId, &payload, nil, nil)
					if err != nil {
						fmt.Printf("ERROR in worker %d for %s: %v\n", workerId, work.uid, err)
						panic(":(")
					}
					break
				case <-done:
					return
				}
			}
		}(i)
	}

	wg.Wait()
	fmt.Printf("PUT %d in %v\n", numRecordsToWrite, time.Now().Sub(start))
}
