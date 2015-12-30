package storage

import (
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func randUID() string {
	b := make([]byte, 16)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}

func TestSubdir(t *testing.T) {
	assert := assert.New(t)
	assert.Equal("", Subdir("9"))
	assert.Equal("98", Subdir("89"))
	assert.Equal("98", Subdir("789"))
	assert.Equal("98/76", Subdir("6789"))
	assert.Equal("98/76", Subdir("56789"))
	assert.Equal("98/76", Subdir("456789"))
}

func TestAbsdirPath(t *testing.T) {
	// don't want to muck up other testing
	defer SetStorageBase(os.TempDir())

	SetStorageBase("/tmp")
	assert.Equal(t, "/tmp/ol/le", AbsdirPath("hello"))

	SetStorageBase("/tmp/")
	assert.Equal(t, "/tmp/ol/le", AbsdirPath("hello"))

	tmpdir := os.TempDir()
	SetStorageBase(tmpdir)
	assert.Equal(t, tmpdir+"ol/le", AbsdirPath("hello"))
}

func TestMakeSubdir(t *testing.T) {
	SetStorageBase(os.TempDir())
	assert := assert.New(t)
	testUID := randUID()

	// remove the temp directory  to test correctly
	dir := AbsdirPath(testUID)
	assert.NoError(os.RemoveAll(dir))

	made, err := MakeSubdir(testUID)
	assert.True(made)
	assert.NoError(err)

	made, err = MakeSubdir(testUID)
	assert.False(made)
	assert.NoError(err)
}

func TestGetDB(t *testing.T) {
	assert := assert.New(t)
	testUID := randUID()

	db, err := getDB(testUID)

	assert.NotNil(db)
	assert.NoError(err)

	// Check that tables init correctly

	check := "SELECT name from sqlite_master WHERE type='table' AND name=?"
	var name string

	err = db.QueryRow(check, "BSO").Scan(&name)
	if assert.NoError(err) {
		assert.Equal("BSO", name)
	}

	err = db.QueryRow(check, "Collections").Scan(&name)
	if assert.NoError(err) {
		assert.Equal("Collections", name)
	}
}

func TestDBLockingWorks(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}

	assert := assert.New(t)

	next := struct{}{}
	step := make(chan struct{})
	uid := "123458" // randUID()

	os.Remove(AbsDBPath(uid))

	var wg sync.WaitGroup

	// use 2 goroutines. go#1 will start a new
	// trasaction, which would lock the db internally.
	// go#2 will wait for its transaction to complete
	wg.Add(2)
	go func(uid string) {
		defer wg.Done()

		_, err := getDB(uid)
		if !assert.NoError(err) {
			return
		}

		//tx, err := beginRetry(db)
		//if !assert.NoError(err) {
		//	return
		//}
		// tell other go routine to try to get
		// a transaction

		step <- next
		time.Sleep(300 * time.Millisecond)

		//tx.Rollback()
		//db.Close()
	}(uid)

	go func(uid string) {
		defer wg.Done()

		getDB(uid)
		//if !assert.NoError(err) {
		//	return
		//}
		<-step

		return

		// wait for goroutine #1 to lock the db
		<-step

		//tx, err := beginRetry(db)

		//if !assert.NoError(err) {
		//	return
		//}
		//tx.Rollback()
	}(uid)

	wg.Wait()
}
