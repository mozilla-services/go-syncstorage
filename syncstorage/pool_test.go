package syncstorage

import (
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func init() {
	// turn off logging
	log.SetLevel(log.FatalLevel)
}

func TestPoolGetDB(t *testing.T) {
	assert := assert.New(t)
	uid0 := "abc123"
	uid1 := "xyz456"

	p, _ := NewPool(":memory:")

	db1, err := p.getDB(uid0)
	if !assert.NoError(err) {
		return
	}
	if !assert.NotNil(db1) {
		return
	}

	db2, err := p.getDB(uid0)
	if !assert.NoError(err) {
		return
	}
	if !assert.NotNil(db2) {
		return
	}

	db3, err := p.getDB(uid1)
	if !assert.NoError(err) {
		return
	}
	if !assert.NotNil(db3) {
		return
	}

	// make sure the cache size is right
	assert.Equal(2, len(p.dbs))
	assert.Equal(2, p.lru.Len())

	// make sure items are in the lru are in
	// the right order
	newest := p.lru.Front()
	assert.Equal(uid1, newest.Value.(*dbelement).uid)

	older := newest.Next()
	assert.Equal(uid0, older.Value.(*dbelement).uid)
}

// TestPoolCleanup makes sure the cleanup goroutine is
// working
func TestPoolCleanup(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}

	assert := assert.New(t)

	// make sure TTL cleanup works correctly by adding
	// the db elements into the purge queue
	p, _ := NewPoolTime(":memory:", time.Millisecond*50)
	_, err := p.getDB("uid1") //t=0, expires @t=50ms
	if !assert.NoError(err) {
		return
	}

	// wait 25ms, add another one, t=25ms
	time.Sleep(time.Millisecond * 25)
	_, err = p.getDB("uid2") // expires @t=75ms
	if !assert.NoError(err) {
		return
	}

	assert.Equal(2, p.lru.Len())
	assert.Equal(2, len(p.dbs))

	// wait 40ms, t=75ms
	time.Sleep(time.Millisecond * 40)
	p.cleanup() // should remove "uid1"

	assert.Equal(1, len(p.purgeCh))

	// wait 50ms, t=125ms, uid2 expired at 75ms
	time.Sleep(time.Millisecond * 50)
	p.cleanup()

	// why 3? purgeCh contains: uid1, uid1, uid2
	// because cleanup() doesn't know what's in the queue
	// and will the same elements again
	assert.Equal(3, len(p.purgeCh))
}

func TestPoolCleanupGoroutine(t *testing.T) {
	assert := assert.New(t)
	// make sure the goroutine cleans things up

	// pool cleans up with 20ms TTL
	ttl := time.Millisecond * 5
	p, _ := NewPoolTime(":memory:", ttl)
	p.getDB("uid1")
	p.getDB("uid2")
	p.getDB("uid3")

	assert.Equal(3, p.lru.Len())
	assert.Equal(3, len(p.dbs))

	p.Start() // start cleaup goroutine

	// wait enough time for cleanup to run
	time.Sleep(ttl * 3)
	assert.Equal(0, p.lru.Len())
	assert.Equal(0, len(p.dbs))

	p.Stop()
}

// TestPoolCleanupSkipUsed tests that used items are skipped over for cleanup
func TestPoolCleanupSkipUsed(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	// pool cleans up with 20ms TTL
	p, _ := NewPoolTime(":memory:", time.Millisecond*10)
	p.getDB("testused-1")
	db, _ := p.getDB("testused-2")
	p.getDB("testused-3")

	assert.Equal(3, p.lru.Len())
	assert.Equal(3, len(p.dbs))

	// Note:  this code is a little hairy
	//
	// the logic for this got a little funky since the
	// non-blocking purge expired BSO code got added as
	// a goroutine. To keep the initial purpose of this test
	// it's easier to look at how many things have been queued
	// for purging

	// make sure it skipped when used
	db.Use()
	time.Sleep(time.Millisecond * 20)

	p.cleanup()
	assert.Equal(2, len(p.purgeCh))
	// in purgeCh are testused-1, and testused-3

	// make sure it cleaned up now
	db.Release()
	p.cleanup()

	assert.Equal(5, len(p.purgeCh))

	// in the purge channel is:
	//   testused-1
	//   testused-3
	//   testused-1
	//   testused-2
	//   testused-3
	// there are duplicates since they get put in there
	// without synchronization
}

func TestPoolCleanupStop(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}
	t.Parallel()
	assert := assert.New(t)

	p, _ := NewPool(":memory:")
	p.Start()
	assert.NotNil(p.stopCh)
	assert.NotNil(p.purgeCh)

	p.Stop()
	assert.Nil(p.stopCh)
	assert.Nil(p.purgeCh)
}

func TestPoolPathAndFile(t *testing.T) {
	assert := assert.New(t)

	tmpdir := "pool_test_path_and_file"
	T_basepath, _ := ioutil.TempDir(os.TempDir(), tmpdir)
	T_sep := string(os.PathSeparator)

	p, _ := NewPool(T_basepath)

	path, file := p.PathAndFile("uid1234")
	assert.Equal("uid1234.db", file)
	assert.Equal(T_basepath+T_sep+"43"+T_sep+"21", path)
}

// TestPoolParallel uses a very small LRU cache and uses multiple
// goroutines to update users in parallel
func TestPoolParallel(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}

	assert := assert.New(t)

	var wg sync.WaitGroup

	ttl := time.Millisecond * 5

	// need to use actual files rather than :memory:
	// since the pool will open/close connections
	tmpdir := "pool_test_parallel"
	basepath, _ := ioutil.TempDir(os.TempDir(), tmpdir)
	pool, err := NewPoolTime(basepath, ttl)

	if !assert.NoError(err) {
		return
	}

	users := 20
	for u := 0; u < users; u++ {
		uid := strconv.Itoa(u)
		_, err := pool.CreateCollection(uid, "test")
		if !assert.NoError(err) {
			return
		}
	}

	for u := 0; u < users; u++ {
		wg.Add(1)
		go func(uid string) {
			defer wg.Done()

			cId, _ := pool.GetCollectionId(uid, "test")
			modified := Now()
			for i := 0; i < 5; i++ {
				err := pool.TouchCollection(uid, cId, modified)
				time.Sleep(1 * time.Millisecond)
				if !assert.NoError(err) {
					return
				}
			}
		}(strconv.Itoa(u))
	}
	wg.Wait()
	assert.Equal(users, pool.lru.Len())

	// start / wait and stop the goroutines that handle the cleanup
	// this isn't the most precise way of doing things
	pool.Start()
	defer pool.Stop()

	time.Sleep(ttl * 10)

	assert.Equal(0, pool.lru.Len())
	assert.Equal(0, len(pool.dbs))
}

func BenchmarkTwoLevelPath(b *testing.B) {
	for i := 0; i < b.N; i++ {
		TwoLevelPath("10000123456")
	}
}

func TestPoolPurge(t *testing.T) {
	assert := assert.New(t)
	uid := "abc123"

	p, _ := NewPool(":memory:")

	payload := String(strings.Repeat("x", 1024))
	bsos := 100
	for i := 0; i < bsos; i++ {
		bId := "bso" + strconv.Itoa(i)
		_, err := p.PutBSO(uid, 1, bId, payload, nil, Int(0))

		if !assert.NoError(err) {
			return
		}
	}

	time.Sleep(2 * time.Millisecond)

	el, err := p.getDB(uid)
	if !assert.NoError(err) {
		return
	}

	results, err := el.purge()
	if !assert.NoError(err) {
		return
	}

	assert.Equal(uid, results.uid)
	assert.Equal(bsos, results.numPurged)

	// page stats
	assert.True(results.total > 0)
	assert.True(results.free > 0)
	assert.True(results.bytes > 0)
}

// Use poolwrap to test that the abstracted interface for SyncApi
// works all the way through
func TestPoolSyncApiGetCollectionId(t *testing.T) {
	t.Parallel()
	testApiGetCollectionId(newPoolwrap(), t)
}
func TestPoolSyncApiGetCollectionModified(t *testing.T) {
	t.Parallel()
	testApiGetCollectionModified(newPoolwrap(), t)
}
func TestPoolSyncApiCreateCollection(t *testing.T) {
	t.Parallel()
	testApiCreateCollection(newPoolwrap(), t)
}

func TestPoolSyncApiDeleteCollection(t *testing.T) {
	t.Parallel()
	testApiDeleteCollection(newPoolwrap(), t)
}

func TestPoolSyncApiTouchCollection(t *testing.T) {
	t.Parallel()
	testApiTouchCollection(newPoolwrap(), t)
}

func TestPoolSyncApiInfoCollections(t *testing.T) {
	t.Parallel()
	testApiInfoCollections(newPoolwrap(), t)
}

func TestPoolSyncApiInfoCollectionUsage(t *testing.T) {
	t.Parallel()
	testApiInfoCollectionUsage(newPoolwrap(), t)
}

func TestPoolSyncApiInfoCollectionCounts(t *testing.T) {
	t.Parallel()
	testApiInfoCollectionCounts(newPoolwrap(), t)
}

func TestPoolSyncApiPublicPostBSOs(t *testing.T) {
	t.Parallel()
	testApiPostBSOs(newPoolwrap(), t)
}

func TestPoolSyncApiPublicPutBSO(t *testing.T) {
	t.Parallel()
	testApiPutBSO(newPoolwrap(), t)
}

func TestPoolSyncApiPublicGetBSO(t *testing.T) {
	t.Parallel()
	testApiGetBSO(newPoolwrap(), t)
}

func TestPoolSyncApiPublicGetBSOs(t *testing.T) {
	t.Parallel()
	testApiGetBSOs(newPoolwrap(), t)
}

func TestPoolSyncApiPublicGetBSOModified(t *testing.T) {
	t.Parallel()
	testApiGetBSOModified(newPoolwrap(), t)
}

func TestPoolSyncApiDeleteBSO(t *testing.T) {
	t.Parallel()
	testApiDeleteBSO(newPoolwrap(), t)
}
func TestPoolSyncApiDeleteBSOs(t *testing.T) {
	t.Parallel()
	testApiDeleteBSOs(newPoolwrap(), t)
}

func TestPoolSyncApiPurgeExpired(t *testing.T) {
	t.Parallel()
	testApiPurgeExpired(newPoolwrap(), t)
}

func TestPoolSyncApiUsageStats(t *testing.T) {
	t.Parallel()
	testApiUsageStats(newPoolwrap(), t)
}

func TestPoolSyncApiOptimize(t *testing.T) {
	t.Parallel()
	testApiOptimize(newPoolwrap(), t)
}
func TestPoolSyncApiDeleteEverything(t *testing.T) {
	t.Parallel()
	testApiDeleteEverything(newPoolwrap(), t)
}
