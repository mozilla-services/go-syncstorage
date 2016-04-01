package syncstorage

import (
	"io/ioutil"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func getTempBase() string {
	tmpdir := "pool_test"
	dir, _ := ioutil.TempDir(os.TempDir(), tmpdir)
	return dir
}

func TestPoolGetDB(t *testing.T) {
	assert := assert.New(t)
	uid0 := "abc123"
	uid1 := "xyz456"

	p, _ := NewPool(getTempBase())

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

	// make sure TTL cleanup works correctly
	p, _ := NewPoolTime(getTempBase(), time.Millisecond*50)
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
	p.Cleanup() // should remove "uid1"

	assert.Equal(1, p.lru.Len())
	assert.Equal(1, len(p.dbs))

	// wait 50ms, t=125ms, uid2 expired at 75ms
	time.Sleep(time.Millisecond * 50)
	p.Cleanup()

	assert.Equal(0, p.lru.Len())
	assert.Equal(0, len(p.dbs))
}

func TestPoolCleanupGoroutine(t *testing.T) {
	assert := assert.New(t)
	// make sure the goroutine cleans things up

	// pool cleans up with 20ms TTL
	p, _ := NewPoolTime(getTempBase(), time.Millisecond*10)
	p.getDB("uid1")
	p.getDB("uid2")
	p.getDB("uid3")

	assert.Equal(3, p.lru.Len())
	assert.Equal(3, len(p.dbs))

	p.Start() // start cleaup goroutine

	// wait enough time for cleanup to run
	time.Sleep(time.Millisecond * 20)
	assert.Equal(0, p.lru.Len())
	assert.Equal(0, len(p.dbs))

	p.Stop()
}

// TestPoolCleanupSkipUsed tests that used items are skipped over for cleanup
func TestPoolCleanupSkipUsed(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	// pool cleans up with 20ms TTL
	p, _ := NewPoolTime(getTempBase(), time.Millisecond*10)
	p.getDB("testused-1")
	db, _ := p.getDB("testused-2")
	p.getDB("testused-3")

	assert.Equal(3, p.lru.Len())
	assert.Equal(3, len(p.dbs))

	// make sure it skipped when used
	db.used(true)
	time.Sleep(time.Millisecond * 20)
	p.Cleanup()
	assert.Equal(1, p.lru.Len())
	assert.Equal(1, len(p.dbs))

	// make sure it cleaned up now
	db.used(false)
	p.Cleanup()
	assert.Equal(0, p.lru.Len())
	assert.Equal(0, len(p.dbs))
}

func TestPoolCleanupStop(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}
	t.Parallel()
	assert := assert.New(t)
	// make sure pool.Stop() stops cleanup

	// pool cleans up with 20ms TTL
	p, _ := NewPoolTime(getTempBase(), time.Millisecond*10)
	p.Start()
	p.getDB("uid1")
	assert.Equal(1, p.lru.Len())
	assert.Equal(1, len(p.dbs))
	// wait enough time for cleanup to run
	time.Sleep(time.Millisecond * 20)
	p.Stop()

	assert.Equal(0, p.lru.Len())
	assert.Equal(0, len(p.dbs))

	// add it again
	p.getDB("uid1")
	assert.Equal(1, p.lru.Len())
	assert.Equal(1, len(p.dbs))
	// wait for cleanup
	time.Sleep(time.Millisecond * 20)

	// should still be there
	assert.Equal(1, p.lru.Len())
	assert.Equal(1, len(p.dbs))
}

func TestPoolPathAndFile(t *testing.T) {
	assert := assert.New(t)

	T_basepath := getTempBase()
	T_sep := string(os.PathSeparator)

	p, _ := NewPool(T_basepath)

	path, file := p.PathAndFile("uid1234")
	assert.Equal("uid1234.db", file)
	assert.Equal(T_basepath+T_sep+"4"+T_sep+"3", path)
}

// TestPoolParallel uses a very small LRU cache and uses multiple
// goroutines to update users in parallel
func TestPoolParallel(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}

	assert := assert.New(t)

	var wg sync.WaitGroup

	ttl := time.Millisecond * 10
	pool, err := NewPoolTime(getTempBase(), ttl)
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
	time.Sleep(ttl)
	pool.Cleanup()
	assert.Equal(0, pool.lru.Len())
	assert.Equal(0, len(pool.dbs))
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
