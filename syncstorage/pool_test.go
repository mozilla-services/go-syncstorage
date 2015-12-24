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
	dir, _ := ioutil.TempDir(os.TempDir(), "pool_test")
	return dir
}

func TestPoolPathAndFile(t *testing.T) {
	assert := assert.New(t)

	T_basepath := getTempBase()
	T_sep := string(os.PathSeparator)

	p, _ := NewPool(T_basepath, TwoLevelPath)

	path, file := p.PathAndFile("uid1234")
	assert.Equal("uid1234.db", file)
	assert.Equal(T_basepath+T_sep+"4"+T_sep+"3", path)
}

func TestPoolBorrowAndReturn(t *testing.T) {

	assert := assert.New(t)

	uid := "abc123"
	p, _ := NewPool(getTempBase(), TwoLevelPath)
	db, err := p.borrowdb(uid)

	assert.NoError(err)
	assert.NotNil(db)

	// we have to return it for the next test
	p.returndb(uid)

	// make sure we get the same value of of the DB
	db2, err := p.borrowdb(uid)
	assert.NoError(err)
	assert.NotNil(db2)
	assert.Equal(db, db2)

	p.returndb(uid)
}

func TestPoolBorrowAllowsOnlyOne(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}

	assert := assert.New(t)

	uid := "abc123"
	p, _ := NewPool(getTempBase(), TwoLevelPath)
	_, err := p.borrowdb(uid)

	if !assert.NoError(err) {
		return
	}

	ch := make(chan bool, 1)

	go func(uid string) {
		p.borrowdb(uid)
		defer p.returndb(uid)
		ch <- true
	}(uid)

	select {
	case <-time.After(100 * time.Millisecond):
		// expect the borrow to wait, timing seems pretty arbitrary
		// but 100ms should be *MORE* than enough for something that
		// should already be in the pool's cache
		return
	case <-ch:
		assert.Fail("Expected lock to prevent a new borrow")
		return
	}

	p.returndb(uid)
	assert.True(<-ch)
}

// make a pool with a very small cache size so it must evict
// *DBs to make room
func TestPoolLRU(t *testing.T) {
	assert := assert.New(t)
	cachesize := 1

	pool, err := NewPoolCacheSize(getTempBase(), TwoLevelPath, cachesize)
	if !assert.NoError(err) {
		return
	}

	users := 5
	name := "custom"
	cIds := make([]int, users)

	for i := 0; i < users; i++ {
		uid := "123456" + strconv.Itoa(i)
		cIds[i], err = pool.CreateCollection(uid, name)
		if !assert.NoError(err) {
			return
		}
	}

	for i := 0; i < users; i++ {
		uid := "123456" + strconv.Itoa(i)
		cId, err := pool.GetCollectionId(uid, name)
		if !assert.NoError(err) {
			return
		}

		assert.Equal(cIds[i], cId)
	}

	assert.Equal(cachesize, pool.cache.Len())
}

// TestPoolParallel uses a very small LRU cache and uses multiple
// goroutines to update users in parallel
func TestPoolParallel(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}

	assert := assert.New(t)
	cachesize := 1

	var wg sync.WaitGroup

	pool, err := NewPoolCacheSize(getTempBase(), TwoLevelPath, cachesize)
	if !assert.NoError(err) {
		return
	}
	users := 5
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
