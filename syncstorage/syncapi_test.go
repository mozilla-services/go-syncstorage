package syncstorage

import (
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func testApiGetCollectionId(db SyncApi, t *testing.T) {
	_, err := db.GetCollectionId("bookmarks")
	assert.NoError(t, err)
}

func testApiGetCollectionModified(db SyncApi, t *testing.T) {
	assert := assert.New(t)

	name := "test"
	cId, err := db.CreateCollection(name)
	if !assert.NoError(err) {
		return
	}

	cols, err := db.InfoCollections()
	if !assert.NoError(err) {
		return
	}

	if !assert.NotNil(cols[name]) {
		return
	}

	modified, err := db.GetCollectionModified(cId)
	if !assert.NoError(err) {
		return
	}

	assert.Equal(cols[name], modified)
}

func testApiCreateCollection(db SyncApi, t *testing.T) {
	assert := assert.New(t)
	cName := "NewCollection"
	cId, err := db.CreateCollection(cName)
	if assert.Nil(err) {
		assert.NotEqual(0, cId)
		assertId, err := db.GetCollectionId(cName)
		if assert.Nil(err) {
			assert.Equal(assertId, cId)
		}
	}
}

func testApiTouchCollection(db SyncApi, t *testing.T) {
	assert := assert.New(t)

	name := "test"
	cId, err := db.CreateCollection(name)
	if !assert.NoError(err) {
		return
	}

	modified := Now()
	assert.NoError(db.TouchCollection(cId, modified))
}

func testApiDeleteCollection(db SyncApi, t *testing.T) {
	assert := assert.New(t)
	cName := "NewConnection"
	cId, err := db.CreateCollection(cName)
	if assert.Nil(err) {
		err = db.DeleteCollection(cId)

		// make sure it was deleted
		if assert.Nil(err) {
			id, err := db.GetCollectionId(cName)
			assert.Equal(0, id)
			assert.Equal(ErrNotFound, err)
		}
	}
}

func testApiInfoCollections(db SyncApi, t *testing.T) {
	assert := assert.New(t)

	id, err := db.GetCollectionId("bookmarks")
	assert.NoError(err)

	modified := Now()
	assert.NoError(db.TouchCollection(id, modified))

	results, err := db.InfoCollections()
	assert.NoError(err)

	keys := make([]string, len(results))
	i := 0
	for k := range results {
		keys[i] = k
		i++
	}

	assert.Contains(keys, "bookmarks")
	assert.Equal(modified, results["bookmarks"])
}

func testApiInfoCollectionUsage(db SyncApi, t *testing.T) {
	assert := assert.New(t)

	// put in 100 records into bookmarks, history and prefs collections
	expected := make(map[string]int)
	for _, name := range []string{"bookmarks", "history", "prefs"} {
		cId, err := db.GetCollectionId(name)

		if assert.NoError(err) {
			for i := 0; i < 100; i++ {
				numRandBytes := 50 + rand.Intn(100)
				payload := String(randData(numRandBytes))
				_, err := db.PutBSO(cId, "b"+strconv.Itoa(i), payload, nil, nil)

				if !assert.NoError(err) {
					t.Fatal()
				}

				// keep a count of amount of random data we created per collection
				expected[name] += numRandBytes
			}
		}
	}

	results, err := db.InfoCollectionUsage()
	assert.NoError(err)
	if assert.NotNil(results) {
		keys := make([]string, len(results))
		i := 0
		for k := range results {
			keys[i] = k
			i++
		}

		assert.Contains(keys, "bookmarks")
		assert.Contains(keys, "history")
		assert.Contains(keys, "prefs")

		assert.Equal(results["bookmarks"], expected["bookmarks"])
		assert.Equal(results["history"], expected["history"])
		assert.Equal(results["prefs"], expected["prefs"])
	}
}

func testApiInfoCollectionCounts(db SyncApi, t *testing.T) {
	assert := assert.New(t)

	// put in random num of records into bookmarks, history and prefs collections
	expected := make(map[string]int)
	for _, name := range []string{"bookmarks", "history", "prefs"} {
		cId, err := db.GetCollectionId(name)

		if assert.NoError(err) {
			numRecords := 5 + rand.Intn(99)
			expected[name] = numRecords
			for i := 0; i < numRecords; i++ {
				_, err := db.PutBSO(cId, "b"+strconv.Itoa(i), String("x"), nil, nil)
				if !assert.NoError(err) {
					t.Fatal()
				}
			}
		}
	}

	results, err := db.InfoCollectionCounts()
	assert.NoError(err)
	if assert.NotNil(results) {
		keys := make([]string, len(results))
		i := 0
		for k := range results {
			keys[i] = k
			i++
		}

		assert.Contains(keys, "bookmarks")
		assert.Contains(keys, "history")
		assert.Contains(keys, "prefs")

		assert.Equal(results["bookmarks"], expected["bookmarks"])
		assert.Equal(results["history"], expected["history"])
		assert.Equal(results["prefs"], expected["prefs"])
	}
}

func testApiPutBSO(db SyncApi, t *testing.T) {
	assert := assert.New(t)

	cId := 1
	bId := "b0"

	// test an INSERT
	modified, err := db.PutBSO(cId, bId, String("foo"), Int(1), Int(DEFAULT_BSO_TTL))
	assert.NoError(err)
	assert.NotZero(modified)

	cModified, err := db.GetCollectionModified(cId)
	assert.NoError(err)
	assert.Equal(modified, cModified)

	bso, err := db.GetBSO(cId, bId)
	assert.NoError(err)
	assert.NotNil(bso)
	assert.Equal("foo", bso.Payload)
	assert.Equal(1, bso.SortIndex)

	// sleep a bit so we have a least a 100th of a millisecond difference
	// between the operations
	time.Sleep(19 * time.Millisecond)

	// test the UPDATE
	modified2, err := db.PutBSO(cId, bId, String("bar"), Int(2), Int(DEFAULT_BSO_TTL))
	assert.NoError(err)
	assert.NotZero(modified2)
	assert.NotEqual(modified2, modified)

	cModified, err = db.GetCollectionModified(cId)
	assert.NoError(err)
	assert.Equal(modified2, cModified)

	bso2, err := db.GetBSO(cId, bId)
	assert.NoError(err)
	assert.NotNil(bso2)
	assert.Equal("bar", bso2.Payload)
	assert.Equal(2, bso2.SortIndex)
}

func testApiPostBSOs(db SyncApi, t *testing.T) {
	assert := assert.New(t)

	cId := 1

	create := PostBSOInput{
		NewPutBSOInput("b0", String("payload 0"), Int(10), nil),
		NewPutBSOInput("b1", String("payload 1"), Int(-1), nil),
		NewPutBSOInput("b2", String("payload 2"), Int(100), nil),
	}

	results, err := db.PostBSOs(cId, create)
	assert.NoError(err)

	// test successes and failures are what we expect
	assert.NotNil(results)
	assert.Contains(results.Success, "b0")
	assert.Contains(results.Success, "b2")
	assert.NotContains(results.Success, "b1")
	assert.NotNil(results.Failed["b1"])
	assert.Len(results.Failed["b1"], 1)

	// make sure modified timestamps are correct
	cModified, err := db.GetCollectionModified(cId)
	assert.NoError(err)
	assert.Equal(results.Modified, cModified)

	updates := PostBSOInput{
		NewPutBSOInput("b0", String("updated 0"), Int(11), Int(100000)),
		NewPutBSOInput("b2", String("updated 2"), Int(22), Int(10000)),
	}

	results2, err := db.PostBSOs(cId, updates)
	assert.NoError(err)
	assert.NotNil(results2)
	assert.Len(results2.Success, 2)
	assert.Len(results2.Failed, 0)

	assert.Contains(results.Success, "b0")
	assert.Contains(results.Success, "b2")

	bso0, err := db.GetBSO(cId, "b0")
	assert.NoError(err)
	assert.NotNil(bso0)
	assert.Equal(11, bso0.SortIndex)
	assert.Equal("updated 0", bso0.Payload)

	bso2, err := db.GetBSO(cId, "b2")
	assert.NoError(err)
	assert.NotNil(bso2)
	assert.Equal(22, bso2.SortIndex)
	assert.Equal("updated 2", bso2.Payload)

	cModified, err = db.GetCollectionModified(cId)
	assert.NoError(err)
	assert.Equal(results2.Modified, cModified)
}

func testApiGetBSO(db SyncApi, t *testing.T) {
	assert := assert.New(t)

	cId := 1
	bId := "b0"
	payload := "a"

	_, err := db.PutBSO(cId, bId, String(payload), nil, nil)
	if !assert.NoError(err) {
		return
	}

	// Make sure it returns the right BSO
	bso, err := db.GetBSO(cId, "b0")
	assert.NoError(err)
	if assert.NotNil(bso) {
		assert.Equal(bId, bso.Id)
		assert.Equal(payload, bso.Payload)
	}

	bso, err = db.GetBSO(cId, "nope")
	assert.Exactly(ErrNotFound, err)
	assert.Nil(bso)
}

func testApiGetBSOs(db SyncApi, t *testing.T) {
	assert := assert.New(t)

	cId := 1

	// mucks with sorting order so we can test
	// it easier
	sortIndexes := []int{1, 3, 4, 2, 0}
	for i := 4; i >= 0; i-- {
		// make custom modified/sortOrder to test sorting
		bId := "b" + strconv.Itoa(i)
		payload := String("Hello")
		sortOrder := sortIndexes[i]

		_, err := db.PutBSO(cId, bId, payload, Int(sortOrder), nil)
		if !assert.NoError(err) {
			return
		}

		// sleep to get a different modified timestamp which we can
		// set through the public API
		time.Sleep(10 * time.Millisecond)
	}

	results, err := db.GetBSOs(cId, []string{"b0", "b2", "b4"}, 0, SORT_NEWEST, 10, 0)
	assert.NoError(err)
	if assert.NotNil(results) {
		assert.Equal(3, results.Total)
		assert.Equal("b0", results.BSOs[0].Id)
		assert.Equal("b2", results.BSOs[1].Id)
		assert.Equal("b4", results.BSOs[2].Id)
	}

	results, err = db.GetBSOs(cId, nil, 0, SORT_INDEX, 2, 0)
	assert.NoError(err)
	if assert.NotNil(results) {
		assert.Equal(5, results.Total)
		assert.Equal(2, len(results.BSOs))
		assert.Equal(2, results.Offset)
		assert.True(results.More)
		assert.Equal("b4", results.BSOs[0].Id)
		assert.Equal("b0", results.BSOs[1].Id)
	}
}

func testApiDeleteBSO(db SyncApi, t *testing.T) {
	assert := assert.New(t)

	cId := 1
	bId := "b0"
	payload := String("a")
	_, err := db.PutBSO(cId, bId, payload, nil, nil)
	if !assert.NoError(err) {
		return
	}

	_, err = db.DeleteBSO(cId, bId)
	if !assert.NoError(err) {
		return
	}

	bso, err := db.GetBSO(cId, bId)
	assert.Exactly(ErrNotFound, err)
	assert.Nil(bso)
}

func testApiDeleteBSOs(db SyncApi, t *testing.T) {
	assert := assert.New(t)
	// create some testing data
	cId := 1
	create := PostBSOInput{
		NewPutBSOInput("b0", String("payload 0"), Int(10), nil),
		NewPutBSOInput("b1", String("payload 1"), Int(10), nil),
		NewPutBSOInput("b2", String("payload 2"), Int(10), nil),
	}

	_, err := db.PostBSOs(cId, create)
	if assert.NoError(err) {

		_, err = db.DeleteBSO(cId, "b0")
		assert.NoError(err)

		// deleting non existant bId returns no errors
		_, err = db.DeleteBSO(cId, "bxi0")
		assert.NoError(err)

		// deleting multiple bIds
		_, err = db.DeleteBSOs(cId, "b1", "b2")
		assert.NoError(err)

	}

	var b *BSO
	b, err = db.GetBSO(cId, "b0")
	assert.Nil(b)
	assert.Exactly(ErrNotFound, err)

	b, err = db.GetBSO(cId, "b1")
	assert.Nil(b)
	assert.Exactly(ErrNotFound, err)

	b, err = db.GetBSO(cId, "b2")
	assert.Nil(b)
	assert.Exactly(ErrNotFound, err)
}

func testApiUsageStats(db SyncApi, t *testing.T) {
	assert := assert.New(t)
	cId := 1
	payload := strings.Repeat("x", 1024)

	create := PostBSOInput{
		NewPutBSOInput("b0", &payload, Int(10), nil),
		NewPutBSOInput("b1", &payload, Int(10), nil),
		NewPutBSOInput("b2", &payload, Int(10), nil),
	}

	_, err := db.PostBSOs(cId, create)
	if assert.NoError(err) {

		_, err = db.DeleteBSOs(cId, "b0", "b1")
		if assert.NoError(err) {
			pageStats, err := db.Usage()
			if assert.NoError(err) {

				// numbers pulled from previous tests
				assert.Equal(12, pageStats.Total)  // total pages in database
				assert.Equal(2, pageStats.Free)    // unused pages (from delete)
				assert.Equal(1024, pageStats.Size) // bytes/page
			}
		}
	}
}

func testApiPurgeExpired(db SyncApi, t *testing.T) {
	assert := assert.New(t)

	cId := 1
	payload := strings.Repeat("x", 10)

	create := PostBSOInput{
		NewPutBSOInput("b0", &payload, Int(10), Int(1)),
		NewPutBSOInput("b1", &payload, Int(10), Int(1)),
		NewPutBSOInput("b2", &payload, Int(10), Int(1)),
	}

	_, err := db.PostBSOs(cId, create)
	if assert.NoError(err) {
		time.Sleep(10 * time.Millisecond)
		purged, err := db.PurgeExpired()
		if assert.NoError(err) {
			assert.Equal(3, purged)
		}
	}
}

func testApiOptimize(db SyncApi, t *testing.T) {
	assert := assert.New(t)
	cId := 1
	payload := strings.Repeat("x", 1024)

	create := PostBSOInput{
		NewPutBSOInput("b0", &payload, Int(10), Int(1)),
		NewPutBSOInput("b1", &payload, Int(10), Int(1)),
		NewPutBSOInput("b2", &payload, Int(10), Int(1)),
	}

	_, err := db.PostBSOs(cId, create)
	if assert.NoError(err) {
		time.Sleep(10 * time.Millisecond)
		purged, err := db.PurgeExpired()
		if assert.NoError(err) {
			assert.Equal(3, purged)
			stats, err := db.Usage()
			if assert.NoError(err) {
				assert.Equal(25, stats.FreePercent()) // we know this from a previous test ;)
				vac, err := db.Optimize(20)
				assert.NoError(err)
				assert.True(vac)

				stats, _ := db.Usage()
				assert.Equal(0, stats.FreePercent())
			}
		}
	}
}

//func (db SyncApi, t *testing.T) {
//func (db SyncApi, t *testing.T) {
