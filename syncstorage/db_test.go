package syncstorage

import (
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func getTestDB() (*DB, error) {
	f, err := ioutil.TempFile("", "syncstorage-test-")
	if err != nil {
		return nil, err
	}

	path := f.Name()
	db, err := New(path)

	if err != nil {
		return nil, err
	}

	return db, nil
}

func removeTestDB(d *DB) error {
	return os.Remove(d.Path)
}

func TestNewDB(t *testing.T) {
	db, err := getTestDB()
	assert.NoError(t, err)
	defer removeTestDB(db)
}

func TestCollectionId(t *testing.T) {
	db, _ := getTestDB()
	defer removeTestDB(db)

	_, err := db.GetCollectionId("bookmarks")
	assert.NoError(t, err)
}

func TestInfoCollections(t *testing.T) {

	assert := assert.New(t)
	db, _ := getTestDB()
	defer removeTestDB(db)

	id, err := db.GetCollectionId("bookmarks")
	assert.NoError(err)

	tx, err := db.db.Begin()
	assert.NoError(err)
	modified := Now()
	assert.NoError(db.touchCollection(tx, id, modified))
	assert.NoError(tx.Commit())

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

func TestInfoCollectionUsage(t *testing.T) {
	assert := assert.New(t)
	db, _ := getTestDB()
	defer removeTestDB(db)

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

func TestInfoCollectionCounts(t *testing.T) {
	assert := assert.New(t)
	db, _ := getTestDB()
	defer removeTestDB(db)

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

func TestBsoExists(t *testing.T) {

	assert := assert.New(t)

	db, _ := getTestDB()
	defer removeTestDB(db)

	tx, err := db.db.Begin()
	assert.NoError(err)
	found, err := db.bsoExists(tx, 1, "nope")
	assert.False(found)
	assert.NoError(err)
	assert.NoError(tx.Rollback())

	// insert a new BSO and test if a
	// true result comes back
	tx, err = db.db.Begin()
	assert.NoError(err)

	cId := 1
	bId := "testBSO"
	modified := Now()
	payload := "payload"
	sortIndex := 1
	ttl := 1000

	assert.NoError(db.insertBSO(tx, cId, bId, modified, payload, sortIndex, ttl))

	found, err = db.bsoExists(tx, cId, bId)
	assert.NoError(err)
	assert.True(found)
}

func TestUpdateBSOReturnsExpectedError(t *testing.T) {
	db, _ := getTestDB()
	defer removeTestDB(db)

	tx, _ := db.db.Begin()
	defer tx.Rollback()

	cId := 1
	bId := "testBSO"
	modified := Now()

	err := db.updateBSO(tx, cId, bId, modified, nil, nil, nil)
	assert.Equal(t, ErrNothingToDo, err)
}

func TestUpdateBSOSuccessfullyUpdatesSingleValues(t *testing.T) {

	assert := assert.New(t)
	db, _ := getTestDB()
	defer removeTestDB(db)

	tx, _ := db.db.Begin()

	cId := 1
	bId := "testBSO"
	modified := Now()
	payload := "initial value"
	sortIndex := 1
	ttl := 3600 * 1000

	var err error

	assert.NoError(db.insertBSO(tx, cId, bId, modified, payload, sortIndex, ttl))

	// remember this for later tests
	expectedTTL := modified + ttl

	modified = Now()
	payload = "Updated payload"
	assert.NoError(db.updateBSO(tx, cId, bId, modified, &payload, nil, nil))
	bso, err := db.getBSO(tx, cId, bId)
	assert.NoError(err)

	assert.True((bso.Modified == modified || bso.Payload == payload || bso.SortIndex == sortIndex || bso.TTL == expectedTTL))

	modified = Now()
	sortIndex = 2
	assert.NoError(db.updateBSO(tx, cId, bId, modified, nil, &sortIndex, nil))

	bso, err = db.getBSO(tx, cId, bId)
	assert.NoError(err)
	assert.NotNil(bso)

	assert.True(bso.Modified == modified || bso.Payload == payload || bso.SortIndex == sortIndex || bso.TTL == expectedTTL)

	modified = Now()
	assert.NoError(db.updateBSO(tx, cId, bId, modified, nil, nil, &ttl))
	bso, err = db.getBSO(tx, cId, bId)
	assert.NoError(err)
	assert.NotNil(bso)

	assert.True(bso.Modified == modified || bso.Payload == payload || bso.SortIndex == sortIndex || bso.TTL == ttl+modified)
}

func TestPrivatePutBSOInsertsWithMissingValues(t *testing.T) {
	assert := assert.New(t)

	db, _ := getTestDB()
	defer removeTestDB(db)

	tx, _ := db.db.Begin()
	defer tx.Rollback()

	cId := 1

	// make sure no data doesn't actually make a row
	err := db.putBSO(tx, cId, "obj-1", Now(), nil, nil, nil)
	assert.Equal(ErrNothingToDo, err, "Unspected error: %s", err)

	assert.NoError(db.putBSO(tx, cId, "obj-2", Now(), String("1"), nil, nil))
	assert.NoError(db.putBSO(tx, cId, "obj-3", Now(), nil, Int(1), nil))
	assert.NoError(db.putBSO(tx, cId, "obj-4", Now(), nil, nil, Int(1)))

	var numRows int
	err = tx.QueryRow("SELECT count(1) FROM BSO").Scan(&numRows)
	if assert.NoError(err) {
		assert.Equal(3, numRows)
	}
}

func TestPrivatePutBSOUpdates(t *testing.T) {
	assert := assert.New(t)

	db, _ := getTestDB()
	defer removeTestDB(db)

	tx, _ := db.db.Begin()
	defer tx.Rollback()

	if err := db.putBSO(tx, 1, "1", Now(), String("initial"), nil, nil); err != nil {
		t.Error(err)
	}

	modified := Now()
	payload := String("Updated")
	sortIndex := Int(100)
	assert.NoError(db.putBSO(tx, 1, "1", modified, payload, sortIndex, nil))
	bso, err := db.getBSO(tx, 1, "1")

	assert.NoError(err)
	assert.NotNil(bso)

	assert.Equal(*payload, bso.Payload)
	assert.Equal(*sortIndex, bso.SortIndex)
	assert.Equal(modified, bso.Modified)
}

func TestPublicPutBSO(t *testing.T) {

	assert := assert.New(t)

	db, _ := getTestDB()
	defer removeTestDB(db)

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

func TestPublicPostBSOs(t *testing.T) {
	assert := assert.New(t)

	db, _ := getTestDB()
	defer removeTestDB(db)

	cId := 1

	create := PostBSOInput{
		"b0": NewPutBSOInput(String("payload 0"), Int(10), nil),
		"b1": NewPutBSOInput(String("payload 1"), Int(-1), nil),
		"b2": NewPutBSOInput(String("payload 2"), Int(100), nil),
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
		"b0": NewPutBSOInput(String("updated 0"), Int(11), Int(100000)),
		"b2": NewPutBSOInput(String("updated 2"), Int(22), Int(10000)),
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

func TestPrivateGetBSOsLimitOffset(t *testing.T) {

	assert := assert.New(t)

	db, _ := getTestDB()
	defer removeTestDB(db)

	tx, _ := db.db.Begin()
	defer tx.Rollback()

	cId := 1

	modified := Now()

	// put in enough records to test offset
	totalRecords := 12
	for i := 0; i < totalRecords; i++ {
		id := strconv.Itoa(i)
		payload := "payload-" + id
		sortIndex := i
		if err := db.insertBSO(tx, cId, id, modified, payload, sortIndex, DEFAULT_BSO_TTL); err != nil {
			t.Fatal("Error inserting BSO #", i, ":", err)
		}
	}

	newer := 0
	sort := SORT_INDEX
	limit := 5
	offset := 0

	// make sure invalid values don't work for limit and offset
	_, err := db.getBSOs(tx, cId, nil, newer, sort, -1, offset)
	assert.Equal(ErrInvalidLimit, err)
	_, err = db.getBSOs(tx, cId, nil, newer, sort, limit, -1)
	assert.Equal(ErrInvalidOffset, err)

	results, err := db.getBSOs(tx, cId, nil, newer, sort, limit, offset)
	assert.NoError(err)

	if assert.NotNil(results) {
		assert.Equal(5, len(results.BSOs), "Expected 5 results")
		assert.Equal(totalRecords, results.Total, "Expected %d bsos to be found", totalRecords)
		assert.True(results.More)
		assert.Equal(5, results.Offset, "Expected next offset to be 5")

		// make sure we get the right BSOs
		assert.Equal("0", results.BSOs[0].Id, "Expected BSO w/ Id = 0")
		assert.Equal("4", results.BSOs[4].Id, "Expected BSO w/ Id = 4")
	}

	results2, err := db.getBSOs(tx, cId, nil, newer, sort, limit, results.Offset)
	assert.NoError(err)
	if assert.NotNil(results2) {
		assert.Equal(5, len(results2.BSOs), "Expected 5 results")
		assert.Equal(totalRecords, results.Total, "Expected %d bsos to be found", totalRecords)
		assert.True(results2.More)
		assert.Equal(10, results2.Offset, "Expected next offset to be 10")

		// make sure we get the right BSOs
		assert.Equal("5", results2.BSOs[0].Id, "Expected BSO w/ Id = 5")
		assert.Equal("9", results2.BSOs[4].Id, "Expected BSO w/ Id = 9")
	}

	results3, err := db.getBSOs(tx, cId, nil, newer, sort, limit, results2.Offset)
	assert.NoError(err)
	if assert.NotNil(results3) {
		assert.Equal(2, len(results3.BSOs), "Expected 2 results")
		assert.Equal(totalRecords, results.Total, "Expected %d bsos to be found", totalRecords)
		assert.False(results3.More)

		// make sure we get the right BSOs
		assert.Equal("10", results3.BSOs[0].Id, "Expected BSO w/ Id = 10")
		assert.Equal("11", results3.BSOs[1].Id, "Expected BSO w/ Id = 11")
	}

}

func TestPrivateGetBSOsNewer(t *testing.T) {

	assert := assert.New(t)

	db, _ := getTestDB()
	defer removeTestDB(db)

	tx, _ := db.db.Begin()
	defer tx.Rollback()

	// put in enough records to test Newer
	cId := 1

	modified := Now()

	_, err := db.getBSOs(tx, cId, nil, -1, SORT_NONE, 10, 0)
	assert.Equal(ErrInvalidNewer, err)

	assert.Nil(db.insertBSO(tx, cId, "b2", modified-2, "a", 1, DEFAULT_BSO_TTL))
	assert.Nil(db.insertBSO(tx, cId, "b1", modified-1, "a", 1, DEFAULT_BSO_TTL))
	assert.Nil(db.insertBSO(tx, cId, "b0", modified, "a", 1, DEFAULT_BSO_TTL))

	results, err := db.getBSOs(tx, cId, nil, modified-3, SORT_NEWEST, 10, 0)
	assert.NoError(err)
	if assert.NotNil(results) {
		assert.Equal(3, results.Total)
		assert.Equal(3, len(results.BSOs))
		assert.Equal("b0", results.BSOs[0].Id)
		assert.Equal("b1", results.BSOs[1].Id)
		assert.Equal("b2", results.BSOs[2].Id)
	}

	results, err = db.getBSOs(tx, cId, nil, modified-2, SORT_NEWEST, 10, 0)
	assert.NoError(err)
	if assert.NotNil(results) {
		assert.Equal(2, results.Total)
		assert.Equal("b0", results.BSOs[0].Id)
		assert.Equal("b1", results.BSOs[1].Id)
	}

	results, err = db.getBSOs(tx, cId, nil, modified-1, SORT_NEWEST, 10, 0)
	assert.NoError(err)
	if assert.NotNil(results) {
		assert.Equal(1, results.Total)
		assert.Equal("b0", results.BSOs[0].Id)
	}

	results, err = db.getBSOs(tx, cId, nil, modified, SORT_NEWEST, 10, 0)
	assert.NoError(err)
	if assert.NotNil(results) {
		assert.Equal(0, results.Total)
	}

}

func TestPrivateGetBSOsSort(t *testing.T) {

	assert := assert.New(t)

	db, _ := getTestDB()
	defer removeTestDB(db)

	tx, _ := db.db.Begin()
	defer tx.Rollback()

	// put in enough records to test Newer
	cId := 1

	modified := Now()

	_, err := db.getBSOs(tx, cId, nil, -1, SORT_NONE, 10, 0)
	assert.Equal(ErrInvalidNewer, err)

	assert.Nil(db.insertBSO(tx, cId, "b2", modified-2, "a", 2, DEFAULT_BSO_TTL))
	assert.Nil(db.insertBSO(tx, cId, "b1", modified-1, "a", 0, DEFAULT_BSO_TTL))
	assert.Nil(db.insertBSO(tx, cId, "b0", modified, "a", 1, DEFAULT_BSO_TTL))

	results, err := db.getBSOs(tx, cId, nil, 0, SORT_NEWEST, 10, 0)
	assert.NoError(err)
	if assert.NotNil(results) {
		assert.Equal(3, len(results.BSOs))
		assert.Equal("b0", results.BSOs[0].Id)
		assert.Equal("b1", results.BSOs[1].Id)
		assert.Equal("b2", results.BSOs[2].Id)
	}

	results, err = db.getBSOs(tx, cId, nil, 0, SORT_OLDEST, 10, 0)
	assert.NoError(err)
	if assert.NotNil(results) {
		assert.Equal(3, len(results.BSOs))
		assert.Equal("b2", results.BSOs[0].Id)
		assert.Equal("b1", results.BSOs[1].Id)
		assert.Equal("b0", results.BSOs[2].Id)
	}

	results, err = db.getBSOs(tx, cId, nil, 0, SORT_INDEX, 10, 0)
	assert.NoError(err)
	if assert.NotNil(results) {
		assert.Equal(3, len(results.BSOs))
		assert.Equal("b1", results.BSOs[0].Id)
		assert.Equal("b0", results.BSOs[1].Id)
		assert.Equal("b2", results.BSOs[2].Id)
	}
}

func TestPublicGetBSO(t *testing.T) {

	assert := assert.New(t)

	db, _ := getTestDB()
	defer removeTestDB(db)

	cId := 1
	bId := "b0"
	payload := "a"

	tx, err := db.db.Begin()
	assert.NoError(err)
	assert.Nil(db.insertBSO(tx, cId, bId, Now(), payload, 0, DEFAULT_BSO_TTL))
	assert.Nil(tx.Commit())

	// Make sure it returns the right BSO
	bso, err := db.GetBSO(cId, "b0")
	assert.NoError(err)
	if assert.NotNil(bso) {
		assert.Equal(bId, bso.Id)
		assert.Equal(payload, bso.Payload)
	}

	bso, err = db.GetBSO(cId, "nope")
	assert.NoError(err)
	assert.Nil(bso)
}

// TestPublicGetBSO asserts the interface to the private getBSOs is correct
func TestPublicGetBSOs(t *testing.T) {

	assert := assert.New(t)
	db, _ := getTestDB()
	defer removeTestDB(db)

	cId := 1

	tx, err := db.db.Begin()
	assert.NoError(err)

	modified := Now()
	assert.Nil(db.insertBSO(tx, cId, "b0", modified-10, "0", 4, DEFAULT_BSO_TTL))
	assert.Nil(db.insertBSO(tx, cId, "b1", modified-20, "1", 2, DEFAULT_BSO_TTL))
	assert.Nil(db.insertBSO(tx, cId, "b2", modified-30, "2", 3, DEFAULT_BSO_TTL))
	assert.Nil(db.insertBSO(tx, cId, "b3", modified-40, "3", 5, DEFAULT_BSO_TTL))
	assert.Nil(db.insertBSO(tx, cId, "b4", modified-50, "4", 1, DEFAULT_BSO_TTL))
	assert.NoError(tx.Commit())

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
		assert.Equal("b1", results.BSOs[1].Id)
	}
}

func TestCreateCollection(t *testing.T) {
	assert := assert.New(t)
	db, _ := getTestDB()
	defer removeTestDB(db)

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

func TestDeleteCollection(t *testing.T) {
	assert := assert.New(t)
	db, _ := getTestDB()
	defer removeTestDB(db)

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

func TestDeleteBSOs(t *testing.T) {
	assert := assert.New(t)
	db, _ := getTestDB()
	defer removeTestDB(db)

	// create some testing data
	cId := 1
	create := PostBSOInput{
		"b0": NewPutBSOInput(String("payload 0"), Int(10), nil),
		"b1": NewPutBSOInput(String("payload 1"), Int(10), nil),
		"b2": NewPutBSOInput(String("payload 2"), Int(10), nil),
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
	assert.Nil(err)

	b, err = db.GetBSO(cId, "b1")
	assert.Nil(b)
	assert.Nil(err)

	b, err = db.GetBSO(cId, "b2")
	assert.Nil(b)
	assert.Nil(err)
}

func TestUsageStats(t *testing.T) {
	assert := assert.New(t)
	db, _ := getTestDB()
	defer removeTestDB(db)

	cId := 1
	payload := strings.Repeat("x", 1024)

	create := PostBSOInput{
		"b0": NewPutBSOInput(&payload, Int(10), nil),
		"b1": NewPutBSOInput(&payload, Int(10), nil),
		"b2": NewPutBSOInput(&payload, Int(10), nil),
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

func TestPurgeExpired(t *testing.T) {
	assert := assert.New(t)
	db, _ := getTestDB()
	defer removeTestDB(db)

	cId := 1
	payload := strings.Repeat("x", 10)

	create := PostBSOInput{
		"b0": NewPutBSOInput(&payload, Int(10), Int(1)),
		"b1": NewPutBSOInput(&payload, Int(10), Int(1)),
		"b2": NewPutBSOInput(&payload, Int(10), Int(1)),
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

func TestOptimize(t *testing.T) {
	assert := assert.New(t)
	db, _ := getTestDB()
	defer removeTestDB(db)

	cId := 1
	payload := strings.Repeat("x", 1024)

	create := PostBSOInput{
		"b0": NewPutBSOInput(&payload, Int(10), Int(1)),
		"b1": NewPutBSOInput(&payload, Int(10), Int(1)),
		"b2": NewPutBSOInput(&payload, Int(10), Int(1)),
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
				assert.NoError(db.Optimize(20))

				stats, _ := db.Usage()
				assert.Equal(0, stats.FreePercent())
			}
		}
	}
}
