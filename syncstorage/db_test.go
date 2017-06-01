package syncstorage

import (
	"database/sql"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func getTestDB() (*DB, error) {
	db, err := NewDB(":memory:", nil)

	if err != nil {
		return nil, err
	}

	return db, nil
}

func removeTestDB(d *DB) error {
	return os.Remove(d.Path)
}

func TestNewDB(t *testing.T) {
	assert := assert.New(t)
	{
		db, err := NewDB(":memory:", nil)
		assert.NoError(err)
		removeTestDB(db)
	}

	{
		for _, testSize := range []int{0, -1, -100, 1, 100} {
			db, err := NewDB(":memory:", &Config{CacheSize: testSize})
			if !assert.NoError(err) {
				return
			}

			var cachesize sql.NullInt64
			err = db.db.QueryRow("PRAGMA cache_size;").Scan(&cachesize)
			if assert.NoError(err) && assert.True(cachesize.Valid) {
				assert.Equal(testSize, int(cachesize.Int64))
			}
		}
	}

}

// TestStaticCollectionId ensures common collection
// names are map to standard id numbers. It should also
// save database looks ups for these as they are
// baked in
func TestStaticCollectionId(t *testing.T) {
	assert := assert.New(t)
	db, err := getTestDB()
	if !assert.NoError(err) {
		return
	}

	// make sure static collection ids match names
	commonCols := map[int]string{
		1: "clients", 2: "crypto", 3: "forms", 4: "history",
		5: "keys", 6: "meta", 7: "bookmarks", 8: "prefs",
		9: "tabs", 10: "passwords", 11: "addons",
	}

	// ensure DB actually has predefined common collections
	{
		rows, err := db.db.Query("SELECT Id, Name FROM Collections")
		if !assert.NoError(err) {
			return
		}

		results := make(map[int]string)

		for rows.Next() {
			var id int
			var name string
			if err := rows.Scan(&id, &name); !assert.NoError(err) {
				return
			}
			results[id] = name
		}
		rows.Close()

		for id, name := range commonCols {
			n, ok := results[id]
			assert.True(ok, id) // make sure it exists
			assert.Equal(name, n)
		}
	}

	// test that GetCollectionId returns the correct Ids
	// for the common collections
	{
		for id, name := range commonCols {
			checkid, err := db.GetCollectionId(name)
			if !assert.NoError(err, name) {
				return
			}

			if !assert.Equal(checkid, id, name) {
				return
			}
		}
	}

	// make sure custom collections start at Id: 100
	{
		id, err := db.CreateCollection("col1")
		if !assert.NoError(err) {
			return
		}

		// make sure new collection start at 100
		assert.Equal(id, 100)
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

	err := db.updateBSO(tx, cId, bId, Now(), nil, nil, nil)
	assert.Equal(t, ErrNothingToDo, err)
}

func TestPrivateUpdateBSOSuccessfullyUpdatesSingleValues(t *testing.T) {

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

	payload = "Updated payload"
	modified = Now()
	err = db.updateBSO(tx, cId, bId, modified, &payload, nil, nil)
	if !assert.NoError(err) {
		return
	}

	bso, err := db.getBSO(tx, cId, bId)
	if !assert.NoError(err) {
		return
	}

	assert.True((bso.Modified == modified || bso.Payload == payload || bso.SortIndex == sortIndex || bso.TTL == modified+ttl))

	sortIndex = 2
	modified = Now()
	err = db.updateBSO(tx, cId, bId, modified, nil, &sortIndex, nil)

	bso, err = db.getBSO(tx, cId, bId)
	if !assert.NoError(err) || !assert.NotNil(bso) {
		return
	}

	assert.True(bso.Modified == modified || bso.Payload == payload || bso.SortIndex == sortIndex || bso.TTL == modified+ttl)

	modified = Now()
	err = db.updateBSO(tx, cId, bId, modified, nil, nil, &ttl)
	if !assert.NoError(err) {
		return
	}

	bso, err = db.getBSO(tx, cId, bId)
	if !assert.NoError(err) || !assert.NotNil(bso) {
		return
	}

	assert.True(bso.Modified == modified || bso.Payload == payload || bso.SortIndex == sortIndex || bso.TTL == ttl+modified)
}

func TestPrivateUpdateBSOModifiedNotChangedOnTTLTouch(t *testing.T) {
	assert := assert.New(t)

	db, _ := getTestDB()
	defer removeTestDB(db)

	tx, _ := db.db.Begin()

	cId := 1
	bId := "testBSO"
	payload := "hello"
	sortIndex := 1
	ttl := 10
	modified := Now() - 100

	err := db.insertBSO(tx, cId, bId, modified, payload, sortIndex, ttl)
	if !assert.NoError(err) {
		return
	}

	ttl = 15
	updateModified := Now()
	err = db.updateBSO(tx, cId, bId, updateModified, nil, nil, &ttl)
	if !assert.NoError(err) {
		return
	}

	bso, err := db.getBSO(tx, cId, bId)
	if !assert.NoError(err) || !assert.NotNil(bso) {
		return
	}

	// ttl has changed
	assert.Equal(ttl+updateModified, bso.TTL)

	// modified has not changed
	assert.Equal(modified, bso.Modified)
}

func TestPrivatePutBSOInsertsWithMissingValues(t *testing.T) {
}

func TestPrivatePutBSOUpdates(t *testing.T) {
	assert := assert.New(t)

	db, _ := getTestDB()
	defer removeTestDB(db)

	tx, _ := db.db.Begin()
	defer tx.Rollback()

	modified := Now()
	if err := db.putBSO(tx, 1, "1", modified, String("initial"), nil, nil); err != nil {
		t.Error(err)
	}

	payload := String("Updated")
	sortIndex := Int(100)
	newModified := modified + 1000
	err := db.putBSO(tx, 1, "1", newModified, payload, sortIndex, nil)
	if !assert.NoError(err) {
		return
	}
	bso, err := db.getBSO(tx, 1, "1")

	assert.NoError(err)
	assert.NotNil(bso)

	assert.Equal(*payload, bso.Payload)
	assert.Equal(*sortIndex, bso.SortIndex)
	assert.Equal(newModified, bso.Modified)
}

func TestPrivateGetBSOsLimitOffset(t *testing.T) {

	assert := assert.New(t)

	db, _ := getTestDB()
	defer removeTestDB(db)

	tx, _ := db.db.Begin()
	defer tx.Rollback()

	cId := 1

	// put in enough records to test offset
	totalRecords := 12
	for i := 0; i < totalRecords; i++ {
		id := strconv.Itoa(i)
		payload := "payload-" + id
		sortIndex := i
		modified := Now()
		if err := db.insertBSO(tx, cId, id, modified, payload, sortIndex, DEFAULT_BSO_TTL); err != nil {
			t.Fatal("Error inserting BSO #", i, ":", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	{ // make sure a limit of 0 returns no records but with the `more` bit set
		results, err := db.getBSOs(tx, cId, nil, MaxTimestamp, 0, SORT_INDEX, 0, 0)
		if !assert.NoError(err) {
			return
		}

		assert.Len(results.BSOs, 0)
		assert.True(results.More)
		assert.Equal(0, results.Offset)
	}

	{ // make sure a limit of -1 returns all the records (unbounded)
		results, err := db.getBSOs(tx, cId, nil, MaxTimestamp, 0, SORT_INDEX, -1, 0)
		if !assert.NoError(err) {
			return
		}

		assert.Len(results.BSOs, totalRecords)
		assert.False(results.More)
		assert.Equal(0, results.Offset)
	}

	newer := 0
	limit := 5
	offset := 0

	// make sure invalid values don't work for limit and offset
	_, err := db.getBSOs(tx, cId, nil, MaxTimestamp, newer, SORT_INDEX, -2, offset)
	assert.Equal(ErrInvalidLimit, err)
	_, err = db.getBSOs(tx, cId, nil, MaxTimestamp, newer, SORT_INDEX, limit, -2)
	assert.Equal(ErrInvalidOffset, err)

	results, err := db.getBSOs(tx, cId, nil, MaxTimestamp, newer, SORT_NEWEST, limit, offset)
	assert.NoError(err)

	if assert.NotNil(results) {
		assert.Equal(5, len(results.BSOs), "Expected 5 results")
		assert.True(results.More)
		assert.Equal(5, results.Offset, "Expected next offset to be 5")

		// make sure we get the right BSOs
		assert.Equal("11", results.BSOs[0].Id, "Expected BSO w/ Id = 11")
		assert.Equal("7", results.BSOs[4].Id, "Expected BSO w/ Id = 7")
	}

	results2, err := db.getBSOs(tx, cId, nil, MaxTimestamp, newer, SORT_INDEX, limit, results.Offset)
	assert.NoError(err)
	if assert.NotNil(results2) {
		assert.Equal(5, len(results2.BSOs), "Expected 5 results")
		assert.True(results2.More)
		assert.Equal(10, results2.Offset, "Expected next offset to be 10")

		// make sure we get the right BSOs
		assert.Equal("6", results2.BSOs[0].Id, "Expected BSO w/ Id = 5")
		assert.Equal("2", results2.BSOs[4].Id, "Expected BSO w/ Id = 9")
	}

	results3, err := db.getBSOs(tx, cId, nil, MaxTimestamp, newer, SORT_INDEX, limit, results2.Offset)
	assert.NoError(err)
	if assert.NotNil(results3) {
		assert.Equal(2, len(results3.BSOs), "Expected 2 results")
		assert.False(results3.More)

		// make sure we get the right BSOs
		assert.Equal("1", results3.BSOs[0].Id, "Expected BSO w/ Id = 1")
		assert.Equal("0", results3.BSOs[1].Id, "Expected BSO w/ Id = 0")
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

	_, err := db.getBSOs(tx, cId, nil, MaxTimestamp, -1, SORT_NONE, 10, 0)
	assert.Equal(ErrInvalidNewer, err)

	assert.Nil(db.insertBSO(tx, cId, "b2", modified-2, "a", 1, DEFAULT_BSO_TTL))
	assert.Nil(db.insertBSO(tx, cId, "b1", modified-1, "a", 1, DEFAULT_BSO_TTL))
	assert.Nil(db.insertBSO(tx, cId, "b0", modified, "a", 1, DEFAULT_BSO_TTL))

	results, err := db.getBSOs(tx, cId, nil, MaxTimestamp, modified-3, SORT_NEWEST, 10, 0)
	assert.NoError(err)
	if assert.NotNil(results) {
		assert.Equal(3, len(results.BSOs))
		assert.Equal("b0", results.BSOs[0].Id)
		assert.Equal("b1", results.BSOs[1].Id)
		assert.Equal("b2", results.BSOs[2].Id)
	}

	results, err = db.getBSOs(tx, cId, nil, MaxTimestamp, modified-2, SORT_NEWEST, 10, 0)
	assert.NoError(err)
	if assert.NotNil(results) {
		assert.Equal("b0", results.BSOs[0].Id)
		assert.Equal("b1", results.BSOs[1].Id)
	}

	results, err = db.getBSOs(tx, cId, nil, MaxTimestamp, modified-1, SORT_NEWEST, 10, 0)
	assert.NoError(err)
	if assert.NotNil(results) {
		assert.Equal("b0", results.BSOs[0].Id)
	}

	results, err = db.getBSOs(tx, cId, nil, MaxTimestamp, modified, SORT_NEWEST, 10, 0)
	assert.NoError(err)
	if assert.NotNil(results) {
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

	_, err := db.getBSOs(tx, cId, nil, MaxTimestamp, -1, SORT_NONE, 10, 0)
	assert.Equal(ErrInvalidNewer, err)

	assert.Nil(db.insertBSO(tx, cId, "b2", modified-2, "a", 2, DEFAULT_BSO_TTL))
	assert.Nil(db.insertBSO(tx, cId, "b1", modified-1, "a", 0, DEFAULT_BSO_TTL))
	assert.Nil(db.insertBSO(tx, cId, "b0", modified, "a", 1, DEFAULT_BSO_TTL))

	results, err := db.getBSOs(tx, cId, nil, MaxTimestamp, 0, SORT_NEWEST, 10, 0)
	assert.NoError(err)
	if assert.NotNil(results) {
		assert.Equal(3, len(results.BSOs))
		assert.Equal("b0", results.BSOs[0].Id)
		assert.Equal("b1", results.BSOs[1].Id)
		assert.Equal("b2", results.BSOs[2].Id)
	}

	results, err = db.getBSOs(tx, cId, nil, MaxTimestamp, 0, SORT_OLDEST, 10, 0)
	assert.NoError(err)
	if assert.NotNil(results) {
		assert.Equal(3, len(results.BSOs))
		assert.Equal("b2", results.BSOs[0].Id)
		assert.Equal("b1", results.BSOs[1].Id)
		assert.Equal("b0", results.BSOs[2].Id)
	}

	results, err = db.getBSOs(tx, cId, nil, MaxTimestamp, 0, SORT_INDEX, 10, 0)
	assert.NoError(err)
	if assert.NotNil(results) {
		assert.Equal(3, len(results.BSOs))
		assert.Equal("b2", results.BSOs[0].Id)
		assert.Equal("b0", results.BSOs[1].Id)
		assert.Equal("b1", results.BSOs[2].Id)
	}
}

// Regression test for bug that deleted BSOs in *all* collections
func TestDeleteBSOsInCorrectCollection(t *testing.T) {
	db, _ := getTestDB()
	assert := assert.New(t)

	payload := "data"
	if _, err := db.PutBSO(1, "b1", &payload, nil, nil); !assert.NoError(err) {
		return
	}
	if _, err := db.PutBSO(2, "b1", &payload, nil, nil); !assert.NoError(err) {
		return
	}

	_, err := db.DeleteBSOs(1, "b1")
	if !assert.NoError(err) {
		return
	}

	bso, err := db.GetBSO(2, "b1")
	assert.NotNil(bso)
	assert.NoError(err)
}

func TestLastModified(t *testing.T) {
	db, _ := getTestDB()
	assert := assert.New(t)
	_, err := db.CreateCollection("col1")
	if !assert.NoError(err) {
		return
	}
	col2, err := db.CreateCollection("col2")
	if !assert.NoError(err) {
		return
	}
	_, err = db.CreateCollection("col3")
	if !assert.NoError(err) {
		return
	}

	modified := Now() + 100000
	if !assert.NoError(db.TouchCollection(col2, modified)) {
		return
	}

	m, err := db.LastModified()
	if !assert.NoError(err) {
		return
	}

	assert.Equal(modified, m)
}

func TestGetCollectionId(t *testing.T) {
	db, _ := getTestDB()
	_, err := db.GetCollectionId("bookmarks")
	assert.NoError(t, err)
}

func TestGetCollectionModified(t *testing.T) {
	db, _ := getTestDB()
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

func TestCreateCollection(t *testing.T) {
	db, _ := getTestDB()
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

func TestTouchCollection(t *testing.T) {
	db, _ := getTestDB()
	assert := assert.New(t)

	name := "test"
	cId, err := db.CreateCollection(name)
	if !assert.NoError(err) {
		return
	}

	modified := Now()
	assert.NoError(db.TouchCollection(cId, modified))
}

func TestDeleteCollection(t *testing.T) {
	db, _ := getTestDB()
	assert := assert.New(t)
	cName := "NewConnection"
	cId, err := db.CreateCollection(cName)
	if assert.Nil(err) {
		bIds := []string{"1", "2", "3"}
		for _, bId := range bIds {
			if _, err := db.PutBSO(cId, bId, String("test"), nil, nil); !assert.NoError(err) {
				return
			}
		}

		modified, err := db.DeleteCollection(cId)

		// make sure it was deleted
		if !assert.Nil(err) {
			return
		}

		lastModified, _ := db.LastModified()
		assert.Equal(lastModified, modified)

		// make sure BSOs are deleted
		for _, bId := range bIds {
			b, err := db.GetBSO(cId, bId)
			assert.Exactly(ErrNotFound, err)
			assert.Nil(b)
		}

		// make sure the collection's last modified is 0
		cModified, _ := db.GetCollectionModified(cId)
		assert.Equal(0, cModified)
	}
}

func TestInfoCollections(t *testing.T) {
	db, _ := getTestDB()
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

func TestInfoCollectionUsage(t *testing.T) {
	db, _ := getTestDB()
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
					t.Fatal(err.Error())
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
	db, _ := getTestDB()
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
					t.Fatal(err.Error())
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

func TestPutBSO(t *testing.T) {
	db, _ := getTestDB()
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

func TestPostBSOs(t *testing.T) {
	db, _ := getTestDB()
	assert := assert.New(t)

	cId := 1

	create := PostBSOInput{
		NewPutBSOInput("b0", String("payload 0"), Int(10), nil),

		// 10 digit sort index is invalid according to API
		NewPutBSOInput("b1", String("payload 1"), Int(1000000000), nil),

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

func TestGetBSO(t *testing.T) {
	db, _ := getTestDB()
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

func TestGetBSOs(t *testing.T) {
	db, _ := getTestDB()
	assert := assert.New(t)

	cId := 1

	// mucks with sorting order so we can test
	// using a slice instead of a map so we can guarantee
	// insert order
	sortIndexes := []int{
		1, // bso0
		3,
		4,
		2,
		0, // bso4
	}

	// Create in reverse order
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

	// get these 3 and sort them in order of newest
	results, err := db.GetBSOs(cId, []string{"b0", "b2", "b4"}, MaxTimestamp, 0, SORT_NEWEST, 10, 0)
	assert.NoError(err)
	if assert.NotNil(results) {
		assert.Equal("b0", results.BSOs[0].Id) // created last
		assert.Equal("b2", results.BSOs[1].Id)
		assert.Equal("b4", results.BSOs[2].Id) // created first
	}

	results, err = db.GetBSOs(cId, nil, MaxTimestamp, 0, SORT_INDEX, 2, 0)
	assert.NoError(err)
	if assert.NotNil(results) {
		assert.Equal(2, len(results.BSOs))
		assert.Equal(2, results.Offset)
		assert.True(results.More)
		assert.Equal("b2", results.BSOs[0].Id)
		assert.Equal("b1", results.BSOs[1].Id)
	}
}

func TestGetBSOModified(t *testing.T) {
	db, _ := getTestDB()
	assert := assert.New(t)

	cId := 1
	bId := "b0"
	payload := "a"

	expected, err := db.PutBSO(cId, bId, String(payload), nil, nil)
	if !assert.NoError(err) {
		return
	}

	// Make sure it returns the right BSO
	modified, err := db.GetBSOModified(cId, bId)
	if !assert.NoError(err) {
		return
	}

	assert.Equal(expected, modified)
}

func TestDeleteBSO(t *testing.T) {
	db, _ := getTestDB()
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

func TestDeleteBSOs(t *testing.T) {
	db, _ := getTestDB()
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

func TestUsageStats(t *testing.T) {
	db, _ := getTestDB()
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
				assert.Equal(10, pageStats.Total)  // total pages in database
				assert.Equal(0, pageStats.Free)    // unused pages (from delete)
				assert.Equal(4096, pageStats.Size) // bytes/page
			}
		}
	}
}

func TestPurgeExpired(t *testing.T) {
	db, _ := getTestDB()
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

func TestOptimize(t *testing.T) {
	db, _ := getTestDB()
	assert := assert.New(t)
	cId := 1
	payload := strings.Repeat("x", 4096)

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
				assert.Equal(23, stats.FreePercent()) // we know this from a previous test ;)
				vac, err := db.Optimize(20)
				assert.NoError(err)
				assert.True(vac)

				stats, _ := db.Usage()
				assert.Equal(0, stats.FreePercent())
			}
		}
	}
}

func TestDeleteEverything(t *testing.T) {
	db, _ := getTestDB()
	assert := assert.New(t)

	var (
		cId int
		err error
	)

	if cId, err = db.CreateCollection("my_collection"); !assert.NoError(err) {
		return
	}

	bId := "test"
	if _, err = db.PutBSO(cId, bId, String("test"), nil, nil); !assert.NoError(err) {
		return
	}

	if !assert.NoError(db.DeleteEverything()) {
		return
	}

	b, err := db.GetBSO(cId, bId)
	assert.Exactly(ErrNotFound, err)
	assert.Nil(b)

	// collection data stick around, maybe an off chance the user
	// makes it back into the server? it doesn't take up much space either way
	cTest, err := db.GetCollectionId("my_collection")
	assert.Nil(err)
	assert.Equal(100, cTest)
}

func TestGetSetKeyValue(t *testing.T) {
	assert := assert.New(t)
	db, _ := getTestDB()

	value, err := db.GetKey("testing")
	if !assert.NoError(err) || !assert.Equal("", value) {
		return
	}

	if !assert.NoError(db.SetKey("testing", "12345")) {
		return
	}

	if val, err := db.GetKey("testing"); assert.NoError(err) {
		assert.Equal("12345", val)
	}
}
