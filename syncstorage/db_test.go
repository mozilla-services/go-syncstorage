package syncstorage

import (
	"io/ioutil"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func getTestDB() (*DB, error) {
	f, err := ioutil.TempFile("", "syncstorage-test-")
	if err != nil {
		return nil, err
	}

	path := f.Name()
	db, err := NewDB(path)

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
	if err != nil {
		t.Fatal(err)
	}
	defer removeTestDB(db)
}

func TestCollectionId(t *testing.T) {
	db, _ := getTestDB()
	defer removeTestDB(db)

	_, err := db.GetCollectionId("bookmarks")
	if err != nil {
		t.Fatal(err)
	}
}

func TestBsoExists(t *testing.T) {

	db, _ := getTestDB()
	defer removeTestDB(db)

	tx, _ := db.db.Begin()
	found, err := db.bsoExists(tx, 1, "nope")
	tx.Rollback()

	if err != nil {
		t.Error(err)
	}

	if found != false {
		t.Error("found expected to be false")
	}

	// insert a new BSO and test if a
	// true result comes back
	tx, _ = db.db.Begin()

	cId := 1
	bId := "testBSO"
	modified := Now()
	payload := "payload"
	sortIndex := 1
	ttl := 1000

	err = db.insertBSO(tx, cId, bId, modified, payload, sortIndex, ttl)
	if err != nil {
		t.Fatal(err)
	}

	found, err = db.bsoExists(tx, cId, bId)

	if err != nil {
		t.Error(err)
	}

	if found != true {
		t.Error("found expected to be true")
	}
}

func TestUpdateBSOReturnsExpectedError(t *testing.T) {
	db, _ := getTestDB()
	defer removeTestDB(db)

	tx, _ := db.db.Begin()
	defer tx.Rollback()

	cId := 1
	bId := "testBSO"
	modified := Now()

	if err := db.updateBSO(tx, cId, bId, modified, nil, nil, nil); err != ErrNothingToDo {
		t.Fatal("Got unexpected error", err)
	}
}

func TestUpdateBSOSuccessfullyUpdatesSingleValues(t *testing.T) {
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

	err = db.insertBSO(tx, cId, bId, modified, payload, sortIndex, ttl)

	if err != nil {
		t.Fatal(err)
	}

	// remember this for later tests
	expectedTTL := modified + ttl

	modified = Now()
	payload = "Updated payload"
	err = db.updateBSO(tx, cId, bId, modified, &payload, nil, nil)

	if err != nil {
		t.Fatal(err)
	}

	bso, _ := db.getBSO(tx, cId, bId)

	if bso.Modified != modified || bso.Payload != payload || bso.SortIndex != sortIndex || bso.TTL != expectedTTL {
		t.Fatal("bso was not updated correctly", bso.TTL, ttl+modified)
	}

	modified = Now()
	sortIndex = 2
	err = db.updateBSO(tx, cId, bId, modified, nil, &sortIndex, nil)
	if err != nil {
		t.Fatal(err)
	}

	bso, _ = db.getBSO(tx, cId, bId)

	if bso.Modified != modified || bso.Payload != payload || bso.SortIndex != sortIndex || bso.TTL != expectedTTL {
		t.Fatal("bso was not updated correctly")
	}

	modified = Now()
	err = db.updateBSO(tx, cId, bId, modified, nil, nil, &ttl)
	if err != nil {
		t.Fatal(err)
	}

	bso, _ = db.getBSO(tx, cId, bId)

	if bso.Modified != modified || bso.Payload != payload || bso.SortIndex != sortIndex || bso.TTL != ttl+modified {
		t.Fatal("bso was not updated correctly")
	}
}

func TestPrivatePutBSOInsertsWithMissingValues(t *testing.T) {
	db, _ := getTestDB()
	defer removeTestDB(db)

	tx, _ := db.db.Begin()
	defer tx.Rollback()

	cId := 1

	// make sure no data doesn't actually make a row
	if err := db.putBSO(tx, cId, "obj-1", Now(), nil, nil, nil); err != ErrNothingToDo {
		t.Error("Unexpected error", err)
	}

	if err := db.putBSO(tx, cId, "obj-2", Now(), String("1"), nil, nil); err != nil {
		t.Error(err)
	}

	if err := db.putBSO(tx, cId, "obj-3", Now(), nil, Int(1), nil); err != nil {
		t.Error(err)
	}

	if err := db.putBSO(tx, cId, "obj-4", Now(), nil, nil, Int(1)); err != nil {
		t.Error(err)
	}

	var numRows int
	if err := tx.QueryRow("SELECT count(1) FROM BSO").Scan(&numRows); err != nil || numRows != 3 {
		t.Errorf("Got err %v, expected 4 rows but got %d", err, numRows)
	}
}

func TestPrivatePutBSOUpdates(t *testing.T) {
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
	if err := db.putBSO(tx, 1, "1", modified, payload, sortIndex, nil); err != nil {
		t.Error(err)
	}

	if bso, err := db.getBSO(tx, 1, "1"); err != nil {
		t.Error(err)
	} else {
		if bso.Payload != *payload {
			t.Errorf("Expected %s got %s", *payload, bso.Payload)
		}

		if bso.SortIndex != *sortIndex {
			t.Errorf("Expected %d got %d", *sortIndex, bso.SortIndex)
		}

		if bso.Modified != modified {
			t.Errorf("Expected %f got %f", modified, bso.Modified)
		}
	}
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
	assert.Nil(err)

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
	assert.Nil(err)
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
	assert.Nil(err)
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
	assert.Nil(err)
	if assert.NotNil(results) {
		assert.Equal(3, results.Total)
		assert.Equal(3, len(results.BSOs))
		assert.Equal("b0", results.BSOs[0].Id)
		assert.Equal("b1", results.BSOs[1].Id)
		assert.Equal("b2", results.BSOs[2].Id)
	}

	results, err = db.getBSOs(tx, cId, nil, modified-2, SORT_NEWEST, 10, 0)
	assert.Nil(err)
	if assert.NotNil(results) {
		assert.Equal(2, results.Total)
		assert.Equal("b0", results.BSOs[0].Id)
		assert.Equal("b1", results.BSOs[1].Id)
	}

	results, err = db.getBSOs(tx, cId, nil, modified-1, SORT_NEWEST, 10, 0)
	assert.Nil(err)
	if assert.NotNil(results) {
		assert.Equal(1, results.Total)
		assert.Equal("b0", results.BSOs[0].Id)
	}

	results, err = db.getBSOs(tx, cId, nil, modified, SORT_NEWEST, 10, 0)
	assert.Nil(err)
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
	assert.Nil(err)
	if assert.NotNil(results) {
		assert.Equal(3, len(results.BSOs))
		assert.Equal("b0", results.BSOs[0].Id)
		assert.Equal("b1", results.BSOs[1].Id)
		assert.Equal("b2", results.BSOs[2].Id)
	}

	results, err = db.getBSOs(tx, cId, nil, 0, SORT_OLDEST, 10, 0)
	assert.Nil(err)
	if assert.NotNil(results) {
		assert.Equal(3, len(results.BSOs))
		assert.Equal("b2", results.BSOs[0].Id)
		assert.Equal("b1", results.BSOs[1].Id)
		assert.Equal("b0", results.BSOs[2].Id)
	}

	results, err = db.getBSOs(tx, cId, nil, 0, SORT_INDEX, 10, 0)
	assert.Nil(err)
	if assert.NotNil(results) {
		assert.Equal(3, len(results.BSOs))
		assert.Equal("b1", results.BSOs[0].Id)
		assert.Equal("b0", results.BSOs[1].Id)
		assert.Equal("b2", results.BSOs[2].Id)
	}
}
