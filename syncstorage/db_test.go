package syncstorage

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
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
	ttl := 1

	var err error

	err = db.insertBSO(tx, cId, bId, modified, payload, sortIndex, ttl)

	if err != nil {
		t.Fatal(err)
	}

	modified = Now()
	payload = "Updated payload"
	err = db.updateBSO(tx, cId, bId, modified, &payload, nil, nil)

	if err != nil {
		t.Fatal(err)
	}

	bso, _ := db.getBSO(tx, cId, bId)

	if bso.Modified != modified || bso.Payload != payload || bso.SortIndex != sortIndex || bso.TTL != ttl {
		t.Fatal("bso was not updated correctly")
	}

	modified = Now()
	sortIndex = 2
	err = db.updateBSO(tx, cId, bId, modified, nil, &sortIndex, nil)
	if err != nil {
		t.Fatal(err)
	}

	bso, _ = db.getBSO(tx, cId, bId)

	if bso.Modified != modified || bso.Payload != payload || bso.SortIndex != sortIndex || bso.TTL != ttl {
		t.Fatal("bso was not updated correctly")
	}

	modified = Now()
	ttl = 2
	err = db.updateBSO(tx, cId, bId, modified, nil, nil, &ttl)
	if err != nil {
		t.Fatal(err)
	}

	bso, _ = db.getBSO(tx, cId, bId)

	if bso.Modified != modified || bso.Payload != payload || bso.SortIndex != sortIndex || bso.TTL != ttl {
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

func TestPrivateGetBSOs(t *testing.T) {
	db, _ := getTestDB()
	defer removeTestDB(db)

	tx, _ := db.db.Begin()
	defer tx.Rollback()

	cId := 1
	bIds := []string{"a", "b", "c", "d"}
	newer := 0
	sort := SORT_NEWEST
	limit := 2000
	offset := 0

	// put a record in
	err := db.insertBSO(tx, cId, "a", Now(), "payload", 10, 1000)

	bsos, err := db.getBSOs(tx, cId, bIds, newer, sort, limit, offset)
	fmt.Println(bsos, err)
}
