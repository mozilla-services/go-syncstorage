package syncstorage

import (
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

	found, err := db.bsoExists(1, "nope")

	if err != nil {
		t.Error(err)
	}

	if found != false {
		t.Error("found expected to be false")
	}

	// insert a new BSO and test if a
	// true result comes back
	tx, err := db.db.Begin()

	if err != nil {
		t.Error(err)
	}

	cId := 1
	bId := "testBSO"
	modified := 1.0
	payload := "payload"
	sortIndex := uint(1)
	ttl := uint(1000)

	err = db.insertBSO(tx, cId, bId, modified, payload, sortIndex, ttl)
	if err != nil {
		t.Fatal(err)
		tx.Rollback()
	} else {
		tx.Commit()
	}

	found, err = db.bsoExists(cId, bId)

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
	sortIndex := uint(1)
	ttl := uint(1)

	var err error

	err = db.insertBSO(tx, cId, bId, modified, payload, sortIndex, ttl)

	if err != nil {
		t.Fatal(err)
	} else {
		tx.Commit()
	}

	tx, _ = db.db.Begin()
	modified = Now()
	payload = "Updated payload"
	err = db.updateBSO(tx, cId, bId, modified, &payload, nil, nil)

	if err != nil {
		t.Fatal(err)
	} else {
		tx.Commit()
	}

	bso, _ := db.getBSO(cId, bId)

	if bso.Modified != modified || bso.Payload != payload || bso.SortIndex != sortIndex || bso.TTL != ttl {
		t.Fatal("bso was not updated correctly")
	}

	modified = Now()
	sortIndex = 2
	tx, _ = db.db.Begin()
	err = db.updateBSO(tx, cId, bId, modified, nil, &sortIndex, nil)
	if err != nil {
		t.Fatal(err)
	} else {
		tx.Commit()
	}

	bso, _ = db.getBSO(cId, bId)

	if bso.Modified != modified || bso.Payload != payload || bso.SortIndex != sortIndex || bso.TTL != ttl {
		t.Fatal("bso was not updated correctly")
	}

	modified = Now()
	ttl = 2
	tx, _ = db.db.Begin()
	err = db.updateBSO(tx, cId, bId, modified, nil, nil, &ttl)
	if err != nil {
		t.Fatal(err)
	} else {
		tx.Commit()
	}

	bso, _ = db.getBSO(cId, bId)

	if bso.Modified != modified || bso.Payload != payload || bso.SortIndex != sortIndex || bso.TTL != ttl {
		t.Fatal("bso was not updated correctly")
	}
}
