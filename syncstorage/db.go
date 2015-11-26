package syncstorage

import (
	"database/sql"
	"errors"
	"sync"

	_ "github.com/mattn/go-sqlite3"
	. "github.com/tj/go-debug"
)

var dbDebug = Debug("syncstorage:db")

var (
	ErrNotFound       = errors.New("syncstorage: Not Found")
	ErrNotImplemented = errors.New("syncstorage: Not Implemented")
	ErrNothingToDo    = errors.New("syncstorage: Nothing to do")

	ErrBSOIdsRequired = errors.New("syncstorage: BSO IDs required")
	ErrBSOIdInvalid   = errors.New("syncstorage: BSO ID invalid")
)

type SortType uint

const (
	SORT_NONE SortType = iota
	SORT_NEWEST
	SORT_OLDEST
	SORT_INDEX
)

type CollectionInfo struct {
	Name     string
	BSOCount uint
	Storage  uint
	Modified float64
}

type DB struct {
	sync.RWMutex

	// sqlite database path
	Path string

	db *sql.DB
}

func (d *DB) Open() (err error) {
	d.db, err = sql.Open("sqlite3", d.Path)

	if err != nil {
		return
	}

	// initialize and or update tables if required

	// Initialize Schema 0 if it doesn't exist
	sqlCheck := "SELECT name from sqlite_master WHERE type='table' AND name=?"
	var name string
	if err := d.db.QueryRow(sqlCheck, "KeyValues").Scan(&name); err == sql.ErrNoRows {

		tx, err := d.db.Begin()
		if err != nil {
			return err
		}

		if _, err := tx.Exec(SCHEMA_0); err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				return rollbackErr
			} else {
				return err
			}
		} else {
			dbDebug("Initialized new database at at %s", d.Path)
			if err := tx.Commit(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (d *DB) Close() {
	if d.db != nil {
		d.db.Close()
	}
}

func NewDB(path string) (*DB, error) {
	/* TODO: I'd be good to do some input validation */

	d := &DB{Path: path}

	if err := d.Open(); err != nil {
		return nil, err
	}

	return d, nil
}

/*
  The public functions in *DB control locking of the main object
  so we can only have one read or write at a time. The actual database
  work is handled by private functions.
*/

func (d *DB) GetCollectionId(name string) (id int, err error) {
	d.Lock()
	defer d.Unlock()
	err = d.db.QueryRow("SELECT Id FROM Collections where Name=?", name).Scan(&id)
	return
}

func (d *DB) CollectionInfo() (map[string]*CollectionInfo, error) {
	d.Lock()
	defer d.Unlock()
	return nil, ErrNotImplemented
}

func (d *DB) GetBSOs(cId int,
	ids []string,
	newer float64,
	fullBSO bool,
	sort SortType,
	limit uint,
	offset uint) ([]*BSO, error) {

	d.Lock()
	defer d.Unlock()

	return nil, ErrNotImplemented
}

func (d *DB) GetBSO(cId int, bId string) (*BSO, error) {
	d.Lock()
	defer d.Unlock()

	return nil, ErrNotImplemented
}

// PostBSOs takes a set of BSO and performs an Insert or Update on
// each of them.
type PostResults struct {
	Modified float64
	Success  []string
	failed   map[string][]string
}

func (d *DB) PostBSOs(cId int, bsos []*BSO) (*PostResults, error) {
	d.Lock()
	defer d.Unlock()

	return nil, ErrNotImplemented
}

// PutBSO creates or updates a BSO
func (d *DB) PutBSO(cId int, bso *BSO) (float64, error) {
	d.Lock()
	defer d.Unlock()

	return 0, ErrNotImplemented
}

func (d *DB) DeleteCollection(name string) error {
	d.Lock()
	defer d.Unlock()

	return ErrNotImplemented
}

// DeleteBSOs deletes multiple BSO. It returns the modified
// timestamp for the collection on success
func (d *DB) DeleteBSOs(cId int, bIds []string) (float64, error) {
	d.Lock()
	defer d.Unlock()

	return 0, ErrNotImplemented
}

// DeleteBSO deletes a single BSO and returns the
// modified timestamp for the collection
func DeleteBSO(cId int, bId string) (float64, error) {
	return 0, ErrNotImplemented
}

// pubBSO will INSERT or UPDATE a BSO
func (d *DB) putBSO(tx *sql.Tx,
	cId int,
	bId string,
	modified float64,
	payload *string,
	sortIndex *uint,
	ttl *uint,
) error {

	if payload == nil && sortIndex == nil && ttl == nil {
		return ErrNothingToDo
	}

	exists, err := d.bsoExists(tx, cId, bId)
	if err != nil {
		return err
	}

	// do an update
	if exists == true {
		return d.updateBSO(tx, cId, bId, modified, payload, sortIndex, ttl)
	} else {
		var p string
		var s, t uint

		if payload == nil {
			p = ""
		} else {
			p = *payload
		}

		if sortIndex == nil {
			s = 0
		} else {
			s = *sortIndex
		}

		if ttl == nil {
			t = 0
		} else {
			t = *ttl
		}

		return d.insertBSO(tx, cId, bId, modified, p, s, t)
	}
}

// bsoExists checks if a BSO is in the database
func (d *DB) bsoExists(tx *sql.Tx, cId int, bId string) (bool, error) {
	var found int
	query := "SELECT 1 FROM BSO WHERE CollectionId=? AND Id=?"
	err := tx.QueryRow(query, cId, bId).Scan(&found)

	if err == sql.ErrNoRows {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return true, nil
}

func (d *DB) getBSO(tx *sql.Tx, cId int, bId string) (*BSO, error) {

	bso := &BSO{Id: bId}
	query := "SELECT Modified, SortIndex, Payload, TTL FROM BSO WHERE CollectionId=? AND Id=?"
	err := tx.QueryRow(query, cId, bId).Scan(
		&bso.Modified,
		&bso.SortIndex,
		&bso.Payload,
		&bso.TTL,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrNotFound
		}
		return nil, err
	}

	return bso, nil

}

func (d *DB) insertBSO(
	tx *sql.Tx,
	cId int,
	bId string,
	modified float64,
	payload string,
	sortIndex uint,
	ttl uint,
) (err error) {
	_, err = tx.Exec(`INSERT INTO BSO (
			CollectionId, Id, SortIndex,
			PayLoad, PayLoadSize,
			Modified, TTL)
			VALUES (
				?,?,?,
				?,?,
				?,?
			)`,
		cId, bId, sortIndex,
		payload, len(payload),
		modified, ttl)

	return
}

// updateBSO updates a BSO. Values that are not provided (pointers)
// are not updated in the SQL statement
func (d *DB) updateBSO(
	tx *sql.Tx,
	cId int,
	bId string,
	modified float64,
	payload *string,
	sortIndex *uint,
	ttl *uint,
) (err error) {

	if payload == nil && sortIndex == nil && ttl == nil {
		return ErrNothingToDo
	}

	var values = make([]interface{}, 7)
	i := 0

	set := "modified=?"
	values[i] = modified
	i += 1

	if payload != nil {
		set = set + ", Payload=?, PayloadSize=?"
		values[i] = *payload
		i += 1
		values[i] = len(*payload)
		i += 1
	}

	if sortIndex != nil {
		set = set + ", SortIndex=?"
		values[i] = *sortIndex
		i += 1
	}

	if ttl != nil {
		set = set + ", TTL=?"
		values[i] = *ttl
		i += 1
	}

	// build the DML based on the data we have
	values[i] = cId
	i += 1
	values[i] = bId
	i += 1

	dml := "UPDATE BSO SET " + set + " WHERE CollectionId=? and Id=?"
	dbDebug("updateBSO(): %s, vals: %v", dml, values)
	_, err = tx.Exec(dml, values[0:i]...)

	return
}
