package syncstorage

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"

	_ "github.com/mattn/go-sqlite3"
	. "github.com/tj/go-debug"
)

var dbDebug = Debug("syncstorage:db")

var (
	ErrNotFound       = errors.New("Not Found")
	ErrNotImplemented = errors.New("Not Implemented")
	ErrNothingToDo    = errors.New("Nothing to do")

	ErrInvalidBSOId        = errors.New("Invalid BSO Id")
	ErrInvalidCollectionId = errors.New("Invalid Collection Id")
	ErrInvalidPayload      = errors.New("Invalid Payload")
	ErrInvalidSortIndex    = errors.New("Invalid Sort Index")
	ErrInvalidTTL          = errors.New("Invalid TTL")

	ErrInvalidLimit  = errors.New("Invalid LIMIT")
	ErrInvalidOffset = errors.New("Invalid OFFSET")
	ErrInvalidNewer  = errors.New("Invalid NEWER than")
)

type SortType int

const (
	SORT_NONE SortType = iota
	SORT_NEWEST
	SORT_OLDEST
	SORT_INDEX

	// absolute maximum records getBSOs can return
	LIMIT_MAX = 1000

	// keep BSO for 1 year
	DEFAULT_BSO_TTL = 365 * 24 * 60 * 60 * 1000
)

type CollectionInfo struct {
	Name     string
	BSOs     int
	Storage  int
	Modified int
}

// PostBSOs takes a set of BSO and performs an Insert or Update on
// each of them.
type PostResults struct {
	Modified int
	Success  []string
	Failed   map[string][]string
}

func NewPostResults(modified int) *PostResults {
	return &PostResults{
		Modified: modified,
		Success:  make([]string, 0),
		Failed:   make(map[string][]string),
	}
}
func (p *PostResults) AddSuccess(bId ...string) {
	p.Success = append(p.Success, bId...)
}
func (p *PostResults) AddFailure(bId string, reasons ...string) {
	p.Failed[bId] = reasons
}

// GetResults holds search results for BSOs, this is what getBSOs() returns
type GetResults struct {
	BSOs   []*BSO
	Total  int
	More   bool
	Offset int
}

func (g *GetResults) String() string {
	s := fmt.Sprintf("Total: %d, More: %v, Offset: %d\nBSOs:\n",
		g.Total, g.More, g.Offset)

	for _, b := range g.BSOs {
		s += fmt.Sprintf("  Id:%s, Modified:%d, SortIndex:%d, TTL:%d, %s\n",
			b.Id, b.Modified, b.SortIndex, b.TTL, b.Payload)
	}

	return s
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

func (d *DB) GetCollectionModified(cId int) (modified int, err error) {
	d.Lock()
	defer d.Unlock()
	err = d.db.QueryRow("SELECT modified FROM Collections where Id=?", cId).Scan(&modified)
	return
}

// InfoCollections create a map of collection names to last modified times
func (d *DB) InfoCollections() (map[string]int, error) {
	d.Lock()
	defer d.Unlock()

	rows, err := d.db.Query("SELECT Name,Modified FROM Collections ORDER BY Id")
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	results := make(map[string]int)
	for rows.Next() {
		var name string
		var modified int
		if err := rows.Scan(&name, &modified); err != nil {
			return nil, err
		}
		results[name] = modified
	}

	return results, nil
}

func (d *DB) GetBSOs(
	cId int,
	ids []string,
	newer int,
	sort SortType,
	limit int,
	offset int) (r *GetResults, err error) {

	d.Lock()
	defer d.Unlock()

	t, err := d.db.Begin()
	if err != nil {
		return
	}

	r, err = d.getBSOs(t, cId, ids, newer, sort, limit, offset)

	if err != nil {
		t.Rollback()
		return
	}

	t.Commit()
	return

}

func (d *DB) GetBSO(cId int, bId string) (b *BSO, err error) {
	d.Lock()
	defer d.Unlock()

	t, err := d.db.Begin()
	if err != nil {
		return nil, err
	}

	b, err = d.getBSO(t, cId, bId)

	if err != nil {
		t.Rollback()
		return
	}

	t.Commit()
	return
}

type PostBSOInput map[string]*PutBSOInput
type PutBSOInput struct {
	TTL, SortIndex *int
	Payload        *string
}

func NewPutBSOInput(payload *string, sortIndex, ttl *int) *PutBSOInput {
	if ttl == nil {
		t := DEFAULT_BSO_TTL
		ttl = &t
	}
	return &PutBSOInput{TTL: ttl, SortIndex: sortIndex, Payload: payload}
}

func (d *DB) PostBSOs(cId int, input PostBSOInput) (*PostResults, error) {
	d.Lock()
	defer d.Unlock()

	tx, err := d.db.Begin()
	if err != nil {
		return nil, err
	}

	modified := Now()
	results := NewPostResults(modified)

	for bId, data := range input {
		err = d.putBSO(tx, cId, bId, modified, data.Payload, data.SortIndex, data.TTL)
		if err != nil {
			results.AddFailure(bId, err.Error())
			continue
		} else {
			results.AddSuccess(bId)
		}
	}

	// update the collection
	err = d.touchCollection(tx, cId, modified)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	tx.Commit()

	return results, nil
}

// PutBSO creates or updates a BSO
func (d *DB) PutBSO(cId int, bId string, payload *string, sortIndex *int, ttl *int) (modified int, err error) {
	d.Lock()
	defer d.Unlock()

	tx, err := d.db.Begin()
	if err != nil {
		return
	}

	modified = Now()
	err = d.putBSO(tx, cId, bId, modified, payload, sortIndex, ttl)

	if err != nil {
		tx.Rollback()
		return
	}

	// update the collection
	err = d.touchCollection(tx, cId, modified)
	if err != nil {
		tx.Rollback()
		return
	}

	tx.Commit()
	return
}

func (d *DB) DeleteCollection(name string) error {
	d.Lock()
	defer d.Unlock()

	return ErrNotImplemented
}

// DeleteBSOs deletes multiple BSO. It returns the modified
// timestamp for the collection on success
func (d *DB) DeleteBSOs(cId int, bIds []string) (int, error) {
	d.Lock()
	defer d.Unlock()

	return 0, ErrNotImplemented
}

// DeleteBSO deletes a single BSO and returns the
// modified timestamp for the collection
func DeleteBSO(cId int, bId string) (int, error) {
	return 0, ErrNotImplemented
}

func (d *DB) touchCollection(tx *sql.Tx, cId, modified int) (err error) {
	_, err = tx.Exec(`UPDATE Collections SET modified=? WHERE Id=?`, modified, cId)
	return
}

// putBSO will INSERT or UPDATE a BSO
func (d *DB) putBSO(tx *sql.Tx,
	cId int,
	bId string,
	modified int,
	payload *string,
	sortIndex *int,
	ttl *int,
) error {
	if payload == nil && sortIndex == nil && ttl == nil {
		return ErrNothingToDo
	}

	if !BSOIdOk(bId) {
		return ErrInvalidBSOId
	}
	if sortIndex != nil && !SortIndexOk(*sortIndex) {
		return ErrInvalidSortIndex
	}

	if ttl != nil && !TTLOk(*ttl) {
		return ErrInvalidTTL
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
		var s, t int

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
			t = DEFAULT_BSO_TTL
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

// getBSOs searches for bsos based on the api 1.5 criteria
func (d *DB) getBSOs(
	tx *sql.Tx,
	cId int,
	ids []string,
	newer int,
	sort SortType,
	limit int,
	offset int) (*GetResults, error) {

	if !OffsetOk(offset) {
		return nil, ErrInvalidOffset
	}

	if !LimitOk(limit) {
		return nil, ErrInvalidLimit
	}

	if !NewerOk(newer) {
		return nil, ErrInvalidNewer
	}

	query := "SELECT Id, SortIndex, Payload, Modified, TTL FROM BSO "

	where := "WHERE CollectionId=? AND Modified > ? AND TTL>=?"
	values := []interface{}{cId, newer, Now()}

	if len(ids) > 0 {
		// spec says only 100 ids at a time
		if len(ids) > 100 {
			ids = ids[0:100]
		}

		where += " AND Id IN (?" + strings.Repeat(",?", len(ids)-1) + ")"
		for _, id := range ids {
			values = append(values, id)
		}
	}

	orderBy := ""
	if sort == SORT_INDEX {
		orderBy = "ORDER BY SortIndex ASC "
	} else if sort == SORT_NEWEST {
		orderBy = "ORDER BY Modified DESC "
	} else if sort == SORT_OLDEST {
		orderBy = "ORDER BY Modified ASC "
	}

	if limit == 0 || limit > LIMIT_MAX {
		limit = LIMIT_MAX
	}

	limitStmt := "LIMIT ?"
	values = append(values, limit)

	if offset != 0 {
		limitStmt += " OFFSET ?"
		values = append(values, offset)
	}

	countQuery := "SELECT COUNT(1) NumRows FROM BSO " + where + " " + orderBy
	var totalRows int

	if err := tx.QueryRow(countQuery, values...).Scan(&totalRows); err != nil {
		return nil, err
	}

	resultQuery := fmt.Sprintf("%s %s %s %s", query, where, orderBy, limitStmt)
	rows, err := tx.Query(resultQuery, values...)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	bsos := make([]*BSO, 0)
	for rows.Next() {
		b := &BSO{}
		if err := rows.Scan(&b.Id, &b.SortIndex, &b.Payload, &b.Modified, &b.TTL); err != nil {
			return nil, err
		} else {
			bsos = append(bsos, b)
		}
	}

	nextOffset := 0
	more := (totalRows > limit+offset)
	if more {
		nextOffset = offset + limit
	}

	results := &GetResults{
		BSOs:   bsos,
		Total:  totalRows,
		More:   more,
		Offset: nextOffset,
	}

	return results, nil

}

// getBSO is a simpler interface to getBSOs that returns a single BSO
func (d *DB) getBSO(tx *sql.Tx, cId int, bId string) (*BSO, error) {

	b := &BSO{Id: bId}

	query := "SELECT SortIndex, Payload, Modified, TTL FROM BSO WHERE CollectionId=? and Id=?"
	err := tx.QueryRow(query, cId, bId).Scan(&b.SortIndex, &b.Payload, &b.Modified, &b.TTL)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}

		return nil, err
	}

	return b, nil
}

func (d *DB) insertBSO(
	tx *sql.Tx,
	cId int,
	bId string,
	modified int,
	payload string,
	sortIndex int,
	ttl int,
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
		modified, modified+ttl)

	return
}

// updateBSO updates a BSO. Values that are not provided (pointers)
// are not updated in the SQL statement
func (d *DB) updateBSO(
	tx *sql.Tx,
	cId int,
	bId string,
	modified int,
	payload *string,
	sortIndex *int,
	ttl *int,
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
		values[i] = *ttl + modified
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
