package syncstorage

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	_ "github.com/mattn/go-sqlite3"
	. "github.com/mostlygeek/go-debug"
)

var dbDebug = Debug("syncstorage:db")

var (
	ErrNotFound       = errors.New("Not Found")
	ErrNotImplemented = errors.New("Not Implemented")
	ErrNothingToDo    = errors.New("Nothing to do")

	ErrInvalidBSOId          = errors.New("Invalid BSO Id")
	ErrInvalidCollectionId   = errors.New("Invalid Collection Id")
	ErrInvalidCollectionName = errors.New("Invalid Collection Name")
	ErrInvalidPayload        = errors.New("Invalid Payload")
	ErrInvalidSortIndex      = errors.New("Invalid Sort Index")
	ErrInvalidTTL            = errors.New("Invalid TTL")

	ErrInvalidLimit  = errors.New("Invalid LIMIT")
	ErrInvalidOffset = errors.New("Invalid OFFSET")
	ErrInvalidNewer  = errors.New("Invalid NEWER than")
)

// dbTx allows passing of sql.DB or sql.Tx
type dbTx interface {
	Exec(string, ...interface{}) (sql.Result, error)
	Query(string, ...interface{}) (*sql.Rows, error)
	QueryRow(string, ...interface{}) *sql.Row
}

type SortType int

const (
	SORT_NONE SortType = iota
	SORT_NEWEST
	SORT_OLDEST
	SORT_INDEX

	// The default TTL is to never expire. Use 100 years
	// which should be enough (in milliseconds)
	DEFAULT_BSO_TTL = 100 * 365 * 24 * 60 * 60 * 1000

	STORAGE_LAST_MODIFIED = "Storage Last Modified"
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
	More   bool
	Offset int
}

func (g *GetResults) String() string {
	s := fmt.Sprintf("Total: %d, More: %v, Offset: %d\nBSOs:\n",
		len(g.BSOs), g.More, g.Offset)

	for _, b := range g.BSOs {
		s += fmt.Sprintf("  Id:%s, Modified:%d, SortIndex:%d, TTL:%d, %s\n",
			b.Id, b.Modified, b.SortIndex, b.TTL, b.Payload)
	}

	return s
}

type DBPageStats struct {
	Size  int
	Total int
	Free  int
}

// FreePercent calculates how much of total space is used up by
// free pages (empty of data)
func (s *DBPageStats) FreePercent() int {
	if s.Total == 0 {
		return 0
	}

	return int(float32(s.Free) / float32(s.Total) * 100)
}

type DB struct {
	sync.RWMutex

	// sqlite database path
	Path string

	db *sql.DB
}

type Config struct {
	CacheSize int
}

func (d *DB) OpenWithConfig(conf *Config) (err error) {
	d.db, err = sql.Open("sqlite3", d.Path)

	if err != nil {
		return
	}

	// settings to apply to the database

	pragmas := []string{
		"PRAGMA page_size=4096;",
		"PRAGMA journal_mode=WAL;",
	}

	if conf != nil {
		if log.GetLevel() == log.DebugLevel {
			log.WithFields(log.Fields{
				"cache_size": conf.CacheSize,
			}).Debug("db config")
		}

		pragmas = append(pragmas, fmt.Sprintf("PRAGMA cache_size=%d;", conf.CacheSize))
	}

	for _, p := range pragmas {
		if _, err = d.db.Exec(p); err != nil {
			return errors.Wrapf(err, "Could not set PRAGMA: %s", p)
		}
	}

	var schemaVersion int
	if err := d.db.QueryRow("PRAGMA schema_version;").Scan(&schemaVersion); err != nil {
		return err
	}

	// Initialize a new database with all the current schemas concatenated together
	if schemaVersion == 0 {
		tx, err := d.db.Begin()
		if err != nil {
			return err
		}

		if _, err := tx.Exec(SCHEMA_0 + SCHEMA_1); err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				return rollbackErr
			} else {
				return err
			}
		} else {
			if err := tx.Commit(); err != nil {
				return err
			}
		}
	} else {
		// Migrate schema to the latest version. At the time of this
		// comment there is only SCHEMA_1. Considering the rate of
		// schema change, we can probably just keep it simple yet
		// slightly more verbose using, `if userVersion == ...` statements
		var userVersion int
		if err := d.db.QueryRow("PRAGMA user_version;").Scan(&userVersion); err != nil {
			return err
		}

		// SCHEMA_0 did not alter PRAGMA user_version so it defaults to 0
		if userVersion == 0 {
			tx, err := d.db.Begin()
			if err != nil {
				return err
			}

			if _, err := tx.Exec(SCHEMA_1); err != nil {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					return rollbackErr
				} else {
					return err
				}
			} else {
				if err := tx.Commit(); err != nil {
					return err
				}
			}
		}

		// SCHEMA_1 sets PRAGMA user_version to 2 so the count
		// of schemas applied is caught up and correct.
		// putting this here for posterity and next schema upgrade

		// if userVersion == 2 { ... }
	}

	return nil
}

func (d *DB) Open() (err error) {
	return d.OpenWithConfig(nil)
}

func (d *DB) Close() {
	if d.db != nil {
		dbDebug("Closing: %s", d.Path)
		d.db.Close()
	}
}

func NewDB(path string, conf *Config) (*DB, error) {
	d := &DB{Path: path}

	if err := d.OpenWithConfig(conf); err != nil {
		return nil, err
	}

	return d, nil
}

/*
  The public functions in *DB control locking of the main object
  so we can only have one read or write at a time. The actual database
  work is handled by private functions.
*/

// LastModified returns the top level last modified timestamp
func (d *DB) LastModified() (int, error) {
	d.Lock()
	defer d.Unlock()

	lastMod, err := getKey(d.db, STORAGE_LAST_MODIFIED)
	if lastMod == "" || err != nil {
		return 0, err
	}

	lastModInt64, err := strconv.ParseInt(lastMod, 10, 64)
	if err != nil {
		return 0, errors.Wrap(err, "Failed converting storage timestamp to int. UhOh?!")
	}

	return int(lastModInt64), nil
}

func (d *DB) GetCollectionId(name string) (id int, err error) {
	d.Lock()
	defer d.Unlock()

	// return common collection id without touching the DB
	// ew? yes, but it'll compile nice and fast
	switch name {
	case "clients":
		return 1, nil
	case "crypto":
		return 2, nil
	case "forms":
		return 3, nil
	case "history":
		return 4, nil
	case "keys":
		return 5, nil
	case "meta":
		return 6, nil
	case "bookmarks":
		return 7, nil
	case "prefs":
		return 8, nil
	case "tabs":
		return 9, nil
	case "passwords":
		return 10, nil
	case "addons":
		return 11, nil
	case "addresses":
		return 12, nil
	case "creditcards":
		return 13, nil
	}

	if !CollectionNameOk(name) {
		err = ErrInvalidCollectionName
		return
	}

	err = d.db.QueryRow("SELECT Id FROM Collections where Name=?", name).Scan(&id)

	if err == sql.ErrNoRows {
		err = ErrNotFound
	}

	return
}

func (d *DB) GetCollectionModified(cId int) (modified int, err error) {
	d.Lock()
	defer d.Unlock()
	err = d.db.QueryRow("SELECT modified FROM Collections where Id=?", cId).Scan(&modified)
	if err == sql.ErrNoRows {
		return 0, nil
	}

	return
}

func (d *DB) CreateCollection(name string) (cId int, err error) {
	d.Lock()
	defer d.Unlock()

	if !CollectionNameOk(name) {
		err = ErrInvalidCollectionName
		return
	}

	tx, err := d.db.Begin()
	if err != nil {
		return 0, err
	}

	modified := Now()
	dml := "INSERT INTO Collections (Name, Modified) VALUES (?,?)"

	results, err := tx.Exec(dml, name, modified)
	if err != nil {
		tx.Rollback()
		return 0, err
	}

	cId64, err := results.LastInsertId()
	if err != nil {
		tx.Rollback()
		return 0, err
	}

	tx.Commit()
	return int(cId64), nil
}

func (d *DB) DeleteCollection(cId int) (int, error) {
	d.Lock()
	defer d.Unlock()

	tx, err := d.db.Begin()
	if err != nil {
		return 0, errors.Wrap(err, "Failed creating transaction")
	}

	dmlB := "DELETE FROM BSO WHERE CollectionId=?"
	if _, err := tx.Exec(dmlB, cId); err != nil {
		tx.Rollback()
		return 0, errors.Wrapf(err, "Failed deleting collection: %d", cId)
	}

	if err := d.touchCollection(tx, cId, 0); err != nil {
		tx.Rollback()
		return 0, errors.Wrapf(err, "Failed resetting last modified for collection: %d", cId)
	}

	modified := Now()
	if err := d.touchStorage(tx, modified); err != nil {
		tx.Rollback()
		return 0, errors.Wrapf(err, "Failed setting storage timestamp")
	}

	tx.Commit()
	return modified, nil
}

// DeleteEverything will delete all BSOs, record when everything was deleted
// and vacuum to free up disk pages.
func (d *DB) DeleteEverything() (err error) {
	d.Lock()
	defer d.Unlock()

	// delete all BSO data and keep the other metadata around
	dml := `
		DELETE FROM BSO;
		INSERT OR REPLACE INTO KeyValues (Key, Value) VALUES ("DELETE_EVERYTHING_DATE", ?);
		VACUUM;
		`
	_, err = d.db.Exec(dml, time.Now().Format(time.RFC3339))
	return
}

func (d *DB) TouchCollection(cId, modified int) (err error) {
	d.Lock()
	defer d.Unlock()

	return d.touchCollectionAndStorage(d.db, cId, modified)
}

// InfoCollections create a map of collection names to last modified times
func (d *DB) InfoCollections() (map[string]int, error) {
	d.Lock()
	defer d.Unlock()

	rows, err := d.db.Query("SELECT Name,Modified FROM Collections WHERE Modified != 0")
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

func (d *DB) InfoQuota() (used, quota int, err error) {
	d.Lock()
	defer d.Unlock()

	var u sql.NullInt64

	query := `SELECT sum(PayloadSize) used
			  FROM BSO`

	err = d.db.QueryRow(query).Scan(&u)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, 0, nil
		}

		return
	}

	if u.Valid {
		used = int(u.Int64)
		return
	} else {
		return 0, 0, nil
	}
}

func (d *DB) InfoCollectionUsage() (map[string]int, error) {
	d.Lock()
	defer d.Unlock()

	query := `SELECT c.Name,sum(b.PayloadSize) used
			  FROM BSO b, Collections C
			  WHERE b.CollectionId=c.Id GROUP BY b.CollectionId`

	rows, err := d.db.Query(query)
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	results := make(map[string]int)
	for rows.Next() {
		var name string
		var used int

		if err := rows.Scan(&name, &used); err != nil {
			return nil, err
		}
		results[name] = used
	}

	return results, nil
}

func (d *DB) InfoCollectionCounts() (map[string]int, error) {
	d.Lock()
	defer d.Unlock()

	query := `SELECT c.Name, count(b.Id) count
			  FROM BSO b, Collections C
			  WHERE b.CollectionId=c.Id GROUP BY b.CollectionId`

	rows, err := d.db.Query(query)
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	results := make(map[string]int)
	for rows.Next() {
		var name string
		var count int

		if err := rows.Scan(&name, &count); err != nil {
			return nil, err
		}
		results[name] = count
	}

	return results, nil
}

type PostBSOInput []*PutBSOInput
type PutBSOInput struct {
	Id        string  `json:"id"`
	Payload   *string `json:"payload,omitempty"`
	TTL       *int    `json:"ttl,omitempty"`
	SortIndex *int    `json:"sortindex,omitempty"`
}

func NewPutBSOInput(id string, payload *string, sortIndex, ttl *int) *PutBSOInput {
	if ttl == nil {
		t := DEFAULT_BSO_TTL
		ttl = &t
	}
	return &PutBSOInput{Id: id, TTL: ttl, SortIndex: sortIndex, Payload: payload}
}

func (d *DB) PostBSOs(cId int, input PostBSOInput) (*PostResults, error) {
	d.Lock()
	defer d.Unlock()

	tx, err := d.db.Begin()
	if err != nil {
		return nil, err
	}

	modified := Now() // same modified timestamp for all INSERT/UPDATES
	results := NewPostResults(modified)

	for _, data := range input {
		err := d.putBSO(tx, cId, data.Id, modified, data.Payload, data.SortIndex, data.TTL)
		if err != nil {
			results.AddFailure(data.Id, err.Error())
			continue
		} else {
			results.AddSuccess(data.Id)
		}
	}

	// update the collection
	err = d.touchCollectionAndStorage(tx, cId, modified)
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
	err = d.touchCollectionAndStorage(tx, cId, modified)
	if err != nil {
		tx.Rollback()
		return
	}

	tx.Commit()
	return
}

func (d *DB) GetBSO(cId int, bId string) (b *BSO, err error) {
	d.Lock()
	defer d.Unlock()

	b, err = d.getBSO(d.db, cId, bId)

	return
}

func (d *DB) GetBSOs(
	cId int,
	ids []string,
	older int,
	newer int,

	sort SortType,
	limit int,
	offset int) (r *GetResults, err error) {

	d.Lock()
	defer d.Unlock()

	r, err = d.getBSOs(d.db, cId, ids, older, newer, sort, limit, offset)

	return
}

func (d *DB) GetBSOModified(cId int, bId string) (modified int, err error) {
	d.Lock()
	defer d.Unlock()
	err = d.db.QueryRow(`SELECT modified
						 FROM BSO
						 WHERE CollectionId=? and Id=? and TTL > ?`, cId, bId, Now()).Scan(&modified)

	if err != nil {
		if err == sql.ErrNoRows {
			return 0, ErrNotFound
		}

		return 0, err
	}

	return
}

// DeleteBSO deletes a single BSO and returns the
// modified timestamp for the collection
func (d *DB) DeleteBSO(cId int, bId string) (int, error) {
	return d.DeleteBSOs(cId, bId)
}

// DeleteBSOs deletes multiple BSO. It returns the modified
// timestamp for the collection on success
func (d *DB) DeleteBSOs(cId int, bIds ...string) (modified int, err error) {
	d.Lock()
	defer d.Unlock()

	if log.GetLevel() == log.DebugLevel {
		log.WithFields(log.Fields{
			"cid":  cId,
			"bIds": bIds,
		}).Debug("db DeleteBSOs")
	}

	tx, err := d.db.Begin()
	if err != nil {
		return
	}

	dml := "DELETE FROM BSO WHERE CollectionId=? AND Id IN (?" +
		strings.Repeat(",?", len(bIds)-1) + ")"

	// https://golang.org/doc/faq#convert_slice_of_interface
	ids := make([]interface{}, len(bIds)+1)
	ids[0] = cId
	for i, v := range bIds {
		ids[i+1] = v
	}

	_, err = tx.Exec(dml, ids...)
	if err != nil {
		tx.Rollback()
		return
	}

	modified = Now()

	// update the collection
	err = d.touchCollectionAndStorage(tx, cId, modified)
	if err != nil {
		tx.Rollback()
		return
	}

	tx.Commit()
	return
}

// PurgeExpired removes all BSOs that have expired out
func (d *DB) PurgeExpired() (removed int, err error) {
	d.Lock()
	defer d.Unlock()

	dmlBSO := "DELETE FROM BSO WHERE TTL <= ?"
	r, err := d.db.Exec(dmlBSO, Now())

	if err != nil {
		return 0, err
	}

	purged, err := r.RowsAffected()
	return int(purged), err
}

func (d *DB) Usage() (stats *DBPageStats, err error) {
	d.Lock()
	defer d.Unlock()

	stats = &DBPageStats{}

	err = d.db.QueryRow("PRAGMA page_count").Scan(&stats.Total)
	if err != nil {
		return nil, err
	}

	err = d.db.QueryRow("PRAGMA freelist_count").Scan(&stats.Free)
	if err != nil {
		return nil, err
	}

	err = d.db.QueryRow("PRAGMA page_size").Scan(&stats.Size)
	if err != nil {
		return nil, err
	}

	return
}

// Optimize recovers disk space by removing empty db pages
// if the number of free pages makes up more than `threshold`
// percent of the total pages
func (d *DB) Optimize(thresholdPercent int) (ItHappened bool, err error) {
	stats, err := d.Usage()

	if err != nil {
		return
	}

	if stats.FreePercent() >= thresholdPercent {
		err = d.Vacuum()
		ItHappened = true
	}

	return
}

// SetKey inserts or replaces a key with the value
func (d *DB) SetKey(key, value string) error {
	d.Lock()
	defer d.Unlock()
	return setKey(d.db, key, value)
}

// GetKey returns a previous key in the database
func (d *DB) GetKey(key string) (string, error) {
	d.Lock()
	defer d.Unlock()
	return getKey(d.db, key)
}

func setKey(tx dbTx, key, value string) (err error) {
	_, err = tx.Exec(`INSERT OR REPLACE INTO KeyValues (Key, Value) VALUES (?, ?)`, key, value)
	return
}

func getKey(tx dbTx, key string) (value string, err error) {
	query := "SELECT Value FROM KeyValues WHERE Key=?"
	err = tx.QueryRow(query, key).Scan(&value)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return
}

// Vacuum recovers free disk pages and reduces fragmentation of the
// data on disk. This could take a long time depending on the size
// of the database
func (d *DB) Vacuum() (err error) {
	d.Lock()
	defer d.Unlock()
	_, err = d.db.Exec("VACUUM")
	return
}

// touchCollection updates a collection's last-modified timestamp
func (d *DB) touchCollection(tx dbTx, cId, modified int) (err error) {
	_, err = tx.Exec("UPDATE Collections SET modified=? WHERE Id=?", modified, cId)
	return err
}

// touchStorage updates the storage level last modified timestamp.
// Added in issue #169 where the server didn't properly update
// timestamps on collection delete
func (d *DB) touchStorage(tx dbTx, modified int) error {
	return setKey(tx, STORAGE_LAST_MODIFIED, strconv.Itoa(modified))
}

// touchCollectionAndStorage updates both the storage and a
// collection's last modified timestamp
func (d *DB) touchCollectionAndStorage(tx dbTx, cId, modified int) error {
	if err := d.touchCollection(tx, cId, modified); err != nil {
		return errors.Wrapf(err, "Failed updating timestamp on cId=%d", cId)
	}

	if err := d.touchStorage(tx, modified); err != nil {
		return errors.Wrap(err, "Failed updating storage timestamp")
	}

	return nil
}

// putBSO will INSERT or UPDATE a BSO
func (d *DB) putBSO(tx dbTx,
	cId int,
	bId string,
	modified int,
	payload *string,
	sortIndex *int,
	ttl *int,
) (err error) {
	if payload == nil && sortIndex == nil && ttl == nil {
		err = ErrNothingToDo
		return
	}

	if !BSOIdOk(bId) {
		err = ErrInvalidBSOId
		return
	}

	if sortIndex != nil && !SortIndexOk(*sortIndex) {
		err = ErrInvalidSortIndex
		return
	}

	if ttl != nil && !TTLOk(*ttl) {
		err = ErrInvalidTTL
		return
	}

	exists, err := d.bsoExists(tx, cId, bId)
	if err != nil {
		return
	}

	// Do an UPDATE or an INSERT
	if exists == true {
		var t *int
		if ttl != nil {
			tmp := *ttl
			t = &tmp
		}
		return d.updateBSO(tx, cId, bId, modified, payload, sortIndex, t)
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
func (d *DB) bsoExists(tx dbTx, cId int, bId string) (bool, error) {
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
	tx dbTx,
	cId int,
	ids []string,
	older int,
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

	cutOffTTL := Now()
	query := "SELECT Id, SortIndex, Payload, Modified, TTL FROM BSO "
	where := "WHERE CollectionId=? AND Modified < ? AND Modified > ? AND TTL > ?"
	values := []interface{}{cId, older, newer, cutOffTTL}

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
		orderBy = "ORDER BY SortIndex DESC "
	} else if sort == SORT_NEWEST {
		orderBy = "ORDER BY Modified DESC "
	} else if sort == SORT_OLDEST {
		orderBy = "ORDER BY Modified ASC "
	}

	limitStmt := "LIMIT ?"

	// fetch an extra row to detect if there are more
	// rows that match the query conditions
	if limit >= 0 {
		values = append(values, limit+1)
	} else {
		values = append(values, limit)
	}

	if offset != 0 {
		limitStmt += " OFFSET ?"
		values = append(values, offset)
	}

	resultQuery := fmt.Sprintf("%s %s %s %s", query, where, orderBy, limitStmt)
	rows, err := tx.Query(resultQuery, values...)
	defer rows.Close()

	if log.GetLevel() == log.DebugLevel {
		log.WithFields(log.Fields{
			"query":  resultQuery,
			"values": values,
		}).Debug("db getBSOs")
	}

	if err != nil {
		return nil, err
	}

	bsos := make([]*BSO, 0)
	for rows.Next() {
		b := &BSO{}
		if err := rows.Scan(&b.Id, &b.SortIndex, &b.Payload, &b.Modified, &b.TTL); err != nil {
			return nil, err
		} else {
			bsos = append(bsos, b)
		}
	}

	var more bool
	var nextOffset int
	num := len(bsos)
	if limit >= 0 && num > limit {
		bsos = bsos[:num-1]
		more = true
		nextOffset = limit + offset
	}

	results := &GetResults{
		BSOs:   bsos,
		More:   more,
		Offset: nextOffset,
	}

	return results, nil

}

// getBSO is a simpler interface to getBSOs that returns a single BSO
func (d *DB) getBSO(tx dbTx, cId int, bId string) (*BSO, error) {

	if !BSOIdOk(bId) {
		return nil, ErrInvalidBSOId
	}

	b := &BSO{Id: bId}

	query := "SELECT SortIndex, Payload, Modified, TTL FROM BSO WHERE CollectionId=? and Id=? and TTL >= ?"
	err := tx.QueryRow(query, cId, bId, Now()).Scan(&b.SortIndex, &b.Payload, &b.Modified, &b.TTL)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrNotFound
		}

		return nil, err
	}

	return b, nil
}

func (d *DB) insertBSO(
	tx dbTx,
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

	if log.GetLevel() == log.DebugLevel {
		log.WithFields(log.Fields{
			"cId":       cId,
			"bId":       bId,
			"sortIndex": sortIndex,
			"payload":   payload,
			"modified":  modified,
			"ttl_db":    modified + ttl,
			"ttl":       ttl,
		}).Debug("db insertBSO")
	}

	return
}

// updateBSO updates a BSO. Values that are not provided (pointers)
// are not updated in the SQL statement
func (d *DB) updateBSO(
	tx dbTx,
	cId int,
	bId string,
	modified int,
	payload *string,
	sortIndex *int,
	ttl *int,
) (err error) {
	if payload == nil && sortIndex == nil && ttl == nil {
		err = ErrNothingToDo
		return
	}

	var values = make([]interface{}, 7)
	i := 0
	set := ""

	// The modified time is *ONLY* changed if the
	// payload or the sortIndex changes.
	if payload != nil || sortIndex != nil {
		set = "modified=?"
		values[i] = modified
		i += 1
	}

	if payload != nil {
		if i != 0 {
			set = set + ","
		}
		set = set + "Payload=?, PayloadSize=?"
		values[i] = *payload
		i += 1
		values[i] = len(*payload)
		i += 1
	}

	if sortIndex != nil {
		if i != 0 {
			set = set + ","
		}
		set = set + "SortIndex=?"
		values[i] = *sortIndex
		i += 1
	}

	if ttl != nil {
		if i != 0 {
			set = set + ","
		}
		set = set + "TTL=?"
		values[i] = *ttl + modified
		i += 1
	}

	// build the DML based on the data we have
	values[i] = cId
	i += 1
	values[i] = bId
	i += 1

	if log.GetLevel() == log.DebugLevel {
		dPayload := "<nil>"
		if payload != nil {
			dPayload = *payload
		}

		dSortIndex := "<nil>"
		if sortIndex != nil {
			dSortIndex = strconv.Itoa(*sortIndex)
		}

		dTTL := "<nil>"
		if ttl != nil {
			dTTL = strconv.Itoa(*ttl)
		}

		log.WithFields(log.Fields{
			"cId":       cId,
			"bId":       bId,
			"sortIndex": dSortIndex,
			"payload":   dPayload,
			"modified":  modified,
			"ttl":       dTTL,
			"zz_set":    set,
		}).Debug("db updateBSO")
	}

	dml := "UPDATE BSO SET " + set + " WHERE CollectionId=? and Id=?"

	_, err = tx.Exec(dml, values[0:i]...)

	if err != nil {
		return
	}

	return
}
