package storage

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
	. "github.com/tj/go-debug"
)

var debug = Debug("storage")

var (
	ErrBaseDoesNotExist = errors.New("storage base directory does not exist")

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

	ErrTooBusy = errors.New("Timeout waiting for DB lock")

	storageBase = "/tmp"
)

func init() {
	SetStorageBase(os.TempDir())
}

// SetStorageBase sets the base directory to save
// database files.
func direxists(path string) bool {

	dir, err := os.Stat(path)
	if err == nil {
		return dir.IsDir()
	} else {
		return false
	}

}

func GetStorageBase() string { return storageBase }
func SetStorageBase(path string) (err error) {
	path = filepath.Clean(path)
	path, err = filepath.Abs(path)
	if err != nil {
		return err
	}

	if !direxists(path) {
		return ErrBaseDoesNotExist
	}

	storageBase = path
	return
}

// Subdir creates a directory structure starting at storageBase
// since storage was design to handle hundreds of thousands of users
// on a single box, it will take a uid like 1000123456789 and
// create a subdirectory like {storageBase}/98/76
// by reversing the order of uid we can ensure an more even distribution
// in the sub-directories.
//
// The number of files in a directory aren't a limit in modern file systems
// but regular POSIX CLI tools sort of blow up when dealing with many thousands
// of files
func Subdir(uid string) (s string) {
	c := 0
	for i := len(uid) - 1; i >= 0; i-- {
		s = s + uid[i:i+1]

		c += 1
		// add a path separator
		if c == 2 && i != 0 {

			// just stop if we can't make another level
			if i < 2 {
				break
			}

			s = s + string(os.PathSeparator)
		}

		// we're done for a 2 level path
		if c == 4 {
			break
		}
	}

	if len(s) < 2 {
		return ""
	}

	return
}

func AbsdirPath(uid string) string {
	if storageBase[len(storageBase)-1:] != string(os.PathSeparator) {
		return storageBase + string(os.PathSeparator) + Subdir(uid)
	} else {
		return storageBase + Subdir(uid)
	}
}
func AbsDBPath(uid string) string {
	return AbsdirPath(uid) + string(os.PathSeparator) + uid + ".db"
}

func MakeSubdir(uid string) (made bool, err error) {
	path := AbsdirPath(uid)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return true, os.MkdirAll(AbsdirPath(uid), 0755)
	}

	return
}

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

func (p *PostResults) AddSuccess(bId ...string) {
	p.Success = append(p.Success, bId...)
}
func (p *PostResults) AddFailure(bId string, reasons ...string) {
	p.Failed[bId] = reasons
}
func NewPostResults(modified int) *PostResults {
	return &PostResults{
		Modified: modified,
		Success:  make([]string, 0),
		Failed:   make(map[string][]string),
	}
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

// getDB opens up the
func getDB(uid string) (db *sql.DB, err error) {

	_, err = MakeSubdir(uid)
	if err != nil {
		return
	}

	dbPath := AbsDBPath(uid)

	// busy timeout set to 250ms, in case there is another writer
	db, err = sql.Open("sqlite3", dbPath+"?_txlock=immediate&_busy_timeout=1000")

	tx, err := beginRetry(db)
	if err != nil {
		return nil, err
	}

	check := "SELECT name from sqlite_master WHERE type='table' AND name=?"
	var name string

	// if not initialized, then create the necessary tables
	if err = tx.QueryRow(check, "BSO").Scan(&name); err == sql.ErrNoRows {
		debug("Initializing Database for %s", dbPath)
		dml := `
			CREATE TABLE BSO (
			  CollectionId	 INTEGER NOT NULL,
			  Id             VARCHAR(64) NOT NULL,

			  SortIndex      INTEGER DEFAULT 0,

			  Payload        TEXT NOT NULL DEFAULT '',
			  PayloadSize    INTEGER NOT NULL DEFAULT 0,

			  -- milliseconds since unix epoch. Sync 1.5 spec says it shoud
			  -- be a float of seconds since epoch accurate to two decimal places
			  -- convert it in the API response, but work with it as an int
			  Modified       INTEGER NOT NULL,

			  TTL            INTEGER NOT NULL,

			  PRIMARY KEY (CollectionId, Id)
			);

			CREATE TABLE Collections (
			  -- storage an integer to save some space
			  Id		INTEGER PRIMARY KEY ASC,
			  Name      VARCHAR(32) UNIQUE,

			  Modified  INTEGER NOT NULL DEFAULT 0
			);

			INSERT INTO Collections (Id, Name) VALUES
				(1,  "bookmarks"),
				(2,  "history"),
				(3,  "forms"),
				(4,  "prefs"),
				(5,  "tabs"),
				(6,  "passwords"),
				(7,  "crypto"),
				(8,  "client"),
				(9,  "keys"),
				(10, "meta");
		`

		_, err = tx.Exec(dml)
		if err != nil {
			err2 := tx.Rollback()
			debug("hmm %v %v", err, err2)
			return nil, err
		} else {
			err = tx.Commit()
			if err != nil {
				debug("could not commit %s", err.Error())
				return nil, err
			}

			debug("Initialized new db")

			return db, nil
		}
	} else {
		tx.Rollback()
		return db, nil
	}

	debug("DB tables already initialized")
	return
}

// beginRetry will create a transaction, but if the database
// is locked due to another writer it will wait and retry
// up to 10 seconds
func beginRetry(db *sql.DB) (tx *sql.Tx, err error) {
	return beginRetryTimeout(db, 10*time.Second)
}

func beginRetryTimeout(db *sql.DB, t time.Duration) (tx *sql.Tx, err error) {
	abort := time.Now().Add(t)
	for {
		if time.Now().After(abort) {
			return nil, ErrTooBusy
		}

		debug("BEGIN")
		tx, err = db.Begin()
		debug("GOT tx")

		if err != nil {
			return
		}

		// too busy, something else has an open transaction
		if err != nil {
			if e, ok := err.(sqlite3.Error); ok {
				debug("hmm1")
				if e.Code == sqlite3.ErrBusy {
					debug("hmm2")
					continue
				}
			} else {
				return nil, err
			}
		} else {
			break
		}
	}

	return
}

// ****************************************************************************
// Public API for accessing Sync Storage
// ****************************************************************************
/*
func GetCollectionId(uid string, name string) (id int, err error) {

}

func GetCollectionModified(uid string, cId int) (modified int, err error) {}

func CreateCollection(uid string, name string) (cId int, err error) {}
func DeleteCollection(uid string, cId int) (err error) {
}
func TouchCollection(uid string, cId, modified int) (err error) {
}

func InfoCollections(uid string) (map[string]int, error) {
}
func InfoQuota(uid string) (used, quota int, err error) {
}
func InfoCollectionUsage(uid string) (map[string]int, error) {
}
func InfoCollectionCounts(uid string) (map[string]int, error) {
}

func PostBSOs(uid string, cId int, input PostBSOInput) (*PostResults, error) {
}

func PutBSO(
	uid string,
	cId int,
	bId string,
	payload *string,
	sortIndex *int,
	ttl *int) (modified int, err error) {

}

func GetBSO(uid string, cId int, bId string) (b *BSO, err error) {}
func GetBSOs(
	uid string,
	cId int,
	ids []string,
	newer int,
	sort SortType,
	limit int,
	offset int) (r *GetResults, err error) {
}

func DeleteBSO(uid string, cId int, bId string) (modified int, err error) {

}
func DeleteBSOs(uid string, cId int, bIds ...string) (modified int, err error) {
}

func PurgeExpired(uid string) (removed int, err error) {
}

func Usage(uid string) (stats *DBPageStats, err error) {
}
func Optimize(uid string, thresholdPercent int) (ItHappened bool, err error) {
}

*/
