package syncstorage

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/hashicorp/golang-lru"
	. "github.com/tj/go-debug"
)

var (
	pDebug = Debug("syncstorage:pool")

	ErrPoolUnexpectedType = errors.New("Unexpected Type from cache")
)

// Using Sqlite databases we have to open a file for
// each user. The pool allows us to keep a limited number
// of files open so we can serve many users without
// running into system Too Many Files errors

type Pool struct {
	basePath []string
	pathfunc PathMaker

	cache *lru.Cache

	// inout stores when a *DB has been lent out and
	// when it has been returned
	locks map[string]*sync.Mutex
}

func NewPool(basepath string, p PathMaker) (*Pool, error) {

	if p == nil {
		p = DefaultPathMaker
	}

	basepath, err := filepath.Abs(basepath)
	if err != nil {
		return nil, err
	}

	path := strings.Split(filepath.Clean(basepath), string(os.PathSeparator))

	cache, _ := lru.New(25)

	return &Pool{
		basePath: path,
		pathfunc: p,
		cache:    cache,
		locks:    make(map[string]*sync.Mutex),
	}, nil

}

func (p *Pool) PathAndFile(uid string) (path string, file string) {
	path = string(os.PathSeparator) + filepath.Join(append(p.basePath, p.pathfunc(uid)...)...)
	file = uid + ".db"
	return
}

func (p *Pool) borrowdb(uid string) (*DB, error) {
	if p.locks[uid] == nil {
		p.locks[uid] = &sync.Mutex{}
	}

	p.locks[uid].Lock()
	var db *DB
	var ok bool

	dbx, ok := p.cache.Get(uid)

	if !ok {
		pDebug("Cache - Miss %s", uid)
		storageDir, filename := p.PathAndFile(uid)

		// create the sub-directory tree if required
		if _, err := os.Stat(storageDir); os.IsNotExist(err) {
			pDebug("Creating directory %s", storageDir)
			if err := os.MkdirAll(storageDir, 0755); err != nil {
				pDebug("Error creating directory %s, %s", storageDir, err.Error())
				return nil, err
			}
		}

		// TODO clean the UID of any weird characters, ie: os.PathSeparator
		dbFile := storageDir + string(os.PathSeparator) + filename
		pDebug("Creating DB %s", dbFile)
		db, err := NewDB(dbFile)
		if err != nil {
			return nil, err
		}

		pDebug("Cache - Add + Return %s", uid)
		p.cache.Add(uid, db)

		return db, nil
	} else {
		pDebug("Cache - Hit + Return %s", uid)
		db, ok = dbx.(*DB)
		if !ok {
			// TODO remove this, or log it?
			return nil, ErrPoolUnexpectedType
		}

		return db, nil
	}
}

func (p *Pool) returndb(uid string) {
	p.locks[uid].Unlock()
}

// =======================================================
// Below implements the public API of *DB, except each
// method takes a `uid string`. This `uid` is used for
// the file path of the sqlite3 files.
// =======================================================

func (p *Pool) GetCollectionId(uid string, name string) (id int, err error) {
	return 0, ErrNotImplemented
}

func (p *Pool) GetCollectionModified(uid string, cId int) (modified int, err error) {
	return 0, ErrNotImplemented
}

func (p *Pool) CreateCollection(uid string, name string) (cId int, err error) {
	return 0, ErrNotImplemented
}
func (p *Pool) DeleteCollection(uid string, cId int) (err error) {
	return ErrNotImplemented
}
func (p *Pool) TouchCollection(cId, modified int) (err error) {
	return ErrNotImplemented
}

func (p *Pool) InfoCollections(uid string) (map[string]int, error) {
	return nil, ErrNotImplemented
}
func (p *Pool) InfoQuota(uid string) (used, quota int, err error) {
	return 0, 0, ErrNotImplemented
}
func (p *Pool) InfoCollectionUsage(uid string) (map[string]int, error) {
	return nil, ErrNotImplemented
}
func (p *Pool) InfoCollectionCounts(uid string) (map[string]int, error) {
	return nil, ErrNotImplemented
}

func (p *Pool) PostBSOs(uid string, cId int, input PostBSOInput) (*PostResults, error) {
	return nil, ErrNotImplemented
}

func (p *Pool) PutBSO(
	uid string,
	cId int,
	bId string,
	payload *string,
	sortIndex *int,
	ttl *int) (modified int, err error) {

	db, err := p.borrowdb(uid)
	defer p.returndb(uid)

	if err != nil {
		return
	}

	return db.PutBSO(cId, bId, payload, sortIndex, ttl)
}

func (p *Pool) GetBSO(uid string, cId int, bId string) (b *BSO, err error) {
	db, err := p.borrowdb(uid)
	defer p.returndb(uid)

	if err != nil {
		return
	}

	return db.GetBSO(cId, bId)
}
func (p *Pool) GetBSOs(uid string) (r *GetResults, err error) {
	return nil, ErrNotImplemented
}

func (p *Pool) DeleteBSO(uid string, cId, bId int) (modified int, err error) {
	return 0, ErrNotImplemented
}
func (p *Pool) DeleteBSOs(uid string, cId int, bIds ...string) (modified int, err error) {
	return 0, ErrNotImplemented
}

func (p *Pool) PurgeExpired(uid string) (int, error) {
	return 0, ErrNotImplemented
}

func (p *Pool) Usage(uid string) (stats *DBPageStats, err error) {
	return nil, ErrNotImplemented
}
func (p *Pool) Optimize(uid string, thresholdPercent int) (ItHappened bool, err error) {
	return false, ErrNotImplemented
}
