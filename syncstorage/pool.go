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
	pDebug      = Debug("syncstorage:pool")
	pDebugCache = Debug("syncstorage:pool:cache")

	ErrPoolUnexpectedType = errors.New("Unexpected Type from cache")
)

// Using Sqlite databases we have to open a file for
// each user. The pool allows us to keep a limited number
// of files open so we can serve many users without
// running into system Too Many Files errors

type Pool struct {
	sync.Mutex
	basePath []string
	pathfunc PathMaker

	cache *lru.Cache

	// inout stores when a *DB has been lent out and
	// when it has been returned
	locks map[string]*sync.Mutex
}

func NewPool(basepath string, p PathMaker) (*Pool, error) {
	return NewPoolCacheSize(basepath, p, 25)
}

func NewPoolCacheSize(basepath string, p PathMaker, cacheSize int) (*Pool, error) {
	if p == nil {
		p = DefaultPathMaker
	}

	basepath, err := filepath.Abs(basepath)
	if err != nil {
		return nil, err
	}

	path := strings.Split(filepath.Clean(basepath), string(os.PathSeparator))

	// make the pool first
	pool := &Pool{
		basePath: path,
		pathfunc: p,
		cache:    nil,
		locks:    make(map[string]*sync.Mutex),
	}

	onevict := func(k interface{}, v interface{}) {
		key, ok := k.(string)
		if !ok {
			pDebugCache("Evict error, not a string key")
			return
		}

		pool.Lock()
		// when a cache item is evicted from the pool it should be removed
		delete(pool.locks, key)
		pool.Unlock()

		pDebugCache("Evicted %s", key)
	}

	cache, err := lru.NewWithEvict(cacheSize, onevict)
	if err != nil {
		return nil, err
	}

	pool.cache = cache
	return pool, nil

}

func (p *Pool) PathAndFile(uid string) (path string, file string) {
	path = string(os.PathSeparator) + filepath.Join(append(p.basePath, p.pathfunc(uid)...)...)
	file = uid + ".db"
	return
}

func (p *Pool) borrowdb(uid string) (*DB, error) {
	p.Lock()
	if p.locks[uid] == nil {
		p.locks[uid] = &sync.Mutex{}
	}
	p.Unlock()

	p.locks[uid].Lock()
	var db *DB
	var ok bool

	dbx, ok := p.cache.Get(uid)

	if !ok {
		pDebugCache("Miss %s", uid)
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

		pDebugCache("Add %s", uid)
		p.cache.Add(uid, db)

		return db, nil
	} else {
		pDebugCache("Hit %s", uid)
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
	db, err := p.borrowdb(uid)
	defer p.returndb(uid)
	if err != nil {
		return
	}
	return db.GetCollectionId(name)
}

func (p *Pool) GetCollectionModified(uid string, cId int) (modified int, err error) {
	db, err := p.borrowdb(uid)
	defer p.returndb(uid)
	if err != nil {
		return
	}
	return db.GetCollectionModified(cId)
}

func (p *Pool) CreateCollection(uid string, name string) (cId int, err error) {
	db, err := p.borrowdb(uid)
	defer p.returndb(uid)
	if err != nil {
		return
	}
	return db.CreateCollection(name)
}
func (p *Pool) DeleteCollection(uid string, cId int) (err error) {
	db, err := p.borrowdb(uid)
	defer p.returndb(uid)
	if err != nil {
		return
	}
	return db.DeleteCollection(cId)
}
func (p *Pool) TouchCollection(uid string, cId, modified int) (err error) {
	db, err := p.borrowdb(uid)
	defer p.returndb(uid)
	if err != nil {
		return
	}
	return db.TouchCollection(cId, modified)
}

func (p *Pool) InfoCollections(uid string) (map[string]int, error) {
	db, err := p.borrowdb(uid)
	defer p.returndb(uid)
	if err != nil {
		return nil, err
	}
	return db.InfoCollections()
}
func (p *Pool) InfoQuota(uid string) (used, quota int, err error) {
	db, err := p.borrowdb(uid)
	defer p.returndb(uid)
	if err != nil {
		return
	}
	return db.InfoQuota()
}
func (p *Pool) InfoCollectionUsage(uid string) (map[string]int, error) {
	db, err := p.borrowdb(uid)
	defer p.returndb(uid)
	if err != nil {
		return nil, err
	}
	return db.InfoCollectionUsage()
}
func (p *Pool) InfoCollectionCounts(uid string) (map[string]int, error) {
	db, err := p.borrowdb(uid)
	defer p.returndb(uid)
	if err != nil {
		return nil, err
	}
	return db.InfoCollectionCounts()
}

func (p *Pool) PostBSOs(uid string, cId int, input PostBSOInput) (*PostResults, error) {
	db, err := p.borrowdb(uid)
	defer p.returndb(uid)
	if err != nil {
		return nil, err
	}
	return db.PostBSOs(cId, input)
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
func (p *Pool) GetBSOs(
	uid string,
	cId int,
	ids []string,
	newer int,
	sort SortType,
	limit int,
	offset int) (r *GetResults, err error) {

	db, err := p.borrowdb(uid)
	defer p.returndb(uid)
	if err != nil {
		return
	}

	return db.GetBSOs(cId, ids, newer, sort, limit, offset)
}

func (p *Pool) DeleteBSO(uid string, cId int, bId string) (modified int, err error) {
	db, err := p.borrowdb(uid)
	defer p.returndb(uid)
	if err != nil {
		return
	}
	return db.DeleteBSO(cId, bId)
}
func (p *Pool) DeleteBSOs(uid string, cId int, bIds ...string) (modified int, err error) {
	db, err := p.borrowdb(uid)
	defer p.returndb(uid)
	if err != nil {
		return
	}
	return db.DeleteBSOs(cId, bIds...)
}

func (p *Pool) PurgeExpired(uid string) (removed int, err error) {
	db, err := p.borrowdb(uid)
	defer p.returndb(uid)
	if err != nil {
		return
	}
	return db.PurgeExpired()
}

func (p *Pool) Usage(uid string) (stats *DBPageStats, err error) {
	db, err := p.borrowdb(uid)
	defer p.returndb(uid)
	if err != nil {
		return
	}
	return db.Usage()
}
func (p *Pool) Optimize(uid string, thresholdPercent int) (ItHappened bool, err error) {
	db, err := p.borrowdb(uid)
	defer p.returndb(uid)
	if err != nil {
		return
	}
	return db.Optimize(thresholdPercent)
}
