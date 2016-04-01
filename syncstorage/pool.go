package syncstorage

import (
	"container/list"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	. "github.com/mostlygeek/go-debug"
)

const (
	// max number of ms pool can be locked in cleanup
	MAX_CLEANUP_TIME = time.Millisecond * 5
)

var (
	pDebug  = Debug("syncstorage:pool")
	pDebugC = Debug("syncstorage:pool:Cleanup")
)

type dbelement struct {
	sync.Mutex
	inUse bool

	uid string
	db  *DB
	// last time this was used
	lastUsed time.Time
}

// used helps keep track if something is using
// this dbelement and won't be eligible for cleanup
func (de *dbelement) used(inUse bool) {
	de.Lock()
	defer de.Unlock()
	de.inUse = inUse
}

type Pool struct {
	sync.Mutex
	base []string

	// max TTL before a DB connection
	// is closed and cleaned up
	ttl time.Duration

	// lru keeps a list of the oldest and the
	// newest. Oldest are at the end, newest at the front
	lru *list.List

	// key based lookup for a db
	dbs map[string]*list.Element

	// used to stop the purge
	stopCh chan struct{}
}

func NewPool(basepath string) (*Pool, error) {
	return NewPoolTime(basepath, time.Minute)
}

func NewPoolTime(basepath string, ttl time.Duration) (*Pool, error) {
	basepath, err := filepath.Abs(basepath)
	if err != nil {
		return nil, err
	}

	path := strings.Split(
		filepath.Clean(basepath),
		string(os.PathSeparator),
	)

	pool := &Pool{
		base: path,
		ttl:  ttl,
		lru:  list.New(),
		dbs:  make(map[string]*list.Element),
	}

	return pool, nil
}

func (p *Pool) PathAndFile(uid string) (path string, file string) {
	path = string(os.PathSeparator) +
		filepath.Join(
			append(p.base, TwoLevelPath(uid)...)...,
		)

	file = uid + ".db"
	return
}

func (p *Pool) getDB(uid string) (*dbelement, error) {
	p.Lock()
	defer p.Unlock()

	// found in cache
	if element, ok := p.dbs[uid]; ok {
		//pDebug("getDB: cache hit")
		p.lru.MoveToFront(element)
		dbel := element.Value.(*dbelement)
		dbel.lastUsed = time.Now()
		return dbel, nil
	}

	storageDir, filename := p.PathAndFile(uid)

	// create the sub-directory tree if required
	if _, err := os.Stat(storageDir); os.IsNotExist(err) {
		//pDebug("Creating directory %s", storageDir)
		if err := os.MkdirAll(storageDir, 0755); err != nil {
			//pDebug("Error creating directory %s, %s", storageDir, err.Error())
			return nil, err
		}
	}

	// TODO clean the UID of any weird characters, ie: os.PathSeparator
	dbFile := storageDir + string(os.PathSeparator) + filename
	db, err := NewDB(dbFile)
	if err != nil {
		return nil, err
	}

	dbel := &dbelement{
		uid:      uid,
		db:       db,
		lastUsed: time.Now(),
	}

	el := p.lru.PushFront(dbel)
	p.dbs[uid] = el
	return dbel, nil
}

// Cleanup removes all entries from the pool that have been
// idle longer than pool.ttl
func (p *Pool) Cleanup() {
	p.Lock()
	defer p.Unlock()

	start := time.Now()
	element := p.lru.Back()

	for {
		if time.Now().Sub(start) > MAX_CLEANUP_TIME {
			return
		}

		if element == nil {
			//pDebugC("empty lru, exit")
			return
		}

		dbel := element.Value.(*dbelement)

		// if an element is in use, do not remove it
		if dbel.inUse {
			//pDebugC("%s in use, skip", dbel.uid)
			element = element.Prev()
			continue
		}

		idleTime := time.Now().Sub(dbel.lastUsed)

		// ever element beyond this point is not
		// eligible for cleanup, exit early
		if idleTime < p.ttl {
			//pDebugC("nothing expired, exit ")
			return
		}

		//pDebugC("Removing %s", dbel.uid)
		// remove the element from the pool

		// need this from element before we remove it
		next := element.Prev()

		p.lru.Remove(element)
		delete(p.dbs, dbel.uid)
		dbel.db.Close()

		element = next
	}
}

// Start a goroutine that cleans up DB entries that
// haven't been used in pool.ttl
func (p *Pool) Start() {
	p.Lock()
	defer p.Unlock()

	if p.stopCh != nil {
		return
	}

	p.stopCh = make(chan struct{})
	go func() {
		for {
			select {
			case <-time.After(p.ttl):
				p.Cleanup()
			case <-p.stopCh:
				return
			}
		}
	}()
}

// Stop the cleanup goroutine
func (p *Pool) Stop() {
	p.Lock()
	defer p.Unlock()
	if p.stopCh != nil {
		close(p.stopCh)
		p.stopCh = nil
	}
}

// =======================================================
// Below implements approximately SyncApi except each
// method takes a `uid string`. This `uid` is used for
// the file path of the sqlite3 files.
// =======================================================
func (p *Pool) LastModified(uid string) (modified int, err error) {
	el, err := p.getDB(uid)
	if err != nil {
		return
	}
	el.used(true)
	defer el.used(false)

	return el.db.LastModified()
}

func (p *Pool) GetCollectionId(uid string, name string) (id int, err error) {
	el, err := p.getDB(uid)
	if err != nil {
		return
	}
	el.used(true)
	defer el.used(false)
	return el.db.GetCollectionId(name)
}

func (p *Pool) GetCollectionModified(uid string, cId int) (modified int, err error) {
	el, err := p.getDB(uid)
	if err != nil {
		return
	}
	el.used(true)
	defer el.used(false)
	return el.db.GetCollectionModified(cId)
}

func (p *Pool) CreateCollection(uid string, name string) (cId int, err error) {
	el, err := p.getDB(uid)
	if err != nil {
		return
	}
	el.used(true)
	defer el.used(false)
	return el.db.CreateCollection(name)
}
func (p *Pool) DeleteCollection(uid string, cId int) (err error) {
	el, err := p.getDB(uid)
	if err != nil {
		return
	}
	el.used(true)
	defer el.used(false)
	return el.db.DeleteCollection(cId)
}
func (p *Pool) TouchCollection(uid string, cId, modified int) (err error) {
	el, err := p.getDB(uid)
	if err != nil {
		return
	}
	el.used(true)
	defer el.used(false)
	return el.db.TouchCollection(cId, modified)
}

func (p *Pool) InfoCollections(uid string) (map[string]int, error) {
	el, err := p.getDB(uid)
	if err != nil {
		return nil, err
	}
	el.used(true)
	defer el.used(false)
	return el.db.InfoCollections()
}
func (p *Pool) InfoQuota(uid string) (used, quota int, err error) {
	el, err := p.getDB(uid)
	if err != nil {
		return
	}
	el.used(true)
	defer el.used(false)
	return el.db.InfoQuota()
}
func (p *Pool) InfoCollectionUsage(uid string) (map[string]int, error) {
	el, err := p.getDB(uid)
	if err != nil {
		return nil, err
	}
	el.used(true)
	defer el.used(false)
	return el.db.InfoCollectionUsage()
}
func (p *Pool) InfoCollectionCounts(uid string) (map[string]int, error) {
	el, err := p.getDB(uid)
	if err != nil {
		return nil, err
	}
	el.used(true)
	defer el.used(false)
	return el.db.InfoCollectionCounts()
}

func (p *Pool) PostBSOs(uid string, cId int, input PostBSOInput) (*PostResults, error) {
	el, err := p.getDB(uid)
	if err != nil {
		return nil, err
	}
	el.used(true)
	defer el.used(false)
	return el.db.PostBSOs(cId, input)
}

func (p *Pool) PutBSO(
	uid string,
	cId int,
	bId string,
	payload *string,
	sortIndex *int,
	ttl *int) (modified int, err error) {

	el, err := p.getDB(uid)

	if err != nil {
		return
	}
	el.used(true)
	defer el.used(false)
	return el.db.PutBSO(cId, bId, payload, sortIndex, ttl)
}

func (p *Pool) GetBSO(uid string, cId int, bId string) (b *BSO, err error) {
	el, err := p.getDB(uid)
	if err != nil {
		return
	}
	el.used(true)
	defer el.used(false)
	return el.db.GetBSO(cId, bId)
}
func (p *Pool) GetBSOs(
	uid string,
	cId int,
	ids []string,
	newer int,
	sort SortType,
	limit int,
	offset int) (r *GetResults, err error) {

	el, err := p.getDB(uid)
	if err != nil {
		return
	}
	el.used(true)
	defer el.used(false)
	return el.db.GetBSOs(cId, ids, newer, sort, limit, offset)
}
func (p *Pool) GetBSOModified(uid string, cId int, bId string) (modified int, err error) {

	el, err := p.getDB(uid)
	if err != nil {
		return
	}
	el.used(true)
	defer el.used(false)
	return el.db.GetBSOModified(cId, bId)
}

func (p *Pool) DeleteBSO(uid string, cId int, bId string) (modified int, err error) {
	el, err := p.getDB(uid)
	if err != nil {
		return
	}
	el.used(true)
	defer el.used(false)
	return el.db.DeleteBSO(cId, bId)
}
func (p *Pool) DeleteBSOs(uid string, cId int, bIds ...string) (modified int, err error) {
	el, err := p.getDB(uid)
	if err != nil {
		return
	}
	el.used(true)
	defer el.used(false)
	return el.db.DeleteBSOs(cId, bIds...)
}

func (p *Pool) PurgeExpired(uid string) (removed int, err error) {
	el, err := p.getDB(uid)
	if err != nil {
		return
	}
	el.used(true)
	defer el.used(false)
	return el.db.PurgeExpired()
}

func (p *Pool) Usage(uid string) (stats *DBPageStats, err error) {
	el, err := p.getDB(uid)
	if err != nil {
		return
	}
	el.used(true)
	defer el.used(false)
	return el.db.Usage()
}
func (p *Pool) Optimize(uid string, thresholdPercent int) (ItHappened bool, err error) {
	el, err := p.getDB(uid)
	if err != nil {
		return
	}
	el.used(true)
	defer el.used(false)
	return el.db.Optimize(thresholdPercent)
}

func (p *Pool) DeleteEverything(uid string) error {
	el, err := p.getDB(uid)
	if err != nil {
		return err
	}
	el.used(true)
	defer el.used(false)
	return el.db.DeleteEverything()
}
