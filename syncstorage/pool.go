package syncstorage

import (
	"container/list"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	. "github.com/mostlygeek/go-debug"
)

var (
	pDebug  = Debug("syncstorage:pool")
	pDebugC = Debug("syncstorage:pool:Cleanup")
)

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

	// used to queue uids to purge deleted records
	purgeCh chan *dbelement

	// used to stop the purge
	stopCh chan struct{}
	stopWG sync.WaitGroup
}

func NewPool(basepath string) (*Pool, error) {
	return NewPoolTime(basepath, 5*time.Minute)
}

func NewPoolTime(basepath string, ttl time.Duration) (*Pool, error) {

	var path []string

	// support in-memory only sqlite3 databases for testing
	if basepath == ":memory:" {
		path = []string{":memory:"}
	} else {
		basepath, err := filepath.Abs(basepath)
		if err != nil {
			return nil, err
		}

		path = strings.Split(
			filepath.Clean(basepath),
			string(os.PathSeparator),
		)
	}

	pool := &Pool{
		base:    path,
		ttl:     ttl,
		lru:     list.New(),
		dbs:     make(map[string]*list.Element),
		purgeCh: make(chan *dbelement, 100), // why 100? just an arbitrary pick for version 1
	}

	return pool, nil
}

// TwoLevelPath creates a reverse sub-directory path structure
// e.g. uid:123456 => DATA_ROOT/65/43/123456.db
func TwoLevelPath(uid string) []string {
	l := len(uid)
	switch {
	case l >= 4:
		return []string{
			uid[l-1:l] + uid[l-2:l-1],
			uid[l-3:l-2] + uid[l-4:l-3],
		}
	case l >= 2:
		return []string{
			uid[l-1:l] + uid[l-2:l-1],
		}
	default:
		return []string{}
	}
}

func (p *Pool) PathAndFile(uid string) (path string, file string) {
	path = string(os.PathSeparator) +
		filepath.Join(
			append(p.base, TwoLevelPath(uid)...)...,
		)

	file = uid + ".db"
	return
}

// getDB returns the db or creates a new one. the db is moved to the front of the
// lru list to mean to most most recent
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

	var dbFile string
	if len(p.base) == 1 && p.base[0] == ":memory:" {
		dbFile = ":memory:"
	} else {
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
		dbFile = storageDir + string(os.PathSeparator) + filename
	}

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
func (p *Pool) cleanup() {
	p.Lock()
	defer p.Unlock()

	element := p.lru.Back()

	for {
		if element == nil {
			//pDebugC("empty lru, exit")
			return
		}

		dbel := element.Value.(*dbelement)

		// if an element is in use, do not remove it
		if dbel.InUse() {
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

		// remove the element from the pool

		next := element.Prev()

		if dbel.BeenPurged() {
			// need this from element before we remove it
			p.lru.Remove(element)
			delete(p.dbs, dbel.uid)
			dbel.db.Close()
		} else {
			if p.purgeCh != nil {
				p.purgeCh <- dbel
			}
		}

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

	// for waiting til all goroutines to start
	var startWG sync.WaitGroup

	p.stopCh = make(chan struct{})
	startWG.Add(1)
	p.stopWG.Add(1)
	go func() {
		startWG.Done()
		defer p.stopWG.Done()
		for {
			select {
			case <-time.After(p.ttl):
				p.cleanup()
			case <-p.stopCh:
				return
			}
		}
	}()

	// run a goroutine to purge expired records from the database
	startWG.Add(1)
	p.stopWG.Add(1)
	go func() {
		startWG.Done()
		defer p.stopWG.Done()
		for {
			select {
			case <-p.stopCh:
				return
			case dbel := <-p.purgeCh:
				if dbel.BeenPurged() {
					continue
				}

				results, err := dbel.purge()

				if err != nil {
					log.WithFields(log.Fields{
						"uid": dbel.uid,
						"err": errors.Cause(err).Error(),
					}).Errorf("Purge Error for %s, %s", dbel.uid, err.Error())
					continue
				}

				log.WithFields(results.Fields()).Infof("Purged Expired BSOs for uid:%s", dbel.uid)
			}
		}
	}()

	startWG.Wait()
}

// Stop the cleanup goroutine
func (p *Pool) Stop() {
	p.Lock()
	defer p.Unlock()

	if p.stopCh != nil {
		close(p.stopCh)
		p.stopCh = nil

		p.stopWG.Wait()
		close(p.purgeCh)
		p.purgeCh = nil
	}
}

// Shutdown contains logic to cleanup the pool when the server
// is shutting down
func (p *Pool) Shutdown() {
	// stop the cleanup goroutine
	p.Stop()

	p.Lock()
	defer p.Unlock()

	element := p.lru.Back()
	for {
		if element == nil {
			return
		}

		dbel := element.Value.(*dbelement)
		pDebugC("Closing: %s", dbel.db.Path)
		dbel.db.Close()
		element = element.Prev()
	}
}

func (p *Pool) Use(uid string) error {
	el, err := p.getDB(uid)
	if err != nil {
		return err
	}

	el.Use()
	return nil
}

func (p *Pool) Release(uid string) error {
	el, err := p.getDB(uid)
	if err != nil {
		return err
	}

	el.Release()
	return nil
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
	return el.db.LastModified()
}

func (p *Pool) GetCollectionId(uid string, name string) (id int, err error) {
	el, err := p.getDB(uid)
	if err != nil {
		return
	}
	return el.db.GetCollectionId(name)
}

func (p *Pool) GetCollectionModified(uid string, cId int) (modified int, err error) {
	el, err := p.getDB(uid)
	if err != nil {
		return
	}
	return el.db.GetCollectionModified(cId)
}

func (p *Pool) CreateCollection(uid string, name string) (cId int, err error) {
	el, err := p.getDB(uid)
	if err != nil {
		return
	}
	return el.db.CreateCollection(name)
}
func (p *Pool) DeleteCollection(uid string, cId int) (err error) {
	el, err := p.getDB(uid)
	if err != nil {
		return
	}
	return el.db.DeleteCollection(cId)
}
func (p *Pool) TouchCollection(uid string, cId, modified int) (err error) {
	el, err := p.getDB(uid)
	if err != nil {
		return
	}
	return el.db.TouchCollection(cId, modified)
}

func (p *Pool) InfoCollections(uid string) (map[string]int, error) {
	el, err := p.getDB(uid)
	if err != nil {
		return nil, err
	}
	return el.db.InfoCollections()
}
func (p *Pool) InfoQuota(uid string) (used, quota int, err error) {
	el, err := p.getDB(uid)
	if err != nil {
		return
	}
	return el.db.InfoQuota()
}
func (p *Pool) InfoCollectionUsage(uid string) (map[string]int, error) {
	el, err := p.getDB(uid)
	if err != nil {
		return nil, err
	}
	return el.db.InfoCollectionUsage()
}
func (p *Pool) InfoCollectionCounts(uid string) (map[string]int, error) {
	el, err := p.getDB(uid)
	if err != nil {
		return nil, err
	}
	return el.db.InfoCollectionCounts()
}

func (p *Pool) PostBSOs(uid string, cId int, input PostBSOInput) (*PostResults, error) {
	el, err := p.getDB(uid)
	if err != nil {
		return nil, err
	}
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
	return el.db.PutBSO(cId, bId, payload, sortIndex, ttl)
}

func (p *Pool) GetBSO(uid string, cId int, bId string) (b *BSO, err error) {
	el, err := p.getDB(uid)
	if err != nil {
		return
	}
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
	return el.db.GetBSOs(cId, ids, newer, sort, limit, offset)
}
func (p *Pool) GetBSOModified(uid string, cId int, bId string) (modified int, err error) {

	el, err := p.getDB(uid)
	if err != nil {
		return
	}
	return el.db.GetBSOModified(cId, bId)
}

func (p *Pool) DeleteBSO(uid string, cId int, bId string) (modified int, err error) {
	el, err := p.getDB(uid)
	if err != nil {
		return
	}
	return el.db.DeleteBSO(cId, bId)
}
func (p *Pool) DeleteBSOs(uid string, cId int, bIds ...string) (modified int, err error) {
	el, err := p.getDB(uid)
	if err != nil {
		return
	}
	return el.db.DeleteBSOs(cId, bIds...)
}

func (p *Pool) PurgeExpired(uid string) (removed int, err error) {
	el, err := p.getDB(uid)
	if err != nil {
		return
	}
	return el.db.PurgeExpired()
}

func (p *Pool) Usage(uid string) (stats *DBPageStats, err error) {
	el, err := p.getDB(uid)
	if err != nil {
		return
	}
	return el.db.Usage()
}
func (p *Pool) Optimize(uid string, thresholdPercent int) (ItHappened bool, err error) {
	el, err := p.getDB(uid)
	if err != nil {
		return
	}
	return el.db.Optimize(thresholdPercent)
}

func (p *Pool) DeleteEverything(uid string) error {
	el, err := p.getDB(uid)
	if err != nil {
		return err
	}
	return el.db.DeleteEverything()
}
