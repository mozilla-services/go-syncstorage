package web

import (
	"container/list"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/mostlygeek/go-syncstorage/syncstorage"
	"github.com/pkg/errors"
)

var (
	errElementStopped = errors.New("handler is Stopped")
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type elementState uint8
type poolElement struct {
	sync.Mutex

	uid     string
	handler *SyncUserHandler
}

// handlerPool has a big job. It opens DBs on demand and
// closes them when they haven't been used after a while.
type handlerPool struct {
	sync.Mutex

	base     []string
	elements map[string]*poolElement

	// lru keeps a list with the recently used elements in Front and the
	// oldest in the back
	lru    *list.List
	lrumap map[string]*list.Element // to find *list.Element by key

	// the max size of the pool
	maxPoolSize int
}

func newHandlerPool(basepath string, maxPoolSize int) *handlerPool {

	var path []string

	// support in-memory only sqlite3 databases for testing
	if basepath == ":memory:" {
		path = []string{":memory:"}
	} else {
		basepath, err := filepath.Abs(basepath)
		if err != nil {
			log.WithFields(log.Fields{
				"err": err.Error(),
			}).Panic("Could not create hawk cache")
		}

		path = strings.Split(
			filepath.Clean(basepath),
			string(os.PathSeparator),
		)
	}

	pool := &handlerPool{
		base:        path,
		elements:    make(map[string]*poolElement),
		lru:         list.New(),
		lrumap:      make(map[string]*list.Element),
		maxPoolSize: maxPoolSize,
	}

	return pool
}

func (p *handlerPool) cleanupHandlers(maxClean int) {
	numCleaned := 0
	lruElement := p.lru.Back()
	for lruElement != nil && numCleaned < maxClean {
		element := lruElement.Value.(*poolElement)

		element.handler.StopHTTP()
		next := lruElement.Prev()

		p.Lock()
		p.lru.Remove(lruElement)
		delete(p.lrumap, element.uid)
		delete(p.elements, element.uid)
		p.Unlock()

		lruElement = next
		numCleaned++
	}
}

// stopHandlers stops all handlers from servicing HTTP requests
func (p *handlerPool) stopHandlers() {
	p.cleanupHandlers(p.lru.Len())
}

func (p *handlerPool) getElement(uid string) (*poolElement, error) {
	var (
		element *poolElement
		ok      bool
		dbFile  string
	)

	p.Lock()
	defer p.Unlock()

	if element, ok = p.elements[uid]; !ok {
		if len(p.base) == 1 && p.base[0] == ":memory:" {
			dbFile = ":memory:"
		} else {
			storageDir, filename := p.PathAndFile(uid)

			// create the sub-directory tree if required
			if _, err := os.Stat(storageDir); os.IsNotExist(err) {
				if err := os.MkdirAll(storageDir, 0755); err != nil {
					return nil, errors.Wrap(err, "Could not create datadir")
				}
			}

			// TODO clean the UID of any weird characters, ie: os.PathSeparator
			dbFile = storageDir + string(os.PathSeparator) + filename
		}

		if p.lru.Len() > p.maxPoolSize {
			// nasty, kinda low level locking. Since p.cleanuphandlers also
			// locks, unlock/lock here to avoid deadlocks
			p.Unlock()
			p.cleanupHandlers(1 + p.maxPoolSize/10) // clean up ~10%
			p.Lock()
		}

		db, err := syncstorage.NewDB(dbFile)
		if err != nil {
			return nil, errors.Wrap(err, "Could not create DB")
		}

		element = &poolElement{
			uid:     uid,
			handler: NewSyncUserHandler(uid, db),
		}

		p.elements[uid] = element

		listElement := p.lru.PushFront(element)
		p.lrumap[uid] = listElement
	} else {
		if element.handler.IsStopped() {
			return nil, errElementStopped
		}

		p.lru.MoveToFront(p.lrumap[uid])
	}

	return element, nil
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

func (p *handlerPool) PathAndFile(uid string) (path string, file string) {
	path = string(os.PathSeparator) +
		filepath.Join(
			append(p.base, TwoLevelPath(uid)...)...,
		)

	file = uid + ".db"
	return
}
