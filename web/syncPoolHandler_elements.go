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

	uid       string
	handler   *SyncUserHandler
	lastTouch time.Time
}

// touch updates the last time the poolElement was used
func (p *poolElement) touch() {
	p.Lock()
	defer p.Unlock()
	p.lastTouch = time.Now()
}

// lastUsed returns how long ago the element was used
func (p *poolElement) lastUsed() time.Duration {
	p.Lock()
	defer p.Unlock()
	return time.Now().Sub(p.lastTouch)
}

// handlerPool has a big job. It opens DBs on demand and
// closes them when they haven't been used after a while.
type handlerPool struct {
	sync.Mutex

	base     []string
	elements map[string]*poolElement

	// lru keeps a list with the recently used elements in Front and the
	// oldest in the back
	lru        *list.List
	lrumap     map[string]*list.Element // to find *list.Element by key
	ttl        time.Duration
	stopSignal chan bool
}

func newHandlerPool(basepath string, ttl time.Duration) *handlerPool {

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
		base:       path,
		elements:   make(map[string]*poolElement),
		lru:        list.New(),
		lrumap:     make(map[string]*list.Element),
		ttl:        ttl,
		stopSignal: make(chan bool),
	}

	return pool
}

func (p *handlerPool) startGarbageCollector() {
	go func() {
		for {

			// add a little fuzzing to the time so not all the
			// cleanup happens at the same time
			fuzzfactor := rand.Int63n(15) // 1x => 1.5x ttl
			fuzzttl := time.Duration(int64(p.ttl) * fuzzfactor / 10)

			select {
			case <-time.After(fuzzttl):
				p.cleanupHandlers(p.ttl)
			case <-p.stopSignal:
				return
			}
		}
	}()
}

func (p *handlerPool) cleanupHandlers(ttl time.Duration) {
	lruElement := p.lru.Back()
	for lruElement != nil {
		element := lruElement.Value.(*poolElement)
		if element.lastUsed() <= ttl {
			continue
		}

		element.handler.StopHTTP()
		next := lruElement.Prev()

		p.Lock()
		p.lru.Remove(lruElement)
		delete(p.lrumap, element.uid)
		delete(p.elements, element.uid)
		p.Unlock()

		lruElement = next
	}
}

// stopHandlers stops all handlers from servicing HTTP requests
func (p *handlerPool) stopHandlers() {
	close(p.stopSignal)
	p.cleanupHandlers(-1)
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

	element.touch()

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
