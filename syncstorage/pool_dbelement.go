package syncstorage

import (
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
)

// dbelement is used by pool to keep track of in use and open db
type dbelement struct {
	sync.Mutex

	uid   string
	inUse bool

	// track if expired BSOs have been purged
	purgeRan bool

	db *DB

	// last time this was used
	lastUsed time.Time
}

func (de *dbelement) BeenPurged() bool {
	de.Use()
	defer de.Release()

	return de.purgeRan
}

// Use locks the dbelement so multiple calls to the database
// can be done. This is used to synchronize access for HTTP api call
// handlers which can make mulitple read/write requests in a discrete
// request
func (de *dbelement) Use() {
	de.Lock()
	de.inUse = true
}
func (de *dbelement) Release() {
	de.inUse = false
	de.Unlock()
}

func (de *dbelement) InUse() bool {
	return de.inUse
}

// purgeElement will purge expired rows and log how much recoverable
// space is in a database
type purgeResults struct {
	uid       string
	numPurged int

	took               time.Duration
	total, free, bytes int // page stats
}

// Fields creates a log.Fields
func (r *purgeResults) Fields() log.Fields {
	return log.Fields{
		"uid":          r.uid,
		"purged":       r.numPurged,
		"pages_total":  r.total,
		"pages_free":   r.free,
		"unused_bytes": r.bytes,
		"t":            int64(r.took / time.Millisecond),
	}
}

// purge removes all expired BSOs from the user's DB
func (de *dbelement) purge() (*purgeResults, error) {
	de.Use()
	defer de.Release()

	if de.purgeRan {
		return nil, nil
	} else {
		de.purgeRan = true
	}

	start := time.Now()
	if numPurged, err := de.db.PurgeExpired(); err == nil {
		if usage, err := de.db.Usage(); err == nil {
			took := time.Now().Sub(start)
			return &purgeResults{
				uid:       de.uid,
				numPurged: numPurged,
				total:     usage.Total,
				free:      usage.Free,
				bytes:     usage.Free * usage.Size,
				took:      took,
			}, nil
		} else {
			return nil, errors.Wrap(err, "Purge Error getting db.Usage")
		}
	} else {
		return nil, errors.Wrap(err, "Purge Error doing db.PurgeExpired")
	}

}
