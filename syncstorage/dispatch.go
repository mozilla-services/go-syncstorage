package syncstorage

import (
	"crypto/sha1"
	"encoding/binary"
)

// Dispatch creates a ring of syncstorage.Pool and distributes
// database accesses over them. This is to provide less lock contention
// as a Pool manages a cache of open files
type Dispatch struct {
	pools    []*Pool
	numPools uint16
}

func NewDispatch(numPools uint16, basepath string, p PathMaker, cachesize int) (d *Dispatch, err error) {
	pools := make([]*Pool, numPools)
	for k, _ := range pools {
		pools[k], err = NewPoolCacheSize(basepath, p, cachesize)
		if err != nil {
			return nil, err
		}
	}

	d = &Dispatch{
		pools:    pools,
		numPools: numPools,
	}

	return
}

// Index returns which pool the uid should use to access
// the database files.
func (d *Dispatch) Index(uid string) uint16 {
	h := sha1.Sum([]byte(uid))

	// There are 20 bytes in a sha1 sum, we only need the
	// last 2 to determine the id
	return binary.BigEndian.Uint16(h[18:]) % d.numPools
}

// =======================================================
// Below implements approximately SyncApi except each
// method takes a `uid string`. This `uid` is used for
// the file path of the sqlite3 files.
// =======================================================

func (d *Dispatch) LastModified(uid string) (int, error) {
	pool := d.pools[d.Index(uid)]
	return pool.LastModified(uid)
}
func (d *Dispatch) GetCollectionId(uid string, name string) (id int, err error) {
	pool := d.pools[d.Index(uid)]
	return pool.GetCollectionId(uid, name)
}

func (d *Dispatch) GetCollectionModified(uid string, cId int) (modified int, err error) {
	pool := d.pools[d.Index(uid)]
	return pool.GetCollectionModified(uid, cId)
}

func (d *Dispatch) CreateCollection(uid string, name string) (cId int, err error) {
	pool := d.pools[d.Index(uid)]
	return pool.CreateCollection(uid, name)
}
func (d *Dispatch) DeleteCollection(uid string, cId int) (err error) {
	pool := d.pools[d.Index(uid)]
	return pool.DeleteCollection(uid, cId)
}
func (d *Dispatch) TouchCollection(uid string, cId, modified int) (err error) {
	pool := d.pools[d.Index(uid)]
	return pool.TouchCollection(uid, cId, modified)
}

func (d *Dispatch) InfoCollections(uid string) (map[string]int, error) {
	pool := d.pools[d.Index(uid)]
	return pool.InfoCollections(uid)
}
func (d *Dispatch) InfoQuota(uid string) (used, quota int, err error) {
	pool := d.pools[d.Index(uid)]
	return pool.InfoQuota(uid)
}
func (d *Dispatch) InfoCollectionUsage(uid string) (map[string]int, error) {
	pool := d.pools[d.Index(uid)]
	return pool.InfoCollectionUsage(uid)
}
func (d *Dispatch) InfoCollectionCounts(uid string) (map[string]int, error) {
	pool := d.pools[d.Index(uid)]
	return pool.InfoCollectionCounts(uid)
}

func (d *Dispatch) PostBSOs(uid string, cId int, input PostBSOInput) (*PostResults, error) {
	pool := d.pools[d.Index(uid)]
	return pool.PostBSOs(uid, cId, input)
}

func (d *Dispatch) PutBSO(
	uid string,
	cId int,
	bId string,
	payload *string,
	sortIndex *int,
	ttl *int) (modified int, err error) {

	pool := d.pools[d.Index(uid)]

	return pool.PutBSO(uid, cId, bId, payload, sortIndex, ttl)
}

func (d *Dispatch) GetBSO(uid string, cId int, bId string) (b *BSO, err error) {
	pool := d.pools[d.Index(uid)]

	return pool.GetBSO(uid, cId, bId)
}
func (d *Dispatch) GetBSOs(
	uid string,
	cId int,
	ids []string,
	newer int,
	sort SortType,
	limit int,
	offset int) (r *GetResults, err error) {

	pool := d.pools[d.Index(uid)]
	return pool.GetBSOs(uid, cId, ids, newer, sort, limit, offset)
}

func (d *Dispatch) GetBSOModified(uid string, cId int, bId string) (modified int, err error) {

	pool := d.pools[d.Index(uid)]
	return pool.GetBSOModified(uid, cId, bId)
}

func (d *Dispatch) DeleteBSO(uid string, cId int, bId string) (modified int, err error) {
	pool := d.pools[d.Index(uid)]
	return pool.DeleteBSO(uid, cId, bId)
}
func (d *Dispatch) DeleteBSOs(uid string, cId int, bIds ...string) (modified int, err error) {
	pool := d.pools[d.Index(uid)]
	return pool.DeleteBSOs(uid, cId, bIds...)
}

func (d *Dispatch) PurgeExpired(uid string) (removed int, err error) {
	pool := d.pools[d.Index(uid)]
	return pool.PurgeExpired(uid)
}

func (d *Dispatch) Usage(uid string) (stats *DBPageStats, err error) {
	pool := d.pools[d.Index(uid)]
	return pool.Usage(uid)
}
func (d *Dispatch) Optimize(uid string, thresholdPercent int) (ItHappened bool, err error) {
	pool := d.pools[d.Index(uid)]
	return pool.Optimize(uid, thresholdPercent)
}

func (d *Dispatch) DeleteEverything(uid string) error {
	pool := d.pools[d.Index(uid)]
	return pool.DeleteEverything(uid)
}
