package syncstorage

// wrapPool makes a pool adhere to the SyncApi interface
// which allows us to reuse tests
type poolwrap struct {
	uid  string
	pool *Pool
}

func newPoolwrap() *poolwrap { return newPoolwrapUID("1234567890") }
func newPoolwrapUID(uid string) *poolwrap {

	p, err := NewPool(getTempBase(), TwoLevelPath)
	if err != nil {
		panic(err)
	}

	return &poolwrap{uid: uid, pool: p}
}

// =======================================================
// Implement SyncApi
// =======================================================

func (p *poolwrap) GetCollectionId(name string) (id int, err error) {
	return p.pool.GetCollectionId(p.uid, name)
}

func (p *poolwrap) GetCollectionModified(cId int) (modified int, err error) {
	return p.pool.GetCollectionModified(p.uid, cId)
}

func (p *poolwrap) CreateCollection(name string) (cId int, err error) {
	return p.pool.CreateCollection(p.uid, name)
}
func (p *poolwrap) DeleteCollection(cId int) (err error) {
	return p.pool.DeleteCollection(p.uid, cId)
}
func (p *poolwrap) TouchCollection(cId, modified int) (err error) {
	return p.pool.TouchCollection(p.uid, cId, modified)
}

func (p *poolwrap) InfoCollections() (map[string]int, error) {
	return p.pool.InfoCollections(p.uid)
}
func (p *poolwrap) InfoQuota() (used, quota int, err error) {
	return p.pool.InfoQuota(p.uid)
}
func (p *poolwrap) InfoCollectionUsage() (map[string]int, error) {
	return p.pool.InfoCollectionUsage(p.uid)
}
func (p *poolwrap) InfoCollectionCounts() (map[string]int, error) {
	return p.pool.InfoCollectionCounts(p.uid)
}

func (p *poolwrap) PostBSOs(cId int, input PostBSOInput) (*PostResults, error) {
	return p.pool.PostBSOs(p.uid, cId, input)
}

func (p *poolwrap) PutBSO(
	cId int,
	bId string,
	payload *string,
	sortIndex *int,
	ttl *int) (modified int, err error) {

	return p.pool.PutBSO(p.uid, cId, bId, payload, sortIndex, ttl)
}

func (p *poolwrap) GetBSO(cId int, bId string) (b *BSO, err error) {
	return p.pool.GetBSO(p.uid, cId, bId)
}
func (p *poolwrap) GetBSOs(
	cId int,
	ids []string,
	newer int,
	sort SortType,
	limit int,
	offset int) (r *GetResults, err error) {

	return p.pool.GetBSOs(p.uid, cId, ids, newer, sort, limit, offset)
}

func (p *poolwrap) DeleteBSO(cId int, bId string) (modified int, err error) {
	return p.pool.DeleteBSO(p.uid, cId, bId)
}
func (p *poolwrap) DeleteBSOs(cId int, bIds ...string) (modified int, err error) {
	return p.pool.DeleteBSOs(p.uid, cId, bIds...)
}

func (p *poolwrap) PurgeExpired() (int, error) {
	return p.pool.PurgeExpired(p.uid)
}

func (p *poolwrap) Usage() (stats *DBPageStats, err error) {
	return p.pool.Usage(p.uid)
}
func (p *poolwrap) Optimize(thresholdPercent int) (ItHappened bool, err error) {
	return p.pool.Optimize(p.uid, thresholdPercent)
}
