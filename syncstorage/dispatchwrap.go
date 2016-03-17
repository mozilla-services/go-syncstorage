package syncstorage

// dispatchwrap makes a pool adhere to the SyncApi interface
// which allows us to reuse tests
type dispatchwrap struct {
	uid      string
	dispatch *Dispatch
}

// =======================================================
// Implement SyncApi
// =======================================================

func (d *dispatchwrap) GetCollectionId(name string) (id int, err error) {
	return d.dispatch.GetCollectionId(d.uid, name)
}

func (d *dispatchwrap) GetCollectionModified(cId int) (modified int, err error) {
	return d.dispatch.GetCollectionModified(d.uid, cId)
}

func (d *dispatchwrap) CreateCollection(name string) (cId int, err error) {
	return d.dispatch.CreateCollection(d.uid, name)
}
func (d *dispatchwrap) DeleteCollection(cId int) (err error) {
	return d.dispatch.DeleteCollection(d.uid, cId)
}
func (d *dispatchwrap) TouchCollection(cId, modified int) (err error) {
	return d.dispatch.TouchCollection(d.uid, cId, modified)
}

func (d *dispatchwrap) InfoCollections() (map[string]int, error) {
	return d.dispatch.InfoCollections(d.uid)
}
func (d *dispatchwrap) InfoQuota() (used, quota int, err error) {
	return d.dispatch.InfoQuota(d.uid)
}
func (d *dispatchwrap) InfoCollectionUsage() (map[string]int, error) {
	return d.dispatch.InfoCollectionUsage(d.uid)
}
func (d *dispatchwrap) InfoCollectionCounts() (map[string]int, error) {
	return d.dispatch.InfoCollectionCounts(d.uid)
}

func (d *dispatchwrap) PostBSOs(cId int, input PostBSOInput) (*PostResults, error) {
	return d.dispatch.PostBSOs(d.uid, cId, input)
}

func (d *dispatchwrap) PutBSO(
	cId int,
	bId string,
	payload *string,
	sortIndex *int,
	ttl *int) (modified int, err error) {

	return d.dispatch.PutBSO(d.uid, cId, bId, payload, sortIndex, ttl)
}

func (d *dispatchwrap) GetBSO(cId int, bId string) (b *BSO, err error) {
	return d.dispatch.GetBSO(d.uid, cId, bId)
}
func (d *dispatchwrap) GetBSOs(
	cId int,
	ids []string,
	newer int,
	sort SortType,
	limit int,
	offset int) (r *GetResults, err error) {

	return d.dispatch.GetBSOs(d.uid, cId, ids, newer, sort, limit, offset)
}

func (d *dispatchwrap) GetBSOModified(cId int, bId string) (int, error) {
	return d.dispatch.GetBSOModified(d.uid, cId, bId)
}

func (d *dispatchwrap) DeleteBSO(cId int, bId string) (modified int, err error) {
	return d.dispatch.DeleteBSO(d.uid, cId, bId)
}
func (d *dispatchwrap) DeleteBSOs(cId int, bIds ...string) (modified int, err error) {
	return d.dispatch.DeleteBSOs(d.uid, cId, bIds...)
}

func (d *dispatchwrap) PurgeExpired() (int, error) {
	return d.dispatch.PurgeExpired(d.uid)
}

func (d *dispatchwrap) Usage() (stats *DBPageStats, err error) {
	return d.dispatch.Usage(d.uid)
}
func (d *dispatchwrap) Optimize(thresholdPercent int) (ItHappened bool, err error) {
	return d.dispatch.Optimize(d.uid, thresholdPercent)
}

func (d *dispatchwrap) DeleteEverything() error {
	return d.dispatch.DeleteEverything(d.uid)
}
