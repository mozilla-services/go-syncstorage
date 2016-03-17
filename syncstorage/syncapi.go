package syncstorage

// SyncApi defines the functions that are required for working with
// the Sync 1.5 API design. By defining an interface we can pass around
// various things that can provide the same functionality.
//
// This was created so we can test syncstorage.Pool and syncstorage.DB
// using similar code
type SyncApi interface {
	GetCollectionId(name string) (id int, err error)
	GetCollectionModified(cId int) (modified int, err error)

	CreateCollection(name string) (cId int, err error)
	DeleteCollection(cId int) (err error)
	TouchCollection(cId, modified int) error

	InfoCollections() (map[string]int, error)
	InfoQuota() (used, quota int, err error)
	InfoCollectionUsage() (map[string]int, error)
	InfoCollectionCounts() (map[string]int, error)

	PostBSOs(cId int, input PostBSOInput) (*PostResults, error)
	PutBSO(
		cId int,
		bId string,
		payload *string,
		sortIndex *int,
		ttl *int) (modified int, err error)

	GetBSO(cId int, bId string) (b *BSO, err error)
	GetBSOs(
		cId int,
		ids []string,
		newer int,
		sort SortType,
		limit int,
		offset int) (r *GetResults, err error)

	GetBSOModified(cId int, bId string) (modified int, err error)

	DeleteBSO(cId int, bId string) (int, error)
	DeleteBSOs(cId int, bIds ...string) (modified int, err error)

	PurgeExpired() (removed int, error error)

	Usage() (stats *DBPageStats, err error)
	Optimize(thresholdPercent int) (ItHappened bool, err error)

	DeleteEverything() error
}
