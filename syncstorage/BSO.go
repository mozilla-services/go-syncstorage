package syncstorage

// ref: https://docs.services.mozilla.com/storage/apis-1.5.html#basic-storage-object
type BSO struct {
	Id        string `json:"id"`
	Modified  int    `json:"modified"`
	Payload   string `json:"payload"`
	SortIndex int    `json:"sortindex"`
	TTL       int    `json:",omitempty"`
}
