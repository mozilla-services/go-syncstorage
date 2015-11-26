package syncstorage

// ref: https://docs.services.mozilla.com/storage/apis-1.5.html#basic-storage-object
type BSO struct {
	Id        string  `json:"id"`
	Modified  float64 `json:"modified"`
	Payload   string  `json:"payload"`
	SortIndex uint    `json:"sortindex"`
	TTL       uint    `json:",omitempty"`
}
