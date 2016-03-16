package syncstorage

import (
	"encoding/json"
	"fmt"
)

// ref: https://docs.services.mozilla.com/storage/apis-1.5.html#basic-storage-object
type BSO struct {
	Id        string
	Modified  int
	Payload   string
	SortIndex int
	TTL       int
}

// MarshalJSON builds a custom json blob since there is no way good way of turning the
// Modified int (in milliseconds) into seconds with two decimal places which the
// api defines as the correct format. meh.
func (b BSO) MarshalJSON() ([]byte, error) {

	var id, payload string

	// turn milliseconds into seconds with two decimal places
	modified := float64(b.Modified) / 1000

	// convert strings using json.Marshal to properly escape/quote thing
	if v, err := json.Marshal(b.Id); err == nil {
		id = string(v)
	} else {
		return nil, err
	}

	if v, err := json.Marshal(b.Payload); err == nil {
		payload = string(v)
	} else {
		return nil, err
	}

	j := fmt.Sprintf(`{"id":%s,"modified":%.02f,"payload":%s}`, id, modified, payload)
	return []byte(j), nil

}
