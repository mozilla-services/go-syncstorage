package syncstorage

import (
	"bytes"
	"encoding/json"
	"strconv"
	"sync"
)

// use a buffer pool to reduce memory allocations
// since we'll be encoding a lot of BSOs
var bsoBufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

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

	buf := bsoBufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bsoBufferPool.Put(buf)

	buf.WriteString(`{"id":`)
	if encoded, err := json.Marshal(b.Id); err == nil {
		buf.Write(encoded)
	} else {
		return nil, err
	}

	buf.WriteString(`,"modified":`)
	buf.WriteString(ModifiedToString(b.Modified))

	buf.WriteString(`,"payload":`)
	if encoded, err := json.Marshal(b.Payload); err == nil {
		buf.Write(encoded)
	} else {
		return nil, err
	}

	if b.SortIndex != 0 {
		buf.WriteString(`,"sortindex":`)
		buf.WriteString(strconv.Itoa(b.SortIndex))
	}

	buf.WriteString("}")
	c := make([]byte, buf.Len())
	copy(c, buf.Bytes())
	return c, nil
}
