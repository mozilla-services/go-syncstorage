package syncstorage

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	bso = BSO{
		Id:       "Testing",
		Modified: 1000000000000099,
		Payload: `Put in a quote: "to make it interesting"
			and some new lines
			`,
		SortIndex: 11,
	}
)

func TestBSOtoJson(t *testing.T) {
	j, err := json.Marshal(bso)
	if assert.NoError(t, err) {
		assert.Equal(t, `{"id":"Testing","modified":1000000000000.10,"payload":"Put in a quote: \"to make it interesting\"\n\t\t\tand some new lines\n\t\t\t"}`, string(j))
	}

	// make sure refs work too
	j, err = json.Marshal(&bso)
	if assert.NoError(t, err) {
		assert.Equal(t, `{"id":"Testing","modified":1000000000000.10,"payload":"Put in a quote: \"to make it interesting\"\n\t\t\tand some new lines\n\t\t\t"}`, string(j))
	}
}

// abouts 2.5x slower than regular marshalling :\
func BenchmarkBSOtoJson(b *testing.B) {
	for i := 0; i < b.N; i++ {
		json.Marshal(bso)
	}
}

func BenchmarkBSOtoJsonRef(b *testing.B) {
	for i := 0; i < b.N; i++ {
		// passing a ref to marshal
		json.Marshal(&bso)
	}

}
