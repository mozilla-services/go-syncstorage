package web

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPostResultJSONMarshaller(t *testing.T) {
	assert := assert.New(t)

	{ // make sure the zero values encode correctly
		x := &PostResults{}
		b, err := json.Marshal(x)
		if !assert.NoError(err) {
			return
		}

		assert.Equal(`{"modified":0.00,"success":[],"failed":{}}`, string(b))
	}

	{ // make sure non zero values encode correctly
		x := &PostResults{
			Batch:    1,
			Modified: 12345678,
			Success:  []string{"bso0", "bso1"},
			Failed: map[string][]string{
				"bso2": {"a", "b", "c"},
				"bso3": {"d", "e", "f"},
			},
		}

		b, err := json.Marshal(x)
		if !assert.NoError(err) {
			return
		}

		assert.Equal(`{"modified":12345.68,"success":["bso0","bso1"],"failed":{"bso2":["a","b","c"],"bso3":["d","e","f"]},"batch":1}`, string(b))

	}
}
