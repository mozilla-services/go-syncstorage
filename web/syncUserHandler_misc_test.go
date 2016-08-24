package web

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/mozilla-services/go-syncstorage/syncstorage"
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

func TestReadNewlineJSON(t *testing.T) {
	assert := assert.New(t)
	numBSOs := 10

	buf := new(bytes.Buffer)
	// make some very large new line separated BSOs to test with

	// create a worst case huge json payload
	size := 256 * 1024 // 256KB = max size
	payload := strings.Repeat("a", size)

	for i := 0; i < numBSOs; i++ {
		buf.WriteString(`{"ttl":1234567,"sortindex":1,"id":"`)
		buf.WriteString(strconv.Itoa(i))

		buf.WriteString(`","payload":"`)
		buf.WriteString(payload)

		buf.WriteString(`"}`)
		buf.WriteByte('\n')
	}

	rawJSON := ReadNewlineJSON(buf)
	if !assert.Equal(numBSOs, len(rawJSON)) {
		return
	}

	// make sure each entry is valid json
	for i, rawBSO := range rawJSON {
		var b syncstorage.PutBSOInput
		parseErr := parseIntoBSO(rawBSO, &b)
		if !assert.Nil(parseErr, "invalid json for id:"+strconv.Itoa(i)) {
			fmt.Println(string(rawBSO))
			return
		}
	}
}

func BenchmarkReadNewlineJSON(b *testing.B) {
	numBSOs := 10
	buf := new(bytes.Buffer)

	// create larger than average BSO payload
	size := 16 * 1024
	payload := strings.Repeat("a", size)

	for i := 0; i < numBSOs; i++ {
		buf.WriteString(`{"ttl":1234567,"sortindex":1,"id":"`)
		buf.WriteString(strconv.Itoa(i))

		buf.WriteString(`","payload":"`)
		buf.WriteString(payload)

		buf.WriteString(`"}`)
		buf.WriteByte('\n')
	}

	// make a ReadSeeker out of it
	reader := bytes.NewReader(buf.Bytes())
	for i := 0; i < b.N; i++ {
		ReadNewlineJSON(reader)
		reader.Seek(0, io.SeekStart)
	}
}
