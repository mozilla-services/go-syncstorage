package web

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
			Batch:    batchIdString(1),
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

		assert.Equal(`{"modified":12345.68,"success":["bso0","bso1"],"failed":{"bso2":["a","b","c"],"bso3":["d","e","f"]},"batch":"b1"}`, string(b))

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

func TestRequestToPostBSOInput(t *testing.T) {
	assert := assert.New(t)
	uid := "123456"
	url := syncurl(uid, "storage/bookmarks")

	{ // test everything ok application/json
		body := bytes.NewBufferString(`[
		{"id":"bso1", "payload": "initial payload", "sortindex": 1, "ttl": 2100000},
		{"id":"bso2", "payload": "initial payload", "sortindex": 1, "ttl": 2100000}
	]`)
		req, _ := http.NewRequest("POST", url, body)
		req.Header.Add("Content-Type", "application/json")
		pInput, pResults, err := RequestToPostBSOInput(req, 256*1024)
		if assert.NoError(err) {
			if assert.NotNil(pInput) {
				assert.Equal(2, len(pInput))
			}
			if assert.NotNil(pResults) {
				assert.Equal(0, len(pResults.Failed))
			}
		}
	}

	{ // test everything ok application/newline
		body := bytes.NewBufferString(`
		{"id":"bso1", "payload": "initial payload", "sortindex": 1, "ttl": 2100000}
		{"id":"bso2", "payload": "initial payload", "sortindex": 1, "ttl": 2100000}
		`)
		req, _ := http.NewRequest("POST", url, body)
		req.Header.Add("Content-Type", "application/newline")
		pInput, pResults, err := RequestToPostBSOInput(req, 256*1024)
		if assert.NoError(err) {
			if assert.NotNil(pInput) {
				assert.Equal(2, len(pInput))
			}
			if assert.NotNil(pResults) {
				assert.Equal(0, len(pResults.Failed))
			}
		}
	}

	{ // test fail on too large body
		body := bytes.NewBufferString(`
		{"id":"bso1", "payload": "12345678"}
		{"id":"bso2", "payload": "12345"}
		`)

		req, _ := http.NewRequest("POST", url, body)
		req.Header.Add("Content-Type", "application/newline")
		pInput, pResults, err := RequestToPostBSOInput(req, 5)
		if assert.NoError(err) {
			if assert.NotNil(pInput) {
				assert.Equal(1, len(pInput))
			}
			if assert.NotNil(pResults) {
				assert.Equal(1, len(pResults.Failed))
				assert.Equal(1, len(pResults.Failed["bso1"])) //fail is for bso1
			}
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

func TestBatchIdInt(t *testing.T) {
	assert := assert.New(t)

	_, err := batchIdInt("1")
	assert.NotNil(err)

	n, err := batchIdInt("abc")
	assert.Equal(0, n)
	assert.NotNil(err)

	n, err = batchIdInt("a1")
	assert.Equal(1, n)
	assert.Nil(err)
}

func TestBatchIdString(t *testing.T) {
	assert.Equal(t, "b-1", batchIdString(-1))
	assert.Equal(t, "b0", batchIdString(0))
	assert.Equal(t, "b123", batchIdString(123))
}
