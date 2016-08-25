package web

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"
	"testing"

	"github.com/mozilla-services/go-syncstorage/syncstorage"
	"github.com/stretchr/testify/assert"
)

func TestSyncUserHandlerStopPurgeClose(t *testing.T) {
	assert := assert.New(t)
	uid := uniqueUID()
	db, _ := syncstorage.NewDB(":memory:")
	handler := NewSyncUserHandler(uid, db, nil)

	handler.StopHTTP()

	// assert 503 with Retry-After header
	url := syncurl(uid, "info/collections")
	resp := request("GET", url, nil, handler)
	assert.Equal(http.StatusServiceUnavailable, resp.Code)
	assert.NotEqual("", resp.Header().Get("Retry-After"))
}

func TestSyncUserHandlerInfoConfiguration(t *testing.T) {

	assert := assert.New(t)
	uid := uniqueUID()
	db, _ := syncstorage.NewDB(":memory:")

	// make sure values propagate from the configuration
	config := &SyncUserHandlerConfig{
		MaxPOSTBytes:    1,
		MaxPOSTRecords:  2,
		MaxTotalBytes:   3,
		MaxTotalRecords: 4,
		MaxRequestBytes: 5,
	}

	handler := NewSyncUserHandler(uid, db, config)
	resp := request("GET", syncurl(uid, "info/configuration"), nil, handler)

	assert.Equal(http.StatusOK, resp.Code)
	assert.Equal("application/json", resp.Header().Get("Content-Type"))

	jdata := make(map[string]int)
	if err := json.Unmarshal(resp.Body.Bytes(), &jdata); assert.NoError(err) {
		if val, ok := jdata["max_post_bytes"]; assert.True(ok, "max_post_bytes") {
			assert.Equal(val, config.MaxPOSTBytes)
		}
		if val, ok := jdata["max_post_records"]; assert.True(ok, "max_post_records") {
			assert.Equal(val, config.MaxPOSTRecords)
		}
		if val, ok := jdata["max_total_bytes"]; assert.True(ok, "max_total_bytes") {
			assert.Equal(val, config.MaxTotalBytes)
		}
		if val, ok := jdata["max_total_records"]; assert.True(ok, "max_total_records") {
			assert.Equal(val, config.MaxTotalRecords)
		}
		if val, ok := jdata["max_request_bytes"]; assert.True(ok, "max_request_bytes") {
			assert.Equal(val, config.MaxRequestBytes)
		}
	}
}

// TestSyncUserHandlerPOST tests that POSTs behave correctly
func TestSyncUserHandlerPOST(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	uid := "123456"

	db, _ := syncstorage.NewDB(":memory:")
	handler := NewSyncUserHandler(uid, db, nil)
	url := syncurl(uid, "storage/bookmarks")

	{
		header := make(http.Header)
		header.Add("Content-Type", "application/octet-stream")

		resp := requestheaders("POST", url, nil, header, handler)
		if !assert.Equal(http.StatusUnsupportedMediaType, resp.Code) {
			return
		}
	}

	// Make sure INSERT works first
	body := bytes.NewBufferString(`[
		{"id":"bso1", "payload": "initial payload", "sortindex": 1, "ttl": 2100000},
		{"id":"bso2", "payload": "initial payload", "sortindex": 1, "ttl": 2100000},
		{"id":"bso3", "payload": "initial payload", "sortindex": 1, "ttl": 2100000}
	]`)

	// POST new data
	header := make(http.Header)
	header.Add("Content-Type", "application/json")
	cId, _ := db.GetCollectionId("bookmarks")

	{
		resp := requestheaders("POST", url, body, header, handler)

		assert.Equal(http.StatusOK, resp.Code)

		var results PostResults
		jsbody := resp.Body.Bytes()
		err := json.Unmarshal(jsbody, &results)
		if !assert.NoError(err) {
			return
		}

		assert.Len(results.Success, 3)
		assert.Len(results.Failed, 0)

		// verify it made it into the db ok
		for _, bId := range []string{"bso1", "bso2", "bso3"} {
			bso, _ := db.GetBSO(cId, bId)
			assert.Equal("initial payload", bso.Payload)
			assert.Equal(1, bso.SortIndex)
		}
	}

	{
		// Test that updates work
		body = bytes.NewBufferString(`[
			{"id":"bso1", "sortindex": 2},
			{"id":"bso2", "payload": "updated payload"},
			{"id":"bso3", "payload": "updated payload", "sortindex":3}
		]`)

		resp := requestheaders("POST", url, body, header, handler)
		assert.Equal(http.StatusOK, resp.Code)

		bso, _ := db.GetBSO(cId, "bso1")
		assert.Equal("initial payload", bso.Payload) // stayed the same
		assert.Equal(2, bso.SortIndex)               // it updated

		bso, _ = db.GetBSO(cId, "bso2")
		assert.Equal("updated payload", bso.Payload) // updated
		assert.Equal(1, bso.SortIndex)               // same

		bso, _ = db.GetBSO(cId, "bso3")
		assert.Equal(bso.Payload, "updated payload") // updated
		assert.Equal(3, bso.SortIndex)               // updated
	}

	{ // ref: issue #108 - handling of Content-Type like application/json;charset=utf-8
		body := bytes.NewBufferString(`[
			{"id":"bsoa", "payload": "initial payload", "sortindex": 1, "ttl": 2100000},
			{"id":"bsob", "payload": "initial payload", "sortindex": 1, "ttl": 2100000},
			{"id":"bsoc", "payload": "initial payload", "sortindex": 1, "ttl": 2100000}
		]`)

		// POST new data
		header := make(http.Header)
		header.Add("Content-Type", "application/json;charset=utf-8")
		resp := requestheaders("POST", url, body, header, handler)
		assert.Equal(http.StatusOK, resp.Code)
	}
}

// TestSyncUserHandlerPOSTBatch tests that a batch can be created, appended to and commited
func TestSyncUserHandlerPOSTBatch(t *testing.T) {

	assert := assert.New(t)

	bodyCreate := bytes.NewBufferString(`[
		{"id":"bso0", "payload": "bso0"},
		{"id":"bso1", "payload": "bso1"}
	]`)
	bodyAppend := bytes.NewBufferString(`[
		{"id":"bso2", "payload": "bso2"},
		{"id":"bso3", "payload": "bso3"}
	]`)
	bodyCommit := bytes.NewBufferString(`[
		{"id":"bso4", "payload": "bso4"},
		{"id":"bso5", "payload": "bso5"}
	]`)

	uid := "123456"
	collection := "testcol"
	url := syncurl(uid, "storage/"+collection)
	header := make(http.Header)
	header.Add("Content-Type", "application/json")

	{ // test common flow
		db, _ := syncstorage.NewDB(":memory:")
		handler := NewSyncUserHandler(uid, db, nil)

		respCreate := requestheaders("POST", url+"?batch=true", bodyCreate, header, handler)
		if !assert.Equal(http.StatusOK, respCreate.Code, respCreate.Body.String()) {
			return
		}
		var createResults PostResults
		if err := json.Unmarshal(respCreate.Body.Bytes(), &createResults); !assert.NoError(err) {
			return
		}

		assert.Equal(batchIdString(1), createResults.Batch) // clean db, always gets 1
		batchIdString := createResults.Batch

		respAppend := requestheaders("POST", url+"?batch="+batchIdString, bodyAppend, header, handler)
		assert.Equal(http.StatusOK, respAppend.Code, respAppend.Body.String())

		respCommit := requestheaders("POST", url+"?commit=1&batch="+batchIdString, bodyCommit, header, handler)
		assert.Equal(http.StatusOK, respCommit.Code, respCommit.Body.String())

		cId, _ := db.GetCollectionId(collection)
		for bIdNum := 0; bIdNum <= 5; bIdNum++ {
			bId := "bso" + strconv.Itoa(bIdNum)
			_, err := db.GetBSO(cId, bId)
			assert.NoError(err, "Could not find bso: %s", bId)
		}

		// make sure the batch is no longer in the db
		batchIdInt, _ := batchIdInt(createResults.Batch)
		_, errMissing := db.BatchLoad(batchIdInt, cId)
		assert.Equal(syncstorage.ErrBatchNotFound, errMissing)
	}

	{ // test commit=1&batch=true
		db, _ := syncstorage.NewDB(":memory:")
		handler := NewSyncUserHandler(uid, db, nil)

		bodyCreate := bytes.NewBufferString(`[
			{"id":"bso0", "payload": "bso0"},
			{"id":"bso1", "payload": "bso1"},
			{"id":"bso2", "payload": "bso2"}
		]`)

		respCreate := requestheaders("POST", url+"?batch=true&commit=1", bodyCreate, header, handler)
		if !assert.Equal(http.StatusOK, respCreate.Code, respCreate.Body.String()) {
			return
		}
	}

	{ // test batch=true and an empty commit
		db, _ := syncstorage.NewDB(":memory:")
		handler := NewSyncUserHandler(uid, db, nil)

		bodyCreate := bytes.NewBufferString(`[
			{"id":"bso0", "payload": "bso0"},
			{"id":"bso1", "payload": "bso1"},
			{"id":"bso2", "payload": "bso2"}
		]`)

		respCreate := requestheaders("POST", url+"?batch=true", bodyCreate, header, handler)
		if !assert.Equal(http.StatusOK, respCreate.Code, respCreate.Body.String()) {
			return
		}

		var createResults PostResults
		if err := json.Unmarshal(respCreate.Body.Bytes(), &createResults); !assert.NoError(err) {
			return
		}

		assert.Equal(batchIdString(1), createResults.Batch) // clean db, always gets 1
		batchIdString := createResults.Batch
		body := bytes.NewReader([]byte("[]"))
		respCommit := requestheaders("POST", url+"?commit=1&batch="+batchIdString, body, header, handler)
		if !assert.Equal(http.StatusOK, respCommit.Code, respCommit.Body.String()) {
			return
		}

	}
}
