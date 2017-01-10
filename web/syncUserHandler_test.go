package web

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mozilla-services/go-syncstorage/syncstorage"
	"github.com/stretchr/testify/assert"
)

func TestSyncUserHandlerStopPurgeClose(t *testing.T) {
	assert := assert.New(t)
	uid := uniqueUID()
	db, _ := syncstorage.NewDB(":memory:", nil)
	handler := NewSyncUserHandler(uid, db, nil)

	handler.StopHTTP()

	// assert 503 with Retry-After header
	url := syncurl(uid, "info/collections")
	resp := request("GET", url, nil, handler)
	assert.Equal(http.StatusServiceUnavailable, resp.Code)
	assert.NotEqual("", resp.Header().Get("Retry-After"))
}

func TestSyncUserHandlerInfoCollections(t *testing.T) {
	assert := assert.New(t)

	uid := "123456"

	db, _ := syncstorage.NewDB(":memory:", nil)
	handler := NewSyncUserHandler(uid, db, nil)
	collections := []string{
		"clients",
		"crypto",
		"forms",
		"history",
		"keys",
		"meta",
		"bookmarks",
		"prefs",
		"tabs",
		"passwords",
		"addons",
	}
	for _, cName := range collections {
		cId, _ := db.GetCollectionId(cName)
		db.TouchCollection(cId, cId*1000) // turn the cId into milliseconds
	}

	resp := request("GET", syncurl(uid, "info/collections"), nil, handler)
	assert.Equal(http.StatusOK, resp.Code)
	results := make(map[string]float32)
	if err := json.Unmarshal(resp.Body.Bytes(), &results); !assert.NoError(err) {
		return
	}

	for cName, modified := range results {
		cId, err := db.GetCollectionId(cName)
		if !assert.NoError(err) {
			return
		}

		// after conversion to the sync modified format, should be equal
		assert.Equal(float32(cId), modified)
	}

}

func TestSyncUserHandlerInfoConfiguration(t *testing.T) {

	assert := assert.New(t)
	uid := uniqueUID()
	db, _ := syncstorage.NewDB(":memory:", nil)

	// make sure values propagate from the configuration
	config := &SyncUserHandlerConfig{
		MaxPOSTBytes:          1,
		MaxPOSTRecords:        2,
		MaxTotalBytes:         3,
		MaxTotalRecords:       4,
		MaxRequestBytes:       5,
		MaxRecordPayloadBytes: 6,
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
		if val, ok := jdata["max_record_payload_bytes"]; assert.True(ok, "max_record_payload_bytes") {
			assert.Equal(val, config.MaxRecordPayloadBytes)
		}
	}
}

// TestSyncUserHandlerPOST tests that POSTs behave correctly
func TestSyncUserHandlerPOST(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	uid := "123456"

	db, _ := syncstorage.NewDB(":memory:", nil)
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

	{ // test error when payload is too large
		body := bytes.NewBufferString(`[
			{"id":"bsoA", "payload": "1234567890"},
			{"id":"bsoB", "payload": "12345"}
		]`)

		// make a small acceptable payload
		config := NewDefaultSyncUserHandlerConfig()
		config.MaxRecordPayloadBytes = 5

		handler := NewSyncUserHandler(uid, db, config)
		url := syncurl(uid, "storage/bookmarks")

		resp := requestheaders("POST", url, body, header, handler)
		assert.Equal(http.StatusOK, resp.Code)

		var results PostResults
		err := json.Unmarshal(resp.Body.Bytes(), &results)
		if !assert.NoError(err) {
			return
		}

		assert.Len(results.Success, 1)
		if assert.Len(results.Failed, 1) {
			assert.Equal("Payload too large", results.Failed["bsoA"][0])
		}

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
		db, _ := syncstorage.NewDB(":memory:", nil)
		handler := NewSyncUserHandler(uid, db, nil)

		// initialize the collection so we have a proper last modified timestamp
		collectionInit := bytes.NewBufferString(`[
			{"id":"bsoA", "payload": "bsoA"},
			{"id":"bsoB", "payload": "bsoB"}
		]`)
		respInit := requestheaders("POST", url, collectionInit, header, handler)
		if !assert.Equal(http.StatusOK, respInit.Code, respInit.Body.String()) {
			return
		}

		respCreate := requestheaders("POST", url+"?batch=true", bodyCreate, header, handler)
		if !assert.Equal(http.StatusAccepted, respCreate.Code, respCreate.Body.String()) {
			return
		}

		// https://bugzilla.mozilla.org/show_bug.cgi?id=1324600
		// X-Last-Modified should not change until the batch is committed
		colLastModified := respInit.Header().Get("X-Last-Modified")
		createLM := respCreate.Header().Get("X-Last-Modified")
		if !assert.Equal(colLastModified, createLM, "ts should be equal got: %s, expected: %s", colLastModified, createLM) {
			return
		}

		var createResults PostResults
		if err := json.Unmarshal(respCreate.Body.Bytes(), &createResults); !assert.NoError(err) {
			return
		}

		assert.Equal(batchIdString(1), createResults.Batch) // clean db, always gets a batch id of 1
		batchIdString := createResults.Batch

		respAppend := requestheaders("POST", url+"?batch="+batchIdString, bodyAppend, header, handler)
		if !assert.Equal(http.StatusAccepted, respAppend.Code, respAppend.Body.String()) {
			return
		}

		appendLM := respAppend.Header().Get("X-Last-Modified")
		if !assert.Equal(colLastModified, appendLM, "ts should be equal got: %s, expected: %s", colLastModified, appendLM) {
			return
		}

		respCommit := requestheaders("POST", url+"?commit=1&batch="+batchIdString, bodyCommit, header, handler)
		assert.Equal(http.StatusOK, respCommit.Code, respCommit.Body.String())

		commitLM, _ := ConvertTimestamp(respCommit.Header().Get("X-Last-Modified"))
		createLMint, _ := ConvertTimestamp(createLM)
		if !assert.True(commitLM > createLMint, "commit ts invalid") {
			return
		}

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
		db, _ := syncstorage.NewDB(":memory:", nil)
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
		db, _ := syncstorage.NewDB(":memory:", nil)
		handler := NewSyncUserHandler(uid, db, nil)

		bodyCreate := bytes.NewBufferString(`[
			{"id":"bso0", "payload": "bso0"},
			{"id":"bso1", "payload": "bso1"},
			{"id":"bso2", "payload": "bso2"}
		]`)

		respCreate := requestheaders("POST", url+"?batch=true", bodyCreate, header, handler)
		if !assert.Equal(http.StatusAccepted, respCreate.Code, respCreate.Body.String()) {
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

func TestSyncUserHandlerPUT(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	uid := "123456"

	db, _ := syncstorage.NewDB(":memory:", nil)
	handler := NewSyncUserHandler(uid, db, nil)
	url := syncurl(uid, "storage/bookmarks/bso0")
	header := make(http.Header)
	header.Add("Content-Type", "application/json")

	{ // test basic create / update flow
		body := bytes.NewBufferString(`{"payload": "1234"}`)
		resp := requestheaders("PUT", url, body, header, handler)
		if !assert.Equal(http.StatusOK, resp.Code) {
			return
		}

		colId, _ := db.GetCollectionId("bookmarks")

		bsoNew, _ := db.GetBSO(colId, "bso0")
		assert.Equal("1234", bsoNew.Payload)

		body2 := bytes.NewBufferString(`{"sortindex": 9, "ttl":1000}`)
		resp2 := requestheaders("PUT", url, body2, header, handler)
		if !assert.Equal(http.StatusOK, resp2.Code) {
			fmt.Println(resp2.Body.String())
			return
		}

		bsoUpdated, _ := db.GetBSO(colId, "bso0")
		assert.Equal("1234", bsoUpdated.Payload)
		assert.Equal(9, bsoUpdated.SortIndex)
		assert.NotEqual(bsoNew.Modified, bsoUpdated.Modified)
		assert.NotEqual(bsoNew.TTL, bsoUpdated.TTL)
	}

	{ // test payload too large w/ a very small limit
		conf := NewDefaultSyncUserHandlerConfig()
		conf.MaxRecordPayloadBytes = 3
		handler := NewSyncUserHandler(uid, db, conf)

		body := bytes.NewBufferString(`{"payload": "1234"}`)
		resp := requestheaders("PUT", url, body, header, handler)
		if !assert.Equal(http.StatusRequestEntityTooLarge, resp.Code) {
			return
		}
	}

}

func TestSyncUserHandlerTidyUp(t *testing.T) {
	assert := assert.New(t)

	{ // test skipping of purging works
		db, _ := syncstorage.NewDB(":memory:", nil)
		config := NewDefaultSyncUserHandlerConfig()
		config.MaxBatchTTL = 1
		handler := NewSyncUserHandler(uniqueUID(), db, config)

		// no purge value in db will always skip, pointless doing a purge
		minPurge := 10 * time.Millisecond
		skipped, _, err := handler.TidyUp(minPurge, minPurge, 1)
		if !assert.NoError(err) || !assert.True(skipped, "Expected to skip") {
			return
		}

		time.Sleep(5 * time.Millisecond)
		// not minPurge yet...
		skipped, _, err = handler.TidyUp(minPurge, minPurge, 1)
		if !assert.NoError(err) || !assert.True(skipped) {
			return
		}

		// enough time has past, the purge should happen
		time.Sleep(10 * time.Millisecond)
		skipped, _, err = handler.TidyUp(minPurge, minPurge, 1)
		if !assert.NoError(err) || !assert.False(skipped) {
			return
		}
	}

	{ // test purging works
		db, _ := syncstorage.NewDB(":memory:", nil)
		config := NewDefaultSyncUserHandlerConfig()
		config.MaxBatchTTL = 1
		handler := NewSyncUserHandler(uniqueUID(), db, config)
		cId := 1
		bId := "bso0"

		payload := "hi"
		ttl := 1

		// write the NEXT_PURGE value
		handler.TidyUp(time.Nanosecond, time.Nanosecond, 1)

		// remember the size a new db
		usageOrig, _ := db.Usage()

		_, err := db.PutBSO(cId, bId, &payload, nil, &ttl)
		if !assert.NoError(err) {
			return
		}

		// put in some large data to make sure the vacuum threshold triggers
		batchId, err := db.BatchCreate(cId, strings.Repeat("1234567890", 5*4*1024))
		if !assert.NoError(err) {
			return
		}

		time.Sleep(time.Millisecond * 10)

		// What's actually being tested... Test that TidyUp will clean up the
		// BSO, Batch and Vacuum the database. Set to 1KB vacuum threshold
		_, _, err = handler.TidyUp(time.Nanosecond, time.Nanosecond, 1)

		if !assert.NoError(err) {
			return
		}

		// make sure the BSO was purged
		_, err = db.GetBSO(cId, bId)
		if !assert.Equal(syncstorage.ErrNotFound, err) {
			return
		}

		// make sure the batch was purged
		exists, err := db.BatchExists(batchId, cId)
		if !assert.NoError(err) && !assert.False(exists) {
			return
		}

		usage, err := db.Usage()
		if !assert.NoError(err) {
			return
		}

		assert.Equal(usageOrig.Total, usage.Total, "Expected size to be back to original")
		assert.Equal(0, usage.Free)
	}
}
