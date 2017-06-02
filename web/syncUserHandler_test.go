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

// used for testing that the returned json data is good
type jsResult []jsonBSO
type jsonBSO struct {
	Id        string  `json:"id"`
	Modified  float64 `json:"modified"`
	Payload   string  `json:"payload"`
	SortIndex int     `json:"sortindex"`
}

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

func TestSyncUserHandlerInfoCollectionCounts(t *testing.T) {
	assert := assert.New(t)

	uid := uniqueUID()
	db, _ := syncstorage.NewDB(":memory:", nil)
	handler := NewSyncUserHandler(uid, db, nil)

	header := make(http.Header)
	header.Add("Content-Type", "application/json")
	for _, col := range []string{"bookmarks", "history", "passwords"} {
		body := bytes.NewBufferString(`{"id":"bso1", "payload": "ppp", "sortindex": 1, "ttl": 2100000}`)
		resp := requestheaders("PUT", syncurl(uid, "storage/"+col+"/bso1"), body, header, handler)
		if !assert.Equal(http.StatusOK, resp.Code, col) {
			return
		}
	}

	resp := request("GET", syncurl(uid, "info/collection_counts"), nil, handler)
	assert.Equal(http.StatusOK, resp.Code)
	assert.Equal(`{"bookmarks":1,"history":1,"passwords":1}`, resp.Body.String())
}

func TestSyncUserHandlerInfoCollectionUsage(t *testing.T) {
	assert := assert.New(t)

	uid := uniqueUID()
	db, _ := syncstorage.NewDB(":memory:", nil)
	handler := NewSyncUserHandler(uid, db, nil)

	testData := map[string]int{ // use exact KB measurements
		"1": 10 * 1024,
		"2": 2 * 1024,
		"3": 512,
		"4": 1024,
	}

	{ // Seed some Data
		header := make(http.Header)
		header.Add("Content-Type", "application/json")
		for colName, payloadSize := range testData {
			body := bytes.NewBufferString(fmt.Sprintf(`{"payload":"%s"}`, strings.Repeat("-", payloadSize)))
			resp := requestheaders("PUT", syncurl(uid, fmt.Sprintf("storage/%s/testBso", colName)), body, header, handler)
			assert.Equal(http.StatusOK, resp.Code, resp.Body.String())
		}
	}

	{
		resp := request("GET", syncurl(uid, "info/collection_usage"), nil, handler)
		if !assert.Equal(http.StatusOK, resp.Code, resp.Body.String()) {
			return
		}

		assert.NotEqual("", resp.Header().Get("X-Last-Modified"))

		data := make(map[string]float64)
		assert.NoError(json.Unmarshal(resp.Body.Bytes(), &data))
		assert.Equal(data["1"], float64(10))
		assert.Equal(data["2"], float64(2))
		assert.Equal(data["3"], float64(0.5))
		assert.Equal(data["4"], float64(1))
	}
}

func TestSyncUserHandlerInfoQuota(t *testing.T) {

	assert := assert.New(t)
	uid := uniqueUID()
	db, _ := syncstorage.NewDB(":memory:", nil)
	handler := NewSyncUserHandler(uid, db, nil)

	{
		resp := request("GET", syncurl(uid, "info/quota"), nil, handler)
		if !assert.Equal(http.StatusOK, resp.Code, resp.Body.String()) {
			return
		}
		assert.Equal("[0.00000000,null]", resp.Body.String())
		assert.NotEqual("", resp.Header().Get("X-Last-Modified"))
	}

	{
		header := make(http.Header)
		header.Add("Content-Type", "application/json")
		body := bytes.NewBufferString(fmt.Sprintf(`{"payload":"%s"}`, strings.Repeat("-", 13*1024+511))) // use 511 for MOAR decimals
		putResp := requestheaders("PUT", syncurl(uid, "storage/test/t"), body, header, handler)
		if !assert.Equal(http.StatusOK, putResp.Code, putResp.Body.String()) {
			return
		}

		resp := request("GET", syncurl(uid, "info/quota"), nil, handler)
		if !assert.Equal(http.StatusOK, resp.Code, resp.Body.String()) {
			return
		}
		assert.Equal("[13.49902344,null]", resp.Body.String())
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

	var badCrypto = `[{"id":"keys", "payload":"{\"ciphertext\":\"IDontKnowWhatImDoing\",\"IV\":\"AAAAAAAAAAAAAAAAAAAAAA==\"}"}]`
	{ // POST to storage/crypto w/ bad data fails
		resp := requestheaders("POST",
			syncurl(uid, "storage/crypto"),
			bytes.NewBufferString(badCrypto), header, handler)
		assert.Equal(http.StatusBadRequest, resp.Code)
		assert.Equal(`{"err":"Known-bad BSO payload"}`, resp.Body.String())
	}

	{ // POST to other collections w/ bad data is ok
		resp := requestheaders("POST",
			syncurl(uid, "storage/col2"),
			bytes.NewBufferString(badCrypto), header, handler)
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

func TestSyncUserHandlerBatchLimits(t *testing.T) {
	assert := assert.New(t)
	db, _ := syncstorage.NewDB(":memory:", nil)
	uid := "123456"

	url := syncurl(uid, "storage/bookmarks?batch=true")

	{ // test limits based on client supplied headers
		handler := NewSyncUserHandler(uid, db, nil)
		// set the config to super low values
		handler.config.MaxTotalRecords = 1
		handler.config.MaxTotalBytes = 1
		handler.config.MaxPOSTBytes = 1
		handler.config.MaxPOSTRecords = 1

		// test the batch info headers from client
		headerList := []string{
			"X-Weave-Total-Records",
			"X-Weave-Total-Bytes",
			"X-Weave-Records",
			"X-Weave-Bytes",
		}

		body := bytes.NewBufferString(`[{"id":"bso0", "payload": "bso0"}]`)

		for _, headerName := range headerList {
			{
				header := make(http.Header)
				header.Add("Content-Type", "application/json")
				header.Add(headerName, "2")
				resp := requestheaders("POST", url, body, header, handler)
				if assert.Equal(resp.Code, http.StatusBadRequest, resp.Body.String()) {
					assert.Equal(WEAVE_SIZE_LIMIT_EXCEEDED, resp.Body.String())
				}
			}

			{ // test with non int value for header ...
				header := make(http.Header)
				header.Add("Content-Type", "application/json")
				header.Add(headerName, "bad value")
				resp := requestheaders("POST", url, body, header, handler)
				assert.Equal(resp.Code, http.StatusBadRequest, resp.Body.String())
			}
		}
	}

	{ // test actual max bytes verify works
		handler := NewSyncUserHandler(uid, db, nil)
		// set the config to super low values
		body := bytes.NewBufferString(`[{"id":"bso0", "payload": "bso0"}]`)
		handler.config.MaxPOSTBytes = 10

		header := make(http.Header)
		header.Add("Content-Type", "application/json")
		resp := requestheaders("POST", url, body, header, handler)
		if assert.Equal(resp.Code, http.StatusBadRequest) {
			assert.Equal(WEAVE_SIZE_LIMIT_EXCEEDED, resp.Body.String())
		}
	}

	{ // test bad post data verify
		handler := NewSyncUserHandler(uid, db, nil)
		// set the config to super low values
		body := bytes.NewBufferString(`[{"id":"bso0","payload": "bso0",oops}]`)
		header := make(http.Header)
		header.Add("Content-Type", "application/json")
		resp := requestheaders("POST", url, body, header, handler)
		if assert.Equal(resp.Code, http.StatusBadRequest) {
			assert.Equal(WEAVE_INVALID_WBO, resp.Body.String())
		}
	}

	{ // test actual max BSO verify works
		handler := NewSyncUserHandler(uid, db, nil)
		// set the config to super low values
		body := bytes.NewBufferString(`[{"id":"bso0", "payload": "bso0"},{"id":"bso1", "payload": "bso1"}]`)
		handler.config.MaxPOSTRecords = 1

		header := make(http.Header)
		header.Add("Content-Type", "application/json")
		resp := requestheaders("POST", url, body, header, handler)
		assert.Equal(resp.Code, http.StatusRequestEntityTooLarge)
	}

	{ // test bad post data verify
		handler := NewSyncUserHandler(uid, db, nil)
		// set the config to super low values
		body := bytes.NewBufferString(`[{"id":"bso0","payload": "bso0"},{"x":"broken"}]`)
		header := make(http.Header)
		header.Add("Content-Type", "application/json")

		// set/get the last modified time
		someData := bytes.NewBufferString(`[{"id":"bsoD","payload": "bsoD"}]`)
		respInfo := requestheaders("POST", syncurl(uid, "storage/bookmarks"), someData, header, handler)
		if assert.Equal(http.StatusOK, respInfo.Code) {
			lastModified := respInfo.Header().Get("X-Last-Modified")
			assert.NotEqual("", lastModified)

			resp := requestheaders("POST", url, body, header, handler)
			if assert.Equal(resp.Code, http.StatusOK) {
				p := new(PostResults)
				if assert.NoError(json.Unmarshal(resp.Body.Bytes(), &p)) {
					assert.Len(p.Success, 0)
					assert.Len(p.Failed, 1)
					assert.Equal(resp.Header().Get("X-Last-Modified"), lastModified)
				}
			}
		}
	}

	{ // test invalid bso id..
		handler := NewSyncUserHandler(uid, db, nil)
		body := bytes.NewBufferString(`[{"id":"(╯°□°）╯︵ ┻━┻)","payload":"should be valid IMO"}]`)
		header := make(http.Header)
		header.Add("Content-Type", "application/json")
		resp := requestheaders("POST", url, body, header, handler)
		p := new(PostResults)
		if assert.NoError(json.Unmarshal(resp.Body.Bytes(), &p)) {
			if assert.NotEmpty(p.Failed["na"]) {
				assert.Equal("Invalid BSO id (╯°□°）╯︵ ┻━┻)", p.Failed["na"][0])
			}
		}
	}

	{ // test invalid sortindex / ttl bso data
		handler := NewSyncUserHandler(uid, db, nil)
		// sortindex should be 9 digits, ttl >= 1
		body := bytes.NewBufferString(`[
			{"id":"bso0", "payload":"bso0"},
			{"id":"bso1", "payload":"bso1", "sortindex":1234567890},
			{"id":"bso2", "payload":"bso2", "ttl":-1}
		]`)
		header := make(http.Header)
		header.Add("Content-Type", "application/json")
		resp := requestheaders("POST", url, body, header, handler)
		if assert.Equal(http.StatusAccepted, resp.Code) {
			p := new(PostResults)
			if assert.NoError(json.Unmarshal(resp.Body.Bytes(), &p)) {
				if assert.NotEmpty(p.Success) {
					assert.Equal("bso0", p.Success[0])
				}
				if assert.NotEmpty(p.Failed["bso1"]) {
					assert.Equal("Invalid sort index for: bso1", p.Failed["bso1"][0])
				}
				if assert.NotEmpty(p.Failed["bso2"]) {
					assert.Equal("Invalid TTL for: bso2", p.Failed["bso2"][0])
				}
			}
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

		bsoNew, err := db.GetBSO(colId, "bso0")
		if !assert.NoError(err) {
			return
		}
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

	{ // test fail on invalid content type
		header := make(http.Header)
		header.Add("Content-Type", "not-application/json")
		body := bytes.NewBufferString(`{"payload": "1234"}`)
		resp := requestheaders("PUT", url, body, header, handler)
		if !assert.Equal(http.StatusUnsupportedMediaType, resp.Code) {
			return
		}
	}

	var badCrypto = `{"id":"keys", "payload":"{\"ciphertext\":\"IDontKnowWhatImDoing\",\"IV\":\"AAAAAAAAAAAAAAAAAAAAAA==\"}"}`
	{ // POST to storage/crypto w/ bad data fails
		resp := requestheaders("PUT",
			syncurl(uid, "storage/crypto/keys"),
			bytes.NewBufferString(badCrypto), header, handler)
		assert.Equal(http.StatusBadRequest, resp.Code)
		assert.Equal(`{"err":"Known-bad BSO payload"}`, resp.Body.String())
	}

	{ // POST to other collections w/ bad data is ok
		resp := requestheaders("PUT",
			syncurl(uid, "storage/col2/keys"),
			bytes.NewBufferString(badCrypto), header, handler)
		assert.Equal(http.StatusOK, resp.Code)
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

func TestSyncUserHandlerCollectionGET(t *testing.T) {

	assert := assert.New(t)
	uid := uniqueUID()
	db, _ := syncstorage.NewDB(":memory:", nil)
	handler := NewSyncUserHandler(uid, db, nil)

	header := make(http.Header)
	header.Add("Content-Type", "application/json")

	{ // empty collections return an empty array
		resp := request("GET", syncurl(uid, "storage/empty"), nil, handler)
		assert.Equal(http.StatusOK, resp.Code, resp.Body.String())
		assert.Equal("[]", resp.Body.String())
	}

	{ // 5 testing BSOs
		numToCreate := 5
		for i := 1; i <= numToCreate; i++ {
			bId := fmt.Sprintf("b%d", i)
			body := fmt.Sprintf(`{"payload":"-","sortindex":%d}`, i)
			resp := requestheaders("PUT", syncurl(uid, "storage/test/"+bId), bytes.NewBufferString(body), header, handler)
			assert.Equal(http.StatusOK, resp.Code)
		}
	}

	{ // test sort=newest
		resp := request("GET", syncurl(uid, "storage/test?sort=newest"), nil, handler)
		assert.Equal(http.StatusOK, resp.Code, resp.Body.String())
		assert.Equal(`["b5","b4","b3","b2","b1"]`, resp.Body.String())

		assert.NotEqual("", resp.Header().Get("X-Last-Modified"))
	}

	{ // sort=oldest
		resp := request("GET", syncurl(uid, "storage/test?sort=oldest"), nil, handler)
		assert.Equal(http.StatusOK, resp.Code, resp.Body.String())
		assert.Equal(`["b1","b2","b3","b4","b5"]`, resp.Body.String())
	}

	{ //full=true and return data is correct
		resp := request("GET", syncurl(uid, "storage/test?sort=oldest&full=y&ids=b5,b1"), nil, handler)
		assert.Equal(http.StatusOK, resp.Code, resp.Body.String())

		var results jsResult
		if !assert.NoError(json.Unmarshal(resp.Body.Bytes(), &results), resp.Body.String()) {
			return
		}

		if !assert.Len(results, 2) {
			return
		}

		assert.Equal("b1", results[0].Id)
		assert.Equal("b5", results[1].Id)
		assert.Equal("-", results[0].Payload)
		assert.Equal("-", results[1].Payload)
		assert.Equal(1, results[0].SortIndex)
		assert.Equal(5, results[1].SortIndex)
		assert.True(results[1].Modified > results[0].Modified)
	}

	{ // test newer parameter
		resp := request("GET", syncurl(uid, "storage/test?full=y&ids=b3"), nil, handler)
		assert.Equal(http.StatusOK, resp.Code, resp.Body.String())
		var results jsResult
		if !assert.NoError(json.Unmarshal(resp.Body.Bytes(), &results), resp.Body.String()) {
			return
		}

		if !assert.Len(results, 1) {
			return
		}

		// make sure we get the the two BSOs that are newer than b3
		modified := fmt.Sprintf("%.02f", results[0].Modified)
		resp2 := request("GET", syncurl(uid, "storage/test?sort=oldest&newer="+modified), nil, handler)
		assert.Equal(http.StatusOK, resp2.Code, resp2.Body.String())
		assert.Equal(`["b4","b5"]`, resp2.Body.String())

	}

	{ // test limit+offset works
		resp := request("GET", syncurl(uid, "storage/test?sort=oldest&limit=2"), nil, handler)
		assert.Equal(http.StatusOK, resp.Code, resp.Body.String())
		assert.Equal(`["b1","b2"]`, resp.Body.String())
		assert.Equal("2", resp.Header().Get("X-Weave-Next-Offset"))

		resp2 := request("GET", syncurl(uid, "storage/test?sort=oldest&limit=2&offset=2"), nil, handler)
		assert.Equal(`["b3","b4"]`, resp2.Body.String())
		assert.Equal("4", resp2.Header().Get("X-Weave-Next-Offset"))

		resp3 := request("GET", syncurl(uid, "storage/test?sort=oldest&limit=2&offset=4"), nil, handler)
		assert.Equal(`["b5"]`, resp3.Body.String())
		assert.Equal("", resp3.Header().Get("X-Weave-Next-Offset"))
	}
}

func TestSyncUserHandlerBsoGET(t *testing.T) {

	assert := assert.New(t)
	uid := uniqueUID()
	db, _ := syncstorage.NewDB(":memory:", nil)
	handler := NewSyncUserHandler(uid, db, nil)

	{ // invalid collections = 404
		resp := request("GET", syncurl(uid, "storage/fakecol/b0"), nil, handler)
		assert.Equal(http.StatusNotFound, resp.Code, resp.Body.String())
	}

	{
		header := make(http.Header)
		header.Add("Content-Type", "application/json")
		body := `{"payload":"-","sortindex":9}`
		resp := requestheaders("PUT", syncurl(uid, "storage/test/b0"),
			bytes.NewBufferString(body), header, handler)
		assert.Equal(http.StatusOK, resp.Code)
	}

	{
		resp := request("GET", syncurl(uid, "storage/test/b0"), nil, handler)
		assert.Equal(http.StatusOK, resp.Code, resp.Body.String())

		var result jsonBSO
		if !assert.NoError(json.Unmarshal(resp.Body.Bytes(), &result), resp.Body.String()) {
			return
		}

		assert.Equal("b0", result.Id)
		assert.Equal("-", result.Payload)
		assert.Equal(9, result.SortIndex)
	}
}

func TestSyncUserHandlerBsoDELETE(t *testing.T) {

	assert := assert.New(t)
	uid := uniqueUID()
	db, _ := syncstorage.NewDB(":memory:", nil)
	handler := NewSyncUserHandler(uid, db, nil)

	{
		header := make(http.Header)
		header.Add("Content-Type", "application/json")
		body := `{"payload":"-","sortindex":9}`
		respPut := requestheaders("PUT", syncurl(uid, "storage/test/b0"),
			bytes.NewBufferString(body), header, handler)
		assert.Equal(http.StatusOK, respPut.Code)

		respGet := request("GET", syncurl(uid, "storage/test/b0"), nil, handler)
		assert.Equal(http.StatusOK, respGet.Code, respGet.Body.String())

		respDelete := request("DELETE", syncurl(uid, "storage/test/b0"), nil, handler)
		assert.Equal(http.StatusOK, respDelete.Code, respDelete.Body.String())
		assert.NotEqual("", respDelete.Header().Get("X-Last-Modified"))
		assert.NotEqual(respGet.Header().Get("X-Last-Modified"),
			respDelete.Header().Get("X-Last-Modified"))

		respGet2 := request("GET", syncurl(uid, "storage/test/b0"), nil, handler)
		assert.Equal(http.StatusNotFound, respGet2.Code, respGet2.Body.String())

		// should 404 now that it is gone
		respDelete404 := request("DELETE", syncurl(uid, "storage/test/b0"), nil, handler)
		assert.Equal(http.StatusNotFound, respDelete404.Code, respDelete404.Body.String())
	}

	{ // test errors on nonexistant collection
		resp := request("DELETE", syncurl(uid, "storage/fakecollection/b0"), nil, handler)
		assert.Equal(http.StatusNotAcceptable, resp.Code, resp.Body.String())
	}
}

func TestSyncUserHandlerCollectionDelete(t *testing.T) {

	assert := assert.New(t)
	uid := uniqueUID()
	db, _ := syncstorage.NewDB(":memory:", nil)
	handler := NewSyncUserHandler(uid, db, nil)

	{ // delete a collection that doesn't exist
		resp := request("DELETE", syncurl(uid, "storage/test"), nil, handler)
		assert.Equal(http.StatusNotFound, resp.Code, resp.Body.String())
	}

	{ // test deleting specific IDs
		body := bytes.NewBufferString(`[
			{"id":"b1", "payload": "-","sortindex":1},
			{"id":"b2", "payload": "-","sortindex":2},
			{"id":"b3", "payload": "-","sortindex":3},
			{"id":"b4", "payload": "-","sortindex":4}
		]`)

		// POST new data
		header := make(http.Header)
		header.Add("Content-Type", "application/json")
		respPOST := requestheaders("POST", syncurl(uid, "storage/col"), body, header, handler)
		assert.Equal(http.StatusOK, respPOST.Code, respPOST.Body.String())

		respDEL := request("DELETE", syncurl(uid, "storage/col?ids=b1,b4,b5"), nil, handler)
		assert.Equal(http.StatusOK, respDEL.Code, respDEL.Body.String())
		assert.NotEqual("", respDEL.Header().Get("X-Last-Modified"))

		respGET := request("GET", syncurl(uid, "storage/col?sort=index"), nil, handler)
		assert.Equal(http.StatusOK, respGET.Code, respGET.Body.String())
		assert.Equal(`["b3","b2"]`, respGET.Body.String()) // highest weight sortindex first
	}

	{ // test deleting specific IDs
		body := bytes.NewBufferString(`[
			{"id":"b1", "payload": "-"},
			{"id":"b2", "payload": "-"},
			{"id":"b3", "payload": "-"}
		]`)

		// POST new data
		header := make(http.Header)
		header.Add("Content-Type", "application/json")
		respPOST := requestheaders("POST", syncurl(uid, "storage/col"), body, header, handler)
		assert.Equal(http.StatusOK, respPOST.Code, respPOST.Body.String())

		respDEL := request("DELETE", syncurl(uid, "storage/col"), nil, handler)
		assert.Equal(http.StatusOK, respDEL.Code, respDEL.Body.String())

		respGET := request("GET", syncurl(uid, "storage/col"), nil, handler)
		assert.Equal(http.StatusOK, respGET.Code, respGET.Body.String())
		assert.Equal(`[]`, respGET.Body.String())
	}

	{ // test limit of deleting ids
		// modifies the handler's config so do this last to avoid sidefeccts
		handler.config.MaxPOSTRecords = 1
		respDEL := request("DELETE", syncurl(uid, "storage/col?ids=a,b,c"), nil, handler)
		assert.Equal(http.StatusBadRequest, respDEL.Code, respDEL.Body.String())
	}
}

func TestSyncUserHandlerDeleteEverything(t *testing.T) {
	assert := assert.New(t)

	uid := uniqueUID()
	db, _ := syncstorage.NewDB(":memory:", nil)
	handler := NewSyncUserHandler(uid, db, nil)

	header := make(http.Header)
	header.Add("Content-Type", "application/json")
	for _, col := range []string{"bookmarks", "history", "passwords"} {
		body := bytes.NewBufferString(`{"id":"bso1", "payload": "ppp", "sortindex": 1, "ttl": 2100000}`)
		resp := requestheaders("PUT", syncurl(uid, "storage/"+col+"/bso1"), body, header, handler)
		if !assert.Equal(http.StatusOK, resp.Code, col) {
			return
		}
	}

	resp := request("GET", syncurl(uid, "info/collection_counts"), nil, handler)
	assert.Equal(http.StatusOK, resp.Code)
	assert.Equal(`{"bookmarks":1,"history":1,"passwords":1}`, resp.Body.String())

	respDelete := request("DELETE", syncurl(uid, "storage"), nil, handler)
	assert.Equal(http.StatusOK, respDelete.Code)

	respCheck := request("GET", syncurl(uid, "info/collection_counts"), nil, handler)
	assert.Equal(http.StatusOK, respCheck.Code)
	assert.Equal(`{}`, respCheck.Body.String())

	for _, col := range []string{"bookmarks", "history", "passwords"} {
		resp := request("GET", syncurl(uid, "storage/"+col+"/bso1"), nil, handler)
		assert.Equal(http.StatusNotFound, resp.Code)
	}
}
