package api

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mostlygeek/go-syncstorage/syncstorage"
	"github.com/stretchr/testify/assert"
)

var (
	collectionNames = []string{
		"bookmarks",
		"history",
		"forms",
		"prefs",
		"tabs",
		"passwords",
		"crypto",
		"client",
		"keys",
		"meta",
	}
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func makeTestDeps() *Dependencies {
	dir, _ := ioutil.TempDir(os.TempDir(), "sync_storage_api_test")
	dispatch, err := syncstorage.NewDispatch(4, dir, syncstorage.TwoLevelPath, 10)
	if err != nil {
		panic(err)
	}

	return &Dependencies{
		Dispatch: dispatch,
	}
}

// testRequest helps remove some boilerplate
func testRequest(method, urlStr string, body io.Reader, deps *Dependencies) *httptest.ResponseRecorder {
	req, err := http.NewRequest(method, urlStr, body)
	if err != nil {
		panic(err)
	}

	w := httptest.NewRecorder()

	if deps == nil {
		deps = makeTestDeps()
	}
	router := NewRouter(deps)
	router.ServeHTTP(w, req)
	return w
}

func TestHeartbeat(t *testing.T) {
	t.Parallel()
	w := testRequest("GET", "http://test/__heartbeat__", nil, nil)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "OK", w.Body.String())
}

func TestEchoUid(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	w := testRequest("GET", "http://test/1.5/123456/echo-uid", nil, nil)

	assert.Equal(http.StatusOK, w.Code)
	assert.Equal("123456", w.Body.String())

	// test that a non-numeric regex fails
	for _, uid := range []string{"a123", "123a", "abcd"} {
		w := testRequest("GET", "http://test/1.5/"+uid+"/echo-uid", nil, nil)
		assert.Equal(http.StatusNotFound, w.Code, "\"%s\" should not have matched route", uid)
	}

}

func TestInfoCollections(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	deps := makeTestDeps()

	uid := "123456"
	modified := syncstorage.Now()
	expected := map[string]int{
		"bookmarks": modified,
		"history":   modified + 1,
		"forms":     modified + 2,
		"prefs":     modified + 3,
		"tabs":      modified + 4,
		"passwords": modified + 5,
		"crypto":    modified + 6,
		"client":    modified + 7,
		"keys":      modified + 8,
		"meta":      modified + 9,
	}

	for cName, modified := range expected {
		cId, err := deps.Dispatch.GetCollectionId(uid, cName)
		if !assert.NoError(err, "%v", err) {
			return
		}
		err = deps.Dispatch.TouchCollection(uid, cId, modified)
		if !assert.NoError(err, "%v", err) {
			return
		}
	}

	resp := testRequest("GET", "http://test/1.5/"+uid+"/info/collections", nil, deps)

	if !assert.Equal(http.StatusOK, resp.Code) {
		return
	}

	data := resp.Body.Bytes()
	var collections map[string]int
	err := json.Unmarshal(data, &collections)
	if !assert.NoError(err) {
		return
	}

	for cName, expectedTs := range expected {
		ts, ok := collections[cName]
		if assert.True(ok, "expected '%s' collection to be set", cName) {
			assert.Equal(expectedTs, ts)
		}
	}
}

func TestInfoQuota(t *testing.T) { t.Skip("TODO") }
func TestInfoCollectionUsage(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	uid := "12345"
	deps := makeTestDeps()

	sizes := []int{463, 467, 479, 487, 491}

	for _, cName := range collectionNames {
		cId, err := deps.Dispatch.GetCollectionId(uid, cName)
		if !assert.NoError(err, "getting cID: %v", err) {
			return
		}

		for id, size := range sizes {
			payload := strings.Repeat("x", size)
			bId := fmt.Sprintf("bid_%d", id)
			_, err = deps.Dispatch.PutBSO(uid, cId, bId, &payload, nil, nil)
			if !assert.NoError(err, "failed PUT into %s, bid(%s): %v", cName, bId, err) {
				return
			}
		}
	}

	resp := testRequest("GET", "http://test/1.5/"+uid+"/info/collection_usage", nil, deps)
	data := resp.Body.Bytes()

	var collections map[string]int
	err := json.Unmarshal(data, &collections)
	if !assert.NoError(err) {
		return
	}

	var total int
	for _, s := range sizes {
		total += s
	}

	for _, cName := range collectionNames {
		assert.Equal(total, collections[cName])
	}
}

func TestCollectionCounts(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	uid := "12345"
	deps := makeTestDeps()

	expected := make(map[string]int)

	for _, cName := range collectionNames {
		expected[cName] = 5 + rand.Intn(25)
	}

	for cName, numBSOs := range expected {
		cId, err := deps.Dispatch.GetCollectionId(uid, cName)
		if !assert.NoError(err, "getting cID: %v", err) {
			return
		}

		payload := "hello"
		for i := 0; i < numBSOs; i++ {
			bId := fmt.Sprintf("bid%d", i)
			_, err = deps.Dispatch.PutBSO(uid, cId, bId, &payload, nil, nil)
			if !assert.NoError(err, "failed PUT into %s, bid(%s): %v", cName, bId, err) {
				return
			}
		}
	}

	resp := testRequest("GET", "http://test/1.5/"+uid+"/info/collection_counts", nil, deps)
	data := resp.Body.Bytes()

	var collections map[string]int
	err := json.Unmarshal(data, &collections)
	if !assert.NoError(err) {
		return
	}

	for cName, expectedCount := range expected {
		assert.Equal(expectedCount, collections[cName])
	}
}

func TestCollectionGET(t *testing.T) { t.Skip("TODO") }
func TestCollectionPOST(t *testing.T) {
	t.Skip("TODO")
}
func TestCollectionDELETE(t *testing.T) { t.Skip("TODO") }

func TestBsoGET(t *testing.T)    { t.Skip("TODO") }
func TestBsoPUT(t *testing.T)    { t.Skip("TODO") }
func TestBsoDELETE(t *testing.T) { t.Skip("TODO") }

func TestDelete(t *testing.T) { t.Skip("TODO") }

func testExtractPostRequestBSOs(t *testing.T) {

	json := `[
		{"Id":"bso1", "Payload": "testing1"},
		{"Id":"bso1", "SortIndex": 1},
		{"Id":"bso1", "TTL": 86400},
	]
	`

}
