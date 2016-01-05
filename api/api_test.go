package api

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/mostlygeek/go-syncstorage/syncstorage"
	"github.com/stretchr/testify/assert"
)

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
	w := testRequest("GET", "http://test/__heartbeat__", nil, nil)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "OK", w.Body.String())
}

func TestEchoUid(t *testing.T) {
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

func TestInfoQuota(t *testing.T)           { t.Skip("TODO") }
func TestInfoCollectionUsage(t *testing.T) { t.Skip("TODO") }
func TestCollectionCounts(t *testing.T)    { t.Skip("TODO") }

func TestStorageCollectionGET(t *testing.T)    { t.Skip("TODO") }
func TestStorageCollectionPOST(t *testing.T)   { t.Skip("TODO") }
func TestStorageCollectionDELETE(t *testing.T) { t.Skip("TODO") }

func TestStorageBsoGET(t *testing.T)    { t.Skip("TODO") }
func TestStorageBsoPUT(t *testing.T)    { t.Skip("TODO") }
func TestStorageBsoDELETE(t *testing.T) { t.Skip("TODO") }

func TestDelete(t *testing.T) { t.Skip("TODO") }
