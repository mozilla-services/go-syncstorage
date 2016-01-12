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

func makeTestContext() *Context {
	dir, _ := ioutil.TempDir(os.TempDir(), "sync_storage_api_test")
	dispatch, err := syncstorage.NewDispatch(4, dir, syncstorage.TwoLevelPath, 10)
	if err != nil {
		panic(err)
	}

	return &Context{
		Dispatch: dispatch,
	}
}

func request(method, urlStr string, body io.Reader, c *Context) *httptest.ResponseRecorder {
	req, err := http.NewRequest(method, urlStr, body)
	if err != nil {
		panic(err)
	}

	return sendrequest(req, c)
}

func sendrequest(req *http.Request, c *Context) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	if c == nil {
		c = makeTestContext()
	}

	router := NewRouterFromContext(c)
	router.ServeHTTP(w, req)
	return w
}

func TestContextHeartbeat(t *testing.T) {
	resp := request("GET", "/__heartbeat__", nil, nil)
	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, "OK", resp.Body.String())
}

func TestContextEchoUID(t *testing.T) {
	resp := request("GET", "/1.5/123456/echo-uid", nil, nil)
	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, "123456", resp.Body.String())
}

func TestContextInfoCollections(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	context := makeTestContext()

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
		cId, err := context.Dispatch.GetCollectionId(uid, cName)
		if !assert.NoError(err, "%v", err) {
			return
		}
		err = context.Dispatch.TouchCollection(uid, cId, modified)
		if !assert.NoError(err, "%v", err) {
			return
		}
	}

	resp := request("GET", "http://test/1.5/"+uid+"/info/collections", nil, context)

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
