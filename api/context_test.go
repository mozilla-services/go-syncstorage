package api

import (
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
	assert.Equal(t, "ok", resp.Body.String())
}

func TestContextEchoUID(t *testing.T) {
	resp := request("GET", "/1.5/123456/echo-uid", nil, nil)
	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, "123456", resp.Body.String())

}
