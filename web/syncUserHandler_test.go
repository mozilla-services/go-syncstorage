package web

import (
	"net/http"
	"testing"

	"github.com/mozilla-services/go-syncstorage/syncstorage"
	"github.com/stretchr/testify/assert"
)

func TestSyncUserHandlerStopPurgeClose(t *testing.T) {
	assert := assert.New(t)
	uid := uniqueUID()
	db, _ := syncstorage.NewDB(":memory:")
	handler := NewSyncUserHandler(uid, db)

	handler.StopHTTP()

	// assert 503 with Retry-After header
	url := syncurl(uid, "info/collections")
	resp := request("GET", url, nil, handler)
	assert.Equal(http.StatusServiceUnavailable, resp.Code)
	assert.NotEqual("", resp.Header().Get("Retry-After"))
}
