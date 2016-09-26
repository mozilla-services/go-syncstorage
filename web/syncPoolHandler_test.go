package web

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func testSyncPoolConfig() *SyncPoolConfig {
	return &SyncPoolConfig{
		Basepath:    ":memory:",
		NumPools:    1,
		MaxPoolSize: 10,
	}
}

func TestSyncPoolHandlerStatusConflict(t *testing.T) {
	if testing.Short() {
		t.Skip()
		return
	}

	assert := assert.New(t)

	uid := uniqueUID()
	handler := NewSyncPoolHandler(testSyncPoolConfig(), nil)

	el, _, err := handler.pools[0].getElement(uid)
	if !assert.NoError(err) {
		return
	}

	// !! Stop It. to simulate a TTL cleanup
	el.handler.StopHTTP()

	// test for conflict
	url := syncurl(uid, "info/collections")
	resp := request("GET", url, nil, handler)
	if !assert.Equal(http.StatusConflict, resp.Code) {
		return
	}

	retryAfter := resp.Header().Get("Retry-After")
	assert.NotEqual("", retryAfter)
}

func TestSyncPoolHandlerStop(t *testing.T) {
	assert := assert.New(t)
	handler := NewSyncPoolHandler(testSyncPoolConfig(), nil)

	uids := []string{uniqueUID(), uniqueUID(), uniqueUID()}

	for _, uid := range uids {
		url := syncurl(uid, "info/collections")
		resp := request("GET", url, nil, handler)
		assert.Equal(http.StatusOK, resp.Code)
	}

	handler.StopHTTP()

	for _, uid := range uids {
		// assert 503 with Retry-After header
		url := syncurl(uid, "info/collections")
		resp := request("GET", url, nil, handler)

		assert.Equal(http.StatusServiceUnavailable, resp.Code)
		assert.NotEqual("", resp.Header().Get("Retry-After"))
	}

	// make sure the pools are empty
	for _, pHandler := range handler.pools {
		assert.Equal(0, pHandler.lru.Len())
		assert.Equal(0, len(pHandler.elements))
		assert.Equal(0, len(pHandler.lrumap))
	}
}

func TestSyncPoolHandlerLRU(t *testing.T) {
	assert := assert.New(t)

	uid0 := uniqueUID()
	uid1 := uniqueUID()
	uid2 := uniqueUID()

	handler := NewSyncPoolHandler(testSyncPoolConfig(), nil)
	pool := handler.pools[0]

	pool.getElement(uid0)
	pool.getElement(uid1)
	pool.getElement(uid2)

	el := pool.lru.Front()

	// check order: uid2, uid1, uid0
	assert.Equal(uid2, el.Value.(*poolElement).handler.uid) // latest
	el = el.Next()
	assert.Equal(uid1, el.Value.(*poolElement).handler.uid)
	el = el.Next()
	assert.Equal(uid0, el.Value.(*poolElement).handler.uid)

	// check order: uid1, uid2, uid0
	pool.getElement(uid1)
	el = pool.lru.Front()
	assert.Equal(uid1, el.Value.(*poolElement).handler.uid)
	el = el.Next()
	assert.Equal(uid2, el.Value.(*poolElement).handler.uid)
	el = el.Next()
	assert.Equal(uid0, el.Value.(*poolElement).handler.uid)
}

func TestSyncPoolCleanupHandlers(t *testing.T) {
	handler := NewSyncPoolHandler(testSyncPoolConfig(), nil)
	pool := handler.pools[0]
	pool.getElement("1")
	pool.getElement("2")
	pool.getElement("3")

	pool.cleanupHandlers(2)
	assert.Equal(t, 1, pool.lru.Len())
}
