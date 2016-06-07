package web

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSyncPoolHandlerStatusConflict(t *testing.T) {
	if testing.Short() {
		t.Skip()
		return
	}

	assert := assert.New(t)

	uid := uniqueUID()
	handler := NewSyncPoolHandler(":memory:", 1, time.Hour)

	el, err := handler.pools[0].getElement(uid)
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
	handler := NewSyncPoolHandler(":memory:", 1, time.Hour)

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

	handler := NewSyncPoolHandler(":memory:", 1, time.Hour)
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

func TestPoolElementLastUsed(t *testing.T) {
	assert := assert.New(t)

	handler := NewSyncPoolHandler(":memory:", 1, time.Hour)
	pool := handler.pools[0]

	uid := uniqueUID()
	el, _ := pool.getElement(uid)

	lastUsed := el.lastUsed()
	time.Sleep(time.Millisecond)

	el, _ = pool.getElement(uid)
	assert.NotEqual(lastUsed, el.lastUsed())
}

func TestPoolElementGarbageCollector(t *testing.T) {

	t.Parallel()

	assert := assert.New(t)

	ttl := 5 * time.Millisecond
	handler := NewSyncPoolHandler(":memory:", 1, ttl)

	pool := handler.pools[0]
	pool.gcCycleMax = 1 // ensure it happens fast (1ms)

	pool.getElement(uniqueUID())
	pool.getElement(uniqueUID())

	assert.Equal(2, pool.lru.Len())

	pool.startGarbageCollector()
	time.Sleep(1500 * time.Millisecond)
	assert.Equal(0, pool.lru.Len())
}
