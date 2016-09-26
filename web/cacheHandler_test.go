package web

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mozilla-services/go-syncstorage/syncstorage"
	"github.com/mozilla-services/go-syncstorage/token"
	"github.com/stretchr/testify/assert"
)

// generates a unique response each time so caching can be tested
var cacheMockHandler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {

	resp := struct {
		Type  string
		Value string
	}{"OK", syncstorage.ModifiedToString(syncstorage.Now())}

	w.Header().Set("X-Last-Modified", resp.Value)

	if infoCollectionsRoute.MatchString(req.URL.Path) {
		resp.Type = "info/collections"
		JSON(w, req, resp)
	} else if infoConfigurationRoute.MatchString(req.URL.Path) {
		resp.Type = "info/configuration"
		JSON(w, req, resp)
	} else {
		JSON(w, req, resp)
	}
})

func TestCacheHandlerInfoCollections(t *testing.T) {
	assert := assert.New(t)
	handler := NewCacheHandler(cacheMockHandler, DefaultCacheHandlerConfig)
	url := syncurl(uniqueUID(), "info/collections?version=1.5") // sometimes we see queries in the requests

	{
		// basic test to make sure data is what's expected and caching works
		resp := request("GET", url, nil, handler)
		assert.Equal(http.StatusOK, resp.Code)
		assert.True(strings.Contains(resp.Body.String(), `"Type":"info/collections"`))
		assert.Equal("application/json", resp.Header().Get("Content-Type"))

		resp2 := request("GET", url, nil, handler)
		assert.Equal(http.StatusOK, resp2.Code)
		assert.Equal(resp.Body.String(), resp2.Body.String(), "Expected cached value")
		assert.Equal(resp.Header().Get("X-Last-Modified"), resp2.Header().Get("X-Last-Modified"))
	}

	// test POST, PUT, DELETE invalidates the cache
	{
		var last *httptest.ResponseRecorder
		for _, method := range []string{"POST", "PUT", "DELETE"} {

			if last == nil {
				last = request("GET", url, nil, handler)
			}

			// ensures things stay the same
			resp := request("GET", url, nil, handler)
			assert.Equal(http.StatusOK, resp.Code)
			assert.Equal(last.Body.String(), resp.Body.String())
			assert.Equal(last.Header().Get("X-Last-Modified"), resp.Header().Get("X-Last-Modified"))

			// swap it
			last = resp

			// this should clear the cache
			// compensate for sync's time format..
			time.Sleep(10 * time.Millisecond)

			request(method, url, nil, handler)

			_ = method

			//// make sure things are changed
			resp = request("GET", url, nil, handler)
			assert.Equal(http.StatusOK, resp.Code)
			assert.NotEqual(last.Body.String(), resp.Body.String(), "Should have changed")
			assert.NotEqual(last.Header().Get("X-Last-Modified"), resp.Header().Get("X-Last-Modified"))

			last = resp
		}
	}

	// test that different uids get different values
	{
		resp1a := request("GET", syncurl("123", "info/collections"), nil, handler)
		resp1b := request("GET", syncurl("123", "info/collections"), nil, handler)

		time.Sleep(10 * time.Millisecond) /// SYNNNC!! .. so awesome timestamps

		resp2b := request("GET", syncurl("456", "info/collections"), nil, handler)
		resp2a := request("GET", syncurl("456", "info/collections"), nil, handler)

		assert.Equal(http.StatusOK, resp1a.Code)
		assert.Equal(http.StatusOK, resp1b.Code)
		assert.Equal(http.StatusOK, resp2a.Code)
		assert.Equal(http.StatusOK, resp2b.Code)

		// cacheing works
		assert.Equal(resp1a.Body.String(), resp1b.Body.String())
		assert.Equal(resp2a.Body.String(), resp2b.Body.String())

		// no conflicts
		assert.NotEqual(resp1a.Body.String(), resp2a.Body.String())
	}

	{ // test with a real SyncUserHandler
		uid := "123456"
		db, _ := syncstorage.NewDB(":memory:", nil)
		userHandler := NewSyncUserHandler(uid, db, nil)
		handler := NewCacheHandler(userHandler, DefaultCacheHandlerConfig)
		collections := []string{
			"clients", "crypto", "forms", "history", "keys", "meta",
			"bookmarks", "prefs", "tabs", "passwords", "addons",
		}
		for _, cName := range collections {
			cId, _ := db.GetCollectionId(cName)
			db.TouchCollection(cId, syncstorage.Now()+cId) // turn the cId into milliseconds
		}

		resp := request("GET", syncurl(uid, "info/collections"), nil, handler)
		resp2 := request("GET", syncurl(uid, "info/collections"), nil, handler)

		assert.Equal(resp.Body.String(), resp2.Body.String())
		assert.Equal(resp.Header().Get("X-Last-Modified"), resp2.Header().Get("X-Last-Modified"))
	}
}

func TestCacheHandlerInfoConfiguration(t *testing.T) {
	assert := assert.New(t)

	handler := NewCacheHandler(cacheMockHandler, DefaultCacheHandlerConfig)

	url := syncurl(uniqueUID(), "info/configuration")
	resp := request("GET", url, nil, handler)
	assert.Equal(http.StatusOK, resp.Code)
	assert.True(strings.Contains(resp.Body.String(), `"Type":"info/configuration"`))

	// info/configuration should be global across users (for now)
	url2 := syncurl(uniqueUID(), "info/configuration")
	resp2 := request("GET", url2, nil, handler)
	assert.Equal(http.StatusOK, resp2.Code)
	assert.Equal(resp.Body.String(), resp2.Body.String(), "Expected same value")
}

func BenchmarkCacheHandler(b *testing.B) {
	uid := "123456"
	db, _ := syncstorage.NewDB(":memory:", nil)
	userHandler := NewSyncUserHandler(uid, db, nil)
	handler := NewCacheHandler(userHandler, DefaultCacheHandlerConfig)
	collections := []string{
		"clients", "crypto", "forms", "history", "keys", "meta",
		"bookmarks", "prefs", "tabs", "passwords", "addons",
	}
	for _, cName := range collections {
		cId, _ := db.GetCollectionId(cName)
		db.TouchCollection(cId, cId*1000) // turn the cId into milliseconds
	}

	req, err := http.NewRequest("GET", "/storage/12345/info/collections", nil)
	req.Header.Set("Accept", "application/json")
	if err != nil {
		panic(err)
	}
	session := &Session{Token: token.TokenPayload{Uid: 12345}}
	reqCtx := req.WithContext(NewSessionContext(req.Context(), session))

	for i := 0; i < b.N; i++ {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, reqCtx)
	}
}
