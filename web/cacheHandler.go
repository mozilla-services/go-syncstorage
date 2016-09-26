package web

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"regexp"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/allegro/bigcache"
)

var (
	infoCollectionsRoute   *regexp.Regexp
	infoConfigurationRoute *regexp.Regexp

	DefaultCacheHandlerConfig = CacheConfig{
		MaxCacheSize: 256,
	}
)

func init() {
	infoCollectionsRoute = regexp.MustCompile(`^/1\.5/([0-9]+)/info/collections$`)
	infoConfigurationRoute = regexp.MustCompile(`^/1\.5/([0-9]+)/info/configuration$`)
}

type CacheConfig struct {
	MaxCacheSize int // megabytes
}

// CacheHandler contains logic for caching and speeding up
// requests that do not need to go to disk. Endpoints such as
// info/collections and info/configuration can be cached and
// served out of RAM.
type CacheHandler struct {
	handler http.Handler

	cache *bigcache.BigCache
}

func NewCacheHandler(handler http.Handler, cacheConfig CacheConfig) *CacheHandler {
	bcConfig := bigcache.DefaultConfig(time.Hour)
	bcConfig.HardMaxCacheSize = cacheConfig.MaxCacheSize

	// use to calculate initial size
	bcConfig.MaxEntrySize = 256 // bytes, should fit almost all responses
	bcConfig.LifeWindow = 2000  // number of unique users seen in time.Hour

	cache, err := bigcache.NewBigCache(bcConfig)
	if err != nil {
		log.WithFields(log.Fields{
			"err": err.Error(),
		}).Panic("Could not create Cache bigcache")
	}

	return &CacheHandler{
		handler: handler,
		cache:   cache,
	}
}

func (s *CacheHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	var uid string

	if session, ok := SessionFromContext(req.Context()); ok {
		uid = session.Token.UidString()
	} else {
		sendRequestProblem(w, req, http.StatusBadRequest, errors.New("CacheHandler no UID"))
		return
	}

	if req.Method == "GET" && infoCollectionsRoute.MatchString(req.URL.Path) { // info/collections
		s.infoCollection(uid, w, req)
	} else if req.Method == "GET" && infoConfigurationRoute.MatchString(req.URL.Path) { // info/configuration
		s.infoConfiguration(uid, w, req)
	} else {
		// clear the cache for the  user
		if req.Method == "POST" || req.Method == "PUT" || req.Method == "DELETE" {
			if log.GetLevel() == log.DebugLevel {
				log.WithFields(log.Fields{
					"uid": uid,
				}).Debug("CacheHandler clear")
			}
			s.cache.Set(uid, nil)
		}
		s.handler.ServeHTTP(w, req)
		return
	}
}

// for serialization of the json body and last modified header
// values into one []byte. The X-Last-Modified timestamp is 13 bytes
// ie: 1234567890.12.
// TODO: update this to 14 before my 307th birthday on Nov 20th, 2286.
const lastModifiedBytes = 13

// infoCollection caches a user's info/collection data. It will clear
// the cached data if a POST, PUT, or DELETE method is done
func (s *CacheHandler) infoCollection(uid string, w http.ResponseWriter, req *http.Request) {
	// cache hit
	if data, err := s.cache.Get(uid); err == nil && len(data) > 0 {
		// TODO in change this
		lastModified := string(data[:lastModifiedBytes])

		if log.GetLevel() == log.DebugLevel {
			log.WithFields(log.Fields{
				"uid":      uid,
				"modified": lastModified,
				"data_len": len(data) - lastModifiedBytes,
			}).Debug("CacheHandler HIT")
		}

		modified, _ := ConvertTimestamp(lastModified)
		if sentNotModified(w, req, modified) {
			return
		}

		// add the the X-Last-Modified header
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Last-Modified", lastModified)
		io.Copy(w, bytes.NewReader(data[lastModifiedBytes:]))
		return
	}

	// cache miss...
	cacheWriter := newCacheResponseWriter(w)
	s.handler.ServeHTTP(cacheWriter, req)

	// cache the results for next time if successful response
	if cacheWriter.code == http.StatusOK {
		data := make([]byte, cacheWriter.Len()+lastModifiedBytes)

		copy(data, w.Header().Get("X-Last-Modified"))
		copy(data[lastModifiedBytes:], cacheWriter.Bytes())

		s.cache.Set(uid, data)
		if log.GetLevel() == log.DebugLevel {
			log.WithFields(log.Fields{
				"uid":      uid,
				"modified": w.Header().Get("X-Last-Modified"),
			}).Debug("CacheHandler MISS")
		}
	}
}

func (s *CacheHandler) infoConfiguration(uid string, w http.ResponseWriter, req *http.Request) {
	if data, err := s.cache.Get("config"); err == nil && len(data) > 0 {
		// add the the X-Last-Modified header
		w.Header().Set("Content-Type", "application/json")
		io.Copy(w, bytes.NewReader(data))
		return
	}

	// fill the cache ...
	cacheWriter := newCacheResponseWriter(w)
	s.handler.ServeHTTP(cacheWriter, req)

	// cache the results for next time if successful response
	if cacheWriter.code == http.StatusOK {
		s.cache.Set("config", cacheWriter.Bytes())
	}
}

type cacheResponseWriter struct {
	w    http.ResponseWriter /// original wrapped ResponseWriter
	buf  *bytes.Buffer
	mw   io.Writer
	code int
}

func (c *cacheResponseWriter) Header() http.Header {
	return c.w.Header()
}

func (c *cacheResponseWriter) WriteHeader(i int) {
	c.code = i
	c.w.WriteHeader(i)
}

func (c *cacheResponseWriter) Write(b []byte) (int, error) {
	return c.mw.Write(b)
}

func (c *cacheResponseWriter) Bytes() []byte {
	return c.buf.Bytes()
}

func (c *cacheResponseWriter) Len() int {
	return c.buf.Len()
}

func newCacheResponseWriter(w http.ResponseWriter) *cacheResponseWriter {
	buffer := new(bytes.Buffer)
	return &cacheResponseWriter{
		w:    w,
		buf:  buffer,
		mw:   io.MultiWriter(w, buffer),
		code: http.StatusOK,
	}
}
