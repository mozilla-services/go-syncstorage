package api

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	benchUid  = "12345"
	benchData = make(map[string]int)
)

func init() {
	for i := 0; i < 10; i++ {
		benchData[fmt.Sprintf("collect%d", i)] = i
	}
}

func gettestmap() colMap {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	return colMap{
		"bookmarks": r.Int(),
		"passwords": r.Int(),
		"meta":      r.Int(),
		"other":     r.Int(),
	}
}

func TestSquasherSquashExpand(t *testing.T) {
	assert := assert.New(t)

	s := NewSquasher()
	m := colMap{
		"bookmarks": 100,
		"passwords": 200,
		"meta":      300,
		"other":     400,
	}

	// squash and expand it and make sure fidelity is maintained
	d := s.expand(s.squash(m))

	assert.Equal(100, d["bookmarks"])
	assert.Equal(200, d["passwords"])
	assert.Equal(300, d["meta"])
	assert.Equal(400, d["other"])
}

func TestCollectionCacheModified(t *testing.T) {
	assert := assert.New(t)
	c := NewCollectionCache()
	uid := "10"

	assert.Equal(0, c.GetModified(uid))
	c.SetModified(uid, 10)
	assert.Equal(10, c.GetModified(uid))
	c.SetModified(uid, 11)
	assert.Equal(11, c.GetModified(uid))
	c.Clear(uid)
	assert.Equal(0, c.GetModified(uid))

}

func TestCollectionCacheInfoCollections(t *testing.T) {
	assert := assert.New(t)
	c := NewCollectionCache()

	uid := "10"
	m := gettestmap()
	c.SetInfoCollections(uid, m)
	m2 := c.GetInfoCollections(uid)
	if !assert.NotNil(m2) {
		return
	}

	assert.Equal(m, m2)

	c.Clear(uid)
	assert.Nil(c.GetInfoCollections(uid))
}

func TestCollectionCacheInfoCollectionUsageC(t *testing.T) {
	assert := assert.New(t)
	c := NewCollectionCache()

	uid := "10"
	m := gettestmap()
	c.SetInfoCollectionUsage(uid, m)
	m2 := c.GetInfoCollectionUsage(uid)
	if !assert.NotNil(m2) {
		return
	}

	assert.Equal(m, m2)

	c.Clear(uid)
	assert.Nil(c.GetInfoCollectionUsage(uid))
}

func TestCollectionCacheInfoCollectionCounts(t *testing.T) {
	assert := assert.New(t)
	c := NewCollectionCache()

	uid := "10"
	m := gettestmap()
	c.SetInfoCollectionCounts(uid, m)
	m2 := c.GetInfoCollectionCounts(uid)
	if !assert.NotNil(m2) {
		return
	}

	assert.Equal(m, m2)
	c.Clear(uid)
	assert.Nil(c.GetInfoCollectionCounts(uid))
}

func TestCollectionCacheSetAll(t *testing.T) {
	assert := assert.New(t)
	c := NewCollectionCache()
	uid := "10"
	m0 := gettestmap()
	m1 := gettestmap()
	m2 := gettestmap()

	c.SetInfoCollections(uid, m0)
	c.SetInfoCollectionCounts(uid, m1)
	c.SetInfoCollectionUsage(uid, m2)

	assert.Equal(m0, c.GetInfoCollections(uid))
	assert.Equal(m1, c.GetInfoCollectionCounts(uid))
	assert.Equal(m2, c.GetInfoCollectionUsage(uid))

	c.Clear(uid)

	assert.Nil(c.GetInfoCollections(uid))
	assert.Nil(c.GetInfoCollectionCounts(uid))
	assert.Nil(c.GetInfoCollectionUsage(uid))
}

func BenchmarkSquasherSquash(b *testing.B) {
	s := NewSquasher()
	m := gettestmap()
	for i := 0; i < b.N; i++ {
		s.squash(m)
	}
}

func BenchmarkSquasherExpand(b *testing.B) {
	s := NewSquasher()
	m := s.squash(gettestmap())

	for i := 0; i < b.N; i++ {
		s.expand(m)
	}
}

func BenchmarkCacheKey(b *testing.B) {
	for i := 0; i < b.N; i++ {
		cacheKey("A", "BCDEFGHIJK")
	}
}
func BenchmarkCollectionCacheClear(b *testing.B) {
	cache := NewCollectionCache()
	for i := 0; i < b.N; i++ {
		cache.Clear(benchUid)
	}
}
func BenchmarkCollectionCacheSetInfoCollections(b *testing.B) {
	cache := NewCollectionCache()
	for i := 0; i < b.N; i++ {
		cache.SetInfoCollections(benchUid, benchData)
	}
}
func BenchmarkCollectionCacheGetInfoCollections(b *testing.B) {
	cache := NewCollectionCache()
	cache.SetInfoCollections(benchUid, benchData)

	for i := 0; i < b.N; i++ {
		cache.GetInfoCollections(benchUid)
	}
}

func BenchmarkCollectionCacheSetInfoCollectionUsage(b *testing.B) {
	cache := NewCollectionCache()
	for i := 0; i < b.N; i++ {
		cache.SetInfoCollectionUsage(benchUid, benchData)
	}
}
func BenchmarkCollectionCacheGetInfoCollectionUsage(b *testing.B) {
	cache := NewCollectionCache()
	cache.SetInfoCollectionUsage(benchUid, benchData)

	for i := 0; i < b.N; i++ {
		cache.GetInfoCollectionUsage(benchUid)
	}
}

func BenchmarkCollectionCacheSetInfoCollectionCounts(b *testing.B) {
	cache := NewCollectionCache()
	for i := 0; i < b.N; i++ {
		cache.SetInfoCollectionCounts(benchUid, benchData)
	}
}
func BenchmarkCollectionCacheGetInfoCollectionCounts(b *testing.B) {
	cache := NewCollectionCache()
	cache.SetInfoCollectionCounts(benchUid, benchData)

	for i := 0; i < b.N; i++ {
		cache.GetInfoCollectionCounts(benchUid)
	}
}
