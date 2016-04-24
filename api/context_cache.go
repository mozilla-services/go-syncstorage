package api

import (
	"bytes"
	"encoding/binary"
	"io"
	"sync"
	"time"

	"github.com/allegro/bigcache"
)

// cache stores collection data reduce disk IO. The api
// endpoints: info/collections, info/collection_usage,
// info/collection_counts are all map[string]int. Which
// is basically map[collection name]<last modified ts>
// this can be turned into []struct{int16,int64} and then
// packed into a byte array for a caching library

type colMap map[string]int

// colValue is two numbers, an Index to lookup a collection name and
// Value which the last modified timestamp
type colValue struct {
	Index uint16 // the index value mapping collection name => uint
	Value uint64 // the timestamp
}

// colValueSize is how many bytes colValue packs down into
const colValueSize = 2 + 8

type colValues []colValue

// the ram requirements to cache a user's collection information can be
// drastically reduced by using ints to refer to a string somewhere
type squasher struct {
	sync.Mutex
	names    map[string]uint16
	namesrev map[uint16]*string
}

func NewSquasher() *squasher {
	return &squasher{
		names:    make(map[string]uint16),
		namesrev: make(map[uint16]*string),
	}
}

// squash reduces a colMap to a colValues
func (s *squasher) squash(d colMap) colValues {
	elements := len(d)
	cValues := make(colValues, elements, elements)
	i := 0
	for k, value := range d {
		s.Lock()
		var nameIndex uint16
		var ok bool

		if nameIndex, ok = s.names[k]; !ok { // not in our map
			nameIndex = uint16(len(s.namesrev))
			key := k                     // copy the string
			s.namesrev[nameIndex] = &key // pointer to the string
			s.names[key] = nameIndex
		}
		s.Unlock()

		cValues[i] = colValue{nameIndex, uint64(value)}
		i = i + 1
	}

	return cValues
}

func (s *squasher) expand(values colValues) (m colMap) {
	size := len(values)
	m = make(colMap, size)
	for _, colVal := range values {
		key := *s.namesrev[colVal.Index]
		m[key] = int(colVal.Value)
	}
	return
}

// NewCollectionCache creates a cache with a 12 hour TTL and 32MB fixed size
// 32MB = ~512bytes/user = ~64K users frequent data cached in RAM
// 512bytes/user is likely very generous,
// (3 types) * (11 collections * colValSize + uidSize (25 bytes)) + 8 = 413 bytes/user's cache
func NewCollectionCache() *collectionCache {
	return NewCollectionCacheConfig(12*time.Hour, 32)
}

// NewCollectionCacheConfig creates a cache with a configurable TTL and size in MB
func NewCollectionCacheConfig(ttl time.Duration, sizeInMB int) *collectionCache {
	// default caches data for 12 hours before expiring it
	cacheConf := bigcache.DefaultConfig(ttl)
	cacheConf.HardMaxCacheSize = sizeInMB
	cacheConf.Verbose = false

	bcache, _ := bigcache.NewBigCache(cacheConf)

	return &collectionCache{
		squasher: squasher{
			names:    make(map[string]uint16),
			namesrev: make(map[uint16]*string),
		},
		cache: bcache,
	}
}

const (
	// constants used to make a namespace for cached values for a uid
	// using a single character keeps things small
	c_Modified             = "a"
	c_InfoCollection       = "b"
	c_InfoCollectionUsage  = "c"
	c_InfoCollectionCounts = "d"
)

// cacheKey creates a cache key with a cache namespace and uid
func cacheKey(namespace, uid string) string {
	return namespace + uid
}

type collectionCache struct {
	squasher
	cache *bigcache.BigCache
}

// Clear wipes all cached data for a user
func (c *collectionCache) Clear(uid string) {
	c.cache.Set(cacheKey(c_Modified, uid), EmptyData)
	c.cache.Set(cacheKey(c_InfoCollection, uid), EmptyData)
	c.cache.Set(cacheKey(c_InfoCollectionUsage, uid), EmptyData)
	c.cache.Set(cacheKey(c_InfoCollectionCounts, uid), EmptyData)
}

func (c *collectionCache) GetModified(uid string) (modified int) {
	key := cacheKey(c_Modified, uid)
	data, err := c.cache.Get(key)

	if err != nil || len(data) == 0 {
		return
	}

	var m uint64
	buf := bytes.NewReader(data)
	binary.Read(buf, binary.LittleEndian, &m)
	return int(m)
}

func (c *collectionCache) SetModified(uid string, modified int) {

	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, uint64(modified)); err != nil {
		return
	} else {
		key := cacheKey(c_Modified, uid)
		c.cache.Set(key, buf.Bytes())
	}
}

// setColMap turns a colMap into a []byte and sets it in the cache
func (c *collectionCache) setColMap(key string, data colMap) error {

	squashed := c.squash(data)
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, squashed); err != nil {
		return err
	}

	return c.cache.Set(key, buf.Bytes())
}

// getColMap turns a []byte into a colMap and returns it
func (c *collectionCache) getColMap(key string) (colMap, error) {

	data, err := c.cache.Get(key)
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return nil, nil
	}

	buf := bytes.NewReader(data)
	numElements := len(data) / colValueSize
	values := make(colValues, numElements, numElements)
	var val colValue

	// read one colValue at a time and append it to values
	for {
		if err := binary.Read(buf, binary.LittleEndian, &val); err != nil {
			if err == io.EOF {
				break
			}

			return nil, err
		}

		values = append(values, val)
	}

	m := c.expand(values)
	return m, nil
}

func (c *collectionCache) GetInfoCollections(uid string) colMap {
	if m, err := c.getColMap(cacheKey(c_InfoCollection, uid)); err != nil {
		return nil
	} else {
		return m
	}
}

func (c *collectionCache) SetInfoCollections(uid string, d colMap) {
	c.setColMap(cacheKey(c_InfoCollection, uid), d)
}

func (c *collectionCache) GetInfoCollectionUsage(uid string) colMap {
	if m, err := c.getColMap(cacheKey(c_InfoCollectionUsage, uid)); err != nil {
		return nil
	} else {
		return m
	}
}

func (c *collectionCache) SetInfoCollectionUsage(uid string, d colMap) {
	c.setColMap(cacheKey(c_InfoCollectionUsage, uid), d)
}

func (c *collectionCache) GetInfoCollectionCounts(uid string) colMap {
	if m, err := c.getColMap(cacheKey(c_InfoCollectionCounts, uid)); err != nil {
		return nil
	} else {
		return m
	}
}

func (c *collectionCache) SetInfoCollectionCounts(uid string, d colMap) {
	c.setColMap(cacheKey(c_InfoCollectionCounts, uid), d)
}
