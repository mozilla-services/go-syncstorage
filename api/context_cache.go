package api

import "sync"

// cache stores collection data reduce disk IO. The api
// endpoints: info/collections, info/collection_usage,
// info/collection_counts are all map[string]int. This
// can be squashed to []int, like: [<name idx>, int val, ...]
// to reduce memory requirements

type colMap map[string]int
type colValues []int
type usercache struct {
	// last time the user's db was modified
	modified int

	infoCollections      colValues
	infoCollectionCounts colValues
	infoCollectionUsage  colValues
	infoQuota            colValues
}

// the ram requirements to cache a user's collection information can be
// drastically reduced since there aren't that many unique collection
// names and we can just use ints to point to an list of strings
type squasher struct {
	sync.Mutex
	names    map[string]int
	namesrev map[int]*string
}

func NewSquasher() *squasher {
	return &squasher{
		names:    make(map[string]int),
		namesrev: make(map[int]*string),
	}
}

// squash reduces a colMap to a colValues
func (s *squasher) squash(d colMap) colValues {
	elements := len(d) * 2
	cValues := make(colValues, elements, elements)
	i := 0
	for k, v := range d {
		s.Lock()
		var nameIndex int
		var ok bool
		if nameIndex, ok = s.names[k]; !ok {
			nameIndex = len(s.namesrev)
			key := k                     // copy the string
			s.namesrev[nameIndex] = &key // pointer to the string
			s.names[key] = nameIndex
		}
		s.Unlock()

		cValues[i] = nameIndex
		i = i + 1
		cValues[i] = v
		i = i + 1
	}

	return cValues
}

func (s *squasher) expand(v colValues) (m colMap) {
	size := len(v) / 2
	m = make(colMap, size)
	for i := 0; i < len(v); i += 2 {
		key := *s.namesrev[v[i]]
		m[key] = v[i+1]
	}
	return
}

func NewCollectionCache() *collectionCache {
	return &collectionCache{
		squasher: squasher{
			names:    make(map[string]int),
			namesrev: make(map[int]*string),
		},
		data: make(map[string]*usercache),
	}
}

type collectionCache struct {
	sync.RWMutex
	squasher

	data map[string]*usercache
}

// Clear wipes all cached data for a user
func (c *collectionCache) Clear(uid string) {
	c.Lock()
	defer c.Unlock()
	delete(c.data, uid)
}

func (c *collectionCache) GetModified(uid string) int {
	c.RLock()
	defer c.RUnlock()

	if d, ok := c.data[uid]; ok {
		return d.modified
	}
	return 0
}

func (c *collectionCache) SetModified(uid string, modified int) {
	c.Lock()
	defer c.Unlock()

	if c.data[uid] == nil {
		c.data[uid] = &usercache{
			modified: modified,
		}
	} else {
		c.data[uid].modified = modified
	}
}

func (c *collectionCache) GetInfoCollections(uid string) colMap {
	c.RLock()
	defer c.RUnlock()

	if d, ok := c.data[uid]; ok {
		if d.infoCollections != nil {
			return c.expand(d.infoCollections)
		}
	}

	return nil
}

func (c *collectionCache) SetInfoCollections(uid string, d colMap) {
	c.Lock()
	defer c.Unlock()
	if c.data[uid] == nil {
		c.data[uid] = &usercache{
			infoCollections: c.squash(d),
		}
	} else {
		c.data[uid].infoCollections = c.squash(d)
	}
}

func (c *collectionCache) GetInfoCollectionUsage(uid string) colMap {
	c.RLock()
	defer c.RUnlock()

	if d, ok := c.data[uid]; ok {
		if d.infoCollectionUsage != nil {
			return c.expand(d.infoCollectionUsage)
		}
	}

	return nil
}

func (c *collectionCache) SetInfoCollectionUsage(uid string, d colMap) {
	c.Lock()
	defer c.Unlock()
	if c.data[uid] == nil {
		c.data[uid] = &usercache{
			infoCollectionUsage: c.squash(d),
		}
	} else {
		c.data[uid].infoCollectionUsage = c.squash(d)
	}
}

func (c *collectionCache) GetInfoCollectionCounts(uid string) colMap {
	c.RLock()
	defer c.RUnlock()

	if d, ok := c.data[uid]; ok {
		if d.infoCollectionCounts != nil {
			return c.expand(d.infoCollectionCounts)
		}
	}

	return nil
}

func (c *collectionCache) SetInfoCollectionCounts(uid string, d colMap) {
	c.Lock()
	defer c.Unlock()
	if c.data[uid] == nil {
		c.data[uid] = &usercache{
			infoCollectionCounts: c.squash(d),
		}
	} else {
		c.data[uid].infoCollectionCounts = c.squash(d)
	}
}
