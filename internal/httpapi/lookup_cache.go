package httpapi

import (
	"container/list"
	"strings"
	"sync"
	"time"

	"github.com/Team-Haruki/moe-assets-gateway/internal/store"
)

const (
	defaultLookupCacheItems = 100_000
	defaultLookupCacheTTL   = 24 * time.Hour
)

// LookupCache is a small bounded positive cache for hot asset path lookups.
// SQLite remains authoritative; entries expire quickly so commits become
// visible without a process-wide index rebuild.
type LookupCache struct {
	mu       sync.Mutex
	maxItems int
	ttl      time.Duration
	ll       *list.List
	items    map[string]*list.Element
}

type lookupCacheEntry struct {
	key     string
	value   store.Placement
	expires time.Time
}

func NewLookupCache(maxItems int, ttl time.Duration) *LookupCache {
	if maxItems < 0 {
		maxItems = 0
	}
	if ttl < 0 {
		ttl = 0
	}
	if maxItems == 0 || ttl == 0 {
		return nil
	}
	return &LookupCache{
		maxItems: maxItems,
		ttl:      ttl,
		ll:       list.New(),
		items:    make(map[string]*list.Element, maxItems),
	}
}

func (c *LookupCache) Get(key string) (store.Placement, bool) {
	if c == nil {
		return store.Placement{}, false
	}
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	elem, ok := c.items[key]
	if !ok {
		return store.Placement{}, false
	}
	entry := elem.Value.(*lookupCacheEntry)
	if now.After(entry.expires) {
		c.ll.Remove(elem)
		delete(c.items, key)
		return store.Placement{}, false
	}
	c.ll.MoveToFront(elem)
	return entry.value, true
}

func (c *LookupCache) Set(key string, value store.Placement) {
	if c == nil || !value.Found {
		return
	}
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[key]; ok {
		entry := elem.Value.(*lookupCacheEntry)
		entry.value = value
		entry.expires = now.Add(c.ttl)
		c.ll.MoveToFront(elem)
		return
	}
	elem := c.ll.PushFront(&lookupCacheEntry{
		key:     key,
		value:   value,
		expires: now.Add(c.ttl),
	})
	c.items[key] = elem
	for len(c.items) > c.maxItems {
		back := c.ll.Back()
		if back == nil {
			return
		}
		entry := back.Value.(*lookupCacheEntry)
		delete(c.items, entry.key)
		c.ll.Remove(back)
	}
}

func (c *LookupCache) Clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ll.Init()
	clear(c.items)
}

func (c *LookupCache) InvalidatePaths(paths []string) {
	if c == nil || len(paths) == 0 {
		return
	}
	pathSet := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		if path != "" {
			pathSet[path] = struct{}{}
		}
	}
	if len(pathSet) == 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	for key, elem := range c.items {
		sep := strings.IndexByte(key, '\x00')
		if sep < 0 {
			continue
		}
		if _, ok := pathSet[key[sep+1:]]; !ok {
			continue
		}
		c.ll.Remove(elem)
		delete(c.items, key)
	}
}
