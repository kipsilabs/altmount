package parser

import (
	"container/list"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kipsilabs/altmount/internal/importer/parser/par2"
	"github.com/javi11/nntppool/v5"
)

// What a parse learns from the wire is immutable per message-id: an article's
// yEnc header, its first 16 KiB, the descriptors in a PAR2 index. A retried or
// repair-resumed import re-parses the same NZB, and without this it fetched
// all of it again — for a deferred release that is every first segment plus
// the PAR2 index, paid twice. The cache is process-local and bounded; a
// restart simply starts cold.
const (
	defaultHeadCacheBytes = 64 << 20
	defaultHeadCacheTTL   = 24 * time.Hour
	maxDescriptorSets     = 256
)

// articleHead is what one article taught: its header, and up to 16 KiB of it.
type articleHead struct {
	meta  nntppool.YEncMeta
	bytes []byte
}

type headEntry struct {
	id   string
	head articleHead
	at   time.Time
}

// headCache is an LRU of article heads keyed by message-id, bounded by bytes
// and age.
type headCache struct {
	mu       sync.Mutex
	budget   int64
	ttl      time.Duration
	used     int64
	order    *list.List
	entries  map[string]*list.Element
	now      func() time.Time
	descMu   sync.Mutex
	descSets map[string]descriptorSet
}

type descriptorSet struct {
	descriptors map[[16]byte]*par2.FileDescriptor
	at          time.Time
}

func newHeadCache(budget int64, ttl time.Duration) *headCache {
	return &headCache{
		budget:   budget,
		ttl:      ttl,
		order:    list.New(),
		entries:  make(map[string]*list.Element),
		now:      time.Now,
		descSets: make(map[string]descriptorSet),
	}
}

func entrySize(h articleHead) int64 { return int64(len(h.bytes)) + 64 }

// get returns the cached head for a message-id, if present and fresh.
func (c *headCache) get(id string) (articleHead, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.entries[id]
	if !ok {
		return articleHead{}, false
	}
	e := el.Value.(*headEntry)
	if c.now().Sub(e.at) > c.ttl {
		c.remove(el)
		return articleHead{}, false
	}
	c.order.MoveToFront(el)
	return e.head, true
}

// put stores a head. A header-only put never replaces an entry that already
// carries bytes: the bytes are what a later phase (16 KiB completion, archive
// header reads) comes back for.
func (c *headCache) put(id string, head articleHead) {
	size := entrySize(head)
	if size > c.budget {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.entries[id]; ok {
		e := el.Value.(*headEntry)
		if len(head.bytes) == 0 && len(e.head.bytes) > 0 {
			head.bytes = e.head.bytes
			size = entrySize(head)
		}
		c.used -= entrySize(e.head)
		e.head = head
		e.at = c.now()
		c.used += size
		c.order.MoveToFront(el)
	} else {
		el := c.order.PushFront(&headEntry{id: id, head: head, at: c.now()})
		c.entries[id] = el
		c.used += size
	}
	for c.used > c.budget && c.order.Len() > 0 {
		c.remove(c.order.Back())
	}
}

func (c *headCache) remove(el *list.Element) {
	e := el.Value.(*headEntry)
	c.used -= entrySize(e.head)
	delete(c.entries, e.id)
	c.order.Remove(el)
}

// descriptorKey identifies a PAR2 index read by the articles it was read from.
func descriptorKey(cache []*par2.FirstSegmentData) string {
	ids := make([]string, 0, len(cache))
	for _, d := range cache {
		if d != nil && d.File != nil && len(d.File.Segments) > 0 && par2.HasMagicBytes(d.RawBytes) {
			ids = append(ids, d.File.Segments[0].ID)
		}
	}
	if len(ids) == 0 {
		return ""
	}
	sort.Strings(ids)
	return strings.Join(ids, "\x00")
}

func (c *headCache) getDescriptors(key string) (map[[16]byte]*par2.FileDescriptor, bool) {
	if key == "" {
		return nil, false
	}
	c.descMu.Lock()
	defer c.descMu.Unlock()
	set, ok := c.descSets[key]
	if !ok {
		return nil, false
	}
	if c.now().Sub(set.at) > c.ttl {
		delete(c.descSets, key)
		return nil, false
	}
	return set.descriptors, true
}

func (c *headCache) putDescriptors(key string, descriptors map[[16]byte]*par2.FileDescriptor) {
	if key == "" || len(descriptors) == 0 {
		return
	}
	c.descMu.Lock()
	defer c.descMu.Unlock()
	if len(c.descSets) >= maxDescriptorSets {
		// Drop the oldest set; a handful of releases at a time is the whole point.
		var oldest string
		var oldestAt time.Time
		for k, s := range c.descSets {
			if oldest == "" || s.at.Before(oldestAt) {
				oldest, oldestAt = k, s.at
			}
		}
		delete(c.descSets, oldest)
	}
	c.descSets[key] = descriptorSet{descriptors: descriptors, at: c.now()}
}
