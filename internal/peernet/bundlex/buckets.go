package bundlex

import (
	"net/netip"
	"sync"
	"time"
)

// bucket is a token bucket measured in arbitrary units.
type bucket struct {
	tokens float64
	last   time.Time
}

// buckets is a bounded set of token buckets keyed by string.
type buckets struct {
	rate, burst float64 // units per second, units
	max         int

	mu sync.Mutex
	m  map[string]*bucket
}

func newBuckets(perHour, burst, max int) *buckets {
	return &buckets{rate: float64(perHour) / 3600, burst: float64(burst), max: max, m: map[string]*bucket{}}
}

// take spends n units of key's bucket at now.
func (b *buckets) take(key string, n float64, now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	e := b.m[key]
	if e == nil {
		if len(b.m) >= b.max {
			oldest, at := "", time.Time{}
			for k, v := range b.m { // full: forget the least recently used key
				if oldest == "" || v.last.Before(at) {
					oldest, at = k, v.last
				}
			}
			delete(b.m, oldest)
		}
		e = &bucket{tokens: b.burst, last: now}
		b.m[key] = e
	}
	if d := now.Sub(e.last).Seconds(); d > 0 {
		e.tokens = min(b.burst, e.tokens+d*b.rate)
	}
	e.last = now
	if e.tokens < n {
		return false
	}
	e.tokens -= n
	return true
}

// give returns n units to key's bucket (undoing a take).
func (b *buckets) give(key string, n float64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if e := b.m[key]; e != nil {
		e.tokens = min(b.burst, e.tokens+n)
	}
}

// counter caps concurrent holders per key.
type counter struct {
	mu sync.Mutex
	m  map[string]int
}

func newCounter() *counter { return &counter{m: map[string]int{}} }

func (c *counter) enter(key string, max int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m[key] >= max {
		return false
	}
	c.m[key]++
	return true
}

func (c *counter) leave(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m[key] <= 1 {
		delete(c.m, key)
	} else {
		c.m[key]--
	}
}

// sharedIP reports whether ip may be shared by honest nodes (private,
// loopback, link-local, unspecified): such an address is never limited or
// struck as a whole. Unparsable addresses count as shared too.
func sharedIP(ip string) bool {
	a, err := netip.ParseAddr(ip)
	if err != nil {
		return true
	}
	a = a.Unmap()
	return a.IsPrivate() || a.IsLoopback() || a.IsLinkLocalUnicast() || a.IsLinkLocalMulticast() ||
		a.IsUnspecified() || a.IsMulticast()
}
