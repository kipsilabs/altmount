package recsync

import (
	"sync"
	"time"

	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
)

type backoffEntry struct {
	strikes int
	until   time.Time
}

// backoff remembers peers skipped for misbehaving. The wait doubles with
// every consecutive strike up to a cap, and the table is bounded: when full,
// expired entries go first, then the one that expires soonest.
type backoff struct {
	base, cap time.Duration
	max       int

	mu sync.Mutex
	m  map[string]*backoffEntry
}

func newBackoff(l Limits, max int) *backoff {
	return &backoff{base: l.PeerBackoffBase, cap: l.PeerBackoffMax, max: max, m: map[string]*backoffEntry{}}
}

func backoffKey(nw netid.NetworkID, peer identity.NodeID) string {
	return nw.String() + "|" + peer.String()
}

func (b *backoff) strike(nw netid.NetworkID, peer identity.NodeID, now time.Time) {
	b.strikeKey(backoffKey(nw, peer), now)
}

// ipBackoffKey is the key for a remote IP (independent of the network).
func ipBackoffKey(ip string) string { return "ip|" + ip }

// strikeKey strikes an arbitrary key (a node or a remote IP).
func (b *backoff) strikeKey(k string, now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	e := b.m[k]
	if e == nil {
		b.makeRoom(now)
		e = &backoffEntry{}
		b.m[k] = e
	}
	e.strikes++
	wait := b.base
	for i := 1; i < e.strikes && wait < b.cap; i++ {
		wait *= 2
	}
	e.until = now.Add(min(wait, b.cap))
}

func (b *backoff) makeRoom(now time.Time) {
	if len(b.m) < b.max {
		return
	}
	var oldest string
	for k, e := range b.m {
		if !e.until.After(now) {
			delete(b.m, k)
			if len(b.m) < b.max {
				return
			}
			continue
		}
		if oldest == "" || e.until.Before(b.m[oldest].until) {
			oldest = k
		}
	}
	if len(b.m) >= b.max && oldest != "" {
		delete(b.m, oldest)
	}
}

func (b *backoff) until(nw netid.NetworkID, peer identity.NodeID) time.Time {
	b.mu.Lock()
	defer b.mu.Unlock()
	if e := b.m[backoffKey(nw, peer)]; e != nil {
		return e.until
	}
	return time.Time{}
}

func (b *backoff) active(nw netid.NetworkID, peer identity.NodeID, now time.Time) bool {
	return b.until(nw, peer).After(now)
}

// activeKey reports whether an arbitrary key is backed off at now.
func (b *backoff) activeKey(k string, now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	e := b.m[k]
	return e != nil && e.until.After(now)
}

func (b *backoff) clear(nw netid.NetworkID, peer identity.NodeID) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.m, backoffKey(nw, peer))
}

func (b *backoff) size() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.m)
}

// countFor returns how many peers of the network are backed off at now.
func (b *backoff) countFor(nw netid.NetworkID, now time.Time) int {
	prefix := nw.String() + "|"
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for k, e := range b.m {
		if e.until.After(now) && len(k) > len(prefix) && k[:len(prefix)] == prefix {
			n++
		}
	}
	return n
}

func (b *backoff) forgetNetwork(nw netid.NetworkID) {
	prefix := nw.String() + "|"
	b.mu.Lock()
	defer b.mu.Unlock()
	for k := range b.m {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			delete(b.m, k)
		}
	}
}

// pushBudget is a bounded per-peer token bucket counting pushed records.
type pushBudget struct {
	perMinute float64
	max       int

	mu sync.Mutex
	m  map[identity.NodeID]*budgetEntry
}

type budgetEntry struct {
	tokens float64
	last   time.Time
}

func newPushBudget(perMinute, max int) *pushBudget {
	return &pushBudget{perMinute: float64(perMinute), max: max, m: map[identity.NodeID]*budgetEntry{}}
}

// take spends n records of peer's budget at now. It reports false, spending
// nothing, when the peer has fewer than n left.
func (p *pushBudget) take(peer identity.NodeID, n int, now time.Time) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	e := p.m[peer]
	if e == nil {
		if len(p.m) >= p.max {
			p.evict(now)
		}
		e = &budgetEntry{tokens: p.perMinute, last: now}
		p.m[peer] = e
	}
	e.tokens = min(p.perMinute, e.tokens+now.Sub(e.last).Seconds()*p.perMinute/60)
	e.last = now
	if e.tokens < float64(n) {
		return false
	}
	e.tokens -= float64(n)
	return true
}

// evict drops one entry, preferring peers whose budget has fully refilled
// (forgetting them loses nothing), otherwise an arbitrary one.
func (p *pushBudget) evict(now time.Time) {
	var any identity.NodeID
	have := false
	for id, e := range p.m {
		if e.tokens+now.Sub(e.last).Seconds()*p.perMinute/60 >= p.perMinute {
			delete(p.m, id)
			return
		}
		if !have {
			any, have = id, true
		}
	}
	if have {
		delete(p.m, any)
	}
}
