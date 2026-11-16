package recsync

import (
	"net/netip"
	"sync"
	"time"
)

// tokenBucket is a node-wide token bucket counting records.
type tokenBucket struct {
	rate, burst float64

	mu     sync.Mutex
	tokens float64
	last   time.Time
}

func newTokenBucket(perSecond float64, burst int) *tokenBucket {
	return &tokenBucket{rate: perSecond, burst: float64(burst), tokens: float64(burst)}
}

// take spends one token at now and reports whether one was available.
func (b *tokenBucket) take(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.last.IsZero() {
		if d := now.Sub(b.last).Seconds(); d > 0 {
			b.tokens = min(b.burst, b.tokens+d*b.rate)
		}
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// relayerBuckets gives every relayer its own token bucket, sized as a fixed
// share of the node-wide one, so a few flooding peers cannot use up the whole
// ingest rate. The table is bounded; a relayer that falls out simply starts
// again with a full bucket.
type relayerBuckets struct {
	rate, burst float64
	max         int

	mu sync.Mutex
	m  map[string]*tokenBucket
}

// relayerShare is the part of the node-wide rate one relayer may use.
const relayerShare = 4

func newRelayerBuckets(nodeRate float64, nodeBurst, max int) *relayerBuckets {
	return &relayerBuckets{
		rate:  max64(nodeRate/relayerShare, 1),
		burst: max64(float64(nodeBurst)/2, 1),
		max:   max, m: map[string]*tokenBucket{},
	}
}

func max64(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

// take spends one token of relayer's bucket.
func (r *relayerBuckets) take(relayer string, now time.Time) bool {
	r.mu.Lock()
	b := r.m[relayer]
	if b == nil {
		if len(r.m) >= r.max {
			for k := range r.m { // table full: forget an arbitrary relayer
				delete(r.m, k)
				break
			}
		}
		b = &tokenBucket{rate: r.rate, burst: r.burst, tokens: r.burst}
		r.m[relayer] = b
	}
	r.mu.Unlock()
	return b.take(now)
}

// ipStrikeNodes is how many distinct hostile node IDs behind one address it
// takes before the address itself is backed off; ipStrikeWindow the span they
// must fall in.
const (
	ipStrikeNodes  = 3
	ipStrikeWindow = 10 * time.Minute
)

// ipHostility remembers which distinct nodes were hostile from one address.
type ipHostility struct {
	max int

	mu sync.Mutex
	m  map[string]*ipHostilityEntry
}

type ipHostilityEntry struct {
	since time.Time
	nodes map[string]struct{}
}

func newIPHostility(max int) *ipHostility {
	return &ipHostility{max: max, m: map[string]*ipHostilityEntry{}}
}

// add records a hostile strike of node from ip and reports whether ipStrikeNodes
// distinct nodes have now been hostile from it inside the window. A true
// result resets the entry.
func (h *ipHostility) add(ip, node string, now time.Time) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	e := h.m[ip]
	if e == nil || now.Sub(e.since) > ipStrikeWindow {
		if e == nil && len(h.m) >= h.max {
			for k := range h.m {
				delete(h.m, k)
				break
			}
		}
		e = &ipHostilityEntry{since: now, nodes: map[string]struct{}{}}
		h.m[ip] = e
	}
	if len(e.nodes) < ipStrikeNodes {
		e.nodes[node] = struct{}{}
	}
	if len(e.nodes) >= ipStrikeNodes {
		delete(h.m, ip)
		return true
	}
	return false
}

// strikableIP reports whether an address may be backed off as a whole: a
// private, loopback or link-local address is shared by honest nodes of one
// LAN (or one gateway) and never is.
func strikableIP(ip string) bool {
	a, err := netip.ParseAddr(ip)
	if err != nil {
		return false
	}
	a = a.Unmap()
	return !a.IsPrivate() && !a.IsLoopback() && !a.IsLinkLocalUnicast() && !a.IsLinkLocalMulticast() &&
		!a.IsUnspecified() && !a.IsMulticast()
}

// pushGuard caps the pushes being handled at once per key (a node ID or a
// remote IP). The table holds only keys with a push in flight.
type pushGuard struct {
	perNode, perIP int

	mu    sync.Mutex
	nodes map[string]int
	ips   map[string]int
}

func newPushGuard(perNode, perIP, _ int) *pushGuard {
	return &pushGuard{perNode: perNode, perIP: perIP, nodes: map[string]int{}, ips: map[string]int{}}
}

// enter admits one push of node from ip ("" when unknown). On success it
// returns the release function.
func (g *pushGuard) enter(node, ip string) (func(), bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.nodes[node] >= g.perNode || (ip != "" && g.ips[ip] >= g.perIP) {
		return nil, false
	}
	g.nodes[node]++
	if ip != "" {
		g.ips[ip]++
	}
	return func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		dec := func(m map[string]int, k string) {
			if m[k] <= 1 {
				delete(m, k)
			} else {
				m[k]--
			}
		}
		dec(g.nodes, node)
		if ip != "" {
			dec(g.ips, ip)
		}
	}, true
}

// tally counts events per key inside a fixed window and is bounded in size.
type tally struct {
	max int

	mu sync.Mutex
	m  map[string]*tallyEntry
}

type tallyEntry struct {
	n     int
	since time.Time
}

func newTally(max int) *tally { return &tally{max: max, m: map[string]*tallyEntry{}} }

// add adds n events to key at now and returns the count inside the window.
func (t *tally) add(key string, n int, now time.Time, window time.Duration) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.m[key]
	if e == nil || now.Sub(e.since) > window {
		if e == nil && len(t.m) >= t.max {
			for k := range t.m { // table full: forget an arbitrary entry
				delete(t.m, k)
				break
			}
		}
		e = &tallyEntry{since: now}
		t.m[key] = e
	}
	e.n += n
	return e.n
}

func (t *tally) reset(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.m, key)
}

// epochGuard bounds how often one peer may change its epoch per hour. A peer
// that flips its epoch makes the puller start over each time.
type epochGuard struct {
	max int
	t   *tally
}

func newEpochGuard(max, tracked int) *epochGuard { return &epochGuard{max: max, t: newTally(tracked)} }

// allow records one epoch reset of key at now and reports whether it is still
// within the hourly bound.
func (g *epochGuard) allow(key string, now time.Time) bool {
	return g.t.add(key, 1, now, time.Hour) <= g.max
}
