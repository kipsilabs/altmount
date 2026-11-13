package transport

import (
	"container/list"
	"sync"
	"time"

	"github.com/kipsilabs/altmount/internal/peernet/identity"
)

type bucket struct {
	tokens   float64
	last     time.Time
	inflight int
	elem     *list.Element // position in the recency list (front = most recent)
	id       identity.NodeID
}

// peerGate enforces the per-peer and the global request rate and the per-peer
// concurrency cap with bounded memory. Peer buckets live in a map plus a
// recency list, so a request, a new identity and an eviction are all O(1)
// (apart from skipping peers with requests in flight when evicting). When the
// table is full the least recently used idle peer is evicted, so a flood of
// new identities cannot push out peers that are actively in use ahead of idle
// ones, and costs the attacker one global-budget token per attempt.
type peerGate struct {
	mu       sync.Mutex
	rate     float64
	burst    float64
	maxConc  int
	maxPeers int
	now      func() time.Time
	peers    map[identity.NodeID]*bucket
	order    *list.List

	// global budget shared by every peer
	gRate   float64
	gBurst  float64
	gTokens float64
	gLast   time.Time
}

func newPeerGate(l Limits, now func() time.Time) *peerGate {
	return &peerGate{
		rate: l.RatePerSecond, burst: float64(l.RateBurst), maxConc: l.MaxConcurrentPerPeer,
		maxPeers: l.MaxTrackedPeers, now: now, peers: make(map[identity.NodeID]*bucket), order: list.New(),
		gRate: l.GlobalRatePerSecond, gBurst: float64(l.GlobalRateBurst), gTokens: float64(l.GlobalRateBurst), gLast: now(),
	}
}

type gateResult int

const (
	gateOK gateResult = iota
	gateRateLimited
	gateBusy
)

func (g *peerGate) refill(b *bucket, t time.Time) {
	b.tokens += t.Sub(b.last).Seconds() * g.rate
	if b.tokens > g.burst {
		b.tokens = g.burst
	}
	b.last = t
}

func (g *peerGate) refillGlobal(t time.Time) {
	if d := t.Sub(g.gLast).Seconds(); d > 0 {
		g.gTokens += d * g.gRate
		if g.gTokens > g.gBurst {
			g.gTokens = g.gBurst
		}
	}
	g.gLast = t
}

// evictOne removes the least recently used peer that has nothing in flight.
func (g *peerGate) evictOne() {
	for e := g.order.Back(); e != nil; e = e.Prev() {
		if b := e.Value.(*bucket); b.inflight == 0 {
			g.order.Remove(e)
			delete(g.peers, b.id)
			return
		}
	}
}

// acquire takes a request slot for the peer. The caller must call release
// when the request finishes if the result is gateOK. A request refused by the
// per-peer limits does not spend the global budget.
func (g *peerGate) acquire(id identity.NodeID) gateResult {
	g.mu.Lock()
	defer g.mu.Unlock()
	t := g.now()
	b, ok := g.peers[id]
	if !ok {
		if len(g.peers) >= g.maxPeers {
			g.evictOne()
		}
		b = &bucket{tokens: g.burst, last: t, id: id}
		b.elem = g.order.PushFront(b)
		g.peers[id] = b
	} else {
		g.refill(b, t)
		g.order.MoveToFront(b.elem)
	}
	if b.tokens < 1 {
		return gateRateLimited
	}
	if b.inflight >= g.maxConc {
		return gateBusy
	}
	g.refillGlobal(t)
	if g.gTokens < 1 {
		return gateRateLimited
	}
	g.gTokens--
	b.tokens--
	b.inflight++
	return gateOK
}

func (g *peerGate) release(id identity.NodeID) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if b, ok := g.peers[id]; ok && b.inflight > 0 {
		b.inflight--
	}
}
