package discovery

import (
	"container/list"
	"net"
	"net/netip"
	"strings"
	"time"
)

// lru is a bounded map: get and put are O(1) and a full table evicts its
// least recently used entry. Not safe for concurrent use; callers hold the
// service lock.
type lru[V any] struct {
	max int
	ll  *list.List
	m   map[string]*list.Element
}

type lruEntry[V any] struct {
	key string
	val V
}

func newLRU[V any](max int) *lru[V] {
	if max < 1 {
		max = 1
	}
	return &lru[V]{max: max, ll: list.New(), m: make(map[string]*list.Element)}
}

func (l *lru[V]) len() int { return len(l.m) }

// get returns the value and marks it most recently used.
func (l *lru[V]) get(key string) (V, bool) {
	if e, ok := l.m[key]; ok {
		l.ll.MoveToFront(e)
		return e.Value.(*lruEntry[V]).val, true
	}
	var zero V
	return zero, false
}

// put stores val, evicting the least recently used entry when full.
func (l *lru[V]) put(key string, val V) {
	if e, ok := l.m[key]; ok {
		e.Value.(*lruEntry[V]).val = val
		l.ll.MoveToFront(e)
		return
	}
	if len(l.m) >= l.max {
		if back := l.ll.Back(); back != nil {
			delete(l.m, back.Value.(*lruEntry[V]).key)
			l.ll.Remove(back)
		}
	}
	l.m[key] = l.ll.PushFront(&lruEntry[V]{key: key, val: val})
}

// windowCount counts events inside a fixed window.
type windowCount struct {
	start time.Time
	n     int
}

// take records one event and reports whether it fits under limit within a
// window of length window starting at the first event.
func (w *windowCount) take(now time.Time, window time.Duration, limit int) bool {
	if w.start.IsZero() || now.Sub(w.start) >= window {
		w.start, w.n = now, 0
	}
	if w.n >= limit {
		return false
	}
	w.n++
	return true
}

// ipPrefixKey groups an address by the prefix that one party plausibly
// controls: v4bits for IPv4, v6bits for IPv6.
func ipPrefixKey(ip netip.Addr, v4bits, v6bits int) string {
	ip = ip.Unmap().WithZone("")
	bits := v6bits
	if ip.Is4() {
		bits = v4bits
	}
	p, err := ip.Prefix(bits)
	if err != nil {
		return ip.String()
	}
	return p.String()
}

// addrPrefixKey is ipPrefixKey for a "host:port" address (/24 and /48); a
// hostname is its own group.
func addrPrefixKey(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return strings.ToLower(host)
	}
	return ipPrefixKey(ip, 24, 48)
}

// helloLimiter bounds the hellos processed: per node, per source address
// prefix (so many identities from one range share one allowance) and
// globally. Every table is bounded and evicts in O(1).
type helloLimiter struct {
	nodes    *lru[time.Time]
	prefixes *lru[*windowCount]
	global   windowCount
}

func newHelloLimiter(max int) *helloLimiter {
	return &helloLimiter{nodes: newLRU[time.Time](max), prefixes: newLRU[*windowCount](max)}
}

// allow decides one hello. The node allowance is spent by every attempt that
// reaches it, valid or not.
func (h *helloLimiter) allow(l Limits, network, node string, ip netip.Addr, now time.Time) bool {
	nodeKey := network + "|" + node
	if last, ok := h.nodes.get(nodeKey); ok && now.Sub(last) < l.HelloInterval {
		return false
	}
	h.nodes.put(nodeKey, now)
	prefKey := network + "|unknown"
	if ip.IsValid() {
		prefKey = network + "|" + ipPrefixKey(ip, 24, 64)
	}
	wc, ok := h.prefixes.get(prefKey)
	if !ok {
		wc = &windowCount{}
		h.prefixes.put(prefKey, wc)
	}
	if !wc.take(now, l.HelloInterval, l.HelloPerPrefixPerInterval) {
		return false
	}
	return h.global.take(now, l.HelloGlobalWindow, l.HelloGlobalPerWindow)
}

// dialBackBudget caps contacts of unverified peers globally and per target
// prefix.
type dialBackBudget struct {
	global   []time.Time
	prefixes *lru[[]time.Time]
}

func newDialBackBudget(max int) *dialBackBudget {
	return &dialBackBudget{prefixes: newLRU[[]time.Time](max)}
}

func pruneTimes(ts []time.Time, cut time.Time) []time.Time {
	keep := ts[:0]
	for _, t := range ts {
		if t.After(cut) {
			keep = append(keep, t)
		}
	}
	return keep
}

// take consumes one slot for contacting addr, or reports that the global
// budget or the budget of addr's prefix is spent. Nothing is consumed on a
// refusal.
func (d *dialBackBudget) take(l Limits, addr string, now time.Time) bool {
	d.global = pruneTimes(d.global, now.Add(-l.DialBackWindow))
	if len(d.global) >= l.DialBacksPerWindow {
		return false
	}
	key := addrPrefixKey(addr)
	times, _ := d.prefixes.get(key)
	times = pruneTimes(times, now.Add(-l.DialBackPrefixWindow))
	if len(times) >= l.DialBacksPerPrefix {
		d.prefixes.put(key, times)
		return false
	}
	d.prefixes.put(key, append(times, now))
	d.global = append(d.global, now)
	return true
}
