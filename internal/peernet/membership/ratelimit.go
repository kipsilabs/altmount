package membership

import (
	"net"
	"net/netip"
	"sync"
	"time"
)

// RedeemLimits configures the invite redemption rate limits. Zero values use
// the defaults.
type RedeemLimits struct {
	PerIPBurst    int           // default 5
	PerIPEvery    time.Duration // one token per interval, default 12s
	GlobalBurst   int           // default 30
	GlobalEvery   time.Duration // default 1s
	MaxTrackedIPs int           // default 4096
}

func (l RedeemLimits) withDefaults() RedeemLimits {
	if l.PerIPBurst <= 0 {
		l.PerIPBurst = 5
	}
	if l.PerIPEvery <= 0 {
		l.PerIPEvery = 12 * time.Second
	}
	if l.GlobalBurst <= 0 {
		l.GlobalBurst = 30
	}
	if l.GlobalEvery <= 0 {
		l.GlobalEvery = time.Second
	}
	if l.MaxTrackedIPs <= 0 {
		l.MaxTrackedIPs = 4096
	}
	return l
}

type tokenBucket struct {
	tokens float64
	last   time.Time
}

func (b *tokenBucket) take(now time.Time, burst int, every time.Duration) bool {
	b.tokens += now.Sub(b.last).Seconds() / every.Seconds()
	if b.tokens > float64(burst) {
		b.tokens = float64(burst)
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// redeemLimiter applies a per-IP and a global token bucket with bounded
// memory. Every attempt counts, successful or not.
type redeemLimiter struct {
	mu     sync.Mutex
	cfg    RedeemLimits
	now    func() time.Time
	global tokenBucket
	ips    map[string]*tokenBucket
}

func newRedeemLimiter(cfg RedeemLimits, now func() time.Time) *redeemLimiter {
	cfg = cfg.withDefaults()
	return &redeemLimiter{cfg: cfg, now: now,
		global: tokenBucket{tokens: float64(cfg.GlobalBurst), last: now()},
		ips:    map[string]*tokenBucket{}}
}

// remoteKey groups a "host:port" remote address by IP (IPv6 by /64).
func remoteKey(remote string) string {
	host, _, err := net.SplitHostPort(remote)
	if err != nil {
		return remote
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return host
	}
	ip = ip.Unmap()
	if ip.Is6() {
		p, _ := ip.Prefix(64)
		return p.String()
	}
	return ip.String()
}

func (l *redeemLimiter) allow(remote string) bool {
	key := remoteKey(remote)
	l.mu.Lock()
	defer l.mu.Unlock()
	t := l.now()
	b, ok := l.ips[key]
	if !ok {
		if len(l.ips) >= l.cfg.MaxTrackedIPs {
			l.evict(t)
		}
		b = &tokenBucket{tokens: float64(l.cfg.PerIPBurst), last: t}
		l.ips[key] = b
	}
	if !b.take(t, l.cfg.PerIPBurst, l.cfg.PerIPEvery) {
		return false
	}
	return l.global.take(t, l.cfg.GlobalBurst, l.cfg.GlobalEvery)
}

func (l *redeemLimiter) evict(t time.Time) {
	for k, b := range l.ips {
		if t.Sub(b.last) >= time.Duration(l.cfg.PerIPBurst)*l.cfg.PerIPEvery {
			delete(l.ips, k) // refilled to full: equivalent to unseen
		}
	}
	for k := range l.ips {
		if len(l.ips) < l.cfg.MaxTrackedIPs {
			return
		}
		delete(l.ips, k)
	}
}
