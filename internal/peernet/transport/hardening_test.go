package transport

import (
	"context"
	"errors"
	"net/http"
	"net/netip"
	"net/url"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/peernet/identity"
)

func pollUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestRemoteIPFromContext(t *testing.T) {
	got := make(chan netip.Addr, 1)
	ts := startServer(t, nil, func(s *Server) {
		must(t, s.Handle("GET /ip", 0, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip, ok := RemoteIPFromContext(r.Context())
			if !ok {
				ip = netip.Addr{}
			}
			got <- ip
			w.WriteHeader(204)
		})))
	})
	c, _ := newTestClient(t, nil)
	resp, err := get(t, c, ts, "/ip")
	must(t, err)
	_ = resp.Body.Close()
	if ip := <-got; ip != netip.MustParseAddr("127.0.0.1") {
		t.Fatalf("remote ip %v", ip)
	}
	if _, ok := RemoteIPFromContext(context.Background()); ok {
		t.Fatal("remote ip in empty context")
	}
}

func TestProxyRefusesHostnamesUnlessTrusted(t *testing.T) {
	seen := make(chan string, 4)
	purl, _ := url.Parse(connectProxy(t, 403, seen))
	c, _ := newTestClient(t, func(c *ClientConfig) {
		c.Proxy = func(*url.URL) (*url.URL, error) { return purl, nil }
	})
	req := Request{Method: "GET", Path: "/"}
	_, err := c.Do(context.Background(), "peer.example.org:7000", newKey(t).NodeID(), req, CallOptions{MaxResponseBytes: 1})
	if !errors.Is(err, ErrAddressRefused) {
		t.Fatalf("untrusted hostname through proxy: %v", err)
	}
	if _, err := c.ProbeNodeID(context.Background(), "peer.example.org:7000", CallOptions{}); !errors.Is(err, ErrAddressRefused) {
		t.Fatalf("untrusted probe hostname through proxy: %v", err)
	}
	if len(seen) != 0 {
		t.Fatal("proxy contacted for an untrusted hostname")
	}
	// Trusted configuration may use the proxy to resolve (here the proxy refuses).
	_, err = c.Do(context.Background(), "peer.example.org:7000", newKey(t).NodeID(), req, CallOptions{MaxResponseBytes: 1, TrustedAddress: true})
	if !errors.Is(err, ErrProxyConnect) {
		t.Fatalf("trusted hostname through proxy: %v", err)
	}
	// IP literals are fully checked and so stay usable when untrusted.
	_, err = c.Do(context.Background(), "127.0.0.1:9", newKey(t).NodeID(), req, CallOptions{MaxResponseBytes: 1})
	if !errors.Is(err, ErrProxyConnect) {
		t.Fatalf("untrusted literal through proxy: %v", err)
	}
}

func TestSensitivePortsRefusedUnlessTrusted(t *testing.T) {
	c, _ := newTestClient(t, nil)
	for _, p := range []string{"22", "25", "445", "3306", "5432", "6379"} {
		addr := "127.0.0.1:" + p
		_, err := c.Do(context.Background(), addr, newKey(t).NodeID(), Request{Method: "GET", Path: "/"}, CallOptions{MaxResponseBytes: 1, Timeout: time.Second})
		if !errors.Is(err, ErrAddressRefused) {
			t.Errorf("%s untrusted: %v", addr, err)
		}
		_, err = c.Do(context.Background(), addr, newKey(t).NodeID(), Request{Method: "GET", Path: "/"}, CallOptions{MaxResponseBytes: 1, Timeout: time.Second, TrustedAddress: true})
		if errors.Is(err, ErrAddressRefused) {
			t.Errorf("%s trusted must not be refused by the port list: %v", addr, err)
		}
		if _, err := c.ProbeNodeID(context.Background(), addr, CallOptions{Timeout: time.Second}); !errors.Is(err, ErrAddressRefused) {
			t.Errorf("%s probe untrusted: %v", addr, err)
		}
	}
	if !PortDenied(22) || PortDenied(7000) {
		t.Fatal("PortDenied")
	}
}

func TestCheckLearnedAddress(t *testing.T) {
	ok := []string{"8.8.8.8:7000", "[2606:4700::1]:7000"}
	for _, a := range ok {
		if _, err := CheckLearnedAddress(a, false, false); err != nil {
			t.Errorf("%s: %v", a, err)
		}
	}
	bad := []string{"peer.example.org:7000", "10.0.0.1:7000", "8.8.8.8:22", "8.8.8.8:5432", "127.0.0.1:7000", "bad"}
	for _, a := range bad {
		if _, err := CheckLearnedAddress(a, true, false); err == nil {
			t.Errorf("%s accepted", a)
		}
	}
	// Private addresses are refused even when the caller would allow them
	// for hello or configuration.
	if _, err := CheckPeerAddress("10.0.0.1:7000", true, false); err != nil {
		t.Fatalf("CheckPeerAddress must keep allowing private: %v", err)
	}
}

func gateLimits() Limits {
	l := DefaultLimits()
	l.RateBurst = 5
	l.RatePerSecond = 1
	return l
}

func TestPeerGateGlobalBudget(t *testing.T) {
	now := time.Unix(1000, 0)
	l := gateLimits()
	l.GlobalRateBurst = 3
	l.GlobalRatePerSecond = 1
	g := newPeerGate(l, func() time.Time { return now })
	// Distinct fresh identities share one budget.
	granted := 0
	for i := 0; i < 20; i++ {
		id := newKey(t).NodeID()
		if g.acquire(id) == gateOK {
			granted++
			g.release(id)
		}
	}
	if granted != 3 {
		t.Fatalf("granted %d, want 3", granted)
	}
	now = now.Add(2 * time.Second)
	id := newKey(t).NodeID()
	if g.acquire(id) != gateOK {
		t.Fatal("budget did not refill")
	}
}

func TestPeerGateRateLimitedPeerDoesNotSpendGlobalBudget(t *testing.T) {
	now := time.Unix(1000, 0)
	l := gateLimits()
	l.RateBurst = 1
	l.GlobalRateBurst = 3
	l.GlobalRatePerSecond = 0.0001
	g := newPeerGate(l, func() time.Time { return now })
	a := newKey(t).NodeID()
	if g.acquire(a) != gateOK {
		t.Fatal("first")
	}
	g.release(a)
	for i := 0; i < 10; i++ {
		if g.acquire(a) != gateRateLimited {
			t.Fatal("expected per-peer limit")
		}
	}
	if g.acquire(newKey(t).NodeID()) != gateOK {
		t.Fatal("per-peer rejections spent the global budget")
	}
}

func TestPeerGateEvictsLeastRecentlyUsedAndStaysBounded(t *testing.T) {
	now := time.Unix(1000, 0)
	l := gateLimits()
	l.MaxTrackedPeers = 3
	l.GlobalRateBurst = 1 << 20
	l.GlobalRatePerSecond = 1 << 20
	g := newPeerGate(l, func() time.Time { return now })
	active, idle1, idle2 := newKey(t).NodeID(), newKey(t).NodeID(), newKey(t).NodeID()
	for _, id := range []identity.NodeID{idle1, idle2, active} {
		if g.acquire(id) != gateOK {
			t.Fatal("seed")
		}
		g.release(id)
		now = now.Add(time.Millisecond)
	}
	// The active peer keeps using its bucket (its tokens run down).
	for i := 0; i < 3; i++ {
		if g.acquire(active) != gateOK {
			t.Fatal("active")
		}
		g.release(active)
	}
	// A flood of fresh identities evicts idle ones first, one each.
	g.acquire(newKey(t).NodeID())
	if _, ok := g.peers[idle1]; ok {
		t.Fatal("idlest peer not evicted first")
	}
	if _, ok := g.peers[active]; !ok {
		t.Fatal("recently used peer evicted ahead of an idle one")
	}
	for i := 0; i < 100; i++ {
		id := newKey(t).NodeID()
		g.acquire(id)
		g.release(id)
		if len(g.peers) > 3 || g.order.Len() != len(g.peers) {
			t.Fatalf("table not bounded/consistent: %d %d", len(g.peers), g.order.Len())
		}
	}
}

// connStats reports how many connections the server admitted and how many are
// still open.
func (ts *testServer) connStats() (accepted, active int) {
	ts.mu.Lock()
	ln := ts.ln.(*limitedTLSListener)
	ts.mu.Unlock()
	ln.mu.Lock()
	defer ln.mu.Unlock()
	return ln.accepted, ln.total
}

func FuzzCheckLearnedAddress(f *testing.F) {
	f.Add("8.8.8.8:7000", false)
	f.Add("peer.example.org:80", true)
	f.Add("[fd00::1]:1", true)
	f.Add("8.8.8.8:22", false)
	f.Fuzz(func(t *testing.T, addr string, priv bool) {
		pa, err := CheckLearnedAddress(addr, priv, false)
		if err != nil {
			return
		}
		if !pa.IP.IsValid() || PortDenied(pa.Port) {
			t.Fatalf("accepted %q", addr)
		}
		if _, err := CheckPeerAddress(pa.HostPort(), false, false); err != nil {
			t.Fatalf("accepted non-public %q: %v", addr, err)
		}
	})
}
