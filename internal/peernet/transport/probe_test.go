package transport

import (
	"context"
	"errors"
	"net/url"
	"sync/atomic"
	"testing"
	"time"
)

func TestProbeNodeIDReturnsServerIDAndSendsNoRequest(t *testing.T) {
	var hits atomic.Int32
	ts := startServer(t, nil, func(s *Server) { must(t, s.Handle("GET /x", 0, okHandler(&hits))) })
	c, _ := newTestClient(t, nil)
	id, err := c.ProbeNodeID(context.Background(), ts.addr, CallOptions{Timeout: 3 * time.Second})
	must(t, err)
	if !id.Equal(ts.key.NodeID()) {
		t.Fatalf("got %s want %s", id, ts.key.NodeID())
	}
	// Wait until the server has accepted the probe's connection and finished
	// with it, then check that no request was ever dispatched.
	pollUntil(t, "probe connection to be accepted and closed", func() bool {
		accepted, active := ts.connStats()
		return accepted >= 1 && active == 0
	})
	if hits.Load() != 0 {
		t.Fatal("probe must not send a request")
	}
}

func TestProbeNodeIDAddressPolicyApplies(t *testing.T) {
	c, _ := newTestClient(t, func(c *ClientConfig) { c.AllowLoopbackForTesting = false })
	for _, a := range []string{"127.0.0.1:1", "10.0.0.1:1", "localhost:1", "169.254.169.254:80", "bad"} {
		_, err := c.ProbeNodeID(context.Background(), a, CallOptions{Timeout: time.Second})
		if !errors.Is(err, ErrAddressRefused) && !errors.Is(err, ErrInvalidAddress) {
			t.Errorf("%s: want policy refusal, got %v", a, err)
		}
	}
}

func TestProbeNodeIDRejectsNonPeer(t *testing.T) {
	c, _ := newTestClient(t, nil)
	// A closed port gives a dial error, never a node id.
	id, err := c.ProbeNodeID(context.Background(), "127.0.0.1:1", CallOptions{Timeout: time.Second})
	if err == nil || !id.IsZero() {
		t.Fatalf("want error, got %v %v", id, err)
	}
}

func TestProbeNodeIDThroughProxy(t *testing.T) {
	ts := startServer(t, nil, nil)
	seen := make(chan string, 4)
	purl, _ := url.Parse(connectProxy(t, 200, seen))
	c, _ := newTestClient(t, func(c *ClientConfig) {
		c.Proxy = func(*url.URL) (*url.URL, error) { return purl, nil }
	})
	id, err := c.ProbeNodeID(context.Background(), ts.addr, CallOptions{Timeout: 3 * time.Second})
	must(t, err)
	if !id.Equal(ts.key.NodeID()) {
		t.Fatal("wrong id through proxy")
	}
	select {
	case <-seen:
	default:
		t.Fatal("proxy not used")
	}
	// Literal private addresses stay refused through a proxy.
	c2, _ := newTestClient(t, func(c *ClientConfig) {
		c.AllowLoopbackForTesting = false
		c.Proxy = func(*url.URL) (*url.URL, error) { return purl, nil }
	})
	if _, err := c2.ProbeNodeID(context.Background(), "10.1.1.1:5", CallOptions{Timeout: time.Second}); !errors.Is(err, ErrAddressRefused) {
		t.Fatalf("want refusal, got %v", err)
	}
}

func TestProbeNodeIDClosedClient(t *testing.T) {
	c, _ := newTestClient(t, nil)
	c.Close()
	if _, err := c.ProbeNodeID(context.Background(), "127.0.0.1:1", CallOptions{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("got %v", err)
	}
}

func TestCheckPeerAddress(t *testing.T) {
	cases := []struct {
		addr           string
		private, loop  bool
		want           error
		wantHost, desc string
	}{
		{"8.8.8.8:80", false, false, nil, "8.8.8.8", "public"},
		{"example.org:80", false, false, nil, "example.org", "hostname"},
		{"10.0.0.1:80", false, false, ErrAddressRefused, "", "private refused"},
		{"10.0.0.1:80", true, false, nil, "10.0.0.1", "private allowed"},
		{"127.0.0.1:80", true, false, ErrAddressRefused, "", "loopback refused"},
		{"127.0.0.1:80", false, true, nil, "127.0.0.1", "loopback test"},
		{"localhost:80", false, false, ErrAddressRefused, "", "local name"},
		{"foo.internal:80", true, false, ErrAddressRefused, "", "local name private"},
		{"169.254.1.1:80", true, false, ErrAddressRefused, "", "link local"},
		{"nope", false, false, ErrInvalidAddress, "", "invalid"},
	}
	for _, tc := range cases {
		pa, err := CheckPeerAddress(tc.addr, tc.private, tc.loop)
		if !errors.Is(err, tc.want) || (tc.want == nil && err != nil) {
			t.Errorf("%s: got %v want %v", tc.desc, err, tc.want)
		}
		if tc.want == nil && pa.Host != tc.wantHost {
			t.Errorf("%s: host %q", tc.desc, pa.Host)
		}
	}
}

func FuzzCheckPeerAddress(f *testing.F) {
	for _, s := range []string{"8.8.8.8:80", "[::1]:1", "10.0.0.1:1", "a.b:1", "localhost:1", "x@y:1", "[::ffff:127.0.0.1]:9"} {
		f.Add(s, false)
	}
	f.Fuzz(func(t *testing.T, s string, priv bool) {
		pa, err := CheckPeerAddress(s, priv, false)
		if err != nil {
			return
		}
		if pa.IP.IsValid() {
			if pa.IP.IsLoopback() || pa.IP.IsUnspecified() || pa.IP.IsLinkLocalUnicast() || pa.IP.IsMulticast() {
				t.Fatalf("accepted %q", s)
			}
			if !priv && (pa.IP.IsPrivate() || cgnat.Contains(pa.IP)) {
				t.Fatalf("accepted private %q", s)
			}
		}
		if _, err := ValidateAddress(pa.HostPort()); err != nil {
			t.Fatalf("canonical form invalid: %q", pa.HostPort())
		}
	})
}
