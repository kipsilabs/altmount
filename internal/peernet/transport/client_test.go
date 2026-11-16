package transport

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type fakeResolver map[string][]netip.Addr

func (f fakeResolver) LookupNetIP(_ context.Context, _, host string) ([]netip.Addr, error) {
	if a, ok := f[host]; ok {
		return a, nil
	}
	return nil, errors.New("no such host")
}

func TestAddressPolicyClasses(t *testing.T) {
	c, _ := newTestClient(t, func(c *ClientConfig) { c.AllowLoopbackForTesting = false })
	refused := []string{
		"127.0.0.1:1", "127.8.9.10:1", "[::1]:1", "10.0.0.1:1", "172.16.5.5:1", "172.31.255.255:1",
		"192.168.1.1:1", "[fd00::1]:1", "[fc00::1]:1", "169.254.169.254:1", "[fe80::1]:1", "100.64.0.1:1",
		"100.127.255.254:1", "224.0.0.1:1", "239.255.255.250:1", "[ff02::1]:1", "0.0.0.0:1", "[::]:1",
		"[::ffff:127.0.0.1]:1", "[::ffff:10.0.0.1]:1", "[::ffff:192.168.0.1]:1", "[::ffff:169.254.1.1]:1",
		"[::ffff:100.64.0.1]:1", "[::ffff:0.0.0.0]:1", "[::ffff:224.0.0.1]:1", "255.255.255.255:1",
		"[64:ff9b::7f00:1]:1", "[2002:7f00:1::]:1", "[::7f00:1]:1", "240.0.0.1:1", "localhost:1", "foo.localhost:1",
	}
	for _, a := range refused {
		_, err := c.Do(context.Background(), a, newKey(t).NodeID(), Request{Method: "GET", Path: "/"},
			CallOptions{MaxResponseBytes: 1, Timeout: time.Second})
		if !errors.Is(err, ErrAddressRefused) {
			t.Errorf("%s: want refusal, got %v", a, err)
		}
	}
	allowed := ipPolicy{}
	for _, ip := range []string{"8.8.8.8", "1.1.1.1", "2606:4700:4700::1111", "::ffff:8.8.8.8"} {
		if err := allowed.check(netip.MustParseAddr(ip)); err != nil {
			t.Errorf("%s refused: %v", ip, err)
		}
	}
}

func TestAllowPrivateOption(t *testing.T) {
	p := ipPolicy{allowPrivate: true}
	for _, ip := range []string{"10.1.2.3", "172.20.0.1", "192.168.0.9", "fd12::1", "100.64.1.1", "::ffff:10.0.0.1"} {
		if err := p.check(netip.MustParseAddr(ip)); err != nil {
			t.Errorf("%s should be allowed with AllowPrivate: %v", ip, err)
		}
	}
	for _, ip := range []string{"127.0.0.1", "::1", "169.254.169.254", "fe80::1", "224.0.0.1", "0.0.0.0", "::ffff:127.0.0.1"} {
		if err := p.check(netip.MustParseAddr(ip)); err == nil {
			t.Errorf("%s must stay refused with AllowPrivate", ip)
		}
	}
	if err := (ipPolicy{allowLoopback: true}).check(netip.MustParseAddr("10.0.0.1")); err == nil {
		t.Error("loopback option must not allow private")
	}
	if err := (ipPolicy{allowLoopback: true}).check(netip.MustParseAddr("127.0.0.1")); err != nil {
		t.Error("loopback option should allow loopback")
	}
}

func TestHostnameResolvingToPrivateIsRefused(t *testing.T) {
	res := fakeResolver{
		"evil.example":   {netip.MustParseAddr("10.0.0.5")},
		"mixed.example":  {netip.MustParseAddr("8.8.8.8"), netip.MustParseAddr("192.168.0.1")},
		"mapped.example": {netip.MustParseAddr("::ffff:127.0.0.1")},
		"empty.example":  {},
	}
	c, _ := newTestClient(t, func(c *ClientConfig) { c.AllowLoopbackForTesting = false; c.Resolver = res })
	for _, h := range []string{"evil.example", "mixed.example", "mapped.example"} {
		_, err := c.Do(context.Background(), h+":443", newKey(t).NodeID(), Request{Method: "GET", Path: "/"},
			CallOptions{MaxResponseBytes: 1, Timeout: time.Second})
		if !errors.Is(err, ErrAddressRefused) {
			t.Errorf("%s: %v", h, err)
		}
	}
	for _, h := range []string{"empty.example", "unknown.example"} {
		_, err := c.Do(context.Background(), h+":443", newKey(t).NodeID(), Request{Method: "GET", Path: "/"},
			CallOptions{MaxResponseBytes: 1, Timeout: time.Second})
		if err == nil || errors.Is(err, ErrAddressRefused) {
			t.Errorf("%s: %v", h, err)
		}
	}
	// With AllowPrivate the private answer is accepted by policy (connect then fails to dial).
	cc, _ := newTestClient(t, func(c *ClientConfig) { c.AllowLoopbackForTesting = false; c.Resolver = res })
	_, err := cc.Do(context.Background(), "evil.example:1", newKey(t).NodeID(), Request{Method: "GET", Path: "/"},
		CallOptions{MaxResponseBytes: 1, Timeout: 200 * time.Millisecond, AllowPrivate: true})
	if errors.Is(err, ErrAddressRefused) {
		t.Errorf("AllowPrivate ignored: %v", err)
	}
}

func TestDialerControlChecksSocketAddress(t *testing.T) {
	c, _ := newTestClient(t, func(c *ClientConfig) { c.AllowLoopbackForTesting = false })
	ctl := c.controlFn(ipPolicy{})
	for _, a := range []string{"127.0.0.1:80", "[::1]:80", "10.0.0.1:80", "[::ffff:10.0.0.1]:80", "garbage"} {
		if err := ctl("tcp", a, nil); !errors.Is(err, ErrAddressRefused) {
			t.Errorf("%s: %v", a, err)
		}
	}
	if err := ctl("tcp", "8.8.8.8:80", nil); err != nil {
		t.Errorf("public refused: %v", err)
	}
}

func TestOwnListenAddressRefused(t *testing.T) {
	ts := startServer(t, nil, func(s *Server) { must(t, s.Handle("GET /x", 0, okHandler(nil))) })
	_, port, _ := net.SplitHostPort(ts.addr)
	for _, own := range []string{ts.addr, ":" + port, "0.0.0.0:" + port, "[::]:" + port} {
		c, _ := newTestClient(t, func(c *ClientConfig) { c.OwnListenAddrs = []string{own} })
		_, err := get(t, c, ts, "/x")
		if !errors.Is(err, ErrAddressRefused) {
			t.Errorf("own=%s: %v", own, err)
		}
	}
	// A different port is not "own".
	c, _ := newTestClient(t, func(c *ClientConfig) { c.OwnListenAddrs = []string{"127.0.0.1:1"} })
	if _, err := get(t, c, ts, "/x"); err != nil {
		t.Errorf("unrelated own addr blocked: %v", err)
	}
	// Wildcard with interface addresses.
	c2, _ := newTestClient(t, func(c *ClientConfig) {
		c.OwnListenAddrs = []string{":4242"}
		c.AllowLoopbackForTesting = false
		c.localAddrs = func() []netip.Addr { return []netip.Addr{netip.MustParseAddr("203.0.113.9")} }
	})
	if !c2.isOwnAddr(netip.MustParseAddr("203.0.113.9"), 4242) || c2.isOwnAddr(netip.MustParseAddr("203.0.113.9"), 4243) {
		t.Error("isOwnAddr wildcard logic")
	}
}

func TestMalformedAddresses(t *testing.T) {
	bad := []string{
		"", "example.com", "example.com:", ":80", "http://example.com:80", "user@example.com:80",
		"example.com:80/path", "example.com:80?x=1", "example.com:80#f", "example.com:0", "example.com:65536",
		"example.com:-1", "example.com:+80", "example.com:8o", "example.com:080000", "[::1]", "::1:80",
		"[fe80::1%eth0]:80", "exa mple.com:80", "example.com :80", "example.com:80\n", "ex\x00ample.com:80",
		"-bad.example:80", "bad-.example:80", "a..b:80", "under_score.example:80", "ünï.example:80",
		strings.Repeat("a", 64) + ".example:80", strings.Repeat("a.", 130) + "com:80", "1.2.3:80", "0x7f.1:80",
		"1.2.3.4.5:80", "example.com:80:80", "[::1]:80:80", "*.example.com:80", "\\\\host:80", "host%41:80",
		strings.Repeat("a", 300) + ":80", "[1.2.3.4]:80", "[example.com]:80",
	}
	for _, s := range bad {
		if _, err := ValidateAddress(s); !errors.Is(err, ErrInvalidAddress) {
			t.Errorf("%q accepted: %v", s, err)
		}
	}
	good := map[string]string{
		"example.com:443": "example.com:443", "EXAMPLE.com:1": "example.com:1", "1.2.3.4:65535": "1.2.3.4:65535",
		"[2001:db8::1]:80": "[2001:db8::1]:80", "[::ffff:1.2.3.4]:80": "1.2.3.4:80", "a-b.c-d.example:8080": "a-b.c-d.example:8080",
	}
	for in, want := range good {
		p, err := ValidateAddress(in)
		if err != nil || p.HostPort() != want {
			t.Errorf("%q: %v %v", in, p.HostPort(), err)
		}
	}
	c, _ := newTestClient(t, nil)
	_, err := c.Do(context.Background(), "http://x:1", newKey(t).NodeID(), Request{Method: "GET", Path: "/"}, CallOptions{MaxResponseBytes: 1})
	if !errors.Is(err, ErrInvalidAddress) {
		t.Errorf("Do: %v", err)
	}
}

func TestDoArgumentValidation(t *testing.T) {
	c, _ := newTestClient(t, nil)
	id := newKey(t).NodeID()
	ok := CallOptions{MaxResponseBytes: 1}
	cases := []struct {
		name string
		id   [32]byte
		req  Request
		opts CallOptions
	}{
		{"zero id", [32]byte{}, Request{Method: "GET", Path: "/"}, ok},
		{"no cap", id, Request{Method: "GET", Path: "/"}, CallOptions{}},
		{"bad method", id, Request{Method: "DELETE", Path: "/"}, ok},
		{"bad path", id, Request{Method: "GET", Path: "x"}, ok},
		{"path query", id, Request{Method: "GET", Path: "/x?y"}, ok},
	}
	for _, tc := range cases {
		if _, err := c.Do(context.Background(), "127.0.0.1:1", tc.id, tc.req, tc.opts); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%s: %v", tc.name, err)
		}
	}
	c.Close()
	if _, err := c.Do(context.Background(), "127.0.0.1:1", id, Request{Method: "GET", Path: "/"}, ok); err == nil {
		t.Error("closed client usable")
	}
	if _, err := NewClient(ClientConfig{}); err == nil {
		t.Error("missing identity accepted")
	}
}

// connectProxy is a minimal CONNECT proxy for tests.
func connectProxy(t *testing.T, status int, seen chan<- string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	must(t, err)
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			w.WriteHeader(405)
			return
		}
		seen <- r.Host
		if status != 200 {
			w.WriteHeader(status)
			return
		}
		up, err := net.Dial("tcp", r.Host)
		if err != nil {
			w.WriteHeader(502)
			return
		}
		hj, _ := w.(http.Hijacker)
		down, _, err := hj.Hijack()
		if err != nil {
			return
		}
		_, _ = io.WriteString(down, "HTTP/1.1 200 Connection established\r\n\r\n")
		go func() { _, _ = io.Copy(up, down); up.Close() }()
		_, _ = io.Copy(down, up)
		down.Close()
	})}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	return "http://" + ln.Addr().String()
}

func TestProxyTunnelsEndToEnd(t *testing.T) {
	ts := startServer(t, nil, func(s *Server) { must(t, s.Handle("GET /x", 0, okHandler(nil))) })
	seen := make(chan string, 4)
	purl, _ := url.Parse(connectProxy(t, 200, seen))
	c, ckey := newTestClient(t, func(c *ClientConfig) {
		c.Proxy = func(*url.URL) (*url.URL, error) { return purl, nil }
	})
	resp, err := get(t, c, ts, "/x")
	must(t, err)
	b, _ := resp.ReadAll()
	if string(b) != ckey.NodeID().String() {
		t.Fatalf("body %q", b)
	}
	if got := <-seen; got != ts.addr {
		t.Fatalf("proxy saw %q", got)
	}
	// Pinning still applies through the tunnel.
	_, err = c.Do(context.Background(), ts.addr, newKey(t).NodeID(), Request{Method: "GET", Path: "/x"}, CallOptions{MaxResponseBytes: 5})
	if !errors.Is(err, ErrNodeIDMismatch) {
		t.Fatalf("pin through proxy: %v", err)
	}
}

func TestProxyRefusals(t *testing.T) {
	seen := make(chan string, 4)
	purl, _ := url.Parse(connectProxy(t, 200, seen))
	c, _ := newTestClient(t, func(c *ClientConfig) {
		c.AllowLoopbackForTesting = false
		c.Proxy = func(*url.URL) (*url.URL, error) { return purl, nil }
	})
	for _, a := range []string{"10.0.0.1:1", "127.0.0.1:1", "[::ffff:192.168.0.1]:1", "localhost:1", "x.local:1"} {
		_, err := c.Do(context.Background(), a, newKey(t).NodeID(), Request{Method: "GET", Path: "/"}, CallOptions{MaxResponseBytes: 1})
		if !errors.Is(err, ErrAddressRefused) {
			t.Errorf("%s: %v", a, err)
		}
	}
	if len(seen) != 0 {
		t.Fatal("proxy contacted for refused address")
	}

	denied, _ := url.Parse(connectProxy(t, 403, seen))
	c2, _ := newTestClient(t, func(c *ClientConfig) { c.Proxy = func(*url.URL) (*url.URL, error) { return denied, nil } })
	_, err := c2.Do(context.Background(), "127.0.0.1:9", newKey(t).NodeID(), Request{Method: "GET", Path: "/"}, CallOptions{MaxResponseBytes: 1})
	if !errors.Is(err, ErrProxyConnect) {
		t.Errorf("403 proxy: %v", err)
	}
	socks, _ := url.Parse("socks5://127.0.0.1:1")
	c3, _ := newTestClient(t, func(c *ClientConfig) { c.Proxy = func(*url.URL) (*url.URL, error) { return socks, nil } })
	_, err = c3.Do(context.Background(), "127.0.0.1:9", newKey(t).NodeID(), Request{Method: "GET", Path: "/"}, CallOptions{MaxResponseBytes: 1})
	if !errors.Is(err, ErrProxyUnsupported) {
		t.Errorf("socks: %v", err)
	}
	c4, _ := newTestClient(t, func(c *ClientConfig) {
		c.Proxy = func(*url.URL) (*url.URL, error) { return nil, errors.New("boom") }
	})
	if _, err = c4.Do(context.Background(), "127.0.0.1:9", newKey(t).NodeID(), Request{Method: "GET", Path: "/"}, CallOptions{MaxResponseBytes: 1}); !errors.Is(err, ErrProxyConnect) {
		t.Errorf("proxy func error: %v", err)
	}
}

func TestProxyRejectsDataAfterConnect(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	must(t, err)
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		_, _ = bufio.NewReader(c).ReadString('\n')
		_, _ = io.WriteString(c, "HTTP/1.1 200 OK\r\n\r\nEARLY")
		// Keep the tunnel open until the client hangs up (or a deadline).
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, _ = io.Copy(io.Discard, c)
		c.Close()
	}()
	purl, _ := url.Parse("http://" + ln.Addr().String())
	c, _ := newTestClient(t, func(c *ClientConfig) { c.Proxy = func(*url.URL) (*url.URL, error) { return purl, nil } })
	_, err = c.Do(context.Background(), "127.0.0.1:9", newKey(t).NodeID(), Request{Method: "GET", Path: "/"}, CallOptions{MaxResponseBytes: 1})
	if !errors.Is(err, ErrProxyConnect) {
		t.Fatalf("early data: %v", err)
	}
}

func TestProxyFromSettings(t *testing.T) {
	if ProxyFromSettings("", "", "") != nil {
		t.Error("empty settings should mean no proxy")
	}
	f := ProxyFromSettings("http://p:3128", "http://p:3128", "")
	u, err := f(&url.URL{Scheme: "https", Host: "peer.example:443"})
	if err != nil || u == nil || u.Host != "p:3128" {
		t.Errorf("%v %v", u, err)
	}
}

func TestCallTimeout(t *testing.T) {
	ts := startServer(t, nil, func(s *Server) {
		must(t, s.Handle("GET /hang", 0, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			<-r.Context().Done()
		})))
	})
	c, _ := newTestClient(t, nil)
	start := time.Now()
	_, err := c.Do(context.Background(), ts.addr, ts.key.NodeID(), Request{Method: "GET", Path: "/hang"},
		CallOptions{MaxResponseBytes: 10, Timeout: 300 * time.Millisecond})
	if err == nil || time.Since(start) > 3*time.Second {
		t.Fatalf("no timeout: %v after %v", err, time.Since(start))
	}
}

func TestPooledTransportEviction(t *testing.T) {
	c, _ := newTestClient(t, func(c *ClientConfig) { c.MaxPooledPeers = 2 })
	for i := 0; i < 5; i++ {
		pa, _ := ValidateAddress("127.0.0.1:1")
		if _, err := c.transportFor(pa, newKey(t).NodeID(), false, false); err != nil {
			t.Fatal(err)
		}
	}
	if len(c.pool) != 2 {
		t.Fatalf("pool size %d", len(c.pool))
	}
}

func FuzzValidateAddress(f *testing.F) {
	for _, s := range []string{"example.com:443", "[::1]:80", "1.2.3.4:5", "", "a@b:1", "[::ffff:1.2.3.4]:9", "x:99999"} {
		f.Add(s)
	}
	var counter atomic.Int32
	f.Fuzz(func(t *testing.T, s string) {
		counter.Add(1)
		p, err := ValidateAddress(s)
		if err != nil {
			return
		}
		if len(s) > maxAddrLen || p.Port == 0 || strings.ContainsAny(s, "/\\@?# \t\r\n%") {
			t.Fatalf("accepted %q", s)
		}
		again, err := ValidateAddress(p.HostPort())
		if err != nil || again.HostPort() != p.HostPort() {
			t.Fatalf("canonical form unstable: %q -> %q (%v)", s, p.HostPort(), err)
		}
		if p.IP.IsValid() {
			_ = ipPolicy{}.check(p.IP)
		}
	})
}
