package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/peernet/identity"
)

func TestAuthenticatedRoundTrip(t *testing.T) {
	var hits atomic.Int32
	ts := startServer(t, nil, func(s *Server) { must(t, s.Handle("GET /v1/ping", 0, okHandler(&hits))) })
	c, ckey := newTestClient(t, nil)
	resp, err := get(t, c, ts, "/v1/ping")
	must(t, err)
	b, err := resp.ReadAll()
	must(t, err)
	if resp.StatusCode != 200 || string(b) != ckey.NodeID().String() {
		t.Fatalf("status %d body %q want caller id %s", resp.StatusCode, b, ckey.NodeID())
	}
	// Connection reuse.
	resp, err = get(t, c, ts, "/v1/ping")
	must(t, err)
	_, _ = resp.ReadAll()
	if hits.Load() != 2 {
		t.Fatalf("hits %d", hits.Load())
	}
}

func TestClientRefusesWrongNodeIDAndSendsNoBody(t *testing.T) {
	// Impostor TLS server records anything the client sends after handshake.
	imp := newKey(t)
	cert, _ := imp.TLSCertificate()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	must(t, err)
	defer ln.Close()
	got := make(chan int, 1)
	go func() {
		raw, err := ln.Accept()
		if err != nil {
			got <- -1
			return
		}
		tc := tls.Server(raw, &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert},
			ClientAuth: tls.RequireAnyClientCert, NextProtos: []string{ALPNProtocol}})
		_ = tc.Handshake()
		_ = tc.SetReadDeadline(time.Now().Add(time.Second))
		b, _ := io.ReadAll(tc)
		got <- len(b)
	}()
	c, _ := newTestClient(t, nil)
	other := newKey(t).NodeID()
	_, err = c.Do(context.Background(), ln.Addr().String(), other,
		Request{Method: "POST", Path: "/x", Body: []byte("secret-body")}, CallOptions{MaxResponseBytes: 10})
	if !errors.Is(err, ErrNodeIDMismatch) {
		t.Fatalf("want mismatch, got %v", err)
	}
	if n := <-got; n != 0 {
		t.Fatalf("server received %d bytes after handshake", n)
	}
}

func TestPoolNeverReusedForDifferentNodeID(t *testing.T) {
	ts := startServer(t, nil, func(s *Server) { must(t, s.Handle("GET /p", 0, okHandler(nil))) })
	c, _ := newTestClient(t, nil)
	resp, err := get(t, c, ts, "/p")
	must(t, err)
	_, _ = resp.ReadAll()
	_, err = c.Do(context.Background(), ts.addr, newKey(t).NodeID(), Request{Method: "GET", Path: "/p"},
		CallOptions{MaxResponseBytes: 10})
	if !errors.Is(err, ErrNodeIDMismatch) {
		t.Fatalf("reused pooled conn for another node id: %v", err)
	}
}

func TestServerRejectsMissingClientCert(t *testing.T) {
	var hits atomic.Int32
	ts := startServer(t, nil, func(s *Server) { must(t, s.Handle("GET /x", 0, okHandler(&hits))) })
	raw, err := net.Dial("tcp", ts.addr)
	must(t, err)
	c := tls.Client(raw, &tls.Config{MinVersion: tls.VersionTLS13, InsecureSkipVerify: true, NextProtos: []string{ALPNProtocol}})
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(2 * time.Second))
	if err := c.Handshake(); err == nil {
		// TLS 1.3: the rejection arrives on the first read.
		_, _ = io.WriteString(c, "GET /x HTTP/1.1\r\nHost: a\r\n\r\n")
		b, _ := io.ReadAll(c)
		if strings.Contains(string(b), "HTTP/1.1 200") {
			t.Fatal("unauthenticated request served")
		}
	}
	if hits.Load() != 0 {
		t.Fatal("handler reached without client certificate")
	}
}

func TestServerRefusesTLS12AndH2(t *testing.T) {
	ts := startServer(t, nil, nil)
	d := net.Dialer{Timeout: time.Second}
	raw, err := d.Dial("tcp", ts.addr)
	must(t, err)
	c := tls.Client(raw, &tls.Config{MaxVersion: tls.VersionTLS12, InsecureSkipVerify: true})
	if err := c.Handshake(); err == nil {
		t.Fatal("TLS 1.2 handshake succeeded")
	}
	raw, err = d.Dial("tcp", ts.addr)
	must(t, err)
	cert, _ := newKey(t).TLSCertificate()
	c = tls.Client(raw, &tls.Config{MinVersion: tls.VersionTLS13, InsecureSkipVerify: true,
		NextProtos: []string{"h2"}, Certificates: []tls.Certificate{cert}})
	if err := c.Handshake(); err == nil {
		t.Fatal("h2-only ALPN accepted")
	}
}

func TestAdmissionCallback(t *testing.T) {
	var hits atomic.Int32
	blocked := newKey(t)
	ts := startServer(t, func(c *ServerConfig) {
		c.Admit = func(_ context.Context, id identity.NodeID) error {
			if id.Equal(blocked.NodeID()) {
				return ErrPeerNotAdmitted
			}
			return nil
		}
	}, func(s *Server) { must(t, s.Handle("GET /x", 0, okHandler(&hits))) })
	allowed, _ := newTestClient(t, nil)
	denied, _ := newTestClient(t, func(c *ClientConfig) { c.Identity = blocked })
	resp, err := get(t, allowed, ts, "/x")
	must(t, err)
	_, _ = resp.ReadAll()
	resp, err = get(t, denied, ts, "/x")
	must(t, err)
	b, _ := resp.ReadAll()
	if resp.StatusCode != 403 || strings.Contains(string(b), blocked.NodeID().String()) {
		t.Fatalf("status %d body %q", resp.StatusCode, b)
	}
	if hits.Load() != 1 {
		t.Fatalf("handler hits %d", hits.Load())
	}
}

func TestRedirectNotFollowed(t *testing.T) {
	var target atomic.Int32
	ts := startServer(t, nil, func(s *Server) {
		must(t, s.Handle("GET /r", 0, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "/target", http.StatusFound)
		})))
		must(t, s.Handle("GET /target", 0, okHandler(&target)))
	})
	c, _ := newTestClient(t, nil)
	resp, err := get(t, c, ts, "/r")
	must(t, err)
	_, _ = resp.ReadAll()
	if resp.StatusCode != 302 || target.Load() != 0 {
		t.Fatalf("status %d target hits %d", resp.StatusCode, target.Load())
	}
}

func TestRequestBodyCap(t *testing.T) {
	var readErr atomic.Value
	ts := startServer(t, nil, func(s *Server) {
		must(t, s.Handle("POST /up", 16, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := io.ReadAll(r.Body)
			if err != nil {
				readErr.Store(err)
				fail(w, 413, "too large")
				return
			}
			w.WriteHeader(200)
		})))
	})
	c, _ := newTestClient(t, nil)
	do := func(n int) int {
		resp, err := c.Do(context.Background(), ts.addr, ts.key.NodeID(),
			Request{Method: "POST", Path: "/up", Body: []byte(strings.Repeat("a", n))}, CallOptions{MaxResponseBytes: 1024})
		must(t, err)
		_, _ = resp.ReadAll()
		return resp.StatusCode
	}
	if do(16) != 200 {
		t.Fatal("body at cap rejected")
	}
	if do(17) != 413 {
		t.Fatal("declared oversized body accepted")
	}
	// Undeclared length (chunked) is cut off by the streaming cap.
	conn := rawTLS(t, ts.addr, newKey(t))
	_, _ = io.WriteString(conn, "POST /up HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n")
	chunk := strings.Repeat("b", 64)
	_, _ = io.WriteString(conn, "40\r\n"+chunk+"\r\n0\r\n\r\n")
	out := readAll(conn, 2*time.Second)
	if !strings.Contains(out, "413") {
		t.Fatalf("chunked oversize not cut off: %q", out)
	}
	var mbe *http.MaxBytesError
	if e, _ := readErr.Load().(error); !errors.As(e, &mbe) {
		t.Fatalf("handler read error: %v", e)
	}
}

func TestResponseBodyCap(t *testing.T) {
	ts := startServer(t, nil, func(s *Server) {
		must(t, s.Handle("GET /big", 0, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.WriteString(w, strings.Repeat("x", 1000))
		})))
		must(t, s.Handle("GET /stream", 0, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			for i := 0; i < 50; i++ {
				_, _ = io.WriteString(w, strings.Repeat("x", 100))
				w.(http.Flusher).Flush()
			}
		})))
	})
	c, _ := newTestClient(t, nil)
	_, err := c.Do(context.Background(), ts.addr, ts.key.NodeID(), Request{Method: "GET", Path: "/big"},
		CallOptions{MaxResponseBytes: 100})
	if !errors.Is(err, ErrBodyTooLarge) {
		t.Fatalf("declared oversize: %v", err)
	}
	resp, err := c.Do(context.Background(), ts.addr, ts.key.NodeID(), Request{Method: "GET", Path: "/stream"},
		CallOptions{MaxResponseBytes: 250})
	must(t, err)
	b, err := resp.ReadAll()
	if !errors.Is(err, ErrBodyTooLarge) || len(b) != 250 {
		t.Fatalf("streamed oversize: len %d err %v", len(b), err)
	}
	// Exactly at cap is fine.
	resp, err = c.Do(context.Background(), ts.addr, ts.key.NodeID(), Request{Method: "GET", Path: "/big"},
		CallOptions{MaxResponseBytes: 1000})
	must(t, err)
	if b, err := resp.ReadAll(); err != nil || len(b) != 1000 {
		t.Fatalf("at cap: %d %v", len(b), err)
	}
}

func TestResponseHeaderCap(t *testing.T) {
	ts := startServer(t, nil, func(s *Server) {
		must(t, s.Handle("GET /h", 0, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Big", strings.Repeat("h", 8192))
		})))
	})
	c, _ := newTestClient(t, func(c *ClientConfig) { c.MaxResponseHeaderBytes = 1024 })
	if _, err := get(t, c, ts, "/h"); err == nil {
		t.Fatal("oversized response headers accepted")
	}
}

func TestRequestHeaderCapAndBadRequests(t *testing.T) {
	var hits atomic.Int32
	ts := startServer(t, func(c *ServerConfig) { c.Limits.MaxHeaderBytes = 1024 }, func(s *Server) {
		must(t, s.Handle("GET /x", 0, okHandler(&hits)))
	})
	conn := rawTLS(t, ts.addr, newKey(t))
	_, _ = io.WriteString(conn, "GET /x HTTP/1.1\r\nHost: a\r\nX-Pad: "+strings.Repeat("p", 8000)+"\r\n\r\n")
	if out := readAll(conn, 2*time.Second); !strings.Contains(out, "431") {
		t.Fatalf("header cap not enforced: %q", out)
	}
	for _, raw := range []string{
		"TRACE /x HTTP/1.1\r\nHost: a\r\n\r\n",
		"GET /a/../x HTTP/1.1\r\nHost: a\r\n\r\n",
		"GET //x HTTP/1.1\r\nHost: a\r\n\r\n",
		"DELETE /x HTTP/1.1\r\nHost: a\r\n\r\n",
	} {
		conn := rawTLS(t, ts.addr, newKey(t))
		_, _ = io.WriteString(conn, raw)
		out := readAll(conn, time.Second)
		if strings.Contains(out, " 200 ") {
			t.Fatalf("%q served: %q", raw, out)
		}
	}
	conn = rawTLS(t, ts.addr, newKey(t))
	_, _ = io.WriteString(conn, "GET /nope HTTP/1.1\r\nHost: a\r\n\r\n")
	if out := readAll(conn, time.Second); !strings.Contains(out, "404") {
		t.Fatalf("unknown route: %q", out)
	}
	if hits.Load() != 0 {
		t.Fatalf("handler reached by bad requests: %d", hits.Load())
	}
}

func TestConnectionCaps(t *testing.T) {
	for name, mutate := range map[string]func(*ServerConfig){
		"per-ip": func(c *ServerConfig) { c.Limits.MaxConnsPerIP = 2 },
		"total":  func(c *ServerConfig) { c.Limits.MaxConns = 2; c.Limits.MaxConnsPerIP = 100 },
	} {
		t.Run(name, func(t *testing.T) {
			ts := startServer(t, mutate, nil)
			var held []net.Conn
			for i := 0; i < 2; i++ {
				c, err := net.Dial("tcp", ts.addr)
				must(t, err)
				held = append(held, c)
				defer c.Close()
			}
			// Both held connections are counted once the server has accepted
			// them; poll for the third being closed instead of sleeping.
			pollUntil(t, "third connection to be refused", func() bool {
				third, err := net.Dial("tcp", ts.addr)
				if err != nil {
					return false
				}
				defer third.Close()
				_ = third.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
				_, err = third.Read(make([]byte, 1))
				return err != nil && !isTimeout(err)
			})
			// Releasing a slot lets a new connection in.
			held[0].Close()
			pollUntil(t, "slot to be released", func() bool {
				fourth, err := net.Dial("tcp", ts.addr)
				if err != nil {
					return false
				}
				defer fourth.Close()
				_ = fourth.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
				_, err = fourth.Read(make([]byte, 1))
				return isTimeout(err)
			})
		})
	}
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func TestRateLimitAndConcurrency(t *testing.T) {
	ts := startServer(t, func(c *ServerConfig) {
		c.Limits.RateBurst = 3
		c.Limits.RatePerSecond = 0.0001
		c.Limits.MaxConcurrentPerPeer = 1
	}, func(s *Server) { must(t, s.Handle("GET /x", 0, okHandler(nil))) })
	c, _ := newTestClient(t, nil)
	var codes []int
	for i := 0; i < 4; i++ {
		resp, err := get(t, c, ts, "/x")
		must(t, err)
		b, _ := resp.ReadAll()
		codes = append(codes, resp.StatusCode)
		if resp.StatusCode == 429 && len(b) > 64 {
			t.Fatalf("verbose error %q", b)
		}
	}
	if codes[0] != 200 || codes[2] != 200 || codes[3] != 429 {
		t.Fatalf("codes %v", codes)
	}

	release := make(chan struct{})
	entered := make(chan struct{}, 4)
	ts2 := startServer(t, func(c *ServerConfig) { c.Limits.MaxConcurrentPerPeer = 1 }, func(s *Server) {
		must(t, s.Handle("GET /slow", 0, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			entered <- struct{}{}
			<-release
		})))
	})
	c2, _ := newTestClient(t, nil)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		resp, err := get(t, c2, ts2, "/slow")
		if err == nil {
			_, _ = resp.ReadAll()
		}
	}()
	<-entered
	resp, err := get(t, c2, ts2, "/slow")
	must(t, err)
	_, _ = resp.ReadAll()
	if resp.StatusCode != 429 {
		t.Fatalf("concurrency cap: %d", resp.StatusCode)
	}
	close(release)
	wg.Wait()
}

func TestStalledPeerTimeouts(t *testing.T) {
	bodyErr := make(chan error, 1)
	ts := startServer(t, func(c *ServerConfig) {
		c.Limits.HandshakeTimeout = 300 * time.Millisecond
		c.Limits.ReadHeaderTimeout = 300 * time.Millisecond
		c.Limits.ReadTimeout = 600 * time.Millisecond
	}, func(s *Server) {
		must(t, s.Handle("POST /b", 1024, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := io.ReadAll(r.Body)
			bodyErr <- err
		})))
	})
	closedWithin := func(c net.Conn, d time.Duration) bool {
		_ = c.SetReadDeadline(time.Now().Add(d))
		_, err := io.ReadAll(c)
		return err == nil || !isTimeout(err)
	}
	// Stalled handshake.
	raw, err := net.Dial("tcp", ts.addr)
	must(t, err)
	defer raw.Close()
	if !closedWithin(raw, 2*time.Second) {
		t.Fatal("stalled handshake not closed")
	}
	// Slow headers.
	conn := rawTLS(t, ts.addr, newKey(t))
	_, _ = io.WriteString(conn, "GET /b HTTP/1.1\r\nHo")
	if !closedWithin(conn, 2*time.Second) {
		t.Fatal("slow headers not closed")
	}
	// Slow body.
	conn = rawTLS(t, ts.addr, newKey(t))
	_, _ = io.WriteString(conn, "POST /b HTTP/1.1\r\nHost: a\r\nContent-Length: 100\r\n\r\nabc")
	select {
	case err := <-bodyErr:
		if err == nil {
			t.Fatal("slow body read succeeded")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("slow body not cut off")
	}
}

func TestStartStop(t *testing.T) {
	s, err := NewServer(ServerConfig{Identity: newKey(t), ListenAddr: "127.0.0.1:0"})
	must(t, err)
	must(t, s.Stop(context.Background())) // never started
	if s.Addr() != nil {
		t.Fatal("addr before start")
	}
	s2, _ := NewServer(ServerConfig{Identity: newKey(t), ListenAddr: "127.0.0.1:0"})
	must(t, s2.Start(context.Background()))
	if err := s2.Start(context.Background()); !errors.Is(err, ErrServerStarted) {
		t.Fatalf("double start: %v", err)
	}
	addr := s2.Addr().String()
	must(t, s2.Stop(context.Background()))
	must(t, s2.Stop(context.Background()))
	if _, err := net.DialTimeout("tcp", addr, 300*time.Millisecond); err == nil {
		t.Fatal("still listening after Stop")
	}
	if err := s2.Start(context.Background()); err == nil {
		t.Fatal("restart after stop should fail")
	}
	// Bad config.
	if _, err := NewServer(ServerConfig{ListenAddr: ":1"}); err == nil {
		t.Fatal("missing identity accepted")
	}
	if _, err := NewServer(ServerConfig{Identity: newKey(t)}); err == nil {
		t.Fatal("missing listen addr accepted")
	}
	// Listen failure.
	s3, _ := NewServer(ServerConfig{Identity: newKey(t), ListenAddr: "256.1.1.1:99999"})
	if err := s3.Start(context.Background()); err == nil {
		t.Fatal("bad listen addr accepted")
	}
}

func TestHandleValidation(t *testing.T) {
	s, _ := NewServer(ServerConfig{Identity: newKey(t), ListenAddr: "127.0.0.1:0"})
	h := okHandler(nil)
	for _, bad := range []string{"", "/x", "GET x", "GET /x/", "DELETE /x", "GET /{id}", "TRACE /x", "GET /a/*"} {
		if err := s.Handle(bad, 0, h); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	must(t, s.Handle("GET /ok", 0, h))
	if err := s.Handle("GET /ok", 0, h); err == nil {
		t.Error("duplicate accepted")
	}
	if err := s.Handle("GET /nil", 0, nil); err == nil {
		t.Error("nil handler accepted")
	}
	if err := s.Handle("GET /neg", -1, h); err == nil {
		t.Error("negative cap accepted")
	}
	if _, ok := PeerFromContext(context.Background()); ok {
		t.Error("peer in empty context")
	}
}

func TestPeerGateEviction(t *testing.T) {
	now := time.Unix(1000, 0)
	l := DefaultLimits()
	l.MaxTrackedPeers = 2
	l.RateBurst = 1
	l.RatePerSecond = 1
	g := newPeerGate(l, func() time.Time { return now })
	a, b, c := newKey(t).NodeID(), newKey(t).NodeID(), newKey(t).NodeID()
	if g.acquire(a) != gateOK || g.acquire(b) != gateOK {
		t.Fatal("first acquire")
	}
	g.release(a)
	g.release(b)
	if g.acquire(a) != gateRateLimited {
		t.Fatal("a should be limited")
	}
	if g.acquire(c) != gateOK { // table full: evicts someone
		t.Fatal("new peer rejected")
	}
	if len(g.peers) > 2 {
		t.Fatalf("table grew: %d", len(g.peers))
	}
	now = now.Add(5 * time.Second)
	g.release(c)
	if g.acquire(a) != gateOK {
		t.Fatal("refill failed")
	}
	g.release(newKey(t).NodeID()) // unknown peer release is harmless
}
