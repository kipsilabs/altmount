package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/peernet/identity"
)

func newKey(t *testing.T) *identity.Key {
	t.Helper()
	k, err := identity.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	return k
}

type testServer struct {
	*Server
	key  *identity.Key
	addr string
}

func startServer(t *testing.T, mutate func(*ServerConfig), routes func(*Server)) *testServer {
	t.Helper()
	key := newKey(t)
	cfg := ServerConfig{Identity: key, ListenAddr: "127.0.0.1:0"}
	if mutate != nil {
		mutate(&cfg)
	}
	s, err := NewServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if routes != nil {
		routes(s)
	}
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = s.Stop(ctx)
	})
	return &testServer{Server: s, key: key, addr: s.Addr().String()}
}

func newTestClient(t *testing.T, mutate func(*ClientConfig)) (*Client, *identity.Key) {
	t.Helper()
	key := newKey(t)
	cfg := ClientConfig{Identity: key, AllowLoopbackForTesting: true}
	if mutate != nil {
		mutate(&cfg)
	}
	c, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c, key
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func okHandler(counter *atomic.Int32) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if counter != nil {
			counter.Add(1)
		}
		id, _ := PeerFromContext(r.Context())
		_, _ = io.WriteString(w, id.String())
	})
}

func get(t *testing.T, c *Client, ts *testServer, path string) (*Response, error) {
	t.Helper()
	return c.Do(context.Background(), ts.addr, ts.key.NodeID(),
		Request{Method: "GET", Path: path}, CallOptions{MaxResponseBytes: 1 << 20})
}

// rawTLS dials the server as an authenticated peer with the given key and
// returns the raw TLS connection for hand-written HTTP.
func rawTLS(t *testing.T, addr string, key *identity.Key) *tls.Conn {
	t.Helper()
	cfg := &tls.Config{MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13,
		InsecureSkipVerify: true, NextProtos: []string{ALPNProtocol}}
	if key != nil {
		cert, err := key.TLSCertificate()
		must(t, err)
		cfg.Certificates = []tls.Certificate{cert}
	}
	d := net.Dialer{Timeout: 3 * time.Second}
	raw, err := d.Dial("tcp", addr)
	must(t, err)
	c := tls.Client(raw, cfg)
	must(t, c.Handshake())
	t.Cleanup(func() { c.Close() })
	return c
}

var _ = x509.Certificate{}

// readStatus reads until the HTTP status line or connection end.
func readAll(c net.Conn, d time.Duration) string {
	_ = c.SetReadDeadline(time.Now().Add(d))
	b, _ := io.ReadAll(c)
	return string(b)
}
