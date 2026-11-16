package transport

import (
	"crypto/tls"
	"net"
	"sync"
	"time"
)

// limitedTLSListener accepts TCP connections, enforces the total and per-IP
// connection caps before any TLS work, applies the handshake deadline, and
// returns server-side *tls.Conn values (so net/http sees TLS state).
type limitedTLSListener struct {
	net.Listener
	cfg *tls.Config
	l   Limits

	mu    sync.Mutex
	total int
	// accepted counts every admitted connection since start (diagnostics and
	// tests).
	accepted int
	perIP    map[string]int
}

func newLimitedTLSListener(raw net.Listener, cfg *tls.Config, l Limits) *limitedTLSListener {
	return &limitedTLSListener{Listener: raw, cfg: cfg, l: l, perIP: map[string]int{}}
}

func (l *limitedTLSListener) admit(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.total >= l.l.MaxConns || l.perIP[key] >= l.l.MaxConnsPerIP {
		return false
	}
	l.total++
	l.accepted++
	l.perIP[key]++
	return true
}

func (l *limitedTLSListener) release(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.total--
	if l.perIP[key]--; l.perIP[key] <= 0 {
		delete(l.perIP, key)
	}
}

// Accept returns the next admitted connection. Over-limit connections are
// closed immediately without a TLS handshake.
func (l *limitedTLSListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		key := ipKey(c.RemoteAddr())
		if !l.admit(key) {
			_ = c.Close()
			continue
		}
		_ = c.SetDeadline(time.Now().Add(l.l.HandshakeTimeout))
		return tls.Server(&countedConn{Conn: c, release: func() { l.release(key) }}, l.cfg), nil
	}
}

type countedConn struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *countedConn) Close() error {
	c.once.Do(c.release)
	return c.Conn.Close()
}
