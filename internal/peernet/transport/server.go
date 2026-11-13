package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/kipsilabs/altmount/internal/peernet/identity"
)

type ctxKey struct{}

// PeerFromContext returns the authenticated Node ID of the caller. Handlers
// registered on a Server always have one.
func PeerFromContext(ctx context.Context) (identity.NodeID, bool) {
	id, ok := ctx.Value(ctxKey{}).(identity.NodeID)
	return id, ok
}

type ipCtxKey struct{}

// RemoteIPFromContext returns the TCP remote IP (IPv4-mapped forms unmapped)
// of the connection a request arrived on. It comes from the socket, never
// from a header. Handlers registered on a Server always have one.
func RemoteIPFromContext(ctx context.Context) (netip.Addr, bool) {
	ip, ok := ctx.Value(ipCtxKey{}).(netip.Addr)
	return ip, ok && ip.IsValid()
}

// AdmitFunc is consulted after authentication and before any handler runs.
// A non-nil error denies the request (the peer only sees a fixed 403).
type AdmitFunc func(ctx context.Context, peer identity.NodeID) error

// ServerConfig configures a Server.
type ServerConfig struct {
	// Identity is this node's key. Required.
	Identity *identity.Key
	// ListenAddr is the TCP address to bind, for example ":7777".
	ListenAddr string
	// Limits override DefaultLimits field by field (zero values use defaults).
	Limits Limits
	// Admit is optional. When set, only peers it accepts reach handlers.
	Admit AdmitFunc

	now func() time.Time // test hook
}

// Server is the peer-facing HTTPS server. It has its own mux; the only way to
// add routes is Handle, so the application's main API cannot be reached
// through it.
type Server struct {
	cfg    ServerConfig
	limits Limits
	mux    *http.ServeMux
	gate   *peerGate
	routes map[string]int64 // pattern -> max body

	mu      sync.Mutex
	started bool
	stopped bool
	srv     *http.Server
	ln      net.Listener
	done    chan struct{}
}

// NewServer validates the configuration and returns an unstarted server.
func NewServer(cfg ServerConfig) (*Server, error) {
	if cfg.Identity == nil {
		return nil, fmt.Errorf("%w: identity required", ErrInvalidConfig)
	}
	if cfg.ListenAddr == "" {
		return nil, fmt.Errorf("%w: listen address required", ErrInvalidConfig)
	}
	now := cfg.now
	if now == nil {
		now = time.Now
	}
	l := cfg.Limits.withDefaults()
	return &Server{
		cfg: cfg, limits: l, mux: http.NewServeMux(), gate: newPeerGate(l, now),
		routes: map[string]int64{},
	}, nil
}

var allowedMethods = map[string]bool{http.MethodGet: true, http.MethodPost: true, http.MethodPut: true}

// Handle registers a route. pattern must be "METHOD /path" with METHOD one of
// GET, POST or PUT and an exact path (no wildcards or trailing-slash
// subtrees). maxBody is the largest request body the route accepts, enforced
// with a streaming cap; it must be >= 0 (0 means no body allowed).
func (s *Server) Handle(pattern string, maxBody int64, h http.Handler) error {
	method, p, ok := strings.Cut(pattern, " ")
	if !ok || !allowedMethods[method] || !strings.HasPrefix(p, "/") ||
		strings.ContainsAny(p, "{}*") || strings.HasSuffix(p, "/") && p != "/" || h == nil || maxBody < 0 {
		return fmt.Errorf("%w: bad route %q", ErrInvalidConfig, pattern)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.routes[pattern]; dup {
		return fmt.Errorf("%w: duplicate route %q", ErrInvalidConfig, pattern)
	}
	s.routes[pattern] = maxBody
	s.mux.Handle(pattern, s.limitBody(maxBody, h))
	return nil
}

func (s *Server) limitBody(max int64, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength > max {
			fail(w, http.StatusRequestEntityTooLarge, "request too large")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, max)
		h.ServeHTTP(w, r)
	})
}

// fail writes a small fixed error. It never includes peer-supplied data.
func fail(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Connection", "close")
	w.WriteHeader(code)
	_, _ = w.Write([]byte(msg + "\n"))
}

func (s *Server) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.PeerCertificates) != 1 || r.TLS.Version != tls.VersionTLS13 ||
			r.TLS.NegotiatedProtocol != ALPNProtocol {
			fail(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		id, err := identity.PeerNodeID([][]byte{r.TLS.PeerCertificates[0].Raw})
		if err != nil {
			fail(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		if !allowedMethods[r.Method] || r.URL.Path != path.Clean(r.URL.Path) || r.URL.Host != "" {
			fail(w, http.StatusBadRequest, "bad request")
			return
		}
		switch s.gate.acquire(id) {
		case gateRateLimited:
			fail(w, http.StatusTooManyRequests, "rate limited")
			return
		case gateBusy:
			fail(w, http.StatusTooManyRequests, "too many concurrent requests")
			return
		}
		defer s.gate.release(id)
		ctx := r.Context()
		if s.cfg.Admit != nil {
			if err := s.cfg.Admit(ctx, id); err != nil {
				fail(w, http.StatusForbidden, "forbidden")
				return
			}
		}
		ctx = context.WithValue(ctx, ctxKey{}, id)
		if ap, err := netip.ParseAddrPort(r.RemoteAddr); err == nil {
			ctx = context.WithValue(ctx, ipCtxKey{}, ap.Addr().Unmap().WithZone(""))
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Server) tlsConfig() (*tls.Config, error) {
	cert, err := s.cfg.Identity.TLSCertificate()
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		MaxVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAnyClientCert,
		NextProtos:   []string{ALPNProtocol},
		// Pinning replaces CA validation: the certificate only proves key
		// ownership, and the Node ID is derived from that key.
		VerifyPeerCertificate: func(raw [][]byte, _ [][]*x509.Certificate) error {
			_, err := identity.PeerNodeID(raw)
			return err
		},
	}, nil
}

// Start binds the listener and begins serving in the background. ctx governs
// only the bind; call Stop to shut down. A Server can be started once.
func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started || s.stopped {
		return ErrServerStarted
	}
	tc, err := s.tlsConfig()
	if err != nil {
		return err
	}
	var lc net.ListenConfig
	raw, err := lc.Listen(ctx, "tcp", s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("transport: listen: %w", err)
	}
	ln := newLimitedTLSListener(raw, tc, s.limits)
	s.ln = ln
	s.srv = &http.Server{
		Handler:           s.middleware(s.mux),
		ReadHeaderTimeout: s.limits.ReadHeaderTimeout,
		ReadTimeout:       s.limits.ReadTimeout,
		WriteTimeout:      s.limits.WriteTimeout,
		IdleTimeout:       s.limits.IdleTimeout,
		MaxHeaderBytes:    s.limits.MaxHeaderBytes,
		// A non-nil empty map disables HTTP/2 entirely: HTTP/1.1 only.
		TLSNextProto: map[string]func(*http.Server, *tls.Conn, http.Handler){},
		ErrorLog:     slog.NewLogLogger(slog.DiscardHandler, slog.LevelError),
	}
	s.done = make(chan struct{})
	s.started = true
	srv, done := s.srv, s.done
	go func() {
		defer close(done)
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.ErrorContext(context.Background(), "peer transport server stopped", "error", err)
		}
	}()
	return nil
}

// Addr returns the bound address, or nil before Start.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

// Stop gracefully shuts down, forcing close if ctx expires first. It is safe
// to call when the server was never started, and more than once.
func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	srv, done := s.srv, s.done
	s.stopped = true
	s.mu.Unlock()
	if srv == nil {
		return nil
	}
	err := srv.Shutdown(ctx)
	if err != nil {
		_ = srv.Close()
	}
	<-done
	return err
}

// ipKey groups addresses for per-IP accounting (IPv6 by /64).
func ipKey(a net.Addr) string {
	ta, ok := a.(*net.TCPAddr)
	if !ok {
		return a.String()
	}
	ip, ok := netip.AddrFromSlice(ta.IP)
	if !ok {
		return a.String()
	}
	ip = ip.Unmap()
	if ip.Is6() {
		p, _ := ip.Prefix(64)
		return p.String()
	}
	return ip.String()
}
