package transport

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"sync"
	"syscall"
	"time"

	"github.com/kipsilabs/altmount/internal/peernet/identity"
)

var zeroTime time.Time

// Resolver resolves hostnames. *net.Resolver satisfies it.
type Resolver interface {
	LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error)
}

// ClientConfig configures a Client. Zero durations use the defaults noted.
type ClientConfig struct {
	// Identity is this node's key. Required.
	Identity *identity.Key
	// Proxy selects the outbound proxy (see ProxyFromSettings). Nil means direct.
	Proxy ProxyFunc
	// OwnListenAddrs are this instance's listen addresses ("host:port" or
	// ":port"); the client refuses to connect to them.
	OwnListenAddrs []string
	// DialTimeout bounds TCP connect (default 10s).
	DialTimeout time.Duration
	// HandshakeTimeout bounds the TLS handshake (default 10s).
	HandshakeTimeout time.Duration
	// ResponseHeaderTimeout bounds waiting for response headers (default 15s).
	ResponseHeaderTimeout time.Duration
	// DefaultTimeout is the whole-call timeout when CallOptions.Timeout is
	// zero (default 60s).
	DefaultTimeout time.Duration
	// MaxResponseHeaderBytes caps response headers (default 16 KiB).
	MaxResponseHeaderBytes int64
	// MaxPooledPeers bounds the number of pooled (address, node id) pairs (default 64).
	MaxPooledPeers int
	// Resolver overrides DNS resolution (default net.DefaultResolver).
	Resolver Resolver
	// AllowLoopbackForTesting permits loopback destinations. Tests only.
	AllowLoopbackForTesting bool

	localAddrs func() []netip.Addr // test hook
}

// CallOptions are per-call options.
type CallOptions struct {
	// AllowPrivate permits RFC 1918, RFC 4193 and CGNAT destinations. Use it
	// only for private networks that enable it and only for addresses learned
	// inside that network. Loopback, link-local and the rest stay refused.
	AllowPrivate bool
	// TrustedAddress states that the address comes from trusted local
	// configuration (the user's own list or a local discovery source), not
	// from a peer. Untrusted addresses are refused when they use a well-known
	// service port (see PortDenied) and, when an outbound proxy is in use,
	// when they are hostnames: the proxy resolves the name where the local
	// address policy cannot check the result.
	TrustedAddress bool
	// MaxResponseBytes caps the response body. Required, must be > 0.
	MaxResponseBytes int64
	// Timeout is the whole-call timeout including reading the body.
	Timeout time.Duration
}

// Request describes a call. Only GET, POST and PUT are allowed.
type Request struct {
	Method string
	Path   string
	Query  url.Values
	Header http.Header
	Body   []byte
}

// Response is a capped response. Close it to release the connection and the
// call's timer. Reading past CallOptions.MaxResponseBytes returns
// ErrBodyTooLarge.
type Response struct {
	StatusCode int
	Header     http.Header
	Body       io.ReadCloser
}

// ReadAll reads the whole (capped) body and closes it.
func (r *Response) ReadAll() ([]byte, error) {
	defer r.Body.Close()
	return io.ReadAll(r.Body)
}

type pooledTransport struct {
	tr       *http.Transport
	lastUsed time.Time
}

// Client makes authenticated calls to peers. It never follows redirects and
// pools connections per (address, expected node id, allow-private) triple.
type Client struct {
	cfg ClientConfig

	mu     sync.Mutex
	pool   map[string]*pooledTransport
	closed bool
}

// NewClient validates cfg and returns a Client.
func NewClient(cfg ClientConfig) (*Client, error) {
	if cfg.Identity == nil {
		return nil, fmt.Errorf("%w: identity required", ErrInvalidConfig)
	}
	def := func(d *time.Duration, v time.Duration) {
		if *d <= 0 {
			*d = v
		}
	}
	def(&cfg.DialTimeout, 10*time.Second)
	def(&cfg.HandshakeTimeout, 10*time.Second)
	def(&cfg.ResponseHeaderTimeout, 15*time.Second)
	def(&cfg.DefaultTimeout, 60*time.Second)
	if cfg.MaxResponseHeaderBytes <= 0 {
		cfg.MaxResponseHeaderBytes = 16 << 10
	}
	if cfg.MaxPooledPeers <= 0 {
		cfg.MaxPooledPeers = 64
	}
	if cfg.Resolver == nil {
		cfg.Resolver = net.DefaultResolver
	}
	if cfg.localAddrs == nil {
		cfg.localAddrs = interfaceAddrs
	}
	return &Client{cfg: cfg, pool: map[string]*pooledTransport{}}, nil
}

// Close drops all pooled connections.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	for k, p := range c.pool {
		p.tr.CloseIdleConnections()
		delete(c.pool, k)
	}
}

// Do sends req to the peer at addr ("host:port"), which must present the
// certificate of expected. The request is not written until the TLS handshake
// has completed and the peer's key maps to exactly that Node ID.
func (c *Client) Do(ctx context.Context, addr string, expected identity.NodeID, req Request, opts CallOptions) (*Response, error) {
	pa, err := ValidateAddress(addr)
	if err != nil {
		return nil, err
	}
	if expected.IsZero() {
		return nil, fmt.Errorf("%w: expected node id required", ErrInvalidConfig)
	}
	if opts.MaxResponseBytes <= 0 {
		return nil, fmt.Errorf("%w: MaxResponseBytes required", ErrInvalidConfig)
	}
	if !allowedMethods[req.Method] || req.Path == "" || req.Path[0] != '/' || !cleanPath(req.Path) {
		return nil, fmt.Errorf("%w: bad method or path", ErrInvalidConfig)
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = c.cfg.DefaultTimeout
	}
	tr, err := c.transportFor(pa, expected, opts.AllowPrivate, opts.TrustedAddress)
	if err != nil {
		return nil, err
	}
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	u := &url.URL{Scheme: "https", Host: pa.HostPort(), Path: req.Path, RawQuery: req.Query.Encode()}
	var body io.Reader
	if len(req.Body) > 0 {
		body = bytes.NewReader(req.Body)
	}
	hr, err := http.NewRequestWithContext(callCtx, req.Method, u.String(), body)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	for k, vs := range req.Header {
		for _, v := range vs {
			hr.Header.Add(k, v)
		}
	}
	// RoundTrip directly: there is no redirect handling, so a 3xx response is
	// returned to the caller and never followed.
	resp, err := tr.RoundTrip(hr)
	if err != nil {
		cancel()
		return nil, err
	}
	if resp.ContentLength > opts.MaxResponseBytes {
		_ = resp.Body.Close()
		cancel()
		return nil, ErrBodyTooLarge
	}
	return &Response{
		StatusCode: resp.StatusCode, Header: resp.Header,
		Body: &cappedBody{rc: resp.Body, max: opts.MaxResponseBytes, onClose: cancel},
	}, nil
}

func cleanPath(p string) bool {
	for i := 0; i < len(p); i++ {
		if p[i] <= 0x20 || p[i] >= 0x7f || p[i] == '?' || p[i] == '#' || p[i] == '%' {
			return false
		}
	}
	return true
}

func (c *Client) transportFor(pa ParsedAddr, expected identity.NodeID, allowPrivate, trusted bool) (*http.Transport, error) {
	key := fmt.Sprintf("%s|%s|%t|%t", pa.HostPort(), expected, allowPrivate, trusted)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, fmt.Errorf("%w: client closed", ErrInvalidConfig)
	}
	if p, ok := c.pool[key]; ok {
		p.lastUsed = time.Now()
		return p.tr, nil
	}
	if len(c.pool) >= c.cfg.MaxPooledPeers {
		var oldKey string
		var oldest time.Time
		for k, p := range c.pool {
			if oldKey == "" || p.lastUsed.Before(oldest) {
				oldKey, oldest = k, p.lastUsed
			}
		}
		c.pool[oldKey].tr.CloseIdleConnections()
		delete(c.pool, oldKey)
	}
	policy := ipPolicy{allowPrivate: allowPrivate, allowLoopback: c.cfg.AllowLoopbackForTesting, trusted: trusted}
	tr := &http.Transport{
		// The pinned dialer is the only way a connection is made; the
		// transport's own proxy and TLS logic are bypassed on purpose.
		DialTLSContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return c.dialPinned(ctx, pa, expected, policy)
		},
		TLSNextProto:           map[string]func(string, *tls.Conn) http.RoundTripper{},
		DisableCompression:     true,
		MaxIdleConns:           2,
		MaxIdleConnsPerHost:    2,
		MaxConnsPerHost:        4,
		IdleConnTimeout:        60 * time.Second,
		ResponseHeaderTimeout:  c.cfg.ResponseHeaderTimeout,
		MaxResponseHeaderBytes: c.cfg.MaxResponseHeaderBytes,
	}
	c.pool[key] = &pooledTransport{tr: tr, lastUsed: time.Now()}
	return tr, nil
}

func (c *Client) dialPinned(ctx context.Context, pa ParsedAddr, expected identity.NodeID, policy ipPolicy) (net.Conn, error) {
	raw, err := c.dialRaw(ctx, pa, policy)
	if err != nil {
		return nil, err
	}
	cert, err := c.cfg.Identity.TLSCertificate()
	if err != nil {
		_ = raw.Close()
		return nil, err
	}
	tc := tls.Client(raw, &tls.Config{
		MinVersion:   tls.VersionTLS13,
		MaxVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{ALPNProtocol},
		// No CA: trust is pinning the certificate's key to the Node ID.
		InsecureSkipVerify: true, //nolint:gosec // replaced by VerifyPeerCertificate pinning below
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			return checkPinned(rawCerts, expected)
		},
	})
	hctx, cancel := context.WithTimeout(ctx, c.cfg.HandshakeTimeout)
	defer cancel()
	if err := tc.HandshakeContext(hctx); err != nil {
		_ = raw.Close()
		return nil, err
	}
	st := tc.ConnectionState()
	if st.Version != tls.VersionTLS13 || st.NegotiatedProtocol != ALPNProtocol || len(st.PeerCertificates) != 1 {
		_ = raw.Close()
		return nil, fmt.Errorf("%w: unexpected TLS parameters", ErrNodeIDMismatch)
	}
	if err := checkPinned([][]byte{st.PeerCertificates[0].Raw}, expected); err != nil {
		_ = raw.Close()
		return nil, err
	}
	return tc, nil
}

func checkPinned(rawCerts [][]byte, expected identity.NodeID) error {
	got, err := identity.PeerNodeID(rawCerts)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrNodeIDMismatch, err)
	}
	if !got.Equal(expected) {
		return ErrNodeIDMismatch
	}
	return nil
}

// dialRaw returns a TCP connection (direct or tunnelled) after applying the
// address policy.
func (c *Client) dialRaw(ctx context.Context, pa ParsedAddr, policy ipPolicy) (net.Conn, error) {
	if !policy.trusted && PortDenied(pa.Port) {
		return nil, fmt.Errorf("%w: port", ErrAddressRefused)
	}
	d := &net.Dialer{Timeout: c.cfg.DialTimeout}
	var proxy *url.URL
	if c.cfg.Proxy != nil {
		var err error
		proxy, err = c.cfg.Proxy(&url.URL{Scheme: "https", Host: pa.HostPort()})
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrProxyConnect, err)
		}
	}
	if proxy != nil {
		// The resolved address is chosen by the proxy and cannot be checked
		// here: refuse what can be recognised, rely on the Node ID pin.
		if pa.IP.IsValid() {
			if err := c.checkIP(pa.IP, pa.Port, policy); err != nil {
				return nil, err
			}
		} else if !policy.trusted {
			return nil, fmt.Errorf("%w: hostname through proxy needs trusted configuration", ErrAddressRefused)
		} else if !policy.allowLoopback && isLocalName(pa.Host) {
			return nil, fmt.Errorf("%w: local name", ErrAddressRefused)
		}
		return dialViaProxy(ctx, d, proxy, pa.HostPort())
	}

	if !pa.IP.IsValid() && !policy.allowLoopback && isLocalName(pa.Host) {
		return nil, fmt.Errorf("%w: local name", ErrAddressRefused)
	}
	var ips []netip.Addr
	if pa.IP.IsValid() {
		ips = []netip.Addr{pa.IP}
	} else {
		var err error
		ips, err = c.cfg.Resolver.LookupNetIP(ctx, "ip", pa.Host)
		if err != nil {
			return nil, fmt.Errorf("transport: resolve peer: %w", err)
		}
		if len(ips) == 0 {
			return nil, fmt.Errorf("transport: resolve peer: no addresses")
		}
	}
	// Refuse the whole host if any answer is disallowed, so mixed answers
	// cannot be used to probe internal addresses.
	for _, ip := range ips {
		if err := c.checkIP(ip, pa.Port, policy); err != nil {
			return nil, err
		}
	}
	// The Control hook checks the socket address actually being connected to,
	// after the resolver and the kernel have had their say.
	d.Control = c.controlFn(policy)
	var lastErr error
	for _, ip := range ips {
		conn, err := d.DialContext(ctx, "tcp", netip.AddrPortFrom(ip.Unmap(), pa.Port).String())
		if err == nil {
			return conn, nil
		}
		lastErr = err
		if errors.Is(err, ErrAddressRefused) || ctx.Err() != nil {
			break
		}
	}
	return nil, lastErr
}

func (c *Client) checkIP(ip netip.Addr, port uint16, policy ipPolicy) error {
	if err := policy.check(ip); err != nil {
		return err
	}
	if c.isOwnAddr(ip.Unmap(), port) {
		return fmt.Errorf("%w: own listen address", ErrAddressRefused)
	}
	return nil
}

func (c *Client) isOwnAddr(ip netip.Addr, port uint16) bool {
	for _, own := range c.cfg.OwnListenAddrs {
		host, portStr, err := net.SplitHostPort(own)
		if err != nil {
			continue
		}
		p, err := net.LookupPort("tcp", portStr)
		if err != nil || uint16(p) != port {
			continue
		}
		oip, err := netip.ParseAddr(host)
		if host != "" && err == nil && !oip.IsUnspecified() {
			if oip.Unmap() == ip {
				return true
			}
			continue
		}
		// Wildcard listener: every local address is "ours".
		if ip.IsLoopback() {
			return true
		}
		for _, la := range c.cfg.localAddrs() {
			if la.Unmap() == ip {
				return true
			}
		}
	}
	return false
}

func interfaceAddrs() []netip.Addr {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil
	}
	var out []netip.Addr
	for _, a := range addrs {
		if ipn, ok := a.(*net.IPNet); ok {
			if ip, ok := netip.AddrFromSlice(ipn.IP); ok {
				out = append(out, ip.Unmap())
			}
		}
	}
	return out
}

// cappedBody enforces the caller's response cap while streaming.
type cappedBody struct {
	rc      io.ReadCloser
	max     int64
	read    int64
	err     error
	onClose func()
	once    sync.Once
}

func (b *cappedBody) Read(p []byte) (int, error) {
	if b.err != nil {
		return 0, b.err
	}
	if int64(len(p)) > b.max-b.read+1 {
		p = p[:b.max-b.read+1]
	}
	n, err := b.rc.Read(p)
	b.read += int64(n)
	if b.read > b.max {
		b.err = ErrBodyTooLarge
		return n - int(b.read-b.max), ErrBodyTooLarge
	}
	if err != nil {
		b.err = err
	}
	return n, err
}

func (b *cappedBody) Close() error {
	err := b.rc.Close()
	b.once.Do(b.onClose)
	return err
}

// controlFn returns a net.Dialer.Control hook that applies the policy to the
// socket address actually being connected to.
func (c *Client) controlFn(policy ipPolicy) func(network, address string, _ syscall.RawConn) error {
	return func(_, address string, _ syscall.RawConn) error {
		ap, err := netip.ParseAddrPort(address)
		if err != nil {
			return fmt.Errorf("%w: unparsable socket address", ErrAddressRefused)
		}
		return c.checkIP(ap.Addr(), ap.Port(), policy)
	}
}
