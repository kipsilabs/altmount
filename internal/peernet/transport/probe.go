package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"

	"github.com/kipsilabs/altmount/internal/peernet/identity"
)

// ProbeNodeID connects to addr under the same address policy, proxy handling
// and timeouts as Do, completes the mutual TLS 1.3 handshake, sends no HTTP
// request, closes the connection and returns the Node ID the server proved by
// the handshake. It exists for addresses whose Node ID is not known yet (a
// user-entered address). opts.AllowPrivate applies as for Do; MaxResponseBytes
// is ignored.
func (c *Client) ProbeNodeID(ctx context.Context, addr string, opts CallOptions) (identity.NodeID, error) {
	var zero identity.NodeID
	pa, err := ValidateAddress(addr)
	if err != nil {
		return zero, err
	}
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return zero, fmt.Errorf("%w: client closed", ErrInvalidConfig)
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = c.cfg.DefaultTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	policy := ipPolicy{allowPrivate: opts.AllowPrivate, allowLoopback: c.cfg.AllowLoopbackForTesting, trusted: opts.TrustedAddress}
	raw, err := c.dialRaw(ctx, pa, policy)
	if err != nil {
		return zero, err
	}
	defer raw.Close()
	cert, err := c.cfg.Identity.TLSCertificate()
	if err != nil {
		return zero, err
	}
	var proved identity.NodeID
	tc := tls.Client(raw, &tls.Config{
		MinVersion:   tls.VersionTLS13,
		MaxVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{ALPNProtocol},
		// No CA: the certificate only proves key ownership; the Node ID is
		// derived from that key and returned to the caller to pin.
		InsecureSkipVerify: true, //nolint:gosec // replaced by VerifyPeerCertificate below
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			id, err := identity.PeerNodeID(rawCerts)
			if err != nil {
				return fmt.Errorf("%w: %v", ErrNodeIDMismatch, err)
			}
			proved = id
			return nil
		},
	})
	hctx, hcancel := context.WithTimeout(ctx, c.cfg.HandshakeTimeout)
	defer hcancel()
	if err := tc.HandshakeContext(hctx); err != nil {
		return zero, err
	}
	st := tc.ConnectionState()
	if st.Version != tls.VersionTLS13 || st.NegotiatedProtocol != ALPNProtocol || len(st.PeerCertificates) != 1 {
		return zero, fmt.Errorf("%w: unexpected TLS parameters", ErrNodeIDMismatch)
	}
	id, err := identity.PeerNodeID([][]byte{st.PeerCertificates[0].Raw})
	if err != nil || proved.IsZero() || !id.Equal(proved) {
		return zero, fmt.Errorf("%w: handshake identity", ErrNodeIDMismatch)
	}
	_ = tc.Close()
	return id, nil
}

// CheckPeerAddress validates addr like ValidateAddress and additionally
// applies the dial policy's class rules to literal IP addresses and local
// names, so callers can refuse unusable peer-supplied addresses before storing
// or relaying them. allowPrivate permits RFC 1918, RFC 4193 and CGNAT
// literals; allowLoopback is for tests only. Hostnames cannot be resolved
// here and are checked only for always-local names: the dialler still applies
// the policy to the addresses actually connected to.
func CheckPeerAddress(addr string, allowPrivate, allowLoopback bool) (ParsedAddr, error) {
	pa, err := ValidateAddress(addr)
	if err != nil {
		return pa, err
	}
	if pa.IP.IsValid() {
		if err := (ipPolicy{allowPrivate: allowPrivate, allowLoopback: allowLoopback}).check(pa.IP); err != nil {
			return ParsedAddr{}, err
		}
		return pa, nil
	}
	if !allowLoopback && IsLoopbackAddress(pa) {
		return ParsedAddr{}, ErrLoopbackRefused
	}
	if !allowLoopback && isLocalName(pa.Host) {
		return ParsedAddr{}, fmt.Errorf("%w: local name", ErrAddressRefused)
	}
	return pa, nil
}
