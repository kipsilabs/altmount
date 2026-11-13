// Package transport implements the authenticated peer transport: mutual
// TLS 1.3 between nodes identified only by their Ed25519 keys, with plain
// HTTP/1.1 request and response on top. Every interaction is started by the
// client. The server exposes only routes registered on it and never the
// application's main API.
package transport

import (
	"errors"
	"fmt"
	"time"
)

// ALPNProtocol is the only TLS application protocol offered or accepted:
// HTTP/1.1. HTTP/2 is deliberately not available, so every limit in Limits
// applies per connection with no stream multiplexing. (net/http closes the
// connection for any other non-empty ALPN name, so a custom label cannot be
// used with the standard server.)
const ALPNProtocol = "http/1.1"

// Typed errors returned by this package. Callers should use errors.Is.
var (
	ErrNodeIDMismatch = errors.New("transport: peer node id does not match expected")
	ErrInvalidAddress = errors.New("transport: invalid peer address")
	ErrAddressRefused = errors.New("transport: peer address refused by policy")
	// ErrLoopbackRefused is a more specific ErrAddressRefused: loopback is
	// never a usable peer address (only tests may allow it).
	ErrLoopbackRefused  = fmt.Errorf("%w: loopback", ErrAddressRefused)
	ErrBodyTooLarge     = errors.New("transport: body exceeds limit")
	ErrProxyUnsupported = errors.New("transport: unsupported proxy scheme")
	ErrProxyConnect     = errors.New("transport: proxy tunnel failed")
	ErrServerNotStarted = errors.New("transport: server not started")
	ErrServerStarted    = errors.New("transport: server already started")
	ErrInvalidConfig    = errors.New("transport: invalid configuration")
	ErrPeerNotAdmitted  = errors.New("transport: peer not admitted")
)

// Limits are the server-side hard limits. Zero values are replaced by the
// defaults from DefaultLimits.
type Limits struct {
	// MaxConns is the maximum number of simultaneous connections.
	MaxConns int
	// MaxConnsPerIP is the maximum simultaneous connections per remote IP
	// (per /64 for IPv6).
	MaxConnsPerIP int
	// HandshakeTimeout bounds the TCP-accept-to-TLS-complete phase.
	HandshakeTimeout time.Duration
	// ReadHeaderTimeout bounds reading request headers.
	ReadHeaderTimeout time.Duration
	// ReadTimeout bounds reading the entire request including the body.
	ReadTimeout time.Duration
	// WriteTimeout bounds writing the response.
	WriteTimeout time.Duration
	// IdleTimeout bounds keep-alive idle time.
	IdleTimeout time.Duration
	// MaxHeaderBytes caps request header size.
	MaxHeaderBytes int
	// RatePerSecond and RateBurst define the per-peer token bucket.
	RatePerSecond float64
	RateBurst     int
	// GlobalRatePerSecond and GlobalRateBurst define one token bucket shared
	// by every peer, so many identities together cannot exceed it.
	GlobalRatePerSecond float64
	GlobalRateBurst     int
	// MaxConcurrentPerPeer caps in-flight requests per peer.
	MaxConcurrentPerPeer int
	// MaxTrackedPeers bounds the memory used for per-peer accounting.
	MaxTrackedPeers int
}

// DefaultLimits returns conservative defaults.
func DefaultLimits() Limits {
	return Limits{
		MaxConns:             256,
		MaxConnsPerIP:        8,
		HandshakeTimeout:     5 * time.Second,
		ReadHeaderTimeout:    5 * time.Second,
		ReadTimeout:          30 * time.Second,
		WriteTimeout:         60 * time.Second,
		IdleTimeout:          60 * time.Second,
		MaxHeaderBytes:       16 << 10,
		RatePerSecond:        10,
		RateBurst:            30,
		GlobalRatePerSecond:  200,
		GlobalRateBurst:      400,
		MaxConcurrentPerPeer: 4,
		MaxTrackedPeers:      10000,
	}
}

func (l Limits) withDefaults() Limits {
	d := DefaultLimits()
	if l.MaxConns <= 0 {
		l.MaxConns = d.MaxConns
	}
	if l.MaxConnsPerIP <= 0 {
		l.MaxConnsPerIP = d.MaxConnsPerIP
	}
	if l.HandshakeTimeout <= 0 {
		l.HandshakeTimeout = d.HandshakeTimeout
	}
	if l.ReadHeaderTimeout <= 0 {
		l.ReadHeaderTimeout = d.ReadHeaderTimeout
	}
	if l.ReadTimeout <= 0 {
		l.ReadTimeout = d.ReadTimeout
	}
	if l.WriteTimeout <= 0 {
		l.WriteTimeout = d.WriteTimeout
	}
	if l.IdleTimeout <= 0 {
		l.IdleTimeout = d.IdleTimeout
	}
	if l.MaxHeaderBytes <= 0 {
		l.MaxHeaderBytes = d.MaxHeaderBytes
	}
	if l.RatePerSecond <= 0 {
		l.RatePerSecond = d.RatePerSecond
	}
	if l.RateBurst <= 0 {
		l.RateBurst = d.RateBurst
	}
	if l.GlobalRatePerSecond <= 0 {
		l.GlobalRatePerSecond = d.GlobalRatePerSecond
	}
	if l.GlobalRateBurst <= 0 {
		l.GlobalRateBurst = d.GlobalRateBurst
	}
	if l.MaxConcurrentPerPeer <= 0 {
		l.MaxConcurrentPerPeer = d.MaxConcurrentPerPeer
	}
	if l.MaxTrackedPeers <= 0 {
		l.MaxTrackedPeers = d.MaxTrackedPeers
	}
	return l
}
