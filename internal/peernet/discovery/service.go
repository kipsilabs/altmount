package discovery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"sync"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

// Dialer is the part of *transport.Client the service uses.
type Dialer interface {
	Do(ctx context.Context, addr string, expected identity.NodeID, req transport.Request, opts transport.CallOptions) (*transport.Response, error)
	ProbeNodeID(ctx context.Context, addr string, opts transport.CallOptions) (identity.NodeID, error)
}

// Candidate is a peer reported by a Source.
type Candidate struct {
	NodeID  identity.NodeID
	Address string
}

// Source is a way of finding peers beyond the built-in user-entered list and
// peer exchange. Its candidates are treated exactly like exchange results:
// unverified, capped and subject to the same address validation.
type Source interface {
	Name() string
	Discover(ctx context.Context, network netid.NetworkID) ([]Candidate, error)
}

// Config configures a Service.
type Config struct {
	// Self is this node's ID. Required.
	Self identity.NodeID
	// Dialer makes outbound calls. Required (a *transport.Client).
	Dialer Dialer
	// Store persists peers. Required.
	Store *database.PeernetPeerRepository
	// Auth enforces network membership; nil means AllowAll.
	Auth Authorizer
	// Limits override the defaults field by field.
	Limits Limits
	// Now is the clock; nil means time.Now.
	Now func() time.Time
	// AllowLoopbackForTesting lets loopback addresses pass the address filters
	// (the transport client must be configured likewise). Tests only.
	AllowLoopbackForTesting bool
}

// NetworkConfig describes one network the service takes part in.
type NetworkConfig struct {
	ID netid.NetworkID
	// AllowPrivate permits dialling, storing and relaying private addresses
	// for this network only. Supplied by the caller, never inferred.
	AllowPrivate bool
	// AdvertiseAddr is this node's own reachable "host:port" for this network,
	// sent in hello; empty when the node accepts no incoming connections.
	AdvertiseAddr string
	// TargetPeers is how many recently connected peers to aim for and
	// MaxPeers the most contacted per Tick; zero means the Limits defaults.
	TargetPeers int
	MaxPeers    int
}

type pendingUser struct {
	failures int
	next     time.Time
	lastErr  string
}

type netState struct {
	cfg     NetworkConfig
	pending map[string]*pendingUser // configured user addresses without a pinned node yet
	order   []string
}

// Service is the peer store plus the exchange protocol. Safe for concurrent use.
type Service struct {
	cfg    Config
	limits Limits
	auth   Authorizer
	now    func() time.Time

	mu          sync.Mutex
	nets        map[string]*netState
	sources     []Source
	helloLim    *helloLimiter
	dials       *dialBackBudget
	acks        *lru[helloAck] // advertised addresses a peer acknowledged, per (network, node)
	connIPs     sync.Map       // node id -> remote IP of its latest successful call
	contactErrs sync.Map       // network|node -> text of its last failed contact

	logMu      sync.Mutex
	lastLogged map[string]string
}

// helloAck remembers the advertised address a peer last acknowledged.
type helloAck struct {
	addr string
	at   time.Time
}

// New validates cfg and returns a Service.
func New(cfg Config) (*Service, error) {
	if cfg.Self.IsZero() || cfg.Dialer == nil || cfg.Store == nil {
		return nil, errors.New("discovery: Self, Dialer and Store are required")
	}
	s := &Service{cfg: cfg, limits: cfg.Limits.withDefaults(), auth: cfg.Auth, now: cfg.Now,
		nets: map[string]*netState{}, lastLogged: map[string]string{}}
	s.helloLim = newHelloLimiter(s.limits.MaxTrackedHello)
	s.dials = newDialBackBudget(s.limits.MaxTrackedDialPrefixes)
	s.acks = newLRU[helloAck](s.limits.MaxTrackedHello)
	if s.auth == nil {
		s.auth = AllowAll{}
	}
	if s.now == nil {
		s.now = time.Now
	}
	return s, nil
}

// SetNetwork adds or updates a network. User-entered addresses are kept when
// a network is updated.
func (s *Service) SetNetwork(nc NetworkConfig) error {
	if nc.ID.IsZero() {
		return errors.New("discovery: zero network id")
	}
	if nc.AdvertiseAddr != "" {
		pa, err := transport.CheckPeerAddress(nc.AdvertiseAddr, nc.AllowPrivate, s.cfg.AllowLoopbackForTesting)
		if err != nil {
			return fmt.Errorf("discovery: advertise address: %w", err)
		}
		nc.AdvertiseAddr = pa.HostPort()
	}
	if nc.TargetPeers <= 0 {
		nc.TargetPeers = s.limits.DefaultTargetPeers
	}
	if nc.MaxPeers <= 0 {
		nc.MaxPeers = s.limits.DefaultMaxPeers
	}
	if nc.MaxPeers < nc.TargetPeers {
		nc.MaxPeers = nc.TargetPeers
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if ns, ok := s.nets[nc.ID.String()]; ok {
		ns.cfg = nc
		return nil
	}
	s.nets[nc.ID.String()] = &netState{cfg: nc, pending: map[string]*pendingUser{}}
	return nil
}

// RemoveNetwork stops serving and contacting a network. Stored peers are kept.
func (s *Service) RemoveNetwork(id netid.NetworkID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.nets, id.String())
}

// AddSource registers an extra discovery source.
func (s *Service) AddSource(src Source) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sources = append(s.sources, src)
}

func (s *Service) network(id string) (NetworkConfig, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ns, ok := s.nets[id]
	if !ok {
		return NetworkConfig{}, false
	}
	return ns.cfg, true
}

func (s *Service) caps() database.PeernetPeerCaps {
	return database.PeernetPeerCaps{
		MaxTotal: s.limits.MaxPeersPerNetwork,
		MaxPerSource: map[string]int{
			database.PeernetSourceExchange: s.limits.MaxExchangePeers,
			database.PeernetSourceInbound:  s.limits.MaxInboundPeers,
			database.PeernetSourceLocal:    s.limits.MaxLocalPeers,
		},
		MaxUnverifiedPerPrefix: s.limits.MaxUnverifiedPerPrefix,
	}
}

// callOpts builds the transport options. trusted marks an address from local
// configuration (user-entered or a local Source); peer-learned addresses are
// never trusted.
func (s *Service) callOpts(nc NetworkConfig, maxResp int64, trusted bool) transport.CallOptions {
	return transport.CallOptions{AllowPrivate: nc.AllowPrivate, MaxResponseBytes: maxResp, Timeout: s.limits.CallTimeout, TrustedAddress: trusted}
}

// trustedSource reports whether rows of this source come from local
// configuration rather than from a peer.
func trustedSource(source string) bool {
	return source == database.PeernetSourceUser || source == database.PeernetSourceLocal
}

// helloAllowed reports whether a hello from peer, connecting from ip, may be
// processed for the network, and records the attempt.
func (s *Service) helloAllowed(network string, peer identity.NodeID, ip netip.Addr, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.helloLim.allow(s.limits, network, peer.String(), ip, now)
}

// takeDialBack consumes one slot of the global budget, and of the budget of
// addr's target prefix, for contacting an unverified peer at addr.
func (s *Service) takeDialBack(addr string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dials.take(s.limits, addr, now)
}

// logChange logs err at warning level once per state change, keyed by what
// failed; a nil err resets the key so a later failure is logged again. A
// persistently failing store therefore produces one line, not one per tick or
// request.
func (s *Service) logChange(ctx context.Context, key string, err error) {
	s.logMu.Lock()
	defer s.logMu.Unlock()
	if err == nil {
		delete(s.lastLogged, key)
		return
	}
	msg := err.Error()
	if s.lastLogged[key] == msg {
		return
	}
	s.lastLogged[key] = msg
	slog.WarnContext(ctx, "peernet: discovery problem", "what", key, "error", err)
}

// KnownFor reports whether node has been known in network for at least d.
func (s *Service) KnownFor(ctx context.Context, network netid.NetworkID, node identity.NodeID, d time.Duration) (bool, error) {
	return s.cfg.Store.KnownFor(ctx, network.String(), node.String(), d, s.now())
}
