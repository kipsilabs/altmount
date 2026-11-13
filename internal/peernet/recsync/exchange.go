package recsync

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/discovery"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

// ErrNotPublishing is returned by Publish when the network is not one this
// node publishes to (unknown, off, receive only, or removed).
var ErrNotPublishing = errors.New("recsync: not publishing to this network")

// Dialer is the part of *transport.Client the exchange uses.
type Dialer interface {
	Do(ctx context.Context, addr string, expected identity.NodeID, req transport.Request, opts transport.CallOptions) (*transport.Response, error)
}

// Store is the record repository as the exchange uses it.
type Store interface {
	RecordStore
	ListSince(ctx context.Context, networkID string, afterSeq int64, limit int) (database.PeernetRecordPage, error)
	ListOwnSince(ctx context.Context, networkID string, afterSeq int64, limit int) (database.PeernetRecordPage, error)
	Epoch(ctx context.Context, networkID string) (string, error)
	Stats(ctx context.Context, networkID string) (database.PeernetRecordStats, error)
	PurgeExpired(ctx context.Context, limit int) (int, error)
	DeleteNetwork(ctx context.Context, networkID string) error
	NetworkIDs(ctx context.Context) ([]string, error)
}

// Cursors is the sync cursor repository as the exchange uses it.
type Cursors interface {
	Get(ctx context.Context, networkID, peerNodeID string) (database.PeernetSyncCursor, error)
	SetPullCursor(ctx context.Context, networkID, peerNodeID string, seq int64) error
	SetPushCursor(ctx context.Context, networkID, peerNodeID string, seq int64) error
	ResetPullCursorEpoch(ctx context.Context, networkID, peerNodeID string, seq int64, epoch string) error
	DeleteOrphans(ctx context.Context) (int64, error)
}

// Peer is a peer to sync with: a connected, verified one.
type Peer struct {
	NodeID  identity.NodeID
	Address string
	// Trusted marks an address from local configuration rather than learned
	// from a peer (see transport.CallOptions.TrustedAddress).
	Trusted bool
}

// PeerSource lists the verified peers of a network, at most limit of them.
type PeerSource interface {
	SyncPeers(ctx context.Context, network netid.NetworkID, limit int) ([]Peer, error)
}

// Config configures an Exchange.
type Config struct {
	// Self is this node's ID. Required.
	Self identity.NodeID
	// Store and Cursors persist records and sync positions. Required.
	Store   Store
	Cursors Cursors
	// Dialer makes outbound calls. Required (a *transport.Client).
	Dialer Dialer
	// Auth enforces network membership; nil means discovery.AllowAll.
	Auth discovery.Authorizer
	// Peers lists the peers to sync with. Required for SyncRound.
	Peers PeerSource
	// Limits override the defaults field by field.
	Limits Limits
	// Now is the clock; nil means time.Now.
	Now func() time.Time
}

// NetworkInfo describes one network the exchange takes part in.
type NetworkInfo struct {
	ID netid.NetworkID
	// Level is the exposure level (see PolicyFor). Level 0 is the same as not
	// registering the network.
	Level int
	// AllowPrivate permits dialling private addresses for this network.
	AllowPrivate bool
}

// Exchange runs record exchange for every registered network: the server
// routes, the sync rounds and the upkeep. Safe for concurrent use.
type Exchange struct {
	cfg    Config
	limits Limits
	auth   discovery.Authorizer
	now    func() time.Time
	ing    *Ingester

	backoff *backoff
	budget  *pushBudget
	// pushSlots caps the pushes being ingested at once (a held slot is a
	// value in the channel).
	pushSlots chan struct{}
	// pushGuard caps concurrent pushes per node and per IP; hostile tallies
	// and epoch resets are bounded per peer.
	pushGuard  *pushGuard
	hostile    *tally
	ipHostile  *ipHostility
	epochFlips *epochGuard
	maintRuns  int

	roundMu sync.Mutex // one sync round at a time

	mu       sync.Mutex
	nets     map[string]NetworkInfo
	lastPull map[string]time.Time

	logMu      sync.Mutex
	lastLogged map[string]string
}

// New validates cfg and returns an Exchange.
func New(cfg Config) (*Exchange, error) {
	if cfg.Self.IsZero() || cfg.Store == nil || cfg.Cursors == nil || cfg.Dialer == nil {
		return nil, errors.New("recsync: Self, Store, Cursors and Dialer are required")
	}
	x := &Exchange{cfg: cfg, limits: cfg.Limits.WithDefaults(), auth: cfg.Auth, now: cfg.Now,
		nets: map[string]NetworkInfo{}, lastPull: map[string]time.Time{}, lastLogged: map[string]string{}}
	if x.auth == nil {
		x.auth = discovery.AllowAll{}
	}
	if x.now == nil {
		x.now = time.Now
	}
	x.ing = NewIngester(IngestConfig{Self: cfg.Self, Store: cfg.Store, Now: x.now,
		Limits: database.PeernetRecordLimits{MaxPerPublisher: x.limits.MaxPerPublisher,
			MaxPerRelayer: x.limits.MaxPerRelayer, MaxPerNetwork: x.limits.MaxPerNetwork},
		RatePerSecond: x.limits.IngestRate, Burst: x.limits.IngestBurst})
	x.backoff = newBackoff(x.limits, 4096)
	x.budget = newPushBudget(x.limits.PushRecordsPerMinute, x.limits.MaxTrackedPushers)
	x.pushSlots = make(chan struct{}, x.limits.MaxConcurrentPushes)
	x.pushGuard = newPushGuard(x.limits.MaxPushesPerNode, x.limits.MaxPushesPerIP, x.limits.MaxTrackedPushers)
	x.hostile = newTally(x.limits.MaxTrackedPushers)
	x.ipHostile = newIPHostility(x.limits.MaxTrackedPushers)
	x.epochFlips = newEpochGuard(x.limits.MaxEpochResetsPerHour, x.limits.MaxTrackedPushers)
	return x, nil
}

// SetNetwork registers a network or updates its level. A level whose policy
// does not exist (0) removes the network.
func (x *Exchange) SetNetwork(info NetworkInfo) {
	if info.ID.IsZero() {
		return
	}
	if !PolicyFor(info.Level).Exists {
		x.RemoveNetwork(info.ID)
		return
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	x.nets[info.ID.String()] = info
}

// RemoveNetwork stops serving, pulling and publishing for a network at once.
// Stored records stay until DeleteNetworkData.
func (x *Exchange) RemoveNetwork(id netid.NetworkID) {
	x.mu.Lock()
	delete(x.nets, id.String())
	x.mu.Unlock()
}

func (x *Exchange) network(id netid.NetworkID) (NetworkInfo, bool) {
	x.mu.Lock()
	defer x.mu.Unlock()
	info, ok := x.nets[id.String()]
	return info, ok
}

// networks returns the registered networks sorted by ID.
func (x *Exchange) networks() []NetworkInfo {
	x.mu.Lock()
	out := make([]NetworkInfo, 0, len(x.nets))
	for _, n := range x.nets {
		out = append(out, n)
	}
	x.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].ID.String() < out[j].ID.String() })
	return out
}

// PublishNetworks returns the networks this node currently publishes to
// (level 2 or higher), sorted by ID. A removed or revoked network is not
// registered and so is never listed.
func (x *Exchange) PublishNetworks() []netid.NetworkID {
	var out []netid.NetworkID
	for _, n := range x.networks() {
		if PolicyFor(n.Level).Publish {
			out = append(out, n.ID)
		}
	}
	return out
}

// ReceivingNetworks returns the registered networks this node receives
// records from (level 1 or higher), sorted by ID. Removed and revoked
// networks are not registered and so never listed.
func (x *Exchange) ReceivingNetworks() []NetworkInfo {
	var out []NetworkInfo
	for _, n := range x.networks() {
		if PolicyFor(n.Level).Pull {
			out = append(out, n)
		}
	}
	return out
}

// Publish ingests a record this node built into network nw. The record must
// be signed by this node's key and the network must be one this node
// publishes to, otherwise ErrNotPublishing (or a rejected outcome) results.
func (x *Exchange) Publish(ctx context.Context, nw netid.NetworkID, raw []byte) (Outcome, error) {
	info, ok := x.network(nw)
	if !ok || !PolicyFor(info.Level).Publish {
		return Outcome{}, ErrNotPublishing
	}
	return x.ing.Ingest(ctx, nw, raw, OriginLocal)
}

// DeleteNetworkData removes every record, cursor and counter of a network.
// Call it when the network is left or deleted.
func (x *Exchange) DeleteNetworkData(ctx context.Context, nw netid.NetworkID) error {
	err := x.cfg.Store.DeleteNetwork(ctx, nw.String())
	x.ing.Forget(nw)
	x.backoff.forgetNetwork(nw)
	x.mu.Lock()
	delete(x.lastPull, nw.String())
	x.mu.Unlock()
	return err
}

func (x *Exchange) noteSuccessfulPull(nw netid.NetworkID) {
	now := x.now()
	x.mu.Lock()
	x.lastPull[nw.String()] = now
	x.mu.Unlock()
}

// logChange logs err at warning level once per state change, keyed by what
// failed; a nil err resets the key. It never logs record content.
func (x *Exchange) logChange(ctx context.Context, key string, err error) {
	x.logMu.Lock()
	defer x.logMu.Unlock()
	if err == nil {
		delete(x.lastLogged, key)
		return
	}
	if msg := err.Error(); x.lastLogged[key] != msg {
		x.lastLogged[key] = msg
		slog.WarnContext(ctx, "Peer network record exchange problem", "what", key, "error", err)
	}
}
