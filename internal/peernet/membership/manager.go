package membership

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

// Operation errors.
var (
	ErrUnknownNetwork    = errors.New("membership: unknown network")
	ErrNotHost           = errors.New("membership: not the host of this network")
	ErrNotMember         = errors.New("membership: not a member of this network")
	ErrNoHostAddress     = errors.New("membership: host address not set")
	ErrAlreadyJoined     = errors.New("membership: network already known")
	ErrInviteTTL         = errors.New("membership: invalid invite lifetime")
	ErrRevocationFull    = errors.New("membership: revocation list is full")
	ErrNoValidCert       = errors.New("membership: no valid certificate")
	ErrDenied            = errors.New("membership: request denied")
	ErrInvalidLevel      = errors.New("membership: invalid exposure level")
	ErrOpenNetwork       = errors.New("membership: the open network's level is configured, not stored")
	maxInviteTTL         = 30 * 24 * time.Hour
	revocationRetention  = 3 * CertLifetime
	openNetworkName      = "Open network"
	errMissingRootKeyRow = errors.New("membership: network row has no valid root key")
)

// Network is the stored state of one network.
type Network = database.PeernetNetwork

// Member is a member row as seen by a host.
type Member = database.PeernetMember

// Invite is the at-rest invite record (hash only). ID is the code hash.
type Invite = database.PeernetInvite

// Caller makes authenticated calls to peers. *transport.Client satisfies it.
type Caller interface {
	Do(ctx context.Context, addr string, expected identity.NodeID, req transport.Request, opts transport.CallOptions) (*transport.Response, error)
}

// Config configures a Manager.
type Config struct {
	Store *database.PeernetNetworkRepository
	// KeysDir holds one sub-directory per hosted network (named by network
	// ID) with that network's root key.
	KeysDir string
	// NodeID is this node's own identity.
	NodeID identity.NodeID
	// Caller is used for member-side calls (Join, RenewDue). May be nil on a
	// node that only hosts.
	Caller       Caller
	Now          func() time.Time
	RedeemLimits RedeemLimits
	// CallTimeout bounds one maintenance call to a host (renewal or
	// revocation refresh). Zero means DefaultCallTimeout.
	CallTimeout time.Duration
}

// Manager implements host, member and per-request membership logic.
type Manager struct {
	cfg         Config
	now         func() time.Time
	callTimeout time.Duration
	limiter     *redeemLimiter
	hostMu      sync.Mutex // serialises host-side list re-signing

	openEnsured atomic.Bool

	maintMu sync.Mutex
	maint   map[string]*maintState // per joined network, see RenewDue

	cacheMu  sync.Mutex
	cache    map[string]cachedNetwork
	cacheGen uint64 // bumped by every invalidation
}

// NewManager validates cfg.
func NewManager(cfg Config) (*Manager, error) {
	if cfg.Store == nil || cfg.KeysDir == "" || cfg.NodeID.IsZero() {
		return nil, errors.New("membership: store, keys dir and node id are required")
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	ct := cfg.CallTimeout
	if ct <= 0 {
		ct = DefaultCallTimeout
	}
	return &Manager{cfg: cfg, now: now, callTimeout: ct, limiter: newRedeemLimiter(cfg.RedeemLimits, now),
		maint: map[string]*maintState{}, cache: map[string]cachedNetwork{}}, nil
}

func (m *Manager) unix() int64 { return m.now().Unix() }

// --- accessors ---

// EnsureOpenNetwork creates the open network's settings row if absent. After
// the first success it is a no-op, so reads never write.
func (m *Manager) EnsureOpenNetwork(ctx context.Context) error {
	if m.openEnsured.Load() {
		return nil
	}
	id := netid.Open()
	t := m.unix()
	err := m.cfg.Store.CreateNetwork(ctx, database.PeernetNetwork{
		NetworkID: id.String(), DisplayName: openNetworkName, Role: database.PeernetRoleOpen,
		CreatedAt: t, UpdatedAt: t})
	if err != nil && !errors.Is(err, database.ErrPeernetExists) {
		return err
	}
	m.openEnsured.Store(true)
	m.invalidate()
	return nil
}

// ListNetworks returns every network (the open network included) with its
// exposure level and settings. The level of the open network's row is unused:
// it lives in the configuration.
func (m *Manager) ListNetworks(ctx context.Context) ([]Network, error) {
	if err := m.EnsureOpenNetwork(ctx); err != nil {
		return nil, err
	}
	return m.cfg.Store.ListNetworks(ctx)
}

// GetNetwork returns one network or ErrUnknownNetwork. Rows are cached for a
// few seconds; every local change (including every revocation list update)
// invalidates the cache at once, so a cached row never outlives a revocation
// this node has applied.
func (m *Manager) GetNetwork(ctx context.Context, id netid.NetworkID) (*Network, error) {
	if id.IsOpen() {
		if err := m.EnsureOpenNetwork(ctx); err != nil {
			return nil, err
		}
	}
	key := id.String()
	m.cacheMu.Lock()
	if e, ok := m.cache[key]; ok && time.Now().Before(e.until) {
		cp := e.row
		m.cacheMu.Unlock()
		return &cp, nil
	}
	gen := m.cacheGen
	m.cacheMu.Unlock()

	n, err := m.cfg.Store.GetNetwork(ctx, key)
	if errors.Is(err, database.ErrPeernetNotFound) {
		return nil, ErrUnknownNetwork
	}
	if err != nil {
		return nil, err
	}
	m.cacheMu.Lock()
	if m.cacheGen == gen { // no change landed while we were reading
		m.cache[key] = cachedNetwork{row: *n, until: time.Now().Add(networkCacheTTL)}
	}
	m.cacheMu.Unlock()
	return n, nil
}

// invalidate drops every cached network row.
func (m *Manager) invalidate() {
	m.cacheMu.Lock()
	m.cacheGen++
	clear(m.cache)
	m.cacheMu.Unlock()
}

// SetExposureLevel sets the level 0-3 of a hosted or joined network. The open
// network's level lives in the configuration; setting it here is refused.
func (m *Manager) SetExposureLevel(ctx context.Context, id netid.NetworkID, level int) error {
	if level < 0 || level > 3 {
		return ErrInvalidLevel
	}
	if id.IsOpen() {
		return ErrOpenNetwork
	}
	defer m.invalidate()
	if _, err := m.GetNetwork(ctx, id); err != nil {
		return err
	}
	return m.cfg.Store.SetExposureLevel(ctx, id.String(), level, m.unix())
}

// SettingsUpdate is a partial change to a private network's settings; nil
// fields are kept.
type SettingsUpdate struct {
	Level                 *int
	AllowPrivateAddresses *bool
	HostAddress           *string
}

// UpdateSettings validates every requested change first and then applies them
// in one statement, so a failure never leaves a partial change.
func (m *Manager) UpdateSettings(ctx context.Context, id netid.NetworkID, u SettingsUpdate) error {
	if id.IsOpen() {
		return ErrOpenNetwork
	}
	if u.Level != nil && (*u.Level < 0 || *u.Level > 3) {
		return ErrInvalidLevel
	}
	upd := database.NetworkSettingsUpdate{Level: u.Level, AllowPrivateAddresses: u.AllowPrivateAddresses}
	if u.HostAddress != nil {
		pa, err := transport.ValidateAddress(*u.HostAddress)
		if err != nil {
			return err
		}
		hp := pa.HostPort()
		upd.HostAddress = &hp
	}
	defer m.invalidate()
	n, err := m.GetNetwork(ctx, id)
	if err != nil {
		return err
	}
	if upd.HostAddress != nil && n.Role != database.PeernetRoleHost {
		return ErrNotHost
	}
	return m.cfg.Store.UpdateSettings(ctx, id.String(), upd, m.unix())
}

// SetAllowPrivateAddresses sets the per-network private address switch.
func (m *Manager) SetAllowPrivateAddresses(ctx context.Context, id netid.NetworkID, allow bool) error {
	defer m.invalidate()
	if _, err := m.GetNetwork(ctx, id); err != nil {
		return err
	}
	return m.cfg.Store.SetAllowPrivateAddresses(ctx, id.String(), allow, m.unix())
}

// --- host operations ---

func (m *Manager) keyDir(id netid.NetworkID) string { return filepath.Join(m.cfg.KeysDir, id.String()) }

func (m *Manager) hostKey(id netid.NetworkID) (*identity.Key, error) {
	k, err := identity.Load(m.keyDir(id))
	if err != nil {
		return nil, fmt.Errorf("membership: load network root key: %w", err)
	}
	if !netid.FromRootKey(k.PublicKey()).Equal(id) {
		return nil, errMissingRootKeyRow
	}
	return k, nil
}

func (m *Manager) hostNetwork(ctx context.Context, id netid.NetworkID) (*Network, error) {
	n, err := m.GetNetwork(ctx, id)
	if err != nil {
		return nil, err
	}
	if n.Role != database.PeernetRoleHost {
		return nil, ErrNotHost
	}
	return n, nil
}

// CreateNetwork creates a private network hosted by this node: a dedicated
// root key, the host's own certificate and an empty revocation list.
func (m *Manager) CreateNetwork(ctx context.Context, displayName string) (*Network, error) {
	defer m.invalidate()
	if err := os.MkdirAll(m.cfg.KeysDir, 0o700); err != nil {
		return nil, fmt.Errorf("membership: keys dir: %w", err)
	}
	var salt [8]byte
	if _, err := rand.Read(salt[:]); err != nil {
		return nil, err
	}
	tmp := filepath.Join(m.cfg.KeysDir, ".pending-"+hex.EncodeToString(salt[:]))
	key, err := identity.LoadOrCreate(tmp)
	if err != nil {
		_ = os.RemoveAll(tmp)
		return nil, err
	}
	id := netid.FromRootKey(key.PublicKey())
	dest := m.keyDir(id)
	if err := os.Rename(tmp, dest); err != nil {
		_ = os.RemoveAll(tmp)
		return nil, fmt.Errorf("membership: store root key: %w", err)
	}
	now := m.now()
	cert, err := IssueCertificate(key, id, m.cfg.NodeID, RoleHost, now, CertLifetime)
	if err == nil {
		var list *RevocationList
		list, err = IssueRevocationList(key, id, 1, now, nil)
		if err == nil {
			err = m.cfg.Store.CreateNetwork(ctx, database.PeernetNetwork{
				NetworkID: id.String(), DisplayName: displayName, Role: database.PeernetRoleHost,
				HostNodeID: m.cfg.NodeID.String(), RootPublicKey: []byte(key.PublicKey()),
				OwnCertificate: cert.Marshal(), RevocationList: list.Marshal(), RevocationSeq: 1,
				CreatedAt: now.Unix(), UpdatedAt: now.Unix(),
			})
		}
	}
	if err != nil {
		_ = os.RemoveAll(dest)
		return nil, err
	}
	return m.GetNetwork(ctx, id)
}

// SetHostAddress sets the address members are told to dial (host:port).
func (m *Manager) SetHostAddress(ctx context.Context, id netid.NetworkID, addr string) error {
	pa, err := transport.ValidateAddress(addr)
	if err != nil {
		return err
	}
	defer m.invalidate()
	if _, err := m.hostNetwork(ctx, id); err != nil {
		return err
	}
	return m.cfg.Store.SetHostAddress(ctx, id.String(), pa.HostPort(), m.unix())
}

// CreateInvite creates a single-use invite and returns the invite link. The
// link is returned only here; only a hash of the code is stored.
func (m *Manager) CreateInvite(ctx context.Context, id netid.NetworkID, ttl time.Duration, alias string) (string, error) {
	if ttl <= 0 || ttl > maxInviteTTL {
		return "", ErrInviteTTL
	}
	n, err := m.hostNetwork(ctx, id)
	if err != nil {
		return "", err
	}
	if n.HostAddress == "" {
		return "", ErrNoHostAddress
	}
	code, err := NewInviteCode()
	if err != nil {
		return "", err
	}
	link, err := Link{NetworkID: id, RootKey: ed25519.PublicKey(n.RootPublicKey), HostAddr: n.HostAddress,
		HostNode: m.cfg.NodeID, Code: code}.Encode()
	if err != nil {
		return "", err
	}
	now := m.now()
	if err := m.cfg.Store.CreateInvite(ctx, database.PeernetInvite{
		NetworkID: id.String(), CodeHash: code.Hash(), Alias: alias,
		CreatedAt: now.Unix(), ExpiresAt: now.Add(ttl).Unix()}); err != nil {
		return "", err
	}
	return link, nil
}

// ListInvites lists a hosted network's invites (hashes only).
func (m *Manager) ListInvites(ctx context.Context, id netid.NetworkID) ([]Invite, error) {
	if _, err := m.hostNetwork(ctx, id); err != nil {
		return nil, err
	}
	return m.cfg.Store.ListInvites(ctx, id.String())
}

// RevokeInvite deletes an invite by its ID (code hash).
func (m *Manager) RevokeInvite(ctx context.Context, id netid.NetworkID, inviteID string) error {
	if _, err := m.hostNetwork(ctx, id); err != nil {
		return err
	}
	return m.cfg.Store.DeleteInvite(ctx, id.String(), inviteID)
}

// ListMembers lists a hosted network's members.
func (m *Manager) ListMembers(ctx context.Context, id netid.NetworkID) ([]Member, error) {
	if _, err := m.hostNetwork(ctx, id); err != nil {
		return nil, err
	}
	return m.cfg.Store.ListMembers(ctx, id.String())
}

// RemoveMember marks the member revoked and makes sure the signed revocation
// list lists it. The member can never renew again.
//
// It is idempotent: the authoriser consults only the signed list, so a failure
// after the row was marked (re-signing or storing the list) must not leave the
// member authorised. A retry finds the row already revoked, skips the marking
// and completes the list; RenewDue repairs the same state on its own. A node
// revoked longer ago than the list retention window is reported as not a
// member.
func (m *Manager) RemoveMember(ctx context.Context, id netid.NetworkID, node identity.NodeID) error {
	m.hostMu.Lock()
	defer m.hostMu.Unlock()
	defer m.invalidate()
	n, err := m.hostNetwork(ctx, id)
	if err != nil {
		return err
	}
	key, err := m.hostKey(id)
	if err != nil {
		return err
	}
	now := m.now()
	mem, err := m.cfg.Store.GetMember(ctx, n.NetworkID, node.String())
	if errors.Is(err, database.ErrPeernetNotFound) {
		return ErrNotMember
	}
	if err != nil {
		return err
	}
	if mem.RevokedAt == 0 {
		recent, err := m.cfg.Store.ListRevokedSince(ctx, id.String(), now.Add(-revocationRetention).Unix())
		if err != nil {
			return err
		}
		if len(recent) >= MaxRevoked {
			return ErrRevocationFull
		}
		if err := m.cfg.Store.RevokeMember(ctx, id.String(), node.String(), now.Unix()); err != nil {
			if errors.Is(err, database.ErrPeernetNotFound) {
				return ErrNotMember
			}
			return err
		}
	} else if mem.RevokedAt < now.Add(-revocationRetention).Unix() {
		return ErrNotMember
	}
	changed, err := m.syncRevocations(ctx, key, n.NetworkID, now)
	if err != nil {
		return err
	}
	if !changed && mem.RevokedAt != 0 {
		return ErrNotMember // removed earlier and already in the signed list
	}
	return nil
}

// syncRevocations makes the stored signed list contain every node revoked
// within the retention window, re-signing it (with a higher sequence) when it
// does not, and reports whether it did. It reads the current row, so it must
// run under hostMu.
func (m *Manager) syncRevocations(ctx context.Context, key *identity.Key, networkID string, now time.Time) (bool, error) {
	id := netid.FromRootKey(key.PublicKey())
	n, err := m.cfg.Store.GetNetwork(ctx, networkID)
	if err != nil {
		return false, err
	}
	hexIDs, err := m.cfg.Store.ListRevokedSince(ctx, networkID, now.Add(-revocationRetention).Unix())
	if err != nil {
		return false, err
	}
	ids := make([]identity.NodeID, 0, len(hexIDs))
	for _, h := range hexIDs {
		nid, perr := identity.ParseNodeID(h)
		if perr != nil {
			continue
		}
		ids = append(ids, nid)
	}
	if cur, perr := ParseRevocationList(n.RevocationList); perr == nil && cur.Verify(key.PublicKey(), now) == nil {
		complete := true
		for _, nid := range ids {
			if !cur.Contains(nid) {
				complete = false
				break
			}
		}
		if complete {
			return false, nil
		}
	}
	list, err := IssueRevocationList(key, id, n.RevocationSeq+1, now, ids)
	if err != nil {
		return false, err
	}
	defer m.invalidate()
	if err := m.cfg.Store.SetRevocationList(ctx, networkID, list.Sequence, list.Marshal(), now.Unix()); err != nil {
		return false, err
	}
	return true, nil
}

// repairRevocations completes the signed list of a hosted network when a
// revoked member is missing from it (an interrupted RemoveMember).
func (m *Manager) repairRevocations(ctx context.Context, n *Network) error {
	id, err := netid.ParseNetworkID(n.NetworkID)
	if err != nil {
		return err
	}
	m.hostMu.Lock()
	defer m.hostMu.Unlock()
	key, err := m.hostKey(id)
	if err != nil {
		return err
	}
	_, err = m.syncRevocations(ctx, key, n.NetworkID, m.now())
	return err
}

// DeleteNetwork deletes a hosted network, its members, invites and root key.
func (m *Manager) DeleteNetwork(ctx context.Context, id netid.NetworkID) error {
	defer m.invalidate()
	if _, err := m.hostNetwork(ctx, id); err != nil {
		return err
	}
	if err := m.cfg.Store.DeleteNetwork(ctx, id.String()); err != nil {
		return err
	}
	return os.RemoveAll(m.keyDir(id))
}

// ensureHostCert returns this node's certificate on a hosted network,
// re-issuing it when it is missing or past half-life.
func (m *Manager) ensureHostCert(ctx context.Context, n *Network) (*Certificate, error) {
	id, err := netid.ParseNetworkID(n.NetworkID)
	if err != nil {
		return nil, err
	}
	if c, perr := ParseCertificate(n.OwnCertificate); perr == nil && !certDue(c, m.now()) {
		return c, nil
	}
	key, err := m.hostKey(id)
	if err != nil {
		return nil, err
	}
	c, err := IssueCertificate(key, id, m.cfg.NodeID, RoleHost, m.now(), CertLifetime)
	if err != nil {
		return nil, err
	}
	err = m.cfg.Store.SetOwnCertificate(ctx, n.NetworkID, c.Marshal(), m.unix())
	m.invalidate()
	if err != nil {
		return nil, err
	}
	n.OwnCertificate = c.Marshal()
	return c, nil
}

// certDue reports whether the certificate is at or past half-life.
func certDue(c *Certificate, now time.Time) bool {
	return now.Unix() >= c.IssuedAt+(c.ExpiresAt-c.IssuedAt)/2
}

// networkCacheTTL is how long GetNetwork may serve a row without reading the
// database. Local changes invalidate it sooner.
const networkCacheTTL = 3 * time.Second

type cachedNetwork struct {
	row   Network
	until time.Time
}
