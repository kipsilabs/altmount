package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/membership"
	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

// Network kinds reported in NetworkStatus.
const (
	KindOpen    = "open"
	KindPrivate = "private"
)

// PeerCounts summarises a network's peers.
type PeerCounts struct {
	Total, Verified, Active, NodeIDChanged, Unresolved int
}

// NetworkStatus is one network as shown on the settings screen. Names come
// from other machines for joined networks and must be rendered as plain text.
type NetworkStatus struct {
	ID                    string
	Name                  string
	Kind                  string // open or private
	Role                  string // open, host or member
	Level                 int
	AllowPrivateAddresses bool
	HostAddress           string
	CertExpiresAt         time.Time // zero when the network has no certificate
	// Revoked is set on a joined network whose host removed this node;
	// RevokedAt is when this node found out. A removed network carries no
	// peer traffic and can only be left.
	Revoked   bool
	RevokedAt time.Time
	Peers     PeerCounts
	// Records is the record exchange state (zero when record storage is not
	// configured).
	Records RecordStatus
}

// BackfillStatus is the progress of the content-index backfill.
type BackfillStatus struct {
	Running                   bool
	Scanned, Indexed, Skipped int
}

// Status is the snapshot behind GET /peernet/status. It never contains the
// private key or any invite code.
type Status struct {
	Enabled        bool
	NodeID         string // only while enabled and started
	Listening      bool
	ListenPort     int
	ListenError    string
	Error          string // why the feature could not start, if it could not
	AcceptIncoming bool
	// AdvertiseWarning is non-empty when the configured advertise address
	// cannot be used on an active network. It never contains the address.
	AdvertiseWarning string
	Networks         []NetworkStatus
	Backfill         BackfillStatus
	// Library says how many library files can be shared.
	Library LibraryStatus
	// Publish counts what the publishing pipeline did since start (zero while
	// publishing is not configured).
	Publish PublishStatus
	// Advice counts what the consuming side did since start.
	Advice AdviceStatus
	// Bundles counts bundle building, serving and fetching since start.
	Bundles BundleStatus
}

// Status returns the current snapshot.
func (s *Service) Status(ctx context.Context) (Status, error) {
	set := s.currentSettings()
	st := Status{Enabled: set.Enabled, ListenPort: set.ListenPort, Networks: []NetworkStatus{}}
	if !set.Enabled {
		return st, nil
	}
	st.AcceptIncoming = set.AcceptIncoming
	s.mu.RLock()
	st.Error = s.startErr
	s.mu.RUnlock()
	n := s.currentNode()
	if n == nil {
		return st, nil
	}
	st.NodeID = n.nodeID.String()
	port, listenErr := n.listenState()
	st.Listening = port != 0
	if st.Listening {
		st.ListenPort = port
	}
	st.ListenError = listenErr
	s.mu.RLock()
	idx := s.index
	s.mu.RUnlock()
	if idx != nil {
		st.Backfill = BackfillStatus{
			Running: idx.running.Load(), Scanned: int(idx.scanned.Load()),
			Indexed: int(idx.indexed.Load()), Skipped: int(idx.skipped.Load()),
		}
		if idx.deps.Content != nil {
			st.Library = idx.library(ctx)
		}
	}
	rows, err := n.mem.ListNetworks(ctx)
	if err != nil {
		return st, fmt.Errorf("list networks: %w", err)
	}
	if n.pipe != nil {
		st.Publish = n.pipe.stats.snapshot()
	}
	st.Advice = s.adviceStatus()
	st.Bundles = n.bundleStatus(ctx)
	st.AdvertiseWarning = advertiseWarning(set.AdvertiseAddress, rows, set, n.deps.allowLoopback)
	for i := range rows {
		ns, err := n.networkStatus(ctx, &rows[i], set)
		if err != nil {
			return st, err
		}
		st.Networks = append(st.Networks, ns)
	}
	return st, nil
}

func (n *node) networkStatus(ctx context.Context, row *database.PeernetNetwork, set Settings) (NetworkStatus, error) {
	ns := NetworkStatus{
		ID: row.NetworkID, Name: row.DisplayName, Kind: KindPrivate, Role: row.Role,
		Level: row.ExposureLevel, AllowPrivateAddresses: row.AllowPrivateAddresses, HostAddress: row.HostAddress,
	}
	if row.Role == database.PeernetRoleOpen {
		// The open network's level lives in the configuration, not the row.
		ns.Kind, ns.Level, ns.AllowPrivateAddresses = KindOpen, set.OpenLevel, false
	} else if c, err := membership.ParseCertificate(row.OwnCertificate); err == nil {
		ns.CertExpiresAt = time.Unix(c.ExpiresAt, 0)
	}
	id, idErr := parseID(row.NetworkID)
	if idErr == nil && n.rx != nil {
		ns.Records = n.rx.Status(ctx, id)
	}
	if row.RevokedAt != 0 {
		ns.Revoked, ns.RevokedAt = true, unixTime(row.RevokedAt)
		return ns, nil // no peer traffic: nothing to count
	}
	if idErr != nil {
		return ns, nil
	}
	c, err := n.disc.Counts(ctx, id)
	if err != nil {
		return ns, fmt.Errorf("count peers: %w", err)
	}
	ns.Peers = PeerCounts{Total: c.Total, Verified: c.Verified, Active: c.Active,
		NodeIDChanged: c.NodeIDChanged, Unresolved: c.Unresolved}
	return ns, nil
}

// advertiseWarning explains why the configured advertise address is not used,
// or returns "". Loopback is never usable; a private address is refused on
// every network that does not allow private addresses (the open network never
// does). The text never contains the address.
func advertiseWarning(adv string, rows []database.PeernetNetwork, set Settings, allowLoopback bool) string {
	if adv == "" {
		return ""
	}
	if _, err := transport.CheckPeerAddress(adv, true, allowLoopback); errors.Is(err, transport.ErrLoopbackRefused) {
		return "The advertise address is a loopback address, which other machines can never reach. " +
			"Use this machine's LAN or public address instead."
	}
	refusedOn := 0
	for _, row := range rows {
		level, allowPrivate := row.ExposureLevel, row.AllowPrivateAddresses
		if row.Role == database.PeernetRoleOpen {
			level, allowPrivate = set.OpenLevel, false
		}
		if level < 1 || row.RevokedAt != 0 {
			continue
		}
		if _, err := transport.CheckPeerAddress(adv, allowPrivate, allowLoopback); err != nil {
			refusedOn++
		}
	}
	if refusedOn == 0 {
		return ""
	}
	return "The advertise address is a private or local address, so it is not announced on networks " +
		"that do not allow private addresses (the open network never does). Other peers will not learn it there."
}
