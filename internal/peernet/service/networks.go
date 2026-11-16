package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/discovery"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/membership"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

// rejectLoopback refuses a host address that names this machine: nobody else
// can ever use it, so a host must not hand it out in invites. Tests may allow
// loopback.
func (s *Service) rejectLoopback(addr string) error {
	if s.deps.allowLoopback || addr == "" {
		return nil
	}
	pa, err := transport.ValidateAddress(addr)
	if err != nil {
		return nil // malformed addresses are reported by their own validation
	}
	if transport.IsLoopbackAddress(pa) {
		return transport.ErrLoopbackRefused
	}
	return nil
}

func parseID(s string) (netid.NetworkID, error) { return netid.ParseNetworkID(s) }

// NetworkUpdate is a partial change to a private network; nil fields are kept.
type NetworkUpdate struct {
	Level                 *int
	AllowPrivateAddresses *bool
	HostAddress           *string
}

// Invite is a stored invite. ID is a hash of the code, never the code.
type Invite struct {
	ID                           string
	Alias                        string
	CreatedAt, ExpiresAt, UsedAt time.Time // zero when unset
	UsedBy                       string
}

// Member is a member of a hosted network.
type Member struct {
	NodeID                             string
	Alias                              string
	JoinedAt, CertExpiresAt, RevokedAt time.Time // zero when unset
}

func unixTime(u int64) time.Time {
	if u <= 0 {
		return time.Time{}
	}
	return time.Unix(u, 0)
}

// networkOf returns the stored network, rejecting the open network when
// privateOnly is set.
func (n *node) networkOf(ctx context.Context, id netid.NetworkID, privateOnly bool) (*database.PeernetNetwork, error) {
	if privateOnly && id.IsOpen() {
		return nil, ErrOpenNetwork
	}
	row, err := n.mem.GetNetwork(ctx, id)
	if err != nil {
		return nil, err
	}
	if privateOnly && row.Role == database.PeernetRoleOpen {
		return nil, ErrOpenNetwork
	}
	return row, nil
}

func (s *Service) statusOf(ctx context.Context, n *node, id string) (NetworkStatus, error) {
	nid, err := parseID(id)
	if err != nil {
		return NetworkStatus{}, err
	}
	row, err := n.mem.GetNetwork(ctx, nid)
	if err != nil {
		return NetworkStatus{}, err
	}
	return n.networkStatus(ctx, row, s.currentSettings())
}

// Peers lists a network's known peers.
func (s *Service) Peers(ctx context.Context, id netid.NetworkID) ([]discovery.PeerStatus, error) {
	n, err := s.requireNode()
	if err != nil {
		return nil, err
	}
	if _, err := n.networkOf(ctx, id, false); err != nil {
		return nil, err
	}
	return n.disc.Peers(ctx, id)
}

// ConfirmPeer re-confirms a changed node ID at a user-entered address.
func (s *Service) ConfirmPeer(ctx context.Context, id netid.NetworkID, address string) error {
	n, err := s.requireNode()
	if err != nil {
		return err
	}
	if _, err := n.networkOf(ctx, id, false); err != nil {
		return err
	}
	return n.disc.ConfirmChangedNode(ctx, id, address)
}

// CreateNetwork creates a hosted private network.
func (s *Service) CreateNetwork(ctx context.Context, name string) (NetworkStatus, error) {
	n, err := s.requireNode()
	if err != nil {
		return NetworkStatus{}, err
	}
	if !s.currentSettings().AcceptIncoming {
		return NetworkStatus{}, ErrAcceptIncomingRequired
	}
	row, err := n.mem.CreateNetwork(ctx, database.CleanPeerText(name, database.PeernetMaxDisplayNameRunes))
	if err != nil {
		return NetworkStatus{}, fmt.Errorf("create network: %w", err)
	}
	// Members are told to dial the advertise address, so use it from the start
	// when the network could actually use it (a new network does not allow
	// private addresses, and a loopback address is never reachable).
	if adv := s.currentSettings().AdvertiseAddress; adv != "" {
		if _, err := transport.CheckPeerAddress(adv, row.AllowPrivateAddresses, s.deps.allowLoopback); err == nil {
			if id, perr := parseID(row.NetworkID); perr == nil {
				if err := n.mem.SetHostAddress(ctx, id, adv); err != nil {
					return NetworkStatus{}, fmt.Errorf("set host address: %w", err)
				}
			}
		}
	}
	n.refresh(ctx)
	return s.statusOf(ctx, n, row.NetworkID)
}

// JoinNetwork joins a private network from an invite link. The link is never
// logged and never appears in a returned error.
func (s *Service) JoinNetwork(ctx context.Context, link string, allowPrivate bool) (NetworkStatus, error) {
	n, err := s.requireNode()
	if err != nil {
		return NetworkStatus{}, err
	}
	if l, perr := membership.ParseLink(link); perr == nil {
		if err := s.rejectLoopback(l.HostAddr); err != nil {
			return NetworkStatus{}, err
		}
	}
	row, err := n.mem.Join(ctx, link, membership.JoinOptions{AllowPrivateAddresses: allowPrivate})
	if err != nil {
		return NetworkStatus{}, err
	}
	n.refresh(ctx)
	return s.statusOf(ctx, n, row.NetworkID)
}

// UpdateNetwork changes a private network's level, private-address policy or
// host address.
func (s *Service) UpdateNetwork(ctx context.Context, id netid.NetworkID, u NetworkUpdate) (NetworkStatus, error) {
	n, err := s.requireNode()
	if err != nil {
		return NetworkStatus{}, err
	}
	if _, err := n.networkOf(ctx, id, true); err != nil {
		return NetworkStatus{}, err
	}
	if u.HostAddress != nil {
		if err := s.rejectLoopback(*u.HostAddress); err != nil {
			return NetworkStatus{}, err
		}
	}
	// Everything is validated first and applied in one step: a failure never
	// leaves a partial change.
	if err := n.mem.UpdateSettings(ctx, id, membership.SettingsUpdate{
		Level: u.Level, AllowPrivateAddresses: u.AllowPrivateAddresses, HostAddress: u.HostAddress}); err != nil {
		return NetworkStatus{}, err
	}
	n.refresh(ctx)
	return s.statusOf(ctx, n, id.String())
}

// DeleteNetwork deletes a hosted network or leaves a joined one.
func (s *Service) DeleteNetwork(ctx context.Context, id netid.NetworkID) error {
	n, err := s.requireNode()
	if err != nil {
		return err
	}
	row, err := n.networkOf(ctx, id, true)
	if err != nil {
		return err
	}
	if row.Role == database.PeernetRoleHost {
		err = n.mem.DeleteNetwork(ctx, id)
	} else {
		err = n.mem.Leave(ctx, id)
	}
	if err != nil {
		return err
	}
	n.refresh(ctx)
	n.forgetRecords(ctx, id)
	// The network is gone; a failed peer cleanup is logged and finished by the
	// next upkeep pass (cleanupOrphanPeers), so nothing is orphaned for good.
	if err := forgetPeers(ctx, n.deps.Peers, id.String()); err != nil {
		slog.WarnContext(ctx, "Could not remove all peers of a removed network; it will be retried",
			"network", id.String()[:8], "error", err)
	}
	return nil
}

// peerStore is the part of the peer repository the cleanup needs.
type peerStore interface {
	List(ctx context.Context, network string) ([]database.PeernetPeer, error)
	Delete(ctx context.Context, network, node string) error
}

// forgetPeers drops every stored peer of a network. It continues past a
// failing delete and returns the first error, so one bad row cannot keep the
// rest in place.
func forgetPeers(ctx context.Context, store peerStore, network string) error {
	rows, err := store.List(ctx, network)
	if err != nil {
		return fmt.Errorf("list peers: %w", err)
	}
	var first error
	for _, p := range rows {
		if err := store.Delete(ctx, p.NetworkID, p.NodeID); err != nil && first == nil {
			first = fmt.Errorf("delete peer: %w", err)
		}
	}
	return first
}

// cleanupOrphanPeers removes peers stored for networks that no longer exist
// (an earlier cleanup was interrupted or failed). It runs on every upkeep
// pass, which also covers the first one after a restart.
func (n *node) cleanupOrphanPeers(ctx context.Context) {
	ids, err := n.deps.Networks.ListOrphanPeerNetworkIDs(ctx)
	if err != nil {
		n.logChange(ctx, "orphan peers", err)
		return
	}
	n.logChange(ctx, "orphan peers", nil)
	for _, id := range ids {
		n.logChange(ctx, "orphan peers "+id[:8], forgetPeers(ctx, n.deps.Peers, id))
	}
}

// ListInvites lists the invites of a hosted network.
func (s *Service) ListInvites(ctx context.Context, id netid.NetworkID) ([]Invite, error) {
	n, err := s.requireNode()
	if err != nil {
		return nil, err
	}
	rows, err := n.mem.ListInvites(ctx, id)
	if err != nil {
		return nil, err
	}
	out := make([]Invite, 0, len(rows))
	for _, r := range rows {
		out = append(out, Invite{ID: r.CodeHash, Alias: r.Alias, CreatedAt: unixTime(r.CreatedAt),
			ExpiresAt: unixTime(r.ExpiresAt), UsedAt: unixTime(r.UsedAt), UsedBy: r.UsedBy})
	}
	return out, nil
}

// CreateInvite creates an invite and returns its link, the only time the code
// is available. The host address defaults to the configured advertise address.
func (s *Service) CreateInvite(ctx context.Context, id netid.NetworkID, ttl time.Duration, alias string) (string, error) {
	n, err := s.requireNode()
	if err != nil {
		return "", err
	}
	row, err := n.mem.GetNetwork(ctx, id)
	if err != nil {
		return "", err
	}
	if err := s.rejectLoopback(row.HostAddress); err != nil {
		return "", err
	}
	if row.Role == database.PeernetRoleHost && row.HostAddress == "" {
		if adv := s.currentSettings().AdvertiseAddress; adv != "" {
			if err := s.rejectLoopback(adv); err != nil {
				return "", err
			}
			if err := n.mem.SetHostAddress(ctx, id, adv); err != nil {
				return "", err
			}
		}
	}
	return n.mem.CreateInvite(ctx, id, ttl, database.CleanPeerText(alias, database.PeernetMaxAliasRunes))
}

// RevokeInvite deletes an invite.
func (s *Service) RevokeInvite(ctx context.Context, id netid.NetworkID, inviteID string) error {
	n, err := s.requireNode()
	if err != nil {
		return err
	}
	err = n.mem.RevokeInvite(ctx, id, inviteID)
	if errors.Is(err, database.ErrPeernetNotFound) {
		return ErrNotFound
	}
	return err
}

// ListMembers lists the members of a hosted network.
func (s *Service) ListMembers(ctx context.Context, id netid.NetworkID) ([]Member, error) {
	n, err := s.requireNode()
	if err != nil {
		return nil, err
	}
	rows, err := n.mem.ListMembers(ctx, id)
	if err != nil {
		return nil, err
	}
	out := make([]Member, 0, len(rows))
	for _, r := range rows {
		out = append(out, Member{NodeID: r.NodeID, Alias: r.Alias, JoinedAt: unixTime(r.JoinedAt),
			CertExpiresAt: unixTime(r.CertExpiresAt), RevokedAt: unixTime(r.RevokedAt)})
	}
	return out, nil
}

// RemoveMember revokes a member of a hosted network.
func (s *Service) RemoveMember(ctx context.Context, id netid.NetworkID, node identity.NodeID) error {
	n, err := s.requireNode()
	if err != nil {
		return err
	}
	if err := n.mem.RemoveMember(ctx, id, node); err != nil {
		return err
	}
	// Removal is immediate on the host: the node is no longer a peer, so its
	// stored row must not keep showing as verified or active.
	if err := n.deps.Peers.Delete(ctx, id.String(), node.String()); err != nil {
		slog.WarnContext(ctx, "Could not forget a removed member's peer row", "error", err)
	}
	return nil
}
