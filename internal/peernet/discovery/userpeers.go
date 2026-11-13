package discovery

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

// ErrUnknownNetwork is returned for a network the service was not given.
var ErrUnknownNetwork = errors.New("discovery: unknown network")

// ErrNothingToConfirm is returned by ConfirmChangedNode when the address has
// no pending node ID change.
var ErrNothingToConfirm = errors.New("discovery: no node id change to confirm")

// Reconcile makes the user-entered peers of a network match addrs
// ("host:port" strings from configuration). Removed entries are deleted from
// the store, new ones wait to be resolved by the next Tick. Invalid entries
// are skipped and reported together in the returned error; the valid ones are
// still applied.
func (s *Service) Reconcile(ctx context.Context, network netid.NetworkID, addrs []string) error {
	var errs []error
	keep := make([]string, 0, len(addrs))
	seen := map[string]struct{}{}
	for _, a := range addrs {
		pa, err := transport.ValidateAddress(strings.TrimSpace(a))
		if err != nil {
			errs = append(errs, fmt.Errorf("peer address %q: %w", truncate(a, 64), err))
			continue
		}
		hp := pa.HostPort()
		if _, dup := seen[hp]; dup {
			continue
		}
		seen[hp] = struct{}{}
		keep = append(keep, hp)
	}
	if _, ok := s.network(network.String()); !ok {
		return ErrUnknownNetwork
	}
	if _, err := s.cfg.Store.DeleteUserNotIn(ctx, network.String(), keep); err != nil {
		return err
	}
	unresolved := make([]string, 0, len(keep))
	for _, a := range keep {
		p, err := s.cfg.Store.GetUserByAddress(ctx, network.String(), a)
		if err != nil {
			return err
		}
		if p == nil {
			unresolved = append(unresolved, a)
		}
	}
	s.mu.Lock()
	if ns, ok := s.nets[network.String()]; ok {
		next := make(map[string]*pendingUser, len(unresolved))
		for _, a := range unresolved {
			if old, ok := ns.pending[a]; ok {
				next[a] = old
			} else {
				next[a] = &pendingUser{}
			}
		}
		ns.pending, ns.order = next, unresolved
	}
	s.mu.Unlock()
	return errors.Join(errs...)
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// resolveUser performs the handshake-only probe for a configured address and
// pins the proved Node ID to it.
func (s *Service) resolveUser(ctx context.Context, nc NetworkConfig, addr string) error {
	now := s.now()
	id, err := s.cfg.Dialer.ProbeNodeID(ctx, addr, s.callOpts(nc, 1, true))
	if err != nil {
		return err
	}
	if id.Equal(s.cfg.Self) {
		return errors.New("address belongs to this node")
	}
	return s.pinUser(ctx, nc, addr, id, now)
}

func (s *Service) pinUser(ctx context.Context, nc NetworkConfig, addr string, id identity.NodeID, now time.Time) error {
	net, node := nc.ID.String(), id.String()
	existing, err := s.cfg.Store.Get(ctx, net, node)
	if err != nil {
		return err
	}
	switch {
	case existing == nil:
		if _, err := s.cfg.Store.Insert(ctx, database.PeernetPeer{
			NetworkID: net, NodeID: node, Address: addr, Source: database.PeernetSourceUser,
			FirstSeen: now, LastSeen: now,
		}, s.caps()); err != nil {
			return err
		}
	case existing.Source != database.PeernetSourceUser:
		if err := s.cfg.Store.PromoteToUser(ctx, net, node, addr); err != nil {
			return err
		}
	}
	// The handshake probe proved only that some node answers at this address.
	// The peer is not marked verified and has no success time: only a full
	// authenticated contact (hello and exchange through the membership
	// authoriser) sets those, on the next Tick.
	s.mu.Lock()
	if ns, ok := s.nets[net]; ok {
		delete(ns.pending, addr)
		for i, a := range ns.order {
			if a == addr {
				ns.order = append(ns.order[:i:i], ns.order[i+1:]...)
				break
			}
		}
	}
	s.mu.Unlock()
	return nil
}

// ConfirmChangedNode accepts the new Node ID that a pinned user-entered
// address presented. The new identity is probed again first and must match
// what was flagged. The old peer is removed; the new one is a fresh peer.
func (s *Service) ConfirmChangedNode(ctx context.Context, network netid.NetworkID, address string) error {
	nc, ok := s.network(network.String())
	if !ok {
		return ErrUnknownNetwork
	}
	pa, err := transport.ValidateAddress(address)
	if err != nil {
		return err
	}
	p, err := s.cfg.Store.GetUserByAddress(ctx, network.String(), pa.HostPort())
	if err != nil {
		return err
	}
	if p == nil || p.ChangedNodeID == "" {
		return ErrNothingToConfirm
	}
	want, err := identity.ParseNodeID(p.ChangedNodeID)
	if err != nil {
		return err
	}
	got, err := s.cfg.Dialer.ProbeNodeID(ctx, p.Address, s.callOpts(nc, 1, true))
	if err != nil {
		return err
	}
	if !got.Equal(want) {
		return errors.New("discovery: address now presents yet another node id; not confirmed")
	}
	if err := s.cfg.Store.Delete(ctx, network.String(), p.NodeID); err != nil {
		return err
	}
	return s.pinUser(ctx, nc, p.Address, got, s.now())
}
