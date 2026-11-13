package discovery

import (
	"context"
	"errors"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

// intake stores up to MaxNewPerExchange previously unknown peers as
// unverified exchange entries. Known peers are left untouched: hearsay never
// refreshes a peer or overwrites its address.
func (s *Service) intake(ctx context.Context, nc NetworkConfig, peers []sanitizedPeer) (int, error) {
	added := 0
	now := s.now()
	for _, p := range peers {
		if added >= s.limits.MaxNewPerExchange {
			break
		}
		ok, err := s.cfg.Store.Insert(ctx, database.PeernetPeer{
			NetworkID: nc.ID.String(), NodeID: p.node.String(), Address: p.addr, Source: database.PeernetSourceExchange,
			FirstSeen: now, LastSeen: now,
		}, s.caps())
		if errors.Is(err, database.ErrPeernetPeerCapReached) {
			break
		}
		if err != nil {
			return added, err
		}
		if ok {
			added++
		}
	}
	return added, nil
}

// candidate is a validated Source candidate.
type candidate struct {
	node identity.NodeID
	addr string
}

// sanitizeCandidates validates candidates from a local Source. They are
// trusted configuration, so hostnames and (where the network allows them)
// private addresses are accepted; self and malformed entries are dropped.
func (s *Service) sanitizeCandidates(nc NetworkConfig, cands []Candidate) []candidate {
	if len(cands) > MaxPeerEntries {
		cands = cands[:MaxPeerEntries]
	}
	seen := make(map[identity.NodeID]struct{}, len(cands))
	var out []candidate
	for _, c := range cands {
		if c.NodeID.IsZero() || c.NodeID.Equal(s.cfg.Self) {
			continue
		}
		if _, dup := seen[c.NodeID]; dup {
			continue
		}
		pa, err := transport.CheckPeerAddress(c.Address, nc.AllowPrivate, s.cfg.AllowLoopbackForTesting)
		if err != nil {
			continue
		}
		seen[c.NodeID] = struct{}{}
		out = append(out, candidate{node: c.NodeID, addr: pa.HostPort()})
	}
	return out
}

// intakeCandidates validates and stores candidates from a Source (trusted
// local configuration, stored with source "local"). Unknown nodes are added,
// at most MaxNewPerExchange per call. For a node already known, a different
// address reported by the Source becomes a candidate address, which is only
// adopted after a pinned dial proved the same Node ID there. It returns the
// number of new peers stored.
func (s *Service) intakeCandidates(ctx context.Context, nc NetworkConfig, cands []Candidate) (int, error) {
	added := 0
	now := s.now()
	net := nc.ID.String()
	for _, c := range s.sanitizeCandidates(nc, cands) {
		existing, err := s.cfg.Store.Get(ctx, net, c.node.String())
		if err != nil {
			return added, err
		}
		if existing != nil {
			if err := s.updateFromSource(ctx, net, existing, c, now); err != nil {
				return added, err
			}
			continue
		}
		if added >= s.limits.MaxNewPerExchange {
			continue
		}
		ok, err := s.cfg.Store.Insert(ctx, database.PeernetPeer{
			NetworkID: net, NodeID: c.node.String(), Address: c.addr, Source: database.PeernetSourceLocal,
			FirstSeen: now, LastSeen: now,
		}, s.caps())
		if errors.Is(err, database.ErrPeernetPeerCapReached) {
			continue
		}
		if err != nil {
			return added, err
		}
		if ok {
			added++
		}
	}
	return added, nil
}

// updateFromSource reconciles a known peer with what a local Source reports.
// User pins always win. Otherwise the peer is marked as coming from the
// source, and a different address is only recorded as a candidate to be
// verified by a dial.
func (s *Service) updateFromSource(ctx context.Context, net string, existing *database.PeernetPeer, c candidate, now time.Time) error {
	if existing.Source == database.PeernetSourceUser {
		return nil
	}
	if existing.Address == c.addr || existing.CandidateAddress == c.addr {
		return s.promoteToLocal(ctx, net, existing)
	}
	if !existing.Verified {
		// Nothing verified is lost by replacing a peer-supplied address that
		// has never been proved; the new one is still dialled before trust.
		if _, err := s.cfg.Store.SetUnverifiedAddress(ctx, net, existing.NodeID, c.addr, now); err != nil {
			return err
		}
	} else if err := s.cfg.Store.SetCandidateAddress(ctx, net, existing.NodeID, c.addr, now); err != nil {
		return err
	}
	return s.promoteToLocal(ctx, net, existing)
}

func (s *Service) promoteToLocal(ctx context.Context, net string, p *database.PeernetPeer) error {
	if p.Source == database.PeernetSourceLocal {
		return nil
	}
	return s.cfg.Store.PromoteToLocal(ctx, net, p.NodeID)
}
