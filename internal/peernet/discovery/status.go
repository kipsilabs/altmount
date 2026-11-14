package discovery

import (
	"context"
	"sort"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
)

// Source names reported in PeerStatus.
const (
	SourceUser     = database.PeernetSourceUser
	SourceExchange = database.PeernetSourceExchange
	SourceInbound  = database.PeernetSourceInbound
)

// PeerStatus is one known peer as shown on a settings screen.
type PeerStatus struct {
	NodeID      string // empty while Unresolved
	Address     string
	Source      string
	Verified    bool
	FirstSeen   time.Time
	LastSuccess time.Time
	Failures    int
	// NodeIDChanged is set on a user-entered peer whose address presented a
	// different Node ID than the pinned one; ChangedNodeID is the new one. The
	// old pin is kept until the user calls ConfirmChangedNode.
	NodeIDChanged bool
	ChangedNodeID string
	// Unresolved marks a configured address whose Node ID is not known yet.
	Unresolved bool
	LastError  string
}

// statusSource maps a stored source to the three reported ones: peers from a
// local discovery source are shown as learned (exchange) peers.
func statusSource(source string) string {
	if source == database.PeernetSourceLocal {
		return SourceExchange
	}
	return source
}

// Counts summarises a network.
type Counts struct {
	Total, Verified, User, Exchange, Inbound int
	Active                                   int // success within Limits.ActiveWindow
	NodeIDChanged                            int
	Unresolved                               int
}

// Peers lists the known peers of a network: user-entered first (including
// unresolved addresses), then verified, then the rest.
func (s *Service) Peers(ctx context.Context, network netid.NetworkID) ([]PeerStatus, error) {
	rows, err := s.cfg.Store.List(ctx, network.String())
	if err != nil {
		return nil, err
	}
	out := make([]PeerStatus, 0, len(rows))
	for _, p := range rows {
		out = append(out, PeerStatus{
			NodeID: p.NodeID, Address: p.Address, Source: statusSource(p.Source), Verified: p.Verified,
			FirstSeen: p.FirstSeen, LastSuccess: p.LastSuccess, Failures: p.Failures,
			NodeIDChanged: p.ChangedNodeID != "", ChangedNodeID: p.ChangedNodeID,
			LastError: s.lastContactError(p.NetworkID, p.NodeID),
		})
	}
	s.mu.Lock()
	if ns, ok := s.nets[network.String()]; ok {
		for _, a := range ns.order {
			if pu, ok := ns.pending[a]; ok {
				out = append(out, PeerStatus{Address: a, Source: SourceUser, Unresolved: true, Failures: pu.failures, LastError: pu.lastErr})
			}
		}
	}
	s.mu.Unlock()
	rank := func(p PeerStatus) int {
		switch {
		case p.Source == SourceUser:
			return 0
		case p.Verified:
			return 1
		}
		return 2
	}
	sort.SliceStable(out, func(i, j int) bool { return rank(out[i]) < rank(out[j]) })
	return out, nil
}

// Counts returns the counters for a network.
func (s *Service) Counts(ctx context.Context, network netid.NetworkID) (Counts, error) {
	rows, err := s.cfg.Store.List(ctx, network.String())
	if err != nil {
		return Counts{}, err
	}
	now := s.now()
	var c Counts
	for _, p := range rows {
		c.Total++
		if p.Verified {
			c.Verified++
		}
		if p.ChangedNodeID != "" {
			c.NodeIDChanged++
		}
		if !p.LastSuccess.IsZero() && now.Sub(p.LastSuccess) <= s.limits.ActiveWindow {
			c.Active++
		}
		switch p.Source {
		case SourceUser:
			c.User++
		case SourceExchange, database.PeernetSourceLocal:
			c.Exchange++
		case SourceInbound:
			c.Inbound++
		}
	}
	s.mu.Lock()
	if ns, ok := s.nets[network.String()]; ok {
		c.Unresolved = len(ns.pending)
		c.User += len(ns.pending)
	}
	s.mu.Unlock()
	return c, nil
}

// lastContactError is the text of the peer's last failed contact ("" after a
// success or when it never failed since start).
func (s *Service) lastContactError(network, node string) string {
	if v, ok := s.contactErrs.Load(network + "|" + node); ok {
		return v.(string)
	}
	return ""
}
