package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/records"
	"github.com/kipsilabs/altmount/internal/peernet/recsync"
)

// ErrNotPublishing is returned by PublishLocal for a network this node does
// not publish to (unknown, off, receive only, or removed by its host).
var ErrNotPublishing = recsync.ErrNotPublishing

// ErrRecordsUnavailable is returned when record exchange has no storage
// configured.
var ErrRecordsUnavailable = errors.New("peer network record storage is not configured")

// RecordStatus is the record exchange state of one network (counts of stored
// records by kind and source, distinct publishers, last successful pull and
// the ingest and rejection counters). It contains no record content.
type RecordStatus = recsync.NetworkStatus

// startExchange creates the record exchange when storage is configured.
func (n *node) startExchange() error {
	if n.deps.Records == nil || n.deps.Cursors == nil {
		return nil
	}
	rx, err := recsync.New(recsync.Config{
		Self: n.nodeID, Store: n.deps.Records, Cursors: n.deps.Cursors, Dialer: n.client,
		Auth:  authBridge{mem: n.mem, onRefused: n.noteRefused},
		Peers: peerSource{store: n.deps.Peers, maxFailures: recsync.DefaultLimits().MaxPeerFailures},
	})
	if err != nil {
		return err
	}
	n.rx = rx
	return nil
}

// peerSource offers the verified peers of a network to the record exchange.
type peerSource struct {
	store       *database.PeernetPeerRepository
	maxFailures int
}

func (p peerSource) SyncPeers(ctx context.Context, nw netid.NetworkID, limit int) ([]recsync.Peer, error) {
	rows, err := p.store.ListVerified(ctx, nw.String(), p.maxFailures, limit)
	if err != nil {
		return nil, err
	}
	out := make([]recsync.Peer, 0, len(rows))
	for _, r := range rows {
		id, err := identity.ParseNodeID(r.NodeID)
		if err != nil {
			continue
		}
		// Addresses from local configuration are trusted; learned ones are not.
		trusted := r.Source == database.PeernetSourceUser || r.Source == database.PeernetSourceLocal
		out = append(out, recsync.Peer{NodeID: id, Address: r.Address, Trusted: trusted})
	}
	return out, nil
}

// RecordSigner builds signed records with this node's identity. It is the only
// way to sign: the key itself is never handed out. Every method returns the
// encoded record, ready for PublishLocal.
type RecordSigner interface {
	// NodeID is the publisher ID the records carry.
	NodeID() identity.NodeID
	BuildRelease(nw netid.NetworkID, in records.ReleaseRecord, now time.Time) ([]byte, error)
	BuildAttestation(nw netid.NetworkID, in records.HealthAttestation, now time.Time) ([]byte, error)
}

type nodeSigner struct{ key *identity.Key }

func (s nodeSigner) NodeID() identity.NodeID { return s.key.NodeID() }

func (s nodeSigner) BuildRelease(nw netid.NetworkID, in records.ReleaseRecord, now time.Time) ([]byte, error) {
	_, raw, err := records.BuildReleaseRecord(s.key, nw, in, now)
	return raw, err
}

func (s nodeSigner) BuildAttestation(nw netid.NetworkID, in records.HealthAttestation, now time.Time) ([]byte, error) {
	_, raw, err := records.BuildHealthAttestation(s.key, nw, in, now)
	return raw, err
}

// Signer returns the record signer, or ErrDisabled while the feature is off.
func (s *Service) Signer() (RecordSigner, error) {
	n, err := s.requireNode()
	if err != nil {
		return nil, err
	}
	return nodeSigner{key: n.key}, nil
}

// PublishLocal stores a record this node built (see Signer) in a network it
// publishes to, through the same pipeline as records from peers: it must be
// signed by this node and be for that network. It returns ErrNotPublishing for
// a network at level 0 or 1, unknown, or removed by its host. Peers receive it
// by pull, and by push at the next sync round.
func (s *Service) PublishLocal(ctx context.Context, nw netid.NetworkID, encoded []byte) (recsync.Outcome, error) {
	n, err := s.requireNode()
	if err != nil {
		return recsync.Outcome{}, err
	}
	if n.rx == nil {
		return recsync.Outcome{}, ErrRecordsUnavailable
	}
	return n.rx.Publish(ctx, nw, encoded)
}

// PublishNetworks lists the networks this node currently publishes to: level
// 2 or higher and not revoked. It is empty while the feature is off.
func (s *Service) PublishNetworks() []netid.NetworkID {
	n := s.currentNode()
	if n == nil || n.rx == nil {
		return nil
	}
	return n.rx.PublishNetworks()
}

// setRecordNetwork registers (or updates) a network with the record exchange.
func (n *node) setRecordNetwork(id netid.NetworkID, level int, allowPrivate bool) {
	if n.rx != nil {
		n.rx.SetNetwork(recsync.NetworkInfo{ID: id, Level: level, AllowPrivate: allowPrivate})
	}
}

// removeRecordNetwork stops record exchange for a network at once.
func (n *node) removeRecordNetwork(id netid.NetworkID) {
	n.clearHintCursor(id)
	if n.rx != nil {
		n.rx.RemoveNetwork(id)
	}
}

// forgetRecords deletes a left or deleted network's records, cursors and
// counters. A failure is logged and finished by the next orphan cleanup.
func (n *node) forgetRecords(ctx context.Context, id netid.NetworkID) {
	n.clearHintCursor(id)
	if st := n.deps.PublishState; st != nil {
		err := st.DeleteNetwork(ctx, id.String())
		if err != nil {
			err = fmt.Errorf("%w (retried by upkeep)", err)
		}
		n.logChange(ctx, "delete network publish state "+id.String()[:8], err)
	}
	if n.rx == nil {
		return
	}
	if err := n.rx.DeleteNetworkData(ctx, id); err != nil {
		n.logChange(ctx, "delete network records "+id.String()[:8], fmt.Errorf("%w (retried by upkeep)", err))
		return
	}
	n.logChange(ctx, "delete network records "+id.String()[:8], nil)
}

// orphanCleanupEvery is how many upkeep passes lie between orphan-network
// record cleanups (the pass count starts at zero, so one runs at the start).
const orphanCleanupEvery = 20

// maintainRecords is the record part of an upkeep pass: expiry purge in
// bounded batches and cursor cleanup every time, orphan-network cleanup when
// orphans is set.
func (n *node) maintainRecords(ctx context.Context, orphans bool) {
	if n.rx == nil {
		return
	}
	var known func(context.Context) (map[string]bool, error)
	if orphans {
		known = n.knownNetworkIDs
	}
	n.rx.Maintain(ctx, known)
}

// knownNetworkIDs lists the IDs of the networks that currently exist.
func (n *node) knownNetworkIDs(ctx context.Context) (map[string]bool, error) {
	rows, err := n.mem.ListNetworks(ctx)
	if err != nil {
		return nil, err
	}
	m := make(map[string]bool, len(rows))
	for _, r := range rows {
		m[r.NetworkID] = true
	}
	return m, nil
}

// syncLoop runs the record sync rounds. The first one waits a short while so
// discovery has found peers; later ones come every SyncInterval (jittered),
// sooner while a peer still has a backlog. It stops with ctx.
func (n *node) syncLoop(ctx context.Context) {
	defer n.wg.Done()
	every := n.deps.SyncInterval
	if every <= 0 {
		every = recsync.DefaultLimits().PullInterval
	}
	timer := time.NewTimer(jitter(min(every, firstSyncDelay)))
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		res := n.rx.SyncRound(ctx)
		n.logChange(ctx, "record sync", res.Err)
		wait := jitter(every)
		if res.Backlog {
			wait = min(wait, jitter(recsync.DefaultLimits().BacklogInterval))
		}
		timer.Reset(wait)
	}
}

// firstSyncDelay bounds the wait before the first sync round after start.
const firstSyncDelay = 20 * time.Second
