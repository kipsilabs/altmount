package recsync

import (
	"context"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
)

// RecordCounts splits stored records by source.
type RecordCounts struct {
	// Local are this node's own records, Peer those received from peers.
	Local, Peer int
}

// NetworkStatus is the record exchange state of one network. Nothing in it
// is record content.
type NetworkStatus struct {
	Releases     RecordCounts
	Attestations RecordCounts
	// Publishers is the number of distinct publishers of stored records.
	Publishers int
	// LastPull is the last time a pull from any peer succeeded (zero: never;
	// derived from the persisted sync cursors after a restart).
	LastPull time.Time
	// Stored, Duplicates and Rejected count ingest outcomes since start;
	// Rejected is keyed by reason name.
	Stored, Duplicates int64
	Rejected           map[string]int64
	// PeersBackedOff is how many peers are currently skipped for sending
	// invalid records; PeerBackoffs counts every back-off since start.
	PeersBackedOff int
	PeerBackoffs   int64
}

// Status returns the status of a network. A store failure leaves the stored
// counts at zero and is logged once per change.
func (x *Exchange) Status(ctx context.Context, nw netid.NetworkID) NetworkStatus {
	c := x.ing.Counters(nw)
	st := NetworkStatus{Stored: c.Stored, Duplicates: c.Duplicates, Rejected: c.Rejected, PeerBackoffs: c.Backoffs,
		PeersBackedOff: x.backoff.countFor(nw, x.now())}
	x.mu.Lock()
	st.LastPull = x.lastPull[nw.String()]
	x.mu.Unlock()
	if st.LastPull.IsZero() {
		// After a restart the in-memory time is gone: the sync cursors, which
		// are persisted, still say when records were last pulled.
		if lp, ok := x.cfg.Cursors.(interface {
			LastPullAt(ctx context.Context, networkID string) (time.Time, error)
		}); ok {
			if at, err := lp.LastPullAt(ctx, nw.String()); err == nil {
				st.LastPull = at
			}
		}
	}
	stats, err := x.cfg.Store.Stats(ctx, nw.String())
	x.logChange(ctx, "record stats "+nw.String()[:8], err)
	if err != nil {
		return st
	}
	st.Publishers = stats.Publishers
	st.Releases = RecordCounts{
		Local: stats.Count(database.PeernetRecordKindRelease, database.PeernetRecordSourceLocal),
		Peer:  stats.Count(database.PeernetRecordKindRelease, database.PeernetRecordSourcePeer),
	}
	st.Attestations = RecordCounts{
		Local: stats.Count(database.PeernetRecordKindAttestation, database.PeernetRecordSourceLocal),
		Peer:  stats.Count(database.PeernetRecordKindAttestation, database.PeernetRecordSourcePeer),
	}
	return st
}
