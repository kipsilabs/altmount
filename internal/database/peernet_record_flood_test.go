package database

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func fromRelayer(i int) func(*PeernetRecord) {
	return func(r *PeernetRecord) { r.RelayerNodeID = nodeHex(1000 + i) }
}

func byPublisher(i int) func(*PeernetRecord) {
	return func(r *PeernetRecord) { r.PublisherNodeID = nodeHex(i) }
}

func ownRec(i int) func(*PeernetRecord) {
	return func(r *PeernetRecord) { r.Source = PeernetRecordSourceLocal; r.PublisherNodeID = nodeHex(i) }
}

func requireCountsConsistent(t *testing.T, b *recBackend, network string) {
	t.Helper()
	diff, err := b.recs.CheckCounts(context.Background(), network)
	require.NoError(t, err)
	require.Empty(t, diff, "maintained counters disagree with the rows")
}

func (b *recBackend) relayerRows(t *testing.T, network string, relayer int) int {
	t.Helper()
	var n int
	require.NoError(t, b.recs.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM peernet_records WHERE network_id = ? AND source = 1 AND relayer_node_id = ?`,
		network, nodeHex(1000+relayer)).Scan(&n))
	return n
}

func TestPeernetRecords_CountersStayExactAcrossEveryMutation(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		ctx := context.Background()
		nw := hexID('a')
		var first PeernetRecord
		for i := 0; i < 5; i++ {
			r := mkRec(fromRelayer(1), byPublisher(i+1))
			if i == 0 {
				first = r
			}
			insertOK(t, b, r)
			requireCountsConsistent(t, b, nw)
		}
		// supersede
		upd := first
		upd.ContentHash = hashHex(recCounter.Add(1))
		upd.CreatedAt = recT0.Add(time.Second)
		upd.RelayerNodeID = nodeHex(1002)
		require.Equal(t, PeernetRecordSuperseded, insertOK(t, b, upd).Outcome)
		requireCountsConsistent(t, b, nw)
		// relayer share eviction, network cap eviction, own rows
		for i := 0; i < 6; i++ {
			insertOK(t, b, mkRec(fromRelayer(3), byPublisher(20+i)))
			requireCountsConsistent(t, b, nw)
		}
		insertOK(t, b, mkRec(ownRec(99)))
		requireCountsConsistent(t, b, nw)
		// expiry purge
		b.setNow(recT0.Add(48 * time.Hour))
		n, err := b.recs.PurgeExpired(ctx, 100)
		require.NoError(t, err)
		assert.Positive(t, n)
		requireCountsConsistent(t, b, nw)
		b.setNow(recT0)
		// delete network
		require.NoError(t, b.recs.DeleteNetwork(ctx, nw))
		requireCountsConsistent(t, b, nw)
		var left int
		require.NoError(t, b.recs.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM peernet_record_counts WHERE network_id = ?`, nw).Scan(&left))
		assert.Zero(t, left)
	})
}

func TestPeernetRecords_ReconcileRepairsDriftedCounters(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		ctx := context.Background()
		nw := hexID('a')
		for i := 0; i < 3; i++ {
			insertOK(t, b, mkRec(fromRelayer(1)))
		}
		_, err := b.recs.db.ExecContext(ctx, `UPDATE peernet_record_counts SET n = n + 40 WHERE network_id = ? AND scope = 1`, nw)
		require.NoError(t, err)
		diff, err := b.recs.CheckCounts(ctx, nw)
		require.NoError(t, err)
		require.NotEmpty(t, diff)
		fixed, err := b.recs.ReconcileCounts(ctx, nw)
		require.NoError(t, err)
		assert.NotEmpty(t, fixed)
		requireCountsConsistent(t, b, nw)
	})
}

func TestPeernetRecords_RelayerOnlyDisplacesItsOwnRows(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		lim := PeernetRecordLimits{MaxPerPublisher: 100, MaxPerRelayer: 3, MaxPerNetwork: 100}
		nw := hexID('a')
		for i := 0; i < 3; i++ {
			res, err := b.recs.Insert(context.Background(), mkRec(fromRelayer(2), byPublisher(500+i)), lim)
			require.NoError(t, err)
			require.Equal(t, PeernetRecordInserted, res.Outcome)
		}
		// Relayer 1 floods with a fresh publisher per record.
		for i := 0; i < 10; i++ {
			res, err := b.recs.Insert(context.Background(), mkRec(fromRelayer(1), byPublisher(i+1)), lim)
			require.NoError(t, err)
			require.Equal(t, PeernetRecordInserted, res.Outcome)
		}
		assert.Equal(t, 3, b.relayerRows(t, nw, 1), "the flooding relayer is capped at its own share")
		assert.Equal(t, 3, b.relayerRows(t, nw, 2), "another relayer's rows are untouched")
		requireCountsConsistent(t, b, nw)
	})
}

func TestPeernetRecords_NetworkCapTakesFromTheLargestRelayer(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		lim := PeernetRecordLimits{MaxPerPublisher: 100, MaxPerRelayer: 100, MaxPerNetwork: 10}
		nw := hexID('a')
		ins := func(m ...func(*PeernetRecord)) {
			res, err := b.recs.Insert(context.Background(), mkRec(m...), lim)
			require.NoError(t, err)
			require.Equal(t, PeernetRecordInserted, res.Outcome)
		}
		ins(ownRec(99))
		for i := 0; i < 6; i++ {
			ins(fromRelayer(1), byPublisher(i+1)) // the big one
		}
		for i := 0; i < 3; i++ {
			ins(fromRelayer(2), byPublisher(50+i))
		}
		// A newcomer takes from relayer 1 (the largest), never from relayer 2 or own rows.
		ins(fromRelayer(3), byPublisher(80))
		assert.Equal(t, 5, b.relayerRows(t, nw, 1))
		assert.Equal(t, 3, b.relayerRows(t, nw, 2))
		assert.Equal(t, 1, b.relayerRows(t, nw, 3))
		stats, err := b.recs.Stats(context.Background(), nw)
		require.NoError(t, err)
		assert.Equal(t, 1, stats.Count(PeernetRecordKindRelease, PeernetRecordSourceLocal))
		requireCountsConsistent(t, b, nw)
	})
}

func TestPeernetRecords_OwnRecordsExemptFromPublisherAndRelayerQuotas(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		lim := PeernetRecordLimits{MaxPerPublisher: 2, MaxPerRelayer: 2, MaxPerNetwork: 6}
		nw := hexID('a')
		for i := 0; i < 5; i++ { // far over the per-publisher quota of 2
			res, err := b.recs.Insert(context.Background(), mkRec(ownRec(7)), lim)
			require.NoError(t, err)
			require.Equal(t, PeernetRecordInserted, res.Outcome, "own record %d", i)
		}
		// Peer rows fill the rest; an own record then evicts a peer row instead of being refused.
		res, err := b.recs.Insert(context.Background(), mkRec(fromRelayer(1), byPublisher(1)), lim)
		require.NoError(t, err)
		require.Equal(t, PeernetRecordInserted, res.Outcome)
		res, err = b.recs.Insert(context.Background(), mkRec(ownRec(7)), lim)
		require.NoError(t, err)
		require.Equal(t, PeernetRecordInserted, res.Outcome)
		assert.Equal(t, 0, b.relayerRows(t, nw, 1))
		requireCountsConsistent(t, b, nw)
	})
}

func TestPeernetRecords_SupersedeKeepsTheOriginalRelayer(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		nw := hexID('a')
		first := mkRec(fromRelayer(1), byPublisher(7))
		insertOK(t, b, first)
		upd := first
		upd.ContentHash = hashHex(recCounter.Add(1))
		upd.CreatedAt = recT0.Add(time.Second)
		upd.RelayerNodeID = nodeHex(1002)
		require.Equal(t, PeernetRecordSuperseded, insertOK(t, b, upd).Outcome)
		assert.Equal(t, 1, b.relayerRows(t, nw, 1), "the row stays with the relayer that delivered it first")
		assert.Equal(t, 0, b.relayerRows(t, nw, 2))
		requireCountsConsistent(t, b, nw)
	})
}

func TestPeernetRecords_SupersedeOfAnUnattributedRowRespectsTheNewRelayerShare(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		ctx := context.Background()
		nw := hexID('a')
		lim := PeernetRecordLimits{MaxPerRelayer: 2}
		orphan := mkRec(byPublisher(7)) // peer row without a relayer
		_, err := b.recs.Insert(ctx, orphan, lim)
		require.NoError(t, err)
		for i := 0; i < 2; i++ {
			_, err := b.recs.Insert(ctx, mkRec(fromRelayer(2), byPublisher(20+i)), lim)
			require.NoError(t, err)
		}
		upd := orphan
		upd.ContentHash = hashHex(recCounter.Add(1))
		upd.CreatedAt = recT0.Add(time.Second)
		upd.RelayerNodeID = nodeHex(1002)
		res, err := b.recs.Insert(ctx, upd, lim)
		require.NoError(t, err)
		require.Equal(t, PeernetRecordSuperseded, res.Outcome)
		assert.LessOrEqual(t, b.relayerRows(t, nw, 2), 2, "attribution moved to relayer 2, whose share is capped")
		requireCountsConsistent(t, b, nw)
	})
}

func TestPeernetRecords_DeleteNetworkRunsInBatchesAndIsIdempotent(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		ctx := context.Background()
		b.recs.deleteBatch = 3
		nw := hexID('a')
		for i := 0; i < 10; i++ {
			insertOK(t, b, mkRec(byPublisher(i+1)))
		}
		insertOK(t, b, mkRec(func(r *PeernetRecord) { r.NetworkID = hexID('b') }))
		// One step deletes one batch only: the write lock is released between batches.
		done, err := b.recs.deleteNetworkStep(ctx, nw)
		require.NoError(t, err)
		assert.False(t, done)
		requireCountsConsistent(t, b, nw)
		var left int
		require.NoError(t, b.recs.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM peernet_records WHERE network_id = ?`, nw).Scan(&left))
		assert.Equal(t, 7, left)
		// A record arriving mid-delete is removed too, and the end state is clean.
		insertOK(t, b, mkRec())
		require.NoError(t, b.recs.DeleteNetwork(ctx, nw))
		require.NoError(t, b.recs.DeleteNetwork(ctx, nw))
		require.NoError(t, b.recs.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM peernet_records WHERE network_id = ?`, nw).Scan(&left))
		assert.Zero(t, left)
		requireCountsConsistent(t, b, nw)
		var other int
		require.NoError(t, b.recs.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM peernet_records WHERE network_id = ?`, hexID('b')).Scan(&other))
		assert.Equal(t, 1, other, "another network is untouched")
	})
}

func TestPeernetRecords_AttestationCoverageStoredAndCheckedAtClamped(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		ctx := context.Background()
		rec := mkAtt(true, func(r *PeernetRecord) {
			r.SegmentsChecked, r.TotalSegments = 40, 100
			r.CheckedAt = recT0.Add(5 * time.Minute) // future relative to the receive time
		})
		insertOK(t, b, rec)
		got, err := b.recs.AttestationsFor(ctx, rec.NetworkID, rec.ReleaseID, "", time.Time{})
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, 40, got[0].SegmentsChecked)
		assert.Equal(t, 100, got[0].TotalSegments)
		assert.False(t, got[0].CheckedAt.After(recT0), "checked_at must not exceed the receive time, got %v", got[0].CheckedAt)
	})
}

func explainPlan(t *testing.T, b *recBackend, q string, args ...any) string {
	t.Helper()
	rows, err := b.recs.db.QueryContext(context.Background(), `EXPLAIN QUERY PLAN `+q, args...)
	require.NoError(t, err)
	defer rows.Close()
	var plan string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
		plan += detail + "\n"
	}
	return plan
}

// The flood test: a hostile relayer serves valid records from freshly
// generated publishers into a store that is already at its cap.
func TestPeernetRecords_FloodFromOneRelayer(t *testing.T) {
	if testing.Short() {
		t.Skip("flood test is skipped under -short")
	}
	b := newRecBackend(t, "sqlite")
	ctx := context.Background()
	nw := hexID('a')
	lim := PeernetRecordLimits{MaxPerPublisher: 1000, MaxPerRelayer: 1500, MaxPerNetwork: 3000}

	// No insert-path query may scan the table: every one has an index.
	for name, q := range map[string]string{
		"hash":     `SELECT 1 FROM peernet_records WHERE network_id = ? AND content_hash = ?`,
		"relayer":  `SELECT seq, publisher_node_id, source, relayer_node_id FROM peernet_records WHERE network_id = ? AND source = 1 AND relayer_node_id = ? ORDER BY seq LIMIT 1`,
		"expired":  `SELECT seq, publisher_node_id, source, relayer_node_id FROM peernet_records WHERE network_id = ? AND expires_at <= ? ORDER BY expires_at, seq LIMIT 1`,
		"largest":  `SELECT key FROM peernet_record_counts WHERE network_id = ? AND scope = 3 ORDER BY n DESC LIMIT 1`,
		"counters": `SELECT n FROM peernet_record_counts WHERE network_id = ? AND scope = 2 AND key = ?`,
	} {
		var args []any
		switch name {
		case "hash", "relayer", "expired", "counters":
			args = []any{nw, hashHex(1)}
		default:
			args = []any{nw}
		}
		plan := explainPlan(t, b, q, args...)
		assert.NotContains(t, plan, "SCAN peernet_records", "%s query scans the table:\n%s", name, plan)
		assert.NotContains(t, plan, "USE TEMP B-TREE", "%s query sorts:\n%s", name, plan)
	}

	// Own and honest relayed rows first.
	for i := 0; i < 300; i++ {
		res, err := b.recs.Insert(ctx, mkRec(ownRec(9)), lim)
		require.NoError(t, err)
		require.Equal(t, PeernetRecordInserted, res.Outcome)
	}
	for r := 1; r <= 2; r++ {
		for i := 0; i < 600; i++ {
			res, err := b.recs.Insert(ctx, mkRec(fromRelayer(r), byPublisher(r*10000+i%3)), lim)
			require.NoError(t, err)
			require.Equal(t, PeernetRecordInserted, res.Outcome)
		}
	}
	// The store is at its cap now: 300 + 600 + 600 + room.
	const attackRows = 5000
	start := time.Now()
	var slowest time.Duration
	for i := 0; i < attackRows; i++ {
		t0 := time.Now()
		res, err := b.recs.Insert(ctx, mkRec(fromRelayer(66), byPublisher(100000+i)), lim)
		require.NoError(t, err)
		require.Equal(t, PeernetRecordInserted, res.Outcome)
		if d := time.Since(t0); d > slowest {
			slowest = d
		}
	}
	total := time.Since(start)
	t.Logf("flood: %d inserts at the cap, avg %v, slowest %v", attackRows, total/attackRows, slowest)

	assert.Equal(t, 1500, b.relayerRows(t, nw, 66), "the attacker holds at most its own share")
	assert.Equal(t, 600, b.relayerRows(t, nw, 1), "honest relayer 1 survives")
	assert.Equal(t, 600, b.relayerRows(t, nw, 2), "honest relayer 2 survives")
	stats, err := b.recs.Stats(ctx, nw)
	require.NoError(t, err)
	assert.Equal(t, 300, stats.Count(PeernetRecordKindRelease, PeernetRecordSourceLocal), "own rows survive")
	assert.LessOrEqual(t, stats.Total, 3000)
	requireCountsConsistent(t, b, nw)
	assert.Less(t, total/attackRows, 20*time.Millisecond, "inserts must not scan the network")
}

func TestPeernetRecords_PeerInsertsAreSingleWriterAndBackOffWhenBusy(t *testing.T) {
	b := newRecBackend(t, "sqlite")
	ctx := context.Background()
	b.recs.busyWait = 50 * time.Millisecond
	b.recs.peerGate <- struct{}{} // another peer insert holds the writer slot
	_, err := b.recs.Insert(ctx, mkRec(fromRelayer(1)), PeernetRecordLimits{})
	assert.ErrorIs(t, err, ErrPeernetBusy)
	// Own records do not queue behind peer ingest.
	res, err := b.recs.Insert(ctx, mkRec(ownRec(9)), PeernetRecordLimits{})
	require.NoError(t, err)
	assert.Equal(t, PeernetRecordInserted, res.Outcome)
	<-b.recs.peerGate
	res, err = b.recs.Insert(ctx, mkRec(fromRelayer(1)), PeernetRecordLimits{})
	require.NoError(t, err)
	assert.Equal(t, PeernetRecordInserted, res.Outcome)
}

func TestPeernetRecords_DeleteOwnReleaseKeepsPeerRows(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		ctx := context.Background()
		nw := hexID('a')
		rel := relHex(77)
		sameRelease := func(r *PeernetRecord) { r.ReleaseID = rel }
		insertOK(t, b, mkRec(ownRec(9), sameRelease))
		insertOK(t, b, mkRec(ownRec(9), sameRelease, func(r *PeernetRecord) {
			r.Kind = PeernetRecordKindAttestation
			r.FileID = fileHex(1)
			h := true
			r.Healthy = &h
			r.Title = ""
			r.TotalSize = 0
		}))
		insertOK(t, b, mkRec(ownRec(9)))                                   // another release
		insertOK(t, b, mkRec(fromRelayer(1), byPublisher(3), sameRelease)) // a peer's record of the same release
		n, err := b.recs.DeleteOwnRelease(ctx, nw, rel)
		require.NoError(t, err)
		assert.Equal(t, 2, n)
		stats, err := b.recs.Stats(ctx, nw)
		require.NoError(t, err)
		assert.Equal(t, 2, stats.Total)
		requireCountsConsistent(t, b, nw)
	})
}

func TestPeernetRecords_DeleteOwnReleaseRemovesEverythingInBatches(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		ctx := context.Background()
		nw := hexID('a')
		rel := relHex(77)
		b.recs.ownReleaseBatch = 3
		insertOK(t, b, mkRec(ownRec(9), func(r *PeernetRecord) { r.ReleaseID = rel }))
		for i := 1; i <= 9; i++ {
			insertOK(t, b, mkRec(ownRec(9), func(r *PeernetRecord) {
				r.ReleaseID = rel
				r.Kind = PeernetRecordKindAttestation
				r.FileID = fileHex(i)
				h := true
				r.Healthy = &h
				r.Title = ""
				r.TotalSize = 0
			}))
		}
		n, err := b.recs.DeleteOwnRelease(ctx, nw, rel)
		require.NoError(t, err)
		assert.Equal(t, 10, n, "a release with more rows than one batch must still be removed completely")
		stats, err := b.recs.Stats(ctx, nw)
		require.NoError(t, err)
		assert.Equal(t, 0, stats.Total)
		requireCountsConsistent(t, b, nw)
	})
}
