package database

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var recT0 = time.Unix(1_700_000_000, 0)

// recBackend is one dialect's record and cursor repositories over one
// database, with a controllable clock.
type recBackend struct {
	recs  *PeernetRecordRepository
	cur   *PeernetSyncCursorRepository
	peers *PeernetPeerRepository
	now   *atomic.Int64
}

func (b *recBackend) setNow(t time.Time) { b.now.Store(t.Unix()) }

func newRecBackend(t *testing.T, name string) *recBackend {
	t.Helper()
	b := &recBackend{now: &atomic.Int64{}}
	b.now.Store(recT0.Unix())
	clock := func() time.Time { return time.Unix(b.now.Load(), 0) }
	if name == "postgres" {
		db := openPostgresLatest(t)
		b.recs = NewPeernetRecordRepositoryWithClock(db, DialectPostgres, clock)
		b.cur = NewPeernetSyncCursorRepositoryWithClock(db, DialectPostgres, clock)
		b.peers = NewPeernetPeerRepository(db, DialectPostgres)
		return b
	}
	db, err := NewDB(Config{DatabasePath: filepath.Join(t.TempDir(), "db")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	b.recs = NewPeernetRecordRepositoryWithClock(db.Connection(), DialectSQLite, clock)
	b.cur = NewPeernetSyncCursorRepositoryWithClock(db.Connection(), DialectSQLite, clock)
	b.peers = NewPeernetPeerRepository(db.Connection(), DialectSQLite)
	return b
}

// forEachRecBackend runs fn against SQLite and, when ALTMOUNT_TEST_PG_DSN is
// set, PostgreSQL. Each subtest gets a fresh, empty schema.
func forEachRecBackend(t *testing.T, fn func(t *testing.T, b *recBackend)) {
	t.Helper()
	names := []string{"sqlite"}
	if hasPostgresDSN() {
		names = append(names, "postgres")
	}
	for _, n := range names {
		t.Run(n, func(t *testing.T) { fn(t, newRecBackend(t, n)) })
	}
}

var recCounter atomic.Int64

func hashHex(i int64) string { return fmt.Sprintf("%064x", i+1_000_000) }
func relHex(i int) string    { return fmt.Sprintf("%064x", i+2_000_000) }
func fileHex(i int) string   { return fmt.Sprintf("%064x", i+3_000_000) }

// mkRec builds a valid release record (kind 1) from publisher nodeHex(1) in
// network hexID('a'), created at recT0 and valid for a day. Every call gets a
// fresh content hash and release ID unless a mod overrides them.
func mkRec(mods ...func(*PeernetRecord)) PeernetRecord {
	n := recCounter.Add(1)
	r := PeernetRecord{
		NetworkID: hexID('a'), Kind: PeernetRecordKindRelease, ContentHash: hashHex(n), ReleaseID: relHex(int(n)),
		PublisherNodeID: nodeHex(1), CreatedAt: recT0, ExpiresAt: recT0.Add(24 * time.Hour),
		Source: PeernetRecordSourcePeer, Title: "Some Title", TotalSize: 1234, Payload: []byte("payload"),
	}
	for _, m := range mods {
		m(&r)
	}
	return r
}

func mkAtt(healthy bool, mods ...func(*PeernetRecord)) PeernetRecord {
	h := healthy
	return mkRec(append([]func(*PeernetRecord){func(r *PeernetRecord) {
		r.Kind = PeernetRecordKindAttestation
		r.FileID = fileHex(1)
		r.Healthy = &h
		r.Backbones = []string{"omicron", "abavia"}
		r.CheckedAt = recT0.Add(-time.Hour)
		r.Title, r.TotalSize = "", 0
	}}, mods...)...)
}

func insertOK(t *testing.T, b *recBackend, r PeernetRecord) PeernetInsertResult {
	t.Helper()
	res, err := b.recs.Insert(context.Background(), r, PeernetRecordLimits{})
	require.NoError(t, err)
	return res
}

func TestPeernetRecords_InsertAssignsIncreasingSeqPerNetwork(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		r1 := insertOK(t, b, mkRec())
		r2 := insertOK(t, b, mkRec())
		other := insertOK(t, b, mkRec(func(r *PeernetRecord) { r.NetworkID = hexID('b') }))
		assert.Equal(t, PeernetRecordInserted, r1.Outcome)
		assert.Equal(t, int64(1), r1.Seq)
		assert.Equal(t, int64(2), r2.Seq)
		assert.Equal(t, int64(1), other.Seq, "each network has its own sequence")

		page, err := b.recs.ListSince(context.Background(), hexID('a'), 0, 100)
		require.NoError(t, err)
		require.Len(t, page.Rows, 2)
		assert.Equal(t, []byte("payload"), page.Rows[0].Payload)
		assert.Equal(t, int64(2), page.HighestSeq)
		assert.False(t, page.More)
	})
}

func TestPeernetRecords_SupersedeKeepsOnlyNewestAndBumpsSeq(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		ctx := context.Background()
		old := mkRec()
		first := insertOK(t, b, old)
		other := insertOK(t, b, mkRec())

		newer := old
		newer.ContentHash = hashHex(recCounter.Add(1))
		newer.CreatedAt = recT0.Add(time.Minute)
		newer.Payload = []byte("newer")
		res := insertOK(t, b, newer)
		assert.Equal(t, PeernetRecordSuperseded, res.Outcome)
		assert.Greater(t, res.Seq, other.Seq)
		assert.Greater(t, res.Seq, first.Seq)

		page, err := b.recs.ListSince(ctx, hexID('a'), 0, 100)
		require.NoError(t, err)
		require.Len(t, page.Rows, 2)
		assert.Equal(t, other.Seq, page.Rows[0].Seq)
		assert.Equal(t, []byte("newer"), page.Rows[1].Payload)
		assert.Equal(t, res.Seq, page.Rows[1].Seq)

		// Older, identical (same hash) and same-time-different-hash are no-ops.
		older := old
		older.ContentHash = hashHex(recCounter.Add(1))
		older.CreatedAt = recT0.Add(-time.Minute)
		sameTime := newer
		sameTime.ContentHash = hashHex(recCounter.Add(1))
		for name, dup := range map[string]PeernetRecord{"older": older, "identical": newer, "same time": sameTime} {
			got := insertOK(t, b, dup)
			assert.Equal(t, PeernetRecordDuplicate, got.Outcome, name)
		}
		page, err = b.recs.ListSince(ctx, hexID('a'), 0, 100)
		require.NoError(t, err)
		assert.Len(t, page.Rows, 2, "duplicates must not add rows")
		assert.Equal(t, res.Seq, page.HighestSeq, "duplicates must not consume a seq")
	})
}

func TestPeernetRecords_SupersedeKeyIncludesPublisherKindReleaseFile(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		base := mkRec()
		insertOK(t, b, base)
		variants := []PeernetRecord{
			mkRec(func(r *PeernetRecord) {
				r.ReleaseID = base.ReleaseID
				r.PublisherNodeID = nodeHex(2)
				r.CreatedAt = recT0.Add(time.Hour)
			}),
			mkAtt(true, func(r *PeernetRecord) { r.ReleaseID = base.ReleaseID; r.CreatedAt = recT0.Add(time.Hour) }),
			mkAtt(true, func(r *PeernetRecord) {
				r.ReleaseID = base.ReleaseID
				r.FileID = fileHex(2)
				r.CreatedAt = recT0.Add(time.Hour)
			}),
		}
		for i, v := range variants {
			assert.Equal(t, PeernetRecordInserted, insertOK(t, b, v).Outcome, "variant %d is a different key", i)
		}
		page, err := b.recs.ListSince(context.Background(), hexID('a'), 0, 100)
		require.NoError(t, err)
		assert.Len(t, page.Rows, 4)
	})
}

func TestPeernetRecords_DuplicateContentHashWithDifferentKeyIsDuplicate(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		a := mkRec()
		insertOK(t, b, a)
		clash := mkRec(func(r *PeernetRecord) { r.ContentHash = a.ContentHash })
		assert.Equal(t, PeernetRecordDuplicate, insertOK(t, b, clash).Outcome)
	})
}

func TestPeernetRecords_ValidationAndPayloadCap(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		ctx := context.Background()
		ok := mkRec(func(r *PeernetRecord) { r.Payload = make([]byte, PeernetRecordMaxPayload) })
		assert.Equal(t, PeernetRecordInserted, insertOK(t, b, ok).Outcome, "exactly at the cap is accepted")

		bad := map[string]PeernetRecord{
			"payload too big":    mkRec(func(r *PeernetRecord) { r.Payload = make([]byte, PeernetRecordMaxPayload+1) }),
			"empty payload":      mkRec(func(r *PeernetRecord) { r.Payload = nil }),
			"bad network":        mkRec(func(r *PeernetRecord) { r.NetworkID = "x" }),
			"upper-case hash":    mkRec(func(r *PeernetRecord) { r.ContentHash = hexID('A') }),
			"bad release":        mkRec(func(r *PeernetRecord) { r.ReleaseID = "short" }),
			"bad publisher":      mkRec(func(r *PeernetRecord) { r.PublisherNodeID = "" }),
			"bad kind":           mkRec(func(r *PeernetRecord) { r.Kind = 9 }),
			"bad source":         mkRec(func(r *PeernetRecord) { r.Source = 7 }),
			"file id on release": mkRec(func(r *PeernetRecord) { r.FileID = fileHex(1) }),
			"expiry before create": mkRec(func(r *PeernetRecord) {
				r.ExpiresAt = r.CreatedAt
			}),
			"already expired": mkRec(func(r *PeernetRecord) {
				r.CreatedAt = recT0.Add(-48 * time.Hour)
				r.ExpiresAt = recT0.Add(-24 * time.Hour)
			}),
			"healthy on release":  mkRec(func(r *PeernetRecord) { t := true; r.Healthy = &t }),
			"attestation no file": mkAtt(true, func(r *PeernetRecord) { r.FileID = "" }),
			"attestation no verdict": mkAtt(true, func(r *PeernetRecord) {
				r.Healthy = nil
			}),
			"backbone with comma": mkAtt(true, func(r *PeernetRecord) { r.Backbones = []string{"a,b"} }),
			"negative season":     mkRec(func(r *PeernetRecord) { s := -1; r.Season = &s }),
			"title too long":      mkRec(func(r *PeernetRecord) { r.Title = string(make([]byte, PeernetRecordMaxTitle+1)) }),
		}
		for name, r := range bad {
			_, err := b.recs.Insert(ctx, r, PeernetRecordLimits{})
			assert.ErrorIs(t, err, ErrPeernetRecordInvalid, name)
		}
		page, err := b.recs.ListSince(ctx, hexID('a'), 0, 100)
		require.NoError(t, err)
		assert.Len(t, page.Rows, 1)
	})
}

func TestPeernetRecords_PublisherQuota(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		ctx := context.Background()
		lim := PeernetRecordLimits{MaxPerPublisher: 3, MaxPerNetwork: 100}
		var first PeernetRecord
		for i := 0; i < 3; i++ {
			r := mkRec()
			if i == 0 {
				first = r
			}
			res, err := b.recs.Insert(ctx, r, lim)
			require.NoError(t, err)
			require.Equal(t, PeernetRecordInserted, res.Outcome)
		}
		res, err := b.recs.Insert(ctx, mkRec(), lim)
		require.NoError(t, err)
		assert.Equal(t, PeernetRecordRejectedQuota, res.Outcome)
		assert.Equal(t, PeernetRecordQuotaPublisher, res.QuotaScope)

		// Other publisher and other network are unaffected.
		res, _ = b.recs.Insert(ctx, mkRec(func(r *PeernetRecord) { r.PublisherNodeID = nodeHex(2) }), lim)
		assert.Equal(t, PeernetRecordInserted, res.Outcome)
		res, _ = b.recs.Insert(ctx, mkRec(func(r *PeernetRecord) { r.NetworkID = hexID('b') }), lim)
		assert.Equal(t, PeernetRecordInserted, res.Outcome)

		// Replacing one of its own records does not grow the count.
		upd := first
		upd.ContentHash = hashHex(recCounter.Add(1))
		upd.CreatedAt = recT0.Add(time.Second)
		res, err = b.recs.Insert(ctx, upd, lim)
		require.NoError(t, err)
		assert.Equal(t, PeernetRecordSuperseded, res.Outcome)

		// Expired rows do not count against the quota.
		b.setNow(recT0.Add(25 * time.Hour))
		res, err = b.recs.Insert(ctx, mkRec(func(r *PeernetRecord) {
			r.CreatedAt = b.nowTime()
			r.ExpiresAt = r.CreatedAt.Add(time.Hour)
		}), lim)
		require.NoError(t, err)
		assert.Equal(t, PeernetRecordInserted, res.Outcome)
	})
}

func (b *recBackend) nowTime() time.Time { return time.Unix(b.now.Load(), 0) }

func TestPeernetRecords_DefaultLimits(t *testing.T) {
	assert.Equal(t, 5000, DefaultPeernetMaxPerPublisher)
	assert.Equal(t, 50000, DefaultPeernetMaxPerNetwork)
	assert.Equal(t, 20000, DefaultPeernetMaxPerRelayer)
	l := PeernetRecordLimits{}.withDefaults()
	assert.Equal(t, DefaultPeernetMaxPerPublisher, l.MaxPerPublisher)
	assert.Equal(t, DefaultPeernetMaxPerNetwork, l.MaxPerNetwork)
}

func TestPeernetRecords_GlobalCapEvictsOldestPeerRecordsAndProtectsOwn(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		ctx := context.Background()
		lim := PeernetRecordLimits{MaxPerPublisher: 100, MaxPerNetwork: 4}
		pub := func(i int) func(*PeernetRecord) {
			return func(r *PeernetRecord) { r.PublisherNodeID = nodeHex(i) }
		}
		own := func(r *PeernetRecord) { r.Source = PeernetRecordSourceLocal; r.PublisherNodeID = nodeHex(9) }
		// seq1 own, seq2 peer, seq3 own, seq4 peer.
		var seqs []int64
		for _, m := range []func(*PeernetRecord){own, pub(2), own, pub(3)} {
			res, err := b.recs.Insert(ctx, mkRec(m), lim)
			require.NoError(t, err)
			require.Equal(t, PeernetRecordInserted, res.Outcome)
			seqs = append(seqs, res.Seq)
		}

		// A new peer record evicts the lowest-seq peer record (seq2), not own seq1.
		res, err := b.recs.Insert(ctx, mkRec(pub(4)), lim)
		require.NoError(t, err)
		require.Equal(t, PeernetRecordInserted, res.Outcome)
		assertSeqs(t, b, hexID('a'), seqs[0], seqs[2], seqs[3], res.Seq)

		// An own record also evicts a peer record (seq4) rather than being refused.
		res2, err := b.recs.Insert(ctx, mkRec(own), lim)
		require.NoError(t, err)
		require.Equal(t, PeernetRecordInserted, res2.Outcome)
		assertSeqs(t, b, hexID('a'), seqs[0], seqs[2], res.Seq, res2.Seq)

		// Fill with own only: a peer record can no longer make room.
		res4, err := b.recs.Insert(ctx, mkRec(own), lim) // evicts the last peer record
		require.NoError(t, err)
		require.Equal(t, PeernetRecordInserted, res4.Outcome)
		// Own records are exempt from the cap when no peer row is left to evict:
		// they are bounded by the library itself.
		res5, err := b.recs.Insert(ctx, mkRec(own), lim)
		require.NoError(t, err)
		require.Equal(t, PeernetRecordInserted, res5.Outcome)
		assertSeqs(t, b, hexID('a'), seqs[0], seqs[2], res2.Seq, res4.Seq, res5.Seq)
		res3, err := b.recs.Insert(ctx, mkRec(pub(5)), lim)
		require.NoError(t, err)
		assert.Equal(t, PeernetRecordRejectedQuota, res3.Outcome)
		assert.Equal(t, PeernetRecordQuotaStorage, res3.QuotaScope)
		stats, err := b.recs.Stats(ctx, hexID('a'))
		require.NoError(t, err)
		assert.Equal(t, 5, stats.Total)
		assert.Equal(t, 5, stats.Count(PeernetRecordKindRelease, PeernetRecordSourceLocal))
		assert.Equal(t, 0, stats.Count(PeernetRecordKindRelease, PeernetRecordSourcePeer))
	})
}

func TestPeernetRecords_GlobalCapEvictsExpiredFirstAndIsPerNetwork(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		ctx := context.Background()
		lim := PeernetRecordLimits{MaxPerPublisher: 100, MaxPerNetwork: 2}
		short := func(r *PeernetRecord) { r.ExpiresAt = recT0.Add(time.Hour); r.Source = PeernetRecordSourceLocal }
		a, _ := b.recs.Insert(ctx, mkRec(short), lim)
		_, _ = b.recs.Insert(ctx, mkRec(), lim)
		b.setNow(recT0.Add(2 * time.Hour))
		// The expired own record makes room even though own records are protected.
		res, err := b.recs.Insert(ctx, mkRec(func(r *PeernetRecord) {
			r.CreatedAt = b.nowTime()
			r.ExpiresAt = r.CreatedAt.Add(time.Hour)
		}), lim)
		require.NoError(t, err)
		assert.Equal(t, PeernetRecordInserted, res.Outcome)
		page, _ := b.recs.ListSince(ctx, hexID('a'), a.Seq-1, 10)
		assert.Len(t, page.Rows, 2)
		// Another network has its own budget.
		res, err = b.recs.Insert(ctx, mkRec(func(r *PeernetRecord) { r.NetworkID = hexID('b') }), lim)
		require.NoError(t, err)
		assert.Equal(t, PeernetRecordInserted, res.Outcome)
	})
}

func assertSeqs(t *testing.T, b *recBackend, network string, want ...int64) {
	t.Helper()
	page, err := b.recs.ListSince(context.Background(), network, 0, 500)
	require.NoError(t, err)
	var got []int64
	for _, r := range page.Rows {
		got = append(got, r.Seq)
	}
	assert.Equal(t, want, got)
}

func TestPeernetRecords_PurgeExpiredInBoundedBatches(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		ctx := context.Background()
		for i := 0; i < 5; i++ {
			insertOK(t, b, mkRec(func(r *PeernetRecord) { r.ExpiresAt = recT0.Add(time.Hour) }))
		}
		keep := insertOK(t, b, mkRec())
		n, err := b.recs.PurgeExpired(ctx, 10)
		require.NoError(t, err)
		assert.Zero(t, n, "nothing expired yet")

		b.setNow(recT0.Add(2 * time.Hour))
		n, err = b.recs.PurgeExpired(ctx, 2)
		require.NoError(t, err)
		assert.Equal(t, 2, n, "a batch deletes at most the limit")
		n, err = b.recs.PurgeExpired(ctx, 2)
		require.NoError(t, err)
		assert.Equal(t, 2, n)
		n, err = b.recs.PurgeExpired(ctx, 2)
		require.NoError(t, err)
		assert.Equal(t, 1, n)
		assertSeqs(t, b, hexID('a'), keep.Seq)
		_, err = b.recs.PurgeExpired(ctx, 0)
		assert.Error(t, err)

		// A seq is never reused, even after the highest row was purged.
		b.setNow(recT0)
		gone := insertOK(t, b, mkRec(func(r *PeernetRecord) { r.ExpiresAt = recT0.Add(time.Hour) }))
		b.setNow(recT0.Add(3 * time.Hour))
		_, err = b.recs.PurgeExpired(ctx, 10)
		require.NoError(t, err)
		b.setNow(recT0)
		next := insertOK(t, b, mkRec())
		assert.Greater(t, next.Seq, gone.Seq)
	})
}

func TestPeernetRecords_ListSincePagingCapAndExpiry(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		ctx := context.Background()
		var seqs []int64
		for i := 0; i < PeernetListMaxLimit+20; i++ {
			exp := recT0.Add(24 * time.Hour)
			if i == 1 {
				exp = recT0.Add(time.Hour) // expires, still occupies a seq
			}
			seqs = append(seqs, insertOK(t, b, mkRec(func(r *PeernetRecord) { r.ExpiresAt = exp })).Seq)
		}
		b.setNow(recT0.Add(2 * time.Hour))

		page, err := b.recs.ListSince(ctx, hexID('a'), 0, 100000)
		require.NoError(t, err)
		assert.Equal(t, PeernetListMaxLimit-1, len(page.Rows), "cap applies to scanned rows, expired are skipped")
		assert.Equal(t, seqs[PeernetListMaxLimit-1], page.HighestSeq)
		assert.True(t, page.More)

		page2, err := b.recs.ListSince(ctx, hexID('a'), page.HighestSeq, 100000)
		require.NoError(t, err)
		assert.Len(t, page2.Rows, 20)
		assert.False(t, page2.More)
		assert.Equal(t, seqs[len(seqs)-1], page2.HighestSeq)

		empty, err := b.recs.ListSince(ctx, hexID('a'), page2.HighestSeq, 10)
		require.NoError(t, err)
		assert.Empty(t, empty.Rows)
		assert.Equal(t, page2.HighestSeq, empty.HighestSeq, "cursor unchanged when nothing new")

		_, err = b.recs.ListSince(ctx, hexID('a'), 0, 0)
		assert.Error(t, err)
		_, err = b.recs.ListSince(ctx, "bad", 0, 10)
		assert.Error(t, err)
	})
}

// TestPeernetRecords_PagingNeverSkipsUnderConcurrentInserts has several
// writers racing a reader that pages by seq. Every seq a writer was told it
// got must show up in the reader's view: a row that commits later always has a
// higher seq than every row already visible, so a cursor can never pass it.
func TestPeernetRecords_PagingNeverSkipsUnderConcurrentInserts(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		ctx := context.Background()
		const writers, perWriter = 6, 25
		var wg sync.WaitGroup
		var mu sync.Mutex
		written := map[int64]bool{}
		var werrs []error
		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for i := 0; i < perWriter; i++ {
					res, err := b.recs.Insert(ctx, mkRec(func(r *PeernetRecord) {
						r.PublisherNodeID = nodeHex(100 + w)
					}), PeernetRecordLimits{})
					mu.Lock()
					if err != nil || res.Outcome != PeernetRecordInserted {
						werrs = append(werrs, fmt.Errorf("outcome %v err %w", res.Outcome, err))
					} else {
						written[res.Seq] = true
					}
					mu.Unlock()
				}
			}(w)
		}
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()

		seen := map[int64]bool{}
		var cursor int64
		drain := func() {
			for {
				page, err := b.recs.ListSince(ctx, hexID('a'), cursor, 7)
				require.NoError(t, err)
				for _, r := range page.Rows {
					seen[r.Seq] = true
				}
				cursor = page.HighestSeq
				if !page.More {
					return
				}
			}
		}
		for finished := false; !finished; {
			select {
			case <-done:
				finished = true
			default:
			}
			drain()
		}
		drain()
		require.Empty(t, werrs)
		assert.Len(t, written, writers*perWriter, "every insert got a distinct seq")
		for s := range written {
			assert.True(t, seen[s], "seq %d was skipped by the paging reader", s)
		}
	})
}

func TestPeernetRecords_ListOwnSince(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		ctx := context.Background()
		insertOK(t, b, mkRec())
		o1 := insertOK(t, b, mkRec(func(r *PeernetRecord) { r.Source = PeernetRecordSourceLocal }))
		insertOK(t, b, mkRec())
		o2 := insertOK(t, b, mkRec(func(r *PeernetRecord) { r.Source = PeernetRecordSourceLocal }))
		page, err := b.recs.ListOwnSince(ctx, hexID('a'), 0, 10)
		require.NoError(t, err)
		require.Len(t, page.Rows, 2)
		assert.Equal(t, o1.Seq, page.Rows[0].Seq)
		assert.Equal(t, o2.Seq, page.HighestSeq)
		page, err = b.recs.ListOwnSince(ctx, hexID('a'), o1.Seq, 1)
		require.NoError(t, err)
		require.Len(t, page.Rows, 1)
		assert.Equal(t, o2.Seq, page.Rows[0].Seq)
	})
}
