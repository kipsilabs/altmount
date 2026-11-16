package database

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPeernetRecords_AttestationsFor(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		ctx := context.Background()
		rel := relHex(7)
		insertOK(t, b, mkAtt(true, func(r *PeernetRecord) { r.ReleaseID = rel; r.PublisherNodeID = nodeHex(1) }))
		insertOK(t, b, mkAtt(false, func(r *PeernetRecord) {
			r.ReleaseID = rel
			r.PublisherNodeID = nodeHex(2)
			r.FileID = fileHex(2)
			r.Backbones = []string{"zeta"}
		}))
		insertOK(t, b, mkAtt(true, func(r *PeernetRecord) {
			r.ReleaseID = rel
			r.PublisherNodeID = nodeHex(3)
			r.CheckedAt = recT0.Add(-10 * 24 * time.Hour)
			r.CreatedAt = recT0.Add(-10 * 24 * time.Hour)
			r.ExpiresAt = recT0.Add(time.Hour)
		}))
		insertOK(t, b, mkAtt(true, func(r *PeernetRecord) { r.ReleaseID = relHex(8) }))
		insertOK(t, b, mkRec(func(r *PeernetRecord) { r.ReleaseID = rel; r.PublisherNodeID = nodeHex(4) }))

		all, err := b.recs.AttestationsFor(ctx, hexID('a'), rel, "", recT0.Add(-7*24*time.Hour))
		require.NoError(t, err)
		require.Len(t, all, 2, "release records and stale attestations are excluded")
		byPub := map[string]PeernetAttestation{}
		for _, a := range all {
			byPub[a.PublisherNodeID] = a
		}
		assert.True(t, byPub[nodeHex(1)].Healthy)
		assert.Equal(t, []string{"abavia", "omicron"}, byPub[nodeHex(1)].Backbones, "canonical sorted order")
		assert.False(t, byPub[nodeHex(2)].Healthy)
		assert.Equal(t, fileHex(2), byPub[nodeHex(2)].FileID)
		assert.Equal(t, recT0.Add(-time.Hour).Unix(), byPub[nodeHex(1)].CheckedAt.Unix())
		assert.Equal(t, recT0.Unix(), byPub[nodeHex(1)].CreatedAt.Unix())

		one, err := b.recs.AttestationsFor(ctx, hexID('a'), rel, fileHex(2), time.Time{})
		require.NoError(t, err)
		assert.Len(t, one, 1)

		// Expired attestations are not returned.
		b.setNow(recT0.Add(30 * time.Hour))
		none, err := b.recs.AttestationsFor(ctx, hexID('a'), rel, "", time.Time{})
		require.NoError(t, err)
		assert.Empty(t, none)
		_, err = b.recs.AttestationsFor(ctx, hexID('a'), "bad", "", time.Time{})
		assert.Error(t, err)
	})
}

func TestPeernetRecords_HealthSummaryForReleases(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		ctx := context.Background()
		r1, r2, r3 := relHex(1), relHex(2), relHex(3)
		att := func(rel string, pub int, healthy bool, file int, checked time.Time) {
			insertOK(t, b, mkAtt(healthy, func(r *PeernetRecord) {
				r.ReleaseID, r.PublisherNodeID, r.FileID, r.CheckedAt = rel, nodeHex(pub), fileHex(file), checked
			}))
		}
		// r1: publishers 1 and 2 healthy (1 on two files), publisher 3 unhealthy.
		att(r1, 1, true, 1, recT0.Add(-3*time.Hour))
		att(r1, 1, true, 2, recT0.Add(-2*time.Hour))
		att(r1, 2, true, 1, recT0.Add(-time.Hour))
		att(r1, 3, false, 1, recT0.Add(-30*time.Minute))
		// r2: only a stale attestation.
		att(r2, 1, true, 1, recT0.Add(-9*24*time.Hour))

		got, err := b.recs.HealthSummaryForReleases(ctx, hexID('a'), []string{r1, r2, r3, r1}, recT0.Add(-7*24*time.Hour))
		require.NoError(t, err)
		require.Len(t, got, 1, "releases without fresh attestations are absent")
		s := got[r1]
		assert.Equal(t, 2, s.HealthyPublishers, "distinct publishers, not rows")
		assert.Equal(t, 1, s.UnhealthyPublishers)
		assert.Equal(t, recT0.Add(-time.Hour).Unix(), s.NewestHealthy.Unix())
		assert.Equal(t, recT0.Add(-30*time.Minute).Unix(), s.NewestUnhealthy.Unix())

		empty, err := b.recs.HealthSummaryForReleases(ctx, hexID('a'), nil, time.Time{})
		require.NoError(t, err)
		assert.Empty(t, empty)

		ids := make([]string, PeernetMaxReleaseIDsPerQuery+1)
		for i := range ids {
			ids[i] = relHex(i)
		}
		_, err = b.recs.HealthSummaryForReleases(ctx, hexID('a'), ids, time.Time{})
		assert.ErrorIs(t, err, ErrPeernetTooManyIDs)
		_, err = b.recs.HealthSummaryForReleases(ctx, hexID('a'), ids[:PeernetMaxReleaseIDsPerQuery], time.Time{})
		assert.NoError(t, err, "exactly at the bound works")
		_, err = b.recs.HealthSummaryForReleases(ctx, hexID('a'), []string{"nothex"}, time.Time{})
		assert.Error(t, err)
	})
}

func TestPeernetRecords_CandidatesByMedia(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		ctx := context.Background()
		ep := func(s, e int) func(*PeernetRecord) {
			return func(r *PeernetRecord) {
				r.IMDbID = "tt0000001"
				r.Season, r.Episode = &s, &e
			}
		}
		movie := func(r *PeernetRecord) { r.IMDbID = "tt0000002" }
		at := func(d time.Duration) func(*PeernetRecord) {
			return func(r *PeernetRecord) { r.CreatedAt = recT0.Add(d); r.ExpiresAt = recT0.Add(d + 48*time.Hour) }
		}
		insertOK(t, b, mkRec(ep(1, 2), at(-3*time.Hour), func(r *PeernetRecord) { r.Title = "old" }))
		insertOK(t, b, mkRec(ep(1, 2), at(-2*time.Hour), func(r *PeernetRecord) { r.Title = "mid" }))
		insertOK(t, b, mkRec(ep(1, 3)))
		insertOK(t, b, mkRec(movie))
		insertOK(t, b, mkRec(ep(1, 2), func(r *PeernetRecord) { r.NetworkID = hexID('b'); r.Title = "othernet" }))
		insertOK(t, b, mkRec(ep(1, 2), at(-time.Hour), func(r *PeernetRecord) { r.Title = "expiring"; r.ExpiresAt = recT0.Add(time.Minute) }))

		s, e := 1, 2
		got, err := b.recs.CandidatesByMedia(ctx, []string{hexID('a'), hexID('b')}, "tt0000001", &s, &e, 10)
		require.NoError(t, err)
		require.Len(t, got, 4)
		assert.Equal(t, "othernet", got[0].Title, "newest first across networks")
		assert.Equal(t, hexID('b'), got[0].NetworkID)
		assert.Equal(t, int64(1234), got[0].TotalSize)

		onlyA, err := b.recs.CandidatesByMedia(ctx, []string{hexID('a')}, "tt0000001", &s, &e, 2)
		require.NoError(t, err)
		require.Len(t, onlyA, 2, "limit applies")
		assert.Equal(t, "expiring", onlyA[0].Title)
		assert.Equal(t, "mid", onlyA[1].Title)

		movies, err := b.recs.CandidatesByMedia(ctx, []string{hexID('a')}, "tt0000002", nil, nil, 10)
		require.NoError(t, err)
		assert.Len(t, movies, 1)
		none, err := b.recs.CandidatesByMedia(ctx, []string{hexID('a')}, "tt0000002", &s, &e, 10)
		require.NoError(t, err)
		assert.Empty(t, none, "season/episode must match exactly")

		b.setNow(recT0.Add(time.Hour))
		after, err := b.recs.CandidatesByMedia(ctx, []string{hexID('a')}, "tt0000001", &s, &e, 10)
		require.NoError(t, err)
		assert.Len(t, after, 2, "expired record dropped")

		_, err = b.recs.CandidatesByMedia(ctx, []string{hexID('a')}, "", nil, nil, 10)
		assert.Error(t, err)
		_, err = b.recs.CandidatesByMedia(ctx, nil, "tt1", nil, nil, 10)
		assert.Error(t, err)
		many := make([]string, PeernetMaxNetworksPerQuery+1)
		for i := range many {
			many[i] = hexID('a')
		}
		_, err = b.recs.CandidatesByMedia(ctx, many, "tt1", nil, nil, 10)
		assert.ErrorIs(t, err, ErrPeernetTooManyIDs)
		big, err := b.recs.CandidatesByMedia(ctx, []string{hexID('a')}, "tt0000001", &s, &e, 100000)
		require.NoError(t, err)
		assert.LessOrEqual(t, len(big), PeernetCandidatesMaxLimit)
	})
}

func TestPeernetRecords_Stats(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		ctx := context.Background()
		insertOK(t, b, mkRec())
		insertOK(t, b, mkRec(func(r *PeernetRecord) { r.PublisherNodeID = nodeHex(2); r.Source = PeernetRecordSourceLocal }))
		insertOK(t, b, mkAtt(true, func(r *PeernetRecord) { r.PublisherNodeID = nodeHex(2) }))
		insertOK(t, b, mkAtt(false, func(r *PeernetRecord) { r.PublisherNodeID = nodeHex(3) }))
		insertOK(t, b, mkRec(func(r *PeernetRecord) { r.NetworkID = hexID('b') }))
		insertOK(t, b, mkRec(func(r *PeernetRecord) { r.ExpiresAt = recT0.Add(time.Minute) }))
		b.setNow(recT0.Add(time.Hour))

		s, err := b.recs.Stats(ctx, hexID('a'))
		require.NoError(t, err)
		assert.Equal(t, 4, s.Total, "expired rows are not counted")
		assert.Equal(t, 1, s.Count(PeernetRecordKindRelease, PeernetRecordSourcePeer))
		assert.Equal(t, 1, s.Count(PeernetRecordKindRelease, PeernetRecordSourceLocal))
		assert.Equal(t, 2, s.Count(PeernetRecordKindAttestation, PeernetRecordSourcePeer))
		assert.Equal(t, 0, s.Count(PeernetRecordKindAttestation, PeernetRecordSourceLocal))
		assert.Equal(t, 3, s.Publishers)

		empty, err := b.recs.Stats(ctx, hexID('c'))
		require.NoError(t, err)
		assert.Zero(t, empty.Total)
		assert.Zero(t, empty.Publishers)
	})
}

func TestPeernetRecords_DeleteNetworkRemovesRecordsCursorsAndOnlyThatNetwork(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		ctx := context.Background()
		insertOK(t, b, mkRec())
		insertOK(t, b, mkRec())
		keep := insertOK(t, b, mkRec(func(r *PeernetRecord) { r.NetworkID = hexID('b') }))
		require.NoError(t, b.cur.SetPullCursor(ctx, hexID('a'), nodeHex(1), 5))
		require.NoError(t, b.cur.SetPullCursor(ctx, hexID('b'), nodeHex(1), 6))

		require.NoError(t, b.recs.DeleteNetwork(ctx, hexID('a')))
		page, err := b.recs.ListSince(ctx, hexID('a'), 0, 10)
		require.NoError(t, err)
		assert.Empty(t, page.Rows)
		c, err := b.cur.Get(ctx, hexID('a'), nodeHex(1))
		require.NoError(t, err)
		assert.Zero(t, c.PulledSeq)
		kept, err := b.cur.Get(ctx, hexID('b'), nodeHex(1))
		require.NoError(t, err)
		assert.Equal(t, int64(6), kept.PulledSeq)
		assertSeqs(t, b, hexID('b'), keep.Seq)

		// Idempotent, and a rejoined network starts a fresh sequence.
		require.NoError(t, b.recs.DeleteNetwork(ctx, hexID('a')))
		assert.Equal(t, int64(1), insertOK(t, b, mkRec()).Seq)
		assert.Error(t, b.recs.DeleteNetwork(ctx, "bad"))
	})
}

func TestPeernetRecords_BackbonesAreCanonicalised(t *testing.T) {
	assert.Equal(t, "a,b,c", canonicalBackbones([]string{" C", "a", "b", "A"}))
	assert.Equal(t, "", canonicalBackbones(nil))
	assert.Equal(t, []string{"a", "b"}, splitBackbones("a,b"))
	assert.Nil(t, splitBackbones(""))
}

func TestPeernetRecords_UnhealthyAttestationsSince(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		ctx := context.Background()
		insertOK(t, b, mkAtt(true, func(r *PeernetRecord) { r.ReleaseID = relHex(1); r.PublisherNodeID = nodeHex(1) }))
		insertOK(t, b, mkAtt(false, func(r *PeernetRecord) { r.ReleaseID = relHex(2); r.PublisherNodeID = nodeHex(2) }))
		insertOK(t, b, mkAtt(false, func(r *PeernetRecord) {
			r.ReleaseID = relHex(3)
			r.PublisherNodeID = nodeHex(3)
			r.Source = PeernetRecordSourceLocal
		}))
		insertOK(t, b, mkAtt(false, func(r *PeernetRecord) { r.ReleaseID = relHex(4); r.PublisherNodeID = nodeHex(4) }))
		insertOK(t, b, mkRec(func(r *PeernetRecord) { r.ReleaseID = relHex(5); r.PublisherNodeID = nodeHex(5) }))

		page, err := b.recs.UnhealthyAttestationsSince(ctx, hexID('a'), 0, 10)
		require.NoError(t, err)
		require.Len(t, page.Rows, 2, "only unhealthy attestations received from peers")
		assert.Equal(t, relHex(2), page.Rows[0].ReleaseID)
		assert.Equal(t, relHex(4), page.Rows[1].ReleaseID)
		assert.Equal(t, page.Rows[1].Seq, page.HighestSeq)
		assert.False(t, page.More)

		// A cursor makes each attestation come up once.
		next, err := b.recs.UnhealthyAttestationsSince(ctx, hexID('a'), page.Rows[0].Seq, 10)
		require.NoError(t, err)
		require.Len(t, next.Rows, 1)
		assert.Equal(t, relHex(4), next.Rows[0].ReleaseID)

		// The page is bounded and reports more.
		small, err := b.recs.UnhealthyAttestationsSince(ctx, hexID('a'), 0, 1)
		require.NoError(t, err)
		require.Len(t, small.Rows, 1)
		assert.True(t, small.More)
		_, err = b.recs.UnhealthyAttestationsSince(ctx, "bad", 0, 1)
		assert.Error(t, err)
	})
}
