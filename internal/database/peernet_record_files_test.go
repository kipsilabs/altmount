package database

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPeernetRecords_AttestationsForFilesMatchesAcrossReleaseIDs(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		ctx := context.Background()
		file := fileHex(5)
		// The same file attested under an "nzb" and a "files" release ID.
		insertOK(t, b, mkAtt(true, func(r *PeernetRecord) { r.ReleaseID = relHex(1); r.FileID = file; r.PublisherNodeID = nodeHex(1) }))
		insertOK(t, b, mkAtt(true, func(r *PeernetRecord) {
			r.ReleaseID, r.FileID, r.PublisherNodeID, r.ReleaseScheme = relHex(2), file, nodeHex(2), PeernetSchemeFiles
		}))
		insertOK(t, b, mkAtt(true, func(r *PeernetRecord) { r.ReleaseID = relHex(3); r.FileID = fileHex(6); r.PublisherNodeID = nodeHex(3) }))
		insertOK(t, b, mkRec(func(r *PeernetRecord) { r.ReleaseID = relHex(1); r.PublisherNodeID = nodeHex(4) }))

		got, err := b.recs.AttestationsForFiles(ctx, hexID('a'), []string{file}, time.Time{})
		require.NoError(t, err)
		require.Len(t, got, 2, "both publishers, whatever release ID they used; no other file, no release record")
		pubs := []string{got[0].PublisherNodeID, got[1].PublisherNodeID}
		assert.ElementsMatch(t, []string{nodeHex(1), nodeHex(2)}, pubs)
		for _, a := range got {
			assert.Equal(t, file, a.FileID)
		}

		other, err := b.recs.AttestationsForFiles(ctx, hexID('b'), []string{file}, time.Time{})
		require.NoError(t, err)
		assert.Empty(t, other, "networks stay separate")

		many, err := b.recs.AttestationsForFiles(ctx, hexID('a'), []string{file, fileHex(6), file}, time.Time{})
		require.NoError(t, err)
		assert.Len(t, many, 3, "one batch, duplicates in the request are ignored")
	})
}

// One publisher must not be able to occupy the result with rows it made up
// under many release IDs for the same file.
func TestPeernetRecords_AttestationsForFilesOneRowPerPublisherAndVerdict(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		ctx := context.Background()
		file := fileHex(9)
		for i := range 40 {
			insertOK(t, b, mkAtt(true, func(r *PeernetRecord) {
				r.ReleaseID, r.FileID, r.PublisherNodeID = relHex(100+i), file, nodeHex(1)
				r.CheckedAt = recT0.Add(-time.Duration(i+1) * time.Minute)
			}))
		}
		insertOK(t, b, mkAtt(false, func(r *PeernetRecord) { r.ReleaseID, r.FileID, r.PublisherNodeID = relHex(500), file, nodeHex(1) }))
		insertOK(t, b, mkAtt(true, func(r *PeernetRecord) { r.ReleaseID, r.FileID, r.PublisherNodeID = relHex(501), file, nodeHex(2) }))

		got, err := b.recs.AttestationsForFiles(ctx, hexID('a'), []string{file}, time.Time{})
		require.NoError(t, err)
		require.Len(t, got, 3, "publisher 1 healthy (newest only), publisher 1 unhealthy, publisher 2 healthy")
		assert.False(t, got[0].Healthy, "unhealthy rows come first so truncation can only drop healthy evidence")
		var p1Healthy PeernetAttestation
		for _, a := range got {
			if a.PublisherNodeID == nodeHex(1) && a.Healthy {
				p1Healthy = a
			}
		}
		assert.Equal(t, recT0.Add(-time.Minute).Unix(), p1Healthy.CheckedAt.Unix(), "the newest of the publisher's rows is kept")
	})
}

func TestPeernetRecords_AttestationsForFilesFiltersAndBounds(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		ctx := context.Background()
		file := fileHex(7)
		insertOK(t, b, mkAtt(true, func(r *PeernetRecord) {
			r.FileID, r.PublisherNodeID = file, nodeHex(1)
			r.CheckedAt = recT0.Add(-10 * 24 * time.Hour)
			r.CreatedAt = recT0.Add(-10 * 24 * time.Hour)
			r.ExpiresAt = recT0.Add(time.Hour)
		}))
		insertOK(t, b, mkAtt(true, func(r *PeernetRecord) { r.FileID, r.PublisherNodeID = file, nodeHex(2) }))
		got, err := b.recs.AttestationsForFiles(ctx, hexID('a'), []string{file}, recT0.Add(-7*24*time.Hour))
		require.NoError(t, err)
		require.Len(t, got, 1, "stale check excluded")

		b.setNow(recT0.Add(30 * time.Hour))
		none, err := b.recs.AttestationsForFiles(ctx, hexID('a'), []string{file}, time.Time{})
		require.NoError(t, err)
		assert.Empty(t, none, "expired rows are not evidence")

		empty, err := b.recs.AttestationsForFiles(ctx, hexID('a'), nil, time.Time{})
		require.NoError(t, err)
		assert.Empty(t, empty)

		ids := make([]string, PeernetMaxFilesPerQuery+1)
		for i := range ids {
			ids[i] = fileHex(1000 + i)
		}
		_, err = b.recs.AttestationsForFiles(ctx, hexID('a'), ids, time.Time{})
		assert.ErrorIs(t, err, ErrPeernetTooManyIDs)
		_, err = b.recs.AttestationsForFiles(ctx, hexID('a'), ids[:PeernetMaxFilesPerQuery], time.Time{})
		assert.NoError(t, err, "exactly at the bound works")
		_, err = b.recs.AttestationsForFiles(ctx, "bad", []string{file}, time.Time{})
		assert.Error(t, err)
		_, err = b.recs.AttestationsForFiles(ctx, hexID('a'), []string{"nothex"}, time.Time{})
		assert.Error(t, err)
	})
}

func TestPeernetRecords_ReleaseSchemeStoredAndReturned(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		ctx := context.Background()
		insertOK(t, b, mkRec(func(r *PeernetRecord) { r.IMDbID = "tt0000009"; r.ReleaseID = relHex(70) }))
		insertOK(t, b, mkRec(func(r *PeernetRecord) {
			r.IMDbID = "tt0000009"
			r.ReleaseID = relHex(71)
			r.ReleaseScheme = PeernetSchemeFiles
			r.PublisherNodeID = nodeHex(2)
		}))
		got, err := b.recs.CandidatesByMedia(ctx, []string{hexID('a')}, "tt0000009", nil, nil, 10)
		require.NoError(t, err)
		schemes := map[string]int{}
		for _, c := range got {
			schemes[c.ReleaseID] = c.ReleaseScheme
		}
		assert.Equal(t, map[string]int{relHex(70): PeernetSchemeNZB, relHex(71): PeernetSchemeFiles}, schemes,
			"an unset scheme is stored as nzb")

		bad := mkRec(func(r *PeernetRecord) { r.ReleaseScheme = 3 })
		_, err = b.recs.Insert(ctx, bad, PeernetRecordLimits{})
		assert.ErrorIs(t, err, ErrPeernetRecordInvalid)
	})
}

// The file lookup must be served by an index, not a table scan.
func TestPeernetRecords_FileLookupUsesIndex(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		if strings.Contains(t.Name(), "postgres") {
			t.Skip("plan shape depends on table statistics; checked on sqlite")
		}
		rows, err := b.recs.db.QueryContext(context.Background(),
			"EXPLAIN QUERY PLAN "+attestationsForFilesSQL(2), hexID('a'), 2, fileHex(1), fileHex(2), 0, 0, 10)
		require.NoError(t, err)
		defer rows.Close()
		var plan []string
		for rows.Next() {
			var id, parent, unused int
			var detail string
			require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
			plan = append(plan, detail)
		}
		joined := strings.Join(plan, "; ")
		assert.Contains(t, joined, "idx_peernet_records_file", fmt.Sprintf("plan: %s", joined))
	})
}
