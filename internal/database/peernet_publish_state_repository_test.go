package database

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stateBackend struct {
	state   *PeernetPublishStateRepository
	content *PeernetContentRepository
}

func forEachStateBackend(t *testing.T, fn func(t *testing.T, b *stateBackend)) {
	t.Helper()
	mk := func(conn *sql.DB, d Dialect) *stateBackend {
		return &stateBackend{state: NewPeernetPublishStateRepository(conn, d), content: NewPeernetContentRepository(conn, d)}
	}
	t.Run("sqlite", func(t *testing.T) {
		db, err := NewDB(Config{DatabasePath: filepath.Join(t.TempDir(), "db")})
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		fn(t, mk(db.Connection(), DialectSQLite))
	})
	if hasPostgresDSN() {
		t.Run("postgres", func(t *testing.T) { fn(t, mk(openPostgresLatest(t), DialectPostgres)) })
	}
}

func st(nw string, kind int, rel, file string, published int64) PeernetPublishState {
	return PeernetPublishState{NetworkID: nw, Kind: kind, ReleaseID: rel, FileID: file, PublishedAt: time.Unix(published, 0)}
}

var zeroFile = ""

func TestPeernetPublishState_RoundTripAndReplace(t *testing.T) {
	forEachStateBackend(t, func(t *testing.T, b *stateBackend) {
		ctx := context.Background()
		_, ok, err := b.state.Get(ctx, hexID('a'), 1, hexID('1'), zeroFile)
		require.NoError(t, err)
		assert.False(t, ok)

		in := st(hexID('a'), 2, hexID('1'), hexID('f'), 1000)
		in.CheckedAt, in.Verdict = time.Unix(900, 0), 1
		require.NoError(t, b.state.Put(ctx, in))
		got, ok, err := b.state.Get(ctx, hexID('a'), 2, hexID('1'), hexID('f'))
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, int64(1000), got.PublishedAt.Unix())
		assert.Equal(t, int64(900), got.CheckedAt.Unix())
		assert.Equal(t, 1, got.Verdict)

		in.PublishedAt, in.Verdict = time.Unix(2000, 0), 2
		require.NoError(t, b.state.Put(ctx, in))
		got, _, _ = b.state.Get(ctx, hexID('a'), 2, hexID('1'), hexID('f'))
		assert.Equal(t, int64(2000), got.PublishedAt.Unix())
		assert.Equal(t, 2, got.Verdict)

		// A release record uses the empty file ID; other keys are independent.
		require.NoError(t, b.state.Put(ctx, st(hexID('a'), 1, hexID('1'), zeroFile, 5)))
		_, ok, _ = b.state.Get(ctx, hexID('a'), 1, hexID('1'), zeroFile)
		assert.True(t, ok)
		_, ok, _ = b.state.Get(ctx, hexID('b'), 1, hexID('1'), zeroFile)
		assert.False(t, ok, "state is per network")
	})
}

func TestPeernetPublishState_Validation(t *testing.T) {
	forEachStateBackend(t, func(t *testing.T, b *stateBackend) {
		ctx := context.Background()
		assert.Error(t, b.state.Put(ctx, st("bad", 1, hexID('1'), zeroFile, 1)))
		assert.Error(t, b.state.Put(ctx, st(hexID('a'), 1, "bad", zeroFile, 1)))
		assert.Error(t, b.state.Put(ctx, st(hexID('a'), 1, hexID('1'), "bad", 1)))
		assert.Error(t, b.state.Put(ctx, st(hexID('a'), 9, hexID('1'), zeroFile, 1)))
		_, _, err := b.state.Get(ctx, "bad", 1, hexID('1'), zeroFile)
		assert.Error(t, err)
	})
}

func TestPeernetPublishState_ListReleasesDue(t *testing.T) {
	forEachStateBackend(t, func(t *testing.T, b *stateBackend) {
		ctx := context.Background()
		for i, ts := range []int64{300, 100, 200, 400} {
			require.NoError(t, b.state.Put(ctx, st(hexID('a'), 1, relHex(i), zeroFile, ts)))
		}
		require.NoError(t, b.state.Put(ctx, st(hexID('a'), 2, relHex(0), hexID('f'), 50)), "attestations are never listed")

		got, err := b.state.ListReleasesPublishedBefore(ctx, time.Unix(350, 0), 10)
		require.NoError(t, err)
		require.Len(t, got, 3)
		assert.Equal(t, []int64{100, 200, 300}, []int64{got[0].PublishedAt.Unix(), got[1].PublishedAt.Unix(), got[2].PublishedAt.Unix()}, "oldest first")

		got, err = b.state.ListReleasesPublishedBefore(ctx, time.Unix(350, 0), 2)
		require.NoError(t, err)
		assert.Len(t, got, 2, "bounded")
		_, err = b.state.ListReleasesPublishedBefore(ctx, time.Unix(350, 0), 0)
		assert.Error(t, err)
	})
}

func TestPeernetPublishState_DeleteNetworkAndOrphanNetworks(t *testing.T) {
	forEachStateBackend(t, func(t *testing.T, b *stateBackend) {
		ctx := context.Background()
		require.NoError(t, b.state.Put(ctx, st(hexID('a'), 1, hexID('1'), zeroFile, 1)))
		require.NoError(t, b.state.Put(ctx, st(hexID('a'), 2, hexID('1'), hexID('f'), 1)))
		require.NoError(t, b.state.Put(ctx, st(hexID('b'), 1, hexID('1'), zeroFile, 1)))
		require.NoError(t, b.state.Put(ctx, st(hexID('c'), 1, hexID('1'), zeroFile, 1)))

		require.NoError(t, b.state.DeleteNetwork(ctx, hexID('a')))
		_, ok, _ := b.state.Get(ctx, hexID('a'), 1, hexID('1'), zeroFile)
		assert.False(t, ok)
		_, ok, _ = b.state.Get(ctx, hexID('b'), 1, hexID('1'), zeroFile)
		assert.True(t, ok, "other networks are untouched")
		assert.Error(t, b.state.DeleteNetwork(ctx, "bad"))

		n, err := b.state.DeleteOrphanNetworks(ctx, func(context.Context) (map[string]bool, error) {
			return map[string]bool{hexID('b'): true}, nil
		})
		require.NoError(t, err)
		assert.EqualValues(t, 1, n)
		_, ok, _ = b.state.Get(ctx, hexID('c'), 1, hexID('1'), zeroFile)
		assert.False(t, ok)
		_, ok, _ = b.state.Get(ctx, hexID('b'), 1, hexID('1'), zeroFile)
		assert.True(t, ok)
	})
}

func TestPeernetPublishState_PruneUnindexedIsBounded(t *testing.T) {
	forEachStateBackend(t, func(t *testing.T, b *stateBackend) {
		ctx := context.Background()
		require.NoError(t, b.content.Upsert(ctx, PeernetContentEntry{FilePath: "m/kept.mkv", FileID: hexID('f'), ReleaseID: hexID('1')}))
		require.NoError(t, b.state.Put(ctx, st(hexID('a'), 1, hexID('1'), zeroFile, 1)))
		require.NoError(t, b.state.Put(ctx, st(hexID('a'), 2, hexID('1'), hexID('f'), 1)))
		for i := range 5 {
			require.NoError(t, b.state.Put(ctx, st(hexID('a'), 1, relHex(i), zeroFile, 1)))
		}

		// Seven rows in key order: five orphans, then the two rows of the kept release.
		n, cur, err := b.state.PruneUnindexed(ctx, PeernetPublishCursor{}, 3)
		require.NoError(t, err)
		assert.EqualValues(t, 3, n, "only the window is scanned")
		assert.NotZero(t, cur)
		n, cur, err = b.state.PruneUnindexed(ctx, cur, 3)
		require.NoError(t, err)
		assert.EqualValues(t, 2, n)
		n, cur, err = b.state.PruneUnindexed(ctx, cur, 3)
		require.NoError(t, err)
		assert.Zero(t, n)
		assert.Zero(t, cur, "the scan wrapped around")

		_, ok, _ := b.state.Get(ctx, hexID('a'), 1, hexID('1'), zeroFile)
		assert.True(t, ok, "a release still in the content index is kept")
		_, ok, _ = b.state.Get(ctx, hexID('a'), 2, hexID('1'), hexID('f'))
		assert.True(t, ok)
		_, _, err = b.state.PruneUnindexed(ctx, PeernetPublishCursor{}, 0)
		assert.Error(t, err)
	})
}

func TestPeernetPublishState_OrphanCleanupReadsStoredNetworksBeforeLiveOnes(t *testing.T) {
	forEachStateBackend(t, func(t *testing.T, b *stateBackend) {
		ctx := context.Background()
		require.NoError(t, b.state.Put(ctx, st(hexID('a'), 1, hexID('1'), zeroFile, 1)))
		// A network is created (and publishes) after the stored IDs were read
		// but is missing from the live list the callback returns: it must
		// survive, because it was not in the snapshot.
		n, err := b.state.DeleteOrphanNetworks(ctx, func(ctx context.Context) (map[string]bool, error) {
			require.NoError(t, b.state.Put(ctx, st(hexID('b'), 1, hexID('1'), zeroFile, 1)))
			return map[string]bool{hexID('a'): true}, nil
		})
		require.NoError(t, err)
		assert.Zero(t, n)
		_, ok, _ := b.state.Get(ctx, hexID('b'), 1, hexID('1'), zeroFile)
		assert.True(t, ok, "a network created during the pass was deleted as an orphan")
	})
}

func TestPeernetPublishState_OriginTagRoundTripAndExclusionLookup(t *testing.T) {
	forEachStateBackend(t, func(t *testing.T, b *stateBackend) {
		ctx := context.Background()
		tagged := func(nw string, rel string, kind int, file, cat, idx string) PeernetPublishState {
			s := st(nw, kind, rel, file, 1)
			s.OriginCategory, s.OriginIndexer = cat, idx
			return s
		}
		require.NoError(t, b.state.Put(ctx, tagged(hexID('a'), hexID('1'), 1, zeroFile, " Movies ", "IdxOne")))
		require.NoError(t, b.state.Put(ctx, tagged(hexID('a'), hexID('1'), 2, hexID('f'), "movies", "idxone")))
		require.NoError(t, b.state.Put(ctx, tagged(hexID('a'), hexID('2'), 1, zeroFile, "tv", "other")))
		require.NoError(t, b.state.Put(ctx, st(hexID('a'), 1, hexID('3'), zeroFile, 1))) // no tag: pre-migration

		got, _, err := b.state.Get(ctx, hexID('a'), 1, hexID('1'), zeroFile)
		require.NoError(t, err)
		assert.Equal(t, "movies", got.OriginCategory, "tags are stored lower-cased and trimmed")

		hits, err := b.state.ListExcluded(ctx, []string{"MOVIES"}, nil, 10)
		require.NoError(t, err)
		assert.Equal(t, []PeernetPublishedRelease{{NetworkID: hexID('a'), ReleaseID: hexID('1')}}, hits)
		hits, err = b.state.ListExcluded(ctx, nil, []string{" Other "}, 10)
		require.NoError(t, err)
		assert.Equal(t, []PeernetPublishedRelease{{NetworkID: hexID('a'), ReleaseID: hexID('2')}}, hits)
		hits, err = b.state.ListExcluded(ctx, nil, nil, 10)
		require.NoError(t, err)
		assert.Empty(t, hits)

		require.NoError(t, b.state.DeleteRelease(ctx, hexID('a'), hexID('1')))
		_, ok, _ := b.state.Get(ctx, hexID('a'), 2, hexID('1'), hexID('f'))
		assert.False(t, ok)
	})
}

// The publisher treats an indexer of "unknown" as no indexer; the retroactive
// exclusion lookup must agree, or an entry literally named "unknown" would
// retract every release whose indexer is not known.
func TestPeernetPublishState_UnknownIndexerEntryMatchesNothing(t *testing.T) {
	forEachStateBackend(t, func(t *testing.T, b *stateBackend) {
		ctx := context.Background()
		s := st(hexID('a'), 1, hexID('1'), zeroFile, 1)
		s.OriginIndexer = "unknown"
		require.NoError(t, b.state.Put(ctx, s))
		hits, err := b.state.ListExcluded(ctx, nil, []string{"Unknown"}, 10)
		require.NoError(t, err)
		assert.Empty(t, hits)
	})
}
