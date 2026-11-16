package contentid

import (
	"context"
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/stretchr/testify/require"
)

// synthetic fills a repository with `files` old-format rows in groups of
// `perGroup` files (every row has a file ID and a group key, no release ID).
func synthetic(tb testing.TB, files, perGroup int) *database.PeernetContentRepository {
	tb.Helper()
	db, err := database.NewDB(database.Config{DatabasePath: filepath.Join(tb.TempDir(), "db")})
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = db.Close() })
	repo := database.NewPeernetContentRepository(db.Connection(), database.DialectSQLite)
	ctx := context.Background()
	batch := make([]database.PeernetContentEntry, 0, 1000)
	for n := range files {
		fid := sha256.Sum256(fmt.Appendf(nil, "file-%d", n))
		gid := sha256.Sum256(fmt.Appendf(nil, "group-%d", n/perGroup))
		batch = append(batch, database.PeernetContentEntry{
			FilePath: fmt.Sprintf("complete/Release.%d/file.%d.mkv", n/perGroup, n),
			FileID:   fmt.Sprintf("%x", fid), GroupKey: fmt.Sprintf("%x", gid),
		})
		if len(batch) == cap(batch) {
			require.NoError(tb, repo.UpsertBatch(ctx, batch))
			batch = batch[:0]
		}
	}
	require.NoError(tb, repo.UpsertBatch(ctx, batch))
	return repo
}

// TestDeriveAll_200kRows measures a full derive pass over 200,000 rows
// (40,000 groups of 5) and a second, no-op pass, on a fresh SQLite database.
// The bound is generous: it exists to catch an accidental O(n^2), not to be a
// benchmark. The measured times are logged.
func TestDeriveAll_200kRows(t *testing.T) {
	if testing.Short() {
		t.Skip("200k-row measurement")
	}
	repo := synthetic(t, 200_000, 5)
	idx := NewIndexer(nil, repo)
	ctx := context.Background()

	start := time.Now()
	res, err := idx.DeriveAll(ctx)
	first := time.Since(start)
	require.NoError(t, err)
	require.Equal(t, 40_000, res.Groups)
	require.Equal(t, 200_000, res.Assigned)

	start = time.Now()
	res, err = idx.DeriveAll(ctx)
	second := time.Since(start)
	require.NoError(t, err)
	require.Equal(t, 0, res.Assigned)

	t.Logf("derive 200k rows / 40k groups: first pass %v (200k rows written), unchanged pass %v", first, second)
	require.Less(t, first, 2*time.Minute)
}

func BenchmarkDeriveAll200k(b *testing.B) {
	for b.Loop() {
		b.StopTimer()
		repo := synthetic(b, 200_000, 5)
		idx := NewIndexer(nil, repo)
		b.StartTimer()
		if _, err := idx.DeriveAll(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}
