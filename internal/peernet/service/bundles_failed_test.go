package service

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kipsilabs/altmount/internal/database"
)

func TestFailedBundleBookReadsTheNegativeCache(t *testing.T) {
	db, err := database.NewDB(database.Config{DatabasePath: filepath.Join(t.TempDir(), "db")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	repo := database.NewPeernetBundleImportRepository(db.Connection(), database.DialectSQLite)
	book := failedBundleBook{repo: repo}
	ctx := context.Background()
	id := "ab" + string(make([]byte, 0)) + "cdef"
	assert.False(t, book.IsFailedBundle(ctx, id))
	require.NoError(t, repo.RecordFailedBundle(ctx, id, "rel", time.Now()))
	assert.True(t, book.IsFailedBundle(ctx, id))
	require.NoError(t, repo.RecordFailedBundle(ctx, "old", "rel", time.Now().Add(-failedBundleTTL-time.Hour)))
	assert.False(t, book.IsFailedBundle(ctx, "old"), "entries expire after the TTL")
	assert.False(t, failedBundleBook{}.IsFailedBundle(ctx, id), "no repository: nothing is skipped")
}
