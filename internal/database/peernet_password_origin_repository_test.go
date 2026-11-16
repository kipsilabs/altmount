package database

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPeernetPasswordOrigin(t *testing.T) {
	db, err := NewDB(Config{DatabasePath: filepath.Join(t.TempDir(), "db")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	run := func(t *testing.T, r *PeernetPasswordOriginRepository) {
		ctx := context.Background()
		ok, err := r.FromNZB(ctx, hex64("a"))
		require.NoError(t, err)
		assert.False(t, ok, "no row: not from the NZB")

		require.NoError(t, r.MarkFromNZB(ctx, []string{hex64("a"), hex64("b")}))
		require.NoError(t, r.MarkFromNZB(ctx, []string{hex64("a")}), "idempotent")
		require.NoError(t, r.MarkFromNZB(ctx, nil), "nothing to do")
		for _, id := range []string{hex64("a"), hex64("b")} {
			ok, err = r.FromNZB(ctx, id)
			require.NoError(t, err)
			assert.True(t, ok)
		}
		ok, err = r.FromNZB(ctx, hex64("c"))
		require.NoError(t, err)
		assert.False(t, ok)

		assert.Error(t, r.MarkFromNZB(ctx, []string{"not-an-id"}), "only well-formed File IDs are stored")
		ok, err = r.FromNZB(ctx, "not-an-id")
		require.NoError(t, err)
		assert.False(t, ok)
	}
	t.Run("sqlite", func(t *testing.T) { run(t, NewPeernetPasswordOriginRepository(db.Connection(), DialectSQLite)) })
	if hasPostgresDSN() {
		t.Run("postgres", func(t *testing.T) {
			run(t, NewPeernetPasswordOriginRepository(openPostgresLatest(t), DialectPostgres))
		})
	}
}
