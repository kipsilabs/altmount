package database

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A requeued fast-path import must skip the fast path once, and that must
// survive a restart: the flag lives on the queue row, not in memory.
func TestPeerSkipFastPath_TheMarkLivesOnTheRowAndTheClaimConsumesItOnce(t *testing.T) {
	forEachBundleImportBackend(t, func(t *testing.T, r *PeernetBundleImportRepository) {
		ctx := context.Background()
		f := newHoldFixture(t, r, "skipfp", false)
		got, err := r.ClaimSkipFastPath(ctx, f.id)
		require.NoError(t, err)
		assert.False(t, got, "nothing was requeued yet")

		require.NoError(t, r.SetSkipFastPath(ctx, f.id, true))
		got, err = r.ClaimSkipFastPath(ctx, f.id)
		require.NoError(t, err)
		require.True(t, got)
		require.NoError(t, r.SetSkipFastPath(ctx, f.id, true))
		require.NoError(t, r.SetSkipFastPath(ctx, f.id, false))
		got, err = r.ClaimSkipFastPath(ctx, f.id)
		require.NoError(t, err)
		assert.False(t, got, "an unmarked item is not claimed")
		require.NoError(t, r.SetSkipFastPath(ctx, f.id, true))

		got, err = r.ClaimSkipFastPath(ctx, f.id)
		require.NoError(t, err)
		assert.True(t, got, "the marked item skips the fast path")
		got, err = r.ClaimSkipFastPath(ctx, f.id)
		require.NoError(t, err)
		assert.False(t, got, "once")
	})
}
