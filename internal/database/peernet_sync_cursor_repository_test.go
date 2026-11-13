package database

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPeernetSyncCursors_UnknownPeerIsZero(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		c, err := b.cur.Get(context.Background(), hexID('a'), nodeHex(1))
		require.NoError(t, err)
		assert.Zero(t, c.PulledSeq)
		assert.Zero(t, c.PushedSeq)
	})
}

func TestPeernetSyncCursors_NeverMoveBackwardsExceptOnReset(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		ctx := context.Background()
		net, peer := hexID('a'), nodeHex(1)
		require.NoError(t, b.cur.SetPullCursor(ctx, net, peer, 10))
		require.NoError(t, b.cur.SetPullCursor(ctx, net, peer, 4), "a lower value is ignored, not an error")
		c, _ := b.cur.Get(ctx, net, peer)
		assert.Equal(t, int64(10), c.PulledSeq)
		require.NoError(t, b.cur.SetPullCursor(ctx, net, peer, 12))
		c, _ = b.cur.Get(ctx, net, peer)
		assert.Equal(t, int64(12), c.PulledSeq)

		require.NoError(t, b.cur.SetPushCursor(ctx, net, peer, 7))
		require.NoError(t, b.cur.SetPushCursor(ctx, net, peer, 3))
		c, _ = b.cur.Get(ctx, net, peer)
		assert.Equal(t, int64(7), c.PushedSeq)
		assert.Equal(t, int64(12), c.PulledSeq, "push and pull are independent")

		b.setNow(recT0.Add(time.Hour))
		require.NoError(t, b.cur.ResetPullCursor(ctx, net, peer, 2))
		c, _ = b.cur.Get(ctx, net, peer)
		assert.Equal(t, int64(2), c.PulledSeq)
		assert.Equal(t, int64(7), c.PushedSeq, "reset leaves the push cursor")
		assert.Equal(t, recT0.Add(time.Hour).Unix(), c.UpdatedAt.Unix())

		// Reset works on a row that does not exist yet, and per peer.
		require.NoError(t, b.cur.ResetPullCursor(ctx, net, nodeHex(2), 0))
		require.NoError(t, b.cur.SetPullCursor(ctx, net, nodeHex(2), 3))
		other, _ := b.cur.Get(ctx, net, nodeHex(2))
		assert.Equal(t, int64(3), other.PulledSeq)
	})
}

func TestPeernetSyncCursors_ValidationAndDelete(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		ctx := context.Background()
		assert.Error(t, b.cur.SetPullCursor(ctx, "bad", nodeHex(1), 1))
		assert.Error(t, b.cur.SetPullCursor(ctx, hexID('a'), "bad", 1))
		assert.Error(t, b.cur.SetPullCursor(ctx, hexID('a'), nodeHex(1), -1))
		assert.Error(t, b.cur.ResetPullCursor(ctx, hexID('a'), nodeHex(1), -1))
		assert.Error(t, b.cur.SetPushCursor(ctx, hexID('a'), nodeHex(1), -1))

		require.NoError(t, b.cur.SetPullCursor(ctx, hexID('a'), nodeHex(1), 5))
		require.NoError(t, b.cur.Delete(ctx, hexID('a'), nodeHex(1)))
		require.NoError(t, b.cur.Delete(ctx, hexID('a'), nodeHex(1)), "absent row is not an error")
		c, _ := b.cur.Get(ctx, hexID('a'), nodeHex(1))
		assert.Zero(t, c.PulledSeq)
	})
}
