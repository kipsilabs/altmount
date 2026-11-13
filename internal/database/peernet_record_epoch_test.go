package database

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPeernetRecordEpoch_StableAndPerNetwork(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		ctx := context.Background()
		e1, err := b.recs.Epoch(ctx, hexID('a'))
		require.NoError(t, err)
		assert.Regexp(t, `^[0-9a-f]{32}$`, e1)
		again, err := b.recs.Epoch(ctx, hexID('a'))
		require.NoError(t, err)
		assert.Equal(t, e1, again, "stable while the network is joined")
		other, err := b.recs.Epoch(ctx, hexID('b'))
		require.NoError(t, err)
		assert.NotEqual(t, e1, other)
		_, err = b.recs.Epoch(ctx, "bad")
		assert.Error(t, err)
	})
}

func TestPeernetRecordEpoch_ChangesAfterDeleteNetwork(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		ctx := context.Background()
		before, err := b.recs.Epoch(ctx, hexID('a'))
		require.NoError(t, err)
		insertOK(t, b, mkRec())
		require.NoError(t, b.recs.DeleteNetwork(ctx, hexID('a')))
		after, err := b.recs.Epoch(ctx, hexID('a'))
		require.NoError(t, err)
		assert.NotEqual(t, before, after, "a restarted sequence must be visible to pullers")
	})
}

func TestPeernetRecords_NetworkIDs(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		ctx := context.Background()
		ids, err := b.recs.NetworkIDs(ctx)
		require.NoError(t, err)
		assert.Empty(t, ids)
		insertOK(t, b, mkRec())
		insertOK(t, b, mkRec(func(r *PeernetRecord) { r.NetworkID = hexID('b') }))
		_, err = b.recs.Epoch(ctx, hexID('c'))
		require.NoError(t, err)
		ids, err = b.recs.NetworkIDs(ctx)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{hexID('a'), hexID('b'), hexID('c')}, ids)
	})
}

func TestPeernetSyncCursors_RemoteEpoch(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		ctx := context.Background()
		net, peer := hexID('a'), nodeHex(1)
		c, err := b.cur.Get(ctx, net, peer)
		require.NoError(t, err)
		assert.Empty(t, c.RemoteEpoch)

		require.NoError(t, b.cur.SetPullCursor(ctx, net, peer, 9))
		require.NoError(t, b.cur.SetPushCursor(ctx, net, peer, 4))
		require.NoError(t, b.cur.ResetPullCursorEpoch(ctx, net, peer, 0, "e1"))
		c, _ = b.cur.Get(ctx, net, peer)
		assert.Equal(t, int64(0), c.PulledSeq, "reset moves backwards")
		assert.Equal(t, int64(4), c.PushedSeq, "push cursor untouched")
		assert.Equal(t, "e1", c.RemoteEpoch)

		// A plain pull update keeps the epoch.
		require.NoError(t, b.cur.SetPullCursor(ctx, net, peer, 3))
		c, _ = b.cur.Get(ctx, net, peer)
		assert.Equal(t, int64(3), c.PulledSeq)
		assert.Equal(t, "e1", c.RemoteEpoch)

		// Works on a row that does not exist.
		require.NoError(t, b.cur.ResetPullCursorEpoch(ctx, net, nodeHex(2), 0, "x"))
		assert.Error(t, b.cur.ResetPullCursorEpoch(ctx, net, nodeHex(2), -1, "x"))
		assert.Error(t, b.cur.ResetPullCursorEpoch(ctx, net, nodeHex(2), 0, "bad epoch"))
	})
}

func TestPeernetSyncCursors_DeleteOrphans(t *testing.T) {
	forEachRecBackend(t, func(t *testing.T, b *recBackend) {
		ctx := context.Background()
		net := hexID('a')
		_, err := b.peers.Insert(ctx, PeernetPeer{NetworkID: net, NodeID: nodeHex(1), Address: "203.0.113.5:7777",
			Source: PeernetSourceUser, FirstSeen: recT0, LastSeen: recT0}, PeernetPeerCaps{})
		require.NoError(t, err)
		require.NoError(t, b.cur.SetPullCursor(ctx, net, nodeHex(1), 5))
		require.NoError(t, b.cur.SetPullCursor(ctx, net, nodeHex(2), 5))        // no peer row
		require.NoError(t, b.cur.SetPullCursor(ctx, hexID('b'), nodeHex(1), 5)) // other network: no peer row

		n, err := b.cur.DeleteOrphans(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(2), n)
		c, _ := b.cur.Get(ctx, net, nodeHex(1))
		assert.Equal(t, int64(5), c.PulledSeq, "a cursor with a peer row stays")
		c, _ = b.cur.Get(ctx, net, nodeHex(2))
		assert.Zero(t, c.PulledSeq)
	})
}
