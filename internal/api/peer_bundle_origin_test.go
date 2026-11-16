package api

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeLookup struct {
	queues map[int64]bool
	paths  map[string]bool
	calls  int
}

func (f *fakeLookup) StatesForQueues(_ context.Context, _ []int64) (map[int64]bool, error) {
	f.calls++
	return f.queues, nil
}
func (f *fakeLookup) StatesForPaths(_ context.Context, _ []string) (map[string]bool, error) {
	f.calls++
	return f.paths, nil
}

func TestPeerOrigin_QueueItems(t *testing.T) {
	s := &Server{}
	items := []*database.ImportQueueItem{{ID: 1}, {ID: 2}, {ID: 3}}
	resp := []*QueueItemResponse{ToQueueItemResponse(items[0]), ToQueueItemResponse(items[1]), ToQueueItemResponse(items[2])}

	s.annotateQueuePeerOrigin(context.Background(), items, resp)
	for _, r := range resp {
		assert.False(t, r.PeerOrigin || r.PeerFullCheckPending, "no lookup installed: nothing flagged")
	}

	l := &fakeLookup{queues: map[int64]bool{1: true, 2: false}}
	s.SetPeerBundleLookup(l)
	s.annotateQueuePeerOrigin(context.Background(), items, resp)
	assert.True(t, resp[0].PeerOrigin)
	assert.True(t, resp[0].PeerFullCheckPending)
	assert.True(t, resp[1].PeerOrigin)
	assert.False(t, resp[1].PeerFullCheckPending, "verified")
	assert.False(t, resp[2].PeerOrigin)
	assert.Equal(t, 1, l.calls, "one lookup for the page")

	s.SetPeerBundleLookup(nil)
	l.calls = 0
	s.annotateQueuePeerOrigin(context.Background(), items, resp)
	assert.Zero(t, l.calls, "removed lookup is never asked")
}

func TestPeerOrigin_HealthItemsMatchByNormalisedPath(t *testing.T) {
	s := &Server{}
	l := &fakeLookup{paths: map[string]bool{"movies/a.mkv": true}}
	s.SetPeerBundleLookup(l)
	items := []*database.FileHealth{{FilePath: "movies/a.mkv"}, {FilePath: "movies/b.mkv"}}
	resp := []*HealthItemResponse{ToHealthItemResponse(items[0]), ToHealthItemResponse(items[1])}
	s.annotateHealthPeerOrigin(context.Background(), items, resp)
	assert.True(t, resp[0].PeerOrigin)
	assert.True(t, resp[0].PeerFullCheckPending)
	assert.False(t, resp[1].PeerOrigin)
}

func TestPeerOrigin_JSONNames(t *testing.T) {
	b, err := json.Marshal(ToQueueItemResponse(&database.ImportQueueItem{}))
	require.NoError(t, err)
	assert.Contains(t, string(b), `"peer_origin":false`)
	assert.Contains(t, string(b), `"peer_full_check_pending":false`)
	b, err = json.Marshal(ToHealthItemResponse(&database.FileHealth{}))
	require.NoError(t, err)
	assert.Contains(t, string(b), `"peer_origin":false`)
	assert.Contains(t, string(b), `"peer_full_check_pending":false`)
}
