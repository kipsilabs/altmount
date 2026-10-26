package failures

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBumpOnce_DeduplicatesFailureEventAcrossProducers(t *testing.T) {
	tracker := NewTracker()
	key := EpisodeKey("sonarr", 42)
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() { tracker.BumpOnce(key, "download-a") })
	}
	wg.Wait()
	require.Equal(t, 1, tracker.BumpOnce(key, "download-a"))
	require.Equal(t, 2, tracker.BumpOnce(key, "download-b"))
	tracker.Reset(key)
	require.Equal(t, 1, tracker.BumpOnce(key, "download-c"))
}

func TestSonarrAction_ReservationAndRetry(t *testing.T) {
	tracker := NewTracker()
	finish, ok := tracker.BeginAction("pack")
	require.True(t, ok)
	_, ok = tracker.BeginAction("pack")
	require.False(t, ok)
	finish(false) // an unmet precondition must allow a retry
	finish, ok = tracker.BeginAction("pack")
	require.True(t, ok)
	finish(true)
	_, ok = tracker.BeginAction("pack")
	require.False(t, ok)
	_, ok = tracker.BeginAction("replacement-pack")
	require.True(t, ok)
}

func TestLockAction_CancellationAndRelease(t *testing.T) {
	tracker := NewTracker()
	unlock, err := tracker.LockAction(context.Background(), "download")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = tracker.LockAction(ctx, "download")
	require.ErrorIs(t, err, context.Canceled)
	unlock()
	release, err := tracker.LockAction(context.Background(), "download")
	require.NoError(t, err)
	release()
	require.Empty(t, tracker.locks)
}

func TestSonarrDownloadKeySeparatesAmbiguousInstanceNames(t *testing.T) {
	require.NotEqual(t, SonarrDownloadKey("a|b", "c"), SonarrDownloadKey("a", "b|c"))
	require.Equal(t, SonarrDownloadKey("Sonarr", " PACK "), SonarrDownloadKey("Sonarr", "pack"))
}
