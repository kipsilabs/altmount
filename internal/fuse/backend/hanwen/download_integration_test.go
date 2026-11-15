//go:build linux

package hanwen_test

import (
	"context"
	"log/slog"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/kipsilabs/altmount/internal/api"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/fuse/backend/hanwen"
	"github.com/kipsilabs/altmount/internal/metadata"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/nzbfilesystem"
	"github.com/kipsilabs/altmount/internal/nzbfilesystem/segcache"
	"github.com/kipsilabs/altmount/internal/pool"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/kipsilabs/altmount/internal/testsupport/segments"
	"github.com/stretchr/testify/require"
)

type downloadPool struct {
	pool.Manager
	client *fakepool.Client
}

func (p downloadPool) GetPool() (pool.NntpClient, error)        { return p.client, nil }
func (downloadPool) SpeculativeBudget() *pool.SpeculativeBudget { return nil }

// A completed read can precede download accounting and cache insertion.
// Synchronize the observer with accounting before taking API snapshots.
type completedDownloadTracker struct {
	*api.StreamTracker
	downloaded chan struct{}
}

func (s completedDownloadTracker) UpdateDownloadProgress(id string, n int64) {
	s.StreamTracker.UpdateDownloadProgress(id, n)
	close(s.downloaded)
}

// This runs through actual FUSE opens/reads, persisted metadata, the Usenet
// downloader, the segment cache, and the API stream tracker without a mount.
func TestFUSEDownloadMetricsIntegration(t *testing.T) {
	const size = 1024
	ctx := context.Background()
	ms := metadata.NewMetadataService(t.TempDir())
	meta := ms.CreateFileMetadata(size, "movie.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY,
		[]*metapb.SegmentData{{Id: segments.MessageID(0), SegmentSize: size, StartOffset: 0, EndOffset: size - 1}},
		metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "")
	require.NoError(t, ms.WriteFileMetadata("movie.mkv", meta))
	client := fakepool.New()
	client.SetBehavior(segments.MessageID(0), fakepool.SegmentBehavior{Bytes: segments.Payload(0, size)})
	cfg := config.DefaultConfig()
	disabled := false
	cfg.Streaming.FailureMasking.Enabled = &disabled
	memoryMB := 1
	cfg.SegmentCache.MemoryMB = &memoryMB
	getCfg := func() *config.Config { return cfg }
	tracker := completedDownloadTracker{StreamTracker: api.NewStreamTracker(nil), downloaded: make(chan struct{})}
	t.Cleanup(tracker.Stop)
	cache := segcache.NewSource(getCfg)
	remote := nzbfilesystem.NewMetadataRemoteFile(ms, nil, nil, nil, downloadPool{client: client}, getCfg, tracker, cache)
	backing := nzbfilesystem.NewNzbFilesystem(remote)
	root := hanwen.NewDir(backing, "", slog.Default(), 0, 0, tracker, 0, false)
	fs.NewNodeFS(root, &fs.Options{})
	node, errno := root.Lookup(ctx, "movie.mkv", &fuse.EntryOut{})
	require.Zero(t, errno)
	file := node.Operations().(*hanwen.File)
	first, _, errno := file.Open(ctx, syscall.O_RDONLY)
	require.Zero(t, errno)
	h1 := first.(*hanwen.Handle)
	defer h1.Release(ctx)
	result, errno := h1.Read(ctx, make([]byte, size), 0)
	require.Zero(t, errno)
	got, status := result.Bytes(nil)
	require.True(t, status.Ok())
	require.Equal(t, segments.FileBytes(1, size), got)
	select {
	case <-tracker.downloaded:
	case <-time.After(3 * time.Second):
		t.Fatal("downloader did not report progress")
	}
	require.Eventually(t, func() bool { _, ok := cache.Store().Get(segments.MessageID(0)); return ok }, time.Second, time.Millisecond, "downloaded article must be cached before the warm open")
	streams := tracker.GetAll()
	require.Len(t, streams, 1)
	require.Equal(t, "FUSE", streams[0].Source)
	require.Equal(t, int64(size), streams[0].BytesDownloaded)
	require.Equal(t, int64(size), streams[0].BytesSent, "OUT must be counted exactly once")
	require.Equal(t, 1, tracker.ActiveConnections(), "underlying open must not register a second stream")
	second, _, errno := file.Open(ctx, syscall.O_RDONLY)
	require.Zero(t, errno)
	h2 := second.(*hanwen.Handle)
	defer h2.Release(ctx)
	require.Equal(t, 2, tracker.ActiveConnections())
	_, errno = h2.Read(ctx, make([]byte, size), 0)
	require.Zero(t, errno)
	require.Equal(t, syscall.Errno(0), h1.Release(ctx))
	streams = tracker.GetAll()
	require.Len(t, streams, 1)
	require.Zero(t, streams[0].BytesDownloaded, "second handle is entirely cached")
	require.Equal(t, int64(size), streams[0].BytesSent)
	require.Equal(t, int64(1), client.BodyCalls()+client.BodyPriorityCalls())
	require.Equal(t, syscall.Errno(0), h2.Release(ctx))
	require.Zero(t, tracker.ActiveConnections())
}
