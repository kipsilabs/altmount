//go:build linux

package hanwen

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"syscall"
	"testing"

	"github.com/kipsilabs/altmount/internal/nzbfilesystem"
	"github.com/kipsilabs/altmount/internal/utils"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

type openContextFS struct {
	*fakeNzbFS
	mu         sync.Mutex
	ids        []string
	suppressed []bool
	ctxs       []context.Context
	err        error
}

func (f *openContextFS) Open(ctx context.Context, name string) (afero.File, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id, _ := ctx.Value(utils.StreamIDKey).(string)
	suppress, _ := ctx.Value(utils.SuppressStreamTrackingKey).(bool)
	f.ids = append(f.ids, id)
	f.suppressed = append(f.suppressed, suppress)
	f.ctxs = append(f.ctxs, ctx)
	if f.err != nil {
		return nil, f.err
	}
	return f.fakeNzbFS.Open(ctx, name)
}

type handleStreamTracker struct {
	mu      sync.Mutex
	streams map[string]bool
}

func (s *handleStreamTracker) AddStream(path, _, _, _, _ string, size int64) *nzbfilesystem.ActiveStream {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := fmt.Sprintf("handle-%d", len(s.streams))
	s.streams[id] = true
	return &nzbfilesystem.ActiveStream{ID: id, FilePath: path, TotalSize: size}
}
func (*handleStreamTracker) UpdateProgress(string, int64) {}
func (s *handleStreamTracker) Remove(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.streams[id] = false
}

func TestFileOpenPassesPerHandleDownloadID(t *testing.T) {
	backing := &openContextFS{fakeNzbFS: newFakeNzbFS()}
	backing.addFile("movie.mkv", 1024)
	tracker := &handleStreamTracker{streams: make(map[string]bool)}
	file := &File{nzbfs: backing, path: "movie.mkv", logger: slog.Default(), streamTracker: tracker}
	var wg sync.WaitGroup
	handles := make([]*Handle, 2)
	for i := range handles {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			handle, _, errno := file.Open(context.Background(), syscall.O_RDONLY)
			if errno == 0 {
				handles[i] = handle.(*Handle)
			}
		}(i)
	}
	wg.Wait()
	for _, handle := range handles {
		require.NotNil(t, handle)
		defer handle.Release(context.Background())
	}
	require.Len(t, backing.ids, 2)
	require.NotEmpty(t, backing.ids[0], "downloader needs the FUSE handle stream ID before open")
	require.NotEqual(t, backing.ids[0], backing.ids[1])
	require.Equal(t, []bool{true, true}, backing.suppressed, "underlying opens must suppress duplicate streams")
	require.Len(t, tracker.streams, 2)
	for _, handle := range handles {
		require.Contains(t, backing.ids, handle.stream.ID)
		require.Equal(t, int64(1024), handle.stream.TotalSize)
		require.True(t, tracker.streams[handle.stream.ID])
		id := handle.stream.ID
		require.Equal(t, syscall.Errno(0), handle.Release(context.Background()))
		require.False(t, tracker.streams[id])
	}
}

func TestFileOpenIgnoresOpenContextCancellation(t *testing.T) {
	backing := &openContextFS{fakeNzbFS: newFakeNzbFS()}
	backing.addFile("movie.mkv", 1024)
	file := &File{nzbfs: backing, path: "movie.mkv", logger: slog.Default()}

	reqCtx, cancel := context.WithCancel(context.Background())
	handle, _, errno := file.Open(reqCtx, syscall.O_RDONLY)
	require.Equal(t, syscall.Errno(0), errno)
	defer handle.(*Handle).Release(context.Background())

	cancel()
	require.Len(t, backing.ctxs, 1)
	require.NoError(t, backing.ctxs[0].Err(), "handle context must not follow the open request's cancellation")
}

func TestFileOpenFailureRemovesDownloadStream(t *testing.T) {
	backing := &openContextFS{fakeNzbFS: newFakeNzbFS(), err: context.Canceled}
	backing.addFile("movie.mkv", 1024)
	tracker := &handleStreamTracker{streams: make(map[string]bool)}
	file := &File{nzbfs: backing, path: "movie.mkv", logger: slog.Default(), streamTracker: tracker}
	handle, _, errno := file.Open(context.Background(), syscall.O_RDONLY)
	require.Nil(t, handle)
	require.Equal(t, syscall.EINTR, errno)
	require.Len(t, tracker.streams, 1)
	require.NotEmpty(t, backing.ids[0])
	require.False(t, tracker.streams[backing.ids[0]], "failed open must not leave an active stream")
}
