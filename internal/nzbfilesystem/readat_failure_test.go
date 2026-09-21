package nzbfilesystem

import (
	"context"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/kipsilabs/altmount/internal/testsupport/segments"
	"github.com/javi11/nntppool/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// readat_failure_test.go pins the two safety nets that must hold on the FUSE
// read path (ReadAtContext), as opposed to the WebDAV path (Read):
//
//  1. Read errors must propagate. The sequential branch used to `return n, nil`
//     after breaking out of its read loop, so a permanently missing segment
//     surfaced as a silent short read — the caller saw truncated data and no
//     error, and streaming failure masking never saw a failure to count.
//
//  2. A read must not block forever. Nothing on this path carried a deadline,
//     so a stalled fetch parked the FUSE request indefinitely and left the
//     calling process (e.g. Sonarr's ffprobe) in uninterruptible D-state.

// withConfig attaches a config getter to a test file handle. newTestMVF leaves
// configGetter nil; every path touched here reads config.
func withConfig(mvf *MetadataVirtualFile, cfg *config.Config) *MetadataVirtualFile {
	mvf.configGetter = func() *config.Config { return cfg }
	return mvf
}

// TestReadAtContext_MissingSegment_ReturnsError verifies that a sequential
// ReadAtContext spanning a permanently missing (DMCA'd) segment reports an
// error rather than silently returning a short read.
func TestReadAtContext_MissingSegment_ReturnsError(t *testing.T) {
	const (
		segCount = 4
		segSize  = 1024
	)

	ctx := context.Background()
	fp := fakepool.New()
	configurePoolForFile(fp, segCount, segSize, fakepool.SegmentBehavior{})
	// Segment 2 is gone from every provider — a permanent 430.
	fp.SetBehavior(segments.MessageID(2), fakepool.SegmentBehavior{
		Err: nntppool.ErrArticleNotFound,
	})

	mvf := withConfig(newTestMVF(t, ctx, fp, segCount, segSize, 2), config.DefaultConfig())

	buf := make([]byte, segCount*segSize)
	n, err := mvf.ReadAtContext(ctx, buf, 0)

	require.Error(t, err, "read spanning a permanently missing segment must surface an error")
	assert.Less(t, n, len(buf), "read must be short — segment 2 is unavailable")
}

// TestReadAtContext_MissingSegment_CountsStreamingFailure verifies that the
// FUSE read path feeds streaming failure masking. Previously only Read() (the
// WebDAV path) called updateFileHealthOnError, so `streaming.failure_masking`
// was inert for FUSE mounts and a DMCA'd file was never masked.
func TestReadAtContext_MissingSegment_CountsStreamingFailure(t *testing.T) {
	const (
		segCount = 4
		segSize  = 1024
	)

	repo, db, ms := setupStreamHealthEnv(t)
	ctx := context.Background()

	filePath := "series/masked.s01e06.mkv"
	_, err := db.Exec(
		`INSERT INTO file_health (file_path, library_path, status, scheduled_check_at, streaming_failure_count)
		 VALUES (?, ?, 'healthy', datetime('now'), 0)`,
		filePath, "/media/library/masked.s01e06.mkv",
	)
	require.NoError(t, err)

	fp := fakepool.New()
	configurePoolForFile(fp, segCount, segSize, fakepool.SegmentBehavior{})
	fp.SetBehavior(segments.MessageID(2), fakepool.SegmentBehavior{
		Err: nntppool.ErrArticleNotFound,
	})

	healthEnabled := true
	maskingEnabled := true
	cfg := config.DefaultConfig()
	cfg.Health.Enabled = &healthEnabled
	cfg.Streaming.FailureMasking.Enabled = &maskingEnabled
	cfg.Streaming.FailureMasking.Threshold = 3
	cfg.MountPath = ""

	mvf := withConfig(newTestMVF(t, ctx, fp, segCount, segSize, 2), cfg)
	// Route health updates at the real repository. The file is named .mkv so
	// hole padding would normally apply; clip boundaries keep it ineligible so
	// the miss surfaces as an error instead of being zero-filled.
	mvf.name = filePath
	mvf.healthRepository = repo
	mvf.metadataService = ms
	mvf.meta.ClipBoundaries = []*metapb.ClipBoundary{{}}

	buf := make([]byte, segCount*segSize)
	_, readErr := mvf.ReadAtContext(ctx, buf, 0)
	require.Error(t, readErr)

	fh, err := repo.GetFileHealth(ctx, filePath)
	require.NoError(t, err)
	require.NotNil(t, fh)
	assert.Equal(t, 1, fh.StreamingFailureCount,
		"a failed FUSE read must increment the streaming failure counter")
	assert.Equal(t, database.HealthStatusPending, fh.Status,
		"below the masking threshold the file stays pending, not repair_triggered")
}

// TestReadAtContext_StalledFetch_TimesOut verifies that a fetch which never
// completes and ignores context cancellation still lets ReadAtContext return.
// The fake pool's gate blocks without consulting ctx, reproducing the stall
// that left ffprobe in D-state for over half an hour.
func TestReadAtContext_StalledFetch_TimesOut(t *testing.T) {
	const (
		segCount = 4
		segSize  = 1024
	)

	ctx := context.Background()
	fp := fakepool.New()
	configurePoolForFile(fp, segCount, segSize, fakepool.SegmentBehavior{})
	fp.BlockUntil(make(chan struct{})) // never released, never cancelled

	cfg := config.DefaultConfig()
	cfg.Streaming.ReadTimeoutSeconds = 1

	mvf := withConfig(newTestMVF(t, ctx, fp, segCount, segSize, 2), cfg)

	type result struct {
		n   int
		err error
	}
	done := make(chan result, 1)
	go func() {
		buf := make([]byte, segSize)
		n, err := mvf.ReadAtContext(ctx, buf, 0)
		done <- result{n, err}
	}()

	select {
	case got := <-done:
		require.Error(t, got.err, "a stalled fetch must fail the read, not return success")
		assert.ErrorIs(t, got.err, ErrReadTimeout)
	case <-time.After(20 * time.Second):
		t.Fatal("ReadAtContext hung well past the configured 1s read timeout")
	}
}

// TestReadAtContext_StalledEphemeralFetch_TimesOut covers the seek path. It
// builds a short-lived reader that is never registered for interruption, so it
// relies on the read context carrying the deadline instead. A player scrubbing
// into a stalled region must not hang either.
func TestReadAtContext_StalledEphemeralFetch_TimesOut(t *testing.T) {
	const (
		segCount = 8
		segSize  = 1024
	)

	ctx := context.Background()
	fp := fakepool.New()
	configurePoolForFile(fp, segCount, segSize, fakepool.SegmentBehavior{})
	fp.BlockUntil(make(chan struct{})) // never released, never cancelled

	cfg := config.DefaultConfig()
	cfg.Streaming.ReadTimeoutSeconds = 1

	mvf := withConfig(newTestMVF(t, ctx, fp, segCount, segSize, 2), cfg)

	done := make(chan error, 1)
	go func() {
		buf := make([]byte, segSize)
		// Offset 4*segSize is not the shared reader's next position, so this
		// takes the ephemeral branch.
		_, err := mvf.ReadAtContext(ctx, buf, 4*segSize)
		done <- err
	}()

	select {
	case err := <-done:
		require.Error(t, err, "a stalled seek-driven fetch must fail the read")
		assert.ErrorIs(t, err, ErrReadTimeout)
	case <-time.After(20 * time.Second):
		t.Fatal("ephemeral ReadAtContext hung past the configured 1s read timeout")
	}
}

// TestReadTimeout_Defaults pins the configured/default resolution so a zero
// value cannot silently disable the safety net.
func TestReadTimeout_Defaults(t *testing.T) {
	tests := []struct {
		name    string
		seconds int
		want    time.Duration
	}{
		{"explicit value wins", 30, 30 * time.Second},
		{"zero falls back to the default", 0, defaultStreamReadTimeout},
		{"negative disables the timeout", -1, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.Streaming.ReadTimeoutSeconds = tt.seconds
			assert.Equal(t, tt.want, cfg.GetStreamReadTimeout())
		})
	}
}
