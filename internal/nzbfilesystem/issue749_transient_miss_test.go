package nzbfilesystem

import (
	"context"
	"testing"

	"github.com/javi11/nntppool/v5"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/metadata"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/kipsilabs/altmount/internal/testsupport/segments"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// issue749_transient_miss_test.go reproduces the report end to end, through
// the real FUSE read path rather than by calling the health handler:
//
//	"I have a file that altmount complained corrupted when playing half way.
//	 But then when the file is re-added, the file can be added and in healthy
//	 condition. I am also able to continue the playback."
//
// A provider answers 430 for a segment that is still there. Before the fix the
// read failed, the metadata moved to the corrupted safety folder, and the ARR
// was asked to redownload a healthy release.

// writeMetaForTestFile persists metadata matching what newTestMVF builds in
// memory, so the "was it moved to the corrupted folder?" assertions below have
// a real file to check.
func writeMetaForTestFile(t *testing.T, ms *metadata.MetadataService, filePath string, n, segSize int) {
	t.Helper()
	meta := ms.CreateFileMetadata(
		int64(n*segSize), "test.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY,
		buildSegmentData(t, n, segSize), metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "",
	)
	require.NoError(t, ms.WriteFileMetadata(filePath, meta))
}

// TestIssue749_TransientMissDoesNotBreakPlaybackOrCondemnTheFile: playback
// must survive and the file must be left alone.
func TestIssue749_TransientMissDoesNotBreakPlaybackOrCondemnTheFile(t *testing.T) {
	const (
		segCount = 4
		segSize  = 1024
	)

	repo, db, ms := setupStreamHealthEnv(t)
	ctx := context.Background()

	filePath := "series/transient.s01e03.mkv"
	_, err := db.Exec(
		`INSERT INTO file_health (file_path, library_path, status, scheduled_check_at, streaming_failure_count)
		 VALUES (?, ?, 'healthy', datetime('now'), 0)`,
		filePath, "/media/library/transient.s01e03.mkv",
	)
	require.NoError(t, err)
	writeMetaForTestFile(t, ms, filePath, segCount, segSize)

	fp := fakepool.New()
	configurePoolForFile(fp, segCount, segSize, fakepool.SegmentBehavior{})
	// Segment 2 answers 430 once — a desynced backend — then serves normally,
	// like the release that verified clean on re-import.
	fp.SetBehavior(segments.MessageID(2), fakepool.SegmentBehavior{
		Bytes:     segments.Payload(2, segSize),
		FailFirst: 1,
		FailErr:   nntppool.ErrArticleNotFound,
	})

	healthEnabled := true
	maskingEnabled := true
	cfg := config.DefaultConfig()
	cfg.Health.Enabled = &healthEnabled
	cfg.Streaming.FailureMasking.Enabled = &maskingEnabled
	cfg.Streaming.FailureMasking.Threshold = 3
	cfg.MountPath = ""

	mvf := withConfig(newTestMVF(t, ctx, fp, segCount, segSize, 2), cfg)
	mvf.name = filePath
	mvf.healthRepository = repo
	mvf.metadataService = ms
	// Clip boundaries keep the file ineligible for hole padding, so a miss
	// fails the read instead of being zero-filled.
	mvf.meta.ClipBoundaries = []*metapb.ClipBoundary{{}}

	buf := make([]byte, segCount*segSize)
	n, readErr := mvf.ReadAtContext(ctx, buf, 0)

	require.NoError(t, readErr, "playback must survive a transient 430")
	require.Equal(t, len(buf), n, "the whole range must be served")

	var want []byte
	for i := range segCount {
		want = append(want, segments.Payload(i, segSize)...)
	}
	assert.Equal(t, want, buf, "the recovered segment must carry its real bytes, not zeros")

	fh, err := repo.GetFileHealth(ctx, filePath)
	require.NoError(t, err)
	require.NotNil(t, fh)
	assert.Equal(t, database.HealthStatusHealthy, fh.Status,
		"a transient miss must leave the file healthy")
	assert.Zero(t, fh.StreamingFailureCount,
		"a transient miss must not count toward masking, which never resets itself")

	meta, readMetaErr := ms.ReadFileMetadata(filePath)
	require.NoError(t, readMetaErr)
	assert.NotNil(t, meta, "metadata must not be moved to the corrupted safety folder")
}

// TestIssue749_PermanentMissStillCondemns is the other half: the fix must not
// buy transient-miss tolerance by making real corruption invisible.
func TestIssue749_PermanentMissStillCondemns(t *testing.T) {
	const (
		segCount = 4
		segSize  = 1024
	)

	repo, db, ms := setupStreamHealthEnv(t)
	ctx := context.Background()

	filePath := "series/permanent.s01e04.mkv"
	_, err := db.Exec(
		`INSERT INTO file_health (file_path, library_path, status, scheduled_check_at, streaming_failure_count)
		 VALUES (?, ?, 'healthy', datetime('now'), 0)`,
		filePath, "/media/library/permanent.s01e04.mkv",
	)
	require.NoError(t, err)
	writeMetaForTestFile(t, ms, filePath, segCount, segSize)

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
	mvf.name = filePath
	mvf.healthRepository = repo
	mvf.metadataService = ms
	mvf.meta.ClipBoundaries = []*metapb.ClipBoundary{{}}

	buf := make([]byte, segCount*segSize)
	_, readErr := mvf.ReadAtContext(ctx, buf, 0)
	require.Error(t, readErr, "a genuinely missing article must still fail the read")

	fh, err := repo.GetFileHealth(ctx, filePath)
	require.NoError(t, err)
	require.NotNil(t, fh)
	assert.Equal(t, 1, fh.StreamingFailureCount,
		"a confirmed miss must still count toward masking")
}
