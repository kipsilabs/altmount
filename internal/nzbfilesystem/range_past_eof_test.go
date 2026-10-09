package nzbfilesystem

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/kipsilabs/altmount/internal/testsupport/segments"
	"github.com/kipsilabs/altmount/internal/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newRangeTestMVF builds a plain file whose ctx carries the given HTTP Range
// header (as the WebDAV adapter does) and whose health persistence is real,
// so a corruption verdict would leave a visible record.
func newRangeTestMVF(t *testing.T, rangeHeader string, n, segSize int) (*MetadataVirtualFile, func() bool) {
	t.Helper()
	repo, _, ms := setupStreamHealthEnv(t)
	ctx := context.WithValue(context.Background(), utils.RangeKey, rangeHeader)
	fp := fakepool.New()
	configurePoolForFile(fp, n, segSize, fakepool.SegmentBehavior{})
	mvf := newTestMVF(t, ctx, fp, n, segSize, 4)
	cfg := config.DefaultConfig()
	mvf.healthRepository = repo
	mvf.metadataService = ms
	mvf.configGetter = func() *config.Config { return cfg }
	// Force Range-header parsing on first read (production handles start
	// with the zero value; the shared helper presets unbounded).
	mvf.originalRangeEnd = 0

	recorded := func() bool {
		fh, err := repo.GetFileHealth(context.Background(), mvf.name)
		require.NoError(t, err)
		return fh != nil
	}
	return mvf, recorded
}

func TestRangeEndPastEOFIsClampedNotCorrupted(t *testing.T) {
	const n, segSize = 8, 64 << 10
	fileSize := int64(n * segSize)
	start := int64(6*segSize + 100)
	mvf, recorded := newRangeTestMVF(t, fmt.Sprintf("bytes=%d-%d", start, fileSize+1_000_000), n, segSize)

	_, err := mvf.Seek(start, io.SeekStart)
	require.NoError(t, err)

	got, err := io.ReadAll(mvf)
	var corrupted *CorruptedFileError
	require.False(t, errors.As(err, &corrupted), "range past EOF must not be a corruption verdict: %v", err)
	require.NoError(t, err)

	want := segments.FileBytes(n, segSize)[start:]
	assert.True(t, bytes.Equal(got, want), "got %d bytes, want %d", len(got), len(want))
	assert.False(t, recorded(), "no health record must be written for a legal range")
}

func TestRangeStartAtOrPastEOFIsEOFNotCorrupted(t *testing.T) {
	const n, segSize = 4, 64 << 10
	fileSize := int64(n * segSize)
	mvf, recorded := newRangeTestMVF(t, fmt.Sprintf("bytes=%d-%d", fileSize, fileSize+10), n, segSize)

	_, err := mvf.Seek(fileSize, io.SeekStart)
	require.NoError(t, err)

	// An explicit Range whose start sits at/past EOF is unsatisfiable:
	// 416 upstream (ErrInvalidRange), never a corruption verdict and never
	// silent full-file bytes.
	buf := make([]byte, 16)
	n0, err := mvf.Read(buf)
	require.ErrorIs(t, err, ErrInvalidRange)
	assert.Equal(t, 0, n0)
	var corrupted *CorruptedFileError
	require.False(t, errors.As(err, &corrupted), "unsatisfiable range must not be a corruption verdict: %v", err)
	assert.False(t, recorded(), "no health record must be written for an unsatisfiable range")
}

func TestSuffixRangeReadsFromPositionNotRangeStart(t *testing.T) {
	const n, segSize = 8, 64 << 10
	fileSize := int64(n * segSize)
	const tail = 65_536
	mvf, recorded := newRangeTestMVF(t, fmt.Sprintf("bytes=-%d", tail), n, segSize)

	// The shared Read path honors the seek position: a suffix Range header
	// parsed at open must still serve from the current position, matching the
	// pre-fix behavior for `bytes=0-` style headers.
	_, err := mvf.Seek(0, io.SeekStart)
	require.NoError(t, err)

	got, err := io.ReadAll(mvf)
	var corrupted *CorruptedFileError
	require.False(t, errors.As(err, &corrupted), "suffix range must not be a corruption verdict: %v", err)
	require.NoError(t, err)

	want := segments.FileBytes(n, segSize)[fileSize-tail:]
	assert.Equal(t, len(want), len(got), "suffix read serves the tail window")
	assert.True(t, bytes.Equal(got, want))
	assert.False(t, recorded(), "no health record must be written for a suffix range")
}

func TestSuffixRangeLargerThanFileServesWholeFile(t *testing.T) {
	const n, segSize = 4, 64 << 10
	fileSize := int64(n * segSize)
	mvf, recorded := newRangeTestMVF(t, fmt.Sprintf("bytes=-%d", fileSize+1_000_000), n, segSize)

	got, err := io.ReadAll(mvf)
	var corrupted *CorruptedFileError
	require.False(t, errors.As(err, &corrupted), "oversized suffix range must not be a corruption verdict: %v", err)
	require.NoError(t, err)

	want := segments.FileBytes(n, segSize)
	assert.True(t, bytes.Equal(got, want), "got %d bytes, want %d", len(got), len(want))
	assert.False(t, recorded(), "no health record must be written for an oversized suffix range")
}

func TestOpenEndedRangeServesFromStart(t *testing.T) {
	const n, segSize = 8, 64 << 10
	start := int64(6*segSize + 100)
	mvf, recorded := newRangeTestMVF(t, fmt.Sprintf("bytes=%d-", start), n, segSize)

	_, err := mvf.Seek(start, io.SeekStart)
	require.NoError(t, err)

	got, err := io.ReadAll(mvf)
	var corrupted *CorruptedFileError
	require.False(t, errors.As(err, &corrupted), "open-ended range must not be a corruption verdict: %v", err)
	require.NoError(t, err)

	want := segments.FileBytes(n, segSize)[start:]
	assert.True(t, bytes.Equal(got, want), "got %d bytes, want %d", len(got), len(want))
	assert.False(t, recorded(), "no health record must be written for an open-ended range")
}

func TestCreateUsenetReaderRejectsNegativeStartWithoutCorruption(t *testing.T) {
	const n, segSize = 4, 64 << 10
	mvf, recorded := newRangeTestMVF(t, "bytes=0-", n, segSize)

	_, err := mvf.createUsenetReader(context.Background(), -1, 65535)
	require.ErrorIs(t, err, ErrInvalidRange)
	var corrupted *CorruptedFileError
	require.False(t, errors.As(err, &corrupted), "negative start must not be a corruption verdict")
	assert.False(t, recorded(), "no health record must be written for an invalid range")
}

func TestUnsatisfiableRangeReturns416NotCorruption(t *testing.T) {
	const n, segSize = 4, 64 << 10
	fileSize := int64(n * segSize)
	mvf, recorded := newRangeTestMVF(t, fmt.Sprintf("bytes=%d-", fileSize+100), n, segSize)
	// Seek to EOF is legal; the read must surface ErrInvalidRange (416
	// upstream), not a corruption verdict and not full-file bytes.
	_, err := mvf.Seek(fileSize, io.SeekStart)
	require.NoError(t, err)
	buf := make([]byte, 16)
	n0, err := mvf.Read(buf)
	require.ErrorIs(t, err, ErrInvalidRange)
	assert.Equal(t, 0, n0)
	var corrupted *CorruptedFileError
	require.False(t, errors.As(err, &corrupted), "unsatisfiable range must not be a corruption verdict")
	assert.False(t, recorded(), "no health record must be written for an unsatisfiable range")
}
