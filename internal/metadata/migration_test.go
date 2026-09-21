package metadata

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/config"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testConfigGetter(t *testing.T) config.ConfigGetter {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "altmount.db")
	cfg := &config.Config{}
	cfg.Database.Path = dbPath
	cfg.Metadata.Migration.DefaultGroup = "alt.binaries.misc"
	return func() *config.Config { return cfg }
}

func seedLegacyRelease(t *testing.T, ms *MetadataService) {
	t.Helper()
	require.NoError(t, ms.WriteFileMetadata(filepath.Join("movies", "A.mkv"), &metapb.FileMetadata{
		FileSize: 200, SourceNzbPath: "/nzbs/rel.nzb",
		SegmentData: []*metapb.SegmentData{
			{Id: "s1@n", SegmentSize: 100, EndOffset: 99},
			{Id: "s2@n", SegmentSize: 100, EndOffset: 99},
		},
	}))
	require.NoError(t, ms.WriteFileMetadata(filepath.Join("movies", "B.mkv"), &metapb.FileMetadata{
		FileSize: 100, SourceNzbPath: "/nzbs/rel.nzb",
		SegmentData: []*metapb.SegmentData{{Id: "s3@n", SegmentSize: 100, EndOffset: 99}},
	}))
}

func TestMigrationWorker_DryRunLeavesLibraryUntouched(t *testing.T) {
	root := t.TempDir()
	ms := NewMetadataService(root)
	seedLegacyRelease(t, ms)

	w := NewMigrationWorker(ms, testConfigGetter(t))
	res, err := w.DryRun(context.Background())
	require.NoError(t, err)
	require.NotNil(t, res)

	assert.True(t, res.DryRun)
	assert.Equal(t, 1, res.Groups)
	assert.Equal(t, 2, res.FilesMigrated)
	assert.Equal(t, 0, res.FilesFailed)
	assert.Positive(t, res.BytesBefore)
	assert.Positive(t, res.BytesAfter)

	groups, err := ms.ScanLegacyMetas()
	require.NoError(t, err)
	require.Len(t, groups, 1)
	assert.Len(t, groups[0].Files, 2)

	status := w.GetStatus()
	assert.False(t, status.IsRunning)
	require.NotNil(t, status.LastDryRun)
	assert.Nil(t, status.LastResult, "a dry run is not a migration")
}

func TestMigrationWorker_StartMigratesEverything(t *testing.T) {
	root := t.TempDir()
	ms := NewMetadataService(root)
	seedLegacyRelease(t, ms)

	w := NewMigrationWorker(ms, testConfigGetter(t))
	require.NoError(t, w.Start(context.Background()))

	require.Eventually(t, func() bool {
		return !w.GetStatus().IsRunning && w.GetStatus().LastResult != nil
	}, 10*time.Second, 20*time.Millisecond)

	res := w.GetStatus().LastResult
	require.NotNil(t, res)
	assert.False(t, res.DryRun)
	assert.Equal(t, 2, res.FilesMigrated)
	assert.Equal(t, 0, res.FilesFailed)
	assert.Equal(t, 1, res.SynthesizedGroups)
	assert.Equal(t, 0, res.FaithfulGroups)

	groups, err := ms.ScanLegacyMetas()
	require.NoError(t, err)
	assert.Empty(t, groups, "nothing legacy is left")

	got, err := ms.ReadFileMetadata(filepath.Join("movies", "A.mkv"))
	require.NoError(t, err)
	require.Len(t, got.SegmentData, 2)
	assert.Equal(t, "s1@n", got.SegmentData[0].Id)
}

func TestMigrationWorker_RejectsConcurrentRuns(t *testing.T) {
	root := t.TempDir()
	ms := NewMetadataService(root)
	w := NewMigrationWorker(ms, testConfigGetter(t))

	w.mu.Lock()
	w.running = true
	w.mu.Unlock()

	err := w.Start(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already running")

	_, dryErr := w.DryRun(context.Background())
	require.Error(t, dryErr)
}

// seedLargeLegacyRelease writes a release whose metas carry enough inline
// segments that the shared store measurably shrinks the library.
func seedLargeLegacyRelease(t *testing.T, ms *MetadataService) {
	t.Helper()
	for _, name := range []string{"A.mkv", "B.mkv"} {
		segs := make([]*metapb.SegmentData, 0, 400)
		for i := range 400 {
			segs = append(segs, &metapb.SegmentData{
				Id: fmt.Sprintf("%032x@%s", i, name), SegmentSize: 700000, EndOffset: 699999,
			})
		}
		require.NoError(t, ms.WriteFileMetadata(filepath.Join("movies", name), &metapb.FileMetadata{
			FileSize: 400 * 700000, SourceNzbPath: "/nzbs/rel.nzb", SegmentData: segs,
		}))
	}
}

// A dry run converts into a throwaway root, so its projected size must be
// measured there. Measuring the untouched legacy metas instead reports the
// legacy bytes plus the new store, which can never be smaller than "before".
func TestMigrationWorker_DryRunProjectsRealMigrationSize(t *testing.T) {
	real := NewMetadataService(t.TempDir())
	seedLargeLegacyRelease(t, real)
	realWorker := NewMigrationWorker(real, testConfigGetter(t))
	require.NoError(t, realWorker.Start(context.Background()))
	require.Eventually(t, func() bool {
		return !realWorker.GetStatus().IsRunning && realWorker.GetStatus().LastResult != nil
	}, 10*time.Second, 20*time.Millisecond)
	realRes := realWorker.GetStatus().LastResult

	dry := NewMetadataService(t.TempDir())
	seedLargeLegacyRelease(t, dry)
	dryRes, err := NewMigrationWorker(dry, testConfigGetter(t)).DryRun(context.Background())
	require.NoError(t, err)

	assert.Equal(t, realRes.BytesBefore, dryRes.BytesBefore)
	assert.Less(t, dryRes.BytesAfter, dryRes.BytesBefore, "moving segments into one store must project a shrink")
	// The v3 meta embeds its store path, whose length differs between the real
	// config dir and the dry-run temp root; allow that much slack and no more.
	assert.InDelta(t, realRes.BytesAfter, dryRes.BytesAfter, 1024,
		"dry run must measure the converted metas, not the legacy ones")
}
