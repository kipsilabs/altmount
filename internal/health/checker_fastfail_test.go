package health

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"testing"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/javi11/nntppool/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeMultiSegmentFile writes metadata for a file made of segCount segments
// and returns their IDs in order, so tests can make an arbitrary segment miss.
func writeMultiSegmentFile(t *testing.T, env *repairTestEnv, filePath string, segCount int) []string {
	t.Helper()
	const segSize = int64(1024)

	segs := make([]*metapb.SegmentData, segCount)
	ids := make([]string, segCount)
	for i := range segs {
		ids[i] = fmt.Sprintf("seg-%s-%d@test.example.com", filePath, i)
		segs[i] = &metapb.SegmentData{
			Id:          ids[i],
			SegmentSize: segSize,
			StartOffset: 0,
			EndOffset:   segSize - 1,
		}
	}
	meta := env.metadataService.CreateFileMetadata(
		segSize*int64(segCount), "test.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY,
		segs, metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "",
	)
	require.NoError(t, env.metadataService.WriteFileMetadata(filePath, meta))
	return ids
}

func parseDetails(t *testing.T, ev HealthEvent) database.HealthErrorDetails {
	t.Helper()
	require.NotNil(t, ev.Details, "event carries no error details")
	var d database.HealthErrorDetails
	require.NoError(t, json.Unmarshal([]byte(*ev.Details), &d))
	return d
}

// TestCheckFilesBatch_TransportErrorIsNotCorruption is the correctness fix the
// issue calls for: a connection failure must neither condemn a file nor let it
// pass as healthy, and must not consume a retry either — it proves nothing,
// so the existing retry ladder must not advance on its account (#861).
func TestCheckFilesBatch_TransportErrorIsNotCorruption(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks not supported on Windows")
	}

	client := fakepool.New()
	env := newBatchTestEnv(t, t.TempDir(), client)

	paths := []string{"complete/flaky.mkv", "complete/fine.mkv"}
	flakyID := writeHealthyFile(t, env, paths[0])
	writeHealthyFile(t, env, paths[1])
	client.SetBehavior(flakyID, fakepool.SegmentBehavior{Err: nntppool.ErrConnectionDied})

	events := env.healthChecker.CheckFilesBatch(context.Background(), paths, nil)
	require.Len(t, events, 2)

	assert.Equal(t, EventTypeCheckInconclusive, events[0].Type,
		"a transport error proves nothing, so it must not be read as corruption or burn a retry")
	assert.NotEqual(t, EventTypeFileHealthy, events[0].Type,
		"a segment we never resolved must not pass as available")
	assert.Equal(t, EventTypeFileHealthy, events[1].Type, "sibling unaffected")
}

// TestCheckFilesBatch_TransportErrorPersistsNoHoles guards the latent bug the
// old any-error-is-missing classification caused: phantom misses could reach a
// degraded verdict and be written into the file's hole map, after which
// playback zero-fills bytes that were never actually missing.
func TestCheckFilesBatch_TransportErrorPersistsNoHoles(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks not supported on Windows")
	}

	client := fakepool.New()
	env := newBatchTestEnv(t, t.TempDir(), client, func(c *config.Config) {
		checkAll := true
		c.Health.CheckAllSegments = &checkAll
		c.Health.AcceptableMissingSegmentsPercentage = 100 // never fail on misses alone
	})

	const path = "complete/blip.mkv"
	ids := writeMultiSegmentFile(t, env, path, 20)
	client.SetBehavior(ids[3], fakepool.SegmentBehavior{Err: nntppool.ErrConnectionDied})

	events := env.healthChecker.CheckFilesBatch(context.Background(), []string{path}, nil)
	require.Len(t, events, 1)
	assert.Equal(t, EventTypeCheckInconclusive, events[0].Type)

	meta, err := env.metadataService.ReadFileMetadata(path)
	require.NoError(t, err)
	require.NotNil(t, meta)
	assert.Empty(t, meta.KnownHoles, "a transport blip must never be recorded as a hole")
}

// TestCheckFilesBatch_FastFailStopsDoomedFileOnly is the feature end to end: a
// file that has already exceeded its threshold stops consuming stats while its
// batch siblings are swept in full.
func TestCheckFilesBatch_FastFailStopsDoomedFileOnly(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks not supported on Windows")
	}

	const segCount = 20
	client := fakepool.New()
	env := newBatchTestEnv(t, t.TempDir(), client, func(c *config.Config) {
		checkAll := true
		c.Health.CheckAllSegments = &checkAll
		c.Health.AcceptableMissingSegmentsPercentage = 0 // zero tolerance
	})

	paths := []string{"complete/doomed.mkv", "complete/fine.mkv"}
	doomedIDs := writeMultiSegmentFile(t, env, paths[0], segCount)
	writeMultiSegmentFile(t, env, paths[1], segCount)
	client.SetBehavior(doomedIDs[0], fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})

	events := env.healthChecker.CheckFilesBatch(context.Background(), paths, nil)
	require.Len(t, events, 2)

	assert.Equal(t, EventTypeFileCorrupted, events[0].Type)
	assert.Equal(t, EventTypeFileHealthy, events[1].Type)

	doomed := parseDetails(t, events[0])
	assert.True(t, doomed.TerminatedEarly, "doomed file records a partial check")
	assert.Equal(t, 1, doomed.MissingArticles)
	assert.Equal(t, segCount, doomed.TotalArticles)
	assert.Less(t, doomed.Sampled, segCount, "stopped short of a full sweep")

	assert.Less(t, client.StatCalls(), int64(2*segCount),
		"fast-fail must save stat calls versus sweeping both files in full")
}

// TestCheckFilesBatch_NoFastFailWhenToleranceUnreached proves the threshold is
// respected rather than any miss terminating the sweep: with tolerance above
// the observed miss rate the file is swept in full, preserving the hole map.
func TestCheckFilesBatch_NoFastFailWhenToleranceUnreached(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks not supported on Windows")
	}

	const segCount = 20
	client := fakepool.New()
	env := newBatchTestEnv(t, t.TempDir(), client, func(c *config.Config) {
		checkAll := true
		c.Health.CheckAllSegments = &checkAll
		c.Health.AcceptableMissingSegmentsPercentage = 50 // 1/20 = 5%, well under
	})

	const path = "complete/tolerated.mkv"
	ids := writeMultiSegmentFile(t, env, path, segCount)
	client.SetBehavior(ids[0], fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})

	events := env.healthChecker.CheckFilesBatch(context.Background(), []string{path}, nil)
	require.Len(t, events, 1)

	d := parseDetails(t, events[0])
	assert.False(t, d.TerminatedEarly)
	assert.Equal(t, segCount, d.Sampled, "every segment was checked")
	assert.Equal(t, int64(segCount), client.StatCalls())
}

// TestCheckFile_FastFailParityWithBatch keeps the manual single-file check on
// the same semantics as the batch sweep.
func TestCheckFile_FastFailParityWithBatch(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks not supported on Windows")
	}

	const segCount = 20
	client := fakepool.New()
	env := newBatchTestEnv(t, t.TempDir(), client, func(c *config.Config) {
		checkAll := true
		c.Health.CheckAllSegments = &checkAll
		c.Health.AcceptableMissingSegmentsPercentage = 0
	})

	const path = "complete/solo.mkv"
	ids := writeMultiSegmentFile(t, env, path, segCount)
	client.SetBehavior(ids[0], fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})

	event := env.healthChecker.CheckFile(context.Background(), path)
	assert.Equal(t, EventTypeFileCorrupted, event.Type)
	assert.True(t, parseDetails(t, event).TerminatedEarly)
	assert.Less(t, client.StatCalls(), int64(segCount))
}
