package health

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countingPatchIndex counts Has calls (each one is a hash plus a stat in the
// real store).
type countingPatchIndex struct {
	calls   atomic.Int64
	patched map[string]bool
}

func (c *countingPatchIndex) Has(id string) bool {
	c.calls.Add(1)
	return c.patched[id]
}

// A scheduled check with no adviser and no observer must do per-segment patch
// lookups only for the segments it samples, as it did before the peer network.
func TestPrepareCheck_NoPerSegmentPatchLookupsWithoutConsumers(t *testing.T) {
	env := newHoleTestEnv(t, "big.mp4", 8*1024*1024, 1024) // 8192 segments
	checkAll := false
	env.cfg.Health.CheckAllSegments = &checkAll
	env.cfg.Health.SegmentSamplePercentage = 5
	idx := &countingPatchIndex{}
	env.checker.SetPatchIndex(idx)

	event := env.checker.CheckFile(context.Background(), env.filePath, CheckOptions{PeerFiles: map[string]PeerFileInfo{}})
	require.Equal(t, EventTypeFileHealthy, event.Type, "err: %v", event.Error)
	require.NotNil(t, event.Stats)
	assert.Equal(t, 8192, event.Stats.TotalSegments)
	assert.LessOrEqual(t, idx.calls.Load(), int64(8192*5/100+16),
		"patch lookups must be bounded by the sample, not the file's segment count")
}

// With a healthy observer installed the patch summary is computed, but only
// for a healthy result, and without it PatchedByPAR2 stays false.
func TestCheckStats_PatchSummaryOnlyWithObserver(t *testing.T) {
	env := newHoleTestEnv(t, "p.mp4", 1024*1024, 1024)
	idx := &countingPatchIndex{patched: map[string]bool{env.segIDs[7]: true}}
	env.checker.SetPatchIndex(idx)
	// Patched segment 7 is served locally, so the sweep itself looks it up too.
	ev := env.checker.CheckFile(context.Background(), env.filePath)
	require.Equal(t, EventTypeFileHealthy, ev.Type, "err: %v", ev.Error)
	assert.False(t, ev.Stats.PatchedByPAR2, "not computed without a consumer")
	assert.False(t, ev.Stats.PatchKnown, "an uncomputed summary must not read as 'not patched'")

	env.checker.patchStats.Store(true)
	ev = env.checker.CheckFile(context.Background(), env.filePath)
	assert.True(t, ev.Stats.PatchedByPAR2)
	assert.True(t, ev.Stats.PatchKnown)
}

// An adviser alone costs nothing for files that are not in the scheduled
// cycle's PeerFiles; the patch scan runs only for an eligible, listed file.
func TestPeerAdviserScansPatchesOnlyForEligibleFiles(t *testing.T) {
	env := sampledEnv(t)
	idx := &countingPatchIndex{}
	env.checker.SetPatchIndex(idx)
	env.checker.SetPeerAdviser(&fakeAdviser{advice: PeerAdvice{Kind: PeerAdviceNone}})
	sample := int64(bigFileSegments / 2)

	env.checker.CheckFile(context.Background(), env.filePath) // manual: no PeerFiles
	assert.LessOrEqual(t, idx.calls.Load(), sample+16, "not a scheduled check: no scan")

	idx.calls.Store(0)
	opts := autoOpts(env, database.HealthStatusPending) // listed but not healthy
	env.checker.CheckFile(context.Background(), env.filePath, opts)
	assert.LessOrEqual(t, idx.calls.Load(), sample+16, "ineligible file: no scan")

	idx.calls.Store(0)
	env.checker.CheckFile(context.Background(), env.filePath, autoOpts(env, database.HealthStatusHealthy))
	assert.Greater(t, idx.calls.Load(), sample+16, "an eligible file needs to know whether it is patched")
}

// A scan cut short by its context is unknown, which reads as patched.
func TestAnyPatchedStopsWithItsContext(t *testing.T) {
	env := newHoleTestEnv(t, "c.mp4", 4*1024*1024, 1024)
	idx := &countingPatchIndex{}
	env.checker.SetPatchIndex(idx)
	meta, err := env.ms.ReadFileMetadata(env.filePath)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.True(t, env.checker.anyPatched(ctx, meta.SegmentData))
	assert.Zero(t, idx.calls.Load())
}
