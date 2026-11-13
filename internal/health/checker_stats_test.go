package health

import (
	"context"
	"errors"
	"testing"

	"github.com/kipsilabs/altmount/internal/holes"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/par2repair"
	"github.com/kipsilabs/altmount/internal/usenet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckStats_HealthyFullCheck(t *testing.T) {
	for _, batch := range []bool{false, true} {
		env := newHoleTestEnv(t, "full.mp4", 4*1024*1024, 1024)
		var event HealthEvent
		if batch {
			event = env.checker.CheckFilesBatch(context.Background(), []string{env.filePath}, nil)[0]
		} else {
			event = env.checker.CheckFile(context.Background(), env.filePath)
		}
		require.Equal(t, EventTypeFileHealthy, event.Type)
		require.NotNil(t, event.Stats)
		assert.Equal(t, 4096, event.Stats.TotalSegments)
		assert.Equal(t, 4096, event.Stats.SegmentsChecked)
		assert.Zero(t, event.Stats.MissingCount)
		assert.True(t, event.Stats.FullCheck)
		assert.False(t, event.Stats.MediaHeaderVerified, "no content probe ran")
		assert.False(t, event.Stats.PatchedByPAR2)
		assert.False(t, event.Stats.KnownHoles)
	}
}

func TestCheckStats_SampledCheckIsNotFull(t *testing.T) {
	env := newHoleTestEnv(t, "sampled.mp4", 4*1024*1024, 1024)
	checkAll := false
	env.cfg.Health.CheckAllSegments = &checkAll
	env.cfg.Health.SegmentSamplePercentage = 10

	event := env.checker.CheckFile(context.Background(), env.filePath)
	require.Equal(t, EventTypeFileHealthy, event.Type)
	require.NotNil(t, event.Stats)
	assert.Equal(t, 4096, event.Stats.TotalSegments)
	assert.Positive(t, event.Stats.SegmentsChecked)
	assert.Less(t, event.Stats.SegmentsChecked, 4096)
	assert.False(t, event.Stats.FullCheck)
}

func TestCheckStats_PlaceholdersAreNotSegments(t *testing.T) {
	env := newHoleTestEnv(t, "gap.mp4", 4*1024, 1024)
	require.NoError(t, env.ms.UpdateFileMetadata(env.filePath, func(m *metapb.FileMetadata) {
		m.SegmentData[1].Id = holes.PlaceholderID(1, "t")
	}))
	event := env.checker.CheckFile(context.Background(), env.filePath)
	require.Equal(t, EventTypeFileHealthy, event.Type, "err: %v", event.Error)
	require.NotNil(t, event.Stats)
	assert.Equal(t, 3, event.Stats.TotalSegments, "gap placeholders are not real segments")
	assert.Equal(t, 3, event.Stats.SegmentsChecked)
	assert.True(t, event.Stats.FullCheck)
}

func TestCheckStats_Corrupted(t *testing.T) {
	env := newHoleTestEnv(t, "bad.mp4", 4*1024*1024, 1024)
	env.markSegmentMissing(10)
	env.markSegmentMissing(11)

	event := env.checker.CheckFile(context.Background(), env.filePath)
	require.Equal(t, EventTypeFileCorrupted, event.Type)
	require.NotNil(t, event.Stats)
	assert.Equal(t, 4096, event.Stats.TotalSegments)
	assert.Equal(t, 2, event.Stats.MissingCount)
	assert.Equal(t, 4096, event.Stats.SegmentsChecked)
	assert.False(t, event.Stats.MediaHeaderVerified)
}

func TestCheckStats_Inconclusive(t *testing.T) {
	hc := newTestHealthCheckerWithVerifyContentEnabled(&fakeContentOpener{data: matroskaHeader()})
	prep := preparedCheck{filePath: "/movie.mkv", totalSegments: 10, realSegments: 10}
	res := usenet.ValidationResult{TotalChecked: 3, UnresolvedCount: 2, Err: errors.New("timeout")}

	event := hc.judgeValidation(context.Background(), prep, res, nil)
	require.Equal(t, EventTypeCheckInconclusive, event.Type)
	require.NotNil(t, event.Stats)
	assert.Equal(t, 10, event.Stats.TotalSegments)
	assert.Equal(t, 3, event.Stats.SegmentsChecked)
	assert.False(t, event.Stats.FullCheck)
	assert.False(t, event.Stats.MediaHeaderVerified, "the probe never ran")

	event = hc.judgeValidation(context.Background(), prep, usenet.ValidationResult{}, errors.New("pool down"))
	require.Equal(t, EventTypeCheckInconclusive, event.Type)
	require.NotNil(t, event.Stats)
	assert.Zero(t, event.Stats.SegmentsChecked)
}

func TestCheckStats_EarlyEventsCarryNoStats(t *testing.T) {
	env := newHoleTestEnv(t, "x.mp4", 4*1024, 1024)
	require.NoError(t, env.ms.UpdateFileMetadata(env.filePath, func(m *metapb.FileMetadata) { m.SegmentData = nil }))
	event := env.checker.CheckFile(context.Background(), env.filePath)
	require.Equal(t, EventTypeCheckFailed, event.Type)
	assert.Nil(t, event.Stats)
}

func TestCheckStats_MediaHeaderVerifiedOnlyWhenProbePassedInThisCheck(t *testing.T) {
	full := usenet.ValidationResult{TotalChecked: 5}
	prep := func(override *bool) preparedCheck {
		return preparedCheck{filePath: "/movie.mkv", totalSegments: 5, realSegments: 5, currentStatus: "pending", verifyContentOverride: override}
	}
	on := true

	// Probe ran and passed.
	hc := newTestHealthCheckerWithVerifyContentEnabled(&fakeContentOpener{data: matroskaHeader()})
	ev := hc.judgeValidation(context.Background(), prep(nil), full, nil)
	require.Equal(t, EventTypeFileHealthy, ev.Type)
	require.NotNil(t, ev.Stats)
	assert.True(t, ev.Stats.MediaHeaderVerified)

	// Probe is disabled for a non-pending file: not verified in this check,
	// whatever an earlier check recorded.
	p := prep(nil)
	p.currentStatus = "healthy"
	ev = hc.judgeValidation(context.Background(), p, full, nil)
	require.NotNil(t, ev.Stats)
	assert.False(t, ev.Stats.MediaHeaderVerified)

	// Probe could not complete: unavailable is not verified.
	hc = newTestHealthCheckerWithVerifyContentEnabled(&fakeContentOpener{err: errors.New("connection reset")})
	ev = hc.judgeValidation(context.Background(), prep(&on), full, nil)
	require.Equal(t, EventTypeFileHealthy, ev.Type)
	require.NotNil(t, ev.Stats)
	assert.False(t, ev.Stats.MediaHeaderVerified)

	// Content failure is a corrupted event; it must not claim verification.
	hc = newTestHealthCheckerWithVerifyContentEnabled(&fakeContentOpener{data: make([]byte, 512)})
	ev = hc.judgeValidation(context.Background(), prep(nil), full, nil)
	require.Equal(t, EventTypeFileCorrupted, ev.Type)
	require.NotNil(t, ev.Stats)
	assert.False(t, ev.Stats.MediaHeaderVerified)

	// Not a media file: no probe, no claim.
	hc = newTestHealthCheckerWithVerifyContentEnabled(&fakeContentOpener{data: matroskaHeader()})
	p = prep(nil)
	p.filePath = "/readme.txt"
	ev = hc.judgeValidation(context.Background(), p, full, nil)
	require.NotNil(t, ev.Stats)
	assert.False(t, ev.Stats.MediaHeaderVerified)
}

func TestCheckStats_PatchedAndKnownHoles(t *testing.T) {
	env := newHoleTestEnv(t, "patched.mp4", 4*1024*1024, 1024)
	env.cfg.Health.AcceptableMissingSegmentsPercentage = 1
	env.markSegmentMissing(10)
	store := par2repair.NewPatchStore(t.TempDir())
	require.NoError(t, store.Put(env.segIDs[10], make([]byte, 1024)))
	env.checker.SetPatchIndex(store)
	env.checker.patchStats.Store(true) // a healthy observer is installed

	event := env.checker.CheckFile(context.Background(), env.filePath)
	require.Equal(t, EventTypeFileHealthy, event.Type)
	require.NotNil(t, event.Stats)
	assert.True(t, event.Stats.PatchedByPAR2, "a patch covers one of the file's segments")

	// A patch on a segment the sample never reaches still counts: the check
	// covers the whole file's segment list, not just the sample.
	other := newHoleTestEnv(t, "other.mp4", 4*1024*1024, 1024)
	checkAll := false
	other.cfg.Health.CheckAllSegments = &checkAll
	other.cfg.Health.SegmentSamplePercentage = 1
	patchedIdx := -1
	sampled := map[string]bool{}
	var segs []*metapb.SegmentData
	meta, err := other.ms.ReadFileMetadata(other.filePath)
	require.NoError(t, err)
	segs = meta.SegmentData
	for _, s := range usenet.SelectSegmentsForValidation(segs, 1) {
		sampled[s.Id] = true
	}
	for i, id := range other.segIDs {
		if !sampled[id] {
			patchedIdx = i
			break
		}
	}
	require.GreaterOrEqual(t, patchedIdx, 0)
	st2 := par2repair.NewPatchStore(t.TempDir())
	require.NoError(t, st2.Put(other.segIDs[patchedIdx], make([]byte, 1024)))
	other.checker.SetPatchIndex(st2)
	other.checker.patchStats.Store(true)
	event = other.checker.CheckFile(context.Background(), other.filePath)
	require.Equal(t, EventTypeFileHealthy, event.Type)
	require.NotNil(t, event.Stats)
	assert.True(t, event.Stats.PatchedByPAR2)
	assert.False(t, event.Stats.FullCheck)

	// Known holes persisted by earlier sweeps are reported.
	holed := newHoleTestEnv(t, "holed.mp4", 4*1024*1024, 1024)
	require.NoError(t, holed.ms.AddKnownHoles(holed.filePath, []holes.Run{{Start: 5, Count: 1}}, holed.cfg.ProviderFingerprint()))
	event = holed.checker.CheckFile(context.Background(), holed.filePath)
	require.Equal(t, EventTypeFileHealthy, event.Type)
	require.NotNil(t, event.Stats)
	assert.True(t, event.Stats.KnownHoles)
}
