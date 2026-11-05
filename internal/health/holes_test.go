package health

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/holes"
	"github.com/kipsilabs/altmount/internal/metadata"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/par2repair"
	"github.com/kipsilabs/altmount/internal/pool"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/kipsilabs/altmount/internal/usenet"
	"github.com/javi11/nntppool/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakePoolManager is a pool.Manager backed by a fakepool.Client, so health
// checks can stat segments without a network.
type fakePoolManager struct {
	mockPoolManager
	client *fakepool.Client
}

func (m *fakePoolManager) GetPool() (pool.NntpClient, error) { return m.client, nil }
func (m *fakePoolManager) HasPool() bool                     { return true }

// holeTestEnv wires a HealthChecker over a fake pool that stats a video file
// split into fixed-size segments.
type holeTestEnv struct {
	checker  *HealthChecker
	ms       *metadata.MetadataService
	fp       *fakepool.Client
	filePath string
	segIDs   []string
	cfg      *config.Config
	metaRoot string
}

func newHoleTestEnv(t *testing.T, fileName string, fileSize, segSize int64) *holeTestEnv {
	t.Helper()
	tempDir := t.TempDir()
	ms := metadata.NewMetadataService(tempDir)
	fp := fakepool.New()

	var segs []*metapb.SegmentData
	var ids []string
	for off := int64(0); off < fileSize; off += segSize {
		end := min(off+segSize, fileSize)
		id := fmt.Sprintf("hole-seg-%d@test", len(segs))
		fp.SetBehavior(id, fakepool.SegmentBehavior{Bytes: make([]byte, end-off)})
		segs = append(segs, &metapb.SegmentData{
			Id:          id,
			SegmentSize: end - off,
			StartOffset: 0,
			EndOffset:   end - off - 1,
		})
		ids = append(ids, id)
	}

	filePath := "/movies/" + fileName
	meta := ms.CreateFileMetadata(
		fileSize, "test.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY,
		segs, metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "",
	)
	require.NoError(t, ms.WriteFileMetadata(filePath, meta))

	healthEnabled := true
	cfg := config.DefaultConfig()
	cfg.Health.Enabled = &healthEnabled
	cfg.Metadata.RootPath = tempDir
	cfg.Health.MaxConcurrentSegmentChecks = ptr(2)
	checkAll := true
	cfg.Health.CheckAllSegments = &checkAll // deterministic: stat every segment

	checker := NewHealthChecker(
		nil, // healthRepo unused by CheckFile happy paths (no deletes)
		ms,
		&fakePoolManager{client: fp},
		func() *config.Config { return cfg },
		nil,
		nil,
	)

	return &holeTestEnv{
		checker:  checker,
		ms:       ms,
		fp:       fp,
		filePath: filePath,
		segIDs:   ids,
		cfg:      cfg,
		metaRoot: tempDir,
	}
}

// markSegmentMissing makes Stat fail for the segment.
func (e *holeTestEnv) markSegmentMissing(index int) {
	e.fp.SetBehavior(e.segIDs[index], fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
}

func TestHealthCheckCleanFileIsHealthy(t *testing.T) {
	env := newHoleTestEnv(t, "movie.mp4", 4*1024*1024, 1024)

	event := env.checker.CheckFile(context.Background(), env.filePath)
	require.Equal(t, EventTypeFileHealthy, event.Type, "err: %v", event.Error)
	assert.Nil(t, event.Classification)
}

func TestHealthCheckClassifiesSmallHoleAsDegraded(t *testing.T) {
	env := newHoleTestEnv(t, "movie.mp4", 4*1024*1024, 1024)
	// Raise the acceptable-missing threshold so these two isolated misses
	// (well within the pad caps) stay degraded instead of failing outright —
	// see TestHealthCheckAcceptableMissingThresholdTriggersRepair for the
	// zero-tolerance default.
	env.cfg.Health.AcceptableMissingSegmentsPercentage = 1
	env.markSegmentMissing(10)
	env.markSegmentMissing(30)

	event := env.checker.CheckFile(context.Background(), env.filePath)
	require.Equal(t, EventTypeFileCorrupted, event.Type)
	require.NotNil(t, event.Classification)
	assert.Equal(t, holes.VerdictDegraded, event.Classification.Verdict)
	assert.Equal(t, 2, event.Classification.TotalMissing)
	assert.Equal(t, 1, event.Classification.LongestRun)

	// Details envelope round-trips with playback impact.
	require.NotNil(t, event.Details)
	var details database.HealthErrorDetails
	require.NoError(t, json.Unmarshal([]byte(*event.Details), &details))
	assert.Equal(t, "missing_segments", details.ErrorType)
	require.NotNil(t, details.PlaybackImpact)
	assert.Equal(t, holes.VerdictDegraded, details.PlaybackImpact.Verdict)

	// Degraded holes are persisted so playback can pre-pad them.
	meta, err := env.ms.ReadFileMetadata(env.filePath)
	require.NoError(t, err)
	assert.NotEmpty(t, meta.KnownHoles, "degraded holes should be persisted")
}

// TestHealthCheckAcceptableMissingThresholdTriggersRepair covers issue #813:
// with the threshold set to 0 (zero tolerance), even a single confirmed
// missing segment must fail the file (not merely degrade it) so the repair
// path (worker.go) picks it up instead of leaving it degraded forever.
func TestHealthCheckAcceptableMissingThresholdTriggersRepair(t *testing.T) {
	env := newHoleTestEnv(t, "movie.mp4", 4*1024*1024, 1024)
	env.cfg.Health.AcceptableMissingSegmentsPercentage = 0
	// A single isolated missing segment — within the pad caps, so only the
	// acceptable-missing threshold (not the run/total caps) forces the fail.
	env.markSegmentMissing(10)

	event := env.checker.CheckFile(context.Background(), env.filePath)
	require.Equal(t, EventTypeFileCorrupted, event.Type)
	require.NotNil(t, event.Classification)
	assert.Equal(t, holes.VerdictFailed, event.Classification.Verdict)

	// Raising the threshold above the actual missing fraction keeps it degraded.
	env.markSegmentMissing(20)
	env.cfg.Health.AcceptableMissingSegmentsPercentage = 100
	event = env.checker.CheckFile(context.Background(), env.filePath)
	require.NotNil(t, event.Classification)
	assert.Equal(t, holes.VerdictDegraded, event.Classification.Verdict)
}

func TestHealthCheckClassifiesLongRunAsFailed(t *testing.T) {
	env := newHoleTestEnv(t, "movie.mp4", 4*1024*1024, 1024)
	// A run of 5 consecutive missing segments exceeds MaxPadRunSegments (4).
	for i := 10; i < 15; i++ {
		env.markSegmentMissing(i)
	}

	event := env.checker.CheckFile(context.Background(), env.filePath)
	require.Equal(t, EventTypeFileCorrupted, event.Type)
	require.NotNil(t, event.Classification)
	assert.Equal(t, holes.VerdictFailed, event.Classification.Verdict)

	// Failed files are not persisted as known holes (they head to repair).
	meta, err := env.ms.ReadFileMetadata(env.filePath)
	require.NoError(t, err)
	assert.Empty(t, meta.KnownHoles)
}

func TestHealthCheckSkipsClassificationForNonVideo(t *testing.T) {
	env := newHoleTestEnv(t, "archive.rar", 4*1024*1024, 1024)
	env.markSegmentMissing(10)

	event := env.checker.CheckFile(context.Background(), env.filePath)
	require.Equal(t, EventTypeFileCorrupted, event.Type)
	assert.Nil(t, event.Classification, "non-video files are not hole-classified")
}

func TestHealthCheckSkipsClassificationForEncrypted(t *testing.T) {
	env := newHoleTestEnv(t, "movie.mp4", 4*1024*1024, 1024)
	require.NoError(t, env.ms.UpdateFileMetadata(env.filePath, func(m *metapb.FileMetadata) {
		m.Encryption = metapb.Encryption_RCLONE
	}))
	env.markSegmentMissing(10)

	event := env.checker.CheckFile(context.Background(), env.filePath)
	require.Equal(t, EventTypeFileCorrupted, event.Type)
	assert.Nil(t, event.Classification, "encrypted files are not hole-classified")
}

func TestHealthCheckMergesPersistedHoles(t *testing.T) {
	env := newHoleTestEnv(t, "movie.mp4", 4*1024*1024, 1024)
	// Raise the acceptable-missing threshold: this test is about merging
	// persisted holes with newly observed ones, not the threshold itself.
	env.cfg.Health.AcceptableMissingSegmentsPercentage = 1
	// Seed a persisted hole (as if playback padded it earlier).
	require.NoError(t, env.ms.AddKnownHoles(env.filePath, []holes.Run{{Start: 5, Count: 1}}, env.cfg.ProviderFingerprint()))
	// A fresh check finds a different missing segment.
	env.markSegmentMissing(20)

	event := env.checker.CheckFile(context.Background(), env.filePath)
	require.Equal(t, EventTypeFileCorrupted, event.Type)
	require.NotNil(t, event.Classification)
	assert.Equal(t, holes.VerdictDegraded, event.Classification.Verdict)
	// Persisted hole (5) + newly observed (20) both counted.
	assert.Equal(t, 2, event.Classification.TotalMissing)
}

// A STAT sweep cannot clear a known hole: providers can answer STAT for an
// article whose body is gone. Persisted holes therefore keep the file
// degraded through a clean sweep, sampled or full, single or batched, and
// whatever their age or provider fingerprint.
func TestHealthCheckKnownHolesSurviveCleanSweep(t *testing.T) {
	for _, tc := range []struct {
		name    string
		sampled bool
		batch   bool
		stale   bool
	}{
		{name: "sampled single", sampled: true},
		{name: "sampled batch", sampled: true, batch: true},
		{name: "full single"},
		{name: "full batch", batch: true},
		{name: "stale foreign hole", sampled: true, stale: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newHoleTestEnv(t, "movie.mp4", 4*1024*1024, 1024)
			if tc.sampled {
				checkAll := false
				env.cfg.Health.CheckAllSegments = &checkAll
				env.cfg.Health.SegmentSamplePercentage = 5
			}
			require.NoError(t, env.ms.AddKnownHoles(env.filePath, []holes.Run{{Start: 5, Count: 2}}, env.cfg.ProviderFingerprint()))
			if tc.stale {
				require.NoError(t, env.ms.UpdateFileMetadata(env.filePath, func(m *metapb.FileMetadata) {
					m.HoleProviderFingerprint = "another-provider-set"
					for _, r := range m.KnownHoles {
						r.RecordedAt = time.Now().Add(-2 * holes.TTL).Unix()
					}
				}))
			}

			var event HealthEvent
			if tc.batch {
				event = env.checker.CheckFilesBatch(context.Background(), []string{env.filePath}, nil)[0]
			} else {
				event = env.checker.CheckFile(context.Background(), env.filePath)
			}

			require.Equal(t, EventTypeFileCorrupted, event.Type, "err: %v", event.Error)
			require.NotNil(t, event.Classification)
			assert.Equal(t, holes.VerdictDegraded, event.Classification.Verdict)
			assert.Equal(t, 2, event.Classification.TotalMissing)
			require.NotNil(t, event.Details)
			var details database.HealthErrorDetails
			require.NoError(t, json.Unmarshal([]byte(*event.Details), &details))
			assert.Equal(t, "known_holes", details.ErrorType)
		})
	}
}

// Known holes never escalate a clean sweep to failed, even when they exceed
// the acceptable-missing threshold: the sweep found no new damage.
func TestHealthCheckKnownHolesCleanSweepNeverFails(t *testing.T) {
	env := newHoleTestEnv(t, "movie.mp4", 4*1024*1024, 1024)
	env.cfg.Health.AcceptableMissingSegmentsPercentage = 0
	require.NoError(t, env.ms.AddKnownHoles(env.filePath, []holes.Run{{Start: 5, Count: 1}}, env.cfg.ProviderFingerprint()))

	event := env.checker.CheckFile(context.Background(), env.filePath)
	require.NotNil(t, event.Classification)
	assert.Equal(t, holes.VerdictDegraded, event.Classification.Verdict)
}

// Once every known hole is covered by a local patch, a clean sweep is healthy.
func TestHealthCheckPatchedKnownHolesCleanSweepIsHealthy(t *testing.T) {
	env := newHoleTestEnv(t, "movie.mp4", 4*1024*1024, 1024)
	require.NoError(t, env.ms.AddKnownHoles(env.filePath, []holes.Run{{Start: 5, Count: 2}}, env.cfg.ProviderFingerprint()))
	store := par2repair.NewPatchStore(t.TempDir())
	require.NoError(t, store.Put(env.segIDs[5], make([]byte, 1024)))
	require.NoError(t, store.Put(env.segIDs[6], make([]byte, 1024)))
	env.checker.SetPatchIndex(store)

	event := env.checker.CheckFile(context.Background(), env.filePath)
	require.Equal(t, EventTypeFileHealthy, event.Type, "err: %v", event.Error)
	assert.Nil(t, event.Classification)
}

// Hole classification only applies to plain video: other files keep the
// old clean-sweep verdict even when they carry a hole map.
func TestHealthCheckKnownHolesIgnoredForIneligibleFile(t *testing.T) {
	env := newHoleTestEnv(t, "archive.rar", 4*1024*1024, 1024)
	require.NoError(t, env.ms.AddKnownHoles(env.filePath, []holes.Run{{Start: 5, Count: 1}}, env.cfg.ProviderFingerprint()))

	event := env.checker.CheckFile(context.Background(), env.filePath)
	require.Equal(t, EventTypeFileHealthy, event.Type, "err: %v", event.Error)
}

// cleanSweep runs the clean-sweep verdict on a check prepared earlier, so a
// test can change the metadata in between, as playback or a delete would.
func (e *holeTestEnv) cleanSweep(t *testing.T, prep preparedCheck) HealthEvent {
	t.Helper()
	require.Nil(t, prep.earlyEvent)
	return e.checker.judgeValidation(context.Background(), prep, usenet.ValidationResult{TotalChecked: len(prep.sampledIDs)}, nil)
}

// metaFile is the on-disk metadata file of the env's video.
func (e *holeTestEnv) metaFile(t *testing.T) string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(e.metaRoot, "movies", "*.meta"))
	require.NoError(t, err)
	require.Len(t, matches, 1)
	return matches[0]
}

// A hole that playback records while the sweep runs must still count: the
// verdict reads the hole map after the sweep, not the one seen at prepare.
func TestHealthCheckHoleRecordedDuringSweepIsNotHealthy(t *testing.T) {
	env := newHoleTestEnv(t, "movie.mp4", 4*1024*1024, 1024)
	prep := env.checker.prepareCheck(context.Background(), env.filePath)
	require.NoError(t, env.ms.AddKnownHoles(env.filePath, []holes.Run{{Start: 5, Count: 1}}, env.cfg.ProviderFingerprint()))

	event := env.cleanSweep(t, prep)
	require.Equal(t, EventTypeFileCorrupted, event.Type, "err: %v", event.Error)
	require.NotNil(t, event.Classification)
	assert.Equal(t, holes.VerdictDegraded, event.Classification.Verdict)
}

// When the hole map cannot be read after a clean sweep, the sweep proves
// nothing about the file: the check is inconclusive, never healthy.
func TestHealthCheckUnreadableMetadataAfterCleanSweepIsInconclusive(t *testing.T) {
	for _, tc := range []struct {
		name  string
		breakMeta func(t *testing.T, path string)
	}{
		{name: "read error", breakMeta: func(t *testing.T, path string) {
			require.NoError(t, os.Remove(path))
			require.NoError(t, os.Mkdir(path, 0o755))
		}},
		{name: "metadata gone", breakMeta: func(t *testing.T, path string) {
			require.NoError(t, os.Remove(path))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newHoleTestEnv(t, "movie.mp4", 4*1024*1024, 1024)
			require.NoError(t, env.ms.AddKnownHoles(env.filePath, []holes.Run{{Start: 5, Count: 1}}, env.cfg.ProviderFingerprint()))
			prep := env.checker.prepareCheck(context.Background(), env.filePath)
			tc.breakMeta(t, env.metaFile(t))

			event := env.cleanSweep(t, prep)
			assert.Equal(t, EventTypeCheckInconclusive, event.Type)
			assert.Equal(t, database.HealthStatusPending, event.Status)
			assert.Error(t, event.Error)
		})
	}
}

// An ineligible file whose metadata reads fine keeps the old clean-sweep
// verdict, with or without a hole map.
func TestHealthCheckIneligibleFileCleanSweepIsHealthy(t *testing.T) {
	env := newHoleTestEnv(t, "archive.rar", 4*1024*1024, 1024)
	prep := env.checker.prepareCheck(context.Background(), env.filePath)
	require.NoError(t, env.ms.AddKnownHoles(env.filePath, []holes.Run{{Start: 5, Count: 1}}, env.cfg.ProviderFingerprint()))

	event := env.cleanSweep(t, prep)
	require.Equal(t, EventTypeFileHealthy, event.Type, "err: %v", event.Error)
}
