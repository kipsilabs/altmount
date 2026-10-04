package health

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/javi11/nntppool/v5"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/holes"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/par2repair"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/stretchr/testify/require"
)

// A successful PAR2 repair only restores the local article. Providers keep
// reporting it missing, but health must not enqueue the same repair again.
func TestHealthCheckPatchedArticleDoesNotRequeueRepair(t *testing.T) {
	for _, batch := range []bool{false, true} {
		name := "single"
		if batch {
			name = "batch"
		}
		t.Run(name, func(t *testing.T) {
			env := newHoleTestEnv(t, "patched.mp4", 4*1024*1024, 1024)
			env.cfg.Health.AcceptableMissingSegmentsPercentage = 1
			env.markSegmentMissing(10)
			patchDir := t.TempDir()
			store := par2repair.NewPatchStore(patchDir)
			require.NoError(t, store.Put(env.segIDs[10], make([]byte, 1024)))
			env.checker.SetPatchIndex(store)
			// Segment metadata may contain NNTP brackets; the store keys are bare IDs.
			require.NoError(t, env.ms.UpdateFileMetadata(env.filePath, func(m *metapb.FileMetadata) {
				m.SegmentData[10].Id = "<" + env.segIDs[10] + ">"
			}))
			env.fp.SetBehavior("<"+env.segIDs[10]+">", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
			check := func() HealthEvent {
				if batch {
					return env.checker.CheckFilesBatch(context.Background(), []string{env.filePath}, nil)[0]
				}
				return env.checker.CheckFile(context.Background(), env.filePath)
			}
			event := check()
			require.Equal(t, EventTypeFileHealthy, event.Type)
			require.Nil(t, event.Classification)
			workerEnv := newRepairTestEnv(t, t.TempDir(), nil, func(c *config.Config) { c.Health.AcceptableMissingSegmentsPercentage = 1 })
			workerEnv.hw.metadataService = env.ms
			queued := &recordingPar2Enqueuer{}
			workerEnv.hw.SetPar2RepairEnqueuer(queued)
			fh := &database.FileHealth{FilePath: env.filePath, Status: database.HealthStatusHealthy, CreatedAt: time.Now().UTC()}
			_, effect := workerEnv.hw.prepareUpdateForResult(context.Background(), fh, event)
			require.NoError(t, effect())
			require.Empty(t, queued.calls)
			// Eviction must make the article missing again, rather than suppressing
			// future repairs forever based on a past successful job.
			patches, err := filepath.Glob(filepath.Join(patchDir, "*", "*.patch"))
			require.NoError(t, err)
			require.Len(t, patches, 1)
			require.NoError(t, os.Remove(patches[0]))
			require.Equal(t, EventTypeFileCorrupted, check().Type)
		})
	}
}

func TestHealthCheckExcludesPatchedKnownHoles(t *testing.T) {
	env := newHoleTestEnv(t, "patched.mp4", 4*1024*1024, 1024)
	env.cfg.Health.AcceptableMissingSegmentsPercentage = 1
	env.markSegmentMissing(10)
	env.markSegmentMissing(11)
	require.NoError(t, env.ms.AddKnownHoles(env.filePath, []holes.Run{{Start: 10, Count: 2}}, env.cfg.ProviderFingerprint()))
	store := par2repair.NewPatchStore(t.TempDir())
	require.NoError(t, store.Put(env.segIDs[10], make([]byte, 1024)))
	env.checker.SetPatchIndex(store)
	event := env.checker.CheckFile(context.Background(), env.filePath)
	require.Equal(t, EventTypeFileCorrupted, event.Type)
	require.NotNil(t, event.Classification)
	require.Equal(t, 1, event.Classification.TotalMissing)
	require.Equal(t, 1, event.Classification.LongestRun)
	require.Equal(t, holes.VerdictDegraded, event.Classification.Verdict)
}
