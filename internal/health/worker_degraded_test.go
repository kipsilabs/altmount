package health

import (
	"context"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/holes"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPrepareUpdateForResultDegraded verifies the worker's decision table for
// classified events: degraded verdicts skip repair entirely, while fatal and
// unclassified events follow the pre-existing retry/repair path.
func TestPrepareUpdateForResultDegraded(t *testing.T) {
	tempDir := t.TempDir()
	env := newRepairTestEnv(t, tempDir, nil)

	filePath := "/movies/movie.mp4"
	meta := validSegmentMeta(env.metadataService, 1024)
	require.NoError(t, env.metadataService.WriteFileMetadata(filePath, meta))

	now := time.Now().UTC()
	baseFH := database.FileHealth{
		FilePath:  filePath,
		Status:    database.HealthStatusPending,
		CreatedAt: now,
	}
	corruptedEvent := func(cls *holes.Impact) HealthEvent {
		return HealthEvent{
			Type:           EventTypeFileCorrupted,
			FilePath:       filePath,
			Status:         database.HealthStatusCorrupted,
			Classification: cls,
		}
	}

	t.Run("degraded verdict skips repair", func(t *testing.T) {
		fh := baseFH
		fh.RetryCount = 99 // even with retries exhausted, degraded wins
		update, sideEffect := env.hw.prepareUpdateForResult(context.Background(), &fh,
			corruptedEvent(&holes.Impact{
				Verdict:      holes.VerdictDegraded,
				TotalMissing: 2,
				LongestRun:   1,
			}))

		assert.Equal(t, database.UpdateTypeDegraded, update.Type)
		assert.Equal(t, database.HealthStatusDegraded, update.Status)
		assert.False(t, update.ScheduledCheckAt.IsZero(), "degraded records stay on the re-check schedule")

		require.NoError(t, sideEffect())
		assert.Empty(t, env.mockARRs.calls, "degraded must not trigger an ARR rescan")

		got, err := env.metadataService.ReadFileMetadata(filePath)
		require.NoError(t, err)
		assert.Equal(t, metapb.FileStatus_FILE_STATUS_DEGRADED, got.Status)
	})

	t.Run("in-flight repair wins over degraded", func(t *testing.T) {
		fh := baseFH
		fh.Status = database.HealthStatusRepairTriggered
		update, _ := env.hw.prepareUpdateForResult(context.Background(), &fh,
			corruptedEvent(&holes.Impact{Verdict: holes.VerdictDegraded}))
		assert.NotEqual(t, database.UpdateTypeDegraded, update.Type,
			"an already-triggered repair must not be downgraded")
	})

	t.Run("fatal verdict follows normal retry path", func(t *testing.T) {
		fh := baseFH
		update, _ := env.hw.prepareUpdateForResult(context.Background(), &fh,
			corruptedEvent(&holes.Impact{Verdict: holes.VerdictFailed}))
		assert.Equal(t, database.UpdateTypeRetry, update.Type)
		assert.Equal(t, database.HealthStatusPending, update.Status)
	})

	t.Run("unknown verdict follows normal retry path", func(t *testing.T) {
		fh := baseFH
		update, _ := env.hw.prepareUpdateForResult(context.Background(), &fh,
			corruptedEvent(&holes.Impact{Verdict: holes.VerdictUnknown}))
		assert.Equal(t, database.UpdateTypeRetry, update.Type)
	})

	t.Run("no classification follows normal retry path", func(t *testing.T) {
		fh := baseFH
		update, _ := env.hw.prepareUpdateForResult(context.Background(), &fh, corruptedEvent(nil))
		assert.Equal(t, database.UpdateTypeRetry, update.Type)
	})
}

// A repair can fail immediately (e.g. no PAR2 files). Its verdict must land
// after the health check's degraded write, so that write cannot undo it.
func TestDegradedRepairEnqueuedAfterHealthUpdate(t *testing.T) {
	for _, mode := range []string{"batch", "direct"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			holesEnv := newHoleTestEnv(t, "movie.mp4", 128*1024, 1024)
			holesEnv.cfg.Health.AcceptableMissingSegmentsPercentage = 2
			holesEnv.markSegmentMissing(10)
			env := newRepairTestEnv(t, t.TempDir(), nil)
			env.hw.healthChecker = holesEnv.checker
			env.hw.metadataService = holesEnv.ms
			env.hw.configGetter = func() *config.Config { return holesEnv.cfg }
			require.NoError(t, env.healthRepo.UpdateFileHealth(ctx, holesEnv.filePath, database.HealthStatusPending, nil, nil, nil, false))
			called := false
			env.hw.SetPar2RepairEnqueuer(par2EnqueueFunc(func(ctx context.Context, path, _ string) {
				called = true
				got, err := env.healthRepo.GetFileHealth(ctx, path)
				require.NoError(t, err)
				require.Equal(t, database.HealthStatusDegraded, got.Status, "persist degraded before PAR2 can finish")
				reason := "no PAR2 files"
				require.NoError(t, env.healthRepo.RecordPar2RepairFailure(ctx, path, reason))
			}))
			if mode == "direct" {
				require.NoError(t, env.hw.performDirectCheck(ctx, holesEnv.filePath, database.HealthStatusPending, nil))
			} else {
				require.NoError(t, env.hw.runHealthCheckCycle(ctx))
			}
			require.True(t, called)
			got, err := env.healthRepo.GetFileHealth(ctx, holesEnv.filePath)
			require.NoError(t, err)
			require.Equal(t, database.HealthStatusDegraded, got.Status, "PAR2 failure must keep the file degraded")
			require.NotNil(t, got.LastError)
			require.Equal(t, "no PAR2 files", *got.LastError, "PAR2 failure must survive the health cycle")
		})
	}
}

type par2EnqueueFunc func(context.Context, string, string)

func (f par2EnqueueFunc) Enqueue(ctx context.Context, path, segment string) { f(ctx, path, segment) }
