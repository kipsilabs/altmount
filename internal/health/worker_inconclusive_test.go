package health

import (
	"context"
	"database/sql"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/pool"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/javi11/nntppool/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type beforeStatManyClient struct {
	pool.NntpClient
	once   sync.Once
	before func()
}

func (c *beforeStatManyClient) ExistsMany(ctx context.Context, ids []string, opts nntppool.ManyOptions) <-chan nntppool.ExistsResult {
	c.once.Do(c.before)
	return c.NntpClient.ExistsMany(ctx, ids, opts)
}

// TestPrepareUpdateForResultInconclusive covers the worker's half of #861: a
// check that could not reach a verdict must reschedule the file without
// burning a retry, changing its status, or escalating it into repair.
func TestPrepareUpdateForResultInconclusive(t *testing.T) {
	env := newRepairTestEnv(t, t.TempDir(), nil)

	filePath := "/movies/movie.mp4"
	meta := validSegmentMeta(env.metadataService, 1024)
	require.NoError(t, env.metadataService.WriteFileMetadata(filePath, meta))

	event := HealthEvent{
		Type:     EventTypeCheckInconclusive,
		FilePath: filePath,
		Status:   database.HealthStatusPending,
		Error:    errors.New("segment check inconclusive: nntp: connection died"),
	}

	t.Run("reschedules without consuming a retry", func(t *testing.T) {
		fh := database.FileHealth{
			FilePath:   filePath,
			Status:     database.HealthStatusPending,
			RetryCount: 2,
			CreatedAt:  time.Now().UTC(),
		}
		update, sideEffect := env.hw.prepareUpdateForResult(context.Background(), &fh, event)

		assert.Equal(t, database.UpdateTypeInconclusive, update.Type)
		assert.Equal(t, database.HealthStatusPending, update.Status)
		assert.False(t, update.ScheduledCheckAt.IsZero())
		assert.True(t, update.ScheduledCheckAt.After(time.Now().UTC()))
		require.NotNil(t, update.ErrorMessage)
		assert.Contains(t, *update.ErrorMessage, "inconclusive")

		require.NoError(t, sideEffect())
		assert.Empty(t, env.mockARRs.calls, "an inconclusive check must not trigger an ARR rescan")
	})

	t.Run("exhausted retries still do not trigger repair", func(t *testing.T) {
		fh := database.FileHealth{
			FilePath:   filePath,
			Status:     database.HealthStatusPending,
			RetryCount: 99,
			CreatedAt:  time.Now().UTC(),
		}
		update, sideEffect := env.hw.prepareUpdateForResult(context.Background(), &fh, event)

		assert.Equal(t, database.UpdateTypeInconclusive, update.Type)
		require.NoError(t, sideEffect())
		assert.Empty(t, env.mockARRs.calls)
	})

	t.Run("restores the status the record had before the check", func(t *testing.T) {
		for _, status := range []database.HealthStatus{
			database.HealthStatusDegraded,
			database.HealthStatusRepairTriggered,
		} {
			fh := database.FileHealth{FilePath: filePath, Status: status, CreatedAt: time.Now().UTC()}
			update, _ := env.hw.prepareUpdateForResult(context.Background(), &fh, event)
			assert.Equal(t, database.UpdateTypeInconclusive, update.Type)
			assert.Equal(t, status, update.Status, "status %q must survive an inconclusive check", status)
		}
	})

	t.Run("a record still marked checking falls back to pending", func(t *testing.T) {
		fh := database.FileHealth{
			FilePath:  filePath,
			Status:    database.HealthStatusChecking,
			CreatedAt: time.Now().UTC(),
		}
		update, _ := env.hw.prepareUpdateForResult(context.Background(), &fh, event)
		assert.Equal(t, database.HealthStatusPending, update.Status,
			"'checking' is transient — never persist it as the resting status")
	})
}

// TestInconclusiveCycleLeavesRecordIntact drives a full health-check cycle
// against a provider that fails transiently and asserts the persisted record
// is untouched apart from being pushed to a later check.
func TestInconclusiveCycleLeavesRecordIntact(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks not supported on Windows")
	}

	client := fakepool.New()
	env := newBatchTestEnv(t, t.TempDir(), client)

	filePath := "complete/flaky.mkv"
	segID := writeHealthyFile(t, env, filePath)
	client.SetBehavior(segID, fakepool.SegmentBehavior{Err: nntppool.ErrConnectionDied})
	insertFileHealth(t, env.db, filePath, "", 1, 3)

	require.NoError(t, env.hw.runHealthCheckCycle(context.Background()))

	var status string
	var retryCount, repairRetryCount int
	var scheduled sql.NullString
	require.NoError(t, env.db.QueryRow(`
		SELECT status, retry_count, repair_retry_count, scheduled_check_at
		FROM file_health WHERE file_path = ?`, filePath,
	).Scan(&status, &retryCount, &repairRetryCount, &scheduled))

	assert.Equal(t, string(database.HealthStatusPending), status)
	assert.Equal(t, 1, retryCount, "an inconclusive check must not consume a retry")
	assert.Equal(t, 0, repairRetryCount)
	assert.True(t, scheduled.Valid, "the file must stay on the check schedule")

	var future int
	require.NoError(t, env.db.QueryRow(
		`SELECT scheduled_check_at > datetime('now') FROM file_health WHERE file_path = ?`, filePath,
	).Scan(&future))
	assert.Equal(t, 1, future, "the next check must be pushed into the future")

	assert.Empty(t, env.mockARRs.calls)
}

func TestInconclusiveManualRecheckRestoresPreCheckStatus(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks not supported on Windows")
	}

	for _, originalStatus := range []database.HealthStatus{
		database.HealthStatusDegraded,
		database.HealthStatusRepairTriggered,
	} {
		t.Run(string(originalStatus), func(t *testing.T) {
			client := fakepool.New()
			env := newBatchTestEnv(t, t.TempDir(), client)
			filePath := "complete/" + string(originalStatus) + ".mkv"
			segID := writeHealthyFile(t, env, filePath)
			client.SetBehavior(segID, fakepool.SegmentBehavior{Err: nntppool.ErrConnectionDied})

			_, err := env.db.Exec(`
				INSERT INTO file_health (file_path, status, retry_count, max_retries, repair_retry_count, max_repair_retries, scheduled_check_at)
				VALUES (?, ?, 1, 3, 1, 3, datetime('now', '-1 second'))
			`, filePath, originalStatus)
			require.NoError(t, err)
			require.NoError(t, env.healthRepo.SetFileChecking(context.Background(), filePath))

			require.NoError(t, env.hw.performDirectCheck(context.Background(), filePath, originalStatus, nil))

			fh, err := env.healthRepo.GetFileHealth(context.Background(), filePath)
			require.NoError(t, err)
			require.NotNil(t, fh)
			assert.Equal(t, originalStatus, fh.Status)
			assert.Equal(t, 1, fh.RetryCount, "inconclusive manual recheck must not consume a health retry")
			assert.Equal(t, 1, fh.RepairRetryCount, "inconclusive manual recheck must not consume a repair retry")
		})
	}
}

func TestManualRecheckDoesNotOverwriteConcurrentStatusChange(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks not supported on Windows")
	}

	baseClient := fakepool.New()
	env := newBatchTestEnv(t, t.TempDir(), baseClient)
	filePath := "complete/concurrent.mkv"
	segID := writeHealthyFile(t, env, filePath)
	baseClient.SetBehavior(segID, fakepool.SegmentBehavior{Err: nntppool.ErrConnectionDied})

	_, err := env.db.Exec(`
		INSERT INTO file_health (file_path, status, retry_count, max_retries, repair_retry_count, max_repair_retries, scheduled_check_at)
		VALUES (?, 'degraded', 1, 3, 0, 3, datetime('now', '-1 second'))
	`, filePath)
	require.NoError(t, err)
	require.NoError(t, env.healthRepo.SetFileChecking(context.Background(), filePath))

	client := &beforeStatManyClient{
		NntpClient: baseClient,
		before: func() {
			require.NoError(t, env.healthRepo.UpdateFileHealth(
				context.Background(), filePath, database.HealthStatusHealthy, nil, nil, nil, false,
			))
		},
	}
	env.healthChecker.poolManager = &fakeClientPoolManager{client: client}

	require.NoError(t, env.hw.performDirectCheck(
		context.Background(), filePath, database.HealthStatusDegraded, nil,
	))

	fh, err := env.healthRepo.GetFileHealth(context.Background(), filePath)
	require.NoError(t, err)
	require.NotNil(t, fh)
	assert.Equal(t, database.HealthStatusHealthy, fh.Status,
		"a webhook or re-import status change during a manual check must win")
}
