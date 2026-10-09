package par2repair

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/stretchr/testify/require"
)

// A terminal PAR2 failure keeps a degraded imported file playable without
// resetting or consuming either retry budget.
func TestServiceFailedRepairPreservesDegradedFile(t *testing.T) {
	for _, failure := range []struct {
		name     string
		err      error
		attempts int
	}{
		{"unrepairable", fmt.Errorf("%w: no PAR2 files", ErrUnrepairable), 0},
		{"nothing to repair", ErrNothingToRepair, 0},
		{"attempts exhausted", errors.New("provider timeout"), maxJobAttempts - 1},
	} {
		t.Run(failure.name, func(t *testing.T) {
			ctx := context.Background()
			db, err := database.NewDB(database.Config{DatabasePath: filepath.Join(t.TempDir(), "repair.db")})
			require.NoError(t, err)
			t.Cleanup(func() { db.Close() })
			health := database.NewHealthRepository(db.Connection(), database.DialectSQLite)
			repo := database.NewPar2RepairRepository(db.Connection(), database.DialectSQLite)
			s := NewService(repo, nil, nil, NewPatchStore(t.TempDir()), func() Config { return Config{Enabled: true} }, testLogger())
			s.SetHealthStore(health)
			for _, status := range []database.HealthStatus{database.HealthStatusDegraded, database.HealthStatusCorrupted, database.HealthStatusRepairTriggered} {
				path := string(status) + ".mkv"
				require.NoError(t, health.UpdateFileHealth(ctx, path, status, nil, nil, nil, false))
				_, err = db.Connection().Exec(`UPDATE file_health SET retry_count=1, repair_retry_count=1, error_details='missing segments', library_path='/library/movie.mkv', scheduled_check_at='2099-01-01 00:00:00' WHERE file_path=?`, path)
				require.NoError(t, err)
				s.Enqueue(ctx, path, "")
				job, err := repo.ClaimNext(ctx, time.Now().UTC())
				require.NoError(t, err)
				require.NotNil(t, job)
				job.Attempts = failure.attempts
				s.handleOutcome(ctx, job, failure.err)
				got, err := health.GetFileHealth(ctx, path)
				require.NoError(t, err)
				require.Equal(t, status, got.Status)
				if status != database.HealthStatusCorrupted {
					var scheduled time.Time
					require.NoError(t, db.Connection().QueryRow(`SELECT scheduled_check_at FROM file_health WHERE file_path=?`, path).Scan(&scheduled))
					require.Equal(t, time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC), scheduled,
						"keep periodic health checks after failed repair")
				}
				if status != database.HealthStatusCorrupted {
					// Repeated failures must not condemn degraded files or cancel
					// an existing ARR repair.
					s.markFileUnrepairable(ctx, path, failure.err.Error())
					latest, err := health.GetFileHealth(ctx, path)
					require.NoError(t, err)
					require.Equal(t, status, latest.Status)
				}
				require.Equal(t, 1, got.RetryCount)
				require.Equal(t, 1, got.RepairRetryCount)
				require.NotNil(t, got.LastError)
				require.Contains(t, *got.LastError, failure.err.Error())
				require.Equal(t, "/library/movie.mkv", *got.LibraryPath)
				require.Equal(t, "missing segments", *got.ErrorDetails)
			}
			due, err := health.GetFilesForRepairNotification(ctx, 10)
			require.NoError(t, err)
			require.Empty(t, due)
			jobs, err := repo.List(ctx, 10)
			require.NoError(t, err)
			require.Empty(t, jobs)
		})
	}
}

func TestServiceFailedNzbRepairDoesNotQueueArrReplacement(t *testing.T) {
	ctx := context.Background()
	repo := newTestRepo(t)
	s := testService(t, repo, true)
	health := &recordingHealth{}
	resumer := &recordingResumer{}
	s.SetHealthStore(health)
	s.SetImportResumer(resumer)
	for _, failure := range []struct {
		err      error
		attempts int
	}{
		{ErrUnrepairable, 0}, {ErrNothingToRepair, 0}, {errors.New("provider timeout"), maxJobAttempts - 1},
	} {
		_, err := repo.EnqueueNzb(ctx, "/nzbs/release.nzb", "")
		require.NoError(t, err)
		job, err := repo.ClaimNext(ctx, time.Now().UTC())
		require.NoError(t, err)
		require.NotNil(t, job)
		job.Attempts = failure.attempts
		s.handleOutcome(ctx, job, failure.err)
	}
	require.Empty(t, health.calls, "pre-import failures belong to the import queue")
	require.Empty(t, resumer.resumed)
	require.Len(t, resumer.failed, 3)
	for _, failed := range resumer.failed {
		require.Equal(t, "/nzbs/release.nzb", failed.nzb)
		require.NotEmpty(t, failed.reason)
	}
}
