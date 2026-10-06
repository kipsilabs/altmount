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

// A terminal PAR2 failure must hand a degraded imported file to the ARR
// notification queue, without resetting or consuming either retry budget.
func TestServiceFailedRepairQueuesArrReplacement(t *testing.T) {
	for _, failure := range []struct {
		name     string
		err      error
		attempts int
	}{
		{"unrepairable", fmt.Errorf("%w: no PAR2 files", ErrUnrepairable), 0},
		{"nothing to repair", ErrNothingToRepair, 0},
		{"attempts exhausted", errors.New("provider timeout"), maxJobAttempts - 1},
	} {
		for _, enabled := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/arr=%t", failure.name, enabled), func(t *testing.T) {
				ctx := context.Background()
				db, err := database.NewDB(database.Config{DatabasePath: filepath.Join(t.TempDir(), "repair.db")})
				require.NoError(t, err)
				t.Cleanup(func() { db.Close() })
				health := database.NewHealthRepository(db.Connection(), database.DialectSQLite)
				repo := database.NewPar2RepairRepository(db.Connection(), database.DialectSQLite)
				s := NewService(repo, nil, nil, NewPatchStore(t.TempDir()), func() Config { return Config{Enabled: true, ArrRepairEnabled: enabled} }, testLogger())
				s.SetHealthStore(health)
				for _, status := range []database.HealthStatus{database.HealthStatusDegraded, database.HealthStatusCorrupted} {
					path := string(status) + ".mkv"
					require.NoError(t, health.UpdateFileHealth(ctx, path, status, nil, nil, nil, false))
					_, err = db.Connection().Exec(`UPDATE file_health SET retry_count=1, repair_retry_count=1, error_details='missing segments', library_path='/library/movie.mkv' WHERE file_path=?`, path)
					require.NoError(t, err)
					s.Enqueue(ctx, path, "")
					job, err := repo.ClaimNext(ctx, time.Now().UTC())
					require.NoError(t, err)
					require.NotNil(t, job)
					job.Attempts = failure.attempts
					s.handleOutcome(ctx, job, failure.err)
					got, err := health.GetFileHealth(ctx, path)
					require.NoError(t, err)
					want := database.HealthStatusCorrupted
					if enabled && status == database.HealthStatusDegraded {
						want = database.HealthStatusRepairTriggered
					}
					require.Equal(t, want, got.Status)
					require.Equal(t, 1, got.RetryCount)
					require.Equal(t, 1, got.RepairRetryCount)
					require.NotNil(t, got.LastError)
					require.Contains(t, *got.LastError, failure.err.Error())
					require.Equal(t, "/library/movie.mkv", *got.LibraryPath)
					if want == database.HealthStatusRepairTriggered {
						// Playback may enqueue another PAR2 job before the ARR
						// notification sweep runs. Its verdict must not cancel
						// the already queued replacement.
						s.markFileUnrepairable(ctx, path, failure.err.Error())
						got, err = health.GetFileHealth(ctx, path)
						require.NoError(t, err)
						require.Equal(t, database.HealthStatusRepairTriggered, got.Status)
					}
				}
				due, err := health.GetFilesForRepairNotification(ctx, 10)
				require.NoError(t, err)
				if enabled {
					require.Len(t, due, 1)
					require.Equal(t, "degraded.mkv", due[0].FilePath)
				} else {
					require.Empty(t, due)
				}
				jobs, err := repo.List(ctx, 10)
				require.NoError(t, err)
				require.Empty(t, jobs)
			})
		}
	}
}

func TestServiceFailedNzbRepairDoesNotQueueArrReplacement(t *testing.T) {
	ctx := context.Background()
	repo := newTestRepo(t)
	s := testService(t, repo, true)
	s.cfg = func() Config { return Config{Enabled: true, ArrRepairEnabled: true} }
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
