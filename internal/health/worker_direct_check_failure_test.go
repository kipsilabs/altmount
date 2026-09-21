package health

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/pool"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/javi11/nntppool/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stallingClient parks StatMany long enough for the manual recheck's own
// deadline to expire, so the check returns an error instead of a verdict.
type stallingClient struct {
	pool.NntpClient
	delay time.Duration
}

func (c *stallingClient) ExistsMany(ctx context.Context, ids []string, opts nntppool.ManyOptions) <-chan nntppool.ExistsResult {
	select {
	case <-ctx.Done():
	case <-time.After(c.delay):
	}
	return c.NntpClient.ExistsMany(ctx, ids, opts)
}

// TestManualRecheckTimeoutRestoresPreCheckStatus covers the recovery path that
// runs when the check errors out rather than producing an event: a timed-out
// manual recheck is no more evidence than an inconclusive one, so the status
// captured before the row went to 'checking' must be restored instead of the
// record being demoted to Pending.
func TestManualRecheckTimeoutRestoresPreCheckStatus(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks not supported on Windows")
	}

	for _, originalStatus := range []database.HealthStatus{
		database.HealthStatusDegraded,
		database.HealthStatusRepairTriggered,
	} {
		t.Run(string(originalStatus), func(t *testing.T) {
			base := fakepool.New()
			env := newBatchTestEnv(t, t.TempDir(), &stallingClient{NntpClient: base, delay: time.Minute})

			filePath := "complete/stalled-" + string(originalStatus) + ".mkv"
			writeHealthyFile(t, env, filePath)

			_, err := env.db.Exec(`
				INSERT INTO file_health (file_path, status, retry_count, max_retries, repair_retry_count, max_repair_retries, scheduled_check_at)
				VALUES (?, ?, 1, 3, 1, 3, datetime('now', '-1 second'))
			`, filePath, originalStatus)
			require.NoError(t, err)
			require.NoError(t, env.healthRepo.SetFileChecking(context.Background(), filePath))

			env.hw.mu.Lock()
			env.hw.running = true
			env.hw.mu.Unlock()
			env.hw.directCheckTimeout = 50 * time.Millisecond

			require.NoError(t, env.hw.PerformBackgroundCheck(context.Background(), filePath, originalStatus, nil))

			require.Eventually(t, func() bool {
				fh, err := env.healthRepo.GetFileHealth(context.Background(), filePath)
				return err == nil && fh != nil && fh.Status != database.HealthStatusChecking
			}, 5*time.Second, 10*time.Millisecond, "the timed-out check must release the 'checking' row")

			fh, err := env.healthRepo.GetFileHealth(context.Background(), filePath)
			require.NoError(t, err)
			require.NotNil(t, fh)
			assert.Equal(t, originalStatus, fh.Status,
				"a timed-out recheck proves nothing and must not demote the record")
			assert.Equal(t, 1, fh.RetryCount, "a timed-out recheck must not consume a health retry")
			assert.Equal(t, 1, fh.RepairRetryCount, "a timed-out recheck must not consume a repair retry")
		})
	}
}

// TestManualRecheckCancellationLeavesPendingIntact verifies the deliberate
// Pending written by CancelHealthCheck survives: the recovery path must not
// restore the pre-check status over a user's explicit cancellation.
func TestManualRecheckCancellationLeavesPendingIntact(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks not supported on Windows")
	}

	base := fakepool.New()
	env := newBatchTestEnv(t, t.TempDir(), &stallingClient{NntpClient: base, delay: time.Minute})

	filePath := "complete/cancelled.mkv"
	writeHealthyFile(t, env, filePath)

	_, err := env.db.Exec(`
		INSERT INTO file_health (file_path, status, retry_count, max_retries, repair_retry_count, max_repair_retries, scheduled_check_at)
		VALUES (?, 'degraded', 1, 3, 1, 3, datetime('now', '-1 second'))
	`, filePath)
	require.NoError(t, err)
	require.NoError(t, env.healthRepo.SetFileChecking(context.Background(), filePath))

	env.hw.mu.Lock()
	env.hw.running = true
	env.hw.mu.Unlock()

	require.NoError(t, env.hw.PerformBackgroundCheck(context.Background(), filePath, database.HealthStatusDegraded, nil))

	require.Eventually(t, func() bool {
		return env.hw.IsCheckActive(filePath)
	}, 5*time.Second, 10*time.Millisecond, "the check must register itself before it can be cancelled")

	require.NoError(t, env.hw.CancelHealthCheck(context.Background(), filePath))

	require.Never(t, func() bool {
		fh, err := env.healthRepo.GetFileHealth(context.Background(), filePath)
		return err == nil && fh != nil && fh.Status != database.HealthStatusPending
	}, 500*time.Millisecond, 25*time.Millisecond,
		"cancellation parks the record on Pending on the user's behalf; the recovery path must not undo it")
}
