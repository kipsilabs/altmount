package health

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/javi11/nntppool/v5"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrepareUpdateForResult_DeferredRestoresStatusAndRecordsDeferral(t *testing.T) {
	env := newRepairTestEnv(t, t.TempDir(), nil)
	until := time.Now().UTC().Add(48 * time.Hour)
	verified := `{"content_verification":"passed"}`
	fh := &database.FileHealth{FilePath: "movies/a.mkv", Status: database.HealthStatusHealthy,
		ErrorDetails: &verified, RetryCount: 1, CreatedAt: time.Now().UTC()}

	update, effect := env.hw.prepareUpdateForResult(context.Background(), fh,
		HealthEvent{Type: EventTypeCheckDeferred, FilePath: fh.FilePath, DeferUntil: until})
	assert.Equal(t, database.UpdateTypeDeferred, update.Type)
	assert.Equal(t, database.HealthStatusHealthy, update.Status)
	assert.Equal(t, until, update.ScheduledCheckAt)
	assert.True(t, database.HasPeerDeferral(update.ErrorDetails))
	assert.Contains(t, *update.ErrorDetails, `"content_verification":"passed"`)
	assert.Nil(t, update.ErrorMessage)
	if effect != nil {
		assert.NoError(t, effect())
	}
}

func TestPrepareUpdateForResult_DeferredWhileCheckingRestoresTheOldStatus(t *testing.T) {
	// The cycle loads rows before marking them checking, but a record that is
	// already 'checking' must still come back healthy, never stay checking.
	env := newRepairTestEnv(t, t.TempDir(), nil)
	fh := &database.FileHealth{FilePath: "movies/a.mkv", Status: database.HealthStatusChecking, CreatedAt: time.Now().UTC()}
	update, _ := env.hw.prepareUpdateForResult(context.Background(), fh,
		HealthEvent{Type: EventTypeCheckDeferred, FilePath: fh.FilePath, Status: database.HealthStatusHealthy, DeferUntil: time.Now().Add(time.Hour)})
	assert.Equal(t, database.HealthStatusHealthy, update.Status)
}

// An inconclusive check proves nothing, so it must not end a deferral.
func TestPrepareUpdateForResult_InconclusiveKeepsTheDeferral(t *testing.T) {
	env := newRepairTestEnv(t, t.TempDir(), nil)
	marked := database.WithPeerDeferral(nil, time.Now())
	fh := &database.FileHealth{FilePath: "movies/a.mkv", Status: database.HealthStatusHealthy, ErrorDetails: marked, CreatedAt: time.Now().UTC()}
	update, _ := env.hw.prepareUpdateForResult(context.Background(), fh,
		HealthEvent{Type: EventTypeCheckInconclusive, FilePath: fh.FilePath})
	assert.True(t, database.HasPeerDeferral(update.ErrorDetails))

	// A real verdict ends it.
	update, _ = env.hw.prepareUpdateForResult(context.Background(), fh, healthyEvent(fh.FilePath))
	assert.False(t, database.HasPeerDeferral(update.ErrorDetails))
}

// cycleEnv wires a worker whose checker runs over a 4096-segment fake file
// and whose healthy row is due now.
func cycleEnv(t *testing.T) (*repairTestEnv, *holeTestEnv) {
	t.Helper()
	ctx := context.Background()
	holesEnv := sampledEnv(t)
	holesEnv.cfg.Import.ImportStrategy = config.ImportStrategyNone
	env := newRepairTestEnv(t, t.TempDir(), nil)
	env.hw.healthChecker = holesEnv.checker
	env.hw.metadataService = holesEnv.ms
	env.hw.configGetter = func() *config.Config { return holesEnv.cfg }
	_, err := env.db.ExecContext(ctx, `INSERT INTO file_health
		(file_path, status, last_checked, release_date, scheduled_check_at, retry_count)
		VALUES (?, 'healthy', datetime('now', '-3 days'), datetime('now', '-400 days'), datetime('now', '-1 minute'), 0)`,
		healthPath(holesEnv))
	require.NoError(t, err)
	return env, holesEnv
}

// healthPath is the row key the health table uses (no leading slash).
func healthPath(h *holeTestEnv) string { return strings.TrimPrefix(h.filePath, "/") }

func rowOf(t *testing.T, env *repairTestEnv, path string) (status string, lastChecked, scheduled time.Time, details *string) {
	t.Helper()
	require.NoError(t, env.db.QueryRow(`SELECT status, last_checked, scheduled_check_at, error_details FROM file_health WHERE file_path = ?`, path).
		Scan(&status, &lastChecked, &scheduled, &details))
	return
}

func TestCycle_SkipOnceThenRealCheck(t *testing.T) {
	ctx := context.Background()
	env, holesEnv := cycleEnv(t)
	adv := &fakeAdviser{advice: PeerAdvice{Kind: PeerAdviceSkipOnce, SkipUntil: time.Now().Add(5 * 24 * time.Hour)}}
	env.hw.SetPeerAdviser(adv)
	_, lastBefore, _, _ := rowOf(t, env, healthPath(holesEnv))

	// Cycle 1: advised, so the due check is deferred.
	stats := statCount(holesEnv, func() { require.NoError(t, env.hw.runHealthCheckCycle(ctx)) })
	assert.Zero(t, stats, "no articles were asked for")
	status, last, sched, details := rowOf(t, env, healthPath(holesEnv))
	assert.Equal(t, "healthy", status)
	assert.True(t, last.Equal(lastBefore), "a deferral is not a check: last_checked is untouched")
	assert.WithinDuration(t, time.Now().Add(5*24*time.Hour), sched, time.Minute)
	assert.True(t, database.HasPeerDeferral(details))
	assert.Equal(t, 1, adv.consulted())

	// Due again: the deferral is spent, so this is a real, normal-size check
	// and the adviser is not even asked.
	advanceScheduledCheck(t, env.db, healthPath(holesEnv))
	stats = statCount(holesEnv, func() { require.NoError(t, env.hw.runHealthCheckCycle(ctx)) })
	assert.Equal(t, 2048, stats)
	assert.Equal(t, 1, adv.consulted())
	status, last, sched, details = rowOf(t, env, healthPath(holesEnv))
	assert.Equal(t, "healthy", status)
	assert.True(t, last.After(lastBefore))
	assert.False(t, database.HasPeerDeferral(details), "a real check ends the deferral")
	assert.True(t, sched.After(time.Now().Add(24*time.Hour)))

	// With the marker gone, a later due check may be deferred again.
	advanceScheduledCheck(t, env.db, healthPath(holesEnv))
	stats = statCount(holesEnv, func() { require.NoError(t, env.hw.runHealthCheckCycle(ctx)) })
	assert.Zero(t, stats)
	assert.Equal(t, 2, adv.consulted())
}

// An inconclusive check between two deferrals must not reopen the door.
func TestCycle_InconclusiveCheckDoesNotAllowASecondSkip(t *testing.T) {
	ctx := context.Background()
	env, holesEnv := cycleEnv(t)
	adv := &fakeAdviser{advice: PeerAdvice{Kind: PeerAdviceSkipOnce, SkipUntil: time.Now().Add(5 * 24 * time.Hour)}}
	env.hw.SetPeerAdviser(adv)
	require.NoError(t, env.hw.runHealthCheckCycle(ctx)) // deferred

	// Providers fail: the next check proves nothing.
	for i := range holesEnv.segIDs {
		holesEnv.fp.SetBehavior(holesEnv.segIDs[i], fakepool.SegmentBehavior{Err: nntppool.ErrConnectionDied})
	}
	advanceScheduledCheck(t, env.db, healthPath(holesEnv))
	require.NoError(t, env.hw.runHealthCheckCycle(ctx))
	_, _, _, details := rowOf(t, env, healthPath(holesEnv))
	assert.True(t, database.HasPeerDeferral(details), "no verdict, so the deferral stands")

	advanceScheduledCheck(t, env.db, healthPath(holesEnv))
	before := adv.consulted()
	require.NoError(t, env.hw.runHealthCheckCycle(ctx))
	assert.Equal(t, before, adv.consulted(), "still no second deferral")
}

func TestCycle_NoAdviserIsUnchanged(t *testing.T) {
	ctx := context.Background()
	env, holesEnv := cycleEnv(t)
	stats := statCount(holesEnv, func() { require.NoError(t, env.hw.runHealthCheckCycle(ctx)) })
	assert.Equal(t, 2048, stats)
	status, _, _, details := rowOf(t, env, healthPath(holesEnv))
	assert.Equal(t, "healthy", status)
	assert.False(t, database.HasPeerDeferral(details))
}

func TestCycle_ReduceAdviceShrinksOnlyTheScheduledCheck(t *testing.T) {
	ctx := context.Background()
	env, holesEnv := cycleEnv(t)
	adv := &fakeAdviser{advice: PeerAdvice{Kind: PeerAdviceReduceSample}}
	env.hw.SetPeerAdviser(adv)

	stats := statCount(holesEnv, func() { require.NoError(t, env.hw.runHealthCheckCycle(ctx)) })
	assert.Equal(t, 4096*PeerFloorPercent/100, stats)
	_, _, sched, _ := rowOf(t, env, healthPath(holesEnv))
	assert.True(t, sched.After(time.Now().Add(24*time.Hour)), "the next check is scheduled as after any healthy check")

	// A manual recheck of the same file is a normal-size check.
	adv.mu.Lock()
	adv.calls = nil
	adv.mu.Unlock()
	stats = statCount(holesEnv, func() {
		require.NoError(t, env.hw.performDirectCheck(ctx, holesEnv.filePath, database.HealthStatusHealthy, nil))
	})
	assert.Equal(t, 2048, stats)
	assert.Zero(t, adv.consulted())
}

func TestScheduleCheckNow_OnlyMovesAHealthyCheckEarlier(t *testing.T) {
	ctx := context.Background()
	env, holesEnv := cycleEnv(t)
	_, err := env.db.ExecContext(ctx, `UPDATE file_health SET scheduled_check_at = datetime('now', '+30 days') WHERE file_path = ?`, healthPath(holesEnv))
	require.NoError(t, err)

	moved, err := env.hw.ScheduleCheckNow(ctx, healthPath(holesEnv))
	require.NoError(t, err)
	assert.True(t, moved)
	status, _, sched, _ := rowOf(t, env, healthPath(holesEnv))
	assert.Equal(t, "healthy", status, "never writes a status")
	assert.True(t, sched.Before(time.Now().Add(time.Minute)))

	moved, err = env.hw.ScheduleCheckNow(ctx, healthPath(holesEnv))
	require.NoError(t, err)
	assert.False(t, moved, "already due: nothing to move")
}
