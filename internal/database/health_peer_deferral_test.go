package database

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPeerDeferralMarker_RoundTrip(t *testing.T) {
	at := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	assert.False(t, HasPeerDeferral(nil))
	assert.False(t, HasPeerDeferral(strPtr(`{"content_verification":"passed"}`)))
	assert.False(t, HasPeerDeferral(strPtr("not json")))

	marked := WithPeerDeferral(strPtr(`{"content_verification":"passed"}`), at)
	require.NotNil(t, marked)
	assert.True(t, HasPeerDeferral(marked))
	assert.JSONEq(t, `{"content_verification":"passed","peer_deferred_at":"2026-11-12T10:00:00Z"}`, *marked,
		"the content verification outcome must survive the marker")

	// Unparseable or missing details still yield a marked envelope.
	assert.True(t, HasPeerDeferral(WithPeerDeferral(nil, at)))
	assert.True(t, HasPeerDeferral(WithPeerDeferral(strPtr("legacy text"), at)))

	// Carrying the marker onto other details keeps those details.
	carried := CarryPeerDeferral(strPtr(`{"message":"x"}`), marked)
	assert.True(t, HasPeerDeferral(carried))
	assert.Contains(t, *carried, `"message":"x"`)
	// Nothing to carry: details are returned as given.
	plain := strPtr(`{"message":"x"}`)
	assert.Same(t, plain, CarryPeerDeferral(plain, strPtr(`{"message":"y"}`)))
}

func strPtr(s string) *string { return &s }

func scheduledAt(t *testing.T, repo *HealthRepository, path string) *time.Time {
	t.Helper()
	var at *time.Time
	require.NoError(t, repo.db.QueryRowContext(context.Background(),
		`SELECT scheduled_check_at FROM file_health WHERE file_path = ?`, path).Scan(&at))
	return at
}

func TestUpdateHealthStatusBulk_DeferredMovesScheduleOnly(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()
	last := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Second)
	_, err := repo.db.ExecContext(ctx, `INSERT INTO file_health
		(file_path, status, last_checked, retry_count, repair_retry_count, last_error, scheduled_check_at)
		VALUES ('a.mkv', 'checking', ?, 2, 1, 'old', ?)`, last, time.Now().UTC().Add(-time.Hour))
	require.NoError(t, err)

	until := time.Now().UTC().Add(72 * time.Hour)
	checking := HealthStatusChecking
	require.NoError(t, repo.UpdateHealthStatusBulk(ctx, []HealthStatusUpdate{{
		Type: UpdateTypeDeferred, FilePath: "a.mkv", Status: HealthStatusHealthy,
		ScheduledCheckAt: until, ErrorDetails: WithPeerDeferral(nil, time.Now().UTC()), ExpectedStatus: &checking,
	}}))

	fh, err := repo.GetFileHealth(ctx, "a.mkv")
	require.NoError(t, err)
	assert.Equal(t, HealthStatusHealthy, fh.Status, "status restored")
	at := scheduledAt(t, repo, "a.mkv")
	require.NotNil(t, at)
	assert.WithinDuration(t, until, *at, time.Second)
	require.NotNil(t, fh.LastChecked)
	assert.WithinDuration(t, last, *fh.LastChecked, time.Second, "a deferral is not a check")
	assert.Equal(t, 2, fh.RetryCount)
	assert.Equal(t, 1, fh.RepairRetryCount)
	require.NotNil(t, fh.LastError)
	assert.Equal(t, "old", *fh.LastError)
	assert.True(t, HasPeerDeferral(fh.ErrorDetails))

	// Guarded: a record that left 'checking' is not touched.
	_, err = repo.db.ExecContext(ctx, `UPDATE file_health SET status = 'pending' WHERE file_path = 'a.mkv'`)
	require.NoError(t, err)
	require.NoError(t, repo.UpdateHealthStatusBulk(ctx, []HealthStatusUpdate{{
		Type: UpdateTypeDeferred, FilePath: "a.mkv", Status: HealthStatusHealthy,
		ScheduledCheckAt: until.Add(time.Hour), ExpectedStatus: &checking,
	}}))
	fh, err = repo.GetFileHealth(ctx, "a.mkv")
	require.NoError(t, err)
	assert.Equal(t, HealthStatusPending, fh.Status)
}

func TestUpdateHealthStatusBulk_HealthyClearsPeerDeferral(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()
	_, err := repo.db.ExecContext(ctx, `INSERT INTO file_health (file_path, status, error_details)
		VALUES ('a.mkv', 'checking', ?)`, *WithPeerDeferral(nil, time.Now().UTC()))
	require.NoError(t, err)
	require.NoError(t, repo.UpdateHealthStatusBulk(ctx, []HealthStatusUpdate{{
		Type: UpdateTypeHealthy, FilePath: "a.mkv", ScheduledCheckAt: time.Now().UTC().Add(time.Hour),
	}}))
	fh, err := repo.GetFileHealth(ctx, "a.mkv")
	require.NoError(t, err)
	assert.False(t, HasPeerDeferral(fh.ErrorDetails), "a real check ends the deferral")
}

func TestBringCheckForward(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	insert := func(path, status string, at *time.Time) {
		_, err := repo.db.ExecContext(ctx, `INSERT INTO file_health (file_path, status, scheduled_check_at, retry_count, last_error)
			VALUES (?, ?, ?, 0, NULL)`, path, status, at)
		require.NoError(t, err)
	}
	later := now.Add(48 * time.Hour)
	sooner := now.Add(time.Hour)
	insert("later.mkv", "healthy", &later)
	insert("sooner.mkv", "healthy", &sooner)
	insert("none.mkv", "healthy", nil)
	insert("pending.mkv", "pending", &later)
	insert("corrupt.mkv", "corrupted", nil)

	moved, err := repo.BringCheckForward(ctx, "later.mkv", now)
	require.NoError(t, err)
	assert.True(t, moved)
	fh, _ := repo.GetFileHealth(ctx, "later.mkv")
	assert.Equal(t, HealthStatusHealthy, fh.Status, "never writes a status")
	assert.WithinDuration(t, now, *scheduledAt(t, repo, "later.mkv"), time.Second)

	moved, err = repo.BringCheckForward(ctx, "sooner.mkv", now.Add(2*time.Hour))
	require.NoError(t, err)
	assert.False(t, moved, "never moves a check later")
	assert.WithinDuration(t, sooner, *scheduledAt(t, repo, "sooner.mkv"), time.Second)

	moved, err = repo.BringCheckForward(ctx, "none.mkv", now)
	require.NoError(t, err)
	assert.True(t, moved, "a healthy file with no schedule becomes due")

	for _, p := range []string{"pending.mkv", "corrupt.mkv", "missing.mkv"} {
		moved, err = repo.BringCheckForward(ctx, p, now)
		require.NoError(t, err)
		assert.False(t, moved, p)
	}
	assert.WithinDuration(t, later, *scheduledAt(t, repo, "pending.mkv"), time.Second)
}
