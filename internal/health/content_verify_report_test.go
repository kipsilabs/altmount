package health

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/stretchr/testify/require"
)

func matroskaHeader() []byte {
	data := append([]byte{0x1A, 0x45, 0xDF, 0xA3, 0x42, 0x82, 0x88}, []byte("matroska")...)
	return append(data, make([]byte, 512-len(data))...)
}

// A healthy file that was actually probed must say so, otherwise the UI
// cannot distinguish "verified clean" from "never verified".
func TestJudgeValidation_HealthyRecordsContentVerificationPassed(t *testing.T) {
	hc := newTestHealthCheckerWithVerifyContentEnabled(t, &fakeContentOpener{data: matroskaHeader()})
	prep := preparedCheck{filePath: "/movie.mkv", currentStatus: database.HealthStatusPending}

	event := hc.judgeValidation(context.Background(), prep, allPresentResult(), nil)

	if event.Type != EventTypeFileHealthy {
		t.Fatalf("got event type %v, want EventTypeFileHealthy", event.Type)
	}
	if got := parseDetails(t, event).ContentVerification; got != database.ContentVerificationPassed {
		t.Errorf("got content_verification %q, want %q", got, database.ContentVerificationPassed)
	}
}

// A probe that could not complete leaves the file healthy, but the result
// must record that verification did not actually run.
func TestJudgeValidation_HealthyRecordsContentVerificationUnavailable(t *testing.T) {
	hc := newTestHealthCheckerWithVerifyContentEnabled(t, &fakeContentOpener{err: errors.New("connection reset")})
	prep := preparedCheck{filePath: "/movie.mkv", currentStatus: database.HealthStatusPending}

	event := hc.judgeValidation(context.Background(), prep, allPresentResult(), nil)

	if event.Type != EventTypeFileHealthy {
		t.Fatalf("got event type %v, want EventTypeFileHealthy", event.Type)
	}
	details := parseDetails(t, event)
	if details.ContentVerification != database.ContentVerificationUnavailable {
		t.Errorf("got content_verification %q, want %q", details.ContentVerification, database.ContentVerificationUnavailable)
	}
	if details.Message == "" {
		t.Error("expected the transient probe error to be recorded in message")
	}
}

// A file that was never eligible for probing must carry no verification
// claim at all.
func TestJudgeValidation_NonMediaFileRecordsNoContentVerification(t *testing.T) {
	hc := newTestHealthCheckerWithVerifyContentEnabled(t, &fakeContentOpener{data: make([]byte, 512)})
	prep := preparedCheck{filePath: "/release.nfo", currentStatus: database.HealthStatusPending}

	event := hc.judgeValidation(context.Background(), prep, allPresentResult(), nil)

	if event.Details != nil {
		t.Errorf("expected no details for a file that was never probed, got %q", *event.Details)
	}
}

func TestJudgeValidation_CorruptedRecordsContentVerificationFailed(t *testing.T) {
	hc := newTestHealthCheckerWithVerifyContentEnabled(t, &fakeContentOpener{data: make([]byte, 512)})
	prep := preparedCheck{filePath: "/movie.mkv", currentStatus: database.HealthStatusPending}

	event := hc.judgeValidation(context.Background(), prep, allPresentResult(), nil)

	if event.Status != database.HealthStatusCorrupted {
		t.Fatalf("got status %v, want Corrupted", event.Status)
	}
	if got := parseDetails(t, event).ContentVerification; got != database.ContentVerificationFailed {
		t.Errorf("got content_verification %q, want %q", got, database.ContentVerificationFailed)
	}
}

// A forced override must not probe through a checker that has no
// filesystem wired: calling OpenFile on a nil Opener panics.
func TestShouldVerifyContent_OverrideWithoutFilesystemDoesNotProbe(t *testing.T) {
	force := true
	enabled := true
	cfg := &config.Config{Health: config.HealthConfig{VerifyContent: &enabled}}
	hc := &HealthChecker{configGetter: func() *config.Config { return cfg }, metadataService: newContentTestMetadata(t)}
	prep := preparedCheck{filePath: "/movie.mkv", currentStatus: database.HealthStatusPending, verifyContentOverride: &force}

	if hc.shouldVerifyContent(prep) {
		t.Fatal("expected no probe when contentVerifyFS is nil, even with the override forced on")
	}

	event := hc.judgeValidation(context.Background(), prep, allPresentResult(), nil)
	if event.Type != EventTypeFileHealthy {
		t.Errorf("got event type %v, want EventTypeFileHealthy", event.Type)
	}
}

// End-to-end proof that a passing probe survives all the way to the stored
// record: the healthy write clears last_error and every stale detail, but must
// keep the verification outcome, which is the only thing that distinguishes a
// verified-clean file from an unprobed one.
func TestPerformBackgroundCheck_HealthyRecordKeepsContentVerification(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks not supported on Windows")
	}

	client := fakepool.New()
	env := newBatchTestEnv(t, t.TempDir(), client)

	enabled := true
	cfg := env.hw.configGetter()
	cfg.Health.VerifyContent = &enabled
	env.healthChecker.contentVerifyFS = &fakeContentOpener{data: matroskaHeader()}

	path := "complete/movie.mkv"
	writeHealthyFile(t, env, path)

	_, err := env.db.Exec(`
		INSERT INTO file_health (file_path, status, retry_count, max_retries, repair_retry_count, max_repair_retries, scheduled_check_at)
		VALUES (?, 'pending', 0, 3, 0, 3, datetime('now', '-1 second'))
	`, path)
	require.NoError(t, err)

	env.hw.mu.Lock()
	env.hw.running = true
	env.hw.mu.Unlock()

	require.NoError(t, env.hw.PerformBackgroundCheck(context.Background(), path, database.HealthStatusPending, nil))

	require.Eventually(t, func() bool {
		fh, err := env.healthRepo.GetFileHealth(context.Background(), path)
		return err == nil && fh != nil &&
			fh.Status == database.HealthStatusHealthy &&
			fh.ErrorDetails != nil &&
			strings.Contains(*fh.ErrorDetails, `"content_verification":"passed"`)
	}, 2*time.Second, 10*time.Millisecond, "a healthy record must retain the content-verification outcome")

	fh, err := env.healthRepo.GetFileHealth(context.Background(), path)
	require.NoError(t, err)
	require.Nil(t, fh.LastError, "a healthy record must not keep an error message")
}
