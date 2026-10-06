package par2repair

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/kipsilabs/altmount/internal/database"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
)

func newTestRepo(t *testing.T) *database.Par2RepairRepository {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	_, err = db.Exec(`
		CREATE TABLE par2_repair_jobs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			file_path TEXT NOT NULL,
			nzb_path TEXT,
			status TEXT NOT NULL DEFAULT 'pending',
			attempts INTEGER NOT NULL DEFAULT 0,
			last_error TEXT,
			failing_segment_id TEXT,
			dead_segment_ids TEXT,
			next_attempt_at TIMESTAMP,
			started_at TIMESTAMP,
			finished_at TIMESTAMP,
			release_ref TEXT,
			member_paths TEXT,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		);
		CREATE UNIQUE INDEX idx_par2_repair_active ON par2_repair_jobs(file_path)
			WHERE status IN ('pending','running') AND file_path <> '';
		CREATE UNIQUE INDEX idx_par2_repair_active_nzb ON par2_repair_jobs(nzb_path)
			WHERE status IN ('pending','running') AND nzb_path IS NOT NULL;
		CREATE UNIQUE INDEX idx_par2_repair_active_release ON par2_repair_jobs(release_ref)
			WHERE status IN ('pending','running') AND release_ref IS NOT NULL;
	`)
	if err != nil {
		t.Fatal(err)
	}
	return database.NewPar2RepairRepository(db, database.DialectSQLite)
}

func testService(t *testing.T, repo JobStore, enabled bool) *Service {
	t.Helper()
	cfg := func() Config {
		return Config{Enabled: enabled, MaxRepairRatio: 0.02, MaxMemoryMB: 256, MaxConcurrentJobs: 1}
	}
	return NewService(repo, nil, nil, NewPatchStore(t.TempDir()), cfg, testLogger())
}

func TestServiceEnqueueDisabledNoOps(t *testing.T) {
	repo := newTestRepo(t)
	s := testService(t, repo, false)
	s.Enqueue(context.Background(), "/a.mkv", "")

	job, err := repo.ClaimNext(context.Background(), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if job != nil {
		t.Fatal("disabled service must not enqueue")
	}
}

func TestServiceRunNextOutcomes(t *testing.T) {
	tests := []struct {
		name     string
		execErr  error
		wantGone bool
	}{
		{"success deletes the job", nil, true},
		{"unrepairable deletes the job", fmt.Errorf("%w: nope", ErrUnrepairable), true},
		{"nothing-to-repair deletes the job", ErrNothingToRepair, true},
		{"transient schedules retry", errors.New("nntp timeout"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := newTestRepo(t)
			s := testService(t, repo, true)
			s.execute = func(context.Context, *database.Par2RepairJob) error { return tt.execErr }

			s.Enqueue(context.Background(), "/a.mkv", "<seg@x>")
			if !s.runNext(context.Background()) {
				t.Fatal("runNext found no job")
			}

			// Terminal outcomes translate to health/queue and delete the row;
			// only a retrying job remains.
			rows, err := repo.List(context.Background(), 10)
			if err != nil {
				t.Fatal(err)
			}
			if tt.wantGone && len(rows) != 0 {
				t.Fatalf("rows = %+v, want none after a terminal outcome", rows)
			}
			if !tt.wantGone {
				if len(rows) != 1 || rows[0].Attempts != 1 {
					t.Fatalf("rows = %+v, want one retrying job with attempts=1", rows)
				}
				job, err := repo.ClaimNext(context.Background(), time.Now().UTC().Add(48*time.Hour))
				if err != nil || job == nil {
					t.Fatalf("transient failure must be claimable after backoff, err=%v", err)
				}
			}
		})
	}
}

func TestServiceAttemptsExhaustedTranslatesToHealthAndDeletes(t *testing.T) {
	repo := newTestRepo(t)
	s := testService(t, repo, true)
	healthStore := &recordingHealth{}
	s.SetHealthStore(healthStore)

	s.Enqueue(context.Background(), "/a.mkv", "")
	for i := range maxJobAttempts {
		job, err := repo.ClaimNext(context.Background(), time.Now().UTC().Add(1000*time.Hour))
		if err != nil || job == nil {
			t.Fatalf("attempt %d: expected claimable job, err=%v", i, err)
		}
		s.handleOutcome(context.Background(), job, errors.New("always fails"))
	}

	rows, err := repo.List(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("rows = %+v, want none after attempts exhausted", rows)
	}
	if len(healthStore.calls) != 1 ||
		healthStore.calls[0].status != database.HealthStatusCorrupted ||
		!strings.Contains(healthStore.calls[0].lastError, "attempts exhausted") {
		t.Fatalf("health calls = %+v, want one corrupted record carrying the reason", healthStore.calls)
	}
}

// recordingMeta is a MetadataSource fake that records UpdateFileStatus calls.
type recordingMeta struct {
	storeRef    string // when set, every file reports this release ref
	statusCalls []struct {
		path   string
		status metapb.FileStatus
	}
}

func (m *recordingMeta) ReadFileMetadata(string) (*metapb.FileMetadata, error) {
	if m.storeRef == "" {
		return nil, nil
	}
	return &metapb.FileMetadata{StoreRef: m.storeRef}, nil
}
func (m *recordingMeta) ReadStore(string) (*metapb.NzbStore, error) { return nil, nil }
func (m *recordingMeta) UpdateFileStatus(p string, s metapb.FileStatus) error {
	m.statusCalls = append(m.statusCalls, struct {
		path   string
		status metapb.FileStatus
	}{p, s})
	return nil
}

// healthCall is one recorded UpdateFileHealth invocation.
type healthCall struct {
	path      string
	status    database.HealthStatus
	lastError string
}

// recordingHealth is a HealthStore fake that records UpdateFileHealth calls.
type recordingHealth struct {
	calls []healthCall
}

func (h *recordingHealth) RecordPar2RepairFailure(ctx context.Context, filePath, reason string, _ bool) error {
	return h.UpdateFileHealth(ctx, filePath, database.HealthStatusCorrupted, &reason, nil, nil, false)
}

func (h *recordingHealth) UpdateFileHealth(_ context.Context, filePath string, status database.HealthStatus, errorMessage *string, _ *string, _ *string, _ bool) error {
	call := healthCall{path: filePath, status: status}
	if errorMessage != nil {
		call.lastError = *errorMessage
	}
	h.calls = append(h.calls, call)
	return nil
}

func TestServiceSuccessFlipsMetadataAndHealth(t *testing.T) {
	repo := newTestRepo(t)
	s := testService(t, repo, true)
	meta := &recordingMeta{}
	healthStore := &recordingHealth{}
	s.meta = meta
	s.SetHealthStore(healthStore)
	s.execute = func(context.Context, *database.Par2RepairJob) error { return nil }

	s.Enqueue(context.Background(), "/movies/a.mkv", "")
	if !s.runNext(context.Background()) {
		t.Fatal("runNext found no job")
	}

	if len(meta.statusCalls) != 1 ||
		meta.statusCalls[0].path != "/movies/a.mkv" ||
		meta.statusCalls[0].status != metapb.FileStatus_FILE_STATUS_HEALTHY {
		t.Fatalf("metadata status calls = %+v, want one healthy for /movies/a.mkv", meta.statusCalls)
	}
	if len(healthStore.calls) != 1 ||
		healthStore.calls[0].path != "/movies/a.mkv" ||
		healthStore.calls[0].status != database.HealthStatusHealthy {
		t.Fatalf("health calls = %+v, want one healthy for /movies/a.mkv", healthStore.calls)
	}
}

// Two damaged files of the same release must share one job (the repair sweeps
// the whole release anyway), and a successful repair must flip every member
// back to healthy — not just the file that triggered the job.
func TestServiceGroupsSiblingFilesIntoOneJob(t *testing.T) {
	repo := newTestRepo(t)
	s := testService(t, repo, true)
	meta := &recordingMeta{storeRef: "store-1"}
	healthStore := &recordingHealth{}
	s.meta = meta
	s.SetHealthStore(healthStore)
	s.execute = func(context.Context, *database.Par2RepairJob) error { return nil }

	s.Enqueue(context.Background(), "/movies/a.mkv", "<a@x>")
	s.Enqueue(context.Background(), "/movies/b.mkv", "<b@x>")

	rows, err := repo.List(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want one job for the whole release", len(rows))
	}

	if !s.runNext(context.Background()) {
		t.Fatal("runNext found no job")
	}

	wantPaths := map[string]bool{"/movies/a.mkv": true, "/movies/b.mkv": true}
	if len(meta.statusCalls) != 2 {
		t.Fatalf("metadata status calls = %+v, want one healthy per member", meta.statusCalls)
	}
	for _, c := range meta.statusCalls {
		if !wantPaths[c.path] || c.status != metapb.FileStatus_FILE_STATUS_HEALTHY {
			t.Fatalf("metadata status calls = %+v, want healthy for both members", meta.statusCalls)
		}
	}
	if len(healthStore.calls) != 2 {
		t.Fatalf("health calls = %+v, want one healthy per member", healthStore.calls)
	}
	for _, c := range healthStore.calls {
		if !wantPaths[c.path] || c.status != database.HealthStatusHealthy {
			t.Fatalf("health calls = %+v, want healthy for both members", healthStore.calls)
		}
	}
}

// A file can join the release's job while it is already running (the sweep
// repairs the whole release either way). The outcome bookkeeping must see the
// late joiner, not the member list snapshotted at claim time.
func TestServiceLateJoinerGetsOutcomeToo(t *testing.T) {
	repo := newTestRepo(t)
	s := testService(t, repo, true)
	meta := &recordingMeta{storeRef: "store-1"}
	healthStore := &recordingHealth{}
	s.meta = meta
	s.SetHealthStore(healthStore)

	joined := make(chan struct{})
	s.execute = func(ctx context.Context, _ *database.Par2RepairJob) error {
		// Sibling file turns out damaged mid-repair and joins the job.
		s.Enqueue(ctx, "/movies/b.mkv", "<b@x>")
		close(joined)
		return nil
	}

	s.Enqueue(context.Background(), "/movies/a.mkv", "<a@x>")
	if !s.runNext(context.Background()) {
		t.Fatal("runNext found no job")
	}
	<-joined

	if len(healthStore.calls) != 2 {
		t.Fatalf("health calls = %+v, want healthy for both members incl. the late joiner", healthStore.calls)
	}
}

// An unrepairable release must mark every member corrupted, so no sibling is
// left dangling in a repair state that will never resolve.
func TestServiceUnrepairableMarksAllMembersCorrupted(t *testing.T) {
	repo := newTestRepo(t)
	s := testService(t, repo, true)
	meta := &recordingMeta{storeRef: "store-1"}
	healthStore := &recordingHealth{}
	s.meta = meta
	s.SetHealthStore(healthStore)
	s.execute = func(context.Context, *database.Par2RepairJob) error {
		return fmt.Errorf("%w: needs 9 recovery slices, set has 2", ErrUnrepairable)
	}

	s.Enqueue(context.Background(), "/movies/a.mkv", "")
	s.Enqueue(context.Background(), "/movies/b.mkv", "")
	if !s.runNext(context.Background()) {
		t.Fatal("runNext found no job")
	}

	if len(healthStore.calls) != 2 {
		t.Fatalf("health calls = %+v, want one corrupted record per member", healthStore.calls)
	}
	for _, c := range healthStore.calls {
		if c.status != database.HealthStatusCorrupted || !strings.Contains(c.lastError, "recovery slices") {
			t.Fatalf("health calls = %+v, want corrupted with the reason for both members", healthStore.calls)
		}
	}
}

func TestServiceTransientFailureDoesNotFlipStatus(t *testing.T) {
	repo := newTestRepo(t)
	s := testService(t, repo, true)
	meta := &recordingMeta{}
	healthStore := &recordingHealth{}
	s.meta = meta
	s.SetHealthStore(healthStore)
	s.execute = func(context.Context, *database.Par2RepairJob) error { return errors.New("nntp timeout") }

	s.Enqueue(context.Background(), "/movies/a.mkv", "")
	if !s.runNext(context.Background()) {
		t.Fatal("runNext found no job")
	}

	if len(meta.statusCalls) != 0 {
		t.Fatalf("metadata status calls = %+v, want none on transient failure", meta.statusCalls)
	}
	if len(healthStore.calls) != 0 {
		t.Fatalf("health calls = %+v, want none on transient failure", healthStore.calls)
	}
}

// An unrepairable verdict must survive the job row's deletion: it lands on
// the file's health record, which is where the user sees why.
func TestServiceUnrepairableTranslatesToHealth(t *testing.T) {
	repo := newTestRepo(t)
	s := testService(t, repo, true)
	meta := &recordingMeta{}
	healthStore := &recordingHealth{}
	s.meta = meta
	s.SetHealthStore(healthStore)
	s.execute = func(context.Context, *database.Par2RepairJob) error {
		return fmt.Errorf("%w: damage ratio 0.8810 exceeds max_repair_ratio 0.0500", ErrUnrepairable)
	}

	s.Enqueue(context.Background(), "/movies/a.mkv", "")
	if !s.runNext(context.Background()) {
		t.Fatal("runNext found no job")
	}

	if len(meta.statusCalls) != 0 {
		t.Fatalf("metadata status calls = %+v, want none on failure", meta.statusCalls)
	}
	if len(healthStore.calls) != 1 ||
		healthStore.calls[0].path != "/movies/a.mkv" ||
		healthStore.calls[0].status != database.HealthStatusCorrupted ||
		!strings.Contains(healthStore.calls[0].lastError, "damage ratio") {
		t.Fatalf("health calls = %+v, want one corrupted record carrying the reason", healthStore.calls)
	}
	rows, err := repo.List(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("rows = %+v, want none: the verdict lives on the health record", rows)
	}
}

// A nil health store must be skipped, not panic.
func TestServiceSuccessWithoutHealthStore(t *testing.T) {
	repo := newTestRepo(t)
	s := testService(t, repo, true)
	s.execute = func(context.Context, *database.Par2RepairJob) error { return nil }

	s.Enqueue(context.Background(), "/movies/a.mkv", "")
	if !s.runNext(context.Background()) {
		t.Fatal("runNext found no job")
	}
}

func TestServiceSweepDeadArticlePersistsAndUsesShortBackoff(t *testing.T) {
	repo := newTestRepo(t)
	s := testService(t, repo, true)

	// First attempt: plain transient failure bumps attempts to 1 so the
	// exponential backoff (baseBackoff<<1 = 2m) diverges from the short one.
	s.execute = func(context.Context, *database.Par2RepairJob) error { return errors.New("nntp timeout") }
	s.Enqueue(context.Background(), "/movies/a.mkv", "")
	if !s.runNext(context.Background()) {
		t.Fatal("runNext found no job")
	}

	// Second attempt: sweep discovers a dead article.
	s.execute = func(context.Context, *database.Par2RepairJob) error {
		return &SweepDeadArticleError{MessageID: "<dead@x>", Err: errors.New("430 no such article")}
	}
	// Make the retry due immediately by claiming past the first backoff.
	job, err := repo.ClaimNext(context.Background(), time.Now().UTC().Add(48*time.Hour))
	if err != nil || job == nil {
		t.Fatalf("expected claimable job, err=%v", err)
	}
	// Drive the execute + bookkeeping through the service path directly.
	s.handleOutcome(context.Background(), job, s.execute(context.Background(), job))

	// Short backoff: due after baseBackoff, well before the exponential 2m.
	notDue, err := repo.ClaimNext(context.Background(), time.Now().UTC().Add(30*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if notDue != nil {
		t.Fatalf("job claimable before short backoff elapsed: %+v", notDue)
	}
	got, err := repo.ClaimNext(context.Background(), time.Now().UTC().Add(baseBackoff+10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("job not claimable after short backoff; exponential backoff used instead")
	}
	dead := got.DeadSegments()
	if len(dead) != 1 || dead[0] != "<dead@x>" {
		t.Fatalf("dead segments = %v, want [<dead@x>]", dead)
	}
}

func TestMergeDeadIDs(t *testing.T) {
	got := mergeDeadIDs([]string{"<a@x>", "<b@x>"}, []string{"<b@x>", "<c@x>"})
	want := []string{"<a@x>", "<b@x>", "<c@x>"}
	if len(got) != len(want) {
		t.Fatalf("mergeDeadIDs = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("mergeDeadIDs = %v, want %v", got, want)
		}
	}
	if r := mergeDeadIDs(nil, nil); len(r) != 0 {
		t.Fatalf("mergeDeadIDs(nil, nil) = %v, want empty", r)
	}
}

func TestServiceWakeChannelDoesNotBlock(t *testing.T) {
	repo := newTestRepo(t)
	s := testService(t, repo, true)
	// Multiple enqueues without a running worker must not deadlock.
	for i := range 10 {
		s.Enqueue(context.Background(), fmt.Sprintf("/f%d.mkv", i), "")
	}
}

// NZB-mode jobs resolve from the NZB, not from file metadata (which does not
// exist for a release that was never imported).
func TestServiceExecutesNzbModeJob(t *testing.T) {
	repo := newTestRepo(t)
	s := testService(t, repo, true)

	var sawNzbPath string
	s.resolveNzb = func(_ context.Context, nzbPath string, _ []string, _ JobProgress) (*Resolution, error) {
		sawNzbPath = nzbPath
		return nil, ErrNothingToRepair // terminal, keeps the test focused on routing
	}

	if _, err := repo.EnqueueNzb(context.Background(), "/nzbs/rel.nzb", "<dead@x>"); err != nil {
		t.Fatal(err)
	}
	job, err := repo.ClaimNext(context.Background(), time.Now().UTC())
	if err != nil || job == nil {
		t.Fatalf("claim: %v", err)
	}
	if err := s.executeJob(context.Background(), job); !errors.Is(err, ErrNothingToRepair) {
		t.Fatalf("err = %v", err)
	}
	if sawNzbPath != "/nzbs/rel.nzb" {
		t.Fatalf("resolveNzb called with %q, want the job's NZB path", sawNzbPath)
	}
}

// recordingResumer is an ImportResumer fake that records both calls.
type recordingResumer struct {
	resumed []string
	failed  []struct{ nzb, reason string }
}

func (r *recordingResumer) ResumeWaitingRepair(_ context.Context, nzbPath string) error {
	r.resumed = append(r.resumed, nzbPath)
	return nil
}

func (r *recordingResumer) FailWaitingRepair(_ context.Context, nzbPath, reason string) error {
	r.failed = append(r.failed, struct{ nzb, reason string }{nzbPath, reason})
	return nil
}

// Cancelling a running job must stop it, delete the row without scheduling a
// retry, and return only after the job goroutine has unwound (which is what
// runs RunJob's arena cleanup defers).
func TestServiceCancelRunningJob(t *testing.T) {
	repo := newTestRepo(t)
	s := testService(t, repo, true)
	healthStore := &recordingHealth{}
	s.SetHealthStore(healthStore)

	started := make(chan struct{})
	unwound := make(chan struct{})
	s.execute = func(ctx context.Context, _ *database.Par2RepairJob) error {
		close(started)
		<-ctx.Done()
		close(unwound)
		return ctx.Err()
	}

	s.Enqueue(context.Background(), "/movies/a.mkv", "")
	go s.runNext(context.Background())
	<-started

	rows, err := repo.List(context.Background(), 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("list before cancel: err=%v rows=%d", err, len(rows))
	}
	jobID := rows[0].ID
	if err := s.Cancel(context.Background(), jobID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	select {
	case <-unwound:
	default:
		t.Fatal("Cancel returned before the job goroutine unwound")
	}

	rows, err = repo.List(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("rows = %+v, want none after cancel", rows)
	}
	if len(healthStore.calls) != 0 {
		t.Fatalf("health calls = %+v, want none: cancel is a plain stop", healthStore.calls)
	}
	if _, ok := s.Progress(jobID); ok {
		t.Fatal("progress not cleared after cancel")
	}
}

// An NZB-mode job parks an import; cancelling must release it as failed so it
// cannot wait in waiting_repair forever.
func TestServiceCancelRunningNzbJobFailsParkedImport(t *testing.T) {
	repo := newTestRepo(t)
	s := testService(t, repo, true)
	resumer := &recordingResumer{}
	s.SetImportResumer(resumer)

	started := make(chan struct{})
	s.execute = func(ctx context.Context, _ *database.Par2RepairJob) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}

	if _, err := repo.EnqueueNzb(context.Background(), "/nzbs/rel.nzb", "<dead@x>"); err != nil {
		t.Fatal(err)
	}
	go s.runNext(context.Background())
	<-started

	rows, err := repo.List(context.Background(), 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("list: err=%v rows=%d", err, len(rows))
	}
	if err := s.Cancel(context.Background(), rows[0].ID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	if len(resumer.resumed) != 0 {
		t.Fatalf("resumed = %v, want none on cancel", resumer.resumed)
	}
	if len(resumer.failed) != 1 ||
		resumer.failed[0].nzb != "/nzbs/rel.nzb" ||
		resumer.failed[0].reason != cancelReason {
		t.Fatalf("failed = %+v, want one /nzbs/rel.nzb with the cancel reason", resumer.failed)
	}
}

// A pending job is held by no worker: Cancel deletes it directly.
func TestServiceCancelPendingJob(t *testing.T) {
	repo := newTestRepo(t)
	s := testService(t, repo, true)

	s.Enqueue(context.Background(), "/movies/a.mkv", "")
	rows, err := repo.List(context.Background(), 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("list: err=%v rows=%d", err, len(rows))
	}
	if err := s.Cancel(context.Background(), rows[0].ID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	rows, err = repo.List(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("rows = %+v, want none after cancelling a pending job", rows)
	}
}

func TestServiceCancelUnknownJob(t *testing.T) {
	repo := newTestRepo(t)
	s := testService(t, repo, true)
	if err := s.Cancel(context.Background(), 4242); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("Cancel(unknown) = %v, want ErrJobNotFound", err)
	}
}

// Cancel removes the solver scratch directory once nothing is running.
func TestServiceCancelSweepsScratch(t *testing.T) {
	repo := newTestRepo(t)
	s := testService(t, repo, true)
	scratch := s.store.ScratchDir()
	if err := os.MkdirAll(scratch, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(scratch, ".par2repair-1.mem"), []byte("arena"), 0o644); err != nil {
		t.Fatal(err)
	}

	s.Enqueue(context.Background(), "/movies/a.mkv", "")
	rows, err := repo.List(context.Background(), 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("list: err=%v rows=%d", err, len(rows))
	}
	if err := s.Cancel(context.Background(), rows[0].ID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if _, err := os.Stat(scratch); !os.IsNotExist(err) {
		t.Fatalf("scratch dir survived cancel, stat err = %v", err)
	}
}

// A job that hangs must not occupy its worker forever: the service bounds
// every job with a wall-clock timeout, and a timed-out job is a transient
// failure that schedules a retry.
func TestServiceJobTimeoutSchedulesRetry(t *testing.T) {
	repo := newTestRepo(t)
	s := testService(t, repo, true)
	s.jobTimeout = 50 * time.Millisecond
	s.execute = func(ctx context.Context, _ *database.Par2RepairJob) error {
		<-ctx.Done() // a stuck job only ends when the timeout fires
		return ctx.Err()
	}

	s.Enqueue(context.Background(), "/a.mkv", "<seg@x>")
	done := make(chan bool, 1)
	go func() { done <- s.runNext(context.Background()) }()
	select {
	case worked := <-done:
		if !worked {
			t.Fatal("runNext found no job")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runNext did not return: job timeout never fired")
	}

	rows, err := repo.List(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Attempts != 1 {
		t.Fatalf("rows = %+v, want one retrying job with attempts=1", rows)
	}
}

// Stage progress tracks when the current stage started: carried forward while
// the same stage advances, reset on a stage change or when the counter
// regresses (a re-sweep), so ETA extrapolation never divides by another
// stage's elapsed time.
func TestSetProgressTracksStageStart(t *testing.T) {
	s := &Service{}
	t0 := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	now := t0
	s.now = func() time.Time { return now }

	s.setProgress(1, StageChecking, 1, 10)
	p, ok := s.Progress(1)
	if !ok || !p.StageStartedAt.Equal(t0) {
		t.Fatalf("StageStartedAt = %v, want %v", p.StageStartedAt, t0)
	}

	now = t0.Add(time.Minute)
	s.setProgress(1, StageChecking, 5, 10)
	if p, _ = s.Progress(1); !p.StageStartedAt.Equal(t0) {
		t.Fatalf("same-stage advance reset StageStartedAt to %v, want %v", p.StageStartedAt, t0)
	}

	s.setProgress(1, StageRepairing, 1, 100)
	if p, _ = s.Progress(1); !p.StageStartedAt.Equal(t0.Add(time.Minute)) {
		t.Fatalf("stage change: StageStartedAt = %v, want %v", p.StageStartedAt, t0.Add(time.Minute))
	}

	now = t0.Add(2 * time.Minute)
	s.setProgress(1, StageRepairing, 50, 100)
	s.setProgress(1, StageRepairing, 1, 100) // re-sweep: counter regressed
	if p, _ = s.Progress(1); !p.StageStartedAt.Equal(t0.Add(2 * time.Minute)) {
		t.Fatalf("counter regression: StageStartedAt = %v, want %v", p.StageStartedAt, t0.Add(2*time.Minute))
	}
}
