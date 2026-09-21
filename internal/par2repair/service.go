package par2repair

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/javi11/nzbparser"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/metadata"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/nzbfile"
)

// Config is the repair policy snapshot the service reads per job (so config
// hot-reloads apply to the next job without a restart).
type Config struct {
	Enabled           bool
	MaxRepairRatio    float64
	MaxMemoryMB       int
	MaxConcurrentJobs int
	// MaxConnections bounds how many article fetches a job keeps in flight
	// (the repair connection budget enforces the pool-wide bound).
	MaxConnections int
	// MinReleaseSizeMB / MaxReleaseSizeMB bound the size of releases repair
	// takes on; 0 means unbounded on that side.
	MinReleaseSizeMB int
	MaxReleaseSizeMB int
	// MaxPatchStoreMB bounds the on-disk patch store; <= 0 means unlimited.
	MaxPatchStoreMB int
}

// caps translates the service policy into planner caps.
func (c Config) caps() Caps {
	return Caps{
		MaxRepairRatio:      c.MaxRepairRatio,
		MaxMemoryBytes:      int64(c.MaxMemoryMB) << 20,
		MinReleaseSizeBytes: int64(c.MinReleaseSizeMB) << 20,
		MaxReleaseSizeBytes: int64(c.MaxReleaseSizeMB) << 20,
	}
}

// JobStore is the persistence the service needs (satisfied by
// database.Par2RepairRepository).
type JobStore interface {
	Enqueue(ctx context.Context, filePath, releaseRef, failingSegmentID string) (created, attached bool, err error)
	ClaimNext(ctx context.Context, now time.Time) (*database.Par2RepairJob, error)
	Get(ctx context.Context, id int64) (*database.Par2RepairJob, error)
	Delete(ctx context.Context, id int64) error
	DeleteFinished(ctx context.Context) error
	MarkRetry(ctx context.Context, id int64, reason string, nextAttempt time.Time) error
	AppendDeadSegment(ctx context.Context, id int64, messageID string) error
	EnqueueNzb(ctx context.Context, nzbPath string, failingSegmentID string) (bool, error)
	ResetRunning(ctx context.Context) error
}

// ImportResumer returns an import parked pending a repair to the normal queue,
// or fails it when the damage proved unrepairable. Optional: nil skips resume.
type ImportResumer interface {
	ResumeWaitingRepair(ctx context.Context, nzbPath string) error
	FailWaitingRepair(ctx context.Context, nzbPath string, reason string) error
}

// HealthStore updates a file's health record (satisfied by
// database.HealthRepository). Optional: nil skips health updates.
type HealthStore interface {
	UpdateFileHealth(ctx context.Context, filePath string, status database.HealthStatus, errorMessage *string, sourceNzbPath *string, errorDetails *string, noRetry bool) error
}

// MetadataSource reads file metadata and the shared NzbStore (satisfied by
// the adapter over metadata.MetadataService).
type MetadataSource interface {
	ReadFileMetadata(virtualPath string) (*metapb.FileMetadata, error)
	ReadStore(ref string) (*metapb.NzbStore, error)
	UpdateFileStatus(virtualPath string, status metapb.FileStatus) error
}

// NewMetadataSource adapts a metadata.MetadataService.
func NewMetadataSource(ms *metadata.MetadataService) MetadataSource {
	return metadataSource{ms: ms}
}

type metadataSource struct{ ms *metadata.MetadataService }

func (m metadataSource) ReadFileMetadata(p string) (*metapb.FileMetadata, error) {
	return m.ms.ReadFileMetadata(p)
}
func (m metadataSource) ReadStore(ref string) (*metapb.NzbStore, error) {
	return m.ms.Store().ReadStore(ref)
}
func (m metadataSource) UpdateFileStatus(p string, status metapb.FileStatus) error {
	return m.ms.UpdateFileStatus(p, status)
}

const (
	idlePoll       = 5 * time.Second
	baseBackoff    = time.Minute
	maxBackoff     = 6 * time.Hour
	maxJobAttempts = 8

	// defaultJobTimeout bounds one repair job's wall clock. Reading a whole
	// release through the article pipeline is slow on purpose-large posts,
	// but a job that has not finished in this long is stuck, not slow; the
	// timeout surfaces it as a transient failure so the job retries.
	defaultJobTimeout = 2 * time.Hour
)

// ErrJobNotFound reports that no active repair job has the requested ID —
// either it never existed or it finished moments earlier.
var ErrJobNotFound = errors.New("par2repair: job not found")

// cancelReason is recorded on an import parked by an NZB-mode job whose repair
// the user cancelled.
const cancelReason = "repair cancelled by user"

// cancelUnwindTimeout bounds how long Cancel waits for a running job to
// unwind. Unwinding runs RunJob's arena cleanup defers, so Cancel waits rather
// than returning while scratch files are still mapped.
const cancelUnwindTimeout = 30 * time.Second

// runningJob is the live handle on a job a worker is executing, so Cancel can
// stop it and wait for its cleanup defers to run.
type runningJob struct {
	cancel    context.CancelFunc
	done      chan struct{} // closed once the worker has unwound
	cancelled bool          // set by Cancel, read by the worker
}

// Service owns the repair queue: triggers enqueue files, worker goroutines
// claim jobs, resolve them against metadata and run the repair pipeline.
type Service struct {
	repo    JobStore
	meta    MetadataSource
	store   *PatchStore
	fetcher ArticleFetcher
	cfg     func() Config
	log     *slog.Logger
	wake    chan struct{}
	health  HealthStore   // optional; nil skips health-record updates
	resumer ImportResumer // optional; releases imports parked pending a repair

	// streamsActive is the optional playback-activity signal jobs yield to
	// (fetch depth and fold width); nil runs jobs at full speed.
	streamsActive func() bool

	// progress holds live stage progress per running job ID.
	progressMu sync.Mutex
	progress   map[int64]JobProgressSnapshot

	// running holds the live handle on each job a worker is executing.
	runningMu sync.Mutex
	running   map[int64]*runningJob

	// resolveNzb plans an NZB-mode repair; replaced in tests. The default
	// parses the NZB from disk and calls ResolveFromNzb.
	resolveNzb func(ctx context.Context, nzbPath string, deadIDs []string, progress JobProgress) (*Resolution, error)

	// execute runs one claimed job; replaced in tests. The default is
	// (*Service).executeJob.
	execute func(ctx context.Context, job *database.Par2RepairJob) error

	// now is the clock used for stage-progress timestamps; nil means wall
	// clock. Replaced in tests.
	now func() time.Time

	// jobTimeout bounds one job's wall clock. A job that has not finished in
	// this long is stuck, not slow: reads hang on pool-level timeouts, so a
	// healthy job always makes progress. Shrunk in tests.
	jobTimeout time.Duration
}

// NewService wires the repair service. fetcher is typically a PoolFetcher.
func NewService(repo JobStore, meta MetadataSource, fetcher ArticleFetcher, store *PatchStore, cfg func() Config, log *slog.Logger) *Service {
	s := &Service{
		repo:       repo,
		meta:       meta,
		store:      store,
		fetcher:    fetcher,
		cfg:        cfg,
		log:        log,
		wake:       make(chan struct{}, 1),
		jobTimeout: defaultJobTimeout,
	}
	s.execute = s.executeJob
	s.resolveNzb = s.resolveNzbFromDisk
	return s
}

// PatchStore exposes the store for read-path wiring.
func (s *Service) PatchStore() *PatchStore { return s.store }

// SetHealthStore wires the optional health repository so successful repairs
// flip the file's health record back to healthy. Nil-safe: unset skips it.
func (s *Service) SetHealthStore(h HealthStore) { s.health = h }

// SetStreamsActive wires the playback-activity signal (typically the stream
// tracker's active count) so running jobs throttle their fetch depth and
// solver fold width while anything is streaming. Nil-safe: unset means jobs
// always run at full speed.
func (s *Service) SetStreamsActive(active func() bool) { s.streamsActive = active }

// SetImportResumer wires the import queue so NZB-mode repairs release the
// import they were queued for. Nil-safe.
func (s *Service) SetImportResumer(r ImportResumer) { s.resumer = r }

// EnqueueNzb registers a never-imported release for repair, planned from its
// NZB. Used when an import defers an archive set whose volumes have missing
// articles.
func (s *Service) EnqueueNzb(ctx context.Context, nzbPath string, failingSegmentID string) {
	if !s.cfg().Enabled {
		return
	}
	created, err := s.repo.EnqueueNzb(ctx, nzbPath, failingSegmentID)
	if err != nil {
		s.log.ErrorContext(ctx, "Failed to enqueue NZB par2 repair", "error", err, "nzb", nzbPath)
		return
	}
	if created {
		s.log.InfoContext(ctx, "Queued PAR2 repair for deferred import", "nzb", nzbPath)
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}
}

// Enqueue registers a file for repair. Safe from any goroutine; a no-op when
// the feature is disabled or the file is already covered by an active job.
// Files of the same release share one job — the repair sweeps the whole
// release anyway, so the file joins the release's active job when one exists
// instead of creating a duplicate. failingSegmentID may be empty.
func (s *Service) Enqueue(ctx context.Context, filePath string, failingSegmentID string) {
	if !s.cfg().Enabled {
		return
	}
	created, attached, err := s.repo.Enqueue(ctx, filePath, s.releaseRef(filePath), failingSegmentID)
	if err != nil {
		s.log.ErrorContext(ctx, "Failed to enqueue par2 repair", "error", err, "file", filePath)
		return
	}
	if attached {
		// The release's job repairs this file too; no new work to wake for.
		s.log.InfoContext(ctx, "Joined existing PAR2 repair for the release",
			"file", filePath, "failing_segment", failingSegmentID)
		return
	}
	if created {
		s.log.InfoContext(ctx, "Queued PAR2 repair", "file", filePath, "failing_segment", failingSegmentID)
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}
}

// releaseRef is the repair grouping key: the file's NzbStore ref, shared by
// every file imported from the same NZB release. Best-effort — a file without
// readable metadata simply gets no grouping, never an error.
func (s *Service) releaseRef(filePath string) string {
	if s.meta == nil {
		return ""
	}
	fm, err := s.meta.ReadFileMetadata(filePath)
	if err != nil || fm == nil {
		return ""
	}
	return fm.StoreRef
}

// Start recovers interrupted jobs and runs worker loops until ctx ends.
func (s *Service) Start(ctx context.Context) {
	if err := s.repo.ResetRunning(ctx); err != nil {
		s.log.ErrorContext(ctx, "Failed to reset running par2 repair jobs", "error", err)
	}
	// Solver arenas from a crashed run are worthless; reclaim the disk.
	if err := os.RemoveAll(s.store.ScratchDir()); err != nil {
		s.log.ErrorContext(ctx, "Failed to clean par2 repair scratch dir", "error", err)
	}
	// Terminal rows written by older versions are history we no longer keep:
	// outcomes live on health records / import queue entries now.
	if err := s.repo.DeleteFinished(ctx); err != nil {
		s.log.ErrorContext(ctx, "Failed to delete finished par2 repair jobs", "error", err)
	}
	workers := max(s.cfg().MaxConcurrentJobs, 1)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.workerLoop(ctx)
		}()
	}
	wg.Wait()
}

func (s *Service) workerLoop(ctx context.Context) {
	ticker := time.NewTicker(idlePoll)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		if s.cfg().Enabled {
			if worked := s.runNext(ctx); worked {
				continue // drain the queue without waiting
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-s.wake:
		}
	}
}

// runNext claims and executes one job. Returns false when the queue is idle.
func (s *Service) runNext(ctx context.Context) bool {
	job, err := s.repo.ClaimNext(ctx, time.Now().UTC())
	if err != nil {
		s.log.ErrorContext(ctx, "Failed to claim par2 repair job", "error", err)
		return false
	}
	if job == nil {
		return false
	}

	start := time.Now()
	jobCtx, cancel := context.WithTimeout(ctx, s.jobTimeout)
	rj := s.registerRunning(job.ID, cancel)
	// Deferred so every return path retires the job, and so done closes only
	// after the bookkeeping below has run.
	defer s.finishRunning(job.ID, rj)

	err = s.execute(jobCtx, job)

	if s.wasCancelled(job.ID) {
		// Cancel owns the user-visible verdict; skip retry bookkeeping so the
		// row cannot be resurrected.
		s.handleCancelled(context.WithoutCancel(ctx), job)
		return true
	}
	if err != nil && !errors.Is(err, ErrUnrepairable) && !errors.Is(err, ErrNothingToRepair) && ctx.Err() != nil {
		// Shutdown mid-job: leave it running; ResetRunning reclaims at boot.
		return false
	}
	if err == nil {
		s.log.InfoContext(ctx, "PAR2 repair completed", "file", job.FilePath, "duration", time.Since(start))
	}
	s.handleOutcome(ctx, job, err)
	return true
}

// handleOutcome applies the bookkeeping for one executed job. Job rows are
// working state, not history: a terminal outcome is translated to the file's
// health record (file mode) or the import queue entry (NZB mode), then the
// row is deleted. Only retrying jobs keep a row.
func (s *Service) handleOutcome(ctx context.Context, job *database.Par2RepairJob, err error) {
	switch {
	case err == nil:
		// One job covers every damaged file of the release: flip them all.
		for _, member := range s.jobMembers(ctx, job) {
			s.markFileHealthy(ctx, member)
		}
		s.resumeImport(ctx, job)
		s.deleteJob(ctx, job.ID)
		s.pruneStore(ctx)
	case errors.Is(err, ErrUnrepairable), errors.Is(err, ErrNothingToRepair):
		s.log.WarnContext(ctx, "PAR2 repair not possible", "file", job.FilePath, "reason", err)
		for _, member := range s.jobMembers(ctx, job) {
			s.markFileUnrepairable(ctx, member, err.Error())
		}
		s.failImport(ctx, job, err.Error())
		s.deleteJob(ctx, job.ID)
	default:
		if job.Attempts+1 >= maxJobAttempts {
			s.log.ErrorContext(ctx, "PAR2 repair failed permanently after retries",
				"file", job.FilePath, "attempts", job.Attempts+1, "error", err)
			reason := "attempts exhausted: " + err.Error()
			for _, member := range s.jobMembers(ctx, job) {
				s.markFileUnrepairable(ctx, member, reason)
			}
			s.failImport(ctx, job, reason)
			s.deleteJob(ctx, job.ID)
			return
		}
		delay := min(baseBackoff<<job.Attempts, maxBackoff)
		var sweepDead *SweepDeadArticleError
		if errors.As(err, &sweepDead) {
			// Every article the sweep proved dead — the one that broke the
			// margin plus the ones it absorbed — becomes a known input to the
			// next plan, so the retry is expected to succeed: persist them all
			// and skip the exponent. Persisting only the breaking article would
			// advance the plan one article per attempt while re-reading the
			// release each time, exhausting maxJobAttempts on any release with
			// more dead articles than margin rows.
			ids := sweepDead.DeadMessageIDs()
			for _, id := range ids {
				if perr := s.repo.AppendDeadSegment(ctx, job.ID, id); perr != nil {
					s.log.ErrorContext(ctx, "Failed to persist dead segment on repair job",
						"error", perr, "job", job.ID, "message_id", id)
				}
			}
			s.log.InfoContext(ctx, "Persisted articles proved dead this sweep for the next plan",
				"job", job.ID, "count", len(ids))
			delay = baseBackoff
		}
		s.log.WarnContext(ctx, "PAR2 repair failed, will retry",
			"file", job.FilePath, "error", err, "retry_in", delay)
		if err := s.repo.MarkRetry(ctx, job.ID, err.Error(), time.Now().UTC().Add(delay)); err != nil {
			s.log.ErrorContext(ctx, "Failed to schedule repair retry", "error", err, "job", job.ID)
		}
	}
}

// jobMembers returns the job's member files as of now. Files can join the
// release's job while it runs (the sweep covers them either way), so the
// member list snapshotted at claim time may be stale; the row is re-read so a
// late joiner gets the outcome too. Falls back to the snapshot when the row
// cannot be re-read.
func (s *Service) jobMembers(ctx context.Context, job *database.Par2RepairJob) []string {
	fresh, err := s.repo.Get(ctx, job.ID)
	if err != nil || fresh == nil {
		if err != nil {
			s.log.ErrorContext(ctx, "Failed to refresh repair job members; using claim-time snapshot",
				"error", err, "job", job.ID)
		}
		return job.Members()
	}
	return fresh.Members()
}

// registerRunning publishes a running job so Cancel can reach it.
func (s *Service) registerRunning(id int64, cancel context.CancelFunc) *runningJob {
	rj := &runningJob{cancel: cancel, done: make(chan struct{})}
	s.runningMu.Lock()
	defer s.runningMu.Unlock()
	if s.running == nil {
		s.running = make(map[int64]*runningJob)
	}
	s.running[id] = rj
	return rj
}

// wasCancelled reports whether Cancel flagged this job, leaving it registered
// so Cancel keeps waiting until the bookkeeping is done.
func (s *Service) wasCancelled(id int64) bool {
	s.runningMu.Lock()
	defer s.runningMu.Unlock()
	rj := s.running[id]
	return rj != nil && rj.cancelled
}

// finishRunning retires the job: it releases the job context, drops the
// registry entry, and only then closes done. Closing done last is what makes
// Cancel's wait mean "the job unwound AND its bookkeeping landed" — close it
// any earlier and Cancel could return while the row still exists.
func (s *Service) finishRunning(id int64, rj *runningJob) {
	rj.cancel()
	s.runningMu.Lock()
	delete(s.running, id)
	s.runningMu.Unlock()
	close(rj.done)
}

// handleCancelled applies the bookkeeping for a user-cancelled job. Cancel is
// a plain stop: no health record and no metadata status are written, so the
// file is left exactly as it was and a later read may queue the repair again.
// An NZB-mode job's parked import is failed, since nothing else would ever
// release it.
func (s *Service) handleCancelled(ctx context.Context, job *database.Par2RepairJob) {
	s.log.InfoContext(ctx, "PAR2 repair cancelled", "file", job.FilePath, "job", job.ID)
	s.failImport(ctx, job, cancelReason)
	s.deleteJob(ctx, job.ID)
	s.clearProgress(job.ID)
}

// deleteJob removes a finished job row; failures are logged, never fatal.
func (s *Service) deleteJob(ctx context.Context, id int64) {
	if err := s.repo.Delete(ctx, id); err != nil {
		s.log.ErrorContext(ctx, "Failed to delete finished repair job", "error", err, "job", id)
	}
}

// markFileUnrepairable records why repair could not fix the file on its
// health record, so the verdict survives the job row's deletion and the file
// stays on the existing ARR/corruption replacement path. No-op for NZB-mode
// jobs (empty filePath): their verdict lands on the import queue entry.
func (s *Service) markFileUnrepairable(ctx context.Context, filePath, reason string) {
	if s.health == nil || filePath == "" {
		return
	}
	if err := s.health.UpdateFileHealth(ctx, filePath, database.HealthStatusCorrupted, &reason, nil, nil, false); err != nil {
		s.log.ErrorContext(ctx, "Failed to record unrepairable verdict on health record",
			"error", err, "file", filePath)
	}
}

// markFileHealthy flips the repaired file's metadata and health record back to
// healthy. Failures are logged, never fatal: the repair itself succeeded.
func (s *Service) markFileHealthy(ctx context.Context, filePath string) {
	if s.meta != nil {
		if err := s.meta.UpdateFileStatus(filePath, metapb.FileStatus_FILE_STATUS_HEALTHY); err != nil {
			s.log.ErrorContext(ctx, "Failed to flip metadata status to healthy after repair",
				"error", err, "file", filePath)
		}
	}
	if s.health != nil {
		note := "restored by PAR2 repair"
		if err := s.health.UpdateFileHealth(ctx, filePath, database.HealthStatusHealthy, nil, nil, &note, false); err != nil {
			s.log.ErrorContext(ctx, "Failed to flip health record to healthy after repair",
				"error", err, "file", filePath)
		}
	}
}

// pruneStore enforces the configured patch-store size cap after a successful
// job. Errors are logged, never fatal.
func (s *Service) pruneStore(ctx context.Context) {
	maxBytes := int64(s.cfg().MaxPatchStoreMB) << 20
	if err := s.store.Prune(maxBytes); err != nil {
		s.log.ErrorContext(ctx, "Failed to prune patch store", "error", err)
	}
}

// executeJob is the real pipeline: metadata -> resolve -> run.
func (s *Service) executeJob(ctx context.Context, job *database.Par2RepairJob) error {
	// NZB-mode: the release was never imported, so there is no metadata to
	// read — plan straight from the NZB instead.
	if job.NzbPath.Valid && job.NzbPath.String != "" {
		return s.executeNzbJob(ctx, job)
	}
	fm, err := s.meta.ReadFileMetadata(job.FilePath)
	if err != nil {
		return err
	}
	if fm == nil {
		return errors.Join(ErrUnrepairable, errors.New("file metadata not found"))
	}
	var store *metapb.NzbStore
	if fm.StoreRef != "" {
		if store, err = s.meta.ReadStore(fm.StoreRef); err != nil {
			return err
		}
	}

	deadIDs := mergeDeadIDs(deadSegmentIDs(fm, job.FailingSegmentID.String), job.DeadSegments())
	cfg := s.cfg()
	caps := cfg.caps()
	defer s.clearProgress(job.ID)
	progress := s.jobProgress(job.ID)
	fetch := s.fetcherFor(metaArticleDate(fm))
	res, err := Resolve(ctx, fm, store, deadIDs, fetch, caps, s.log, progress)
	if err != nil {
		return err
	}
	return RunJob(ctx, res.Plan, res.Index, res.Par2Files, fetch, s.store, s.log,
		WithProgress(progress), WithLiveConcurrency(func() int { return s.cfg().MaxConnections }),
		WithYieldToStreams(s.streamsActive))
}

// ReleaseBinder is an optional ArticleFetcher capability: returning a fetcher
// bound to one release's article date, so the pool can apply per-provider
// retention limits (see nntppool Provider.MaxArticleAge). A fetcher that does
// not implement it is used unchanged, which applies no retention policy.
type ReleaseBinder interface {
	ForArticleDate(at time.Time) ArticleFetcher
}

// fetcherFor binds the shared fetcher to one release's article date when it
// supports that, so a repair does not hand a short-retention provider ids it
// cannot hold.
func (s *Service) fetcherFor(at time.Time) ArticleFetcher {
	if at.IsZero() {
		return s.fetcher
	}
	binder, ok := s.fetcher.(ReleaseBinder)
	if !ok {
		return s.fetcher
	}
	return binder.ForArticleDate(at)
}

// jobProgress returns the JobProgress callback that publishes a job's live
// stage progress for the API.
func (s *Service) jobProgress(jobID int64) JobProgress {
	return func(stage Stage, done, total int) { s.setProgress(jobID, stage, done, total) }
}

// JobProgressSnapshot is a running job's stage progress: which phase it is in
// and how far through that phase it is (the unit depends on the stage).
type JobProgressSnapshot struct {
	Stage         Stage
	DoneArticles  int
	TotalArticles int
	// StageStartedAt is when the current stage began (or restarted: a re-sweep
	// resets it along with the counter), so ETAs extrapolate from this stage's
	// own rate rather than the whole attempt's elapsed time.
	StageStartedAt time.Time
}

// Progress returns the live stage progress of a running job, when known.
func (s *Service) Progress(jobID int64) (JobProgressSnapshot, bool) {
	s.progressMu.Lock()
	defer s.progressMu.Unlock()
	p, ok := s.progress[jobID]
	return p, ok
}

func (s *Service) setProgress(jobID int64, stage Stage, done, total int) {
	s.progressMu.Lock()
	defer s.progressMu.Unlock()
	if s.progress == nil {
		s.progress = make(map[int64]JobProgressSnapshot)
	}
	start := s.timeNow()
	if prev, ok := s.progress[jobID]; ok && prev.Stage == stage && done >= prev.DoneArticles {
		start = prev.StageStartedAt
	}
	s.progress[jobID] = JobProgressSnapshot{Stage: stage, DoneArticles: done, TotalArticles: total, StageStartedAt: start}
}

// timeNow is the service clock: the injected test clock when set, wall clock
// otherwise.
func (s *Service) timeNow() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now().UTC()
}

func (s *Service) clearProgress(jobID int64) {
	s.progressMu.Lock()
	defer s.progressMu.Unlock()
	delete(s.progress, jobID)
}

// executeNzbJob runs a repair planned from an NZB file.
func (s *Service) executeNzbJob(ctx context.Context, job *database.Par2RepairJob) error {
	deadIDs := mergeDeadIDs(nil, job.DeadSegments())
	if job.FailingSegmentID.Valid && job.FailingSegmentID.String != "" {
		deadIDs = mergeDeadIDs([]string{job.FailingSegmentID.String}, deadIDs)
	}
	defer s.clearProgress(job.ID)
	progress := s.jobProgress(job.ID)
	res, err := s.resolveNzb(ctx, job.NzbPath.String, deadIDs, progress)
	if err != nil {
		return err
	}
	return RunJob(ctx, res.Plan, res.Index, res.Par2Files, s.fetcher, s.store, s.log,
		WithProgress(progress), WithLiveConcurrency(func() int { return s.cfg().MaxConnections }),
		WithYieldToStreams(s.streamsActive))
}

// resolveNzbFromDisk parses the NZB at nzbPath and plans a repair from it.
func (s *Service) resolveNzbFromDisk(ctx context.Context, nzbPath string, deadIDs []string, progress JobProgress) (*Resolution, error) {
	rc, err := nzbfile.Open(nzbPath)
	if err != nil {
		return nil, fmt.Errorf("%w: open NZB %s: %v", ErrUnrepairable, nzbPath, err)
	}
	defer func() { _ = rc.Close() }()

	n, err := nzbparser.Parse(rc)
	if err != nil {
		return nil, fmt.Errorf("%w: parse NZB %s: %v", ErrUnrepairable, nzbPath, err)
	}
	caps := s.cfg().caps()
	// No known-dead segment means the trigger saw corrupt article DATA (e.g.
	// RAR analysis failed on a present article), not a missing one: run a
	// verify sweep that checks every article against the PAR2 checksums and
	// patches whatever fails.
	caps.VerifySweep = len(deadIDs) == 0
	return ResolveFromNzb(ctx, n, deadIDs, s.fetcher, caps, s.log, progress)
}

// resumeImport returns a parked import to the queue after its repair landed.
func (s *Service) resumeImport(ctx context.Context, job *database.Par2RepairJob) {
	if s.resumer == nil || !job.NzbPath.Valid || job.NzbPath.String == "" {
		return
	}
	if err := s.resumer.ResumeWaitingRepair(ctx, job.NzbPath.String); err != nil {
		s.log.ErrorContext(ctx, "Failed to resume import after repair", "error", err, "nzb", job.NzbPath.String)
		return
	}
	s.log.InfoContext(ctx, "Resuming import after successful PAR2 repair", "nzb", job.NzbPath.String)
}

// failImport fails a parked import whose repair proved impossible.
func (s *Service) failImport(ctx context.Context, job *database.Par2RepairJob, reason string) {
	if s.resumer == nil || !job.NzbPath.Valid || job.NzbPath.String == "" {
		return
	}
	if err := s.resumer.FailWaitingRepair(ctx, job.NzbPath.String, reason); err != nil {
		s.log.ErrorContext(ctx, "Failed to fail parked import", "error", err, "nzb", job.NzbPath.String)
	}
}

// mergeDeadIDs appends extra onto base, skipping duplicates, preserving order.
func mergeDeadIDs(base, extra []string) []string {
	out := base
	for _, id := range extra {
		if !slices.Contains(out, id) {
			out = append(out, id)
		}
	}
	return out
}

// deadSegmentIDs collects the trigger's failing segment plus every persisted
// known hole, mapped from segment indexes to message IDs.
func deadSegmentIDs(fm *metapb.FileMetadata, failing string) []string {
	ids := []string{}
	if failing != "" {
		ids = append(ids, failing)
	}
	for _, run := range fm.KnownHoles {
		for i := run.StartSegment; i < run.StartSegment+run.Count; i++ {
			if i >= 0 && int(i) < len(fm.SegmentData) {
				ids = append(ids, fm.SegmentData[i].Id)
			}
		}
	}
	return ids
}

// Cancel stops a queued or in-flight repair and cleans the transient artifacts
// it generated. For a running job it cancels the job's context and waits for
// the worker to unwind, so the solver's mmap scratch files are gone before
// Cancel returns; the worker then deletes the row and releases any parked
// import. For a job no worker holds, Cancel does that bookkeeping itself.
//
// Returns ErrJobNotFound when no job has that ID. Cancel is a plain stop: the
// file's health record is untouched, so a later read may queue the repair
// again.
func (s *Service) Cancel(ctx context.Context, jobID int64) error {
	s.runningMu.Lock()
	rj := s.running[jobID]
	if rj != nil {
		rj.cancelled = true
	}
	s.runningMu.Unlock()

	if rj != nil {
		rj.cancel()
		select {
		case <-rj.done:
		case <-time.After(cancelUnwindTimeout):
			return fmt.Errorf("par2repair: job %d did not stop within %s", jobID, cancelUnwindTimeout)
		case <-ctx.Done():
			return ctx.Err()
		}
		s.sweepArtifacts(ctx)
		return nil
	}

	job, err := s.repo.Get(ctx, jobID)
	if err != nil {
		return err
	}
	if job == nil {
		return ErrJobNotFound
	}
	s.log.InfoContext(ctx, "PAR2 repair cancelled before it started",
		"file", job.FilePath, "job", jobID)
	s.failImport(ctx, job, cancelReason)
	if err := s.repo.Delete(ctx, jobID); err != nil {
		return fmt.Errorf("par2repair: delete cancelled job %d: %w", jobID, err)
	}
	s.clearProgress(jobID)
	s.sweepArtifacts(ctx)
	return nil
}

// sweepArtifacts reclaims transient repair artifacts, but only while no job is
// running: with an empty registry every file under .scratch and every .tmp-*
// in the patch tree is by definition an orphan, so no per-file ownership
// tracking is needed. Failures are logged, never fatal — the repair is already
// stopped, which is what the caller asked for.
func (s *Service) sweepArtifacts(ctx context.Context) {
	s.runningMu.Lock()
	busy := len(s.running) > 0
	s.runningMu.Unlock()
	if busy {
		return
	}
	if err := s.store.RemoveScratch(); err != nil {
		s.log.ErrorContext(ctx, "Failed to clean par2 repair scratch dir", "error", err)
	}
	if err := s.store.SweepTempFiles(); err != nil {
		s.log.ErrorContext(ctx, "Failed to sweep par2 repair temp files", "error", err)
	}
}

// metaArticleDate reads a file's Usenet post date from its metadata, for the
// pool's per-provider retention limits. Metadata written before release_date
// existed carries 0, which yields the zero time: no retention policy is
// applied, rather than an age guessed from nothing.
func metaArticleDate(fm *metapb.FileMetadata) time.Time {
	if fm == nil || fm.GetReleaseDate() <= 0 {
		return time.Time{}
	}
	return time.Unix(fm.GetReleaseDate(), 0)
}
