package validation

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"github.com/kipsilabs/altmount/internal/holes"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/pool"
	"github.com/kipsilabs/altmount/internal/progress"
	"github.com/javi11/nntppool/v5"
)

const (
	// fastFailStatMaxAttempts is the least number of attempts a sweep gets
	// before a non-converging attempt ends it as inconclusive.
	fastFailStatMaxAttempts = 3
	fastFailRetryBaseDelay  = 100 * time.Millisecond
	fastFailRetryMaxDelay   = 400 * time.Millisecond
)

// fastFailStatBudget caps the wall-clock a sweep may spend across all its
// attempts. A var so tests can shorten it.
var fastFailStatBudget = 15 * time.Second

var (
	// ErrFastFailInconclusive means bounded retries could not establish whether
	// one or more sampled articles exist. It is deliberately distinct from a
	// definitive NNTP 430/423 miss so callers do not discard files as broken.
	ErrFastFailInconclusive = errors.New("fast-fail validation inconclusive")
	errFastFailUnreported   = errors.New("STAT result was not reported")
)

func isDefinitiveFastFailMiss(err error) bool {
	return errors.Is(err, nntppool.ErrArticleNotFound)
}

// statIDsWithBoundedRetries checks ids, retrying for as long as each attempt
// shrinks the unanswered set (at least fastFailStatMaxAttempts times, within
// fastFailStatBudget overall). Successful and definitively missing ids leave
// the retry set immediately; only operational errors and unreported ids are
// retried. The returned map
// contains only definitive misses. When stopOnMissing is true the first such
// miss ends the sweep, preserving the release probe's fast-fail behavior.
func statIDsWithBoundedRetries(
	ctx context.Context,
	client pool.NntpClient,
	ids []string,
	maxConnections int,
	timeout time.Duration,
	stopOnMissing bool,
	patchIdx PatchIndex,
	articleDate time.Time,
) (missing map[string]error, unverified []string, err error) {
	remaining := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		remaining = append(remaining, id)
	}

	missing = make(map[string]error)
	var lastErr error

	sweepStart := time.Now()
	for attempt := 1; len(remaining) > 0; attempt++ {
		statCtx, cancel := context.WithTimeout(ctx, pool.StatManyTimeout(len(remaining), maxConnections, timeout))
		reported := make(map[string]bool, len(remaining))
		transient := make(map[string]error, len(remaining))
		definitive := 0
		deadEarly := false

		for result := range hedgedStatMany(statCtx, client, remaining, maxConnections, articleDate) {
			if _, wanted := seen[result.MessageID]; !wanted {
				continue
			}
			reported[result.MessageID] = true
			if result.Err == nil {
				definitive++
				continue
			}
			// Repaired bytes live only in the local patch store, so an article
			// the providers dropped is still available. Reported with no error
			// recorded, it leaves the retry set as reachable.
			if patched(patchIdx, result.MessageID) {
				definitive++
				continue
			}
			if isDefinitiveFastFailMiss(result.Err) {
				missing[result.MessageID] = result.Err
				definitive++
				if stopOnMissing {
					if ctxErr := ctx.Err(); ctxErr != nil {
						cancel()
						return nil, nil, ctxErr
					}
					cancel()
					return missing, nil, nil
				}
				if releaseLooksDead(len(missing), definitive) {
					// The answers so far already condemn the release; the
					// STATs still in flight are slow 430 lookups that cannot
					// change it and only hold the import and the connections.
					deadEarly = true
					cancel()
					break
				}
				continue
			}
			transient[result.MessageID] = result.Err
			lastErr = result.Err
		}

		statErr := statCtx.Err()
		cancel()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, nil, ctxErr
		}

		next := make([]string, 0, len(remaining))
		for _, id := range remaining {
			if _, definitive := missing[id]; definitive {
				continue
			}
			if err, failed := transient[id]; failed {
				lastErr = err
				next = append(next, id)
				continue
			}
			if !reported[id] {
				if statErr != nil {
					lastErr = statErr
				} else {
					lastErr = errFastFailUnreported
				}
				next = append(next, id)
			}
		}
		converging := len(next) < len(remaining)
		remaining = next

		if len(remaining) == 0 {
			return missing, nil, nil
		}
		if deadEarly {
			return missing, remaining, fmt.Errorf("%w: %d segment(s) left unverified once %d misses condemned the release",
				ErrFastFailInconclusive, len(remaining), len(missing))
		}
		if stopOnMissing && len(missing) == 0 && len(remaining) <= tolerableUnverified(len(ids)) {
			// The release probe answers "is this post damaged?" from a
			// sample. With everything else healthy, an article whose STAT
			// neither the original request nor the priority hedge could get
			// answered inside the ceiling is slow at the provider itself;
			// waiting out further attempts held healthy imports for seconds.
			// It is handled at stream time like the articles never sampled.
			slog.InfoContext(ctx, "Fast-fail release probe proceeding with unverified stragglers",
				"unverified", len(remaining), "sampled", len(ids), "attempt", attempt)
			return missing, remaining, nil
		}
		delay := min(fastFailRetryBaseDelay<<(attempt-1), fastFailRetryMaxDelay)
		// A sweep that is still shrinking is a slow provider answering, not a
		// dead one, so it is followed until the budget runs out. It stops early
		// when an attempt past the minimum made no progress, or when the
		// definitive answers so far already condemn the release: a sweep
		// dominated by 430s is a dead post whose remaining STATs are only queued
		// behind more 430s (each one costs the provider a slow spool lookup), so
		// waiting them out adds tens of seconds and changes nothing.
		stalled := attempt >= fastFailStatMaxAttempts && !converging
		overBudget := time.Since(sweepStart)+delay >= fastFailStatBudget
		if stalled || overBudget || releaseLooksDead(len(missing), len(ids)-len(remaining)) {
			return missing, remaining, fmt.Errorf("%w: %d segment(s) remained unverified after %d attempts: %w",
				ErrFastFailInconclusive, len(remaining), attempt, lastErr)
		}

		slog.WarnContext(ctx, "Retrying inconclusive fast-fail STATs",
			"attempt", attempt+1,
			"remaining", len(remaining),
			"delay", delay,
			"error", lastErr,
		)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, nil, ctx.Err()
		case <-timer.C:
		}
	}

	return missing, nil, nil
}

// maxSweepChunk is the most STATs the per-file sweep has outstanding at once.
const maxSweepChunk = 64

// firstSweepChunk bounds the sweep's opening wave: a dead post is condemned
// by its first few misses, and every STAT past those is a slow 430 lookup left
// pipelined on a connection for the next import to queue behind.
const firstSweepChunk = 16

// Dead-post thresholds for releaseLooksDead: at least this many definitive
// misses, making up at least this share of the definitive answers so far.
const (
	deadReleaseMinMisses    = 8
	deadReleaseMissFraction = 0.5
)

// tolerableUnverified is how many sampled articles the release probe may
// leave unanswered and still pass: two of a full 64-article sample, none of a
// small one, where each article is a large share of the evidence.
func tolerableUnverified(sampled int) int {
	if sampled >= 32 {
		return 2
	}
	return 0
}

// releaseLooksDead reports whether the definitive STAT answers collected so
// far (missing out of reported) already prove the release unservable. A
// release this damaged fails the holes policy regardless of how the
// unverified remainder would answer, so the sweep can stop waiting for it.
func releaseLooksDead(missing, reported int) bool {
	return missing >= deadReleaseMinMisses && float64(missing) >= deadReleaseMissFraction*float64(reported)
}

// PlaceholderResults maps the damage an NZB declares itself: every file whose
// segments include gap placeholders (articles the NZB never listed) is reported
// Broken with those ids as known misses, without a single STAT. Index-aligned
// with files; gap-free files get the zero result. The group is not condemned
// here: an exactly-known gap is judged against the hole caps by the caller,
// which can keep a lightly holed archive set importable.
func PlaceholderResults(files []FastFailFile) []FastFailFileResult {
	results := make([]FastFailFileResult, len(files))
	for fileIdx, file := range files {
		_, placeholders := splitPlaceholders(file.Segments)
		if len(placeholders) == 0 {
			continue
		}
		results[fileIdx].Broken = true
		results[fileIdx].KnownGapCount = len(placeholders)
		for _, ph := range placeholders {
			results[fileIdx].MissingSegmentIDs = append(results[fileIdx].MissingSegmentIDs, ph.Id)
		}
	}
	return results
}

// splitPlaceholders separates a file's real segments from gap placeholders.
func splitPlaceholders(segments []*metapb.SegmentData) (real, placeholders []*metapb.SegmentData) {
	for _, seg := range segments {
		if seg == nil {
			continue
		}
		if holes.IsPlaceholderID(seg.Id) {
			placeholders = append(placeholders, seg)
		} else {
			real = append(real, seg)
		}
	}
	return real, placeholders
}

// selectFastFailSegments picks a lightweight per-file sample for the fast-fail
// reachability gate: always the first and last segment (DMCA/truncation
// detection) plus samplePercentage% of the middle. It is intentionally lighter
// than usenet.SelectSegmentsForValidation (which health checks use and which
// floors at 5 per file): fast-fail Stats run across every file in the NZB, so a
// min-5 floor multiplies badly on multi-part releases. The configured
// percentage is honored exactly — a fixed upper cap used to make every setting
// behave identically on large files (issue #812).
func selectFastFailSegments(segments []*metapb.SegmentData, samplePercentage int) []*metapb.SegmentData {
	n := len(segments)
	if n <= 2 {
		return segments
	}

	middleRange := n - 2 // sampleable indices are [1, n-2]
	middleCount := min((n*samplePercentage)/100, middleRange)

	chosen := make(map[int]struct{}, middleCount+2)
	out := make([]*metapb.SegmentData, 0, middleCount+2)
	add := func(i int) {
		if _, ok := chosen[i]; ok {
			return
		}
		chosen[i] = struct{}{}
		out = append(out, segments[i])
	}

	add(0)     // first — catches whole-article DMCA takedowns / missing files
	add(n - 1) // last — catches truncated/incomplete uploads

	if middleCount > 0 {
		perm := rand.Perm(middleRange)
		for i := 0; i < middleCount && i < len(perm); i++ {
			add(1 + perm[i])
		}
	}

	return out
}

// maxReleaseProbeSamples bounds the release probe. It answers one question —
// is anything missing? — and a release damaged enough to matter (the hole
// caps sit near 2 %) is caught by a few dozen STATs with near certainty,
// while the per-file sweep that follows a miss keeps the full sample. A
// percentage-sized probe on a large release was hundreds of STATs and most
// of a second on every import.
const maxReleaseProbeSamples = 64

// capReleaseProbeSample keeps the selector's edge picks (its first five are
// the first three and last two segments) and thins the random middle to fit.
func capReleaseProbeSample(selected []*metapb.SegmentData) []*metapb.SegmentData {
	if len(selected) <= maxReleaseProbeSamples {
		return selected
	}
	const edge = 5
	keep := make([]*metapb.SegmentData, 0, maxReleaseProbeSamples)
	keep = append(keep, selected[:edge]...)
	middle := selected[edge:]
	for _, i := range rand.Perm(len(middle))[:maxReleaseProbeSamples-edge] {
		keep = append(keep, middle[i])
	}
	return keep
}

// FastFailFile is the minimal file surface needed for early segment reachability checks.
type FastFailFile struct {
	Filename string
	Segments []*metapb.SegmentData
	// GroupKey identifies the multi-volume set this file belongs to (e.g. a RAR
	// base name). Empty means the file is standalone. When any member of a group
	// is found unreachable, FastFailCheckFiles skips the remaining Stats for that
	// group and marks every member Broken — a missing volume dooms the whole set
	// (no PAR2 repair at import time), so probing the rest is wasted work.
	GroupKey string
}

// FastFailReleaseProbe is the cheap phase-1 reachability gate for an NZB import.
// It flattens all candidate segments across the release and Stats a single
// sample (usenet.SelectSegmentsForValidation: first 3 + last 2 + random middle,
// min 5 for the whole release), cancelling the remaining Stats on the
// first definitive miss. Operational errors are retried before the probe is
// declared inconclusive.
//
// Returns (missing, err):
//   - err reports infrastructure failures, caller cancellation, or operational
//     STAT failures that remain inconclusive after bounded retries.
//   - missing reports whether any sampled segment returned a definitive 430/423,
//     so the caller can escalate to the per-file FastFailCheckFiles sweep to map
//     exactly which files are broken. Operational errors and timeouts are retried;
//     exhaustion returns ErrFastFailInconclusive. A clean release returns
//     (false, nil) and proceeds straight to parsing.
//
// PatchIndex reports whether a locally repaired copy of an article exists.
// Repaired bytes live only in AltMount's patch store, never on usenet, so a
// segment the providers have dropped still counts as available when patched —
// without this a repaired release could never be (re-)imported.
type PatchIndex interface {
	Has(messageID string) bool
}

// patched reports whether idx has a local patch for the message ID.
func patched(idx PatchIndex, messageID string) bool {
	return idx != nil && idx.Has(messageID)
}

func FastFailReleaseProbe(
	ctx context.Context,
	files []FastFailFile,
	poolManager pool.Manager,
	segmentSamplePercentage int,
	maxConnections int,
	timeout time.Duration,
	patchIdx PatchIndex,
	articleDate time.Time,
) (bool, error) {
	v, err := FastFailReleaseProbeVerdict(ctx, files, poolManager, segmentSamplePercentage, maxConnections, timeout, patchIdx, articleDate)
	return v.Missing, err
}

// FastFailFileResult records the reachability outcome for a single FastFailFile.
// Results from FastFailCheckFiles are index-aligned with the input slice.
type FastFailFileResult struct {
	Broken            bool
	MissingSegmentIDs []string // segment IDs whose Stat failed, plus known gap placeholders
	// KnownGapCount is how many of MissingSegmentIDs are gap placeholders:
	// articles the NZB never listed, known missing without a STAT. Exact,
	// not sampled, so callers judge them separately from the sample.
	KnownGapCount int
	// SampledCount is how many of the file's segments were Stat-checked (the
	// sample size), needed to project the release-wide miss rate for the
	// tolerant damage policy.
	SampledCount int
}

// FastFailCheckFiles stats a per-file sample of segments from all files.
// Every file with segments is checked — broken files are excluded from
// parsing, and if only PAR2 files survive the import fails naturally. Pass
// nil Segments for files that should be skipped (e.g. PAR2/sidecars) to keep
// index alignment while avoiding wasted Stat round-trips.
// Returns one result per input file (index-aligned). Files with no segments
// are skipped. Infrastructure failures (pool unavailable) are returned as an
// error; definitive article-not-found results mark the owning file Broken.
// Operational failures are retried, and exhaustion returns
// ErrFastFailInconclusive without marking files broken. progressTracker may be
// nil; when set it reports completed Stats as work progresses.
// stopFileOnFirstMiss condemns a file (and its group) on its first definitive
// miss and ends the sweep once no eligible file is left. Callers running with
// zero missing-segment tolerance set it: the extent of the damage cannot
// change the verdict, so mapping the rest of a doomed file is wasted work.
func FastFailCheckFiles(
	ctx context.Context,
	files []FastFailFile,
	poolManager pool.Manager,
	segmentSamplePercentage int,
	maxConnections int,
	timeout time.Duration,
	progressTracker progress.ProgressTracker,
	patchIdx PatchIndex,
	stopFileOnFirstMiss bool,
	articleDate time.Time,
) ([]FastFailFileResult, error) {
	if !poolManager.HasPool() {
		return nil, fmt.Errorf("cannot fast-fail import: usenet connection pool is nil")
	}

	usenetPool, err := poolManager.GetPool()
	if err != nil {
		return nil, fmt.Errorf("cannot fast-fail import: usenet connection pool unavailable: %w", err)
	}

	if maxConnections <= 0 {
		maxConnections = 1
	}

	results := PlaceholderResults(files)

	// brokenGroups records group keys with at least one unreachable segment, so
	// remaining Stats for those groups can be skipped in later chunks.
	brokenGroups := make(map[string]struct{})

	// brokenFiles does the same per file, for stopFileOnFirstMiss.
	brokenFiles := make(map[int]struct{})

	// Build the flat work list first so we know the total up front for progress.
	type statJob struct {
		fileIdx  int
		segID    string
		groupKey string
	}

	// Select each file's sample once, then interleave the jobs round-robin
	// across files (every file's first sample, then every file's second, …).
	// File-by-file ordering would Stat all of a broken set's parts before any
	// sibling, defeating the group short-circuit; round-robin makes the first
	// miss of a set land within roughly len(files) Stats so siblings are
	// skipped. Per-file selection already places Segments[0] first.
	perFile := make([][]*metapb.SegmentData, len(files))
	maxSamples := 0
	for fileIdx, file := range files {
		if len(file.Segments) == 0 {
			continue
		}
		real, _ := splitPlaceholders(file.Segments)
		if len(real) == 0 {
			continue
		}
		perFile[fileIdx] = selectFastFailSegments(real, segmentSamplePercentage)
		results[fileIdx].SampledCount = len(perFile[fileIdx])
		if len(perFile[fileIdx]) > maxSamples {
			maxSamples = len(perFile[fileIdx])
		}
	}

	// Files with no sample generate no jobs and are never eligible: PAR2 and
	// other sidecars reach here with nil Segments to keep index alignment.
	remaining := 0
	for _, selected := range perFile {
		if len(selected) > 0 {
			remaining++
		}
	}

	var jobs []statJob
	for round := 0; round < maxSamples; round++ {
		for fileIdx, selected := range perFile {
			if round < len(selected) {
				jobs = append(jobs, statJob{
					fileIdx:  fileIdx,
					segID:    selected[round].Id,
					groupKey: files[fileIdx].GroupKey,
				})
			}
		}
	}

	total := len(jobs)
	if total == 0 {
		return results, nil
	}

	var done, lastPct int
	advance := func() {
		done++
		if progressTracker == nil {
			return
		}
		pct := done * 100 / total
		if pct != lastPct {
			lastPct = pct
			progressTracker.Update(done, total)
		}
	}

	condemnFile := func(fileIdx int) {
		if _, already := brokenFiles[fileIdx]; already {
			return
		}
		brokenFiles[fileIdx] = struct{}{}
		if len(perFile[fileIdx]) > 0 {
			remaining--
		}
	}

	// Definitive answers accumulated across chunks, so a sweep that turns
	// inconclusive can still be settled when the release is plainly dead.
	var definitiveMissing, definitiveReported int

	// Walk the flat job list in maxConnections-sized chunks. Within a chunk,
	// every not-yet-broken job is Stat-ed together via one StatMany call;
	// brokenGroups is checked and updated between chunks, so a chunk size of 1
	// (as the short-circuit test uses) reproduces the exact per-job
	// short-circuit the previous goroutine-pool implementation gave: the
	// group is marked broken right after its first miss, and every later
	// chunk skips the rest of that group's jobs without a network round-trip.
	// Chunks are bounded below the pool's STAT pipeline capacity: a sweep that
	// pipelines hundreds of STATs over a dead release parks every connection
	// behind a queue of slow 430 lookups (each ~1 s on the server side), and
	// abandoning them does not unqueue them — the next import's own probe then
	// times out behind the backlog. Smaller waves let the dead-release verdict
	// fire after one wave with little left outstanding.
	chunkSize := min(maxConnections, maxSweepChunk)
	for start, size := 0, min(chunkSize, firstSweepChunk); start < total; start, size = start+size, chunkSize {
		end := min(start+size, total)
		chunk := jobs[start:end]

		toCheck := make([]statJob, 0, len(chunk))
		for _, job := range chunk {
			if stopFileOnFirstMiss {
				if _, broken := brokenFiles[job.fileIdx]; broken {
					advance()
					continue
				}
			}
			if job.groupKey != "" {
				if _, broken := brokenGroups[job.groupKey]; broken {
					// Group already doomed — skip the Stat but still advance
					// progress so the bar reaches 100%.
					advance()
					continue
				}
			}
			toCheck = append(toCheck, job)
		}
		if len(toCheck) == 0 {
			continue
		}

		ids := make([]string, len(toCheck))
		for i, job := range toCheck {
			ids[i] = job.segID
		}

		missingByID, unverified, err := statIDsWithBoundedRetries(ctx, usenetPool, ids, maxConnections, timeout, false, patchIdx, articleDate)
		if err != nil && !errors.Is(err, ErrFastFailInconclusive) {
			return nil, err
		}

		for _, job := range toCheck {
			if _, missing := missingByID[job.segID]; missing {
				results[job.fileIdx].Broken = true
				results[job.fileIdx].MissingSegmentIDs = append(results[job.fileIdx].MissingSegmentIDs, job.segID)
				if job.groupKey != "" {
					brokenGroups[job.groupKey] = struct{}{}
				}
				if stopFileOnFirstMiss {
					condemnFile(job.fileIdx)
					if job.groupKey != "" {
						for idx := range files {
							if files[idx].GroupKey == job.groupKey {
								condemnFile(idx)
							}
						}
					}
				}
			}
			advance()
		}

		if stopFileOnFirstMiss && remaining == 0 {
			for range jobs[end:] {
				advance()
			}
			slog.InfoContext(ctx, "Fast-fail sweep stopped early: no eligible files remain",
				"files", len(files),
				"checked", done,
				"total", total)
			break
		}

		if err == nil {
			continue
		}
		definitiveMissing += len(missingByID)
		definitiveReported += len(toCheck) - len(unverified)
		if !releaseLooksDead(definitiveMissing, definitiveReported) {
			return nil, err
		}
		// Dead release: every file still unverified is condemned with the
		// rest rather than holding the import for STATs that cannot change
		// the outcome. Only observed misses are reported as missing IDs.
		unverifiedSet := make(map[string]struct{}, len(unverified))
		for _, id := range unverified {
			unverifiedSet[id] = struct{}{}
		}
		for _, job := range toCheck {
			if _, ok := unverifiedSet[job.segID]; ok {
				results[job.fileIdx].Broken = true
				if job.groupKey != "" {
					brokenGroups[job.groupKey] = struct{}{}
				}
			}
		}
		for _, job := range jobs[end:] {
			results[job.fileIdx].Broken = true
			if job.groupKey != "" {
				brokenGroups[job.groupKey] = struct{}{}
			}
			advance()
		}
		slog.WarnContext(ctx, "Fast-fail sweep stopped early: release is dead",
			"definitive_missing", definitiveMissing,
			"definitive_reported", definitiveReported,
			"unverified", len(unverified))
		break
	}

	// Propagate set breakage: every file in a broken group is marked Broken so
	// the entire doomed set is excluded from parsing as one unit. Siblings carry
	// no synthetic MissingSegmentIDs — only segments actually observed missing
	// are reported.
	if len(brokenGroups) > 0 {
		for i := range files {
			if files[i].GroupKey == "" || results[i].Broken {
				continue
			}
			if _, broken := brokenGroups[files[i].GroupKey]; broken {
				results[i].Broken = true
			}
		}
	}

	return results, nil
}
