package usenet

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
	"github.com/javi11/nntppool/v5"
)

var randPerm = rand.Perm

// SelectSegmentsForValidation is the exported form of the sampling selector.
// It returns the subset of segments that should be validated based on samplePercentage,
// applying the same first-3 / last-2 / random-middle strategy used internally.
func SelectSegmentsForValidation(segments []*metapb.SegmentData, samplePercentage int) []*metapb.SegmentData {
	return selectSegmentsForValidation(withoutPlaceholders(segments), samplePercentage)
}

// withoutPlaceholders drops gap placeholders (articles the NZB never listed)
// from a sweep's candidates: they name nothing a provider could answer for,
// and their absence is already recorded as a known hole at import.
func withoutPlaceholders(segments []*metapb.SegmentData) []*metapb.SegmentData {
	n := 0
	for _, seg := range segments {
		if seg != nil && holes.IsPlaceholderID(seg.Id) {
			n++
		}
	}
	if n == 0 {
		return segments
	}
	out := make([]*metapb.SegmentData, 0, len(segments)-n)
	for _, seg := range segments {
		if seg == nil || holes.IsPlaceholderID(seg.Id) {
			continue
		}
		out = append(out, seg)
	}
	return out
}

// ValidationResult holds the outcome of a segment availability sweep for one
// file. Segments fall into exactly three buckets: available, confirmed
// missing (a 430/423 from the provider), and unresolved (a transport or
// infrastructure failure, or an id the sweep never got to). TotalChecked
// counts only the two resolved buckets, so a segment we learned nothing about
// never dilutes the observed miss rate.
type ValidationResult struct {
	TotalChecked    int
	MissingCount    int
	UnresolvedCount int
	MissingIDs      []string
	// TerminatedEarly reports that the sweep stopped short of this file's
	// planned samples because its verdict was already settled.
	TerminatedEarly bool
	// Err is the first operational (non-definitive) STAT failure observed for
	// this file — a dead connection, timeout, cancellation, auth failure,
	// quota, or an id the sweep never answered for. Non-nil marks the whole
	// result inconclusive: it is not evidence of anything and must not be
	// read as a missing-segment verdict (issue #861).
	Err error
}

// Inconclusive reports whether the sweep failed to reach a definitive answer
// for at least one checked segment. An inconclusive result is not evidence of
// anything — neither of missing articles nor of a healthy file — so callers
// must retry rather than record a verdict from it.
func (r ValidationResult) Inconclusive() bool { return r.Err != nil }

// errSegmentUnreported marks segments the pool never answered for. StatMany
// stops dispatching and drops in-flight results when its context ends, so a
// cancelled or timed-out sweep silently returns fewer results than ids.
var errSegmentUnreported = errors.New("segment availability not reported by the STAT sweep")

// unreportedErr wraps the sweep's context error (when there is one) so the
// caller can tell a truncated sweep from a per-segment failure.
func unreportedErr(ctxErr error) error {
	if ctxErr != nil {
		return fmt.Errorf("%w: %w", errSegmentUnreported, ctxErr)
	}
	return errSegmentUnreported
}

// maxTrackedMissingIDs caps the message-ids stored per file so a wholly dead
// release cannot produce a huge metadata blob. MissingCount stays exact.
const maxTrackedMissingIDs = 50

// BatchOptions tunes a cross-file STAT sweep.
type BatchOptions struct {
	// MaxConnections bounds STATs in flight and also sets the chunk size the
	// sweep dispatches in.
	MaxConnections int

	// Timeout is the per-item budget scaled into each chunk's deadline.
	Timeout time.Duration

	// ShouldStop, when non-nil, is consulted after every chunk for each file
	// still being swept. Returning true removes that file from the remaining
	// chunks, leaving its siblings unaffected. Callers must only return true
	// for a verdict that further checking cannot reverse, since the file's
	// unchecked segments are then never looked at.
	ShouldStop func(fileIdx int, result ValidationResult) bool

	// ArticleDate is when the swept release was posted, forwarded to the pool
	// so a provider whose retention does not reach back that far is not swept
	// with ids it cannot hold. Zero = unknown, which applies no policy.
	ArticleDate time.Time
}

// ValidateSegmentAvailabilityBatch checks pre-sampled segment IDs for many files
// in one interleaved STAT sweep. perFileIDs is index-aligned with the returned
// results: files with an empty ID list yield a zero ValidationResult. IDs are
// interleaved round-robin across files (every file's first sample, then every
// file's second, …) so one file with many segments cannot serialize the sweep
// for the others.
//
// The sweep dispatches in MaxConnections-sized chunks rather than one giant
// StatMany, mirroring the import fast-fail sweep. The chunk boundary is the
// only place a file can be dropped from the remaining work, so it is also what
// bounds how far past a settled verdict the sweep can run.
//
// An error is returned for infrastructure failures (pool unavailable) and for
// caller cancellation; per-segment outcomes are reported in the per-file
// results.
func ValidateSegmentAvailabilityBatch(
	ctx context.Context,
	perFileIDs [][]string,
	poolManager pool.Manager,
	opts BatchOptions,
) ([]ValidationResult, error) {
	results := make([]ValidationResult, len(perFileIDs))
	for i := range results {
		results[i].MissingIDs = []string{}
	}

	total := 0
	maxSamples := 0
	for _, ids := range perFileIDs {
		total += len(ids)
		if len(ids) > maxSamples {
			maxSamples = len(ids)
		}
	}
	if total == 0 {
		return results, nil
	}

	usenetPool, err := poolManager.GetPool()
	if err != nil {
		return results, fmt.Errorf("cannot validate segments: usenet connection pool unavailable: %w", err)
	}
	if usenetPool == nil {
		return results, fmt.Errorf("cannot validate segments: usenet connection pool is nil")
	}

	if opts.MaxConnections <= 0 {
		opts.MaxConnections = 1
	}

	// Round-robin interleave IDs across files into one flat plan. Ownership is
	// positional (ids[i] belongs to fileOf[i]) rather than keyed by message-id,
	// so an id shared by two files is attributed to each of them independently.
	ids := make([]string, 0, total)
	fileOf := make([]int, 0, total)
	for round := 0; round < maxSamples; round++ {
		for fileIdx, fileIDs := range perFileIDs {
			if round < len(fileIDs) {
				ids = append(ids, fileIDs[round])
				fileOf = append(fileOf, fileIdx)
			}
		}
	}

	nonEmptyFiles := 0
	for _, fileIDs := range perFileIDs {
		if len(fileIDs) > 0 {
			nonEmptyFiles++
		}
	}

	sweepStart := time.Now()
	slog.InfoContext(ctx, "Starting STAT sweep",
		"files", nonEmptyFiles,
		"segments", len(ids),
		"concurrency", opts.MaxConnections,
	)

	dropped := make([]bool, len(perFileIDs))
	skipped := 0

	for pos := 0; pos < len(ids); {
		chunk := make([]string, 0, opts.MaxConnections)
		chunkOwners := make([]int, 0, opts.MaxConnections)
		for pos < len(ids) && len(chunk) < opts.MaxConnections {
			fileIdx := fileOf[pos]
			if dropped[fileIdx] {
				skipped++
			} else {
				chunk = append(chunk, ids[pos])
				chunkOwners = append(chunkOwners, fileIdx)
			}
			pos++
		}
		if len(chunk) == 0 {
			continue
		}

		statCtx, cancel := context.WithTimeout(ctx, pool.StatManyTimeout(len(chunk), opts.MaxConnections, opts.Timeout))
		errByID := make(map[string]error, len(chunk))
		for r := range usenetPool.ExistsMany(statCtx, chunk, nntppool.ManyOptions{
			Concurrency: opts.MaxConnections,
			ArticleDate: opts.ArticleDate,
		}) {
			errByID[r.MessageID] = r.Err
		}
		sweepErr := statCtx.Err()
		cancel()

		// Caller cancellation invalidates the whole sweep: the remaining files
		// would otherwise be judged on evidence that was never gathered.
		if ctx.Err() != nil {
			return results, ctx.Err()
		}

		for i, id := range chunk {
			res := &results[chunkOwners[i]]
			statErr, reported := errByID[id]
			switch {
			case !reported:
				// The chunk deadline expired before this id was dispatched, so
				// StatMany abandoned it. Reachability was never proven.
				res.UnresolvedCount++
				if res.Err == nil {
					res.Err = unreportedErr(sweepErr)
				}
			case statErr == nil:
				res.TotalChecked++
				poolManager.IncArticlesDownloaded()
				poolManager.UpdateDownloadProgress("", 100)
			case IsArticleNotFound(statErr):
				res.TotalChecked++
				res.MissingCount++
				if len(res.MissingIDs) < maxTrackedMissingIDs {
					res.MissingIDs = append(res.MissingIDs, id)
				}
			default:
				// Transport or infrastructure failure. Never a confirmed miss:
				// a connection blip must not condemn a file, and reading it as
				// one persisted false holes that no later clean check could
				// clear (#861).
				slog.DebugContext(ctx, "segment check unresolved",
					"segment_id", id,
					"error", statErr,
				)
				res.UnresolvedCount++
				if res.Err == nil {
					res.Err = statErr
				}
			}
		}

		if opts.ShouldStop == nil {
			continue
		}
		for i := range perFileIDs {
			if dropped[i] || len(perFileIDs[i]) == 0 || !opts.ShouldStop(i, results[i]) {
				continue
			}
			dropped[i] = true
			// Only a file with work left to skip was genuinely cut short; one
			// that happened to settle on its final chunk ran to completion.
			if results[i].TotalChecked+results[i].UnresolvedCount < len(perFileIDs[i]) {
				results[i].TerminatedEarly = true
			}
		}
	}

	missingTotal := 0
	unresolvedTotal := 0
	for _, r := range results {
		missingTotal += r.MissingCount
		unresolvedTotal += r.UnresolvedCount
	}
	slog.InfoContext(ctx, "STAT sweep completed",
		"files", nonEmptyFiles,
		"segments", len(ids),
		"checked", len(ids)-skipped,
		"skipped_after_early_termination", skipped,
		"missing", missingTotal,
		"inconclusive", unresolvedTotal,
		"duration", time.Since(sweepStart),
	)

	return results, nil
}

// selectSegmentsForValidation determines which segments to validate based on validation mode and sample percentage.
// For full validation, returns all segments. For sampling, uses a strategic approach that:
// - Validates first 3 segments (DMCA/takedown detection)
// - Validates last 2 segments (incomplete upload detection)
// - Validates random middle segments based on samplePercentage (general integrity check)
// A minimum of 5 segments are always validated for statistical validity when sampling.
func selectSegmentsForValidation(segments []*metapb.SegmentData, samplePercentage int) []*metapb.SegmentData {
	if samplePercentage == 100 {
		return segments
	}

	totalSegments := len(segments)

	// Min 5 for statistical validity. The configured percentage is otherwise
	// honored exactly: a hard upper cap used to collapse every sub-100% setting
	// onto the same handful of segments on large releases (issue #812).
	targetSamples := max((totalSegments*samplePercentage)/100, 5)

	if targetSamples >= totalSegments {
		return segments
	}

	var toValidate []*metapb.SegmentData

	// 1. First 3 segments (DMCA/takedown detection)
	firstCount := min(3, totalSegments)
	for i := range firstCount {
		toValidate = append(toValidate, segments[i])
	}

	// 2. Last 2 segments (incomplete upload detection)
	lastCount := 2
	if firstCount+lastCount > totalSegments {
		lastCount = totalSegments - firstCount
	}
	if lastCount > 0 {
		for i := totalSegments - lastCount; i < totalSegments; i++ {
			toValidate = append(toValidate, segments[i])
		}
	}

	// 3. Random middle segments to reach target sample size
	middleStart := firstCount
	middleEnd := totalSegments - lastCount
	middleRange := middleEnd - middleStart

	if middleRange > 0 {
		randomSamples := min(targetSamples-len(toValidate), middleRange)

		if randomSamples > 0 {
			perm := randPerm(middleRange)
			for i := range randomSamples {
				toValidate = append(toValidate, segments[middleStart+perm[i]])
			}
		}
	}

	return toValidate
}
