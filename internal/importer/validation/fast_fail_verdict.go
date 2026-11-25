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
	"github.com/kipsilabs/altmount/internal/usenet"
)

// probeFirstWave is how many sampled articles the release probe checks before
// committing the rest of the sample. A dead post — every sampled article gone —
// is the verdict after this many, and stopping there matters more than the
// one extra round trip a healthy post pays: a missing article costs the
// provider a slow spool lookup, and dozens of those left pipelined on the
// connections are what the next import's STATs queue behind.
const probeFirstWave = 8

// ProbeVerdict is what the release probe learned from its sample.
type ProbeVerdict struct {
	// Missing reports a definitive 430/423 on at least one sampled article.
	Missing bool
	// Dead reports that the first wave alone condemned the release (see
	// releaseLooksDead): the caller need not map which files are broken.
	Dead bool
	// MissingIDs are the sampled articles found missing.
	MissingIDs []string
}

// EffectiveReleaseProbeBudget resolves a release probe's check budget: d, or
// the default when d is not positive.
func EffectiveReleaseProbeBudget(d time.Duration) time.Duration {
	if d <= 0 {
		return releaseProbeBudget
	}
	return d
}

// FastFailReleaseProbeVerdict runs the release probe in two waves: a small
// first wave whose misses can already prove the post dead, then — only when
// that wave was clean — the rest of the sample, cancelled on the first miss.
// Errors and the inconclusive rules are those of statIDsWithBoundedRetries.
// checkBudget bounds the whole probe (0 uses releaseProbeBudget); a clean
// probe that reaches it passes on the answers it has, see minTimeBoxedChecked.
func FastFailReleaseProbeVerdict(
	ctx context.Context,
	files []FastFailFile,
	poolManager pool.Manager,
	segmentSamplePercentage int,
	maxConnections int,
	timeout time.Duration,
	patchIdx PatchIndex,
	articleDate time.Time,
	checkBudget time.Duration,
) (verdict ProbeVerdict, err error) {
	checkBudget = EffectiveReleaseProbeBudget(checkBudget)
	var segments []*metapb.SegmentData
	for _, file := range files {
		for _, segment := range file.Segments {
			if segment == nil || segment.Id == "" || holes.IsPlaceholderID(segment.Id) {
				continue
			}
			segments = append(segments, segment)
		}
	}
	if len(segments) == 0 {
		return ProbeVerdict{}, nil
	}

	selected := usenet.SelectSegmentsForValidation(segments, segmentSamplePercentage)
	if len(selected) == 0 {
		return ProbeVerdict{}, nil
	}
	// A full selection (100 %, or a percentage that reaches every article)
	// comes back in file order; a probe cut short by its time budget would
	// then only have covered the start. Lead with the edges like the partial
	// selector does, and randomise the rest. Samples of minTimeBoxedChecked
	// or fewer finish well inside the budget and keep file order.
	if len(selected) == len(segments) && len(selected) > minTimeBoxedChecked {
		selected = edgesThenShuffled(selected)
	}
	probeStart := time.Now()
	logPass := func(checked int, timeBoxed bool) {
		slog.InfoContext(ctx, "Import segment check passed",
			"checked", checked,
			"sampled", len(selected),
			"release_articles", len(segments),
			"sample_percentage", segmentSamplePercentage,
			"stopped_at_time_limit", timeBoxed,
			"duration", time.Since(probeStart).Round(time.Millisecond))
	}

	if !poolManager.HasPool() {
		return ProbeVerdict{}, fmt.Errorf("cannot fast-fail import: usenet connection pool is nil")
	}
	usenetPool, err := poolManager.GetPool()
	if err != nil {
		return ProbeVerdict{}, fmt.Errorf("cannot fast-fail import: usenet connection pool unavailable: %w", err)
	}
	if usenetPool == nil {
		return ProbeVerdict{}, fmt.Errorf("cannot fast-fail import: usenet connection pool is nil")
	}
	if maxConnections <= 0 {
		maxConnections = 1
	}

	// Keep the first attempt cheap, but let retries use the validation timeout
	// so a slow provider miss can still fall through to a retained provider.
	// Both waves share one wall-clock budget, including attempts and backoff.
	probeTimeout := min(timeout, 2*time.Second)
	probeCtx, cancelProbe := context.WithTimeout(ctx, checkBudget+releaseProbeSlack)
	defer cancelProbe()
	defer func() {
		// Expiring our own budget is inconclusive; caller cancellation remains
		// a caller error and must not be turned into an availability verdict.
		if err != nil && ctx.Err() == nil && probeCtx.Err() != nil && !errors.Is(err, ErrFastFailInconclusive) {
			err = fmt.Errorf("%w: %w", ErrFastFailInconclusive, err)
		}
	}()

	ids := make([]string, len(selected))
	for i, seg := range selected {
		ids[i] = seg.Id
	}
	first, rest := ids, []string(nil)
	if len(ids) > probeFirstWave {
		first, rest = ids[:probeFirstWave], ids[probeFirstWave:]
	}

	// The first wave is swept to completion, not cancelled on the first miss:
	// its misses are counted to tell a dead post from a damaged one.
	missing, unverified, err := statIDsWithBoundedRetries(probeCtx, usenetPool, first, maxConnections, timeout, probeTimeout, false, patchIdx, articleDate, min(fastFailStatBudget, checkBudget))
	if err != nil && len(missing) == 0 {
		return ProbeVerdict{}, err
	}
	if len(missing) > 0 {
		v := ProbeVerdict{Missing: true, MissingIDs: keys(missing)}
		reported := len(first) - len(unverified)
		if releaseLooksDead(len(missing), reported) {
			v.Dead = true
			slog.InfoContext(ctx, "Fast-fail release probe judged the release dead from its first wave",
				"missing", len(missing), "sampled", len(first))
		}
		return v, nil
	}
	if len(rest) == 0 {
		logPass(len(first)-len(unverified), false)
		return ProbeVerdict{}, nil
	}

	restBudget := checkBudget - time.Since(probeStart)
	if restBudget <= 0 {
		return ProbeVerdict{}, fmt.Errorf("%w: release probe budget spent on its first wave", ErrFastFailInconclusive)
	}
	missing, restUnverified, err := statIDsWithBoundedRetries(probeCtx, usenetPool, rest, maxConnections, timeout, probeTimeout, true, patchIdx, articleDate, restBudget)
	if err != nil {
		if len(missing) > 0 {
			// A definitive miss before running out of patience for the rest;
			// the answer is "damaged" either way.
			return ProbeVerdict{Missing: true, MissingIDs: keys(missing)}, nil
		}
		// Out of time with nothing missing. The sample is in random order,
		// so the answered part is itself a random sample; pass on it when it
		// holds at least minTimeBoxedChecked answers.
		checked := len(first) - len(unverified) + len(rest) - len(restUnverified)
		if errors.Is(err, ErrFastFailInconclusive) && ctx.Err() == nil && checked >= minTimeBoxedChecked {
			logPass(checked, true)
			return ProbeVerdict{}, nil
		}
		return ProbeVerdict{}, err
	}
	if len(missing) == 0 {
		logPass(len(first)-len(unverified)+len(rest)-len(restUnverified), false)
	}
	return ProbeVerdict{Missing: len(missing) > 0, MissingIDs: keys(missing)}, nil
}

// edgesThenShuffled orders a full, file-ordered selection the way the partial
// selector does: first three and last two articles, then the rest at random.
func edgesThenShuffled(all []*metapb.SegmentData) []*metapb.SegmentData {
	n := len(all)
	out := make([]*metapb.SegmentData, 0, n)
	out = append(out, all[:3]...)
	out = append(out, all[n-2:]...)
	middle := make([]*metapb.SegmentData, n-5)
	copy(middle, all[3:n-2])
	shuffleSample(len(middle), func(i, j int) { middle[i], middle[j] = middle[j], middle[i] })
	return append(out, middle...)
}

// shuffleSample is rand.Shuffle; a var so tests can fix the order.
var shuffleSample = rand.Shuffle

// DeadReleaseResults is the per-file outcome of a release the probe judged
// dead: every file with segments is broken, carrying whichever sampled misses
// were its own. Index-aligned with files, like FastFailCheckFiles' results.
func DeadReleaseResults(files []FastFailFile, missingIDs []string) []FastFailFileResult {
	missing := make(map[string]struct{}, len(missingIDs))
	for _, id := range missingIDs {
		missing[id] = struct{}{}
	}
	results := make([]FastFailFileResult, len(files))
	for i, file := range files {
		if len(file.Segments) == 0 {
			continue
		}
		r := FastFailFileResult{Broken: true, SampledCount: len(file.Segments)}
		for _, seg := range file.Segments {
			if seg == nil {
				continue
			}
			if _, gone := missing[seg.Id]; gone {
				r.MissingSegmentIDs = append(r.MissingSegmentIDs, seg.Id)
			}
		}
		results[i] = r
	}
	return results
}

func keys(m map[string]error) []string {
	out := make([]string, 0, len(m))
	for id := range m {
		out = append(out, id)
	}
	return out
}
