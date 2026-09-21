package validation

import (
	"context"
	"fmt"
	"log/slog"
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

// FastFailReleaseProbeVerdict runs the release probe in two waves: a small
// first wave whose misses can already prove the post dead, then — only when
// that wave was clean — the rest of the sample, cancelled on the first miss.
// Errors and the inconclusive rules are those of statIDsWithBoundedRetries.
func FastFailReleaseProbeVerdict(
	ctx context.Context,
	files []FastFailFile,
	poolManager pool.Manager,
	segmentSamplePercentage int,
	maxConnections int,
	timeout time.Duration,
	patchIdx PatchIndex,
	articleDate time.Time,
) (ProbeVerdict, error) {
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

	selected := capReleaseProbeSample(usenet.SelectSegmentsForValidation(segments, segmentSamplePercentage))
	if len(selected) == 0 {
		return ProbeVerdict{}, nil
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

	// Cap each attempt's probe timeout to 2 seconds per item so dead releases
	// stay bounded.
	probeTimeout := min(timeout, 2*time.Second)

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
	missing, unverified, err := statIDsWithBoundedRetries(ctx, usenetPool, first, maxConnections, probeTimeout, false, patchIdx, articleDate)
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
		return ProbeVerdict{}, nil
	}

	missing, _, err = statIDsWithBoundedRetries(ctx, usenetPool, rest, maxConnections, probeTimeout, true, patchIdx, articleDate)
	if err != nil {
		if len(missing) > 0 {
			// A definitive miss before running out of patience for the rest;
			// the answer is "damaged" either way.
			return ProbeVerdict{Missing: true, MissingIDs: keys(missing)}, nil
		}
		return ProbeVerdict{}, err
	}
	return ProbeVerdict{Missing: len(missing) > 0, MissingIDs: keys(missing)}, nil
}

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
