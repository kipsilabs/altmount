package health

import (
	"context"
	"log/slog"

	"github.com/kipsilabs/altmount/internal/holes"
	"github.com/kipsilabs/altmount/internal/metadata"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/usenet"
)

// classifyHoles turns a check's missing segments into a playback-impact
// verdict using the hole model (see internal/holes): merge newly observed
// misses with the file's persisted hole map, then apply the threshold table.
// A full sweep can prove clean; a sampled sweep only projects (never clean).
//
// It re-reads metadata rather than taking a healthCheckInput from the caller:
// prepareCheck deliberately drops the segment slice after sampling so the
// proto is collectible during the (possibly cross-file, batched) network
// sweep, and re-reading only pays a disk read in the rare case a file
// actually has missing segments. Newly observed holes are persisted on
// degraded verdicts so playback pre-pads them without a network round-trip.
//
// Returns nil when the file is ineligible (non-video, encrypted, remuxed) or
// metadata can no longer be read.
func (hc *HealthChecker) classifyHoles(
	ctx context.Context,
	filePath string,
	result usenet.ValidationResult,
) *holes.Impact {
	if len(result.MissingIDs) == 0 {
		return nil
	}
	input, ok := hc.loadClassificationInput(filePath)
	if !ok {
		return nil
	}

	acc := hc.unpatchedKnownHoles(input)
	priorTotal := acc.Total()
	observed := missingRuns(input.segments, result.MissingIDs)
	acc.Load(observed)

	totalSegments := len(input.segments)
	segBytes := avgSegmentBytes(input.fileSize, totalSegments)
	fullCheck := result.TotalChecked >= totalSegments

	var verdict holes.Verdict
	if fullCheck {
		verdict = holes.Classify(acc.Runs(), input.fileSize, segBytes)
	} else {
		// Sampled evidence projects; the persisted map still counts as
		// measured damage, so take the worse of projection and accumulation.
		verdict = holes.ClassifyProjected(result.MissingCount, result.TotalChecked, totalSegments, acc.LongestRun())
		if holes.Classify(acc.Runs(), input.fileSize, segBytes) == holes.VerdictFailed {
			verdict = holes.VerdictFailed
		}
	}

	// The configured acceptable-missing threshold is a user-tunable ceiling
	// independent of the fixed padding caps above: below it a file stays
	// healthy/degraded, at or above it the file is failed so the repair path
	// (worker.go) kicks in instead of leaving it degraded forever.
	if verdict != holes.VerdictFailed {
		acceptable := hc.configGetter().GetAcceptableMissingSegmentsPercentage()
		if holes.ExceedsAcceptableMissing(acc.Total(), totalSegments, acceptable) {
			verdict = holes.VerdictFailed
		}
	}

	// Persist newly observed holes so playback replay pre-pads them. Only on
	// degraded verdicts: failed files head to repair/re-download anyway.
	if verdict == holes.VerdictDegraded && acc.Total() > priorTotal {
		if err := hc.metadataService.AddKnownHoles(filePath, observed, hc.configGetter().ProviderFingerprint()); err != nil {
			slog.WarnContext(ctx, "Failed to persist known holes",
				"file_path", filePath,
				"error", err)
		}
	}

	var ratio float64
	if input.fileSize > 0 {
		ratio = float64(int64(acc.Total())*segBytes) / float64(input.fileSize)
	}
	return &holes.Impact{
		Verdict:       verdict,
		TotalMissing:  acc.Total(),
		LongestRun:    acc.LongestRun(),
		Sampled:       result.TotalChecked,
		TotalSegments: totalSegments,
		PaddedRatio:   ratio,
	}
}

// classifyKnownHoles is the verdict for a sweep that found nothing missing
// in a file that already has persisted holes. A clean STAT sample cannot clear
// them: providers can answer STAT for an article whose body is gone, so the
// hole map is the only body-level evidence there is. Holes count regardless
// of age or provider fingerprint (those only decide whether playback asks
// for the article again before padding), and only a local patch covers one.
//
// The verdict is degraded, never failed: the sweep found no new damage, and
// the pad recorder only persists holes within the padding caps.
//
// Returns nil when the file is ineligible or every known hole is patched.
func (hc *HealthChecker) classifyKnownHoles(filePath string, result usenet.ValidationResult) *holes.Impact {
	input, ok := hc.loadClassificationInput(filePath)
	if !ok {
		return nil
	}
	acc := hc.unpatchedKnownHoles(input)
	if acc.Total() == 0 {
		return nil
	}
	totalSegments := len(input.segments)
	segBytes := avgSegmentBytes(input.fileSize, totalSegments)
	var ratio float64
	if input.fileSize > 0 {
		ratio = float64(int64(acc.Total())*segBytes) / float64(input.fileSize)
	}
	return &holes.Impact{
		Verdict:       holes.VerdictDegraded,
		TotalMissing:  acc.Total(),
		LongestRun:    acc.LongestRun(),
		Sampled:       result.TotalChecked,
		TotalSegments: totalSegments,
		PaddedRatio:   ratio,
	}
}

// unpatchedKnownHoles loads the file's persisted hole map, minus segments a
// local patch has repaired: the provider-missing evidence stays, but repaired
// bytes are no longer holes while their patches remain available.
func (hc *HealthChecker) unpatchedKnownHoles(input healthCheckInput) holes.Accumulator {
	var acc holes.Accumulator
	for _, run := range metadata.KnownHolesFromProto(input.knownHoles) {
		start := min(run.Start, len(input.segments))
		end := start + min(run.Count, len(input.segments)-start)
		for i := start; i < end; i++ {
			if !hc.hasPatch(input.segments[i].Id) {
				acc.Add(i)
			}
		}
	}
	return acc
}

// loadClassificationInput re-reads metadata for hole classification and
// reports whether the file is eligible (plain unencrypted video).
func (hc *HealthChecker) loadClassificationInput(filePath string) (healthCheckInput, bool) {
	fileMeta, err := hc.metadataService.ReadFileMetadata(filePath)
	if err != nil || fileMeta == nil {
		return healthCheckInput{}, false
	}
	input := healthCheckInput{
		fileSize:      fileMeta.FileSize,
		sourceNzbPath: fileMeta.SourceNzbPath,
		segments:      fileMeta.SegmentData,
		encryption:    fileMeta.Encryption,
		knownHoles:    fileMeta.KnownHoles,
		hasNestedOrRemuxedSources: len(fileMeta.NestedSources) > 0 ||
			len(fileMeta.SharedOuterSources) > 0 ||
			len(fileMeta.ClipBoundaries) > 0,
	}
	if !holes.EligibleFile(filePath) ||
		input.encryption != metapb.Encryption_NONE ||
		input.hasNestedOrRemuxedSources {
		return healthCheckInput{}, false
	}
	return input, true
}

// missingRuns folds missing message IDs into hole runs by resolving each ID
// to its index in the file's segment list. The batch health-check path
// reports only IDs, so the mapping happens here. Sampling gaps mean observed
// runs are lower bounds on the real damage.
func missingRuns(segments []*metapb.SegmentData, missingIDs []string) []holes.Run {
	indexByID := make(map[string]int, len(segments))
	for i, seg := range segments {
		indexByID[seg.Id] = i
	}
	var acc holes.Accumulator
	for _, id := range missingIDs {
		if idx, ok := indexByID[id]; ok {
			acc.Add(idx)
		}
	}
	return acc.Runs()
}

// avgSegmentBytes estimates the decoded segment size for the byte-ratio
// guard; encoded/decoded skew is negligible at 2%.
func avgSegmentBytes(fileSize int64, totalSegments int) int64 {
	if totalSegments <= 0 || fileSize <= 0 {
		return 1
	}
	return fileSize / int64(totalSegments)
}
