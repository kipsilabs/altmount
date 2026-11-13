package health

import (
	"context"

	"github.com/kipsilabs/altmount/internal/holes"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
)

// patchScanCtxEvery is how many segments are scanned between context checks.
const patchScanCtxEvery = 256

// countRealSegments counts the real (non-placeholder) segments. It is pure
// in-memory work, safe for every check.
func countRealSegments(segments []*metapb.SegmentData) int {
	n := 0
	for _, seg := range segments {
		if seg != nil && !holes.IsPlaceholderID(seg.Id) {
			n++
		}
	}
	return n
}

// anyPatched reports whether a local PAR2 patch covers any segment. The patch
// store answers per message ID with a hash and a stat, so this is a scan whose
// cost grows with the file: callers run it only when its answer is needed. It
// is bounded by ctx; when the scan is cut short the answer is unknown and is
// reported as patched, the conservative reading for both consumers.
func (hc *HealthChecker) anyPatched(ctx context.Context, segments []*metapb.SegmentData) bool {
	if hc.patchIndex == nil {
		return false
	}
	for i, seg := range segments {
		if i%patchScanCtxEvery == 0 && ctx.Err() != nil {
			return true
		}
		if seg == nil || holes.IsPlaceholderID(seg.Id) {
			continue
		}
		if hc.hasPatch(seg.Id) {
			return true
		}
	}
	return false
}

// patchedFor resolves (once) whether the checked file has any local patch,
// re-reading its metadata when the preparation did not need the answer.
func (hc *HealthChecker) patchedFor(ctx context.Context, prep *preparedCheck) bool {
	if prep.patchedKnown {
		return prep.patched
	}
	prep.patchedKnown = true
	if hc.patchIndex == nil {
		return false
	}
	meta, err := hc.metadataService.ReadFileMetadata(prep.filePath)
	if err != nil || meta == nil {
		prep.patched = true
		return true
	}
	prep.patched = hc.anyPatched(ctx, meta.SegmentData)
	return prep.patched
}
