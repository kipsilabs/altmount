package health

import (
	"github.com/kipsilabs/altmount/internal/usenet"
)

// CheckStats is what a finished segment check measured. It carries counts and
// flags only, never a path or a message ID.
type CheckStats struct {
	// TotalSegments is the number of real segments of the file (gap
	// placeholders excluded).
	TotalSegments int
	// SegmentsChecked is the number of segments that got a definitive answer
	// in this check: found, or confirmed missing. Segments served from a local
	// PAR2 patch count as checked, as the sweep counts them.
	SegmentsChecked int
	// MissingCount is the number of segments confirmed missing.
	MissingCount int
	// FullCheck is set when every real segment was answered in this check.
	FullCheck bool
	// MediaHeaderVerified is set only when the media header probe ran in this
	// check and passed.
	MediaHeaderVerified bool
	// PatchedByPAR2 is set when a local PAR2 patch covers any segment of the
	// file, sampled or not. It is only computed for healthy results while a
	// healthy observer is installed (finding it scans the patch store); it is
	// false otherwise.
	PatchedByPAR2 bool
	// PatchKnown is set when the patch summary was actually computed for this
	// result. When false, PatchedByPAR2 means "unknown", not "not patched": a
	// check that began before the observer was installed must not be read as
	// unpatched.
	PatchKnown bool
	// KnownHoles is set when earlier sweeps or playback recorded holes in the
	// file, which a clean sample does not clear.
	KnownHoles bool
	// ReducedByPeerAdvice is set when peer advice lowered this check's
	// sample, so the check is weaker than the configured one.
	ReducedByPeerAdvice bool
}

// statsFor builds the CheckStats of a finished sweep.
func statsFor(prep preparedCheck, result usenet.ValidationResult) *CheckStats {
	return &CheckStats{
		TotalSegments:   prep.realSegments,
		SegmentsChecked: result.TotalChecked,
		MissingCount:    result.MissingCount,
		FullCheck: result.Err == nil && result.UnresolvedCount == 0 && result.MissingCount == 0 &&
			prep.realSegments > 0 && result.TotalChecked >= prep.realSegments,
		KnownHoles:          prep.knownHoles,
		ReducedByPeerAdvice: prep.reducedByPeer,
	}
}
