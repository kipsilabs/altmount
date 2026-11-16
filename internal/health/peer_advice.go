package health

import (
	"context"
	"log/slog"
	"time"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
)

// PeerAdviceKind is what evidence from the peer network allows a scheduled
// health check to do. Evidence is advisory: it can make a healthy file's
// routine check cheaper or later, never a verdict.
type PeerAdviceKind int

const (
	// PeerAdviceNone: check as usual.
	PeerAdviceNone PeerAdviceKind = iota
	// PeerAdviceReduceSample: the sample of this check may be lowered to its
	// floor (see PeerFloorPercent). The check still runs.
	PeerAdviceReduceSample
	// PeerAdviceSkipOnce: this due check may be deferred once, to
	// PeerAdvice.SkipUntil at the latest.
	PeerAdviceSkipOnce
)

// PeerAdvice is an adviser's answer for one file.
type PeerAdvice struct {
	Kind PeerAdviceKind
	// SkipUntil is the latest time a deferral may last until (PeerAdviceSkipOnce).
	SkipUntil time.Time
}

// PeerAdviser supplies advice for scheduled re-checks of healthy files. It is
// defined here so this package never imports the peer network; the cmd wiring
// installs an adapter and removes it again while the feature is off, so with
// no adviser nothing changes and nothing extra runs.
//
// PeerAdvice is consulted at most once per file and check, under a short
// timeout. It must fail open: any problem is "no advice". PeerAdviceApplied
// is called when the checker actually used a piece of advice.
type PeerAdviser interface {
	PeerAdvice(ctx context.Context, filePath string) PeerAdvice
	PeerAdviceApplied(kind PeerAdviceKind)
}

// PeerFileInfo is what the scheduled cycle knows about one due file. Advice
// is only ever considered for files that appear in CheckOptions.PeerFiles, so
// manual rechecks, repair re-validation and every other caller never get it.
type PeerFileInfo struct {
	// Status is the file's status before this check started. Only healthy
	// files are eligible.
	Status database.HealthStatus
	// ReleaseDate is the date the file's normal schedule is computed from. A
	// zero value means the schedule cannot be computed and no deferral is
	// allowed.
	ReleaseDate time.Time
	// AlreadyDeferred is set when the file was already deferred once and no
	// real check has run since.
	AlreadyDeferred bool
	// AlreadyReduced is set when the file's last completed check was a
	// peer-reduced one and no normal check has run since. At most one reduced
	// check may separate two normal ones.
	AlreadyReduced bool
	// PriorFailure is set when the row carries any recorded sign of a local
	// failure (retry counters, a last error, failure details). Such a file
	// is never given advice.
	PriorFailure bool
}

const (
	// peerAdviceTimeout bounds the adviser lookup so a broken or slow peer
	// database can never hold up a health check.
	peerAdviceTimeout = 300 * time.Millisecond

	// PeerFloorMinSegments is the fewest segments a peer-reduced check
	// samples. The sampler always takes the first three and last two
	// segments (five in all); the floor adds at least as many from the
	// middle so a reduced check still probes the body of the file.
	PeerFloorMinSegments = 10
	// PeerFloorPercent is the smallest share of segments a peer-reduced check
	// samples on files large enough for it to exceed PeerFloorMinSegments.
	// It is a small percentage on purpose: evidence shortens a check, it does
	// not replace it.
	PeerFloorPercent = 2
)

type peerAdviserHolder struct{ a PeerAdviser }

// SetPeerAdviser installs (or, with nil, removes) the adviser. Safe at any time.
func (hc *HealthChecker) SetPeerAdviser(a PeerAdviser) {
	if a == nil {
		hc.peerAdviser.Store(nil)
		return
	}
	hc.peerAdviser.Store(&peerAdviserHolder{a: a})
}

// SetPeerAdviser installs the adviser on the worker's checker.
func (hw *HealthWorker) SetPeerAdviser(a PeerAdviser) {
	hw.healthChecker.SetPeerAdviser(a)
}

// peerAdviserOn reports whether an adviser is installed (the cycle only
// gathers per-file information then).
func (hc *HealthChecker) peerAdviserOn() bool { return hc.peerAdviser.Load() != nil }

// peerFloorPercent returns the sample percentage of a peer-reduced check for
// a file with total real segments, and whether reducing applies at all. The
// result never exceeds the normal sample (it is below the configured
// percentage), never drops below PeerFloorMinSegments (or the whole file when
// it has fewer) and so never below the five segments the sampler always takes.
func peerFloorPercent(total, configured int) (int, bool) {
	if total <= 0 {
		return 0, false
	}
	pct := max(PeerFloorPercent, (PeerFloorMinSegments*100+total-1)/total)
	if pct >= 100 || pct >= configured {
		return 0, false
	}
	// A lower percentage can still sample the very same segments (the sampler
	// never takes fewer than its minimum); that is no reduction.
	if sampleSize(total, pct) >= sampleSize(total, configured) {
		return 0, false
	}
	return pct, true
}

// sampleSize is how many segments the sampler selects out of total real
// segments at the given percentage (see usenet.SelectSegmentsForValidation).
func sampleSize(total, percentage int) int {
	if percentage >= 100 {
		return total
	}
	return min(max(total*percentage/100, 5), total)
}

// peerEligible says whether peer advice may be considered for this file's
// check at all: only a scheduled re-check of a healthy file that is not under
// any special treatment. Cheap and local; nothing is asked of the adviser
// before it passes.
func (hc *HealthChecker) peerEligible(ctx context.Context, prep *preparedCheck, segments []*metapb.SegmentData, opts []CheckOptions, cfg *config.Config) (PeerFileInfo, bool) {
	if len(opts) == 0 || opts[0].PeerFiles == nil || hc.peerAdviser.Load() == nil {
		return PeerFileInfo{}, false
	}
	o := opts[0]
	if owesFullCheck(o, prep.filePath) {
		// A peer-bundle import owes a full check: advice never applies.
		return PeerFileInfo{}, false
	}
	info, ok := o.PeerFiles[prep.filePath]
	switch {
	case !ok, info.Status != database.HealthStatusHealthy:
		return PeerFileInfo{}, false
	case o.ForceFullCheck, o.VerifyContentOverride != nil, cfg.GetCheckAllSegments():
		return PeerFileInfo{}, false
	case info.PriorFailure, info.AlreadyReduced:
		return PeerFileInfo{}, false
	case prep.knownHoles, prep.realSegments <= 0:
		return PeerFileInfo{}, false
	}
	probe := *prep
	probe.currentStatus = info.Status
	if hc.shouldVerifyContent(probe) {
		return PeerFileInfo{}, false
	}
	// Last, because it scans the patch store: only reached for a file that
	// is otherwise eligible, with an adviser installed.
	if !prep.patchedKnown {
		prep.patched = hc.anyPatched(ctx, segments)
		prep.patchedKnown = true
	}
	if prep.patched {
		return PeerFileInfo{}, false
	}
	return info, true
}

// peerAdviceFor asks the adviser once, under the lookup timeout. Anything but
// a clean answer is no advice.
func (hc *HealthChecker) peerAdviceFor(ctx context.Context, filePath string) (adv PeerAdvice, a PeerAdviser) {
	h := hc.peerAdviser.Load()
	if h == nil {
		return PeerAdvice{}, nil
	}
	lookupCtx, cancel := context.WithTimeout(ctx, peerAdviceTimeout)
	defer cancel()
	defer func() {
		if r := recover(); r != nil {
			slog.ErrorContext(ctx, "Peer adviser panicked", "panic", r)
			adv = PeerAdvice{}
		}
	}()
	adv = h.a.PeerAdvice(lookupCtx, filePath)
	if lookupCtx.Err() != nil {
		return PeerAdvice{}, h.a
	}
	return adv, h.a
}

// applyPeerAdvice consults the adviser for an eligible file and returns the
// sample percentage to use. When the advice defers the check it also returns
// the terminal event; the caller then skips the sweep.
func (hc *HealthChecker) applyPeerAdvice(ctx context.Context, prep *preparedCheck, segments []*metapb.SegmentData, opts []CheckOptions, cfg *config.Config, samplePercentage int) (int, *HealthEvent) {
	info, ok := hc.peerEligible(ctx, prep, segments, opts, cfg)
	if !ok {
		return samplePercentage, nil
	}
	if info.AlreadyDeferred {
		// The one allowed deferral is used up until a real check runs, and a
		// reduction is not offered on top of a deferral, so no answer could
		// be used: do not ask.
		return samplePercentage, nil
	}
	advice, adviser := hc.peerAdviceFor(ctx, prep.filePath)
	switch advice.Kind {
	case PeerAdviceReduceSample:
		if pct, ok := peerFloorPercent(prep.realSegments, samplePercentage); ok {
			adviser.PeerAdviceApplied(PeerAdviceReduceSample)
			prep.reducedByPeer = true
			return pct, nil
		}
	case PeerAdviceSkipOnce:
		if until, ok := deferralTime(info, advice.SkipUntil, time.Now().UTC()); ok {
			adviser.PeerAdviceApplied(PeerAdviceSkipOnce)
			event := baseResultEvent(prep.filePath, prep.sourceNzbPath)
			event.Type = EventTypeCheckDeferred
			event.Status = info.Status
			event.DeferUntil = until
			return samplePercentage, &event
		}
	}
	return samplePercentage, nil
}

// deferralTime is when a deferred check becomes due again: no later than the
// evidence allows and never later than a successful check now would schedule
// the next one, so a deferral can only shorten the wait to the next real
// check, never lengthen it. A deferral too short to be worth it is refused.
func deferralTime(info PeerFileInfo, skipUntil, now time.Time) (time.Time, bool) {
	if info.ReleaseDate.IsZero() || skipUntil.IsZero() {
		return time.Time{}, false
	}
	until := skipUntil.UTC()
	if normal := CalculateNextCheck(info.ReleaseDate.UTC(), now); normal.Before(until) {
		until = normal
	}
	if until.Sub(now) < minInterval {
		return time.Time{}, false
	}
	return until, true
}

// peerFilesFor describes the due files of a scheduled cycle to the checker.
// Only healthy files are listed (the only ones advice can apply to).
func peerFilesFor(files []*database.FileHealth) map[string]PeerFileInfo {
	out := make(map[string]PeerFileInfo)
	for _, fh := range files {
		if fh == nil || fh.Status != database.HealthStatusHealthy {
			continue
		}
		release := fh.CreatedAt
		if fh.ReleaseDate != nil {
			release = *fh.ReleaseDate
		}
		out[fh.FilePath] = PeerFileInfo{
			Status:          fh.Status,
			ReleaseDate:     release,
			AlreadyDeferred: database.HasPeerDeferral(fh.ErrorDetails),
			AlreadyReduced:  database.HasPeerReduction(fh.ErrorDetails),
			PriorFailure: fh.RetryCount > 0 || fh.RepairRetryCount > 0 ||
				(fh.LastError != nil && *fh.LastError != "") ||
				database.HasFailureDetails(fh.ErrorDetails),
		}
	}
	return out
}

// ScheduleCheckNow makes a healthy file's next check due now. It only ever
// moves the schedule earlier, never writes a status or touches any counter,
// and leaves files that are not healthy alone. It reports whether a schedule
// was moved. The peer network uses it when evidence from other nodes suggests
// a file may have gone bad; the check itself decides.
func (hw *HealthWorker) ScheduleCheckNow(ctx context.Context, filePath string) (bool, error) {
	return hw.healthRepo.BringCheckForward(ctx, filePath, time.Now())
}
