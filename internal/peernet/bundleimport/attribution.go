package bundleimport

import (
	"context"
	"sync"
	"time"
)

// One attribution rule, decided in one place (decide).
//
// A rejection is attributable to the publisher ONLY when the bytes of an
// article contradict the bundle's own numbers: a yEnc header against the
// declared size, offset or total, or an archive header against the declared
// extent or part. Never attributable: missing articles, timeouts, budget
// exhaustion, cancelled contexts, provider errors, anything a receiver-side
// heuristic derived (media structure, name parsing, part order from names), and
// every reason that is decided before an article is read.
//
// A Bundle ID enters the failed-bundle negative cache only after two
// independent contradictions: two different articles. One corrupt article a
// provider served must not blacklist a good bundle for a week. The first
// contradiction is rejected and strikes the publisher mildly (the publisher's
// bundle fetches back off, and a later verified bundle clears it); a second,
// different contradicting article (in the same or a later attempt, within the
// window) blacklists.

// reasonAttribution says, for every rejection reason, whether article bytes
// can contradict the bundle under it. A reason that is not listed here fails
// TestReasonAttributionCoversEveryReason, so adding one forces a decision.
var reasonAttribution = map[Reason]bool{
	ReasonInvalid:            false, // decided from the bundle alone, before any article
	ReasonTooLarge:           false,
	ReasonTooManyFiles:       false,
	ReasonBadName:            false,
	ReasonBadTarget:          false,
	ReasonCollision:          false,
	ReasonHealthDisabled:     false,
	ReasonProbeMissing:       false, // missing articles
	ReasonProbeInconclusive:  false,
	ReasonNoMediaFile:        false,
	ReasonMediaInvalid:       false, // a receiver-side media decode, no declared number contradicted
	ReasonMediaMissing:       false,
	ReasonMediaInconclusive:  false,
	ReasonStaging:            false,
	ReasonWriteFailed:        false,
	ReasonNZBMismatch:        false, // the bundle against the user's NZB, not article bytes
	ReasonUnavailable:        false,
	ReasonNoNetwork:          false,
	ReasonSizeImplausible:    false,
	ReasonYencMismatch:       true,  // yEnc header against declared size, offset, total or name
	ReasonStructureInvalid:   false, // receiver-side media structure check
	ReasonForbiddenFile:      false,
	ReasonSidecarTooLarge:    false,
	ReasonExtentMismatch:     true, // archive header against declared extent or part
	ReasonExtentUnverifiable: false,
	ReasonPartOrder:          false, // names are poster-chosen
	ReasonTooManyTables:      false,
	ReasonVerifyBudget:       false, // local budget, the bundle was not read
}

// rejection is a typed rejection as the attribution rule sees it.
type rejection struct {
	Reason Reason
	// Bytes says the verifier found article bytes contradicting the bundle.
	Bytes bool
	// Evidence identifies the contradicting article (stable across attempts for
	// the same article, different for a different one).
	Evidence string
}

// decision is what decide concludes.
type decision struct {
	// Attributable: report the publisher (a mild strike).
	Attributable bool
	// Blacklist: put the Bundle ID on the negative cache.
	Blacklist bool
}

// decide is the single place that turns a rejection into attributable and
// blacklist. Only an attributable rejection with a Bundle ID is counted.
func (i *Importer) decide(ctx context.Context, bundleID string, rej rejection) decision {
	if ctx.Err() != nil || !rej.Bytes || !reasonAttribution[rej.Reason] {
		return decision{}
	}
	d := decision{Attributable: true}
	if bundleID != "" && i.strikes.add(bundleID, rej.Evidence, time.Now()) >= blacklistAfter {
		d.Blacklist = true
	}
	return d
}

const (
	// blacklistAfter is the number of distinct contradicting articles that
	// blacklists a Bundle ID.
	blacklistAfter = 2
	// strikeWindow is how long a strike counts (the failed-bundle cache window).
	strikeWindow = 7 * 24 * time.Hour
	// Bounds: the book is in memory, so a restart forgets it, which only makes
	// blacklisting more lenient (the safe direction); a persisted counter would
	// add a table for no benefit.
	maxStrikeBundles  = 1024
	maxStrikeEvidence = 8
)

// strikeBook counts, per Bundle ID, the distinct contradicting articles seen
// within the window.
type strikeBook struct {
	mu sync.Mutex
	m  map[string]map[string]time.Time
}

// add records the evidence for the bundle and returns the number of distinct
// pieces of evidence still inside the window.
func (s *strikeBook) add(bundleID, evidence string, now time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m == nil {
		s.m = map[string]map[string]time.Time{}
	}
	ev := s.m[bundleID]
	for k, at := range ev {
		if now.Sub(at) > strikeWindow {
			delete(ev, k)
		}
	}
	if ev == nil {
		if len(s.m) >= maxStrikeBundles {
			s.evictLocked(now)
		}
		ev = map[string]time.Time{}
		s.m[bundleID] = ev
	}
	if _, seen := ev[evidence]; !seen && len(ev) < maxStrikeEvidence {
		ev[evidence] = now
	}
	return len(ev)
}

// evictLocked drops expired bundles, and the one with the oldest evidence when
// none expired.
func (s *strikeBook) evictLocked(now time.Time) {
	oldest, oldestAt := "", time.Time{}
	for id, ev := range s.m {
		newest := time.Time{}
		for _, at := range ev {
			if at.After(newest) {
				newest = at
			}
		}
		if len(ev) == 0 || now.Sub(newest) > strikeWindow {
			delete(s.m, id)
			continue
		}
		if oldest == "" || newest.Before(oldestAt) {
			oldest, oldestAt = id, newest
		}
	}
	if len(s.m) >= maxStrikeBundles && oldest != "" {
		delete(s.m, oldest)
	}
}
