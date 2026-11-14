package hints

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/records"
)

type adviceEvidence struct {
	openHealthy     int
	privHealthy     int
	privNewest      time.Time
	privNet         netid.NetworkID
	openNet         netid.NetworkID
	unhealthyNewest time.Time
}

// HealthAdvice returns what peer evidence allows for one file. Evidence is
// found by File ID alone, whatever release ID its publisher used: the same
// articles are the same file in every release they are part of, and a file
// imported before the NZB store existed has no NZB-based release ID at all.
// releaseID is therefore optional (empty when the file has none) and is not
// used for the lookup. localBackbones
// are the raw backbone labels of the local providers. now is the caller's
// current time (normally the injected clock). Errors from the store or the
// peer table are returned with AdviceNone: advice is never guessed.
func (a *Advisor) HealthAdvice(ctx context.Context, releaseID, fileID string, localBackbones []string, now time.Time) (Advice, error) {
	if (releaseID != "" && !validID(releaseID)) || !validID(fileID) {
		return Advice{}, errors.New("hints: release and file ids must be 64 lower-case hex characters")
	}
	local := normaliseSet(localBackbones)
	if len(local) == 0 {
		return Advice{}, nil
	}
	var ev adviceEvidence
	for _, n := range a.receiving() {
		if err := a.gather(ctx, n, fileID, local, now, &ev); err != nil {
			return Advice{}, err
		}
	}
	return a.decide(fileID, ev, now), nil
}

func (a *Advisor) gather(ctx context.Context, n Network, fileID string, local map[string]struct{}, now time.Time, ev *adviceEvidence) error {
	atts, err := a.cfg.Store.AttestationsForFiles(ctx, n.ID.String(), []string{fileID}, now.Add(-records.AttestationValidity))
	if err != nil {
		return fmt.Errorf("hints: attestations: %w", err)
	}
	healthy := map[string]struct{}{}    // distinct groups attesting healthy
	unhealthyG := map[string]struct{}{} // distinct groups attesting unhealthy
	seenHealthy := map[string]struct{}{}
	known := newKnownCache(a, ctx, n, now)
	if err := known.prefetch(a.relevantPublishers(atts, fileID, local, now)); err != nil {
		return err
	}
	var newest, unhealthyNewest time.Time
	for _, att := range atts {
		if !strings.EqualFold(att.FileID, fileID) || !a.usable(att, now) {
			continue
		}
		subset, overlap := relation(att.Backbones, local)
		if !att.Healthy {
			if !overlap {
				continue
			}
			// The same standing as healthy evidence: on the open network a
			// publisher known for MinKnown, so fresh identities cannot drain
			// the re-check budget.
			ok, err := known.eligible(att.PublisherNodeID)
			if err != nil {
				return err
			}
			if ok {
				unhealthyG[known.groupOf(att.PublisherNodeID)] = struct{}{}
				if att.CheckedAt.After(unhealthyNewest) {
					unhealthyNewest = att.CheckedAt
				}
			}
			continue
		}
		if !subset || !CoverageSufficient(att.SegmentsChecked, att.TotalSegments) {
			continue
		}
		if _, dup := seenHealthy[att.PublisherNodeID]; dup {
			if att.CheckedAt.After(newest) {
				newest = att.CheckedAt
			}
			continue
		}
		ok, err := known.eligible(att.PublisherNodeID)
		if err != nil {
			return err
		}
		if ok {
			seenHealthy[att.PublisherNodeID] = struct{}{}
			healthy[known.groupOf(att.PublisherNodeID)] = struct{}{}
			if att.CheckedAt.After(newest) {
				newest = att.CheckedAt
			}
		}
	}
	// Unhealthy evidence needs the same number of distinct publishers as
	// healthy corroboration, so a lone identity cannot veto or force work.
	if len(unhealthyG) >= a.threshold(n) && unhealthyNewest.After(ev.unhealthyNewest) {
		ev.unhealthyNewest = unhealthyNewest
	}
	if n.ID.IsOpen() {
		ev.openHealthy += len(healthy)
		ev.openNet = n.ID
	} else if len(healthy) >= a.cfg.PrivateThreshold && newest.After(ev.privNewest) {
		ev.privHealthy += len(healthy)
		ev.privNewest, ev.privNet = newest, n.ID
	}
	return nil
}

// relevantPublishers lists, in order and without repeats, the publishers
// whose attestations gather may count, so their standing can be fetched in
// one go.
func (a *Advisor) relevantPublishers(atts []database.PeernetAttestation, fileID string, local map[string]struct{}, now time.Time) []string {
	var out []string
	seen := map[string]struct{}{}
	for _, att := range atts {
		if !strings.EqualFold(att.FileID, fileID) || !a.usable(att, now) {
			continue
		}
		subset, overlap := relation(att.Backbones, local)
		if att.Healthy && (!subset || !CoverageSufficient(att.SegmentsChecked, att.TotalSegments)) || !att.Healthy && !overlap {
			continue
		}
		if _, dup := seen[att.PublisherNodeID]; !dup {
			seen[att.PublisherNodeID] = struct{}{}
			out = append(out, att.PublisherNodeID)
		}
	}
	return out
}

func (a *Advisor) decide(fileKey string, ev adviceEvidence, now time.Time) Advice {
	if !ev.unhealthyNewest.IsZero() {
		// Unhealthy evidence, even next to healthy evidence, never shortens
		// anything; at most it may move a check forward, once. Deciding is
		// read-only: the budget is spent by SpendRecheck when a check really
		// was moved.
		if a.acted(fileKey, ev.unhealthyNewest) {
			return Advice{}
		}
		return Advice{Kind: AdviceRecheckSooner, EvidenceAt: ev.unhealthyNewest}
	}
	if ev.privHealthy >= a.cfg.PrivateThreshold && !ev.privNewest.IsZero() {
		if until := ev.privNewest.Add(records.AttestationValidity); until.After(now) {
			return Advice{Kind: AdviceMaySkipOneCheck, SkipUntil: until, Network: ev.privNet}
		}
	}
	if ev.openHealthy >= a.corroboration() {
		return Advice{Kind: AdviceReduceSampleToFloor, Network: ev.openNet}
	}
	return Advice{}
}

// acted reports whether evidence of the given check time (or newer) for the
// file was already acted on.
func (a *Advisor) acted(key string, newest time.Time) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	prev, seen := a.advised[key]
	return seen && !newest.After(prev)
}

// markAdvised records acted-on evidence, evicting the entry with the oldest
// evidence when the memory is full. The caller holds a.mu.
func (a *Advisor) markAdvised(key string, newest time.Time) {
	if _, have := a.advised[key]; !have && len(a.advised) >= maxAdvised {
		var oldestKey string
		var oldest time.Time
		first := true
		for k, t := range a.advised {
			if first || t.Before(oldest) {
				oldestKey, oldest, first = k, t, false
			}
		}
		delete(a.advised, oldestKey)
	}
	a.advised[key] = newest
}

// forget clears the memory of acted-on evidence.
func (a *Advisor) forget() {
	a.mu.Lock()
	a.advised = map[string]time.Time{}
	a.mu.Unlock()
}

// SpendRecheck pays one token of the shared budget for moving a check forward
// on the unhealthy evidence of the given check time, and remembers that the
// evidence was acted on. It reports false when this evidence was already acted
// on or no token is left. It is called only once a check really was moved:
// HealthAdvice itself spends nothing.
func (a *Advisor) SpendRecheck(_, fileID string, evidenceAt, now time.Time) bool {
	// Evidence is about the file, so it is paid for once per file however
	// many release IDs reach it.
	key := fileID
	a.mu.Lock()
	defer a.mu.Unlock()
	if prev, seen := a.advised[key]; seen && !evidenceAt.After(prev) {
		return false
	}
	if !a.cfg.Budget.Take(now) {
		return false
	}
	a.markAdvised(key, evidenceAt)
	return true
}
