package service

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/hints"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
)

// The consuming side of the peer network: advice for health checks, labels
// for candidate lists and the periodic pass that moves checks forward on
// unhealthy evidence. Everything here only reads peer data. A broken peer
// database never fails or delays the caller beyond a short timeout.

const (
	hintLookupTimeout = 250 * time.Millisecond
	labelLookupTime   = 300 * time.Millisecond
	labelCacheTTL     = 30 * time.Second
	labelCacheMax     = 512
	// recheckBatch bounds the attestations one upkeep pass looks at per network.
	recheckBatch = 50
	// recheckFilesPerRef bounds the local files one attestation can touch.
	recheckFilesPerRef = 3
	// maxRecheckAttempts bounds how often one piece of evidence is retried
	// when scheduling its check failed.
	maxRecheckAttempts = 3
)

// HintAdvisor is the part of *hints.Advisor the service uses.
type HintAdvisor interface {
	HealthAdvice(ctx context.Context, releaseID, fileID string, localBackbones []string, now time.Time) (hints.Advice, error)
	NetworkHealthy(ctx context.Context, releaseIDs ...string) (map[string]hints.NetworkHealth, error)
	// SpendRecheck pays for a check moved forward on unhealthy evidence.
	SpendRecheck(releaseID, fileID string, evidenceAt, now time.Time) bool
}

// CheckScheduler moves a healthy file's next health check to now. It must
// only move checks earlier and never write a status.
type CheckScheduler interface {
	ScheduleCheckNow(ctx context.Context, filePath string) (bool, error)
}

// HintSources are the consumers' dependencies, installed after construction
// (the advisor needs the service's own network view) and removed again when
// the feature is switched off.
type HintSources struct {
	Advisor HintAdvisor
	Budget  *hints.Budget
	// Backbones returns this node's recognised, normalised backbones.
	Backbones func() []string
	// Scheduler enables the recheck pass; nil leaves it off.
	Scheduler CheckScheduler
}

type labelEntry struct {
	healthy bool
	at      time.Time
}

type hintsHub struct {
	src atomic.Pointer[HintSources]

	mu               sync.Mutex
	given            map[string]int64
	reduceApplied    int64
	skipsApplied     int64
	recheckScheduled int64
	lookupErrors     int64
	cache            map[string]labelEntry
}

func newHintsHub() *hintsHub {
	return &hintsHub{given: map[string]int64{}, cache: map[string]labelEntry{}}
}

func (h *hintsHub) add(f func(*hintsHub)) {
	h.mu.Lock()
	f(h)
	h.mu.Unlock()
}

func (h *hintsHub) lookupError() { h.add(func(h *hintsHub) { h.lookupErrors++ }) }

// AdviceStatus counts what the consuming side did since start. It holds no
// record content, path or identifier.
type AdviceStatus struct {
	// Given counts advice handed to health checks, by kind name.
	Given map[string]int64
	// ReduceApplied and SkipsApplied count advice a check actually used.
	ReduceApplied int64
	SkipsApplied  int64
	// RecheckScheduled counts checks moved forward on unhealthy evidence.
	RecheckScheduled int64
	// LookupErrors counts lookups that failed or timed out (advice then none).
	LookupErrors int64
	// BudgetRemaining is what is left of the hourly peer-triggered re-check budget.
	BudgetRemaining int
}

// SetHintSources installs the consumers' dependencies.
func (s *Service) SetHintSources(src HintSources) { s.deps.hub.src.Store(&src) }

// ClearHintSources removes them: advice, labels and the recheck pass stop.
func (s *Service) ClearHintSources() { s.deps.hub.src.Store(nil) }

// NodeID is this node's ID (lower-case hex), or "" while the feature is off.
func (s *Service) NodeID() string {
	n := s.currentNode()
	if n == nil {
		return ""
	}
	return n.nodeID.String()
}

// ReceivingNetworks lists the networks this node receives records from
// (level 1 or higher, not removed by their host). It reads memory only.
func (s *Service) ReceivingNetworks() []hints.Network {
	n := s.currentNode()
	if n == nil || n.rx == nil {
		return nil
	}
	infos := n.rx.ReceivingNetworks()
	out := make([]hints.Network, 0, len(infos))
	for _, i := range infos {
		out = append(out, hints.Network{ID: i.ID, Level: i.Level})
	}
	return out
}

func (s *Service) hintContext() (*node, *HintSources) {
	n := s.currentNode()
	if n == nil || n.rx == nil || n.deps.Content == nil {
		return nil, nil
	}
	return n, s.deps.hub.src.Load()
}

// HealthAdvice is what peer evidence allows a scheduled check of filePath to
// do. It resolves the file's identifiers from the content index, asks the
// advisor under a short timeout and fails open: any problem is no advice.
func (s *Service) HealthAdvice(ctx context.Context, filePath string) hints.Advice {
	n, src := s.hintContext()
	if n == nil || src == nil || src.Advisor == nil || src.Backbones == nil || len(s.ReceivingNetworks()) == 0 {
		return hints.Advice{}
	}
	ctx, cancel := context.WithTimeout(ctx, hintLookupTimeout)
	defer cancel()
	hub := s.deps.hub
	entry, ok, err := n.deps.Content.GetByPath(ctx, filePath)
	if err != nil {
		hub.lookupError()
		return hints.Advice{}
	}
	if !ok || entry.ReleaseID == "" {
		return hints.Advice{}
	}
	advice, err := src.Advisor.HealthAdvice(ctx, entry.ReleaseID, entry.FileID, src.Backbones(), time.Now())
	if err != nil || ctx.Err() != nil {
		hub.lookupError()
		return hints.Advice{}
	}
	hub.add(func(h *hintsHub) { h.given[advice.Kind.String()]++ })
	return advice
}

// NotePeerAdviceApplied counts advice a health check actually used.
func (s *Service) NotePeerAdviceApplied(kind hints.AdviceKind) {
	s.deps.hub.add(func(h *hintsHub) {
		switch kind {
		case hints.AdviceReduceSampleToFloor:
			h.reduceApplied++
		case hints.AdviceMaySkipOneCheck:
			h.skipsApplied++
		}
	})
}

// NetworkHealthyPaths returns the paths (of those given, at most
// database.PeernetMaxPathsPerQuery) whose release is healthy on the network.
// The match is exact by release ID from the content index; a path without one
// is never labelled. It is one bounded batch lookup, cached for a short time,
// and fails open to an empty result.
func (s *Service) NetworkHealthyPaths(ctx context.Context, paths []string) map[string]bool {
	out := map[string]bool{}
	n, src := s.hintContext()
	if n == nil || src == nil || src.Advisor == nil || len(paths) == 0 || len(s.ReceivingNetworks()) == 0 {
		return out
	}
	if len(paths) > database.PeernetMaxPathsPerQuery {
		paths = paths[:database.PeernetMaxPathsPerQuery]
	}
	ctx, cancel := context.WithTimeout(ctx, labelLookupTime)
	defer cancel()
	hub := s.deps.hub
	rels, err := n.deps.Content.ReleaseIDsByPaths(ctx, paths)
	if err != nil {
		hub.lookupError()
		return out
	}
	healthy, err := s.releaseLabels(ctx, src, rels)
	if err != nil {
		hub.lookupError()
		return out
	}
	for p, rel := range rels {
		if healthy[rel] {
			out[p] = true
		}
	}
	return out
}

// releaseLabels answers per release, from the short cache where it can.
func (s *Service) releaseLabels(ctx context.Context, src *HintSources, rels map[string]string) (map[string]bool, error) {
	hub, now := s.deps.hub, time.Now()
	res := map[string]bool{}
	var missing []string
	seen := map[string]struct{}{}
	hub.mu.Lock()
	for _, rel := range rels {
		if _, dup := seen[rel]; dup {
			continue
		}
		seen[rel] = struct{}{}
		if e, ok := hub.cache[rel]; ok && now.Sub(e.at) < labelCacheTTL {
			res[rel] = e.healthy
		} else {
			missing = append(missing, rel)
		}
	}
	hub.mu.Unlock()
	if len(missing) == 0 {
		return res, nil
	}
	got, err := src.Advisor.NetworkHealthy(ctx, missing...)
	if err != nil {
		return nil, err
	}
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if len(hub.cache)+len(missing) > labelCacheMax {
		clear(hub.cache)
	}
	for _, rel := range missing {
		h := got[rel].Healthy
		res[rel] = h
		hub.cache[rel] = labelEntry{healthy: h, at: now}
	}
	return res, nil
}

func (s *Service) adviceStatus() AdviceStatus {
	hub := s.deps.hub
	hub.mu.Lock()
	st := AdviceStatus{Given: map[string]int64{}, ReduceApplied: hub.reduceApplied, SkipsApplied: hub.skipsApplied,
		RecheckScheduled: hub.recheckScheduled, LookupErrors: hub.lookupErrors}
	for k, v := range hub.given {
		st.Given[k] = v
	}
	hub.mu.Unlock()
	if src := hub.src.Load(); src != nil && src.Budget != nil {
		st.BudgetRemaining = src.Budget.Remaining(time.Now())
	}
	return st
}

// maintainHints is the recheck pass of an upkeep pass: it looks at unhealthy
// attestations received since the last pass (a per-network cursor on the
// record sequence, so each is considered once), for files this node has, asks
// the advisor (which spends the shared budget) and moves the check of a file
// it advises on forward. It never writes a health status. With no scheduler
// or no budget left it does nothing and keeps its place.
func (n *node) maintainHints(ctx context.Context) {
	src := n.deps.hub.src.Load()
	if src == nil || src.Scheduler == nil || src.Advisor == nil || src.Budget == nil || src.Backbones == nil ||
		n.rx == nil || n.deps.Records == nil || n.deps.Content == nil {
		return
	}
	n.hintMu.Lock()
	defer n.hintMu.Unlock()
	for _, nw := range n.rx.ReceivingNetworks() {
		if ctx.Err() != nil || !n.recheckNetwork(ctx, src, nw.ID.String()) {
			return
		}
	}
}

// recheckNetwork reports false when the pass should stop (no budget or a
// failing store).
func (n *node) recheckNetwork(ctx context.Context, src *HintSources, network string) bool {
	if n.hintCursor == nil {
		n.hintCursor = map[string]int64{}
	}
	if n.hintRetries == nil {
		n.hintRetries = map[string]recheckRetry{}
	}
	page, err := n.deps.Records.UnhealthyAttestationsSince(ctx, network, n.hintCursor[network], recheckBatch)
	n.logChange(ctx, "recheck evidence "+network[:8], err)
	if err != nil {
		n.deps.hub.lookupError()
		return false
	}
	hub := n.deps.hub
	for _, ref := range page.Rows {
		now := time.Now()
		if src.Budget.Remaining(now) < 1 {
			return false // keep the cursor: this evidence is still to be considered
		}
		entries, err := n.deps.Content.FindByFileID(ctx, ref.FileID)
		if err != nil {
			hub.lookupError()
			return false
		}
		considered, failed := 0, false
		for _, e := range entries {
			if considered >= recheckFilesPerRef {
				break
			}
			if e.ReleaseID != ref.ReleaseID {
				continue
			}
			considered++
			// Asking is read-only: the budget is spent below, and only when
			// a check really was moved forward.
			advice, err := src.Advisor.HealthAdvice(ctx, ref.ReleaseID, ref.FileID, src.Backbones(), now)
			if err != nil {
				hub.lookupError()
				return false
			}
			if advice.Kind != hints.AdviceRecheckSooner {
				continue
			}
			moved, err := src.Scheduler.ScheduleCheckNow(ctx, e.FilePath)
			n.logChange(ctx, "schedule check on peer evidence", err)
			if err != nil {
				failed = true
				continue
			}
			if moved && src.Advisor.SpendRecheck(ref.ReleaseID, ref.FileID, advice.EvidenceAt, now) {
				hub.add(func(h *hintsHub) { h.recheckScheduled++ })
			}
		}
		if failed && n.retryRecheck(network, ref.Seq) {
			return false // retried on the next pass; the cursor stays
		}
		n.hintCursor[network] = ref.Seq
	}
	if len(page.Rows) == 0 {
		n.hintCursor[network] = page.HighestSeq
	}
	return true
}

// recheckRetry counts the failed scheduling attempts for one position.
type recheckRetry struct {
	seq      int64
	attempts int
}

// retryRecheck records a failed scheduling at seq and reports whether the
// evidence is to be retried on the next pass (at most maxRecheckAttempts
// attempts in all, then it is let go so a persistent failure cannot stall the
// pass). The caller holds hintMu.
func (n *node) retryRecheck(network string, seq int64) bool {
	r := n.hintRetries[network]
	if r.seq != seq {
		r = recheckRetry{seq: seq}
	}
	r.attempts++
	if r.attempts >= maxRecheckAttempts {
		delete(n.hintRetries, network)
		return false
	}
	n.hintRetries[network] = r
	return true
}

// clearHintCursor forgets the recheck position of a network that was removed:
// its record sequence restarts at 1 when it is joined again, so a stale
// position would hide new evidence.
func (n *node) clearHintCursor(id netid.NetworkID) {
	n.hintMu.Lock()
	defer n.hintMu.Unlock()
	delete(n.hintCursor, id.String())
	delete(n.hintRetries, id.String())
}
