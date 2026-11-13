// Package hints turns stored peer data into advice. It is pure logic over a
// read-only store: it never writes a health status, never lengthens a check
// interval and has no goroutines or timers; all time is passed in or comes
// from an injected clock.
//
// # Policy
//
// Evidence that counts as "healthy" must be fresh (checked within the 7-day
// attestation validity and not dated in the future), for the exact file
// asked about, not the local node's own, and carry a non-empty backbone set
// that is a subset of the local backbones. Records are verified at ingest by
// the service; this package relies on the store holding only verified rows and
// re-checks everything cheap that the store's filters might have missed.
//
//   - Open network: healthy evidence only ever reduces the sample to its
//     floor, and only when at least MinCorroboration distinct publishers
//     (default 3) each known locally for 24 hours attest it. Publishers are
//     counted once per network prefix (/24 IPv4, /48 IPv6) of the address this
//     node verified them at; one without a verified address does not count.
//     Young identities are ignored.
//   - Private network: healthy evidence from PrivateThreshold (default 1)
//     members may skip one scheduled check, never past the newest
//     attestation's validity. Members are vouched as members, not for what
//     they say, so a single member suffices and no age is required; a stricter
//     threshold is a parameter.
//   - Unhealthy evidence (any network, backbones overlapping the local ones,
//     from publishers held to the same known-for-a-day standing and the same
//     distinct-prefix threshold as healthy evidence on the open network; a
//     lone unhealthy publisher neither vetoes a label nor moves a check) may
//     move a check forward, paid from one
//     shared hourly Budget. HealthAdvice only reports it; the budget is spent
//     by SpendRecheck once a check really was moved, and the same evidence is
//     acted on once. Conflicting evidence never shortens anything.
//   - Evidence from one network never grants the privileges of another.
package hints

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/records"
)

// Limits and defaults.
const (
	// DefaultMinKnown is how long a publisher must have been known locally to
	// count in the open network.
	DefaultMinKnown = 24 * time.Hour

	defaultMinCorroboration = 3
	defaultPrivateThreshold = 1
	defaultCandidates       = 50
	maxCandidates           = database.PeernetCandidatesMaxLimit
	maxReleaseIDs           = database.PeernetMaxReleaseIDsPerQuery
	maxNetworks             = database.PeernetMaxNetworksPerQuery
	maxPublisherLookups     = 64
	maxAdvised              = 4096
	prefetchChunk           = 200

	// MinCoverageSegments and MinCoveragePercent set how much of a file a
	// healthy attestation must have checked to count as evidence: at least
	// MinCoverageSegments segments and MinCoveragePercent of the file (the
	// larger of the two, capped at the whole file). The health checker's
	// default sample is 5% of the segments (never fewer than 5), while a
	// peer-reduced check samples only max(10 segments, 2%) and is never
	// attested; so an honest node's normal scheduled check qualifies and a
	// floor-level one does not. Nodes configured below 5% sampling simply
	// publish evidence that does not qualify: that is the safe direction.
	MinCoverageSegments = 10
	MinCoveragePercent  = 5
	maxIMDbDigits       = 10
)

// ErrTooManyIDs is returned when more distinct release IDs are asked for than
// one call may handle.
var ErrTooManyIDs = errors.New("hints: too many release ids")

// AdviceKind is what a health check may do with peer evidence.
type AdviceKind int

const (
	// AdviceNone: do nothing special.
	AdviceNone AdviceKind = iota
	// AdviceReduceSampleToFloor: the sample may be cut to its floor (open
	// network evidence). The check still runs.
	AdviceReduceSampleToFloor
	// AdviceMaySkipOneCheck: one scheduled check may be skipped until
	// Advice.SkipUntil (private network evidence).
	AdviceMaySkipOneCheck
	// AdviceRecheckSooner: a check may be moved forward. A token has already
	// been spent from the shared budget for it.
	AdviceRecheckSooner
)

func (k AdviceKind) String() string {
	switch k {
	case AdviceNone:
		return "none"
	case AdviceReduceSampleToFloor:
		return "reduce_sample_to_floor"
	case AdviceMaySkipOneCheck:
		return "may_skip_one_check"
	case AdviceRecheckSooner:
		return "recheck_sooner"
	}
	return "unknown"
}

// Advice is a value; applying it is the caller's decision and it never
// changes a status.
type Advice struct {
	Kind AdviceKind
	// SkipUntil is set only for AdviceMaySkipOneCheck, never later than the
	// newest supporting attestation's validity.
	SkipUntil time.Time
	// Network is the network whose evidence produced a reduce or skip.
	Network netid.NetworkID
	// EvidenceAt is the check time of the newest unhealthy attestation behind
	// an AdviceRecheckSooner; hand it to SpendRecheck.
	EvidenceAt time.Time
}

// Network is one network and this node's exposure level on it. Levels of 1
// and above receive.
type Network struct {
	ID    netid.NetworkID
	Level int
}

// Store is the read-only view of stored peer records. It is satisfied by
// *database.PeernetRecordRepository.
type Store interface {
	AttestationsFor(ctx context.Context, networkID, releaseID, fileID string, notBefore time.Time) ([]database.PeernetAttestation, error)
	HealthSummaryForReleases(ctx context.Context, networkID string, releaseIDs []string, notBefore time.Time) (map[string]database.PeernetHealthSummary, error)
	CandidatesByMedia(ctx context.Context, networkIDs []string, imdbID string, season, episode *int, limit int) ([]database.PeernetCandidate, error)
}

// PeerSeen answers "has this node been known for at least d". It is satisfied
// by *database.PeernetPeerRepository.
type PeerSeen interface {
	KnownFor(ctx context.Context, network, node string, d time.Duration, now time.Time) (bool, error)
}

// PeerSeenBatch is an optional extension of PeerSeen: it answers the question
// for many nodes in one lookup and returns the nodes that are known. When the
// peer lookup has it, fresh identities cannot use up the per-call lookup
// bound before known publishers are reached. It is satisfied by
// *database.PeernetPeerRepository.
type PeerSeenBatch interface {
	KnownAmong(ctx context.Context, network string, nodes []string, d time.Duration, now time.Time) (map[string]bool, error)
}

// PeerPrefixes is the preferred batch lookup on the open network: it returns,
// for the nodes that are known (see PeerSeen), the network prefix (/24 IPv4,
// /48 IPv6) of the address this node verified them at. A known node without a
// verified address is absent, so it counts as nothing. It is satisfied by
// *database.PeernetPeerRepository.
type PeerPrefixes interface {
	KnownPrefixes(ctx context.Context, network string, nodes []string, d time.Duration, now time.Time) (map[string]string, error)
}

// CoverageSufficient reports whether a healthy attestation that checked
// `checked` of `total` segments covers enough of the file to count. Unknown
// coverage (0/0, rows stored before the columns existed) never does.
func CoverageSufficient(checked, total int) bool {
	if total <= 0 || checked <= 0 || checked > total {
		return false
	}
	need := max(MinCoverageSegments, (total*MinCoveragePercent+99)/100)
	return checked >= min(need, total)
}

// Config wires an Advisor. Store, Peers, Budget and Networks are required.
type Config struct {
	Store    Store
	Peers    PeerSeen
	Budget   *Budget
	Networks func() []Network
	Now      func() time.Time
	// SelfNodeID is this node's ID (lower-case hex); its relayed records are
	// never evidence.
	SelfNodeID string
	// MinCorroboration is read on every call (live config). Below 1 means the
	// default of 3.
	MinCorroboration func() int
	// PrivateThreshold is the number of members needed in a private network
	// (default 1).
	PrivateThreshold int
	// MinKnown overrides DefaultMinKnown.
	MinKnown time.Duration
	// CandidateLimit bounds Candidates (default 50, at most 200).
	CandidateLimit int
}

// Advisor produces advice from stored peer data.
type Advisor struct {
	cfg Config

	mu      sync.Mutex
	advised map[string]time.Time // file key -> newest unhealthy evidence acted on
}

// New validates cfg and returns an Advisor.
func New(cfg Config) (*Advisor, error) {
	switch {
	case cfg.Store == nil:
		return nil, errors.New("hints: store required")
	case cfg.Peers == nil:
		return nil, errors.New("hints: peer lookup required")
	case cfg.Budget == nil:
		return nil, errors.New("hints: budget required")
	case cfg.Networks == nil:
		return nil, errors.New("hints: network source required")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.PrivateThreshold < 1 {
		cfg.PrivateThreshold = defaultPrivateThreshold
	}
	if cfg.MinKnown <= 0 {
		cfg.MinKnown = DefaultMinKnown
	}
	if cfg.CandidateLimit < 1 {
		cfg.CandidateLimit = defaultCandidates
	}
	cfg.CandidateLimit = min(cfg.CandidateLimit, maxCandidates)
	cfg.SelfNodeID = strings.ToLower(cfg.SelfNodeID)
	return &Advisor{cfg: cfg, advised: map[string]time.Time{}}, nil
}

func (a *Advisor) corroboration() int {
	if a.cfg.MinCorroboration != nil {
		if n := a.cfg.MinCorroboration(); n >= 1 {
			return n
		}
	}
	return defaultMinCorroboration
}

func (a *Advisor) receiving() []Network {
	var out []Network
	for _, n := range a.cfg.Networks() {
		if n.Level >= 1 && len(out) < maxNetworks {
			out = append(out, n)
		}
	}
	return out
}

func (a *Advisor) threshold(n Network) int {
	if n.ID.IsOpen() {
		return a.corroboration()
	}
	return a.cfg.PrivateThreshold
}

// usable reports whether an attestation row may be evidence at all.
func (a *Advisor) usable(att database.PeernetAttestation, now time.Time) bool {
	if att.Source == database.PeernetRecordSourceLocal || strings.EqualFold(att.PublisherNodeID, a.cfg.SelfNodeID) {
		return false
	}
	c := att.CheckedAt
	return !c.IsZero() && !c.After(now.Add(records.MaxFutureSkew)) && now.Sub(c) <= records.AttestationValidity
}

func normaliseSet(in []string) map[string]struct{} {
	out := make(map[string]struct{}, len(in))
	for _, raw := range in {
		if l, ok := records.NormaliseBackboneLabel(raw); ok {
			out[l] = struct{}{}
		}
	}
	return out
}

// relation of an attestation's backbones to the local ones.
func relation(att []string, local map[string]struct{}) (subset, overlap bool) {
	if len(att) == 0 {
		return false, false
	}
	subset = true
	for _, raw := range att {
		l, ok := records.NormaliseBackboneLabel(raw)
		if !ok {
			subset = false
			continue
		}
		if _, in := local[l]; in {
			overlap = true
		} else {
			subset = false
		}
	}
	return subset, overlap
}

func validID(s string) bool { return netid.IsHexID(s, true) }

type adviceEvidence struct {
	openHealthy     int
	privHealthy     int
	privNewest      time.Time
	privNet         netid.NetworkID
	openNet         netid.NetworkID
	unhealthyNewest time.Time
}

// HealthAdvice returns what peer evidence allows for one file. localBackbones
// are the raw backbone labels of the local providers. now is the caller's
// current time (normally the injected clock). Errors from the store or the
// peer table are returned with AdviceNone: advice is never guessed.
func (a *Advisor) HealthAdvice(ctx context.Context, releaseID, fileID string, localBackbones []string, now time.Time) (Advice, error) {
	if !validID(releaseID) || !validID(fileID) {
		return Advice{}, errors.New("hints: release and file ids must be 64 lower-case hex characters")
	}
	local := normaliseSet(localBackbones)
	if len(local) == 0 {
		return Advice{}, nil
	}
	var ev adviceEvidence
	for _, n := range a.receiving() {
		if err := a.gather(ctx, n, releaseID, fileID, local, now, &ev); err != nil {
			return Advice{}, err
		}
	}
	return a.decide(releaseID+"|"+fileID, ev, now), nil
}

func (a *Advisor) gather(ctx context.Context, n Network, releaseID, fileID string, local map[string]struct{}, now time.Time, ev *adviceEvidence) error {
	atts, err := a.cfg.Store.AttestationsFor(ctx, n.ID.String(), releaseID, fileID, now.Add(-records.AttestationValidity))
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

// knownCache answers per-call "may this publisher count" questions: always
// yes in a private network, and in the open network only for nodes known for
// MinKnown, with a bounded number of lookups per call. On the open network it
// also tells which network prefix a publisher was verified at, so identities
// sharing a prefix can be counted once.
type knownCache struct {
	a       *Advisor
	ctx     context.Context
	n       Network
	now     time.Time
	res     map[string]bool
	prefix  map[string]string
	lookups int
}

func newKnownCache(a *Advisor, ctx context.Context, n Network, now time.Time) *knownCache {
	return &knownCache{a: a, ctx: ctx, n: n, now: now, res: map[string]bool{}, prefix: map[string]string{}}
}

// groupOf is the key distinct publishers are counted by: the publisher itself
// in a private network, its verified network prefix on the open one.
func (k *knownCache) groupOf(node string) string {
	if p, ok := k.prefix[node]; ok {
		return p
	}
	return node
}

// prefetch resolves the standing of many publishers with one batch lookup
// per chunk when the peer lookup supports it. Results are exact, so the
// per-call lookup bound (which only guards one-by-one lookups) cannot let
// fresh identities push known publishers out.
func (k *knownCache) prefetch(nodes []string) error {
	if !k.n.ID.IsOpen() || len(nodes) == 0 {
		return nil
	}
	pre, hasPrefixes := k.a.cfg.Peers.(PeerPrefixes)
	batch, hasBatch := k.a.cfg.Peers.(PeerSeenBatch)
	if !hasPrefixes && !hasBatch {
		return nil
	}
	for start := 0; start < len(nodes); start += prefetchChunk {
		chunk := nodes[start:min(start+prefetchChunk, len(nodes))]
		if hasPrefixes {
			got, err := pre.KnownPrefixes(k.ctx, k.n.ID.String(), chunk, k.a.cfg.MinKnown, k.now)
			if err != nil {
				return fmt.Errorf("hints: peer first-seen: %w", err)
			}
			for _, n := range chunk {
				p, ok := got[n]
				k.res[n] = ok && p != ""
				if k.res[n] {
					k.prefix[n] = p
				}
			}
			continue
		}
		got, err := batch.KnownAmong(k.ctx, k.n.ID.String(), chunk, k.a.cfg.MinKnown, k.now)
		if err != nil {
			return fmt.Errorf("hints: peer first-seen: %w", err)
		}
		for _, n := range chunk {
			k.res[n] = got[n]
		}
	}
	return nil
}

func (k *knownCache) eligible(node string) (bool, error) {
	if !k.n.ID.IsOpen() {
		return true, nil
	}
	if v, ok := k.res[node]; ok {
		return v, nil
	}
	if k.lookups >= maxPublisherLookups {
		return false, nil
	}
	k.lookups++
	ok, err := k.a.cfg.Peers.KnownFor(k.ctx, k.n.ID.String(), node, k.a.cfg.MinKnown, k.now)
	if err != nil {
		return false, fmt.Errorf("hints: peer first-seen: %w", err)
	}
	k.res[node] = ok
	return ok, nil
}

// NetworkHealth is the network-wide health label of one release. It is never
// a status.
type NetworkHealth struct {
	// Healthy: on some receiving network enough eligible publishers attest it
	// healthy and unhealthy evidence does not itself reach the threshold on
	// any network.
	Healthy bool
	// HealthyPublishers and UnhealthyPublishers count eligible publishers
	// (distinct network prefixes on the open network), summed over networks
	// (informational).
	HealthyPublishers   int
	UnhealthyPublishers int
	// Newest is the latest check time among eligible healthy attestations.
	Newest time.Time
}

// NetworkHealthy labels releases for candidate lists (trust rule 5). Every
// requested release is present in the result. At most 200 distinct IDs are
// accepted per call.
func (a *Advisor) NetworkHealthy(ctx context.Context, releaseIDs ...string) (map[string]NetworkHealth, error) {
	ids := make([]string, 0, len(releaseIDs))
	seen := make(map[string]struct{}, len(releaseIDs))
	for _, id := range releaseIDs {
		if !validID(id) {
			return nil, errors.New("hints: release ids must be 64 lower-case hex characters")
		}
		if _, dup := seen[id]; !dup {
			seen[id] = struct{}{}
			ids = append(ids, id)
		}
	}
	if len(ids) > maxReleaseIDs {
		return nil, ErrTooManyIDs
	}
	out := make(map[string]NetworkHealth, len(ids))
	anyHealthy := make(map[string]bool, len(ids))
	vetoed := make(map[string]bool, len(ids))
	for _, id := range ids {
		out[id] = NetworkHealth{}
	}
	if len(ids) == 0 {
		return out, nil
	}
	now := a.cfg.Now()
	for _, n := range a.receiving() {
		if err := a.labelNetwork(ctx, n, ids, now, out, anyHealthy, vetoed); err != nil {
			return nil, err
		}
	}
	for id, h := range out {
		h.Healthy = anyHealthy[id] && !vetoed[id]
		out[id] = h
	}
	return out, nil
}

func (a *Advisor) labelNetwork(ctx context.Context, n Network, ids []string, now time.Time, out map[string]NetworkHealth, anyHealthy, vetoed map[string]bool) error {
	notBefore := now.Add(-records.AttestationValidity)
	summary, err := a.cfg.Store.HealthSummaryForReleases(ctx, n.ID.String(), ids, notBefore)
	if err != nil {
		return fmt.Errorf("hints: health summary: %w", err)
	}
	for _, id := range ids {
		if _, ok := summary[id]; !ok {
			continue
		}
		atts, err := a.cfg.Store.AttestationsFor(ctx, n.ID.String(), id, "", notBefore)
		if err != nil {
			return fmt.Errorf("hints: attestations: %w", err)
		}
		hE, uE, newest, err := a.tally(ctx, n, atts, now)
		if err != nil {
			return err
		}
		h := out[id]
		h.HealthyPublishers += hE
		h.UnhealthyPublishers += uE
		if newest.After(h.Newest) {
			h.Newest = newest
		}
		out[id] = h
		// A lone unhealthy publisher does not veto: unhealthy evidence has to
		// meet the same threshold as healthy corroboration.
		if uE >= a.threshold(n) {
			vetoed[id] = true
		}
		if hE >= a.threshold(n) {
			anyHealthy[id] = true
		}
	}
	return nil
}

// tally counts eligible publishers, one per distinct group (see groupOf). A
// publisher that attested anything unhealthy counts as unhealthy only.
func (a *Advisor) tally(ctx context.Context, n Network, atts []database.PeernetAttestation, now time.Time) (healthy, unhealthy int, newest time.Time, err error) {
	type verdict struct {
		bad    bool
		newest time.Time
	}
	order := []string{}
	per := map[string]*verdict{}
	for _, att := range atts {
		if !a.usable(att, now) {
			continue
		}
		v := per[att.PublisherNodeID]
		if v == nil {
			v = &verdict{}
			per[att.PublisherNodeID] = v
			order = append(order, att.PublisherNodeID)
		}
		if !att.Healthy {
			v.bad = true
		} else if CoverageSufficient(att.SegmentsChecked, att.TotalSegments) && att.CheckedAt.After(v.newest) {
			v.newest = att.CheckedAt
		}
	}
	known := newKnownCache(a, ctx, n, now)
	if err := known.prefetch(order); err != nil {
		return 0, 0, time.Time{}, err
	}
	healthyG, unhealthyG := map[string]struct{}{}, map[string]struct{}{}
	for _, node := range order {
		ok, kerr := known.eligible(node)
		if kerr != nil {
			return 0, 0, time.Time{}, kerr
		}
		if !ok {
			continue
		}
		v := per[node]
		g := known.groupOf(node)
		switch {
		case v.bad:
			unhealthyG[g] = struct{}{}
		case !v.newest.IsZero():
			healthyG[g] = struct{}{}
			if v.newest.After(newest) {
				newest = v.newest
			}
		}
	}
	return len(healthyG), len(unhealthyG), newest, nil
}

// Candidate is one release found for a media key. Title is peer-supplied data
// to be rendered as text only.
type Candidate struct {
	ReleaseID string
	Title     string
	TotalSize int64
	CreatedAt time.Time
	// Networks lists every receiving network the release was seen in, sorted.
	Networks []netid.NetworkID
	// Own marks releases this node published itself.
	Own    bool
	Health NetworkHealth
}

func validIMDb(s string) bool {
	if len(s) < 3 || len(s) > 2+maxIMDbDigits || s[:2] != "tt" {
		return false
	}
	for i := 2; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// Candidates returns the release records known for a media key across the
// networks this node receives from, one entry per release ID, healthy-labelled
// first, then newest, then by release ID; at most CandidateLimit entries. A
// nil season and episode match movies only (see the store contract).
func (a *Advisor) Candidates(ctx context.Context, imdbID string, season, episode *int) ([]Candidate, error) {
	if !validIMDb(imdbID) {
		return nil, errors.New("hints: invalid imdb id")
	}
	if (season != nil && *season < 0) || (episode != nil && *episode < 0) {
		return nil, errors.New("hints: negative season or episode")
	}
	nets := a.receiving()
	if len(nets) == 0 {
		return nil, nil
	}
	idStrs := make([]string, len(nets))
	for i, n := range nets {
		idStrs[i] = n.ID.String()
	}
	rows, err := a.cfg.Store.CandidatesByMedia(ctx, idStrs, imdbID, season, episode, a.cfg.CandidateLimit)
	if err != nil {
		return nil, fmt.Errorf("hints: candidates: %w", err)
	}
	merged := a.merge(rows)
	if len(merged) == 0 {
		return nil, nil
	}
	ids := make([]string, len(merged))
	for i, c := range merged {
		ids[i] = c.ReleaseID
	}
	health, err := a.NetworkHealthy(ctx, ids...)
	if err != nil {
		return nil, err
	}
	for i := range merged {
		merged[i].Health = health[merged[i].ReleaseID]
	}
	sort.Slice(merged, func(i, j int) bool {
		x, y := merged[i], merged[j]
		switch {
		case x.Health.Healthy != y.Health.Healthy:
			return x.Health.Healthy
		case !x.CreatedAt.Equal(y.CreatedAt):
			return x.CreatedAt.After(y.CreatedAt)
		}
		return x.ReleaseID < y.ReleaseID
	})
	if len(merged) > a.cfg.CandidateLimit {
		merged = merged[:a.cfg.CandidateLimit]
	}
	return merged, nil
}

// merge de-duplicates rows by release ID; the newest record defines the entry.
func (a *Advisor) merge(rows []database.PeernetCandidate) []Candidate {
	byID := map[string]*Candidate{}
	var order []string
	for _, r := range rows {
		nw, err := netid.ParseNetworkID(r.NetworkID)
		title := records.CleanTitle(r.Title)
		if err != nil || !validID(r.ReleaseID) || title == "" {
			continue
		}
		own := r.Source == database.PeernetRecordSourceLocal || strings.EqualFold(r.PublisherNodeID, a.cfg.SelfNodeID)
		c := byID[r.ReleaseID]
		if c == nil {
			c = &Candidate{ReleaseID: r.ReleaseID}
			byID[r.ReleaseID] = c
			order = append(order, r.ReleaseID)
		}
		if c.Title == "" || r.CreatedAt.After(c.CreatedAt) {
			c.Title, c.TotalSize, c.CreatedAt = title, r.TotalSize, r.CreatedAt
		}
		c.Own = c.Own || own
		if !containsNet(c.Networks, nw) {
			c.Networks = append(c.Networks, nw)
		}
	}
	out := make([]Candidate, 0, len(order))
	for _, id := range order {
		c := byID[id]
		sort.Slice(c.Networks, func(i, j int) bool { return c.Networks[i].String() < c.Networks[j].String() })
		out = append(out, *c)
	}
	return out
}

func containsNet(list []netid.NetworkID, n netid.NetworkID) bool {
	for _, x := range list {
		if x == n {
			return true
		}
	}
	return false
}

// SpendRecheck pays one token of the shared budget for moving a check forward
// on the unhealthy evidence of the given check time, and remembers that the
// evidence was acted on. It reports false when this evidence was already acted
// on or no token is left. It is called only once a check really was moved:
// HealthAdvice itself spends nothing.
func (a *Advisor) SpendRecheck(releaseID, fileID string, evidenceAt, now time.Time) bool {
	key := releaseID + "|" + fileID
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
