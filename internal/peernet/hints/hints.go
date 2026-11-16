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
	// MaxLabelFilesPerRelease is how many main (video) files of one release the
	// network-health label looks at; a release with more is not labelled.
	MaxLabelFilesPerRelease = 16
	// MaxLabelFiles bounds the distinct files of one labelling call.
	MaxLabelFiles       = database.PeernetMaxFilesPerQuery
	maxPublisherLookups = 64
	maxAdvised          = 4096
	prefetchChunk       = 200

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
	// AttestationsForFiles returns attestations of the given files whatever
	// release they were published under (at most one row per file, publisher
	// and verdict).
	AttestationsForFiles(ctx context.Context, networkID string, fileIDs []string, notBefore time.Time) ([]database.PeernetAttestation, error)
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
