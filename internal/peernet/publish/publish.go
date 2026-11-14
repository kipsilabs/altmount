// Package publish decides what this node publishes to the peer network and
// builds the signed records.
//
// It is pure logic over narrow interfaces: a record signer, a sink that
// accepts encoded records, a store of last-published times, and sources for
// the live network list, exclusions and provider backbones. It never touches
// the database, the config manager or a local path.
//
// # Policy
//
//   - Nothing is built for a network whose level is below 2.
//   - A release in an excluded category or from an excluded indexer produces
//     nothing at all: the check runs before anything is signed.
//   - Unknown indexer or category: see UnknownPolicy. The default refuses to
//     publish an unknown origin while the user has configured exclusions of
//     that kind, because an exclusion exists to honour indexer terms and an
//     unknown origin cannot be shown to be outside it.
//   - Files made healthy by local PAR2 patches are never attested. The release
//     record may still be published.
//   - An attestation needs at least one recognised backbone (normalised and
//     present in the known backbone list). Without one only the release
//     record is published.
//   - Only healthy results are published by default, matching the design's
//     "eligible to publish" rule. Unhealthy attestations are a separate opt-in
//     (Config.PublishUnhealthy) and then only from a result that observed
//     missing segments; an inconclusive result is never published.
//   - A release that came from a peer bundle is published only after its full
//     local check passed.
//   - The title is the release title, never a path: anything up to the last
//     path separator is dropped, a trailing .nzb is removed and control
//     characters are cleaned. Category, indexer and local names never enter a
//     record.
package publish

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/kipsilabs/altmount/internal/nzbname"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/records"
)

// MinLevel is the lowest exposure level that publishes anything.
const MinLevel = 2

// ErrInvalidInput marks a caller bug: input that can never become a record.
var ErrInvalidInput = errors.New("publish: invalid input")

// Verdict is the outcome of one local check.
type Verdict int

const (
	// VerdictHealthy: every checked segment was found.
	VerdictHealthy Verdict = iota + 1
	// VerdictUnhealthy: at least one segment was confirmed missing.
	VerdictUnhealthy
	// VerdictInconclusive: the providers did not answer for some segments, or
	// the check was cut short. It says nothing about the file and is never
	// published.
	VerdictInconclusive
)

// CheckResult describes one locally completed check of one file.
type CheckResult struct {
	ReleaseID records.ReleaseID
	FileID    records.FileID
	CheckedAt time.Time
	Verdict   Verdict

	TotalSegments   uint32
	SegmentsChecked uint32
	// MissingCount is the number of confirmed-missing segments: zero for a
	// healthy verdict, at least one for an unhealthy one.
	MissingCount        uint32
	MediaHeaderVerified bool
	// PatchedByPAR2 is set when any segment of the file is served from a local
	// PAR2 patch.
	PatchedByPAR2 bool
	// AttestationSkip, when set, publishes the release record only and skips
	// the attestation with this reason. It is for checks that cannot serve as
	// evidence for a reason the publisher does not know about itself (a check
	// peer advice shortened, a patch status that was never determined).
	AttestationSkip SkipReason

	// FromPeerBundle marks an import that came from a peer bundle (phase 3).
	// FullCheckPassed reports that every segment was checked and found.
	FromPeerBundle  bool
	FullCheckPassed bool
}

// ReleaseInfo is the release-level data a release record is made of. Category
// and Indexer are used only to apply exclusions and are never published.
type ReleaseInfo struct {
	Title     string
	TotalSize int64
	FileCount uint32
	PostDate  time.Time
	Media     records.MediaIDs
	Category  string
	Indexer   string
	// ReleaseScheme says how the Release ID was derived: records.SchemeNZB (also
	// the zero value) or records.SchemeFiles. It goes into both record types.
	ReleaseScheme uint8
	// Secrets are the import's passwords (archive password, password spelled
	// in the NZB name). They are used only to refuse a title that contains
	// one: never stored, logged or published.
	Secrets []string
}

// BundleMinLevel is the lowest exposure level that announces a bundle.
const BundleMinLevel = 3

// BundleDecision is what a BundleSource says about one release.
type BundleDecision struct {
	// ID is the Bundle ID to announce; zero when the release has no bundle.
	ID records.BundleID
	// Skip names the permanent reason a release cannot be shared as a bundle
	// (the source counts it). The record is published without a Bundle ID and
	// the decision is remembered.
	Skip string
	// Err is a transient failure. The record is published without a Bundle ID
	// and the decision is not remembered, so it is made again soon.
	Err error
}

// BundleSource builds the bundle of a release and returns its ID. It is
// called only for a network at level BundleMinLevel or higher, and only when a
// release record is about to be published.
type BundleSource interface {
	BundleFor(ctx context.Context, rel records.ReleaseID, info ReleaseInfo) BundleDecision
}

// Network is one network and this node's exposure level on it.
type Network struct {
	ID    netid.NetworkID
	Level int
}

// Exclusions are the user's never-share lists. Matching is case-insensitive
// and ignores surrounding space.
type Exclusions struct {
	Categories []string
	Indexers   []string
}

// UnknownPolicy decides what happens when the indexer or category of a release
// is not known.
type UnknownPolicy int

const (
	// UnknownBlockIfExclusions (default) refuses to publish a release whose
	// indexer is unknown while indexer exclusions exist, and likewise for the
	// category. With no exclusion of that kind there is nothing to honour.
	UnknownBlockIfExclusions UnknownPolicy = iota
	// UnknownPublish publishes regardless of unknown origin.
	UnknownPublish
	// UnknownBlock never publishes a release with an unknown indexer or
	// category.
	UnknownBlock
)

// Signer builds signed, encoded records with the node's identity. The node key
// itself never reaches this package: the owner of the key does the signing.
// The method set matches the peer network service's RecordSigner, which
// satisfies it directly.
type Signer interface {
	// NodeID is the publisher ID the built records carry.
	NodeID() identity.NodeID
	BuildRelease(nw netid.NetworkID, in records.ReleaseRecord, now time.Time) ([]byte, error)
	BuildAttestation(nw netid.NetworkID, in records.HealthAttestation, now time.Time) ([]byte, error)
}

// Sink accepts encoded, signed records for a network.
type Sink interface {
	PublishLocal(ctx context.Context, networkID netid.NetworkID, encoded []byte) error
}

// Backbones supplies backbone names. ProviderBackbones returns the raw
// storage_group of each provider a check runs against; KnownBackbones returns
// the known backbone list. An empty known list (not fetched yet) means no
// label can be recognised, so no attestation is published.
type Backbones interface {
	ProviderBackbones(ctx context.Context) []string
	KnownBackbones(ctx context.Context) []string
}

// StateStore remembers what was last published per network, kind and file.
// Release records use the zero FileID.
type StateStore interface {
	LastPublished(ctx context.Context, nw netid.NetworkID, kind records.Kind, rel records.ReleaseID, file records.FileID) (State, bool, error)
	MarkPublished(ctx context.Context, nw netid.NetworkID, kind records.Kind, rel records.ReleaseID, file records.FileID, st State) error
}

// Config wires a Publisher. Signer, Sink, Store, Backbones and Networks are
// required. Networks and Exclusions are called on every Publish so live config
// changes apply at once.
type Config struct {
	Signer     Signer
	Sink       Sink
	Store      StateStore
	Backbones  Backbones
	Networks   func() []Network
	Exclusions func() Exclusions
	Now        func() time.Time
	// Bundles supplies Bundle IDs for networks at level 3. Nil: no record
	// carries one.
	Bundles BundleSource

	Unknown UnknownPolicy
	// PublishUnhealthy opts in to unhealthy attestations. Default off.
	PublishUnhealthy bool
	Plan             PlanConfig
}

// SkipReason says why a record was not published.
type SkipReason string

// Skip reasons. SkipNone means published.
const (
	SkipNone               SkipReason = ""
	SkipLevel              SkipReason = "level_below_share_metadata"
	SkipExcludedCategory   SkipReason = "excluded_category"
	SkipExcludedIndexer    SkipReason = "excluded_indexer"
	SkipUnknownIndexer     SkipReason = "unknown_indexer"
	SkipUnknownCategory    SkipReason = "unknown_category"
	SkipPeerBundle         SkipReason = "peer_bundle_not_fully_checked"
	SkipInconclusive       SkipReason = "inconclusive_result"
	SkipUnhealthyNotShared SkipReason = "unhealthy_not_shared"
	SkipPatched            SkipReason = "patched_by_par2"
	SkipNoBackbone         SkipReason = "no_recognised_backbone"
	SkipStale              SkipReason = "check_too_old"
	SkipNotDue             SkipReason = "not_due"
	SkipBundleUnavailable  SkipReason = "bundle_unavailable"
	SkipTitleHasPassword   SkipReason = "title_contains_password"
)

// Outcome is the result for one record kind on one network. Network and Kind
// are zero when the skip applies to everything.
type Outcome struct {
	Network netid.NetworkID
	Kind    records.Kind
	Skip    SkipReason
	Err     error
	// BundleErr is a transient failure to build the bundle of a release
	// record. The release is skipped (SkipBundleUnavailable) and its stored
	// state kept, so a record that had a bundle is never superseded by one
	// without it; the next pass retries.
	BundleErr error
}

// Publisher builds and publishes records. It is safe for concurrent use as
// far as its collaborators are; two concurrent calls for the same file may at
// worst publish the same verdict twice.
type Publisher struct {
	cfg Config
}

// New validates cfg and returns a Publisher.
func New(cfg Config) (*Publisher, error) {
	switch {
	case cfg.Signer == nil:
		return nil, errors.New("publish: signer required")
	case cfg.Sink == nil:
		return nil, errors.New("publish: sink required")
	case cfg.Store == nil:
		return nil, errors.New("publish: state store required")
	case cfg.Backbones == nil:
		return nil, errors.New("publish: backbone source required")
	case cfg.Networks == nil:
		return nil, errors.New("publish: network source required")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Exclusions == nil {
		cfg.Exclusions = func() Exclusions { return Exclusions{} }
	}
	cfg.Plan = cfg.Plan.withDefaults()
	return &Publisher{cfg: cfg}, nil
}

// Publish decides what to publish for one finished check and publishes it on
// every eligible network. The returned error joins build, sink and store
// failures; policy skips are not errors and are listed in the outcomes.
func (p *Publisher) Publish(ctx context.Context, res CheckResult, info ReleaseInfo) ([]Outcome, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validate(res, info); err != nil {
		return nil, err
	}
	eligible, skipped := p.eligibleNetworks()
	if len(eligible) == 0 {
		return skipped, nil
	}
	if skip := p.exclusionSkip(info); skip != SkipNone {
		return []Outcome{{Skip: skip}}, nil
	}
	if skip := p.resultSkip(res); skip != SkipNone {
		return []Outcome{{Kind: records.KindAttestation, Skip: skip}}, nil
	}

	now := p.cfg.Now().UTC()
	job := &job{p: p, res: res, info: info, now: now}
	var outs []Outcome
	var errs []error
	for _, n := range eligible {
		if err := ctx.Err(); err != nil {
			return outs, errors.Join(append(errs, err)...)
		}
		for _, o := range job.run(ctx, n) {
			outs = append(outs, o)
			if o.Err != nil {
				errs = append(errs, o.Err)
			}
		}
	}
	return outs, errors.Join(errs...)
}

// ReannounceRelease publishes the release record of rel again on one network
// when it is due for a refresh, so live data does not expire after
// RecordLifetime. It applies the same level and exclusion rules as Publish and
// never builds an attestation: an attestation is refreshed only by a new local
// check, so freshness is never invented. The returned Outcome carries the
// network and kind; Err is set on a build, sink or state failure.
func (p *Publisher) ReannounceRelease(ctx context.Context, nw netid.NetworkID, rel records.ReleaseID, info ReleaseInfo) Outcome {
	o := Outcome{Network: nw, Kind: records.KindRelease}
	if err := ctx.Err(); err != nil {
		o.Err = err
		return o
	}
	now := p.cfg.Now().UTC()
	// A placeholder file ID and a healthy verdict stand in for a check: only
	// the release-level fields are validated.
	probe := CheckResult{ReleaseID: rel, FileID: records.FileID{1}, CheckedAt: now, Verdict: VerdictHealthy}
	if err := validate(probe, info); err != nil {
		o.Err = err
		return o
	}
	allowed, level := false, 0
	for _, n := range p.cfg.Networks() {
		if n.ID == nw && n.Level >= MinLevel {
			allowed, level = true, n.Level
			break
		}
	}
	if !allowed {
		o.Skip = SkipLevel
		return o
	}
	if skip := p.exclusionSkip(info); skip != SkipNone {
		o.Skip = skip
		return o
	}
	j := &job{p: p, res: CheckResult{ReleaseID: rel}, info: info, now: now}
	return j.release(ctx, nw, level)
}

func validate(res CheckResult, info ReleaseInfo) error {
	switch {
	case info.ReleaseScheme > records.SchemeFiles:
		return fmt.Errorf("%w: release scheme", ErrInvalidInput)
	case res.ReleaseID == records.ReleaseID{}:
		return fmt.Errorf("%w: zero release id", ErrInvalidInput)
	case res.FileID == records.FileID{}:
		return fmt.Errorf("%w: zero file id", ErrInvalidInput)
	case res.CheckedAt.IsZero():
		return fmt.Errorf("%w: no check time", ErrInvalidInput)
	case res.Verdict < VerdictHealthy || res.Verdict > VerdictInconclusive:
		return fmt.Errorf("%w: no verdict", ErrInvalidInput)
	case res.Verdict == VerdictHealthy && res.MissingCount != 0:
		return fmt.Errorf("%w: healthy verdict with missing segments", ErrInvalidInput)
	}
	if res.Verdict != VerdictHealthy {
		return nil
	}
	switch {
	case info.FileCount < 1:
		return fmt.Errorf("%w: file count", ErrInvalidInput)
	case info.TotalSize < 0:
		return fmt.Errorf("%w: total size", ErrInvalidInput)
	case releaseTitle(info.Title) == "":
		return fmt.Errorf("%w: empty title", ErrInvalidInput)
	}
	return nil
}

func (p *Publisher) eligibleNetworks() (eligible []Network, skipped []Outcome) {
	for _, n := range p.cfg.Networks() {
		if n.Level >= MinLevel {
			eligible = append(eligible, n)
		} else {
			skipped = append(skipped, Outcome{Network: n.ID, Skip: SkipLevel})
		}
	}
	return eligible, skipped
}

func normName(s string) string { return strings.ToLower(strings.TrimSpace(s)) }

func listed(list []string, name string) bool {
	name = normName(name)
	for _, e := range list {
		if normName(e) == name {
			return true
		}
	}
	return false
}

func (p *Publisher) exclusionSkip(info ReleaseInfo) SkipReason {
	if titleLeaks(releaseTitle(info.Title), info.Secrets) {
		return SkipTitleHasPassword
	}
	ex := p.cfg.Exclusions()
	indexer, category := normName(info.Indexer), normName(info.Category)
	unknownIndexer := indexer == "" || indexer == "unknown"
	unknownCategory := category == ""

	if !unknownCategory && listed(ex.Categories, category) {
		return SkipExcludedCategory
	}
	if !unknownIndexer && listed(ex.Indexers, indexer) {
		return SkipExcludedIndexer
	}
	block := func(unknown bool, exclusions []string) bool {
		if !unknown {
			return false
		}
		switch p.cfg.Unknown {
		case UnknownPublish:
			return false
		case UnknownBlock:
			return true
		}
		return len(exclusions) > 0
	}
	if block(unknownIndexer, ex.Indexers) {
		return SkipUnknownIndexer
	}
	if block(unknownCategory, ex.Categories) {
		return SkipUnknownCategory
	}
	return SkipNone
}

// resultSkip applies the verdict-level rules that stop everything (release
// record and attestation alike).
func (p *Publisher) resultSkip(res CheckResult) SkipReason {
	if res.FromPeerBundle && (!res.FullCheckPassed || res.Verdict != VerdictHealthy || res.SegmentsChecked < res.TotalSegments) {
		return SkipPeerBundle
	}
	switch res.Verdict {
	case VerdictInconclusive:
		return SkipInconclusive
	case VerdictUnhealthy:
		if res.MissingCount == 0 {
			return SkipInconclusive
		}
		if !p.cfg.PublishUnhealthy {
			return SkipUnhealthyNotShared
		}
	}
	return SkipNone
}

// job carries the per-call state shared by every network.
type job struct {
	p        *Publisher
	res      CheckResult
	info     ReleaseInfo
	now      time.Time
	backbone []string
	bbDone   bool
}

func (j *job) backbones(ctx context.Context) []string {
	if !j.bbDone {
		j.backbone = RecognisedBackbones(j.p.cfg.Backbones.ProviderBackbones(ctx), j.p.cfg.Backbones.KnownBackbones(ctx))
		j.bbDone = true
	}
	return j.backbone
}

func (j *job) run(ctx context.Context, n Network) []Outcome {
	var outs []Outcome
	if j.res.Verdict == VerdictHealthy {
		outs = append(outs, j.release(ctx, n.ID, n.Level))
	}
	return append(outs, j.attestation(ctx, n.ID))
}

// bundleFor decides the bundle part of a release record for a network at the
// given level: the Bundle ID to announce and the bundle level to remember.
// Below level 3 (or without a source) the record carries no bundle and the
// remembered level is the network's. A transient failure returns an error and
// the caller publishes nothing for this pass.
func (j *job) bundleFor(ctx context.Context, level int) (id records.BundleID, remembered int, err error) {
	if level < BundleMinLevel || j.p.cfg.Bundles == nil {
		return records.BundleID{}, level, nil
	}
	d := j.p.cfg.Bundles.BundleFor(ctx, j.res.ReleaseID, j.info)
	switch {
	case d.Err != nil:
		return records.BundleID{}, 0, d.Err
	case d.Skip == "":
		return d.ID, level, nil
	}
	return records.BundleID{}, level, nil
}

func (j *job) release(ctx context.Context, nw netid.NetworkID, level int) Outcome {
	o := Outcome{Network: nw, Kind: records.KindRelease}
	var zero records.FileID
	last, ok, err := j.p.cfg.Store.LastPublished(ctx, nw, records.KindRelease, j.res.ReleaseID, zero)
	if err != nil {
		o.Err = fmt.Errorf("publish: read state: %w", err)
		return o
	}
	var lp *State
	if ok {
		lp = &last
	}
	// A level change (below 3 to 3 or back) supersedes the record at once: it
	// is the bundle part of the record that differs.
	levelChanged := lp != nil && (lp.BundleLevel >= BundleMinLevel) != (level >= BundleMinLevel)
	if !j.p.cfg.Plan.DueRelease(lp, j.now) && !levelChanged {
		o.Skip = SkipNotDue
		return o
	}
	bundle, remembered, bundleErr := j.bundleFor(ctx, level)
	if bundleErr != nil {
		o.BundleErr, o.Skip = bundleErr, SkipBundleUnavailable
		return o
	}
	enc, err := j.buildRelease(nw, bundle)
	if err != nil {
		o.Err = err
		return o
	}
	o.Err = j.send(ctx, nw, records.KindRelease, zero, enc, State{PublishedAt: j.now, BundleID: bundle, BundleLevel: remembered})
	return o
}

func (j *job) attestation(ctx context.Context, nw netid.NetworkID) Outcome {
	o := Outcome{Network: nw, Kind: records.KindAttestation}
	checked := j.res.CheckedAt.UTC()
	if checked.After(j.now) {
		checked = j.now
	}
	switch {
	case j.res.PatchedByPAR2:
		o.Skip = SkipPatched
		return o
	case j.res.AttestationSkip != "":
		o.Skip = j.res.AttestationSkip
		return o
	case j.now.Sub(checked) > records.AttestationValidity:
		o.Skip = SkipStale
		return o
	}
	labels := j.backbones(ctx)
	if len(labels) == 0 {
		o.Skip = SkipNoBackbone
		return o
	}
	last, ok, err := j.p.cfg.Store.LastPublished(ctx, nw, records.KindAttestation, j.res.ReleaseID, j.res.FileID)
	if err != nil {
		o.Err = fmt.Errorf("publish: read state: %w", err)
		return o
	}
	var lp *State
	if ok {
		lp = &last
	}
	if !j.p.cfg.Plan.DueAttestation(lp, j.res.Verdict, checked, j.now) {
		o.Skip = SkipNotDue
		return o
	}
	enc, err := j.buildAttestation(nw, checked, labels)
	if err != nil {
		o.Err = err
		return o
	}
	st := State{PublishedAt: j.now, CheckedAt: checked, Verdict: j.res.Verdict}
	o.Err = j.send(ctx, nw, records.KindAttestation, j.res.FileID, enc, st)
	return o
}

func (j *job) buildRelease(nw netid.NetworkID, bundle records.BundleID) ([]byte, error) {
	in := records.ReleaseRecord{
		BundleID:  bundle,
		Title:     releaseTitle(j.info.Title),
		TotalSize: j.info.TotalSize,
		FileCount: j.info.FileCount,
		PostDate:  j.info.PostDate,
		Media:     j.info.Media,
	}
	in.ReleaseID, in.ReleaseScheme = j.res.ReleaseID, j.info.ReleaseScheme
	enc, err := j.p.cfg.Signer.BuildRelease(nw, in, j.now)
	if err != nil && errors.Is(err, records.ErrInvalidField) {
		// Optional data from a local source is dropped rather than losing the
		// whole record.
		in.Media, in.PostDate = records.MediaIDs{}, time.Time{}
		enc, err = j.p.cfg.Signer.BuildRelease(nw, in, j.now)
	}
	if err != nil {
		return nil, fmt.Errorf("publish: build release record: %w", err)
	}
	return enc, nil
}

func (j *job) buildAttestation(nw netid.NetworkID, checked time.Time, labels []string) ([]byte, error) {
	in := records.HealthAttestation{
		FileID:              j.res.FileID,
		CheckedAt:           checked,
		TotalSegments:       j.res.TotalSegments,
		SegmentsChecked:     j.res.SegmentsChecked,
		MissingCount:        j.res.MissingCount,
		MediaHeaderVerified: j.res.MediaHeaderVerified && j.res.Verdict == VerdictHealthy,
		Backbones:           labels,
	}
	in.ReleaseID, in.ReleaseScheme = j.res.ReleaseID, j.info.ReleaseScheme
	enc, err := j.p.cfg.Signer.BuildAttestation(nw, in, j.now)
	if err != nil {
		return nil, fmt.Errorf("publish: build attestation: %w", err)
	}
	return enc, nil
}

// send hands the record to the sink and, only on success, remembers it.
func (j *job) send(ctx context.Context, nw netid.NetworkID, kind records.Kind, file records.FileID, enc []byte, st State) error {
	if err := j.p.cfg.Sink.PublishLocal(ctx, nw, enc); err != nil {
		return fmt.Errorf("publish: sink: %w", err)
	}
	st.OriginCategory, st.OriginIndexer = normName(j.info.Category), normName(j.info.Indexer)
	if st.OriginIndexer == "unknown" {
		st.OriginIndexer = "" // an unknown indexer has no tag
	}
	if err := j.p.cfg.Store.MarkPublished(ctx, nw, kind, j.res.ReleaseID, file, st); err != nil {
		return fmt.Errorf("publish: record state: %w", err)
	}
	return nil
}

// RecognisedBackbones normalises provider labels and keeps those in the known
// list, sorted, unique and within the record cap.
func RecognisedBackbones(provider, known []string) []string {
	set := make(map[string]struct{}, len(known))
	for _, k := range known {
		if l, ok := records.NormaliseBackboneLabel(k); ok {
			set[l] = struct{}{}
		}
	}
	seen := map[string]struct{}{}
	var out []string
	for _, raw := range provider {
		l, ok := records.NormaliseBackboneLabel(raw)
		if !ok {
			continue
		}
		if _, in := set[l]; !in {
			continue
		}
		if _, dup := seen[l]; dup {
			continue
		}
		seen[l] = struct{}{}
		out = append(out, l)
	}
	sort.Strings(out)
	if len(out) > records.MaxBackbones {
		out = out[:records.MaxBackbones]
	}
	return out
}

// releaseTitle turns what the caller calls a title into the release title: the
// part after the last path separator, without a .nzb suffix, cleaned.
func releaseTitle(s string) string {
	return records.CleanTitle(nzbname.ReleaseName(s, false))
}

// minSecretLen is the shortest password compared against a title. Shorter
// values would match innocent titles.
const minSecretLen = 4

// titleLeaks reports whether the cleaned title still carries password
// markup or contains one of the import's passwords (case-insensitive). The
// secrets are used only for this comparison.
func titleLeaks(title string, secrets []string) bool {
	if strings.Contains(title, "{{") || strings.Contains(title, "}}") {
		return true
	}
	low := strings.ToLower(title)
	for _, s := range secrets {
		s = strings.ToLower(strings.TrimSpace(s))
		if len(s) >= minSecretLen && strings.Contains(low, s) {
			return true
		}
	}
	return false
}

// CoversOrigin reports whether an origin tag (as kept with the publish state:
// lower-cased category and indexer, either possibly empty) falls under an
// exclusion. An empty tag and the indexer "unknown" match nothing.
func (e Exclusions) CoversOrigin(category, indexer string) bool {
	category, indexer = normName(category), normName(indexer)
	if category != "" && listed(e.Categories, category) {
		return true
	}
	return indexer != "" && indexer != "unknown" && listed(e.Indexers, indexer)
}
