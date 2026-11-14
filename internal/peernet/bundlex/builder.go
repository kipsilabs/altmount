package bundlex

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/holes"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/nzbname"
	"github.com/kipsilabs/altmount/internal/peernet/bundle"
	"github.com/kipsilabs/altmount/internal/peernet/contentid"
	"github.com/kipsilabs/altmount/internal/peernet/records"
)

// Reasons a release cannot be shared as a bundle (beyond the bundle package's
// own: rclone_encrypted, unsupported_encryption, password_not_from_nzb,
// healed_by_local_patch).
const (
	ReasonKnownHoles      = "known_holes"
	ReasonPatchUnknown    = "patch_status_unknown"
	ReasonNoNZBList       = "no_nzb_segment_list"
	ReasonReleaseMismatch = "release_id_mismatch"
	ReasonInvalidMetadata = "invalid_metadata"
	ReasonTooLarge        = "too_large"
	ReasonNoPostDate      = "no_post_date"
	ReasonNoFiles         = "no_files"
	ReasonSelfCheckFailed = "self_check_failed"
	maxBuiltCacheBytes    = 32 << 20
	builtTTL              = 10 * time.Minute
	defaultBuildWait      = 30 * time.Second
	maxReasonCounters     = 64
	// serveBuildsPerHour and serveBuildBurst bound cache-missing builds for
	// peer requests, node-wide.
	serveBuildsPerHour = 360
	serveBuildBurst    = 3
	// serveBuildsPerRequesterPerHour is what one requester (a node ID, or a
	// public address) may use of that: a sixth. Its burst is one, so no
	// requester can spend more than a third of the node-wide burst.
	serveBuildsPerRequesterPerHour = serveBuildsPerHour / 6
	serveBuildRequesterBurst       = 1
	negativeTTL                    = time.Minute
	maxNegative                    = 256
)

// NotShareableError says a release cannot be shared as a bundle, for a
// permanent reason (the same input never succeeds).
type NotShareableError struct{ Reason string }

func (e *NotShareableError) Error() string { return "bundlex: release not shareable: " + e.Reason }

// ErrBusy is returned when the build slot did not become free in time.
var ErrBusy = errors.New("bundlex: another bundle is being built")

// Built is a packed bundle. It is immutable; callers must not modify Wire.
type Built struct {
	ReleaseID string
	ID        bundle.BundleID
	Scheme    bundle.Scheme
	Wire      []byte
}

// BuilderConfig wires a Builder.
type BuilderConfig struct {
	Source  BuildSource
	Library Library
	Patches PatchIndex // nil: patch status unknown, nothing is shareable
	Now     func() time.Time
	// BuildWait bounds the wait for the build slot (default 30 s).
	BuildWait time.Duration
}

// Builder turns a finished local import into a packed bundle. Builds are
// deterministic for the same metadata, one at a time, and the most recent
// results are kept in a small memory cache (nothing is written to disk).
type Builder struct {
	cfg  BuilderConfig
	slot chan struct{}
	// serveSlot and rate keep builds for peer requests apart from this node's
	// own publishing builds, and bound them node-wide whoever asks.
	serveSlot chan struct{}
	rate      *buckets
	byNode    *buckets // per requesting node ID
	byIP      *buckets // per public requesting address
	neg       map[string]negEntry

	mu       sync.Mutex
	cache    map[string]cacheEntry
	byBundle map[bundle.BundleID]string
	bytes    int
	notShare map[string]int64
	built    int64
	hits     int64
	failures int64
}

type negEntry struct {
	err   error
	until time.Time
}

type cacheEntry struct {
	b  *Built
	at time.Time
}

// NewBuilder returns a Builder.
func NewBuilder(cfg BuilderConfig) *Builder {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.BuildWait <= 0 {
		cfg.BuildWait = defaultBuildWait
	}
	return &Builder{cfg: cfg, slot: make(chan struct{}, 1), serveSlot: make(chan struct{}, 1),
		rate:   newBuckets(serveBuildsPerHour, serveBuildBurst, 1),
		byNode: newBuckets(serveBuildsPerRequesterPerHour, serveBuildRequesterBurst, maxRequesters),
		byIP:   newBuckets(serveBuildsPerRequesterPerHour, serveBuildRequesterBurst, maxRequesters), neg: map[string]negEntry{}, cache: map[string]cacheEntry{},
		byBundle: map[bundle.BundleID]string{}, notShare: map[string]int64{}}
}

// BuilderStatus counts what the builder did since start.
type BuilderStatus struct {
	Built, CacheHits, BuildFailures int64
	Cached, CachedBytes             int
	NotShareable                    map[string]int64
}

// Status returns the counters.
func (b *Builder) Status() BuilderStatus {
	b.mu.Lock()
	defer b.mu.Unlock()
	ns := make(map[string]int64, len(b.notShare))
	for k, v := range b.notShare {
		ns[k] = v
	}
	return BuilderStatus{Built: b.built, CacheHits: b.hits, BuildFailures: b.failures,
		Cached: len(b.cache), CachedBytes: b.bytes, NotShareable: ns}
}

// Invalidate forgets the cached bundle of a release.
func (b *Builder) Invalidate(releaseHex string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.drop(releaseHex)
}

func (b *Builder) drop(rel string) {
	if e, ok := b.cache[rel]; ok {
		b.bytes -= len(e.b.Wire)
		delete(b.byBundle, e.b.ID)
		delete(b.cache, rel)
	}
}

func (b *Builder) cached(rel string) (*Built, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	e, ok := b.cache[rel]
	if !ok {
		return nil, false
	}
	if b.cfg.Now().Sub(e.at) > builtTTL {
		b.drop(rel)
		return nil, false
	}
	b.hits++
	return e.b, true
}

func (b *Builder) store(rel string, built *Built) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.drop(rel)
	if len(built.Wire) > maxBuiltCacheBytes {
		return // one wire larger than the whole cap is never cached
	}
	for b.bytes+len(built.Wire) > maxBuiltCacheBytes && len(b.cache) > 0 {
		oldest, at := "", time.Time{}
		for k, e := range b.cache {
			if oldest == "" || e.at.Before(at) {
				oldest, at = k, e.at
			}
		}
		b.drop(oldest)
	}
	b.cache[rel] = cacheEntry{b: built, at: b.cfg.Now()}
	b.byBundle[built.ID] = rel
	b.bytes += len(built.Wire)
}

func (b *Builder) count(reason string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.notShare[reason]; !ok && len(b.notShare) >= maxReasonCounters {
		reason = "other"
	}
	b.notShare[reason]++
}

func (b *Builder) acquire(ctx context.Context) error {
	t := time.NewTimer(b.cfg.BuildWait)
	defer t.Stop()
	select {
	case b.slot <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return ErrBusy
	}
}

// Build returns the packed bundle of the release releaseHex. title is the
// release title and posted its post date, as the release record carries them.
// A *NotShareableError is permanent (and counted); any other error is
// transient.
func (b *Builder) Build(ctx context.Context, releaseHex, title string, posted time.Time) (*Built, error) {
	if c, ok := b.cached(releaseHex); ok {
		return c, nil
	}
	if err := b.acquire(ctx); err != nil {
		return nil, err
	}
	defer func() { <-b.slot }()
	if c, ok := b.cached(releaseHex); ok { // built while this call waited
		return c, nil
	}
	return b.buildAndStore(ctx, releaseHex, title, posted)
}

func (b *Builder) buildAndStore(ctx context.Context, releaseHex, title string, posted time.Time) (*Built, error) {
	built, err := b.build(ctx, releaseHex, title, posted)
	var ns *NotShareableError
	switch {
	case errors.As(err, &ns):
		b.count(ns.Reason)
	case err != nil:
		b.mu.Lock()
		b.failures++
		b.mu.Unlock()
	default:
		b.mu.Lock()
		b.built++
		b.mu.Unlock()
		b.store(releaseHex, built)
	}
	return built, err
}

// Requester identifies who asked for a build, so the node-wide build budget can
// be shared between requesters.
type Requester struct {
	// Node is the authenticated node ID, IP the remote address (may be empty).
	Node, IP string
}

type requesterKey struct{}

// WithRequester attaches the requester to ctx for BuildForServe.
func WithRequester(ctx context.Context, node, ip string) context.Context {
	return context.WithValue(ctx, requesterKey{}, Requester{Node: node, IP: ip})
}

// maxRequesters bounds the per-requester accounting tables.
const maxRequesters = 4096

// spendServeBudget takes one build from the requester's node and address
// shares and from the node-wide budget, all or nothing. It is called only once
// the build slot is held and the build is about to start: a request that found
// the slot taken, timed out, was cancelled or hit the cache costs nothing.
func (b *Builder) spendServeBudget(ctx context.Context) bool {
	now := b.cfg.Now()
	req, _ := ctx.Value(requesterKey{}).(Requester)
	type taken struct {
		bk  *buckets
		key string
	}
	var got []taken
	take := func(bk *buckets, key string) bool {
		if !bk.take(key, 1, now) {
			return false
		}
		got = append(got, taken{bk, key})
		return true
	}
	ok := true
	if req.Node != "" {
		ok = take(b.byNode, req.Node)
	}
	if ok && req.IP != "" && !sharedIP(req.IP) { // a LAN address is shared by honest nodes
		ok = take(b.byIP, req.IP)
	}
	ok = ok && take(b.rate, "all")
	if !ok {
		for _, g := range got {
			g.bk.give(g.key, 1)
		}
	}
	return ok
}

// BuildForServe is Build for a request from a peer: it has its own build slot
// (so a stream of peer requests can never starve this node's own publishing
// builds), a node-wide build rate that no single requester (node ID or public
// address) can use more than a sixth of, and remembers an unbuildable release
// for a short while. The budget is spent only when a build actually starts.
func (b *Builder) BuildForServe(ctx context.Context, releaseHex, title string, posted time.Time) (*Built, error) {
	if c, ok := b.cached(releaseHex); ok {
		return c, nil
	}
	if err := b.negative(releaseHex); err != nil {
		return nil, err
	}
	t := time.NewTimer(b.cfg.BuildWait)
	defer t.Stop()
	select {
	case b.serveSlot <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.C:
		return nil, ErrBusy
	}
	defer func() { <-b.serveSlot }()
	if c, ok := b.cached(releaseHex); ok {
		return c, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !b.spendServeBudget(ctx) {
		return nil, ErrBusy
	}
	built, err := b.buildAndStore(ctx, releaseHex, title, posted)
	var ns *NotShareableError
	if errors.As(err, &ns) {
		b.rememberNegative(releaseHex, err)
	}
	return built, err
}

// negative returns the remembered error of a release that was recently found
// not shareable.
func (b *Builder) negative(rel string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	n, ok := b.neg[rel]
	if !ok {
		return nil
	}
	if b.cfg.Now().After(n.until) {
		delete(b.neg, rel)
		return nil
	}
	return n.err
}

func (b *Builder) rememberNegative(rel string, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.neg) >= maxNegative {
		oldest, at := "", time.Time{}
		for k, n := range b.neg {
			if oldest == "" || n.until.Before(at) {
				oldest, at = k, n.until
			}
		}
		delete(b.neg, oldest)
	}
	b.neg[rel] = negEntry{err: err, until: b.cfg.Now().Add(negativeTTL)}
}

func notShareable(reason string) error { return &NotShareableError{Reason: reason} }

func (b *Builder) build(ctx context.Context, releaseHex, title string, posted time.Time) (*Built, error) {
	rid, err := contentid.ParseID(releaseHex)
	if err != nil {
		return nil, fmt.Errorf("bundlex: release id: %w", err)
	}
	if b.cfg.Patches == nil {
		return nil, notShareable(ReasonPatchUnknown)
	}
	storedScheme, paths, err := b.cfg.Library.ReleaseFiles(ctx, releaseHex)
	if err != nil {
		return nil, fmt.Errorf("bundlex: list release files: %w", err)
	}
	switch {
	case len(paths) == 0:
		return nil, notShareable(ReasonNoFiles)
	case len(paths) > bundle.MaxFiles:
		return nil, notShareable(ReasonTooLarge)
	case posted.Unix() < 1:
		return nil, notShareable(ReasonNoPostDate)
	}
	scheme := bundle.SchemeFiles
	if storedScheme != database.PeernetSchemeFiles {
		scheme = bundle.SchemeNZB
	}
	in := bundle.ReleaseInput{Scheme: scheme, Title: records.CleanTitle(nzbname.ReleaseName(title, false)), PostedAt: posted}
	for _, p := range paths {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		m, err := b.cfg.Source.ReadFile(p)
		if err != nil {
			return nil, fmt.Errorf("bundlex: read file metadata: %w", err)
		}
		if m == nil {
			return nil, fmt.Errorf("bundlex: file metadata vanished")
		}
		if len(m.KnownHoles) > 0 {
			return nil, notShareable(ReasonKnownHoles)
		}
		fi := bundle.FileInput{Meta: m, PatchedLocally: b.anyPatched(m)}
		pw, fromNZB := b.cfg.Source.NZBPassword(p, m)
		fi.PasswordFromNZB = fromNZB
		if fromNZB && in.NZBPassword == "" {
			in.NZBPassword = pw
		}
		in.Files = append(in.Files, fi)
	}
	if scheme == bundle.SchemeNZB {
		lists, err := b.cfg.Source.NZBSegmentIDs(paths[0])
		if err != nil {
			return nil, fmt.Errorf("bundlex: read nzb segment list: %w", err)
		}
		if len(lists) == 0 {
			return nil, notShareable(ReasonNoNZBList)
		}
		in.NZBFiles = lists
	}
	bd, err := bundle.FromLocal(in)
	if err != nil {
		var nse *bundle.NotShareableError
		switch {
		case errors.As(err, &nse):
			return nil, notShareable(string(nse.Reason))
		case errors.Is(err, bundle.ErrLimit), errors.Is(err, bundle.ErrTooLarge):
			return nil, notShareable(ReasonTooLarge)
		}
		return nil, notShareable(ReasonInvalidMetadata)
	}
	if bd.ReleaseID != rid {
		return nil, notShareable(ReasonReleaseMismatch)
	}
	wire, id, err := bundle.Pack(bd)
	if err != nil {
		if errors.Is(err, bundle.ErrTooLarge) {
			return nil, notShareable(ReasonTooLarge)
		}
		return nil, notShareable(ReasonInvalidMetadata)
	}
	if _, err := bundle.Verify(wire, id, rid); err != nil {
		return nil, notShareable(ReasonSelfCheckFailed)
	}
	return &Built{ReleaseID: releaseHex, ID: id, Scheme: scheme, Wire: wire}, nil
}

// anyPatched reports whether a local PAR2 patch covers any article of the
// file's main or outer segments.
func (b *Builder) anyPatched(m *metapb.FileMetadata) bool {
	has := func(segs []*metapb.SegmentData) bool {
		for _, s := range segs {
			if s != nil && !holes.IsPlaceholderID(s.Id) && b.cfg.Patches.Has(trimIDBrackets(s.Id)) {
				return true
			}
		}
		return false
	}
	if has(m.SegmentData) {
		return true
	}
	for _, ns := range m.NestedSources {
		if ns != nil && has(ns.Segments) {
			return true
		}
	}
	for _, ns := range m.SharedOuterSources {
		if ns != nil && has(ns.Segments) {
			return true
		}
	}
	return false
}

func trimIDBrackets(id string) string {
	if len(id) > 1 && id[0] == '<' && id[len(id)-1] == '>' {
		return id[1 : len(id)-1]
	}
	return id
}

// LookupCached returns the cached bundle with this ID, if any.
func (b *Builder) LookupCached(id bundle.BundleID) (*Built, bool) {
	b.mu.Lock()
	rel, ok := b.byBundle[id]
	b.mu.Unlock()
	if !ok {
		return nil, false
	}
	return b.cached(rel)
}

// HexID renders a bundle ID.
func HexID(id bundle.BundleID) string { return hex.EncodeToString(id[:]) }
