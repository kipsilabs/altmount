package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/contentid"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/publish"
	"github.com/kipsilabs/altmount/internal/peernet/records"
	"github.com/kipsilabs/altmount/internal/peernet/recsync"
)

// defaultPublishQueue bounds the notices waiting to be published.
const defaultPublishQueue = 512

// Skip reasons the pipeline adds to publish's own.
const (
	skipNotPublishing = "not_publishing"
	skipKnownHoles    = "known_holes"
	skipNoSegments    = "no_segments"
	skipNoReleaseID   = "no_release_id"
	skipNoReleaseInfo = "no_release_info"
	skipNotHealthy    = "not_healthy"
	// skipReducedByPeerAdvice: the check ran with a sample peer advice lowered.
	skipReducedByPeerAdvice = "reduced_by_peer_advice"
	// skipPatchStatusUnknown: whether a local PAR2 patch covers the file was
	// not determined (the check began before the observer was installed).
	skipPatchStatusUnknown = "patch_status_unknown"
)

// LocalHealth is the read-only view of local file health the re-announce
// guard needs. Statuses returns the health status of each given virtual path
// that has a health record; paths without a record are absent.
type LocalHealth interface {
	Statuses(ctx context.Context, paths []string) (map[string]string, error)
}

type healthLookup struct{ repo *database.HealthRepository }

// NewLocalHealth adapts the health repository to LocalHealth.
func NewLocalHealth(repo *database.HealthRepository) LocalHealth {
	if repo == nil {
		return nil
	}
	return healthLookup{repo: repo}
}

func (h healthLookup) Statuses(ctx context.Context, paths []string) (map[string]string, error) {
	rows, err := h.repo.GetFilesByPaths(ctx, paths)
	if err != nil {
		return nil, err
	}
	norm := make(map[string]string, len(paths))
	for _, p := range paths {
		norm[strings.Trim(strings.ReplaceAll(p, `\`, "/"), "/")] = p
	}
	out := make(map[string]string, len(rows))
	for _, r := range rows {
		if p, ok := norm[strings.Trim(strings.ReplaceAll(r.FilePath, `\`, "/"), "/")]; ok {
			out[p] = string(r.Status)
		}
	}
	return out, nil
}

// Re-announce back-off: a release whose re-announce did not go through is not
// retried for a growing time, and does not occupy a slot of the batch.
const (
	reannounceBackoffBase = 5 * time.Minute
	reannounceBackoffMax  = 6 * time.Hour
	reannounceBackoffCap  = 4096
	reannounceScanMax     = 500
)

type backoffEntry struct {
	failures int
	until    time.Time
}

// HealthyNotice is one completed local check that found a file healthy, as the
// health worker reports it. It carries counts and flags only.
type HealthyNotice struct {
	// FilePath is the file's virtual path. It is used to find the file's
	// identifiers and never leaves this process.
	FilePath  string
	CheckedAt time.Time

	TotalSegments       int
	SegmentsChecked     int
	MissingCount        int
	FullCheck           bool
	MediaHeaderVerified bool
	PatchedByPAR2       bool
	// PatchKnown says PatchedByPAR2 was really determined for this check. A
	// notice without it is never attested: "unknown" is not "unpatched".
	PatchKnown bool
	KnownHoles bool
	// ReducedByPeerAdvice marks a check whose sample peer advice lowered. It
	// is weaker than the configured check, so it is never attested (otherwise
	// nodes would cite each other's floor-level checks); the release record
	// may still be published.
	ReducedByPeerAdvice bool
}

// PublishStatus counts what the publishing pipeline did since start. It holds
// no record content, path or name.
type PublishStatus struct {
	// Published counts records handed to a network, by kind ("release",
	// "attestation").
	Published map[string]int64
	// Skipped counts checks (or records) not published, by reason.
	Skipped map[string]int64
	// Dropped counts notices lost because the queue was full.
	Dropped int64
	// Errors counts failed publish attempts.
	Errors int64
	// Reannounced counts release records announced again before expiry.
	Reannounced int64
}

type publishCounters struct {
	mu          sync.Mutex
	published   map[string]int64
	skipped     map[string]int64
	dropped     int64
	errors      int64
	reannounced int64
}

func newPublishCounters() *publishCounters {
	return &publishCounters{published: map[string]int64{}, skipped: map[string]int64{}}
}

func (c *publishCounters) add(m map[string]int64, key string) {
	c.mu.Lock()
	m[key]++
	c.mu.Unlock()
}

func (c *publishCounters) snapshot() PublishStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	st := PublishStatus{Published: map[string]int64{}, Skipped: map[string]int64{}, Dropped: c.dropped,
		Errors: c.errors, Reannounced: c.reannounced}
	for k, v := range c.published {
		st.Published[k] = v
	}
	for k, v := range c.skipped {
		st.Skipped[k] = v
	}
	return st
}

func kindName(k records.Kind) string {
	if k == records.KindAttestation {
		return "attestation"
	}
	return "release"
}

// BackboneSource supplies what attestations need to name backbones. The peer
// network gets only this narrow view: never the configuration or any provider
// setting other than the raw storage_group labels.
type BackboneSource interface {
	// ProviderGroups returns the raw storage_group of each enabled provider.
	ProviderGroups() []string
	// Known returns the known backbone names (read-only, no fetching); empty
	// when the list is not available, which means no attestation is published.
	Known() []string
	// Refresh renews the known list when it is stale. It is only called while
	// the node publishes to a network at level 2 or higher.
	Refresh(ctx context.Context) error
}

type backboneAdapter struct{ src BackboneSource }

func (a backboneAdapter) ProviderBackbones(context.Context) []string {
	if a.src == nil {
		return nil
	}
	return a.src.ProviderGroups()
}

func (a backboneAdapter) KnownBackbones(context.Context) []string {
	if a.src == nil {
		return nil
	}
	return a.src.Known()
}

// stateStore adapts the publish-state repository to publish.StateStore.
type stateStore struct {
	repo *database.PeernetPublishStateRepository
}

func kindToDB(k records.Kind) int {
	if k == records.KindAttestation {
		return database.PeernetRecordKindAttestation
	}
	return database.PeernetRecordKindRelease
}

func fileHex(f records.FileID) string {
	if f == (records.FileID{}) {
		return ""
	}
	return contentid.ID(f).String()
}

func (s stateStore) LastPublished(ctx context.Context, nw netid.NetworkID, kind records.Kind, rel records.ReleaseID, file records.FileID) (publish.State, bool, error) {
	row, ok, err := s.repo.Get(ctx, nw.String(), kindToDB(kind), contentid.ID(rel).String(), fileHex(file))
	if err != nil || !ok {
		return publish.State{}, false, err
	}
	return publish.State{PublishedAt: row.PublishedAt, CheckedAt: row.CheckedAt, Verdict: publish.Verdict(row.Verdict)}, true, nil
}

func (s stateStore) MarkPublished(ctx context.Context, nw netid.NetworkID, kind records.Kind, rel records.ReleaseID, file records.FileID, st publish.State) error {
	return s.repo.Put(ctx, database.PeernetPublishState{
		NetworkID: nw.String(), Kind: kindToDB(kind), ReleaseID: contentid.ID(rel).String(), FileID: fileHex(file),
		PublishedAt: st.PublishedAt, CheckedAt: st.CheckedAt, Verdict: int(st.Verdict),
		OriginCategory: st.OriginCategory, OriginIndexer: st.OriginIndexer,
	})
}

// exchangeSink hands encoded records to the record exchange. A rejected record
// is an error; a duplicate is not.
type exchangeSink struct{ rx *recsync.Exchange }

func (s exchangeSink) PublishLocal(ctx context.Context, nw netid.NetworkID, encoded []byte) error {
	out, err := s.rx.Publish(ctx, nw, encoded)
	if err != nil {
		return err
	}
	if out.Result == recsync.ResultRejected {
		return fmt.Errorf("record exchange rejected the record: %v", out.Reason)
	}
	return nil
}

// pipelineConfig wires a pipeline.
type pipelineConfig struct {
	Signer     RecordSigner
	Sink       publish.Sink
	Networks   func() []publish.Network
	Exclusions func() publish.Exclusions
	Backbones  BackboneSource

	State   *database.PeernetPublishStateRepository
	Content *database.PeernetContentRepository
	Origins *database.PeernetOriginRepository
	Meta    contentid.Source
	// Health is consulted before a re-announce; nil means no re-announce.
	Health LocalHealth

	QueueSize int
	// Hold, when set, runs before each notice is handled (tests).
	Hold func()
	// Log reports a failure once per state change.
	Log func(ctx context.Context, key string, err error)
	// Now is the clock of the pipeline's scheduling (back-off, re-announce).
	Now func() time.Time
	// PublisherNow is the publisher's clock; it defaults to Now. Records are
	// stamped with it and checked against real time, so it is never moved
	// apart from Now outside tests.
	PublisherNow func() time.Time
}

// pipeline turns healthy-check notices into published records. The health
// worker only enqueues (submit never blocks and never touches the database);
// one worker goroutine does every lookup, the signing and the publishing.
type pipeline struct {
	cfg     pipelineConfig
	pub     *publish.Publisher
	info    *infoSource
	stats   *publishCounters
	queue   chan HealthyNotice
	pending atomic.Int64 // queued plus in flight

	backMu  sync.Mutex
	backoff map[string]backoffEntry // network|release
}

func newPipeline(cfg pipelineConfig) (*pipeline, error) {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.PublisherNow == nil {
		cfg.PublisherNow = cfg.Now
	}
	if cfg.Log == nil {
		cfg.Log = func(context.Context, string, error) {}
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = defaultPublishQueue
	}
	pub, err := publish.New(publish.Config{
		Signer: cfg.Signer, Sink: cfg.Sink, Store: stateStore{repo: cfg.State},
		Backbones: backboneAdapter{src: cfg.Backbones}, Networks: cfg.Networks, Exclusions: cfg.Exclusions, Now: cfg.PublisherNow,
		// Unhealthy attestations stay off (PublishUnhealthy is false).
	})
	if err != nil {
		return nil, err
	}
	info := newInfoSource(cfg.Content, cfg.Origins, NewReleaseFileReader(cfg.Meta))
	info.now = cfg.Now
	return &pipeline{cfg: cfg, pub: pub, info: info, stats: newPublishCounters(),
		queue: make(chan HealthyNotice, cfg.QueueSize), backoff: map[string]backoffEntry{}}, nil
}

// submit queues a notice. It never blocks: with no network at level 2 or
// higher the notice is counted and dropped at once, and a full queue drops it
// and counts that.
func (p *pipeline) submit(n HealthyNotice) {
	if len(p.cfg.Networks()) == 0 {
		p.stats.add(p.stats.skipped, skipNotPublishing)
		return
	}
	p.pending.Add(1)
	select {
	case p.queue <- n:
	default:
		p.pending.Add(-1)
		p.stats.mu.Lock()
		p.stats.dropped++
		p.stats.mu.Unlock()
	}
}

// idle reports that nothing is queued or being handled.
func (p *pipeline) idle() bool { return p.pending.Load() == 0 }

// run is the worker. It returns when ctx is done; queued notices are dropped.
func (p *pipeline) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case n := <-p.queue:
			if p.cfg.Hold != nil {
				p.cfg.Hold()
			}
			p.handle(ctx, n)
			p.pending.Add(-1)
		}
	}
}

func (p *pipeline) skip(reason string) { p.stats.add(p.stats.skipped, reason) }

func (p *pipeline) fail(ctx context.Context, what string, err error) {
	p.stats.mu.Lock()
	p.stats.errors++
	p.stats.mu.Unlock()
	p.cfg.Log(ctx, what, err)
}

// handle publishes one notice.
func (p *pipeline) handle(ctx context.Context, n HealthyNotice) {
	switch {
	case n.KnownHoles:
		// A clean sample does not clear holes recorded earlier.
		p.skip(skipKnownHoles)
		return
	case n.TotalSegments <= 0 || n.SegmentsChecked <= 0 || n.MissingCount != 0 ||
		n.TotalSegments > maxUint32 || n.SegmentsChecked > n.TotalSegments:
		p.skip(skipNoSegments)
		return
	}
	entry, ok, err := p.cfg.Content.GetByPath(ctx, n.FilePath)
	if err != nil {
		p.fail(ctx, "publish: look up content index", err)
		return
	}
	if !ok || entry.ReleaseID == "" {
		p.skip(skipNoReleaseID)
		return
	}
	rel, relErr := contentid.ParseID(entry.ReleaseID)
	file, fileErr := contentid.ParseID(entry.FileID)
	if relErr != nil || fileErr != nil {
		p.skip(skipNoReleaseID)
		return
	}
	info, err := p.info.forRelease(ctx, entry.ReleaseID, entry.FilePath)
	if err != nil {
		if errors.Is(err, errNoReleaseInfo) {
			p.skip(skipNoReleaseInfo)
		} else {
			p.fail(ctx, "publish: release information", err)
		}
		return
	}
	res := publish.CheckResult{
		ReleaseID: records.ReleaseID(rel), FileID: records.FileID(file), CheckedAt: n.CheckedAt, Verdict: publish.VerdictHealthy,
		TotalSegments: uint32(n.TotalSegments), SegmentsChecked: uint32(n.SegmentsChecked),
		MediaHeaderVerified: n.MediaHeaderVerified, PatchedByPAR2: n.PatchedByPAR2,
	}
	// A reduced check, or one whose patch status was never determined, may
	// still announce the release but is no evidence: release record only.
	switch {
	case n.PatchedByPAR2:
	case n.ReducedByPeerAdvice:
		res.AttestationSkip = publish.SkipReason(skipReducedByPeerAdvice)
	case !n.PatchKnown:
		res.AttestationSkip = publish.SkipReason(skipPatchStatusUnknown)
	}
	outs, err := p.pub.Publish(ctx, res, info)
	p.count(ctx, outs, err)
}

// count records the outcomes of one Publish call.
func (p *pipeline) count(ctx context.Context, outs []publish.Outcome, err error) {
	failed := false
	for _, o := range outs {
		switch {
		case o.Err != nil:
			failed = true
			p.fail(ctx, "publish", o.Err)
		case o.Skip != publish.SkipNone:
			p.skip(string(o.Skip))
		default:
			p.stats.add(p.stats.published, kindName(o.Kind))
		}
	}
	if err != nil && !failed {
		p.fail(ctx, "publish", err)
	}
}

// reannounce announces again, on the networks this node still publishes to,
// up to limit release records whose refresh age was reached. A record is only
// refreshed while the release is still in the content index and every indexed
// file has a healthy local status (any doubt fails closed). A release that
// could not be refreshed backs off and does not take a slot of later batches.
// Attestations are never touched: they are refreshed only by a new local check.
func (p *pipeline) reannounce(ctx context.Context, limit int) {
	nets := p.cfg.Networks()
	if len(nets) == 0 || limit < 1 {
		return
	}
	now := p.cfg.Now()
	scan := min(limit+p.backoffCount(now), reannounceScanMax)
	rows, err := p.cfg.State.ListReleasesPublishedBefore(ctx, now.Add(-publish.DefaultRecordRefresh).Add(time.Second), scan)
	if err != nil {
		p.cfg.Log(ctx, "publish: list records to re-announce", err)
		return
	}
	p.cfg.Log(ctx, "publish: list records to re-announce", nil)
	tracked := make([]publish.Tracked, 0, len(rows))
	for _, r := range rows {
		nw, err := netid.ParseNetworkID(r.NetworkID)
		rel, relErr := contentid.ParseID(r.ReleaseID)
		if err != nil || relErr != nil || p.backedOff(nw, r.ReleaseID, now) {
			continue
		}
		tracked = append(tracked, publish.Tracked{Network: nw, Kind: records.KindRelease,
			ReleaseID: records.ReleaseID(rel), State: publish.State{PublishedAt: r.PublishedAt}})
	}
	done := 0
	for _, t := range (publish.PlanConfig{}).RecordsDueForReannounce(tracked, now) {
		if ctx.Err() != nil || done >= limit {
			return
		}
		if !publishing(nets, t.Network) {
			continue
		}
		done++
		relHex := contentid.ID(t.ReleaseID).String()
		if !p.reannounceOne(ctx, t, relHex) {
			p.noteFailure(t.Network, relHex, now)
		} else {
			p.clearFailure(t.Network, relHex)
		}
	}
}

// reannounceOne re-announces one release and reports whether it went through.
func (p *pipeline) reannounceOne(ctx context.Context, t publish.Tracked, relHex string) bool {
	ok, err := p.locallyHealthy(ctx, relHex)
	if err != nil {
		p.cfg.Log(ctx, "publish: re-announce health lookup", err)
		return false
	}
	p.cfg.Log(ctx, "publish: re-announce health lookup", nil)
	if !ok {
		p.skip(skipNotHealthy)
		return false
	}
	info, err := p.info.forRelease(ctx, relHex, "")
	if err != nil {
		if errors.Is(err, errNoReleaseInfo) {
			p.skip(skipNoReleaseInfo)
		} else {
			p.cfg.Log(ctx, "publish: re-announce release information", err)
		}
		return false
	}
	o := p.pub.ReannounceRelease(ctx, t.Network, t.ReleaseID, info)
	switch {
	case o.Err != nil:
		p.fail(ctx, "publish: re-announce", o.Err)
		return false
	case o.Skip != publish.SkipNone:
		p.skip(string(o.Skip))
		return false
	}
	p.stats.mu.Lock()
	p.stats.reannounced++
	p.stats.mu.Unlock()
	return true
}

// locallyHealthy reports whether the release is still indexed and each of its
// indexed files has a healthy status. No health source, no files, too many
// files or any missing record is "not healthy".
func (p *pipeline) locallyHealthy(ctx context.Context, relHex string) (bool, error) {
	if p.cfg.Health == nil {
		return false, nil
	}
	entries, err := p.cfg.Content.FindByReleaseID(ctx, relHex)
	if err != nil {
		return false, fmt.Errorf("list release files: %w", err)
	}
	if len(entries) == 0 || len(entries) > maxReleaseFiles {
		return false, nil
	}
	paths := make([]string, len(entries))
	for i, e := range entries {
		paths[i] = e.FilePath
	}
	st, err := p.cfg.Health.Statuses(ctx, paths)
	if err != nil {
		return false, fmt.Errorf("local health: %w", err)
	}
	for _, path := range paths {
		if st[path] != string(database.HealthStatusHealthy) {
			return false, nil
		}
	}
	return true, nil
}

func backoffKey(nw netid.NetworkID, rel string) string { return nw.String() + "|" + rel }

func (p *pipeline) backedOff(nw netid.NetworkID, rel string, now time.Time) bool {
	p.backMu.Lock()
	defer p.backMu.Unlock()
	e, ok := p.backoff[backoffKey(nw, rel)]
	return ok && now.Before(e.until)
}

// backoffCount is the number of releases currently backing off; expired
// entries are dropped here.
func (p *pipeline) backoffCount(now time.Time) int {
	p.backMu.Lock()
	defer p.backMu.Unlock()
	n := 0
	for k, e := range p.backoff {
		if now.Before(e.until) {
			n++
		} else if e.failures == 0 {
			delete(p.backoff, k)
		}
	}
	return n
}

func (p *pipeline) noteFailure(nw netid.NetworkID, rel string, now time.Time) {
	p.backMu.Lock()
	defer p.backMu.Unlock()
	if len(p.backoff) >= reannounceBackoffCap {
		clear(p.backoff)
	}
	k := backoffKey(nw, rel)
	e := p.backoff[k]
	e.failures++
	wait := reannounceBackoffMax
	if e.failures < 12 {
		wait = min(reannounceBackoffBase<<(e.failures-1), reannounceBackoffMax)
	}
	e.until = now.Add(wait)
	p.backoff[k] = e
}

func (p *pipeline) clearFailure(nw netid.NetworkID, rel string) {
	p.backMu.Lock()
	delete(p.backoff, backoffKey(nw, rel))
	p.backMu.Unlock()
}

func publishing(nets []publish.Network, id netid.NetworkID) bool {
	for _, n := range nets {
		if n.ID == id {
			return true
		}
	}
	return false
}

// NotifyHealthy reports a file a completed local check found healthy. It is
// the entry point the health worker's observer calls: it returns at once, does
// no database or network work and never fails. While the feature is off, or no
// network is at level 2 or higher, nothing is built.
func (s *Service) NotifyHealthy(n HealthyNotice) {
	node := s.currentNode()
	if node == nil || node.pipe == nil {
		return
	}
	node.pipe.submit(n)
}

// startPublishing builds the publishing pipeline when every dependency is
// present; otherwise publishing stays off.
func (n *node) startPublishing() error {
	d := n.deps
	if n.rx == nil || d.Content == nil || d.Origins == nil || d.PublishState == nil || d.Metadata == nil {
		return nil
	}
	p, err := newPipeline(pipelineConfig{
		Signer: nodeSigner{key: n.key}, Sink: exchangeSink{rx: n.rx},
		Networks: func() []publish.Network {
			ids := n.rx.PublishNetworks()
			out := make([]publish.Network, 0, len(ids))
			for _, id := range ids {
				out = append(out, publish.Network{ID: id, Level: publish.MinLevel})
			}
			return out
		},
		Exclusions: d.Exclusions, Backbones: d.Backbones,
		State: d.PublishState, Content: d.Content, Origins: d.Origins, Meta: d.Metadata, Health: d.Health,
		QueueSize: d.publishQueueSize, Now: d.publishNow, PublisherNow: time.Now, Hold: d.beforePublish, Log: n.logChange,
	})
	if err != nil {
		return err
	}
	n.pipe = p
	return nil
}

// maintainPublishing is the publishing part of an upkeep pass. On the orphan
// cadence (orphans set, every orphanCleanupEvery passes) it prunes one bounded
// window of state of releases that left the content index, retracts what an
// exclusion added since now covers, and deletes the state of networks that no
// longer exist; every pass it re-announces due release records (a bounded
// number). The table scans never run on every 30-second pass.
func (n *node) maintainPublishing(ctx context.Context, orphans bool) {
	if n.pipe == nil {
		return
	}
	st := n.deps.PublishState
	if orphans {
		n.retractExcluded(ctx)
		_, next, err := st.PruneUnindexed(ctx, n.pruneCursor, publishPruneWindow)
		n.logChange(ctx, "prune publish state", err)
		if err == nil {
			n.pruneCursor = next
		}
		// The stored IDs are read before the live ones (the order the record
		// exchange uses): a network created in between is never an orphan.
		_, err = st.DeleteOrphanNetworks(ctx, n.knownNetworkIDs)
		n.logChange(ctx, "delete orphan publish state", err)
	}
	n.pipe.reannounce(ctx, reannounceBatch)
}

// retractExcluded deletes this node's own records and publish state of
// releases that an exclusion added after they were published now covers. The
// release's category and indexer are not in a record, so they are matched
// against the private origin tag kept with the publish state; releases
// published before the tag existed cannot be matched. A bounded number of
// releases is handled per pass. Copies other nodes already hold cannot be
// recalled.
func (n *node) retractExcluded(ctx context.Context) {
	if n.deps.Exclusions == nil || n.deps.Records == nil {
		return
	}
	ex := n.deps.Exclusions()
	hits, err := n.deps.PublishState.ListExcluded(ctx, ex.Categories, ex.Indexers, retractBatch)
	if n.logChange(ctx, "list excluded publish state", err); err != nil {
		return
	}
	for _, h := range hits {
		if _, err := n.deps.Records.DeleteOwnRelease(ctx, h.NetworkID, h.ReleaseID); err != nil {
			n.logChange(ctx, "retract excluded records", err)
			return
		}
		if err := n.deps.PublishState.DeleteRelease(ctx, h.NetworkID, h.ReleaseID); err != nil {
			n.logChange(ctx, "retract excluded publish state", err)
			return
		}
	}
	n.logChange(ctx, "retract excluded records", nil)
}

const (
	publishPruneWindow = 2000
	reannounceBatch    = 20
	retractBatch       = 50
)
