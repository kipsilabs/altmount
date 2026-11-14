package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/bundle"
	"github.com/kipsilabs/altmount/internal/peernet/bundlex"
	"github.com/kipsilabs/altmount/internal/peernet/contentid"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/publish"
	"github.com/kipsilabs/altmount/internal/peernet/records"
	"github.com/kipsilabs/altmount/internal/peernet/recsync"
)

// FetchSource says which network and publisher a fetched bundle came from, so
// the import can be marked as coming from the peer network.
type FetchSource = bundlex.FetchSource

// Offer is a stored release record that announces a bundle for a release.
type Offer = bundlex.Offer

// Errors of FetchBundle.
var (
	ErrBundlesUnavailable = errors.New("peer network bundles are not configured")
	ErrNoBundleOffers     = bundlex.ErrNoOffers
	ErrFetchRateLimited   = bundlex.ErrRateLimited
	ErrFetchBusy          = bundlex.ErrFetchBusy
	ErrBundleFetchFailed  = bundlex.ErrAllFailed
)

// knownPeerAge is how long a peer must have been verified to count as known.
const knownPeerAge = 24 * time.Hour

// buildSourceProvider is implemented by the metadata host the production
// wiring passes. Its method is unexported on purpose: only this package can
// obtain the full-metadata reader, and only to hand it to the bundle builder.
type buildSourceProvider interface {
	bundleBuildSource() bundlex.BuildSource
}

// BundleStatus counts bundle activity since start. It holds no bundle content,
// path or name.
type BundleStatus struct {
	// Built counts completed builds; Cached and CachedBytes the bundles held in
	// memory now; CacheHits the builds avoided.
	Built, CacheHits, BuildFailures int64
	Cached, CachedBytes             int
	// Announced is how many release records announce a bundle.
	Announced int
	// NotShareable counts releases that cannot be shared as a bundle, by reason.
	NotShareable map[string]int64
	// Served and ServedBytes count bundles sent; ServeRefused counts refused
	// requests by reason.
	Served, ServedBytes int64
	ServeRefused        map[string]int64
	// Fetched counts verified downloads; FetchFailed failures by reason;
	// FetchRateLimited calls refused by the fetch limits; FetchBackedOff the
	// publishers currently skipped.
	Fetched, FetchRateLimited int64
	FetchFailed               map[string]int64
	FetchBackedOff            int
	// FullCheckReports counts verified bundles whose release then failed the
	// local full check; FullCheckBackedOff the publishers skipped for fetches
	// because of that now.
	FullCheckReports   int64
	FullCheckBackedOff int
	// Import counts what the receiving side did with fetched bundles (the
	// counters of the bundle importer); zero while it is not wired.
	Import BundleImportStatus
}

// BundleImportStatus mirrors the counters of the bundle importer: bundles
// accepted as local imports, rejected (by reason), and how their full checks
// ended. It holds no bundle content, path or name.
type BundleImportStatus struct {
	Accepted, AlreadyImported        int64
	Rejected                         map[string]int64
	FullCheckPassed, FullCheckFailed int64
	Announced, BadBundleReports      int64
	// Pending files still owe their full check; Stale are pending past the
	// maximum age (they are failed, never announced).
	Pending, Stale int64
}

type contentLibrary struct {
	repo *database.PeernetContentRepository
}

// ReleaseFiles implements bundlex.Library: one path per distinct File ID.
func (l contentLibrary) ReleaseFiles(ctx context.Context, releaseHex string) (int, []string, error) {
	entries, err := l.repo.FindByReleaseID(ctx, releaseHex)
	if err != nil || len(entries) == 0 {
		return 0, nil, err
	}
	seen := make(map[string]struct{}, len(entries))
	paths := make([]string, 0, len(entries))
	for _, e := range entries {
		if _, dup := seen[e.FileID]; dup {
			continue
		}
		seen[e.FileID] = struct{}{}
		paths = append(paths, e.FilePath)
	}
	return entries[0].ReleaseScheme, paths, nil
}

// bundleSource adapts the builder to publish.BundleSource.
type bundleSource struct{ b *bundlex.Builder }

func (s bundleSource) BundleFor(ctx context.Context, rel records.ReleaseID, info publish.ReleaseInfo) publish.BundleDecision {
	built, err := s.b.Build(ctx, contentid.ID(rel).String(), info.Title, info.PostDate)
	var ns *bundlex.NotShareableError
	switch {
	case errors.As(err, &ns):
		return publish.BundleDecision{Skip: ns.Reason}
	case err != nil:
		return publish.BundleDecision{Err: err}
	}
	return publish.BundleDecision{ID: records.BundleID(built.ID)}
}

// announcements adapts the publish state and record stores to
// bundlex.Announcements.
type announcements struct {
	state *database.PeernetPublishStateRepository
	recs  *database.PeernetRecordRepository
}

func (a announcements) Announced(ctx context.Context, nw netid.NetworkID, id bundle.BundleID) (bundlex.Announcement, bool, error) {
	st, ok, err := a.state.GetReleaseByBundle(ctx, nw.String(), id.String())
	if err != nil || !ok {
		return bundlex.Announcement{}, false, err
	}
	return bundlex.Announcement{ReleaseID: st.ReleaseID, OriginCategory: st.OriginCategory, OriginIndexer: st.OriginIndexer}, true, nil
}

func (a announcements) Withdraw(ctx context.Context, nw netid.NetworkID, rel string) error {
	if _, err := a.recs.DeleteOwnRelease(ctx, nw.String(), rel); err != nil {
		return err
	}
	return a.state.DeleteRelease(ctx, nw.String(), rel)
}

func (a announcements) MarkStale(ctx context.Context, nw netid.NetworkID, rel string) error {
	return a.state.MarkBundleStale(ctx, nw.String(), rel)
}

// bundleProvider builds the bundle of a release from the same release
// information that went into its record.
type bundleProvider struct {
	b    *bundlex.Builder
	info *infoSource
}

func (p bundleProvider) Bundle(ctx context.Context, releaseHex string) (*bundlex.Built, error) {
	info, err := p.info.forRelease(ctx, releaseHex, "")
	if err != nil {
		return nil, fmt.Errorf("release information: %w", err)
	}
	return p.b.BuildForServe(ctx, releaseHex, info.Title, info.PostDate)
}

// peerBook adapts the peer repository to bundlex.PeerBook.
type peerBook struct {
	store       *database.PeernetPeerRepository
	maxFailures int
}

func (p peerBook) VerifiedPeer(ctx context.Context, nw netid.NetworkID, node identity.NodeID) (recsync.Peer, bool, error) {
	row, err := p.store.Get(ctx, nw.String(), node.String())
	if err != nil || row == nil || !row.Verified || row.Address == "" || row.Failures >= p.maxFailures {
		return recsync.Peer{}, false, err
	}
	trusted := row.Source == database.PeernetSourceUser || row.Source == database.PeernetSourceLocal
	return recsync.Peer{NodeID: node, Address: row.Address, Trusted: trusted}, true, nil
}

func (p peerBook) Known(ctx context.Context, nw netid.NetworkID, nodes []string) (map[string]bool, error) {
	return p.store.KnownAmong(ctx, nw.String(), nodes, knownPeerAge, time.Now())
}

// startBuilder creates the bundle builder and the fetch client. Without a
// full-metadata source the builder is absent: level 3 then announces no
// bundles.
func (n *node) startBuilder() {
	d := n.deps
	if n.rx == nil || d.Content == nil {
		return
	}
	n.fetcher = bundlex.NewFetcher(bundlex.FetchConfig{
		Self: n.nodeID, Store: d.Records, Peers: peerBook{store: d.Peers, maxFailures: recsync.DefaultLimits().MaxPeerFailures},
		Networks: n.rx, Dialer: n.client, Auth: authBridge{mem: n.mem, onRefused: n.noteRefused},
	})
	src := d.BundleSource
	if src == nil {
		if p, ok := d.Metadata.(buildSourceProvider); ok {
			src = p.bundleBuildSource()
		}
	}
	if src == nil {
		return
	}
	n.builder = bundlex.NewBuilder(bundlex.BuilderConfig{Source: src, Library: contentLibrary{repo: d.Content}, Patches: d.Patches})
}

// startBundleServe creates the serve route handler. It needs the publishing
// pipeline (release information) and the publish state (what was announced).
func (n *node) startBundleServe() {
	d := n.deps
	if n.builder == nil || n.pipe == nil || n.rx == nil || d.PublishState == nil || d.Records == nil {
		return
	}
	var prov bundlex.Provider = bundleProvider{b: n.builder, info: n.pipe.info}
	if d.wrapProvider != nil {
		prov = d.wrapProvider(prov)
	}
	n.bsrv = bundlex.NewServer(bundlex.ServerConfig{
		Networks: n.rx, Auth: authBridge{mem: n.mem, onRefused: n.noteRefused},
		Announcements: announcements{state: d.PublishState, recs: d.Records},
		Provider:      prov,
		Exclusions:    d.Exclusions, BackedOff: n.rx.PeerBackedOff, Limits: d.bundleServeLimits,
		Log: n.logChange,
	})
}

// invalidateBundles drops cached bundles of releases whose identity changed.
func (s *Service) invalidateBundles(releaseIDs []string) {
	n := s.currentNode()
	if n == nil || n.builder == nil {
		return
	}
	for _, id := range releaseIDs {
		n.builder.Invalidate(id)
	}
}

// BundleOffers lists the stored release records that announce a bundle for
// releaseID, best first (known publishers, then those with a fresh healthy
// attestation, then newest). Offer.Fetchable says whether the publisher is a
// verified peer this node can dial; a record relayed from a publisher with no
// verified address is listed but never fetched.
func (s *Service) BundleOffers(ctx context.Context, releaseID string) ([]Offer, error) {
	n, err := s.requireNode()
	if err != nil {
		return nil, err
	}
	if n.fetcher == nil {
		return nil, ErrBundlesUnavailable
	}
	return n.fetcher.Offers(ctx, releaseID)
}

// FetchBundle downloads the bundle of releaseID on demand from the best
// offers (at most three attempts), verifies it against the Bundle ID and
// Release ID of the signed record, and returns it with its source. It is rate
// limited and concurrency limited, and is never called speculatively by the
// service itself.
func (s *Service) FetchBundle(ctx context.Context, releaseID string) (*bundle.Bundle, FetchSource, error) {
	n, err := s.requireNode()
	if err != nil {
		return nil, FetchSource{}, err
	}
	if n.fetcher == nil {
		return nil, FetchSource{}, ErrBundlesUnavailable
	}
	return n.fetcher.Fetch(ctx, releaseID)
}

// NoteBundleFullCheck tells the fetcher how the full local check of a release
// imported from publisherID's bundle in networkID ended. A failure backs that
// publisher off for bundle fetches only (never record sync, never a purge); a
// pass clears an earlier penalty. Unparseable identifiers are ignored.
func (s *Service) NoteBundleFullCheck(networkID, publisherID string, passed bool) {
	n := s.currentNode()
	if n == nil || n.fetcher == nil {
		return
	}
	nw, err := netid.ParseNetworkID(networkID)
	if err != nil {
		return
	}
	pub, err := identity.ParseNodeID(publisherID)
	if err != nil {
		return
	}
	if passed {
		n.fetcher.ReportVerifiedBundle(nw, pub)
		return
	}
	n.fetcher.ReportBadBundle(nw, pub)
}

// bundleStatus gathers the bundle counters.
func (n *node) bundleStatus(ctx context.Context) BundleStatus {
	var st BundleStatus
	if n.builder != nil {
		b := n.builder.Status()
		st.Built, st.CacheHits, st.BuildFailures = b.Built, b.CacheHits, b.BuildFailures
		st.Cached, st.CachedBytes, st.NotShareable = b.Cached, b.CachedBytes, b.NotShareable
	}
	if n.bsrv != nil {
		s := n.bsrv.Status()
		st.Served, st.ServedBytes, st.ServeRefused = s.Served, s.Bytes, s.Refused
	}
	if n.fetcher != nil {
		f := n.fetcher.Status()
		st.Fetched, st.FetchFailed, st.FetchRateLimited, st.FetchBackedOff = f.OK, f.Failed, f.RateLimited, f.BackedOff
		st.FullCheckReports, st.FullCheckBackedOff = f.FullCheckReports, f.FullCheckBackedOff
	}
	if n.deps.BundleImportStatus != nil {
		st.Import = n.deps.BundleImportStatus()
	}
	if n.deps.PublishState != nil {
		if c, err := n.deps.PublishState.CountAnnouncedBundles(ctx); err == nil {
			st.Announced = c
		}
	}
	return st
}
