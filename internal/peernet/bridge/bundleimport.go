package bridge

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/kipsilabs/altmount/internal/importer"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/peernet/bundle"
	"github.com/kipsilabs/altmount/internal/peernet/bundleimport"
	"github.com/kipsilabs/altmount/internal/peernet/contentid"
	"github.com/kipsilabs/altmount/internal/peernet/service"
)

// DefaultFastPathFetchTimeout is how long the importer waits for a bundle
// before it imports normally. It is the most the fast path can ever add to an
// import.
const DefaultFastPathFetchTimeout = 5 * time.Second

// DefaultFastPathAcceptTimeout is how long verifying a fetched layout may take
// before the fast path gives up and the NZB is imported normally.
const DefaultFastPathAcceptTimeout = 15 * time.Second

// minOpenCorroborators is how many distinct network ranges must announce the
// same Bundle ID before an open-network layout may replace archive analysis.
const minOpenCorroborators = 2

// BundleAcceptor is the part of *bundleimport.Importer the importer fast path
// needs.
type BundleAcceptor interface {
	Accept(ctx context.Context, req bundleimport.Request) (*bundleimport.Result, error)
	QueueHasPending(ctx context.Context, queueID int64) bool
	AttachQueue(ctx context.Context, paths []string, queueID int64) error
}

// FastPathConfig configures NewPeerBundleFastPath.
type FastPathConfig struct {
	Acceptor BundleAcceptor
	Fetcher  bundleimport.Fetcher
	// Enabled says the peer network is on with at least one network at level 1
	// or above. It is read on every import; false means the fast path does
	// nothing at all.
	Enabled func() bool
	// OpenFastPath says the user allows layouts offered on the open network to
	// replace archive analysis (peernet.open_network.fast_path). Offers from a
	// private network need no switch. Nil means not allowed.
	OpenFastPath func() bool
	// AnyOffers says whether this node holds any bundle offer at all. It must be
	// cheap and cached (it is asked on every import); false makes the fast path
	// do nothing. Nil means "unknown": offers are assumed.
	AnyOffers func(ctx context.Context) bool
	// AcceptTimeout bounds the accept pipeline of the fast path (release probe,
	// sampled header reads, media structure check). A layout that cannot be
	// verified in time is declined and the normal import runs (zero:
	// DefaultFastPathAcceptTimeout).
	AcceptTimeout time.Duration
	// FetchTimeout bounds the fetch (zero: DefaultFastPathFetchTimeout).
	FetchTimeout time.Duration
}

type fastPath struct{ cfg FastPathConfig }

// NewPeerBundleFastPath returns the importer's fast path hook. A nil acceptor
// or fetcher yields nil, so the importer then has no hook at all.
func NewPeerBundleFastPath(cfg FastPathConfig) importer.PeerBundleFastPath {
	if cfg.Acceptor == nil || cfg.Fetcher == nil || cfg.Enabled == nil {
		return nil
	}
	if cfg.FetchTimeout <= 0 {
		cfg.FetchTimeout = DefaultFastPathFetchTimeout
	}
	if cfg.AcceptTimeout <= 0 {
		cfg.AcceptTimeout = DefaultFastPathAcceptTimeout
	}
	return &fastPath{cfg: cfg}
}

// encodedSizes returns the NZB's yEnc-encoded size of every segment, in the
// shape canonicalIDs returns.
func encodedSizes(store *metapb.NzbStore) [][]int64 {
	out := make([][]int64, 0, len(store.GetFiles()))
	for _, f := range store.GetFiles() {
		sizes := make([]int64, 0, len(f.GetSegments()))
		for _, s := range f.GetSegments() {
			sizes = append(sizes, s.GetBytes())
		}
		out = append(out, sizes)
	}
	return out
}

func canonicalIDs(store *metapb.NzbStore) [][]string {
	out := make([][]string, 0, len(store.GetFiles()))
	for _, f := range store.GetFiles() {
		ids := make([]string, 0, len(f.GetSegments()))
		for _, s := range f.GetSegments() {
			id := strings.TrimSuffix(strings.TrimPrefix(s.GetId(), "<"), ">")
			ids = append(ids, id)
		}
		out = append(out, ids)
	}
	return out
}

// TryBundle asks the network for the bundle of the exact release the user's
// NZB describes and, if a verified one arrives in time and survives the accept
// pipeline, imports it. Every failure is "not taken".
func (f *fastPath) TryBundle(ctx context.Context, req importer.PeerBundleRequest) (importer.PeerBundleOutcome, bool) {
	if !f.cfg.Enabled() || req.NZB == nil {
		return importer.PeerBundleOutcome{}, false
	}
	rid, err := contentid.ReleaseID(req.NZB)
	if err != nil {
		return importer.PeerBundleOutcome{}, false
	}
	fetchCtx, cancel := context.WithTimeout(ctx, f.cfg.FetchTimeout)
	b, origin, err := f.cfg.Fetcher.FetchBundle(fetchCtx, rid.String())
	cancel()
	if err != nil || b == nil || b.Scheme != bundle.SchemeNZB || b.ReleaseID != rid {
		return importer.PeerBundleOutcome{}, false
	}
	if origin.Open && (f.cfg.OpenFastPath == nil || !f.cfg.OpenFastPath() || origin.Corroborators < minOpenCorroborators) {
		return importer.PeerBundleOutcome{}, false
	}
	acceptCtx, cancelAccept := context.WithTimeout(ctx, f.cfg.AcceptTimeout)
	defer cancelAccept()
	res, err := f.cfg.Acceptor.Accept(acceptCtx, bundleimport.Request{
		Bundle: b, Origin: origin, ExpectedNZB: canonicalIDs(req.NZB), ExpectedNZBSizes: encodedSizes(req.NZB),
		Target: bundleimport.Target{Dir: req.TargetDir, Category: req.Category, QueueID: int64(req.QueueID),
			TargetPath: req.TargetPath, SkipArr: req.SkipArr},
	})
	if err != nil {
		slog.InfoContext(ctx, "Peer bundle not used; importing normally", "reason", string(bundleimport.ReasonOf(err)))
		return importer.PeerBundleOutcome{}, false
	}
	return importer.PeerBundleOutcome{ResultPath: res.Dir, WrittenPaths: res.Paths, TotalSize: res.TotalSize}, true
}

// MayHaveOffers implements importer.PeerBundleFastPath.
func (f *fastPath) MayHaveOffers(ctx context.Context) bool {
	if !f.cfg.Enabled() {
		return false
	}
	return f.cfg.AnyOffers == nil || f.cfg.AnyOffers(ctx)
}

func (f *fastPath) HoldQueueItem(ctx context.Context, queueID int64) bool {
	return f.cfg.Acceptor.QueueHasPending(ctx, queueID)
}

func (f *fastPath) AttachQueueItem(ctx context.Context, paths []string, queueID int64) error {
	return f.cfg.Acceptor.AttachQueue(ctx, paths, queueID)
}

// ReleaseProber adapts the importer's release probe to bundleimport.
type ReleaseProber interface {
	ProbePeerRelease(ctx context.Context, files []importer.PeerProbeList) error
}

type proberAdapter struct{ p ReleaseProber }

// NewReleaseProber returns the bundle import's release prober over the
// importer's own probe.
func NewReleaseProber(p ReleaseProber) bundleimport.ReleaseProber { return proberAdapter{p: p} }

func (a proberAdapter) ProbeRelease(ctx context.Context, files []bundleimport.ProbeFile) error {
	lists := make([]importer.PeerProbeList, len(files))
	for i, f := range files {
		lists[i] = importer.PeerProbeList{Name: f.Name, SegmentIDs: f.SegmentIDs}
	}
	err := a.p.ProbePeerRelease(ctx, lists)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, importer.ErrPeerProbeMissing):
		return bundleimport.ErrProbeMissing
	default:
		return bundleimport.ErrProbeInconclusive
	}
}

// ImportService is the part of *importer.Service the bundle import needs.
type ImportService interface {
	AnnounceVerifiedImport(ctx context.Context, queueID int64, category, targetPath, resultingPath string) error
	RecordPeerImport(ctx context.Context, rec importer.PeerImportRecord) (int64, error)
	FailImport(ctx context.Context, queueID int64, reason string) error
	RemoveFailedFile(ctx context.Context, virtualPath string) error
	ReleaseHold(ctx context.Context, queueID int64) error
}

type serviceAdapter struct{ s ImportService }

// NewAnnouncer returns the bundle import's announcer over the importer.
func NewAnnouncer(s ImportService) bundleimport.Announcer { return serviceAdapter{s: s} }

// NewRecorder returns the bundle import's direct-import recorder over the importer.
func NewRecorder(s ImportService) bundleimport.Recorder { return serviceAdapter{s: s} }

func (a serviceAdapter) AnnounceVerifiedImport(ctx context.Context, an bundleimport.Announcement) error {
	return a.s.AnnounceVerifiedImport(ctx, an.QueueID, an.Category, an.TargetPath, an.ResultingPath)
}

func (a serviceAdapter) FailImport(ctx context.Context, queueID int64, reason string) error {
	return a.s.FailImport(ctx, queueID, reason)
}

func (a serviceAdapter) RemoveFailedFile(ctx context.Context, virtualPath string) error {
	return a.s.RemoveFailedFile(ctx, virtualPath)
}

func (a serviceAdapter) ReleaseHold(ctx context.Context, queueID int64) error {
	return a.s.ReleaseHold(ctx, queueID)
}

func (a serviceAdapter) RecordPeerImport(ctx context.Context, rec bundleimport.ImportRecord) (int64, error) {
	return a.s.RecordPeerImport(ctx, importer.PeerImportRecord{
		Title: rec.Title, Category: rec.Category, TargetPath: rec.TargetPath, ResultingPath: rec.ResultingPath,
		WrittenPaths: rec.WrittenPaths, TotalSize: rec.TotalSize, ReleaseID: rec.ReleaseID, SkipArr: rec.SkipArr,
		Metadata: rec.Metadata, DownloadID: rec.DownloadID,
	})
}

// BundleService is the part of the peer network service the bundle import
// needs: fetching a verified bundle and learning how its full check ended.
type BundleService interface {
	FetchBundle(ctx context.Context, releaseID string) (*bundle.Bundle, service.FetchSource, error)
	NoteBundleFullCheck(networkID, publisherID string, passed bool)
}

type bundleFetcher struct{ s BundleService }

// NewBundleFetcher adapts the service to bundleimport.Fetcher. A nil service
// yields nil.
func NewBundleFetcher(s BundleService) bundleimport.Fetcher {
	if s == nil {
		return nil
	}
	return bundleFetcher{s: s}
}

func (f bundleFetcher) FetchBundle(ctx context.Context, releaseID string) (*bundle.Bundle, bundleimport.Origin, error) {
	b, src, err := f.s.FetchBundle(ctx, releaseID)
	if err != nil {
		return nil, bundleimport.Origin{}, err
	}
	return b, bundleimport.Origin{NetworkID: src.Network.String(), PublisherID: src.Publisher.String(),
		Open: src.Network.IsOpen(), Corroborators: src.Corroborators}, nil
}

type bundleReporter struct{ s BundleService }

// NewBundleReporter adapts the service to bundleimport.BadBundleReporter (and
// BundleVerifiedReporter). A nil service yields nil.
func NewBundleReporter(s BundleService) bundleimport.BadBundleReporter {
	if s == nil {
		return nil
	}
	return bundleReporter{s: s}
}

func (r bundleReporter) ReportBadBundle(_ context.Context, o bundleimport.Origin, _ string) {
	r.s.NoteBundleFullCheck(o.NetworkID, o.PublisherID, false)
}

func (r bundleReporter) ReportVerifiedBundle(_ context.Context, o bundleimport.Origin, _ string) {
	r.s.NoteBundleFullCheck(o.NetworkID, o.PublisherID, true)
}
