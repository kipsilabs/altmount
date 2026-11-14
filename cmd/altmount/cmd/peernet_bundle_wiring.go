package cmd

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kipsilabs/altmount/internal/api"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/health"
	"github.com/kipsilabs/altmount/internal/importer"
	"github.com/kipsilabs/altmount/internal/importer/postprocessor"
	"github.com/kipsilabs/altmount/internal/metadata"
	"github.com/kipsilabs/altmount/internal/nzbfilesystem/segcache"
	"github.com/kipsilabs/altmount/internal/par2repair"
	"github.com/kipsilabs/altmount/internal/peernet/bridge"
	"github.com/kipsilabs/altmount/internal/peernet/bundle"
	"github.com/kipsilabs/altmount/internal/peernet/bundleimport"
	"github.com/kipsilabs/altmount/internal/peernet/bundlex"
	"github.com/kipsilabs/altmount/internal/peernet/service"
	"github.com/kipsilabs/altmount/internal/pool"
)

var errNoBundleFetcher = errors.New("peer network bundle fetching is not connected")

// peernetBundleSeams are the two things the bundle import needs from the peer
// network side and that are connected later: the bundle fetcher and the
// bad-publisher reporter. Until they are set the defaults do nothing, and the
// importer fast path stays uninstalled, so the feature changes no behaviour.
type peernetBundleSeams struct {
	mu       sync.RWMutex
	fetcher  bundleimport.Fetcher
	reporter bundleimport.BadBundleReporter
}

// SetFetcher connects the bundle fetcher (the peer network service).
func (s *peernetBundleSeams) SetFetcher(f bundleimport.Fetcher) {
	s.mu.Lock()
	s.fetcher = f
	s.mu.Unlock()
}

// SetReporter connects the bad-publisher reporter.
func (s *peernetBundleSeams) SetReporter(r bundleimport.BadBundleReporter) {
	s.mu.Lock()
	s.reporter = r
	s.mu.Unlock()
}

func (s *peernetBundleSeams) hasFetcher() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.fetcher != nil
}

// FetchBundle forwards to the connected fetcher; with none it fails.
func (s *peernetBundleSeams) FetchBundle(ctx context.Context, releaseID string) (*bundle.Bundle, bundleimport.Origin, error) {
	s.mu.RLock()
	f := s.fetcher
	s.mu.RUnlock()
	if f == nil {
		return nil, bundleimport.Origin{}, errNoBundleFetcher
	}
	return f.FetchBundle(ctx, releaseID)
}

// ReportBadBundle forwards to the connected reporter; with none it logs.
func (s *peernetBundleSeams) ReportBadBundle(ctx context.Context, o bundleimport.Origin, releaseID string) {
	s.mu.RLock()
	r := s.reporter
	s.mu.RUnlock()
	if r == nil {
		slog.InfoContext(ctx, "A peer bundle failed its full check; no reporter is connected", "release_id", releaseID)
		return
	}
	r.ReportBadBundle(ctx, o, releaseID)
}

// ReportVerifiedBundle forwards to the connected reporter when it wants to
// hear about verified bundles.
func (s *peernetBundleSeams) ReportVerifiedBundle(ctx context.Context, o bundleimport.Origin, releaseID string) {
	s.mu.RLock()
	r := s.reporter
	s.mu.RUnlock()
	if v, ok := r.(bundleimport.BundleVerifiedReporter); ok {
		v.ReportVerifiedBundle(ctx, o, releaseID)
	}
}

type fastPathTarget interface {
	SetPeerBundleFastPath(importer.PeerBundleFastPath)
}
type peerPlayTarget interface {
	SetPeerPlayback(api.PeerPlayback)
}
type pwRecorderTarget interface {
	SetNZBPasswordRecorder(importer.NZBPasswordRecorder)
}
type healthGateTarget interface {
	SetPeerBundleGate(health.PeerBundleGate)
}
type bundleLookupTarget interface {
	SetPeerBundleLookup(api.PeerBundleLookup)
}

// peernetBundleHost installs the bundle import (importer fast path, health
// gate, API badges) while the peer network is enabled and removes it while it
// is not, now and on every configuration change. With the feature off none of
// the hooks exists, so imports and health checks run no peer-network code and
// no query. The fast path additionally needs a connected fetcher.
type peernetBundleHost struct {
	importerSvc fastPathTarget
	health      healthGateTarget
	api         bundleLookupTarget
	lookup      api.PeerBundleLookup
	// pwTarget and pwRecorder install the hook that records where a keyed
	// file's password came from; both may be nil.
	pwTarget pwRecorderTarget
	// play and peerPlay install the Stremio candidates and play route; both
	// may be nil.
	play       peerPlayTarget
	peerPlay   api.PeerPlayback
	pwRecorder importer.NZBPasswordRecorder
	seams      *peernetBundleSeams
	// receiving says at least one network is at level 1 or above.
	receiving func() bool
	// anyOffers is the cached "this node holds a bundle offer" check (nil: assumed).
	anyOffers func(context.Context) bool
	// hasPending says peer-bundle imports still owe their full check. While it
	// is true the gate stays installed even with the feature off (fail closed):
	// such imports must still be force-checked and released on pass.
	hasPending func() bool
	// observe installs the importer's path observer on the metadata service.
	observe func(metadata.FileObserver)
	build   func() *bundleimport.Importer
	getCfg  config.ConfigGetter

	mu  sync.Mutex
	imp *bundleimport.Importer
}

func (h *peernetBundleHost) importer() *bundleimport.Importer {
	if h.imp == nil {
		h.imp = h.build()
		if h.observe != nil {
			h.observe(h.imp.PathObserver())
		}
		go h.imp.ResumeAnnouncements(context.Background())
		go h.imp.RunHousekeeping(context.Background())
	}
	return h.imp
}

func (h *peernetBundleHost) apply(cfg *config.Config) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.pwTarget != nil {
		// Recording is a one-row write per keyed import; with the feature off
		// the importer has no hook at all and runs no extra code.
		if cfg.PeernetEnabled() {
			h.pwTarget.SetNZBPasswordRecorder(h.pwRecorder)
		} else {
			h.pwTarget.SetNZBPasswordRecorder(nil)
		}
	}
	if h.play != nil {
		// Stremio candidates need the feature and a connected fetcher.
		if cfg.PeernetEnabled() && h.seams.hasFetcher() {
			h.play.SetPeerPlayback(h.peerPlay)
		} else {
			h.play.SetPeerPlayback(nil)
		}
	}
	if !cfg.PeernetEnabled() {
		h.importerSvc.SetPeerBundleFastPath(nil)
		if h.hasPending != nil && h.hasPending() {
			h.health.SetPeerBundleGate(h.importer())
			h.api.SetPeerBundleLookup(h.lookup)
			return
		}
		h.health.SetPeerBundleGate(nil)
		h.api.SetPeerBundleLookup(nil)
		return
	}
	imp := h.importer()
	// Pending peer imports must be honoured even before a fetcher exists.
	h.health.SetPeerBundleGate(imp)
	h.api.SetPeerBundleLookup(h.lookup)
	if h.seams.hasFetcher() {
		h.importerSvc.SetPeerBundleFastPath(bridge.NewPeerBundleFastPath(bridge.FastPathConfig{
			Acceptor: imp, Fetcher: h.seams, Enabled: h.receiving, AnyOffers: h.anyOffers,
			OpenFastPath: func() bool { return h.getCfg != nil && h.getCfg().Peernet.OpenNetwork.FastPath },
		}))
	} else {
		h.importerSvc.SetPeerBundleFastPath(nil)
	}
}

// Importer returns the bundle importer (nil until the feature was first
// enabled). The later Stremio flow calls ImportRelease on it, and the
// publisher asks PeerBundleStatus.
func (h *peernetBundleHost) Importer() *bundleimport.Importer {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.imp
}

// Refresh re-applies the current configuration, for use after a seam was
// connected.
func (h *peernetBundleHost) Refresh(cfg *config.Config) { h.apply(cfg) }

func wirePeernetBundleImport(h *peernetBundleHost, cfg *config.Config, register func(config.ChangeCallback)) {
	h.apply(cfg)
	register(func(_, newCfg *config.Config) { h.apply(newCfg) })
}

type bundleWiringDeps struct {
	Metadata  *metadata.MetadataService
	DB        *database.DB
	Importer  *importer.Service
	Pool      pool.Manager
	Cache     *segcache.Source
	GetConfig config.ConfigGetter
	Receiving func() bool
	Health    healthGateTarget
	API       bundleLookupTarget
}

// peernetStagingRoot is where hidden staging areas live: beside the database,
// as an absolute path, or in the system temp directory when no database path
// is configured (never relative to the working directory).
func peernetStagingRoot(dbPath string) string {
	if dbPath == "" {
		return filepath.Join(os.TempDir(), "altmount-peernet-staging")
	}
	dir, err := filepath.Abs(filepath.Dir(dbPath))
	if err != nil {
		return filepath.Join(os.TempDir(), "altmount-peernet-staging")
	}
	return filepath.Join(dir, ".peernet-staging")
}

// newPeernetBundleHost builds the real host over the application's services.
func newPeernetBundleHost(d bundleWiringDeps, seams *peernetBundleSeams) *peernetBundleHost {
	conn, dialect := d.DB.Connection(), d.DB.Dialect()
	state := database.NewPeernetBundleImportRepository(conn, dialect)
	stagingRoot := peernetStagingRoot(d.GetConfig().Database.Path)
	// A crash can leave staging areas behind; none is ever in use at start, and
	// this runs whether or not the feature is enabled.
	_ = os.RemoveAll(stagingRoot)
	return &peernetBundleHost{
		importerSvc: d.Importer, health: d.Health, api: d.API, lookup: state, seams: seams,
		pwTarget:   d.Importer,
		pwRecorder: bridge.NewNZBPasswordRecorder(d.Metadata, database.NewPeernetPasswordOriginRepository(conn, dialect)),
		receiving:  d.Receiving, getCfg: d.GetConfig,
		anyOffers: bridge.NewOfferCache(database.NewPeernetRecordRepository(conn, dialect), database.BundleOfferGeneration),
		observe:   d.Metadata.SetSecondaryFileObserver,
		hasPending: func() bool {
			n, _, err := state.PendingCounts(context.Background(), time.Time{})
			return err != nil || n > 0
		},
		build: func() *bundleimport.Importer {
			return bundleimport.New(bundleimport.Config{
				Library:           d.Metadata,
				Stager:            bundleimport.NewFSStager(bundleimport.FSStagerDeps{Pool: d.Pool, Config: d.GetConfig, Cache: d.Cache, Root: stagingRoot}),
				Prober:            bridge.NewReleaseProber(d.Importer),
				State:             state,
				Index:             database.NewPeernetContentRepository(conn, dialect),
				Fetcher:           seams,
				Recorder:          bridge.NewRecorder(d.Importer),
				Announcer:         bridge.NewAnnouncer(d.Importer),
				Reporter:          seams,
				HealthEnabled:     func() bool { return d.GetConfig().GetHealthEnabled() },
				WillBeChecked:     func(p string) bool { return postprocessor.WillScheduleHealth(d.GetConfig(), p) },
				AllowedExtensions: func() []string { return d.GetConfig().Import.AllowedFileExtensions },
			})
		},
	}
}

// connectPeernetBundleSeams connects the peer network service as the bundle
// fetcher and the bad-publisher reporter, then re-applies the configuration so
// the importer fast path appears. With the feature off nothing is installed.
func connectPeernetBundleSeams(h *peernetBundleHost, svc bridge.BundleService, cfg *config.Config) {
	h.seams.SetFetcher(bridge.NewBundleFetcher(svc))
	h.seams.SetReporter(bridge.NewBundleReporter(svc))
	h.Refresh(cfg)
}

var errPeerOriginNotReady = errors.New("peer bundle state is not available yet")

// peerOriginBinder is the publisher's view of the bundle importer, bound once
// the bundle host exists (the peer network service is created first). It fails
// closed: until the importer exists no file counts as verified, so nothing is
// published on an unanswerable question.
type peerOriginBinder struct {
	h atomic.Pointer[peernetBundleHost]
}

// Bind connects the bundle host.
func (b *peerOriginBinder) Bind(h *peernetBundleHost) { b.h.Store(h) }

// PeerBundleStatus implements service.PeerBundleOrigin.
func (b *peerOriginBinder) PeerBundleStatus(ctx context.Context, filePath string) (fromPeer, fullCheckPassed bool, err error) {
	h := b.h.Load()
	if h == nil {
		return false, false, errPeerOriginNotReady
	}
	imp := h.Importer()
	if imp == nil {
		return false, false, errPeerOriginNotReady
	}
	return imp.PeerBundleStatus(ctx, filePath)
}

// peernetPatches gives the bundle builder its PAR2 patch index. A node with no
// patch store shares as one with an empty store.
func peernetPatches(store *par2repair.PatchStore) bundlex.PatchIndex {
	if store == nil {
		return bundlex.NoPatches{}
	}
	return store
}

// ImportStatus maps the importer's counters for the peer network status (zero
// while no importer exists).
func (b *peerOriginBinder) ImportStatus() service.BundleImportStatus {
	h := b.h.Load()
	if h == nil || h.Importer() == nil {
		return service.BundleImportStatus{}
	}
	c := h.Importer().Counters()
	rej := make(map[string]int64, len(c.Rejected))
	for k, v := range c.Rejected {
		rej[string(k)] = int64(v)
	}
	pending, stale := h.Importer().PendingStatus(context.Background())
	return service.BundleImportStatus{Pending: int64(pending), Stale: int64(stale), Accepted: int64(c.Accepted), AlreadyImported: int64(c.AlreadyImported), Rejected: rej,
		FullCheckPassed: int64(c.FullCheckPassed), FullCheckFailed: int64(c.FullCheckFailed),
		Announced: int64(c.Announced), BadBundleReports: int64(c.BadBundleReports)}
}
