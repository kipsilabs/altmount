package service

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/metadata"
	"github.com/kipsilabs/altmount/internal/peernet/contentid"
	"github.com/kipsilabs/altmount/internal/peernet/publish"
)

// Errors returned to API callers.
var (
	ErrDisabled               = errors.New("peer network is disabled")
	ErrAcceptIncomingRequired = errors.New("hosting a private network requires accepting incoming connections")
	ErrOpenNetwork            = errors.New("the open network cannot be changed here")
	ErrNotFound               = errors.New("not found")
)

// Deps are the narrow values the service needs. It is deliberately not given
// the config manager, ARR clients, provider credentials or users.
type Deps struct {
	// DataDir is the absolute directory the node key and the private-network
	// root keys live under (in a dedicated owner-only subdirectory). Empty or
	// relative means the caller could not resolve a safe location: the feature
	// then refuses to start, with a status error, instead of writing keys into
	// the working directory.
	DataDir  string
	Networks *database.PeernetNetworkRepository
	Peers    *database.PeernetPeerRepository
	Content  *database.PeernetContentRepository
	// Records and Cursors store signed peer-network records and the per-peer
	// sync positions. When either is nil record exchange is off: nothing is
	// pulled, pushed or served, and publishing is refused.
	Records *database.PeernetRecordRepository
	Cursors *database.PeernetSyncCursorRepository
	// PublishState, Origins, Exclusions and Backbones enable publishing of this
	// node's own healthy releases (levels 2 and 3). When any of PublishState or
	// Origins is nil, or Records, Cursors, Content or Metadata is, nothing is
	// ever published. Exclusions is read on every publish, so config changes
	// apply at once; nil means none. Backbones may be nil: no attestation is
	// then published.
	PublishState *database.PeernetPublishStateRepository
	Origins      *database.PeernetOriginRepository
	Exclusions   func() publish.Exclusions
	Backbones    BackboneSource
	// Health is the read-only local health lookup the periodic re-announce
	// requires; nil means release records are never re-announced.
	Health LocalHealth
	// Metadata is observed (while enabled) to keep the content index current.
	// May be nil.
	Metadata MetadataHost
	// Settings are the initial settings.
	Settings Settings
	// TickInterval is the base period of the discovery loop (default 30 s).
	TickInterval time.Duration

	// SyncInterval is the base period of the record sync rounds (default 2
	// minutes; the first round runs after at most 20 seconds of it).
	SyncInterval time.Duration

	// MaintainInterval is the period of the membership upkeep (certificate
	// renewal, revocation refresh); default 30 s. The upkeep is throttled per
	// network by the membership layer, so this only bounds how soon a due
	// renewal is noticed.
	MaintainInterval time.Duration

	// hub is shared by pointer with the node (see hints.go); New creates it.
	hub *hintsHub

	// Test-only hooks.
	publishQueueSize int              // publishing queue bound (default 512)
	publishNow       func() time.Time // scheduling clock of publishing (tests; default time.Now)
	beforePublish    func()           // runs before each queued notice is handled
	backboneEvery    time.Duration    // backbone refresh check period (default 10 min)
	noContentIndex   bool             // leave the content index to the test
	allowLoopback    bool
	listenHost       string
	listenRetryBase  time.Duration  // first listener rebind delay (default 30 s)
	applyHook        func(Settings) // runs at the start of every Apply
	// beforeListen runs after the routes are registered and before the
	// listener starts accepting.
	beforeListen func(routesRegistered, bound bool)
}

// MetadataHost is what the content index needs from the metadata service:
// read access to the tree and a place to hook a file observer.
type MetadataHost interface {
	contentid.Source
	SetFileObserver(metadata.FileObserver)
}

type metadataHost struct {
	*contentid.ServiceSource
	ms *metadata.MetadataService
}

func (h metadataHost) SetFileObserver(o metadata.FileObserver) { h.ms.SetFileObserver(o) }

// NewMetadataHost adapts the metadata service to MetadataHost.
func NewMetadataHost(ms *metadata.MetadataService) MetadataHost {
	return metadataHost{ServiceSource: contentid.NewServiceSource(ms), ms: ms}
}

// Service owns the feature's lifecycle.
type Service struct {
	deps Deps

	applyMu sync.Mutex // serialises Start, Stop and Apply
	started bool
	parent  context.Context

	mu       sync.RWMutex // guards the fields below
	settings Settings
	node     *node
	index    *contentIndex // survives node rebuilds
	startErr string
	lifeCtx  context.Context // parent, readable without applyMu

	// Asynchronous config application (see OnConfigChange).
	asyncMu      sync.Mutex
	asyncPending *Settings
	asyncRunning bool
	asyncStopped bool // set by Stop under asyncMu: no new worker may start
	asyncWG      sync.WaitGroup
}

// New returns a stopped Service. It touches nothing until Start.
func New(deps Deps) *Service {
	if deps.hub == nil {
		deps.hub = newHintsHub()
	}
	return &Service{deps: deps, settings: deps.Settings}
}

// Start brings the feature up when it is enabled.
func (s *Service) Start(ctx context.Context) {
	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	s.started, s.parent = true, ctx
	s.asyncMu.Lock()
	s.asyncStopped = false
	s.asyncMu.Unlock()
	s.mu.Lock()
	s.lifeCtx = ctx
	s.mu.Unlock()
	s.reconcileLifecycle(ctx)
}

// Stop tears everything down and leaves the service stopped. It waits for a
// pending asynchronous configuration change to finish first.
func (s *Service) Stop(ctx context.Context) {
	// No worker may be added once Stop has begun waiting for them.
	s.asyncMu.Lock()
	s.asyncStopped = true
	s.asyncMu.Unlock()
	s.asyncWG.Wait()
	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	s.started = false
	s.stopNode(ctx)
	s.stopIndex()
}

// Apply installs new settings and starts, stops or updates the feature to
// match. It is a no-op before Start and after Stop.
func (s *Service) Apply(ctx context.Context, set Settings) {
	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	if s.deps.applyHook != nil {
		s.deps.applyHook(set)
	}
	s.mu.Lock()
	s.settings = set
	s.mu.Unlock()
	if s.started {
		s.reconcileLifecycle(ctx)
	}
}

// OnConfigChange is a config.ChangeCallback. It returns at once: nothing here
// may run inside the settings request that triggered it. A change that leaves
// the peer network's derived settings as they were is ignored outright (an
// unrelated settings save must not touch a running node). Otherwise the new
// settings are queued and applied by a single background worker, in order,
// last write wins: a burst of changes applies only the newest. Stop waits for
// the worker.
func (s *Service) OnConfigChange(oldCfg, newCfg *config.Config) {
	next := SettingsFromConfig(newCfg)
	if SettingsFromConfig(oldCfg).Equal(next) {
		return
	}
	s.asyncMu.Lock()
	if s.asyncStopped {
		s.asyncMu.Unlock()
		return
	}
	s.asyncPending = &next
	if !s.asyncRunning {
		s.asyncRunning = true
		s.asyncWG.Add(1)
		go s.applyWorker()
	}
	s.asyncMu.Unlock()
}

func (s *Service) applyWorker() {
	defer s.asyncWG.Done()
	for {
		s.asyncMu.Lock()
		next := s.asyncPending
		s.asyncPending = nil
		if next == nil {
			s.asyncRunning = false
			s.asyncMu.Unlock()
			return
		}
		s.asyncMu.Unlock()
		s.Apply(s.lifecycleContext(), *next)
	}
}

// lifecycleContext is the context Start was given (Background before Start).
func (s *Service) lifecycleContext() context.Context {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.lifeCtx != nil {
		return s.lifeCtx
	}
	return context.Background()
}

func (s *Service) reconcileLifecycle(ctx context.Context) {
	set := s.currentSettings()
	n := s.currentNode()
	switch {
	case !set.Enabled:
		s.stopNode(ctx)
		s.stopIndex()
		s.setStartErr("")
	case n == nil:
		s.startNode(ctx, set)
	case n.snapshot().listenerChanged(set):
		s.stopNode(ctx)
		s.startNode(ctx, set)
	default:
		n.update(s.parent, set)
	}
	s.ensureIndex()
}

// ensureIndex starts the content index once a node is running. A node rebuild
// leaves it alone.
func (s *Service) ensureIndex() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.node != nil && s.index == nil && !s.deps.noContentIndex {
		s.index = startContentIndex(s.parent, s.deps)
	}
}

func (s *Service) stopIndex() {
	s.mu.Lock()
	c := s.index
	s.index = nil
	s.mu.Unlock()
	if c != nil {
		c.stop()
	}
}

func (s *Service) startNode(ctx context.Context, set Settings) {
	n, err := startNode(s.parent, s.deps, set)
	if err != nil {
		slog.ErrorContext(ctx, "Peer network could not start", "error", err)
		s.setStartErr(truncateErr(err))
		return
	}
	s.setStartErr("")
	s.mu.Lock()
	s.node = n
	s.mu.Unlock()
	slog.InfoContext(ctx, "Peer network started", "listening", n.listening(),
		"accept_incoming", set.AcceptIncoming)
}

func (s *Service) stopNode(ctx context.Context) {
	s.mu.Lock()
	n := s.node
	s.node = nil
	s.mu.Unlock()
	if n == nil {
		return
	}
	n.stop(ctx)
	slog.InfoContext(ctx, "Peer network stopped")
}

func (s *Service) currentSettings() Settings {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.settings
}

func (s *Service) currentNode() *node {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.node
}

func (s *Service) setStartErr(msg string) {
	s.mu.Lock()
	s.startErr = msg
	s.mu.Unlock()
}

// requireNode returns the running node or ErrDisabled.
func (s *Service) requireNode() (*node, error) {
	if n := s.currentNode(); n != nil {
		return n, nil
	}
	return nil, ErrDisabled
}
