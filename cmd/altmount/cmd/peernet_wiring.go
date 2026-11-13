package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kipsilabs/altmount/internal/api"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/health"
	"github.com/kipsilabs/altmount/internal/httpclient"
	"github.com/kipsilabs/altmount/internal/peernet/bridge"
	"github.com/kipsilabs/altmount/internal/peernet/hints"
	"github.com/kipsilabs/altmount/internal/peernet/publish"
	"github.com/kipsilabs/altmount/internal/peernet/service"
	"github.com/kipsilabs/altmount/internal/providerbackbone"
)

// peernetBackboneSource gives the peer network its narrow view of provider
// backbones: the raw storage_group of each enabled provider (read live) and
// the shared known-backbone list. It hands over nothing else from the
// configuration.
type peernetBackboneSource struct {
	cfg   config.ConfigGetter
	cache *providerbackbone.Cache
	// client builds the HTTP client of a refresh; nil uses one that follows the
	// configured proxy settings.
	client func() *http.Client
}

func newPeernetBackboneSource(cfg config.ConfigGetter, cache *providerbackbone.Cache, client func() *http.Client) *peernetBackboneSource {
	if client == nil {
		client = func() *http.Client {
			return httpclient.NewForExternal(cfg().Network, providerbackbone.FetchTimeout)
		}
	}
	return &peernetBackboneSource{cfg: cfg, cache: cache, client: client}
}

// ProviderGroups returns the backbone label of every enabled provider: its
// storage_group when set (always wins), otherwise the backbone the shared
// dataset lists for its host (exact host, or a registrable domain whose hosts
// all agree). Only the resulting label leaves this function, never a host.
func (s *peernetBackboneSource) ProviderGroups() []string {
	var out []string
	var entries []providerbackbone.Entry
	loaded := false
	for _, p := range s.cfg().Providers {
		if p.Enabled != nil && !*p.Enabled {
			continue
		}
		if group := strings.TrimSpace(p.StorageGroup); group != "" {
			out = append(out, p.StorageGroup)
			continue
		}
		if !loaded {
			entries, loaded = s.cache.Cached(), true
		}
		if label, ok := providerbackbone.Lookup(p.Host, entries); ok {
			out = append(out, label)
		}
	}
	return out
}

// Known returns the known backbone names without fetching anything.
func (s *peernetBackboneSource) Known() []string {
	return providerbackbone.Backbones(s.cache.Cached())
}

// Refresh renews the known list when it is stale, through the configured
// proxy settings and with the dataset's own timeout and size limits.
func (s *peernetBackboneSource) Refresh(ctx context.Context) error {
	return s.cache.Refresh(ctx, s.client())
}

// peernetExclusions reads the never-share lists from the live configuration.
// It returns copies, so a caller cannot change the configuration through them.
func peernetExclusions(cfg config.ConfigGetter) func() publish.Exclusions {
	return func() publish.Exclusions {
		sharing := cfg().Peernet.Sharing
		return publish.Exclusions{
			Categories: slices.Clone(sharing.ExcludedCategories),
			Indexers:   slices.Clone(sharing.ExcludedIndexers),
		}
	}
}

// healthObserverTarget is the part of the health worker the wiring needs.
type healthObserverTarget interface {
	SetHealthyObserver(health.HealthyObserver)
}

// wirePeernetHealthObserver installs the publishing observer on the health
// worker while the feature is enabled and removes it while it is not, now and
// on every configuration change. With the feature off the worker has no
// observer at all, so a healthy result costs nothing extra.
func wirePeernetHealthObserver(
	target healthObserverTarget,
	cfg *config.Config,
	notifier bridge.Notifier,
	register func(config.ChangeCallback),
) {
	apply := func(c *config.Config) {
		if c.PeernetEnabled() {
			target.SetHealthyObserver(bridge.NewHealthObserver(notifier))
		} else {
			target.SetHealthyObserver(nil)
		}
	}
	apply(cfg)
	register(func(_, newCfg *config.Config) { apply(newCfg) })
}

// peernetRecheckBudgetPerHour is the one global hourly budget for checks
// moved forward by peer evidence. It is not per publisher or per network.
const peernetRecheckBudgetPerHour = 6

// peernetHintsHost is the service as the consumers see it.
type peernetHintsHost interface {
	bridge.AdviceSource
	api.StremioNetworkLabeler
	SetHintSources(service.HintSources)
	ClearHintSources()
	ReceivingNetworks() []hints.Network
	NodeID() string
}

// healthAdviserTarget is the part of the health worker the consumers need.
type healthAdviserTarget interface {
	SetPeerAdviser(health.PeerAdviser)
	ScheduleCheckNow(ctx context.Context, filePath string) (bool, error)
}

// labelerTarget is the part of the API server the consumers need.
type labelerTarget interface {
	SetStremioNetworkLabeler(api.StremioNetworkLabeler)
}

// peernetConsumers installs the consuming side of the peer network (health
// advice, the recheck pass and Stremio labels) while the feature is enabled
// and removes it while it is not, now and on every configuration change. The
// one Budget and the one Advisor are built the first time the feature is
// enabled and reused afterwards.
type peernetConsumers struct {
	host      peernetHintsHost
	health    healthAdviserTarget
	labels    labelerTarget
	getCfg    config.ConfigGetter
	backbones service.BackboneSource
	store     hints.Store
	peers     hints.PeerSeen

	waiting atomic.Bool
	// mu serialises apply and the node-ID waiter; latest is the newest
	// configuration any apply saw, which the waiter installs from.
	mu     sync.Mutex
	latest *config.Config
	// ctx ends the node-ID waiter with the process (nil: never).
	ctx context.Context
	// waitStep and waitSteps bound the node-ID wait (zero: the defaults).
	waitStep  time.Duration
	waitSteps int
	advisor   *hints.Advisor
	budget    *hints.Budget
	err       error
}

func (c *peernetConsumers) build() {
	budget := hints.NewBudget(peernetRecheckBudgetPerHour)
	advisor, err := hints.New(hints.Config{
		Store: c.store, Peers: c.peers, Budget: budget,
		Networks:         c.host.ReceivingNetworks,
		SelfNodeID:       c.host.NodeID(),
		MinCorroboration: func() int { return c.getCfg().PeernetMinCorroboration() },
	})
	c.err = err
	if err != nil {
		return
	}
	c.budget, c.advisor = budget, advisor
}

// localBackbones are this node's recognised, normalised backbones, by the
// same function the publisher uses for the backbones of an attestation.
func (c *peernetConsumers) localBackbones() []string {
	return publish.RecognisedBackbones(c.backbones.ProviderGroups(), c.backbones.Known())
}

// apply installs or removes the consumers per cfg. Calls are serialised and
// each records cfg as the newest configuration, which the node-ID waiter
// installs from, so a waiter can never act on a stale one.
func (c *peernetConsumers) apply(cfg *config.Config) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.latest = cfg
	c.applyLocked(cfg)
}

func (c *peernetConsumers) applyLocked(cfg *config.Config) {
	if !cfg.PeernetEnabled() {
		c.host.ClearHintSources()
		c.health.SetPeerAdviser(nil)
		c.labels.SetStremioNetworkLabeler(nil)
		return
	}
	if c.advisor == nil && c.host.NodeID() == "" {
		// The node is still starting (a runtime enable applies the service
		// asynchronously): the advisor needs this node's ID, so wait for it.
		c.awaitNode()
		return
	}
	if c.advisor == nil {
		// A failed build leaves no advisor, so the next apply tries again.
		c.build()
	}
	if c.err != nil {
		slog.Warn("Peer network hints are unavailable", "error", fmt.Sprint(c.err))
		return
	}
	c.host.SetHintSources(service.HintSources{
		Advisor: c.advisor, Budget: c.budget, Backbones: c.localBackbones, Scheduler: c.health,
	})
	c.health.SetPeerAdviser(bridge.NewPeerAdviser(c.host))
	c.labels.SetStremioNetworkLabeler(c.host)
}

const (
	peernetNodeWaitStep  = 250 * time.Millisecond
	peernetNodeWaitSteps = 120
)

// awaitNode retries in the background until the node has an ID, the feature is
// switched off again, the process is shutting down, or the wait runs out (one
// warning is logged; a later configuration change starts a new wait). Only one
// waiter runs. The caller holds c.mu.
func (c *peernetConsumers) awaitNode() {
	if !c.waiting.CompareAndSwap(false, true) {
		return
	}
	step, steps := c.waitStep, c.waitSteps
	if step <= 0 {
		step = peernetNodeWaitStep
	}
	if steps <= 0 {
		steps = peernetNodeWaitSteps
	}
	done := context.Background().Done()
	if c.ctx != nil {
		done = c.ctx.Done()
	}
	go func() {
		timer := time.NewTimer(step)
		defer timer.Stop()
		for range steps {
			select {
			case <-done:
				c.waiting.Store(false)
				return
			case <-timer.C:
			}
			if c.tryInstallAfterWait() {
				c.waiting.Store(false)
				return
			}
			timer.Reset(step)
		}
		c.finishWait()
	}()
}

// finishWait ends a wait that ran out. Under the lock it clears the waiting
// flag and polls one last time, so a node that became ready after the last
// poll is installed now instead of at the next configuration change, and no
// apply can slip in between the last poll and the flag being cleared.
func (c *peernetConsumers) finishWait() {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.waiting.Store(false) // after the warning, before the unlock
	cfg := c.latest
	if cfg == nil || !cfg.PeernetEnabled() {
		return
	}
	if c.advisor == nil && c.host.NodeID() == "" {
		slog.Warn("Peer network hints are not installed: the node never became ready; a configuration change retries")
		return
	}
	c.applyLocked(cfg)
}

// tryInstallAfterWait installs from the newest configuration. It reports
// whether the wait is over (installed, or the feature was switched off).
func (c *peernetConsumers) tryInstallAfterWait() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	cfg := c.latest
	if cfg == nil || !cfg.PeernetEnabled() {
		return true
	}
	if c.advisor == nil && c.host.NodeID() == "" {
		return false
	}
	// waiting is still set, so applyLocked cannot start a second waiter.
	c.applyLocked(cfg)
	return true
}

// wirePeernetConsumers installs the consumers per the configuration and keeps
// them in step with it. register is the config manager's change hook.
func wirePeernetConsumers(c *peernetConsumers, cfg *config.Config, register func(config.ChangeCallback)) {
	c.apply(cfg)
	register(func(_, newCfg *config.Config) { c.apply(newCfg) })
}

// newPeernetConsumers builds the consumers over the database's read-only
// record and peer views.
func newPeernetConsumers(host peernetHintsHost, h healthAdviserTarget, l labelerTarget, getCfg config.ConfigGetter,
	backbones service.BackboneSource, store hints.Store, peers hints.PeerSeen) *peernetConsumers {
	return &peernetConsumers{host: host, health: h, labels: l, getCfg: getCfg, backbones: backbones, store: store, peers: peers}
}

// wirePeernetHints connects the consuming side of the peer network to the
// health worker and the API server. Both are given nothing while the feature
// is off.
func wirePeernetHints(
	ctx context.Context,
	svc *service.Service,
	healthWorker healthAdviserTarget,
	apiServer labelerTarget,
	db *database.DB,
	configManager *config.Manager,
	cfg *config.Config,
) {
	getter := configManager.GetConfigGetter()
	conn, d := db.Connection(), db.Dialect()
	c := newPeernetConsumers(svc, healthWorker, apiServer, getter,
		newPeernetBackboneSource(getter, providerbackbone.Default, nil),
		database.NewPeernetRecordRepository(conn, d), database.NewPeernetPeerRepository(conn, d))
	c.ctx = ctx
	wirePeernetConsumers(c, cfg, configManager.OnConfigChange)
}
