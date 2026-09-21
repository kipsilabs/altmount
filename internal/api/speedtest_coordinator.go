package api

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/javi11/nntppool/v5"
	"golang.org/x/sync/singleflight"
)

// speedtestCoordinator bounds the structural footprint of the
// /providers/:id/speedtest endpoint.
//
// Without coordination, every HTTP request spawned a fresh
// nntppool.NewClient with up to Provider.MaxConnections dial attempts.
// A user (or monitoring script) hitting the endpoint repeatedly opened
// N independent TCP/TLS connection sets in parallel.
//
// With this coordinator:
//   - A singleflight.Group dedupes concurrent requests for the same
//     provider — only one in-flight speed test per providerID. The
//     other callers share its result.
//   - Each per-provider nntppool.Client is held in a short-lived cache
//     (TTL clientTTL). Subsequent requests within the TTL reuse the
//     same client rather than opening a fresh connection set.
//   - Speed tests issued against a provider already in the running
//     pool prefer that pool (call sites pass a function returning the
//     active client) so production traffic and speed tests share the
//     same connections.
//
// Safe for concurrent use.
type speedtestCoordinator struct {
	sf      singleflight.Group
	mu      sync.Mutex
	clients map[string]*cachedSpeedtestClient // keyed by providerID

	// stopCh signals the janitor goroutine to exit. Closed exactly once
	// by shutdown via stopOnce. The field itself is immutable after
	// construction so janitorLoop can read it without locking.
	stopCh chan struct{}
	// stopOnce gates shutdown so multiple calls are safe and so the
	// janitor channel is closed exactly once.
	stopOnce sync.Once
	// wg tracks the janitor goroutine so shutdown can wait for it.
	wg sync.WaitGroup
}

type cachedSpeedtestClient struct {
	client    *nntppool.Client
	expiresAt time.Time
	host      string // tracked for logging
}

// clientTTL bounds how long a per-provider speed-test client stays
// cached. Short enough to absorb a burst of requests; long enough that
// repeated speed tests in a monitoring loop don't dial each time.
const clientTTL = 5 * time.Minute

// janitorInterval controls how often expired clients are reaped. Set to
// clientTTL/5 so a freshly-expired entry is collected within ~1 minute
// of expiry instead of lingering until the next request for the same
// provider — without the sweep, an idle pod retains a full
// nntppool.Client (with its connection set and goroutines) per provider
// ever speed-tested in the process lifetime.
const janitorInterval = clientTTL / 5

func newSpeedtestCoordinator() *speedtestCoordinator {
	sc := &speedtestCoordinator{
		clients: make(map[string]*cachedSpeedtestClient),
		stopCh:  make(chan struct{}),
	}
	sc.wg.Add(1)
	go sc.janitorLoop()
	return sc
}

// janitorLoop periodically evicts expired cache entries. Exits when
// stopCh is closed. Holds the mutex only while iterating the map; each
// Close() happens after delete to keep the critical section bounded.
func (sc *speedtestCoordinator) janitorLoop() {
	defer sc.wg.Done()
	ticker := time.NewTicker(janitorInterval)
	defer ticker.Stop()
	for {
		select {
		case <-sc.stopCh:
			return
		case <-ticker.C:
			sc.sweepExpired()
		}
	}
}

// sweepExpired removes every entry whose TTL has elapsed and closes the
// underlying client. Splitting the Close calls out of the locked section
// keeps the mutex hold time bounded by map size rather than by however
// long nntppool.Client.Close takes (which can issue QUIT and wait for
// connection teardown).
func (sc *speedtestCoordinator) sweepExpired() {
	now := time.Now()
	var expired []*nntppool.Client
	sc.mu.Lock()
	for id, entry := range sc.clients {
		if now.After(entry.expiresAt) {
			expired = append(expired, entry.client)
			delete(sc.clients, id)
		}
	}
	sc.mu.Unlock()
	for _, c := range expired {
		if c != nil {
			c.Close()
		}
	}
}

// getOrBuildClient returns a cached client for the provider or builds
// a new one. The caller MUST NOT Close the returned client — the
// coordinator owns its lifetime. Returns the client and the
// nntppool-side provider name (used by SpeedTest).
func (sc *speedtestCoordinator) getOrBuildClient(ctx context.Context, p *config.ProviderConfig) (*nntppool.Client, string, error) {
	host := fmt.Sprintf("%s:%d", p.Host, p.Port)
	providerName := p.NNTPPoolName()

	sc.mu.Lock()
	if entry, ok := sc.clients[p.ID]; ok && time.Now().Before(entry.expiresAt) {
		sc.mu.Unlock()
		return entry.client, providerName, nil
	}
	// Drop stale entry before building a new one.
	if entry, ok := sc.clients[p.ID]; ok {
		entry.client.Close()
		delete(sc.clients, p.ID)
	}
	sc.mu.Unlock()

	// Mirror the production pool (ToNNTPProvider): without Inflight the
	// nntppool client defaults to depth 1, making the fallback path
	// latency-bound (one BODY per RTT) instead of pipelined.
	inflight := p.InflightRequests
	if inflight <= 0 {
		inflight = 10
	}

	client, err := buildAdHocClient(ctx, p, p.MaxConnections, inflight)
	if err != nil {
		return nil, "", err
	}

	sc.mu.Lock()
	// Another goroutine may have raced ahead and built a client too;
	// keep whichever was inserted first and close the loser.
	if existing, ok := sc.clients[p.ID]; ok && time.Now().Before(existing.expiresAt) {
		sc.mu.Unlock()
		client.Close()
		return existing.client, providerName, nil
	}
	sc.clients[p.ID] = &cachedSpeedtestClient{
		client:    client,
		expiresAt: time.Now().Add(clientTTL),
		host:      host,
	}
	sc.mu.Unlock()

	return client, providerName, nil
}

// buildAdHocClient dials a throwaway single-provider client; the caller owns Close.
func buildAdHocClient(ctx context.Context, p *config.ProviderConfig, connections, inflight int) (*nntppool.Client, error) {
	provider := p.ToNNTPProvider()
	provider.Connections = connections
	provider.Inflight = inflight
	provider.IdleTimeout = 60 * time.Second
	return nntppool.NewClient(ctx, []nntppool.Provider{provider})
}

// run executes fn under the singleflight key for the given provider,
// so concurrent callers share a single speed-test result. fn receives
// the cached/built client and the nntppool provider name.
func (sc *speedtestCoordinator) run(
	ctx context.Context,
	p *config.ProviderConfig,
	fn func(client *nntppool.Client, providerName string) (any, error),
) (any, error) {
	v, err, _ := sc.sf.Do(p.ID, func() (any, error) {
		client, providerName, err := sc.getOrBuildClient(ctx, p)
		if err != nil {
			return nil, err
		}
		return fn(client, providerName)
	})
	return v, err
}

// shutdown stops the janitor and closes all cached clients. Safe to
// call multiple times — stopOnce makes the janitor-stop idempotent;
// the final drain loop is naturally a no-op on an empty map.
func (sc *speedtestCoordinator) shutdown() {
	sc.stopOnce.Do(func() {
		close(sc.stopCh)
	})
	sc.wg.Wait()

	sc.mu.Lock()
	clients := sc.clients
	sc.clients = make(map[string]*cachedSpeedtestClient)
	sc.mu.Unlock()
	for _, entry := range clients {
		if entry.client != nil {
			entry.client.Close()
		}
	}
}
