package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/bundlex"
	"github.com/kipsilabs/altmount/internal/peernet/discovery"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/membership"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/recsync"
	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

const (
	peernetDirName   = "peernet"
	identityDirName  = "identity"
	networksDirName  = "networks"
	maxListenErrLen  = 200
	defaultTickEvery = 30 * time.Second
	stopTimeout      = 5 * time.Second

	defaultMaintainEvery = 30 * time.Second
	defaultListenRetry   = 30 * time.Second
	maxListenRetry       = 10 * time.Minute
)

// node is one running incarnation of the feature. It exists only while the
// feature is enabled and is torn down completely when it is disabled.
type node struct {
	deps   Deps
	cancel context.CancelFunc
	wg     sync.WaitGroup

	key       *identity.Key
	nodeID    identity.NodeID
	client    *transport.Client
	disc      *discovery.Service
	rx        *recsync.Exchange // nil when record storage is not configured
	pipe      *pipeline         // nil when publishing is not configured
	builder   *bundlex.Builder  // nil without a full-metadata source
	bsrv      *bundlex.Server   // nil unless bundles can be served
	fetcher   *bundlex.Fetcher  // nil when record storage is not configured
	mem       *membership.Manager
	kick      chan struct{} // wakes the discovery loop
	maintKick chan struct{} // wakes the membership upkeep

	maintRuns atomic.Int64 // completed membership upkeep passes
	// pruneCursor is where the next windowed prune of the publish state
	// starts. Only the upkeep goroutine touches it.
	pruneCursor database.PeernetPublishCursor

	hintMu     sync.Mutex       // serialises recheck passes
	hintCursor map[string]int64 // recheck pass position per network (guarded by hintMu)
	// hintRetries counts failed scheduling attempts per network (guarded by hintMu).
	hintRetries map[string]recheckRetry

	syncMu sync.Mutex // serialises syncNetworks (read of the rows and apply)

	mu              sync.Mutex
	settings        Settings
	active          map[string]struct{} // networks currently registered with discovery
	reconciledKey   string
	reconciledValid bool
	reconcileGen    int // bumped whenever reconciledValid is cleared
	lastLogged      map[string]string

	// The listener (guarded by mu). A failed bind leaves server nil and
	// listenErr set; the discovery loop retries with backoff.
	server       *transport.Server
	listenPort   int
	listenErr    string
	bindFailures int
	nextBind     time.Time

	// reconcileFn hands the configured open-network peers to discovery. It is
	// a field so tests can fail it.
	reconcileFn func(ctx context.Context, id netid.NetworkID, peers []string) error
}

// startNode builds and starts everything. A failed bind is not fatal; a
// failed identity or an unusable data directory is.
func startNode(parent context.Context, deps Deps, s Settings) (*node, error) {
	if deps.DataDir == "" || !filepath.IsAbs(deps.DataDir) {
		return nil, errors.New("the peer network data directory could not be determined: set a SQLite " +
			"database path or a metadata root path so its keys have a safe home")
	}
	dir := filepath.Join(deps.DataDir, peernetDirName)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create peernet directory: %w", err)
	}
	key, err := identity.LoadOrCreate(filepath.Join(dir, identityDirName))
	if err != nil {
		return nil, fmt.Errorf("load node identity: %w", err)
	}
	ctx, cancel := context.WithCancel(parent)
	n := &node{
		deps: deps, cancel: cancel, key: key, nodeID: key.NodeID(), settings: s,
		kick: make(chan struct{}, 1), maintKick: make(chan struct{}, 1),
		active: map[string]struct{}{}, lastLogged: map[string]string{},
	}

	var own []string
	if s.AcceptIncoming && s.ListenPort > 0 {
		own = []string{net.JoinHostPort("", strconv.Itoa(s.ListenPort))}
	}
	n.client, err = transport.NewClient(transport.ClientConfig{
		Identity:                key,
		Proxy:                   transport.ProxyFromSettings(s.HTTPProxy, s.HTTPSProxy, s.NoProxy),
		OwnListenAddrs:          own,
		AllowLoopbackForTesting: deps.allowLoopback,
	})
	if err != nil {
		n.abort()
		return nil, fmt.Errorf("create peer client: %w", err)
	}
	n.mem, err = membership.NewManager(membership.Config{
		Store: deps.Networks, KeysDir: filepath.Join(dir, networksDirName), NodeID: n.nodeID, Caller: n.client,
	})
	if err == nil {
		err = n.mem.EnsureOpenNetwork(ctx)
	}
	if err == nil {
		n.disc, err = discovery.New(discovery.Config{
			Self: n.nodeID, Dialer: n.client, Store: deps.Peers, Auth: authBridge{mem: n.mem, onRefused: n.noteRefused},
			AllowLoopbackForTesting: deps.allowLoopback,
		})
	}
	if err != nil {
		n.abort()
		return nil, fmt.Errorf("start peer services: %w", err)
	}
	if err := n.startExchange(); err != nil {
		n.abort()
		return nil, fmt.Errorf("start record exchange: %w", err)
	}
	n.startBuilder()
	if err := n.startPublishing(); err != nil {
		n.abort()
		return nil, fmt.Errorf("start publishing: %w", err)
	}
	n.startBundleServe()
	n.reconcileFn = n.disc.Reconcile
	n.disc.AddSource(hostSource{mem: n.mem})
	if s.AcceptIncoming {
		n.bindListener(ctx)
	}

	n.syncNetworks(ctx)
	n.wg.Add(2)
	go n.loop(ctx)
	go n.maintLoop(ctx)
	if n.rx != nil {
		n.wg.Add(1)
		go n.syncLoop(ctx)
	}
	if n.pipe != nil {
		n.wg.Add(2)
		go func() { defer n.wg.Done(); n.pipe.run(ctx) }()
		go n.backboneLoop(ctx)
	}
	return n, nil
}

// bindListener builds the peer server, registers every route and only then
// starts accepting, so no request can reach a server with missing routes. On
// failure it records the error and the next retry time; on success it clears
// them.
func (n *node) bindListener(ctx context.Context) {
	s := n.snapshot()
	srv, err := n.newServer(ctx, s)
	n.mu.Lock()
	if err != nil {
		n.listenErr = truncateErr(err)
		n.bindFailures++
		n.nextBind = time.Now().Add(n.bindBackoff())
		n.mu.Unlock()
		slog.ErrorContext(ctx, "Peer listener could not start", "port", s.ListenPort, "error", err)
		return
	}
	n.server, n.listenErr, n.bindFailures, n.nextBind = srv, "", 0, time.Time{}
	if ta, ok := srv.Addr().(*net.TCPAddr); ok {
		n.listenPort = ta.Port
	}
	n.mu.Unlock()
	// The advertised address only applies while listening.
	n.syncNetworks(ctx)
}

func (n *node) newServer(ctx context.Context, s Settings) (*transport.Server, error) {
	addr := net.JoinHostPort(n.deps.listenHost, strconv.Itoa(s.ListenPort))
	srv, err := transport.NewServer(transport.ServerConfig{Identity: n.key, ListenAddr: addr})
	if err != nil {
		return nil, err
	}
	if err := n.registerRoutes(srv); err != nil {
		return nil, err
	}
	if h := n.deps.beforeListen; h != nil {
		h(true, srv.Addr() != nil)
	}
	if err := srv.Start(ctx); err != nil {
		return nil, err
	}
	return srv, nil
}

// bindBackoff is the wait before the next rebind attempt (mu held): the base
// doubling per consecutive failure up to a cap.
func (n *node) bindBackoff() time.Duration {
	base := n.deps.listenRetryBase
	if base <= 0 {
		base = defaultListenRetry
	}
	d := base
	for i := 1; i < n.bindFailures && d < maxListenRetry; i++ {
		d *= 2
	}
	return min(d, max(maxListenRetry, base))
}

// retryListener rebinds only the listener when accept_incoming is on, the
// bind failed and its backoff has passed. The error stays visible in the
// status until a bind succeeds.
func (n *node) retryListener(ctx context.Context) {
	n.mu.Lock()
	due := n.settings.AcceptIncoming && n.server == nil && n.listenErr != "" && !time.Now().Before(n.nextBind)
	n.mu.Unlock()
	if due && ctx.Err() == nil {
		n.bindListener(ctx)
	}
}

// listenerRetryIn returns how long until the next rebind attempt, or 0 when
// none is pending.
func (n *node) listenerRetryIn() time.Duration {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.settings.AcceptIncoming || n.server != nil || n.listenErr == "" {
		return 0
	}
	return max(time.Until(n.nextBind), time.Millisecond)
}

func (n *node) registerRoutes(srv *transport.Server) error {
	if err := discovery.RegisterRoutes(srv, n.disc); err != nil {
		return err
	}
	if n.rx != nil {
		if err := n.rx.RegisterRoutes(srv); err != nil {
			return err
		}
	}
	if n.bsrv != nil {
		if err := n.bsrv.RegisterRoutes(srv); err != nil {
			return err
		}
	}
	return n.mem.RegisterHandlers(srv)
}

// takeServer detaches the running server (nil if none).
func (n *node) takeServer() *transport.Server {
	n.mu.Lock()
	defer n.mu.Unlock()
	srv := n.server
	n.server, n.listenPort = nil, 0
	return srv
}

func (n *node) stopServer(ctx context.Context) {
	srv := n.takeServer()
	if srv == nil {
		return
	}
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), stopTimeout)
	defer cancel()
	if err := srv.Stop(sctx); err != nil {
		slog.WarnContext(ctx, "Peer listener did not stop cleanly", "error", err)
	}
}

// abort releases what a failed start created.
func (n *node) abort() {
	n.cancel()
	n.stopServer(context.Background())
	if n.client != nil {
		n.client.Close()
	}
}

// stop ends the loop, the backfill, the listener and the client and waits for
// every goroutine this node started.
func (n *node) stop(ctx context.Context) {
	n.cancel()
	n.wg.Wait()
	n.stopServer(ctx)
	n.client.Close()
}

func (n *node) snapshot() Settings {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.settings
}

func (n *node) listening() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.server != nil
}

// listenState returns the listener's port (0 when not listening) and error.
func (n *node) listenState() (int, string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.listenPort, n.listenErr
}

// update applies new settings that need no rebuild. A failed listener gets an
// immediate retry on the next loop pass.
func (n *node) update(ctx context.Context, s Settings) {
	n.mu.Lock()
	n.settings = s
	n.nextBind = time.Time{}
	n.mu.Unlock()
	n.refresh(ctx)
}

// refresh re-syncs discovery's network set and wakes the loop.
func (n *node) refresh(ctx context.Context) {
	n.syncNetworks(ctx)
	select {
	case n.kick <- struct{}{}:
	default:
	}
}

// logChange logs err once per state change, keyed by what failed; a nil err
// resets the key so a later failure is logged again.
func (n *node) logChange(ctx context.Context, key string, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err == nil {
		delete(n.lastLogged, key)
		return
	}
	msg := err.Error()
	if n.lastLogged[key] == msg {
		return
	}
	n.lastLogged[key] = msg
	slog.WarnContext(ctx, "Peer network problem", "what", key, "error", err)
}

func truncateErr(err error) string {
	s := err.Error()
	if len(s) > maxListenErrLen {
		s = s[:maxListenErrLen]
	}
	return s
}

var openID = netid.Open()

// noteRefused reacts to a host refusing a request of this node: the member
// layer checks the signed revocation list at once, and when that shows the
// node was removed the network is dropped from peer traffic and its peers
// forgotten.
func (n *node) noteRefused(ctx context.Context, network netid.NetworkID) {
	if network.IsOpen() {
		return
	}
	n.mem.NoteRefused(ctx, network)
	row, err := n.mem.GetNetwork(ctx, network)
	if err != nil || row.RevokedAt == 0 {
		return
	}
	n.refresh(ctx)
	if err := forgetPeers(ctx, n.deps.Peers, network.String()); err != nil {
		n.logChange(ctx, "forget peers of removed network", err)
	}
}
