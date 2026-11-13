package service

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
)

func configWith(mutate func(*config.Config)) *config.Config {
	cfg := config.DefaultConfig()
	on := true
	cfg.Peernet.Enabled = &on
	if mutate != nil {
		mutate(cfg)
	}
	return cfg
}

// Item 8a: nothing the peer network depends on changed.
func TestOnConfigChangeIgnoresUnrelatedChanges(t *testing.T) {
	blocker, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	port := blocker.Addr().(*net.TCPAddr).Port
	set := Settings{Enabled: true, AcceptIncoming: true, ListenPort: port, OpenLevel: 1}
	i := newInstance(t, set)
	var applies atomic.Int64
	i.svc.deps.applyHook = func(Settings) { applies.Add(1) }
	i.start(t)
	n := i.node(t)
	if _, listenErr := n.listenState(); listenErr == "" {
		t.Fatal("precondition: the listener must have failed to bind")
	}
	applies.Store(0)

	oldCfg := configWith(func(c *config.Config) {
		c.Peernet.AcceptIncoming = true
		c.Peernet.ListenPort = port
		c.Peernet.OpenNetwork.Level = 1
	})
	newCfg := configWith(func(c *config.Config) {
		c.Peernet.AcceptIncoming = true
		c.Peernet.ListenPort = port
		c.Peernet.OpenNetwork.Level = 1
		c.Log.Level = "debug" // unrelated
	})
	i.svc.OnConfigChange(oldCfg, newCfg)
	i.svc.asyncWG.Wait()
	if applies.Load() != 0 {
		t.Fatal("an unrelated settings save reached the peer network")
	}
	if i.svc.currentNode() != n {
		t.Fatal("the running node was replaced by an unrelated settings save")
	}
}

// Item 8c: apply asynchronously, serialised, last write wins; Stop waits.
func TestOnConfigChangeAppliesInBackgroundLastWriteWins(t *testing.T) {
	i := newInstance(t, Settings{})
	release := make(chan struct{})
	entered := make(chan struct{}, 8)
	var mu sync.Mutex
	var seen []int
	i.svc.deps.applyHook = func(s Settings) {
		mu.Lock()
		seen = append(seen, s.OpenLevel)
		first := len(seen) == 1
		mu.Unlock()
		if first {
			entered <- struct{}{}
			<-release
		}
	}
	i.start(t)
	off := configWith(nil)
	off.Peernet.Enabled = nil
	levels := func(l int) *config.Config {
		return configWith(func(c *config.Config) { c.Peernet.OpenNetwork.Level = config.PeernetLevel(l) })
	}

	returned := make(chan struct{})
	go func() {
		i.svc.OnConfigChange(off, levels(1))
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("OnConfigChange did not return immediately")
	}
	<-entered
	// While the first apply is blocked, queue two more: only the newest runs.
	i.svc.OnConfigChange(levels(1), levels(2))
	i.svc.OnConfigChange(levels(2), levels(3))
	stopped := make(chan struct{})
	go func() { i.svc.Stop(context.Background()); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("Stop did not wait for the pending configuration change")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop never finished")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 || seen[0] != 1 || seen[1] != 3 {
		t.Fatalf("applied levels = %v, want [1 3]", seen)
	}
}

// Item 8b: a failed bind is retried by the loop, rebinding only the listener.
func TestListenerRebindRetriedWithoutRestartingNode(t *testing.T) {
	blocker, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := blocker.Addr().(*net.TCPAddr).Port
	i := newInstance(t, Settings{Enabled: true, AcceptIncoming: true, ListenPort: port})
	i.svc.deps.listenRetryBase = 20 * time.Millisecond
	i.start(t)
	n := i.node(t)
	if _, e := n.listenState(); e == "" {
		t.Fatal("expected a bind error")
	}
	st, _ := i.svc.Status(context.Background())
	if st.ListenError == "" || st.Listening {
		t.Fatalf("error must stay visible while the bind keeps failing: %+v", st)
	}
	time.Sleep(60 * time.Millisecond) // a retry happens while it is still blocked
	n.kick <- struct{}{}
	_ = blocker.Close()
	eventually(t, "listener to come up", func() bool {
		select {
		case n.kick <- struct{}{}:
		default:
		}
		st, _ := i.svc.Status(context.Background())
		return st.Listening && st.ListenError == ""
	})
	if i.svc.currentNode() != n {
		t.Fatal("the whole node was restarted to rebind the listener")
	}
}

// Item 8d.
func TestRoutesRegisteredBeforeListenerAccepts(t *testing.T) {
	i := newInstance(t, Settings{Enabled: true, AcceptIncoming: true})
	var calls, bad atomic.Int64
	i.svc.deps.beforeListen = func(routes, bound bool) {
		calls.Add(1)
		if !routes || bound {
			bad.Add(1)
		}
	}
	i.start(t)
	if calls.Load() != 1 || bad.Load() != 0 {
		t.Fatalf("hook calls=%d violations=%d", calls.Load(), bad.Load())
	}
}

// Item 2: membership upkeep has its own schedule.
func TestMembershipUpkeepIsIndependentOfDiscoveryLoop(t *testing.T) {
	i := newInstance(t, Settings{Enabled: true})
	i.svc.deps.MaintainInterval = time.Hour
	i.start(t)
	n := i.node(t)
	eventually(t, "first upkeep pass", func() bool { return n.maintRuns.Load() >= 1 })
	before := n.maintRuns.Load()
	for k := 0; k < 5; k++ {
		n.step(context.Background())
	}
	if n.maintRuns.Load() != before {
		t.Fatal("a discovery round ran the membership upkeep")
	}
	n.kickMaintenance()
	eventually(t, "kicked upkeep pass", func() bool { return n.maintRuns.Load() > before })
}

func TestUpkeepStopsWithTheNode(t *testing.T) {
	i := newInstance(t, Settings{Enabled: true})
	i.svc.deps.MaintainInterval = 10 * time.Millisecond
	i.start(t)
	n := i.node(t)
	eventually(t, "upkeep passes", func() bool { return n.maintRuns.Load() >= 3 })
	i.svc.Apply(context.Background(), Settings{})
	after := n.maintRuns.Load()
	time.Sleep(60 * time.Millisecond)
	if n.maintRuns.Load() != after {
		t.Fatal("upkeep kept running after the feature was disabled")
	}
}

// Item 6: the peer list counts as reconciled only after a success.
func TestReconcileOpenRetriesAfterFailure(t *testing.T) {
	i := newInstance(t, Settings{Enabled: true, OpenLevel: 1, OpenPeers: []string{"127.0.0.1:9"}})
	i.start(t)
	n := i.node(t)
	eventually(t, "the first reconcile", func() bool {
		n.mu.Lock()
		defer n.mu.Unlock()
		return n.reconciledValid
	})
	var calls atomic.Int64
	fail := atomic.Bool{}
	fail.Store(true)
	n.mu.Lock()
	n.reconcileFn = func(context.Context, netid.NetworkID, []string) error {
		calls.Add(1)
		if fail.Load() {
			return errors.New("transient")
		}
		return nil
	}
	n.reconciledValid = false
	n.mu.Unlock()
	ctx := context.Background()
	n.reconcileOpen(ctx)
	n.reconcileOpen(ctx)
	if calls.Load() != 2 {
		t.Fatalf("calls = %d, a failed reconcile must be retried", calls.Load())
	}
	fail.Store(false)
	n.reconcileOpen(ctx)
	n.reconcileOpen(ctx)
	if calls.Load() != 3 {
		t.Fatalf("calls = %d, a successful reconcile must not repeat", calls.Load())
	}
}

// Item 7: syncNetworks holds its own mutex across read and apply.
func TestSyncNetworksIsSerialised(t *testing.T) {
	i := newInstance(t, Settings{Enabled: true, OpenLevel: 1})
	i.start(t)
	n := i.node(t)
	n.syncMu.Lock()
	done := make(chan struct{})
	go func() { n.syncNetworks(context.Background()); close(done) }()
	select {
	case <-done:
		t.Fatal("syncNetworks ran while another sync held the lock")
	case <-time.After(100 * time.Millisecond):
	}
	n.syncMu.Unlock()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("syncNetworks never ran")
	}
	var wg sync.WaitGroup
	for k := 0; k < 8; k++ {
		wg.Add(1)
		go func() { defer wg.Done(); n.syncNetworks(context.Background()) }()
	}
	wg.Wait()
}

// Item 15.
type flakyPeers struct {
	rows    []database.PeernetPeer
	failFor string
	deleted []string
}

func (f *flakyPeers) List(context.Context, string) ([]database.PeernetPeer, error) {
	return f.rows, nil
}
func (f *flakyPeers) Delete(_ context.Context, _, node string) error {
	if node == f.failFor {
		return errors.New("boom " + node)
	}
	f.deleted = append(f.deleted, node)
	return nil
}

func TestForgetPeersContinuesPastErrors(t *testing.T) {
	f := &flakyPeers{failFor: "n1", rows: []database.PeernetPeer{{NodeID: "n1"}, {NodeID: "n2"}, {NodeID: "n3"}}}
	err := forgetPeers(context.Background(), f, "net")
	if err == nil || err.Error() != "delete peer: boom n1" {
		t.Fatalf("first error not reported: %v", err)
	}
	if len(f.deleted) != 2 {
		t.Fatalf("deleted %v, the cleanup stopped at the first error", f.deleted)
	}
}

func TestOrphanPeersAreCleanedOnUpkeep(t *testing.T) {
	i := newInstance(t, Settings{Enabled: true})
	i.start(t)
	n := i.node(t)
	nw := netid.FromRootKey(make([]byte, 32)).String()
	if _, err := i.db.Connection().Exec(`INSERT INTO peernet_peers
		(network_id, node_id, address, source, first_seen, last_seen) VALUES (?, 'x1', 'h:1', 'user', 1, 1),
		(?, 'x2', 'h:2', 'user', 1, 1)`, nw, nw); err != nil {
		t.Fatal(err)
	}
	n.maintain(context.Background())
	ids, err := i.nets.ListOrphanPeerNetworkIDs(context.Background())
	if err != nil || len(ids) != 0 {
		t.Fatalf("orphans left: %v %v", ids, err)
	}
}

// Item 11.
func TestStartRefusesWithoutSafeDataDir(t *testing.T) {
	for _, dir := range []string{"", "relative/dir"} {
		i := newInstance(t, Settings{Enabled: true})
		i.svc.deps.DataDir = dir
		cwd, _ := os.Getwd()
		before, _ := os.ReadDir(cwd)
		i.start(t)
		st, _ := i.svc.Status(context.Background())
		if i.svc.currentNode() != nil || st.Error == "" {
			t.Fatalf("DataDir %q: started or no error: %+v", dir, st)
		}
		after, _ := os.ReadDir(cwd)
		if len(after) != len(before) {
			t.Fatalf("DataDir %q: files appeared in the working directory", dir)
		}
		if _, err := os.Stat(filepath.Join(cwd, "relative")); err == nil {
			t.Fatal("keys written relative to the working directory")
		}
	}
}

// Item 4: the observer is hooked with a bounded asynchronous queue.
func TestObserverIsAsynchronousAndDetachedOnDisable(t *testing.T) {
	i := newInstance(t, Settings{Enabled: true})
	i.start(t)
	if !i.meta.hasObserver() {
		t.Fatal("observer missing")
	}
	i.svc.Apply(context.Background(), Settings{})
	if i.meta.hasObserver() {
		t.Fatal("observer left attached")
	}
}

// A change arriving once shutdown has begun must not start a worker that Stop
// is no longer waiting for.
func TestOnConfigChangeAfterStopStartsNothing(t *testing.T) {
	i := newInstance(t, Settings{Enabled: false})
	var applies atomic.Int64
	i.svc.deps.applyHook = func(Settings) { applies.Add(1) }
	i.start(t)
	i.svc.Stop(context.Background())

	off := configWith(func(c *config.Config) {})
	on := configWith(func(c *config.Config) { c.Peernet.OpenNetwork.Level = 1 })
	i.svc.OnConfigChange(off, on)
	i.svc.asyncWG.Wait()
	if applies.Load() != 0 {
		t.Fatal("a configuration change after Stop was applied")
	}
}
