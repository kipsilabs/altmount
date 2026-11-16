package service

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
)

func enabledSettings() Settings { return Settings{Enabled: true, ListenPort: 0} }

func TestDisabledCreatesNothing(t *testing.T) {
	i := newInstance(t, Settings{})
	before := runtime.NumGoroutine()
	i.start(t)

	entries, _ := os.ReadDir(i.dataDir)
	for _, e := range entries {
		if e.Name() == "peernet" {
			t.Fatalf("peernet directory created while disabled")
		}
	}
	if i.meta.sets != 0 || i.meta.hasObserver() {
		t.Fatal("observer touched while disabled")
	}
	if i.svc.currentNode() != nil {
		t.Fatal("node running while disabled")
	}
	st, err := i.svc.Status(context.Background())
	if err != nil || st.Enabled || st.NodeID != "" || len(st.Networks) != 0 {
		t.Fatalf("status = %+v err=%v", st, err)
	}
	if after := runtime.NumGoroutine(); after > before {
		t.Fatalf("goroutines grew %d -> %d", before, after)
	}
	if _, err := i.svc.CreateNetwork(context.Background(), "x"); err != ErrDisabled {
		t.Fatalf("CreateNetwork err = %v", err)
	}
}

func TestEnableCreatesOwnerOnlyIdentityNoListener(t *testing.T) {
	i := newInstance(t, enabledSettings())
	i.start(t)
	st, _ := i.svc.Status(context.Background())
	if len(st.NodeID) != 64 || st.Listening || st.AcceptIncoming {
		t.Fatalf("status = %+v", st)
	}
	dir := filepath.Join(i.dataDir, "peernet", "identity")
	di, err := os.Stat(dir)
	if err != nil || di.Mode().Perm() != 0o700 {
		t.Fatalf("identity dir: %v %v", err, di)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Fatalf("entries = %d", len(entries))
	}
	fi, _ := entries[0].Info()
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("key mode = %v", fi.Mode().Perm())
	}
	if !i.meta.hasObserver() {
		t.Fatal("observer not set")
	}
	// Same identity after a restart.
	id := st.NodeID
	i.svc.Apply(context.Background(), Settings{})
	i.svc.Apply(context.Background(), enabledSettings())
	st2, _ := i.svc.Status(context.Background())
	if st2.NodeID != id {
		t.Fatal("node id changed across restart")
	}
}

func TestListenerOnlyWithAcceptIncoming(t *testing.T) {
	i := newInstance(t, Settings{Enabled: true, AcceptIncoming: true})
	i.start(t)
	port := i.port(t)
	c, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = c.Close()
	i.svc.Apply(context.Background(), Settings{Enabled: true, AcceptIncoming: false})
	st, _ := i.svc.Status(context.Background())
	if st.Listening {
		t.Fatal("still listening")
	}
	eventually(t, "port closed", func() bool {
		c, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), 200*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return false
		}
		return true
	})
}

func TestPortInUseReportedAndRecovers(t *testing.T) {
	blocker, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := blocker.Addr().(*net.TCPAddr).Port
	set := Settings{Enabled: true, AcceptIncoming: true, ListenPort: port, OpenLevel: 1}
	i := newInstance(t, set)
	i.start(t)
	st, err := i.svc.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.Listening || st.ListenError == "" || st.NodeID == "" || st.ListenPort != port {
		t.Fatalf("status = %+v", st)
	}
	if len(st.Networks) == 0 {
		t.Fatal("rest of the service did not start")
	}
	_ = blocker.Close()
	i.svc.Apply(context.Background(), set) // asks the loop to retry the listener now
	eventually(t, "listener recovery", func() bool {
		st, _ := i.svc.Status(context.Background())
		return st.Listening && st.ListenError == ""
	})
}

// stableGoroutines polls (under a deadline) until the goroutine count stops
// changing and returns it.
func stableGoroutines(t *testing.T) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	last, same := -1, 0
	for time.Now().Before(deadline) {
		cur := runtime.NumGoroutine()
		if cur == last {
			if same++; same >= 5 {
				return cur
			}
		} else {
			last, same = cur, 0
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("goroutine count never settled (last %d)", last)
	return last
}

func settleGoroutines(t *testing.T, limit int) {
	t.Helper()
	eventually(t, "goroutines to settle", func() bool { return runtime.NumGoroutine() <= limit })
}

func TestEnableDisableCyclesLeakNothing(t *testing.T) {
	i := newInstance(t, Settings{})
	i.start(t)
	// Warm up once so lazily started runtime goroutines are in the baseline.
	on := Settings{Enabled: true, AcceptIncoming: true, OpenLevel: 1, OpenPeers: []string{"127.0.0.1:9"}}
	i.svc.Apply(context.Background(), on)
	port := i.port(t)
	i.svc.Apply(context.Background(), Settings{})
	base := stableGoroutines(t)

	for n := 0; n < 5; n++ {
		on.ListenPort = 0
		i.svc.Apply(context.Background(), on)
		p := i.port(t)
		if !i.meta.hasObserver() {
			t.Fatal("observer missing when enabled")
		}
		i.svc.Apply(context.Background(), Settings{})
		if i.meta.hasObserver() {
			t.Fatal("observer left behind after disable")
		}
		eventually(t, "listener closed", func() bool {
			c, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(p)), 200*time.Millisecond)
			if err == nil {
				_ = c.Close()
				return false
			}
			return true
		})
	}
	_ = port
	settleGoroutines(t, base)
}

func TestConfigChangeUpdatesOpenNetwork(t *testing.T) {
	i := newInstance(t, Settings{Enabled: true})
	i.start(t)
	n := i.node(t)
	open := netid.Open().String()
	if _, ok := n.active[open]; ok {
		t.Fatal("open network active at level 0")
	}
	i.svc.Apply(context.Background(), Settings{Enabled: true, OpenLevel: 1, OpenPeers: []string{"127.0.0.1:9", "127.0.0.1:10"}})
	peers := func() int {
		ps, err := i.svc.Peers(context.Background(), netid.Open())
		if err != nil {
			t.Fatal(err)
		}
		return len(ps)
	}
	eventually(t, "two unresolved peers", func() bool { return peers() == 2 })
	i.svc.Apply(context.Background(), Settings{Enabled: true, OpenLevel: 1, OpenPeers: []string{"127.0.0.1:9"}})
	eventually(t, "one peer", func() bool { return peers() == 1 })
	st, _ := i.svc.Status(context.Background())
	var open1 NetworkStatus
	for _, ns := range st.Networks {
		if ns.Kind == KindOpen {
			open1 = ns
		}
	}
	if open1.Level != 1 || open1.Peers.Unresolved != 1 {
		t.Fatalf("open status = %+v", open1)
	}
	i.svc.Apply(context.Background(), Settings{Enabled: true, OpenLevel: 0})
	eventually(t, "inactive", func() bool {
		n.mu.Lock()
		defer n.mu.Unlock()
		_, ok := n.active[open]
		return !ok
	})
}

func TestOpenLevelComesFromConfigNotDatabase(t *testing.T) {
	i := newInstance(t, Settings{Enabled: true, OpenLevel: 0})
	i.start(t)
	n := i.node(t)
	ctx := context.Background()
	open := netid.Open()
	// Row says 3, config says 0.
	if err := i.nets.SetExposureLevel(ctx, open.String(), 3, time.Now().Unix()); err != nil {
		t.Fatal(err)
	}
	n.syncNetworks(ctx)
	if _, ok := n.active[open.String()]; ok {
		t.Fatal("open network joined because of the database row")
	}
	st, _ := i.svc.Status(ctx)
	for _, ns := range st.Networks {
		if ns.Kind == KindOpen && ns.Level != 0 {
			t.Fatalf("status level = %d", ns.Level)
		}
	}
	// Row says 0, config says 2.
	if err := i.nets.SetExposureLevel(ctx, open.String(), 0, time.Now().Unix()); err != nil {
		t.Fatal(err)
	}
	i.svc.Apply(ctx, Settings{Enabled: true, OpenLevel: 2})
	if _, ok := n.active[open.String()]; !ok {
		t.Fatal("open network not joined despite config level 2")
	}
	st, _ = i.svc.Status(ctx)
	for _, ns := range st.Networks {
		if ns.Kind == KindOpen && ns.Level != 2 {
			t.Fatalf("status level = %d", ns.Level)
		}
	}
}

func TestSettingsFromConfig(t *testing.T) {
	cfg := config.DefaultConfig()
	if s := SettingsFromConfig(cfg); s.Enabled || s.OpenLevel != 0 {
		t.Fatalf("default = %+v", s)
	}
	on := true
	cfg.Peernet.Enabled = &on
	cfg.Peernet.AcceptIncoming = true
	cfg.Peernet.OpenNetwork.Level = 2
	cfg.Peernet.OpenNetwork.Peers = []string{"a.example:1"}
	cfg.Network.HTTPSProxy = "http://p:1"
	s := SettingsFromConfig(cfg)
	if !s.Enabled || !s.AcceptIncoming || s.OpenLevel != 2 || len(s.OpenPeers) != 1 || s.HTTPSProxy != "http://p:1" {
		t.Fatalf("settings = %+v", s)
	}
	if SettingsFromConfig(nil).Enabled {
		t.Fatal("nil config enabled")
	}
	i := newInstance(t, Settings{})
	i.start(t)
	i.svc.OnConfigChange(nil, cfg) // applied in the background
	eventually(t, "config change to start the service", func() bool { return i.svc.currentNode() != nil })
}

func TestIdentityFailureReported(t *testing.T) {
	i := newInstance(t, enabledSettings())
	dir := filepath.Join(i.dataDir, "peernet", "identity")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	i.start(t)
	// A group/world-accessible key directory must not be used silently.
	st, _ := i.svc.Status(context.Background())
	if i.svc.currentNode() != nil || st.Error == "" {
		t.Fatal("failure not reported")
	}
}
