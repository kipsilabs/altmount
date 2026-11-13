package cmd

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/api"
	"github.com/kipsilabs/altmount/internal/peernet/hints"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/health"
	"github.com/kipsilabs/altmount/internal/metadata"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/records"
	"github.com/kipsilabs/altmount/internal/peernet/recsync"
	"github.com/kipsilabs/altmount/internal/peernet/service"
	"github.com/kipsilabs/altmount/internal/providerbackbone"
)

func TestResolvePeernetDataDir(t *testing.T) {
	cwd, _ := os.Getwd()
	abs := filepath.Join(t.TempDir(), "state")
	cases := []struct {
		name       string
		dbType     string
		dbPath     string
		metaRoot   string
		want       string
		wantErr    bool
		wantNotCWD bool
	}{
		{name: "sqlite absolute path", dbType: "sqlite", dbPath: filepath.Join(abs, "altmount.db"), want: abs},
		{name: "postgres keeps the same local-state anchor as the nzb store", dbType: "postgres", dbPath: filepath.Join(abs, "altmount.db"), want: abs},
		{name: "relative path with a directory becomes absolute", dbType: "sqlite", dbPath: "config/altmount.db", want: filepath.Join(cwd, "config")},
		{name: "bare file name is the working directory: use the metadata root", dbType: "sqlite", dbPath: "altmount.db",
			metaRoot: filepath.Join(abs, "meta"), want: filepath.Join(abs, "meta", peernetStateDirName), wantNotCWD: true},
		{name: "empty path uses the metadata root", dbType: "postgres", dbPath: "",
			metaRoot: filepath.Join(abs, "meta"), want: filepath.Join(abs, "meta", peernetStateDirName)},
		{name: "dot directory is the working directory", dbType: "sqlite", dbPath: "./altmount.db", metaRoot: "",
			wantErr: true},
		{name: "nothing usable", dbType: "postgres", dbPath: "", metaRoot: "", wantErr: true},
		{name: "metadata root dot", dbType: "sqlite", dbPath: "altmount.db", metaRoot: ".", wantErr: true},
	}
	for _, tc := range cases {
		cfg := &config.Config{}
		cfg.Database.Type, cfg.Database.Path, cfg.Metadata.RootPath = tc.dbType, tc.dbPath, tc.metaRoot
		got, err := resolvePeernetDataDir(cfg)
		if tc.wantErr {
			if err == nil || got != "" {
				t.Errorf("%s: got %q, %v; want an error", tc.name, got, err)
			}
			continue
		}
		if err != nil || got != tc.want || !filepath.IsAbs(got) {
			t.Errorf("%s: got %q, %v; want %q", tc.name, got, err, tc.want)
		}
		if tc.wantNotCWD && (got == cwd || filepath.Dir(got) == cwd) {
			t.Errorf("%s: resolved into the working directory: %q", tc.name, got)
		}
	}
}

func newPeernetTestDB(t *testing.T, dir string) *database.DB {
	t.Helper()
	db, err := database.NewDB(database.Config{DatabasePath: filepath.Join(dir, "altmount.db")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func peernetCfg(dir string, level config.PeernetLevel) *config.Config {
	cfg := config.DefaultConfig()
	on := true
	cfg.Peernet.Enabled = &on
	cfg.Peernet.OpenNetwork.Level = level
	cfg.Database.Path = filepath.Join(dir, "altmount.db")
	cfg.Metadata.RootPath = filepath.Join(dir, "meta")
	return cfg
}

// Record exchange must be live once the feature is enabled: the service has
// record storage, so a record signed by this node is accepted for publishing.
func TestStartPeernetServiceWiresRecordExchange(t *testing.T) {
	dir := t.TempDir()
	cfg := peernetCfg(dir, config.PeernetLevelShareMetadata)
	db := newPeernetTestDB(t, dir)
	mgr := config.NewManager(cfg, "")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	svc := startPeernetService(ctx, cfg, db, metadata.NewMetadataService(cfg.Metadata.RootPath), nil, mgr)
	defer svc.Stop(context.Background())

	signer, err := svc.Signer()
	if err != nil {
		t.Fatalf("service did not start: %v", err)
	}
	var rid records.ReleaseID
	rid[0] = 1
	raw, err := signer.BuildRelease(netid.Open(), records.ReleaseRecord{ReleaseID: rid, Title: "Some.Release", TotalSize: 10, FileCount: 1}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	out, err := svc.PublishLocal(ctx, netid.Open(), raw)
	if err != nil || out.Result != recsync.ResultStored {
		t.Fatalf("record exchange is not wired: %+v %v", out, err)
	}
}

func TestStartPeernetServiceDisabledStartsNothing(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.Database.Path = filepath.Join(dir, "altmount.db")
	db := newPeernetTestDB(t, dir)
	svc := startPeernetService(context.Background(), cfg, db, metadata.NewMetadataService(dir), nil, config.NewManager(cfg, ""))
	defer svc.Stop(context.Background())
	if _, err := svc.Signer(); err == nil {
		t.Fatal("a disabled feature must not have a signer (no key)")
	}
	if _, err := os.Stat(filepath.Join(dir, "peernet")); err == nil {
		t.Fatal("a disabled feature must not create its key directory")
	}
}

func TestPeernetBackboneSource(t *testing.T) {
	off := false
	cfg := config.DefaultConfig()
	cfg.Providers = []config.ProviderConfig{
		{Host: "a", StorageGroup: "Omicron"},
		{Host: "b", StorageGroup: "Eweka", Enabled: ptrBool(true)},
		{Host: "c", StorageGroup: "Disabled One", Enabled: &off},
		{Host: "d"},
	}
	cache := providerbackbone.NewCache("http://127.0.0.1:1/unused", time.Hour)
	src := newPeernetBackboneSource(func() *config.Config { return cfg }, cache, nil)

	got := src.ProviderGroups()
	if len(got) != 2 || got[0] != "Omicron" || got[1] != "Eweka" {
		t.Fatalf("provider groups = %v, want the raw groups of enabled providers only", got)
	}
	if len(src.Known()) != 0 {
		t.Fatal("known list must be empty until fetched, and reading it must not fetch")
	}

	// Live: a later config is seen without rebuilding.
	cfg = config.DefaultConfig()
	cfg.Providers = []config.ProviderConfig{{Host: "z", StorageGroup: "Highwinds"}}
	if got := src.ProviderGroups(); len(got) != 1 || got[0] != "Highwinds" {
		t.Fatalf("not live: %v", got)
	}
}

func TestPeernetBackboneRefreshUsesTheCacheAndProxyClient(t *testing.T) {
	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		_, _ = w.Write([]byte(`{"providers":[{"name":"N","server":[{"nntp":"news.example.com","backbone":"Omicron"}]}]}`))
	}))
	defer srv.Close()
	cfg := config.DefaultConfig()
	cache := providerbackbone.NewCache(srv.URL, time.Hour)
	src := newPeernetBackboneSource(func() *config.Config { return cfg }, cache, nil)
	if err := src.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := src.Refresh(context.Background()); err != nil || hits != 1 {
		t.Fatalf("a fresh list is not fetched again: hits=%d err=%v", hits, err)
	}
	if got := src.Known(); len(got) != 1 || got[0] != "Omicron" {
		t.Fatalf("known = %v", got)
	}
}

// Without a storage_group the backbone comes from the provider's host, by the
// same host mapping the provider form uses; an explicit group always wins.
func TestPeernetBackboneSourceDerivesTheBackboneFromTheHost(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"providers":[
			{"name":"Alpha","server":[{"nntp":"news.alpha.example","backbone":"Omicron"},{"nntp":"eu.alpha.example","backbone":"Omicron"}]},
			{"name":"Mixed","server":[{"nntp":"news.mixed.example","backbone":"Omicron"},{"nntp":"us.mixed.example","backbone":"Abavia"}]}]}`))
	}))
	defer srv.Close()
	cfg := config.DefaultConfig()
	cfg.Providers = []config.ProviderConfig{
		{Host: "news.alpha.example"},                         // exact
		{Host: "asia.alpha.example"},                         // regional host of a consistent domain
		{Host: "asia.mixed.example"},                         // inconsistent domain: no match
		{Host: "news.alpha.example", StorageGroup: "Custom"}, // explicit wins
		{Host: "news.unlisted.example"},                      // unknown
	}
	cache := providerbackbone.NewCache(srv.URL, time.Hour)
	src := newPeernetBackboneSource(func() *config.Config { return cfg }, cache, nil)
	if got := src.ProviderGroups(); len(got) != 1 || got[0] != "Custom" {
		t.Fatalf("before the dataset is loaded only explicit groups count: %v", got)
	}
	if err := src.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := src.ProviderGroups()
	want := []string{"Omicron", "Omicron", "Custom"}
	if len(got) != len(want) {
		t.Fatalf("groups = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("groups = %v, want %v", got, want)
		}
	}
}

func ptrBool(b bool) *bool { return &b }

func TestPeernetExclusionsAreReadLive(t *testing.T) {
	cfg := config.DefaultConfig()
	ex := peernetExclusions(func() *config.Config { return cfg })
	if got := ex(); len(got.Categories) != 0 || len(got.Indexers) != 0 {
		t.Fatalf("default: %+v", got)
	}
	cfg = config.DefaultConfig()
	cfg.Peernet.Sharing.ExcludedCategories = []string{"private"}
	cfg.Peernet.Sharing.ExcludedIndexers = []string{"Idx"}
	got := ex()
	if len(got.Categories) != 1 || got.Categories[0] != "private" || got.Indexers[0] != "Idx" {
		t.Fatalf("not live: %+v", got)
	}
	// The result is a copy: editing it must not change the configuration.
	got.Categories[0] = "tampered"
	if cfg.Peernet.Sharing.ExcludedCategories[0] != "private" {
		t.Fatal("exclusions alias the configuration")
	}
}

type fakeObserverTarget struct{ obs health.HealthyObserver }

func (f *fakeObserverTarget) SetHealthyObserver(o health.HealthyObserver) { f.obs = o }

type fakeNotifier struct{ got []service.HealthyNotice }

func (f *fakeNotifier) NotifyHealthy(n service.HealthyNotice) { f.got = append(f.got, n) }

func TestHealthObserverIsNilWhileDisabledAndFollowsTheConfig(t *testing.T) {
	target := &fakeObserverTarget{}
	n := &fakeNotifier{}
	var cb config.ChangeCallback
	register := func(c config.ChangeCallback) { cb = c }

	off := config.DefaultConfig()
	wirePeernetHealthObserver(target, off, n, register)
	if target.obs != nil {
		t.Fatal("disabled feature: the health worker must have no observer")
	}

	on := peernetCfg(t.TempDir(), config.PeernetLevelShareMetadata)
	cb(off, on)
	if target.obs == nil {
		t.Fatal("enabling the feature must install the observer")
	}
	target.obs.FileHealthy(health.HealthyNotice{FilePath: "a/b.mkv", CheckedAt: time.Unix(5, 0),
		Stats: health.CheckStats{TotalSegments: 9, SegmentsChecked: 4, MediaHeaderVerified: true, PatchedByPAR2: true, KnownHoles: true, FullCheck: false}})
	if len(n.got) != 1 || n.got[0].FilePath != "a/b.mkv" || n.got[0].TotalSegments != 9 || n.got[0].SegmentsChecked != 4 ||
		!n.got[0].MediaHeaderVerified || !n.got[0].PatchedByPAR2 || !n.got[0].KnownHoles || n.got[0].FullCheck {
		t.Fatalf("converted notice: %+v", n.got)
	}

	cb(on, off)
	if target.obs != nil {
		t.Fatal("disabling the feature must remove the observer")
	}

	enabled := peernetCfg(t.TempDir(), config.PeernetLevelOff)
	target2 := &fakeObserverTarget{}
	wirePeernetHealthObserver(target2, enabled, n, register)
	if target2.obs == nil {
		t.Fatal("feature enabled at start: observer installed")
	}
}

type fakeHintsHost struct {
	idMu     sync.Mutex
	id       string
	sources  []service.HintSources
	clears   int
	networks []hints.Network
}

func (f *fakeHintsHost) HealthAdvice(context.Context, string) hints.Advice { return hints.Advice{} }
func (f *fakeHintsHost) NotePeerAdviceApplied(hints.AdviceKind)            {}
func (f *fakeHintsHost) NetworkHealthyPaths(context.Context, []string) map[string]bool {
	return nil
}
func (f *fakeHintsHost) SetHintSources(s service.HintSources) { f.sources = append(f.sources, s) }
func (f *fakeHintsHost) ClearHintSources()                    { f.clears++ }
func (f *fakeHintsHost) ReceivingNetworks() []hints.Network   { return f.networks }
func (f *fakeHintsHost) NodeID() string {
	f.idMu.Lock()
	defer f.idMu.Unlock()
	return f.id
}
func (f *fakeHintsHost) setID(id string) {
	f.idMu.Lock()
	f.id = id
	f.idMu.Unlock()
}

type fakeHealthTarget struct{ adviser health.PeerAdviser }

func (f *fakeHealthTarget) SetPeerAdviser(a health.PeerAdviser) { f.adviser = a }
func (f *fakeHealthTarget) ScheduleCheckNow(context.Context, string) (bool, error) {
	return false, nil
}

type fakeLabelTarget struct{ l api.StremioNetworkLabeler }

func (f *fakeLabelTarget) SetStremioNetworkLabeler(l api.StremioNetworkLabeler) { f.l = l }

func TestPeernetConsumersFollowTheFeatureAndShareOneAdvisorAndBudget(t *testing.T) {
	dir := t.TempDir()
	db := newPeernetTestDB(t, dir)
	conn, d := db.Connection(), db.Dialect()
	host := &fakeHintsHost{id: strings.Repeat("a", 64)}
	ht, lt := &fakeHealthTarget{}, &fakeLabelTarget{}
	off := config.DefaultConfig()
	on := peernetCfg(dir, config.PeernetLevelReceiveOnly)
	cur := off
	var cb config.ChangeCallback
	c := newPeernetConsumers(host, ht, lt, func() *config.Config { return cur },
		newPeernetBackboneSource(func() *config.Config { return cur }, providerbackbone.Default, nil),
		database.NewPeernetRecordRepository(conn, d), database.NewPeernetPeerRepository(conn, d))
	wirePeernetConsumers(c, off, func(f config.ChangeCallback) { cb = f })

	if ht.adviser != nil || lt.l != nil || len(host.sources) != 0 {
		t.Fatal("disabled feature: no adviser, no labeler, no sources, no advisor built")
	}
	if c.advisor != nil || c.budget != nil {
		t.Fatal("the advisor and budget must not be built while the feature is off")
	}

	cur = on
	cb(off, on)
	if ht.adviser == nil || lt.l == nil || len(host.sources) != 1 {
		t.Fatal("enabling must install every consumer")
	}
	first := host.sources[0]
	if first.Advisor == nil || first.Budget == nil || first.Scheduler == nil || first.Backbones == nil {
		t.Fatalf("incomplete sources: %+v", first)
	}

	cb(on, off)
	if ht.adviser != nil || lt.l != nil || host.clears == 0 {
		t.Fatal("disabling must remove every consumer")
	}

	cb(off, on)
	if len(host.sources) != 2 || host.sources[1].Advisor != first.Advisor || host.sources[1].Budget != first.Budget {
		t.Fatal("exactly one advisor and one budget, reused across enables")
	}
}

func TestPeernetConsumersWaitForTheNodeID(t *testing.T) {
	dir := t.TempDir()
	db := newPeernetTestDB(t, dir)
	conn, d := db.Connection(), db.Dialect()
	host := &fakeHintsHost{}
	ht, lt := &fakeHealthTarget{}, &fakeLabelTarget{}
	on := peernetCfg(dir, config.PeernetLevelReceiveOnly)
	c := newPeernetConsumers(host, ht, lt, func() *config.Config { return on },
		newPeernetBackboneSource(func() *config.Config { return on }, providerbackbone.Default, nil),
		database.NewPeernetRecordRepository(conn, d), database.NewPeernetPeerRepository(conn, d))
	c.apply(on)
	if c.advisor != nil || ht.adviser != nil {
		t.Fatal("an advisor must not be built before the node has an ID")
	}
	c.waiting.Store(false)
}

func TestPeernetLocalBackbonesUseThePublishersNormalisation(t *testing.T) {
	src := &fakeBackboneSrc{provider: []string{"  Omicron ", "made up group"}, known: []string{"Omicron"}}
	c := &peernetConsumers{backbones: src}
	got := c.localBackbones()
	if len(got) != 1 || got[0] != "omicron" {
		t.Fatalf("backbones = %v", got)
	}
}

type fakeBackboneSrc struct{ provider, known []string }

func (f *fakeBackboneSrc) ProviderGroups() []string      { return f.provider }
func (f *fakeBackboneSrc) Known() []string               { return f.known }
func (f *fakeBackboneSrc) Refresh(context.Context) error { return nil }

// The periodic re-announce reads local health through Deps.Health; without it
// the feature never re-announces.
func TestPeernetDepsWiresLocalHealth(t *testing.T) {
	dir := t.TempDir()
	cfg := peernetCfg(dir, config.PeernetLevelShareMetadata)
	db := newPeernetTestDB(t, dir)
	mgr := config.NewManager(cfg, "")
	ms := metadata.NewMetadataService(cfg.Metadata.RootPath)

	deps := peernetDeps(context.Background(), cfg, db, ms, database.NewHealthRepository(db.Connection(), db.Dialect()), mgr)
	if deps.Health == nil {
		t.Fatal("Deps.Health must be set so re-announce can run")
	}
	if got := peernetDeps(context.Background(), cfg, db, ms, nil, mgr).Health; got != nil {
		t.Fatal("no health repository means no health lookup")
	}
}

// newWaiterConsumers builds consumers over a host that has no node ID yet.
func newWaiterConsumers(t *testing.T, cur func() *config.Config) (*peernetConsumers, *fakeHintsHost, *fakeHealthTarget, *fakeLabelTarget) {
	t.Helper()
	dir := t.TempDir()
	db := newPeernetTestDB(t, dir)
	conn, d := db.Connection(), db.Dialect()
	host := &fakeHintsHost{}
	ht, lt := &fakeHealthTarget{}, &fakeLabelTarget{}
	c := newPeernetConsumers(host, ht, lt, cur,
		newPeernetBackboneSource(cur, providerbackbone.Default, nil),
		database.NewPeernetRecordRepository(conn, d), database.NewPeernetPeerRepository(conn, d))
	c.waitStep, c.waitSteps = 5*time.Millisecond, 400
	return c, host, ht, lt
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestPeernetWaiterStopsWithItsContext(t *testing.T) {
	on := peernetCfg(t.TempDir(), config.PeernetLevelReceiveOnly)
	c, _, _, _ := newWaiterConsumers(t, func() *config.Config { return on })
	ctx, cancel := context.WithCancel(context.Background())
	c.ctx = ctx
	c.waitStep, c.waitSteps = time.Hour, 10 // would block a sleeping loop for hours
	c.apply(on)
	waitUntil(t, "waiter to start", func() bool { return c.waiting.Load() })
	cancel()
	waitUntil(t, "waiter to stop on cancel", func() bool { return !c.waiting.Load() })
}

// A waiter must install from the newest configuration it was given, never
// from a stale one: the feature was switched off while it waited.
func TestPeernetWaiterNeverInstallsAfterDisable(t *testing.T) {
	on := peernetCfg(t.TempDir(), config.PeernetLevelReceiveOnly)
	off := config.DefaultConfig()
	// The live getter still reports the enabled configuration (stale).
	c, host, ht, lt := newWaiterConsumers(t, func() *config.Config { return on })
	c.apply(on)
	waitUntil(t, "waiter to start", func() bool { return c.waiting.Load() })
	c.apply(off) // the change callback: feature disabled while waiting
	host.setID(strings.Repeat("a", 64))
	waitUntil(t, "waiter to finish", func() bool { return !c.waiting.Load() })
	if ht.adviser != nil || lt.l != nil || len(host.sources) != 0 {
		t.Fatal("a disabled feature must not be re-installed by the waiter")
	}
}

func TestPeernetWaiterGivesUpWithOneWarningAndALaterChangeRetries(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(prev)

	on := peernetCfg(t.TempDir(), config.PeernetLevelReceiveOnly)
	c, host, ht, _ := newWaiterConsumers(t, func() *config.Config { return on })
	c.waitSteps = 3
	c.apply(on)
	waitUntil(t, "waiter to give up", func() bool { return !c.waiting.Load() })
	if n := strings.Count(buf.String(), "never became ready"); n != 1 {
		t.Fatalf("want exactly one give-up warning, got %d in %q", n, buf.String())
	}

	host.setID(strings.Repeat("a", 64))
	c.apply(on) // a later configuration change retries
	if ht.adviser == nil || len(host.sources) != 1 {
		t.Fatal("a later change must retry and install")
	}
}

// lateHost reports no node ID for its first `after` NodeID calls and the ID
// afterwards, so the node becomes ready exactly after the waiter's last poll.
type lateHost struct {
	*fakeHintsHost
	calls atomic.Int32
	after int32
}

func (l *lateHost) NodeID() string {
	if l.calls.Add(1) <= l.after {
		return ""
	}
	return strings.Repeat("a", 64)
}

// The node becoming ready right after the last poll must not leave the
// consumers uninstalled until the next configuration change.
func TestPeernetWaiterClosesTheLastIterationWindow(t *testing.T) {
	on := peernetCfg(t.TempDir(), config.PeernetLevelReceiveOnly)
	c, base, ht, _ := newWaiterConsumers(t, func() *config.Config { return on })
	c.waitSteps = 3
	// apply polls once, the waiter three times; the fifth call is ready.
	c.host = &lateHost{fakeHintsHost: base, after: 4}
	c.apply(on)
	waitUntil(t, "waiter to finish", func() bool { return !c.waiting.Load() })
	c.mu.Lock()
	installed := ht.adviser != nil && len(base.sources) == 1
	c.mu.Unlock()
	if !installed {
		t.Fatal("the waiter gave up although the node became ready right after its last poll")
	}
}

func TestPeernetConsumersRetryAFailedBuild(t *testing.T) {
	on := peernetCfg(t.TempDir(), config.PeernetLevelReceiveOnly)
	c, host, ht, _ := newWaiterConsumers(t, func() *config.Config { return on })
	host.id = strings.Repeat("a", 64)
	good := c.store
	c.store = nil // hints.New refuses a nil store
	c.apply(on)
	if ht.adviser != nil {
		t.Fatal("a failed build must install nothing")
	}
	c.store = good
	c.apply(on)
	if ht.adviser == nil || len(host.sources) != 1 {
		t.Fatal("a failed build must be retried on the next apply")
	}
}
