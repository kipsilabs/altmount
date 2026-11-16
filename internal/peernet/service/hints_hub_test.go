package service

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/hints"
)

type fakeAdvisor struct {
	mu      sync.Mutex
	advice  hints.Advice
	err     error
	delay   time.Duration
	healthy map[string]hints.NetworkHealth
	calls   int
	nhCalls int
	gotBB   []string
	spends  int
	denied  bool // SpendRecheck refuses (budget gone)
	// gotRel and gotReleases are what the last advice / label call was asked.
	gotRel      string
	gotReleases map[string][]string
}

func (f *fakeAdvisor) SpendRecheck(_, _ string, _, _ time.Time) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.spends++
	return !f.denied
}

func (f *fakeAdvisor) spendCount() int { f.mu.Lock(); defer f.mu.Unlock(); return f.spends }

func (f *fakeAdvisor) HealthAdvice(ctx context.Context, rel, file string, bb []string, _ time.Time) (hints.Advice, error) {
	f.mu.Lock()
	f.calls++
	f.gotBB, f.gotRel = bb, rel
	d, a, err := f.delay, f.advice, f.err
	f.mu.Unlock()
	if d > 0 {
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return hints.Advice{}, ctx.Err()
		}
	}
	return a, err
}

func (f *fakeAdvisor) NetworkHealthy(_ context.Context, ids ...string) (map[string]hints.NetworkHealth, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nhCalls++
	if f.err != nil {
		return nil, f.err
	}
	out := map[string]hints.NetworkHealth{}
	for _, id := range ids {
		out[id] = f.healthy[id]
	}
	return out, nil
}

func (f *fakeAdvisor) NetworkHealthyFiles(_ context.Context, releases map[string][]string) (map[string]hints.NetworkHealth, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nhCalls++
	f.gotReleases = releases
	if f.err != nil {
		return nil, f.err
	}
	out := map[string]hints.NetworkHealth{}
	for rel := range releases {
		out[rel] = f.healthy[rel]
	}
	return out, nil
}

func (f *fakeAdvisor) callCount() int { f.mu.Lock(); defer f.mu.Unlock(); return f.calls }

type fakeScheduler struct {
	mu       sync.Mutex
	paths    []string
	err      error
	notMoved bool
}

func (f *fakeScheduler) ScheduleCheckNow(_ context.Context, p string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.paths = append(f.paths, p)
	return f.err == nil && !f.notMoved, f.err
}
func (f *fakeScheduler) got() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.paths...)
}

func hubFixture(t *testing.T, adv *fakeAdvisor, sch *fakeScheduler, budget *hints.Budget) *pubFixture {
	t.Helper()
	f := newPubInstance(t, level(1), nil)
	f.seedFile(t)
	f.start(t)
	// Let the upkeep loop's immediate first pass finish so it cannot race the
	// passes these tests drive by hand (the next one is about 30 s away).
	n := f.node(t)
	eventually(t, "first upkeep pass", func() bool { return n.maintRuns.Load() >= 1 })
	src := HintSources{Advisor: adv, Budget: budget, Backbones: func() []string { return []string{"omicron"} }}
	if sch != nil {
		src.Scheduler = sch
	}
	f.svc.SetHintSources(src)
	return f
}

func TestHealthAdviceForPath(t *testing.T) {
	ctx := context.Background()
	adv := &fakeAdvisor{advice: hints.Advice{Kind: hints.AdviceReduceSampleToFloor}}
	f := hubFixture(t, adv, nil, hints.NewBudget(10))

	got := f.svc.HealthAdvice(ctx, pubPath)
	if got.Kind != hints.AdviceReduceSampleToFloor {
		t.Fatalf("advice = %v", got.Kind)
	}
	if len(adv.gotBB) != 1 || adv.gotBB[0] != "omicron" {
		t.Fatalf("local backbones not passed: %v", adv.gotBB)
	}
	if g := f.status(t).Advice.Given["reduce_sample_to_floor"]; g != 1 {
		t.Fatalf("given counter = %d", g)
	}

	// A file that is not in the content index has no identifiers: no advice,
	// and the advisor is never asked.
	before := adv.callCount()
	if got := f.svc.HealthAdvice(ctx, "movies/unknown.mkv"); got.Kind != hints.AdviceNone || adv.callCount() != before {
		t.Fatalf("unindexed file: %v", got)
	}

	f.svc.NotePeerAdviceApplied(hints.AdviceReduceSampleToFloor)
	f.svc.NotePeerAdviceApplied(hints.AdviceMaySkipOneCheck)
	st := f.status(t).Advice
	if st.ReduceApplied != 1 || st.SkipsApplied != 1 {
		t.Fatalf("applied counters: %+v", st)
	}
}

func TestHealthAdviceFailsOpen(t *testing.T) {
	ctx := context.Background()
	adv := &fakeAdvisor{advice: hints.Advice{Kind: hints.AdviceReduceSampleToFloor}, err: errors.New("broken database")}
	f := hubFixture(t, adv, nil, hints.NewBudget(10))
	if got := f.svc.HealthAdvice(ctx, pubPath); got.Kind != hints.AdviceNone {
		t.Fatalf("an error must mean no advice: %v", got)
	}
	if n := f.status(t).Advice.LookupErrors; n != 1 {
		t.Fatalf("lookup errors = %d", n)
	}

	slow := &fakeAdvisor{advice: hints.Advice{Kind: hints.AdviceReduceSampleToFloor}, delay: 5 * time.Second}
	f.svc.SetHintSources(HintSources{Advisor: slow, Budget: hints.NewBudget(1), Backbones: func() []string { return []string{"omicron"} }})
	start := time.Now()
	if got := f.svc.HealthAdvice(ctx, pubPath); got.Kind != hints.AdviceNone || time.Since(start) > 2*time.Second {
		t.Fatalf("a slow lookup must give up quickly: %v after %v", got, time.Since(start))
	}
}

func TestHealthAdviceOffWhileDisabledOrUnwired(t *testing.T) {
	ctx := context.Background()
	adv := &fakeAdvisor{advice: hints.Advice{Kind: hints.AdviceReduceSampleToFloor}}

	// No sources installed.
	f := newPubInstance(t, level(1), nil)
	f.seedFile(t)
	f.start(t)
	if got := f.svc.HealthAdvice(ctx, pubPath); got.Kind != hints.AdviceNone {
		t.Fatal("advice without sources")
	}
	// Feature disabled.
	off := newPubInstance(t, Settings{}, nil)
	off.start(t)
	off.svc.SetHintSources(HintSources{Advisor: adv, Budget: hints.NewBudget(1), Backbones: func() []string { return []string{"x"} }})
	if got := off.svc.HealthAdvice(ctx, pubPath); got.Kind != hints.AdviceNone || adv.callCount() != 0 {
		t.Fatal("advice while disabled")
	}
	if got := off.svc.NetworkHealthyPaths(ctx, []string{pubPath}); len(got) != 0 {
		t.Fatal("labels while disabled")
	}
	if n := off.svc.ReceivingNetworks(); len(n) != 0 {
		t.Fatal("receiving networks while disabled")
	}
}

func TestReceivingNetworksAndNodeID(t *testing.T) {
	f := newPubInstance(t, level(1), nil)
	f.start(t)
	nets := f.svc.ReceivingNetworks()
	if len(nets) != 1 || !nets[0].ID.IsOpen() || nets[0].Level != 1 {
		t.Fatalf("networks: %+v", nets)
	}
	if id := f.svc.NodeID(); len(id) != 64 {
		t.Fatalf("node id = %q", id)
	}
	// Level 0 on the open network receives nothing.
	z := newPubInstance(t, level(0), nil)
	z.start(t)
	if n := z.svc.ReceivingNetworks(); len(n) != 0 {
		t.Fatalf("level 0 receives: %+v", n)
	}
}

func insertUnhealthy(t *testing.T, f *pubFixture, rel, file string, publisher byte) {
	t.Helper()
	healthy := false
	rec := database.PeernetRecord{
		NetworkID: openID.String(), Kind: database.PeernetRecordKindAttestation,
		ContentHash: strings.Repeat(string(rune('a'+publisher)), 63) + "0", ReleaseID: rel, FileID: file,
		PublisherNodeID: strings.Repeat(string(rune('a'+publisher)), 64), CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour),
		Source: database.PeernetRecordSourcePeer, Healthy: &healthy, Backbones: []string{"omicron"},
		CheckedAt: time.Now().Add(-time.Minute), Payload: []byte("p"),
	}
	if _, err := f.recs.Insert(context.Background(), rec, database.PeernetRecordLimits{}); err != nil {
		t.Fatal(err)
	}
}

func TestRecheckPassSchedulesFilesWithUnhealthyEvidenceOnce(t *testing.T) {
	ctx := context.Background()
	adv := &fakeAdvisor{advice: hints.Advice{Kind: hints.AdviceRecheckSooner}}
	sch := &fakeScheduler{}
	f := hubFixture(t, adv, sch, hints.NewBudget(10))
	insertUnhealthy(t, f, pubRelease, pubFile, 1)
	// Evidence about a file this node does not have.
	insertUnhealthy(t, f, strings.Repeat("e", 64), strings.Repeat("f", 64), 2)

	n := f.node(t)
	n.maintainHints(ctx)
	if got := sch.got(); len(got) != 1 || got[0] != pubPath {
		t.Fatalf("scheduled: %v", got)
	}
	if adv.callCount() != 1 {
		t.Fatalf("the advisor was asked about a file this node does not have: %d", adv.callCount())
	}
	n.maintainHints(ctx)
	if got := sch.got(); len(got) != 1 {
		t.Fatalf("an attestation was considered twice: %v", got)
	}
	if c := f.status(t).Advice.RecheckScheduled; c != 1 {
		t.Fatalf("recheck counter = %d", c)
	}
}

func TestRecheckPassOnlySchedulesWhenAdvised(t *testing.T) {
	ctx := context.Background()
	adv := &fakeAdvisor{advice: hints.Advice{Kind: hints.AdviceNone}}
	sch := &fakeScheduler{}
	f := hubFixture(t, adv, sch, hints.NewBudget(10))
	insertUnhealthy(t, f, pubRelease, pubFile, 1)
	f.node(t).maintainHints(ctx)
	if got := sch.got(); len(got) != 0 {
		t.Fatalf("scheduled without advice: %v", got)
	}
}

func TestRecheckPassWaitsForBudgetWithoutLosingEvidence(t *testing.T) {
	ctx := context.Background()
	adv := &fakeAdvisor{advice: hints.Advice{Kind: hints.AdviceRecheckSooner}}
	sch := &fakeScheduler{}
	budget := hints.NewBudget(0) // nothing to spend
	f := hubFixture(t, adv, sch, budget)
	insertUnhealthy(t, f, pubRelease, pubFile, 1)
	n := f.node(t)
	n.maintainHints(ctx)
	if adv.callCount() != 0 || len(sch.got()) != 0 {
		t.Fatal("acted without budget")
	}
	// Budget arrives: the same attestation is still there to be considered.
	f.svc.SetHintSources(HintSources{Advisor: adv, Budget: hints.NewBudget(10), Scheduler: sch, Backbones: func() []string { return []string{"omicron"} }})
	n.maintainHints(ctx)
	if got := sch.got(); len(got) != 1 {
		t.Fatalf("evidence was lost while the budget was empty: %v", got)
	}
}

func TestRecheckPassDoesNothingWithoutSchedulerOrWhenDisabled(t *testing.T) {
	ctx := context.Background()
	adv := &fakeAdvisor{advice: hints.Advice{Kind: hints.AdviceRecheckSooner}}
	f := hubFixture(t, adv, nil, hints.NewBudget(10))
	insertUnhealthy(t, f, pubRelease, pubFile, 1)
	f.node(t).maintainHints(ctx)
	if adv.callCount() != 0 {
		t.Fatal("no scheduler installed, yet the advisor (and its budget) was used")
	}
}

// A failing scheduler is logged and the evidence is retried on the next
// passes, a bounded number of times, so a brief database hiccup cannot lose it
// and a persistent one cannot loop forever. Nothing is spent for it.
func TestSchedulerFailureRetriesBoundedAndSpendsNothing(t *testing.T) {
	ctx := context.Background()
	adv := &fakeAdvisor{advice: hints.Advice{Kind: hints.AdviceRecheckSooner}}
	sch := &fakeScheduler{err: errors.New("db busy")}
	f := hubFixture(t, adv, sch, hints.NewBudget(10))
	insertUnhealthy(t, f, pubRelease, pubFile, 1)
	n := f.node(t)
	for range 6 {
		n.maintainHints(ctx)
	}
	if got := sch.got(); len(got) != maxRecheckAttempts {
		t.Fatalf("attempts: %v (want %d)", got, maxRecheckAttempts)
	}
	if c := f.status(t).Advice.RecheckScheduled; c != 0 {
		t.Fatalf("counted a failed scheduling: %d", c)
	}
	if adv.spendCount() != 0 {
		t.Fatalf("spent budget on a failed scheduling: %d", adv.spendCount())
	}
}

// A transient scheduler error does not lose the evidence.
func TestSchedulerFailureThenSuccessStillSchedules(t *testing.T) {
	ctx := context.Background()
	adv := &fakeAdvisor{advice: hints.Advice{Kind: hints.AdviceRecheckSooner}}
	sch := &fakeScheduler{err: errors.New("db busy")}
	f := hubFixture(t, adv, sch, hints.NewBudget(10))
	insertUnhealthy(t, f, pubRelease, pubFile, 1)
	n := f.node(t)
	n.maintainHints(ctx)
	sch.mu.Lock()
	sch.err = nil
	sch.mu.Unlock()
	n.maintainHints(ctx)
	if c := f.status(t).Advice.RecheckScheduled; c != 1 {
		t.Fatalf("evidence lost after a transient error: scheduled=%d", c)
	}
	if adv.spendCount() != 1 {
		t.Fatalf("spends = %d", adv.spendCount())
	}
}

// The budget is spent only when a check was really moved forward.
func TestRecheckPassSpendsOnlyWhenACheckMoved(t *testing.T) {
	ctx := context.Background()
	adv := &fakeAdvisor{advice: hints.Advice{Kind: hints.AdviceRecheckSooner}}
	sch := &fakeScheduler{notMoved: true}
	f := hubFixture(t, adv, sch, hints.NewBudget(10))
	insertUnhealthy(t, f, pubRelease, pubFile, 1)
	f.node(t).maintainHints(ctx)
	if adv.spendCount() != 0 {
		t.Fatalf("spent for a check that did not move: %d", adv.spendCount())
	}
	if c := f.status(t).Advice.RecheckScheduled; c != 0 {
		t.Fatalf("counted: %d", c)
	}
}

// Record sequence numbers restart at 1 after leaving and rejoining a network,
// so the pass position of a removed network must not survive.
func TestRecheckCursorIsClearedWhenANetworkIsRemoved(t *testing.T) {
	ctx := context.Background()
	adv := &fakeAdvisor{advice: hints.Advice{Kind: hints.AdviceNone}}
	f := hubFixture(t, adv, &fakeScheduler{}, hints.NewBudget(10))
	insertUnhealthy(t, f, pubRelease, pubFile, 1)
	n := f.node(t)
	n.maintainHints(ctx)
	cursor := func() (int64, bool) {
		n.hintMu.Lock()
		defer n.hintMu.Unlock()
		v, ok := n.hintCursor[openID.String()]
		return v, ok
	}
	if _, ok := cursor(); !ok {
		t.Fatal("the pass should have recorded a position")
	}
	n.forgetRecords(ctx, openID)
	if _, ok := cursor(); ok {
		t.Fatal("forgetRecords must clear the recheck position")
	}

	n.maintainHints(ctx)
	n.removeRecordNetwork(openID)
	if _, ok := cursor(); ok {
		t.Fatal("removeRecordNetwork must clear the recheck position")
	}
}

func TestNetworkHealthyPathsLabelsExactMatchesOnly(t *testing.T) {
	ctx := context.Background()
	adv := &fakeAdvisor{healthy: map[string]hints.NetworkHealth{pubRelease: {Healthy: true}}}
	f := hubFixture(t, adv, nil, hints.NewBudget(10))
	other := "movies/Other/other.mkv"
	if err := f.content.Upsert(ctx, database.PeernetContentEntry{FilePath: other, FileID: strings.Repeat("c", 64), ReleaseID: strings.Repeat("9", 64)}); err != nil {
		t.Fatal(err)
	}
	got := f.svc.NetworkHealthyPaths(ctx, []string{pubPath, other, "movies/not-indexed.mkv"})
	if len(got) != 1 || !got[pubPath] {
		t.Fatalf("labels: %v", got)
	}
	// Briefly cached: a second call does not ask the advisor again.
	n := adv.nhCalls
	f.svc.NetworkHealthyPaths(ctx, []string{pubPath, other})
	if adv.nhCalls != n {
		t.Fatalf("not cached: %d -> %d", n, adv.nhCalls)
	}
}

func TestNetworkHealthyPathsFailsOpen(t *testing.T) {
	adv := &fakeAdvisor{err: errors.New("broken")}
	f := hubFixture(t, adv, nil, hints.NewBudget(10))
	if got := f.svc.NetworkHealthyPaths(context.Background(), []string{pubPath}); len(got) != 0 {
		t.Fatalf("labels despite an error: %v", got)
	}
}

func TestStatusReportsBudget(t *testing.T) {
	f := hubFixture(t, &fakeAdvisor{}, nil, hints.NewBudget(7))
	if got := f.status(t).Advice.BudgetRemaining; got != 7 {
		t.Fatalf("budget remaining = %d", got)
	}
}

// A file with no Release ID of any kind (its import could not be grouped) still
// has a File ID, and evidence is found by file: it gets advice.
func TestHealthAdviceForFileWithoutReleaseID(t *testing.T) {
	ctx := context.Background()
	adv := &fakeAdvisor{advice: hints.Advice{Kind: hints.AdviceReduceSampleToFloor}}
	f := hubFixture(t, adv, nil, hints.NewBudget(10))
	const bare = "movies/Bare/bare.mkv"
	if err := f.content.Upsert(ctx, database.PeernetContentEntry{FilePath: bare, FileID: strings.Repeat("7", 64)}); err != nil {
		t.Fatal(err)
	}
	if got := f.svc.HealthAdvice(ctx, bare); got.Kind != hints.AdviceReduceSampleToFloor {
		t.Fatalf("advice for a file with only a file ID = %v", got.Kind)
	}
	if adv.gotRel != "" {
		t.Fatalf("release passed for a file without one: %q", adv.gotRel)
	}
}

// Evidence is about the file: an attestation published under another release
// ID (an NZB-based one, for a file this node knows by its file set) still moves
// this node's check forward.
func TestRecheckPassFindsEvidenceUnderAnotherReleaseID(t *testing.T) {
	ctx := context.Background()
	adv := &fakeAdvisor{advice: hints.Advice{Kind: hints.AdviceRecheckSooner}}
	sch := &fakeScheduler{}
	f := hubFixture(t, adv, sch, hints.NewBudget(10))
	insertUnhealthy(t, f, strings.Repeat("e", 64), pubFile, 1) // same file, other release ID
	f.node(t).maintainHints(ctx)
	if got := sch.got(); len(got) != 1 || got[0] != pubPath {
		t.Fatalf("scheduled: %v", got)
	}
}

// The release label asks about the release's video files only, in one batch for
// every release on the page.
func TestNetworkHealthyPathsAsksFileLevelInOneBatch(t *testing.T) {
	ctx := context.Background()
	adv := &fakeAdvisor{healthy: map[string]hints.NetworkHealth{pubRelease: {Healthy: true}}}
	f := hubFixture(t, adv, nil, hints.NewBudget(10))
	hx := func(c string) string { return strings.Repeat(c, 64) }
	rows := []database.PeernetContentEntry{
		{FilePath: "movies/Some.Movie.2020/movie.nfo", FileID: hx("1"), ReleaseID: pubRelease},
		{FilePath: "movies/Some.Movie.2020/extra.MP4", FileID: hx("2"), ReleaseID: pubRelease},
		{FilePath: "movies/Docs/a.pdf", FileID: hx("3"), ReleaseID: hx("8")}, // a release with no video
		{FilePath: "movies/Other/o.mkv", FileID: hx("4"), ReleaseID: hx("9"), ReleaseScheme: database.PeernetSchemeFiles},
	}
	for _, r := range rows {
		if err := f.content.Upsert(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	got := f.svc.NetworkHealthyPaths(ctx, []string{pubPath, "movies/Docs/a.pdf", "movies/Other/o.mkv"})
	if len(got) != 1 || !got[pubPath] {
		t.Fatalf("labels: %v", got)
	}
	if adv.nhCalls != 1 {
		t.Fatalf("%d advisor calls for one page, want 1", adv.nhCalls)
	}
	files := adv.gotReleases[pubRelease]
	if len(files) != 2 || files[0] != pubFile && files[1] != pubFile {
		t.Fatalf("files asked for the release: %v (want its two video files)", files)
	}
	if _, asked := adv.gotReleases[hx("8")]; asked && len(adv.gotReleases[hx("8")]) != 0 {
		t.Fatalf("a release with no video file was asked about: %v", adv.gotReleases[hx("8")])
	}
	if len(adv.gotReleases[hx("9")]) != 1 {
		t.Fatalf("derived release not asked about its file: %v", adv.gotReleases[hx("9")])
	}
}
