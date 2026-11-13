package hints

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/records"
)

var (
	t0       = time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	openNet  = netid.Open()
	privNet  = netid.NetworkID{0x42}
	release  = hexID(1)
	file     = hexID(2)
	otherFil = hexID(3)
	self     = hexID(0xee)
)

func hexID(b byte) string {
	var a [32]byte
	for i := range a {
		a[i] = b
	}
	return hex.EncodeToString(a[:])
}

func node(i int) string {
	var a [32]byte
	a[0], a[1] = byte(i), byte(i>>8)
	a[31] = 0xaa
	return hex.EncodeToString(a[:])
}

// fakeStore is read-only by construction (the Store interface has no writers)
// and deliberately sloppy: AttestationsFor ignores its file and freshness
// filters so the advisor's own checks are what the tests exercise.
type fakeStore struct {
	mu         sync.Mutex
	atts       map[string][]database.PeernetAttestation // key nw|release
	cands      []database.PeernetCandidate
	calls      map[string]int
	err        error
	lastLimit  int
	lastNets   []string
	lastSeason *int
}

func newStore() *fakeStore {
	return &fakeStore{atts: map[string][]database.PeernetAttestation{}, calls: map[string]int{}}
}

func key(nw netid.NetworkID, rel string) string { return nw.String() + "|" + rel }

func (s *fakeStore) add(nw netid.NetworkID, rel string, a database.PeernetAttestation) {
	s.atts[key(nw, rel)] = append(s.atts[key(nw, rel)], a)
}

func (s *fakeStore) AttestationsFor(_ context.Context, nw, rel, _ string, _ time.Time) ([]database.PeernetAttestation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls["att"]++
	if s.err != nil {
		return nil, s.err
	}
	return append([]database.PeernetAttestation(nil), s.atts[nw+"|"+rel]...), nil
}

func (s *fakeStore) HealthSummaryForReleases(_ context.Context, nw string, ids []string, notBefore time.Time) (map[string]database.PeernetHealthSummary, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls["sum"]++
	if s.err != nil {
		return nil, s.err
	}
	out := map[string]database.PeernetHealthSummary{}
	for _, id := range ids {
		h, u := map[string]bool{}, map[string]bool{}
		for _, a := range s.atts[nw+"|"+id] {
			if a.CheckedAt.Before(notBefore) {
				continue
			}
			if a.Healthy {
				h[a.PublisherNodeID] = true
			} else {
				u[a.PublisherNodeID] = true
			}
		}
		if len(h)+len(u) > 0 {
			out[id] = database.PeernetHealthSummary{HealthyPublishers: len(h), UnhealthyPublishers: len(u)}
		}
	}
	return out, nil
}

func (s *fakeStore) CandidatesByMedia(_ context.Context, nets []string, imdb string, season, _ *int, limit int) ([]database.PeernetCandidate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls["cand"]++
	s.lastLimit, s.lastNets, s.lastSeason = limit, nets, season
	if s.err != nil {
		return nil, s.err
	}
	return append([]database.PeernetCandidate(nil), s.cands...), nil
}

type fakePeers struct {
	first map[string]time.Time
	// prefix is the network prefix a node was verified at; nodes without an
	// entry get a prefix of their own, "-" means no verified address.
	prefix map[string]string
	err    error
	calls  int
}

// KnownPrefixes makes fakePeers report where each known node was verified.
func (p *fakePeers) KnownPrefixes(_ context.Context, _ string, nodes []string, d time.Duration, now time.Time) (map[string]string, error) {
	if p.err != nil {
		return nil, p.err
	}
	out := map[string]string{}
	for _, n := range nodes {
		f, ok := p.first[n]
		if !ok || f.After(now.Add(-d)) {
			continue
		}
		switch pre := p.prefix[n]; pre {
		case "-":
		case "":
			out[n] = "own-" + n
		default:
			out[n] = pre
		}
	}
	return out, nil
}

func (p *fakePeers) KnownFor(_ context.Context, _, nodeID string, d time.Duration, now time.Time) (bool, error) {
	p.calls++
	if p.err != nil {
		return false, p.err
	}
	f, ok := p.first[nodeID]
	return ok && !f.After(now.Add(-d)), nil
}

// KnownAmong makes fakePeers batch-capable like the real repository.
func (p *fakePeers) KnownAmong(_ context.Context, _ string, nodes []string, d time.Duration, now time.Time) (map[string]bool, error) {
	if p.err != nil {
		return nil, p.err
	}
	out := map[string]bool{}
	for _, n := range nodes {
		if f, ok := p.first[n]; ok && !f.After(now.Add(-d)) {
			out[n] = true
		}
	}
	return out, nil
}

type env struct {
	store  *fakeStore
	peers  *fakePeers
	budget *Budget
	nets   []Network
	now    time.Time
	minC   int
	a      *Advisor
}

func newEnv(t *testing.T, nets ...Network) *env {
	t.Helper()
	e := &env{store: newStore(), peers: &fakePeers{first: map[string]time.Time{}}, budget: NewBudget(3), nets: nets, now: t0, minC: 2}
	a, err := New(Config{
		Store: e.store, Peers: e.peers, Budget: e.budget, SelfNodeID: self,
		Networks:         func() []Network { return e.nets },
		Now:              func() time.Time { return e.now },
		MinCorroboration: func() int { return e.minC },
	})
	if err != nil {
		t.Fatal(err)
	}
	e.a = a
	return e
}

var (
	openRecv = Network{ID: openNet, Level: 1}
	privRecv = Network{ID: privNet, Level: 2}
)

// seed adds an attestation by node i, known since a week ago unless young.
func (e *env) seed(nw netid.NetworkID, i int, mut func(*database.PeernetAttestation)) {
	a := database.PeernetAttestation{
		PublisherNodeID: node(i), FileID: file, Healthy: true, Backbones: []string{"omicron"},
		CheckedAt: e.now.Add(-time.Hour), CreatedAt: e.now.Add(-time.Hour), Source: database.PeernetRecordSourcePeer,
		SegmentsChecked: 100, TotalSegments: 1000,
	}
	if mut != nil {
		mut(&a)
	}
	e.store.add(nw, release, a)
	e.peers.first[node(i)] = e.now.Add(-7 * 24 * time.Hour)
}

func (e *env) advise(t *testing.T, local ...string) Advice {
	t.Helper()
	if len(local) == 0 {
		local = []string{"omicron"}
	}
	adv, err := e.a.HealthAdvice(context.Background(), release, file, local, e.now)
	if err != nil {
		t.Fatal(err)
	}
	return adv
}

func unhealthy(a *database.PeernetAttestation) { a.Healthy = false }

func TestNewValidates(t *testing.T) {
	e := newEnv(t)
	base := Config{Store: e.store, Peers: e.peers, Budget: e.budget, Networks: func() []Network { return nil }}
	for name, mut := range map[string]func(*Config){
		"store": func(c *Config) { c.Store = nil }, "peers": func(c *Config) { c.Peers = nil },
		"budget": func(c *Config) { c.Budget = nil }, "networks": func(c *Config) { c.Networks = nil },
	} {
		c := base
		mut(&c)
		if _, err := New(c); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	if _, err := New(base); err != nil {
		t.Fatalf("optional fields must default: %v", err)
	}
}

func TestAdviceInputValidation(t *testing.T) {
	e := newEnv(t, openRecv)
	for name, ids := range map[string][2]string{
		"bad release": {"xyz", file}, "bad file": {release, "nothex"}, "empty file": {release, ""},
		"upper case": {hexID(1)[:63] + "A", file},
	} {
		if _, err := e.a.HealthAdvice(context.Background(), ids[0], ids[1], []string{"omicron"}, e.now); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestOpenNetworkReducesSampleOnlyWithCorroboration(t *testing.T) {
	e := newEnv(t, openRecv)
	e.seed(openNet, 1, nil)
	if got := e.advise(t); got.Kind != AdviceNone {
		t.Fatalf("one publisher: %+v", got)
	}
	e.seed(openNet, 2, nil)
	got := e.advise(t)
	if got.Kind != AdviceReduceSampleToFloor || !got.SkipUntil.IsZero() || got.Network != openNet {
		t.Fatalf("two publishers: %+v", got)
	}
	e.minC = 3
	if got := e.advise(t); got.Kind != AdviceNone {
		t.Fatalf("threshold raised: %+v", got)
	}
	e.minC = 0 // invalid config falls back to the default of 3
	if got := e.advise(t); got.Kind != AdviceNone {
		t.Fatalf("default threshold needs three publishers: %+v", got)
	}
	e.seed(openNet, 3, nil)
	if got := e.advise(t); got.Kind != AdviceReduceSampleToFloor {
		t.Fatalf("default threshold: %+v", got)
	}
}

func TestOpenNetworkNeverSkips(t *testing.T) {
	e := newEnv(t, openRecv)
	for i := 1; i <= 100; i++ {
		e.seed(openNet, i, nil)
	}
	got := e.advise(t)
	if got.Kind != AdviceReduceSampleToFloor || got.Kind == AdviceMaySkipOneCheck {
		t.Fatalf("%+v", got)
	}
}

func TestYoungIdentitiesAreIgnoredInOpenNetwork(t *testing.T) {
	e := newEnv(t, openRecv)
	for i := 1; i <= 50; i++ {
		e.seed(openNet, i, nil)
		e.peers.first[node(i)] = e.now.Add(-23 * time.Hour) // just under 24h
	}
	if got := e.advise(t); got.Kind != AdviceNone {
		t.Fatalf("young identities counted: %+v", got)
	}
	// Unknown to the peer table entirely.
	e2 := newEnv(t, openRecv)
	e2.seed(openNet, 1, nil)
	e2.seed(openNet, 2, nil)
	delete(e2.peers.first, node(1))
	if got := e2.advise(t); got.Kind != AdviceNone {
		t.Fatalf("unknown publisher counted: %+v", got)
	}
}

func TestPrivateNetworkMaySkipUntilValidityEnd(t *testing.T) {
	e := newEnv(t, privRecv)
	e.seed(privNet, 1, func(a *database.PeernetAttestation) { a.CheckedAt = e.now.Add(-2 * 24 * time.Hour) })
	e.seed(privNet, 2, func(a *database.PeernetAttestation) { a.CheckedAt = e.now.Add(-1 * time.Hour) })
	got := e.advise(t)
	want := e.now.Add(-time.Hour).Add(records.AttestationValidity)
	if got.Kind != AdviceMaySkipOneCheck || !got.SkipUntil.Equal(want) || got.Network != privNet {
		t.Fatalf("%+v want until %v", got, want)
	}
	if got.SkipUntil.After(e.now.Add(records.AttestationValidity)) {
		t.Fatal("skip beyond the attestation validity")
	}
	// Young identity is fine inside a private network: members are vouched.
	e.peers.first[node(2)] = e.now
	if e.advise(t).Kind != AdviceMaySkipOneCheck {
		t.Fatal("private threshold must not need 24h of history")
	}
}

func TestPrivateThresholdIsConfigurable(t *testing.T) {
	e := newEnv(t, privRecv)
	a, err := New(Config{
		Store: e.store, Peers: e.peers, Budget: e.budget, PrivateThreshold: 2,
		Networks: func() []Network { return e.nets }, Now: func() time.Time { return e.now },
	})
	if err != nil {
		t.Fatal(err)
	}
	e.seed(privNet, 1, nil)
	adv, _ := a.HealthAdvice(context.Background(), release, file, []string{"omicron"}, e.now)
	if adv.Kind != AdviceNone {
		t.Fatalf("%+v", adv)
	}
	e.seed(privNet, 2, nil)
	adv, _ = a.HealthAdvice(context.Background(), release, file, []string{"omicron"}, e.now)
	if adv.Kind != AdviceMaySkipOneCheck {
		t.Fatalf("%+v", adv)
	}
}

func TestMixedNetworksOpenEvidenceNeverUpgrades(t *testing.T) {
	e := newEnv(t, openRecv, privRecv)
	for i := 1; i <= 10; i++ {
		e.seed(openNet, i, nil)
	}
	if got := e.advise(t); got.Kind != AdviceReduceSampleToFloor {
		t.Fatalf("open evidence must only reduce: %+v", got)
	}
	e.seed(privNet, 99, nil)
	if got := e.advise(t); got.Kind != AdviceMaySkipOneCheck || got.Network != privNet {
		t.Fatalf("private evidence: %+v", got)
	}
}

func TestFreshnessRules(t *testing.T) {
	for _, tc := range []struct {
		name string
		at   time.Duration // relative to now
		want AdviceKind
	}{
		{"fresh", -time.Hour, AdviceReduceSampleToFloor},
		{"just inside validity", -records.AttestationValidity + time.Minute, AdviceReduceSampleToFloor},
		{"stale", -records.AttestationValidity - time.Minute, AdviceNone},
		{"within future skew", records.MaxFutureSkew - time.Second, AdviceReduceSampleToFloor},
		{"beyond future skew", records.MaxFutureSkew + time.Minute, AdviceNone},
		{"zero time", 0, AdviceNone},
	} {
		e := newEnv(t, openRecv)
		for i := 1; i <= 2; i++ {
			e.seed(openNet, i, func(a *database.PeernetAttestation) {
				if tc.name == "zero time" {
					a.CheckedAt = time.Time{}
					return
				}
				a.CheckedAt = e.now.Add(tc.at)
			})
		}
		if got := e.advise(t); got.Kind != tc.want {
			t.Errorf("%s: %+v want %v", tc.name, got, tc.want)
		}
	}
}

func TestClockGoingBackwardsIgnoresNewAttestations(t *testing.T) {
	e := newEnv(t, openRecv)
	e.seed(openNet, 1, nil)
	e.seed(openNet, 2, nil)
	if e.advise(t).Kind != AdviceReduceSampleToFloor {
		t.Fatal("setup")
	}
	e.now = e.now.Add(-3 * time.Hour) // the attestations now look like they are from the future
	if got := e.advise(t); got.Kind != AdviceNone {
		t.Fatalf("future-dated evidence accepted: %+v", got)
	}
}

func TestAttestationForAnotherFileIsIgnored(t *testing.T) {
	e := newEnv(t, openRecv)
	for i := 1; i <= 3; i++ {
		e.seed(openNet, i, func(a *database.PeernetAttestation) { a.FileID = otherFil })
	}
	if got := e.advise(t); got.Kind != AdviceNone {
		t.Fatalf("evidence for a different file counted: %+v", got)
	}
	// And an unhealthy one for another file must not trigger a recheck.
	e.seed(openNet, 9, func(a *database.PeernetAttestation) { a.FileID = otherFil; a.Healthy = false })
	if got := e.advise(t); got.Kind != AdviceNone {
		t.Fatalf("%+v", got)
	}
}

func TestBackboneSubsetRule(t *testing.T) {
	for _, tc := range []struct {
		name     string
		attester []string
		local    []string
		want     AdviceKind
	}{
		{"equal", []string{"omicron"}, []string{"omicron"}, AdviceReduceSampleToFloor},
		{"attester subset of local", []string{"omicron"}, []string{"omicron", "eweka"}, AdviceReduceSampleToFloor},
		{"attester superset claim", []string{"omicron", "eweka"}, []string{"omicron"}, AdviceNone},
		{"disjoint", []string{"eweka"}, []string{"omicron"}, AdviceNone},
		{"empty attester set", nil, []string{"omicron"}, AdviceNone},
		{"empty local set", []string{"omicron"}, nil, AdviceNone},
		{"local label needs normalising", []string{"omicron"}, []string{"  OMICRON "}, AdviceReduceSampleToFloor},
		{"local invalid label ignored", []string{"omicron"}, []string{"bad/label"}, AdviceNone},
		{"spaces", []string{"eweka-internet-services"}, []string{"Eweka Internet Services"}, AdviceReduceSampleToFloor},
	} {
		e := newEnv(t, openRecv)
		for i := 1; i <= 2; i++ {
			e.seed(openNet, i, func(a *database.PeernetAttestation) { a.Backbones = tc.attester })
		}
		var got Advice
		if tc.local == nil {
			adv, err := e.a.HealthAdvice(context.Background(), release, file, nil, e.now)
			if err != nil {
				t.Fatal(err)
			}
			got = adv
		} else {
			got = e.advise(t, tc.local...)
		}
		if got.Kind != tc.want {
			t.Errorf("%s: %+v want %v", tc.name, got, tc.want)
		}
	}
}

func TestOwnAttestationsNeverCount(t *testing.T) {
	e := newEnv(t, openRecv, privRecv)
	for i := 1; i <= 3; i++ {
		e.seed(openNet, i, func(a *database.PeernetAttestation) { a.Source = database.PeernetRecordSourceLocal })
	}
	e.seed(privNet, 4, func(a *database.PeernetAttestation) { a.Source = database.PeernetRecordSourceLocal })
	e.store.add(privNet, release, database.PeernetAttestation{ // relayed copy of our own record
		PublisherNodeID: self, FileID: file, Healthy: true, Backbones: []string{"omicron"},
		CheckedAt: e.now.Add(-time.Hour), CreatedAt: e.now, Source: database.PeernetRecordSourcePeer,
	})
	e.peers.first[self] = e.now.Add(-30 * 24 * time.Hour)
	if got := e.advise(t); got.Kind != AdviceNone {
		t.Fatalf("own evidence counted: %+v", got)
	}
	e.seed(privNet, 5, func(a *database.PeernetAttestation) {
		a.Healthy, a.Source = false, database.PeernetRecordSourceLocal
	})
	if got := e.advise(t); got.Kind != AdviceNone || e.budget.Remaining(e.now) != 3 {
		t.Fatalf("own unhealthy attestation triggered a recheck: %+v", got)
	}
}

func TestUnhealthyTriggersRecheckFromGlobalBudget(t *testing.T) {
	e := newEnv(t, openRecv)
	e.minC = 1 // one unhealthy publisher meets the threshold
	e.seed(openNet, 1, unhealthy)
	got := e.advise(t)
	if got.Kind != AdviceRecheckSooner || !got.SkipUntil.IsZero() {
		t.Fatalf("%+v", got)
	}
	if !e.a.SpendRecheck(release, file, got.EvidenceAt, e.now) {
		t.Fatal("spend refused")
	}
	if e.budget.Remaining(e.now) != 2 {
		t.Fatalf("remaining %d", e.budget.Remaining(e.now))
	}
	// The same evidence does not spend a second token.
	if got := e.advise(t); got.Kind != AdviceNone || e.budget.Remaining(e.now) != 2 {
		t.Fatalf("repeat: %+v remaining %d", got, e.budget.Remaining(e.now))
	}
	// Newer evidence for the same file is new information.
	e.store.atts[key(openNet, release)] = nil
	e.seed(openNet, 1, func(a *database.PeernetAttestation) {
		a.Healthy = false
		a.CheckedAt = e.now.Add(time.Minute - time.Second)
	})
	e.now = e.now.Add(time.Minute)
	if got := e.advise(t); got.Kind != AdviceRecheckSooner {
		t.Fatalf("newer evidence: %+v", got)
	}
}

func TestUnhealthyOnDisjointBackboneIsIgnored(t *testing.T) {
	e := newEnv(t, openRecv)
	e.seed(openNet, 1, func(a *database.PeernetAttestation) { a.Healthy = false; a.Backbones = []string{"eweka"} })
	if got := e.advise(t); got.Kind != AdviceNone || e.budget.Remaining(e.now) != 3 {
		t.Fatalf("%+v", got)
	}
}

func TestManyIdentitiesCannotRaiseTheRecheckBudget(t *testing.T) {
	e := newEnv(t, openRecv, privRecv)
	ctx := context.Background()
	granted := 0
	for f := 0; f < 40; f++ {
		rel := hexID(byte(100 + f))
		for i := 0; i < 50; i++ {
			nw := openNet
			if i%2 == 0 {
				nw = privNet
			}
			e.store.add(nw, rel, database.PeernetAttestation{
				PublisherNodeID: node(1000 + i), FileID: file, Healthy: false, Backbones: []string{"omicron"},
				CheckedAt: e.now.Add(-time.Minute), CreatedAt: e.now, Source: database.PeernetRecordSourcePeer,
			})
		}
		adv, err := e.a.HealthAdvice(ctx, rel, file, []string{"omicron"}, e.now)
		if err != nil {
			t.Fatal(err)
		}
		if adv.Kind == AdviceRecheckSooner && e.a.SpendRecheck(rel, file, adv.EvidenceAt, e.now) {
			granted++
		}
	}
	if granted != 3 {
		t.Fatalf("granted %d rechecks with a budget of 3 per hour", granted)
	}
}

func TestConflictingEvidenceNeverShortens(t *testing.T) {
	for _, nets := range [][]Network{{openRecv}, {privRecv}, {openRecv, privRecv}} {
		e := newEnv(t, nets...)
		for _, n := range nets {
			for i := 1; i <= 3; i++ {
				e.seed(n.ID, i, nil)
			}
		}
		e.seed(nets[len(nets)-1].ID, 50, unhealthy)
		e.seed(nets[len(nets)-1].ID, 51, unhealthy) // unhealthy evidence meets the threshold too
		got := e.advise(t)
		if got.Kind == AdviceReduceSampleToFloor || got.Kind == AdviceMaySkipOneCheck {
			t.Fatalf("conflict shortened: %+v", got)
		}
		if got.Kind != AdviceRecheckSooner {
			t.Fatalf("conflict with budget left should recheck: %+v", got)
		}
		// With the budget gone the answer is still not a shortening, and
		// nothing can be paid for.
		for e.budget.Take(e.now) {
		}
		e.a.forget()
		got = e.advise(t)
		if got.Kind == AdviceReduceSampleToFloor || got.Kind == AdviceMaySkipOneCheck ||
			e.a.SpendRecheck(release, file, got.EvidenceAt, e.now) {
			t.Fatalf("conflict with no budget: %+v", got)
		}
	}
}

func TestUnhealthyAcrossNetworksBlocksPrivateSkip(t *testing.T) {
	e := newEnv(t, openRecv, privRecv)
	e.seed(privNet, 1, nil)
	e.seed(openNet, 2, unhealthy)
	e.seed(openNet, 3, unhealthy) // as many as the open threshold needs
	for e.budget.Take(e.now) {
	}
	if got := e.advise(t); got.Kind == AdviceMaySkipOneCheck || got.Kind == AdviceReduceSampleToFloor {
		t.Fatalf("%+v", got)
	}
}

func TestBudgetIsSharedAcrossFiles(t *testing.T) {
	e := newEnv(t, openRecv)
	e.minC = 1
	for i := 0; i < 5; i++ {
		rel := hexID(byte(10 + i))
		e.peers.first[node(i)] = e.now.Add(-7 * 24 * time.Hour)
		e.store.add(openNet, rel, database.PeernetAttestation{
			PublisherNodeID: node(i), FileID: file, Healthy: false, Backbones: []string{"omicron"},
			CheckedAt: e.now.Add(-time.Minute), CreatedAt: e.now, Source: database.PeernetRecordSourcePeer,
		})
	}
	n := 0
	for i := 0; i < 5; i++ {
		adv, _ := e.a.HealthAdvice(context.Background(), hexID(byte(10+i)), file, []string{"omicron"}, e.now)
		if adv.Kind == AdviceRecheckSooner && e.a.SpendRecheck(hexID(byte(10+i)), file, adv.EvidenceAt, e.now) {
			n++
		}
	}
	if n != 3 {
		t.Fatalf("%d", n)
	}
	e.now = e.now.Add(21 * time.Minute) // 3/hour refills one token every 20 minutes
	adv, _ := e.a.HealthAdvice(context.Background(), hexID(14), file, []string{"omicron"}, e.now)
	if adv.Kind != AdviceRecheckSooner || !e.a.SpendRecheck(hexID(14), file, adv.EvidenceAt, e.now) {
		t.Fatalf("budget did not refill: %+v", adv)
	}
}

func TestLevelZeroNetworkIsIgnored(t *testing.T) {
	e := newEnv(t, Network{ID: openNet, Level: 0})
	e.seed(openNet, 1, nil)
	e.seed(openNet, 2, nil)
	if got := e.advise(t); got.Kind != AdviceNone || e.store.calls["att"] != 0 {
		t.Fatalf("%+v calls %v", got, e.store.calls)
	}
}

func TestStoreAndPeerErrorsPropagateAndAdviceIsNone(t *testing.T) {
	e := newEnv(t, openRecv)
	e.seed(openNet, 1, nil)
	e.seed(openNet, 2, nil)
	e.peers.err = errors.New("peer table down")
	adv, err := e.a.HealthAdvice(context.Background(), release, file, []string{"omicron"}, e.now)
	if err == nil || adv.Kind != AdviceNone {
		t.Fatalf("peer error: %+v %v", adv, err)
	}
	e.peers.err = nil
	e.store.err = errors.New("db down")
	adv, err = e.a.HealthAdvice(context.Background(), release, file, []string{"omicron"}, e.now)
	if err == nil || adv.Kind != AdviceNone {
		t.Fatalf("store error: %+v %v", adv, err)
	}
}

func TestAdviceIsReadOnlyAndNeverLengthens(t *testing.T) {
	// Property over a spread of scenarios: advice is a value; the store has
	// only read methods; a skip never reaches past the validity end and a
	// non-skip carries no deadline.
	for i := 0; i < 20; i++ {
		e := newEnv(t, openRecv, privRecv)
		for j := 0; j < i%4; j++ {
			e.seed(privNet, j, nil)
			e.seed(openNet, 10+j, nil)
		}
		if i%5 == 0 {
			e.seed(openNet, 77, unhealthy)
		}
		adv := e.advise(t)
		switch adv.Kind {
		case AdviceMaySkipOneCheck:
			if !adv.SkipUntil.After(e.now) || adv.SkipUntil.After(e.now.Add(records.AttestationValidity)) {
				t.Fatalf("skip deadline %v", adv.SkipUntil)
			}
		default:
			if !adv.SkipUntil.IsZero() {
				t.Fatalf("%v carries a deadline", adv.Kind)
			}
		}
	}
}

func TestAdviceKindStrings(t *testing.T) {
	for k, want := range map[AdviceKind]string{
		AdviceNone: "none", AdviceReduceSampleToFloor: "reduce_sample_to_floor",
		AdviceMaySkipOneCheck: "may_skip_one_check", AdviceRecheckSooner: "recheck_sooner", AdviceKind(99): "unknown",
	} {
		if k.String() != want {
			t.Errorf("%d: %s", k, k.String())
		}
	}
}

func TestForgetBoundsMemory(t *testing.T) {
	e := newEnv(t, openRecv)
	e.a.mu.Lock()
	for i := 0; i < maxAdvised+10; i++ {
		e.a.markAdvised(fmt.Sprintf("k%d", i), e.now)
	}
	n := len(e.a.advised)
	e.a.mu.Unlock()
	if n > maxAdvised {
		t.Fatalf("advised map grew to %d", n)
	}
}

// Asking for advice (the inline check path) is read-only: it never spends the
// shared budget and never marks evidence as acted on.
func TestHealthAdviceIsReadOnlyAndSpendRecheckPays(t *testing.T) {
	e := newEnv(t, openRecv)
	e.minC = 1
	e.seed(openNet, 1, unhealthy)
	for i := 0; i < 3; i++ {
		got := e.advise(t)
		if got.Kind != AdviceRecheckSooner || got.EvidenceAt.IsZero() {
			t.Fatalf("ask %d: %+v", i, got)
		}
	}
	if e.budget.Remaining(e.now) != 3 {
		t.Fatalf("asking must not spend: remaining %d", e.budget.Remaining(e.now))
	}
	adv := e.advise(t)
	if !e.a.SpendRecheck(release, file, adv.EvidenceAt, e.now) {
		t.Fatal("spend refused")
	}
	if e.budget.Remaining(e.now) != 2 {
		t.Fatalf("remaining %d", e.budget.Remaining(e.now))
	}
	if got := e.advise(t); got.Kind != AdviceNone {
		t.Fatalf("acted-on evidence must not be advised again: %+v", got)
	}
	if e.a.SpendRecheck(release, file, adv.EvidenceAt, e.now) || e.budget.Remaining(e.now) != 2 {
		t.Fatal("the same evidence must not be paid for twice")
	}
	for e.budget.Take(e.now) {
	}
	e.a.forget()
	if e.a.SpendRecheck(release, file, adv.EvidenceAt, e.now) {
		t.Fatal("no budget: no spend")
	}
}

// Freshly minted identities must not be able to drain the budget: unhealthy
// evidence needs the same known-for-a-day publishers as healthy evidence on
// the open network. Private networks keep their single-member threshold.
func TestUnhealthyEvidenceNeedsKnownPublishersOnTheOpenNetwork(t *testing.T) {
	e := newEnv(t, openRecv)
	e.minC = 1
	e.seed(openNet, 1, func(a *database.PeernetAttestation) { a.Healthy = false })
	e.peers.first[node(1)] = e.now.Add(-time.Hour) // minted an hour ago
	if got := e.advise(t); got.Kind != AdviceNone {
		t.Fatalf("a young identity's unhealthy attestation must be ignored: %+v", got)
	}
	e.peers.first[node(1)] = e.now.Add(-48 * time.Hour)
	if got := e.advise(t); got.Kind != AdviceRecheckSooner {
		t.Fatalf("a known publisher's unhealthy attestation counts: %+v", got)
	}

	p := newEnv(t, privRecv)
	p.seed(privNet, 1, func(a *database.PeernetAttestation) { a.Healthy = false })
	delete(p.peers.first, node(1)) // not known at all: members are vouched for
	if got := p.advise(t); got.Kind != AdviceRecheckSooner {
		t.Fatalf("private networks keep threshold 1 without an age: %+v", got)
	}
}

func TestAdvisedMemoryEvictsTheOldestNotEverything(t *testing.T) {
	e := newEnv(t, openRecv)
	e.a.mu.Lock()
	for i := 0; i < maxAdvised; i++ {
		e.a.markAdvised(fmt.Sprintf("k%d", i), e.now.Add(time.Duration(i)*time.Second))
	}
	e.a.markAdvised("fresh", e.now.Add(time.Hour))
	_, oldest := e.a.advised["k0"]
	_, newest := e.a.advised[fmt.Sprintf("k%d", maxAdvised-1)]
	_, fresh := e.a.advised["fresh"]
	n := len(e.a.advised)
	e.a.mu.Unlock()
	if oldest || !newest || !fresh || n != maxAdvised {
		t.Fatalf("oldest kept=%v newest kept=%v fresh=%v size=%d", oldest, newest, fresh, n)
	}
}
