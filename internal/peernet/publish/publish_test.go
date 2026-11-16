package publish

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/records"
)

var (
	t0      = time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	openNet = netid.Open()
	privNet = netid.NetworkID{9, 9, 9}
	relID   = records.ReleaseID{1, 2, 3}
	fileID  = records.FileID{4, 5, 6}
)

type fakeKey struct {
	k     *identity.Key
	calls int
}

var errNoKey = errors.New("signing key unavailable")

func (f *fakeKey) NodeID() identity.NodeID { return f.k.NodeID() }

func (f *fakeKey) BuildRelease(nw netid.NetworkID, in records.ReleaseRecord, now time.Time) ([]byte, error) {
	f.calls++
	if f.k == nil {
		return nil, errNoKey
	}
	_, raw, err := records.BuildReleaseRecord(f.k, nw, in, now)
	return raw, err
}

func (f *fakeKey) BuildAttestation(nw netid.NetworkID, in records.HealthAttestation, now time.Time) ([]byte, error) {
	f.calls++
	if f.k == nil {
		return nil, errNoKey
	}
	_, raw, err := records.BuildHealthAttestation(f.k, nw, in, now)
	return raw, err
}

type sunk struct {
	nw  netid.NetworkID
	enc []byte
}

type fakeSink struct {
	got  []sunk
	fail map[netid.NetworkID]error
}

func (s *fakeSink) PublishLocal(_ context.Context, nw netid.NetworkID, enc []byte) error {
	if err := s.fail[nw]; err != nil {
		return err
	}
	s.got = append(s.got, sunk{nw, append([]byte(nil), enc...)})
	return nil
}

type fakeBackbones struct{ provider, known []string }

func (f fakeBackbones) ProviderBackbones(context.Context) []string { return f.provider }
func (f fakeBackbones) KnownBackbones(context.Context) []string    { return f.known }

type storeKey struct {
	nw   netid.NetworkID
	kind records.Kind
	rel  records.ReleaseID
	file records.FileID
}

type memStore struct {
	m       map[storeKey]State
	readErr error
	markErr error
}

func newMemStore() *memStore { return &memStore{m: map[storeKey]State{}} }

func (s *memStore) LastPublished(_ context.Context, nw netid.NetworkID, k records.Kind, r records.ReleaseID, f records.FileID) (State, bool, error) {
	if s.readErr != nil {
		return State{}, false, s.readErr
	}
	st, ok := s.m[storeKey{nw, k, r, f}]
	return st, ok, nil
}

func (s *memStore) MarkPublished(_ context.Context, nw netid.NetworkID, k records.Kind, r records.ReleaseID, f records.FileID, st State) error {
	if s.markErr != nil {
		return s.markErr
	}
	s.m[storeKey{nw, k, r, f}] = st
	return nil
}

type env struct {
	key   *fakeKey
	sink  *fakeSink
	store *memStore
	now   time.Time
	nets  []Network
	excl  Exclusions
	bb    fakeBackbones
	cfg   Config
}

func newEnv(t *testing.T) *env {
	t.Helper()
	k, err := identity.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	e := &env{
		key:   &fakeKey{k: k},
		sink:  &fakeSink{fail: map[netid.NetworkID]error{}},
		store: newMemStore(),
		now:   t0,
		nets:  []Network{{ID: openNet, Level: 2}},
		bb:    fakeBackbones{provider: []string{"Omicron"}, known: []string{"Omicron", "Eweka Internet Services"}},
	}
	e.cfg = Config{
		Signer:     e.key,
		Sink:       e.sink,
		Store:      e.store,
		Backbones:  func() Backbones { return e.bb }(),
		Networks:   func() []Network { return e.nets },
		Exclusions: func() Exclusions { return e.excl },
		Now:        func() time.Time { return e.now },
	}
	return e
}

func (e *env) publisher(t *testing.T) *Publisher {
	t.Helper()
	p, err := New(e.cfg)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func healthy() CheckResult {
	return CheckResult{
		ReleaseID: relID, FileID: fileID, CheckedAt: t0.Add(-time.Minute), Verdict: VerdictHealthy,
		TotalSegments: 100, SegmentsChecked: 10, MediaHeaderVerified: true,
	}
}

func info() ReleaseInfo {
	return ReleaseInfo{
		Title: "Some.Movie.2020.1080p", TotalSize: 5 << 30, FileCount: 3,
		PostDate: t0.Add(-48 * time.Hour), Category: "Movies", Indexer: "IndexerOne",
		Media: records.MediaIDs{IMDb: "tt0133093"},
	}
}

func kinds(t *testing.T, sink *fakeSink) (rel, att int) {
	t.Helper()
	for _, s := range sink.got {
		env, err := records.Peek(s.enc)
		if err != nil {
			t.Fatal(err)
		}
		if env.Kind == records.KindRelease {
			rel++
		} else {
			att++
		}
	}
	return
}

func published(outs []Outcome, k records.Kind) int {
	n := 0
	for _, o := range outs {
		if o.Kind == k && o.Skip == SkipNone {
			n++
		}
	}
	return n
}

func TestNewValidatesConfig(t *testing.T) {
	e := newEnv(t)
	for name, mut := range map[string]func(*Config){
		"signer":   func(c *Config) { c.Signer = nil },
		"sink":     func(c *Config) { c.Sink = nil },
		"store":    func(c *Config) { c.Store = nil },
		"backbone": func(c *Config) { c.Backbones = nil },
		"networks": func(c *Config) { c.Networks = nil },
	} {
		c := e.cfg
		mut(&c)
		if _, err := New(c); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	c := e.cfg
	c.Now, c.Exclusions = nil, nil
	if _, err := New(c); err != nil {
		t.Fatalf("optional fields must default: %v", err)
	}
}

func TestLevelGate(t *testing.T) {
	for _, tc := range []struct {
		level int
		want  int
	}{{0, 0}, {1, 0}, {2, 2}, {3, 2}} {
		e := newEnv(t)
		e.nets = []Network{{ID: openNet, Level: tc.level}}
		outs, err := e.publisher(t).Publish(context.Background(), healthy(), info())
		if err != nil {
			t.Fatal(err)
		}
		if len(e.sink.got) != tc.want {
			t.Errorf("level %d: sunk %d want %d", tc.level, len(e.sink.got), tc.want)
		}
		if tc.want == 0 {
			if e.key.calls != 0 {
				t.Errorf("level %d: key touched although nothing may be built", tc.level)
			}
			for _, o := range outs {
				if o.Skip != SkipLevel {
					t.Errorf("level %d: skip = %v", tc.level, o.Skip)
				}
			}
		}
	}
}

func TestMixedNetworksOnlyEligibleGetRecords(t *testing.T) {
	e := newEnv(t)
	other := netid.NetworkID{7}
	e.nets = []Network{{ID: openNet, Level: 1}, {ID: privNet, Level: 3}, {ID: other, Level: 0}}
	if _, err := e.publisher(t).Publish(context.Background(), healthy(), info()); err != nil {
		t.Fatal(err)
	}
	if len(e.sink.got) != 2 {
		t.Fatalf("sunk %d", len(e.sink.got))
	}
	for _, s := range e.sink.got {
		if s.nw != privNet {
			t.Fatalf("record went to %s", s.nw)
		}
		env, _ := records.Peek(s.enc)
		if env.NetworkID != privNet {
			t.Fatalf("record is signed for %s", env.NetworkID)
		}
	}
}

func TestRecordsVerifyAndCarryExpectedData(t *testing.T) {
	e := newEnv(t)
	e.bb.provider = []string{"Omicron", "Eweka Internet Services"}
	e.cfg.Backbones = e.bb
	if _, err := e.publisher(t).Publish(context.Background(), healthy(), info()); err != nil {
		t.Fatal(err)
	}
	var sawRel, sawAtt bool
	for _, s := range e.sink.got {
		env, _ := records.Peek(s.enc)
		switch env.Kind {
		case records.KindRelease:
			r, err := records.DecodeReleaseRecord(s.enc)
			if err != nil {
				t.Fatal(err)
			}
			if err := r.Verify(t0, openNet); err != nil {
				t.Fatal(err)
			}
			if r.Title != "Some.Movie.2020.1080p" || r.FileCount != 3 || r.Media.IMDb != "tt0133093" || r.TotalSize != 5<<30 {
				t.Fatalf("record %+v", r)
			}
			if r.PublisherID != e.key.k.NodeID() {
				t.Fatal("publisher mismatch")
			}
			sawRel = true
		case records.KindAttestation:
			a, err := records.DecodeHealthAttestation(s.enc)
			if err != nil {
				t.Fatal(err)
			}
			if err := a.Verify(t0, openNet); err != nil {
				t.Fatal(err)
			}
			if !a.Healthy() || a.FileID != fileID || a.ReleaseID != relID || !a.MediaHeaderVerified ||
				a.TotalSegments != 100 || a.SegmentsChecked != 10 {
				t.Fatalf("attestation %+v", a)
			}
			if strings.Join(a.Backbones, ",") != "eweka-internet-services,omicron" {
				t.Fatalf("backbones %v", a.Backbones)
			}
			sawAtt = true
		}
	}
	if !sawRel || !sawAtt {
		t.Fatalf("rel=%v att=%v", sawRel, sawAtt)
	}
}

func TestExclusionsApplyBeforeAnythingIsSigned(t *testing.T) {
	for _, tc := range []struct {
		name string
		excl Exclusions
		info func(*ReleaseInfo)
		skip SkipReason
	}{
		{"category", Exclusions{Categories: []string{"movies"}}, func(*ReleaseInfo) {}, SkipExcludedCategory},
		{"category padded and cased", Exclusions{Categories: []string{"  MOVIES "}}, func(*ReleaseInfo) {}, SkipExcludedCategory},
		{"indexer", Exclusions{Indexers: []string{"indexerone"}}, func(*ReleaseInfo) {}, SkipExcludedIndexer},
		{"indexer with padding in release", Exclusions{Indexers: []string{"indexerone"}}, func(i *ReleaseInfo) { i.Indexer = " IndexerOne " }, SkipExcludedIndexer},
	} {
		e := newEnv(t)
		e.excl = tc.excl
		in := info()
		tc.info(&in)
		outs, err := e.publisher(t).Publish(context.Background(), healthy(), in)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if len(e.sink.got) != 0 || e.key.calls != 0 {
			t.Errorf("%s: sunk=%d key calls=%d", tc.name, len(e.sink.got), e.key.calls)
		}
		if len(outs) != 1 || outs[0].Skip != tc.skip {
			t.Errorf("%s: outcomes %+v", tc.name, outs)
		}
		if len(e.store.m) != 0 {
			t.Errorf("%s: state written", tc.name)
		}
	}
}

func TestNonExcludedPublishes(t *testing.T) {
	e := newEnv(t)
	e.excl = Exclusions{Categories: []string{"tv"}, Indexers: []string{"other"}}
	if _, err := e.publisher(t).Publish(context.Background(), healthy(), info()); err != nil {
		t.Fatal(err)
	}
	if len(e.sink.got) != 2 {
		t.Fatalf("sunk %d", len(e.sink.got))
	}
}

func TestUnknownIndexerAndCategoryPolicy(t *testing.T) {
	cases := []struct {
		name     string
		policy   UnknownPolicy
		excl     Exclusions
		indexer  string
		category string
		want     SkipReason
	}{
		{"default no exclusions publishes", UnknownBlockIfExclusions, Exclusions{}, "", "Movies", SkipNone},
		{"default indexer exclusions set blocks empty", UnknownBlockIfExclusions, Exclusions{Indexers: []string{"x"}}, "", "Movies", SkipUnknownIndexer},
		{"default indexer exclusions set blocks Unknown", UnknownBlockIfExclusions, Exclusions{Indexers: []string{"x"}}, "Unknown", "Movies", SkipUnknownIndexer},
		{"default category exclusions blocks empty category", UnknownBlockIfExclusions, Exclusions{Categories: []string{"x"}}, "IndexerOne", "", SkipUnknownCategory},
		{"default only category exclusions, unknown indexer publishes", UnknownBlockIfExclusions, Exclusions{Categories: []string{"x"}}, "", "Movies", SkipNone},
		{"publish policy ignores unknowns", UnknownPublish, Exclusions{Indexers: []string{"x"}, Categories: []string{"y"}}, "", "", SkipNone},
		{"block policy blocks unknown even with no exclusions", UnknownBlock, Exclusions{}, "", "Movies", SkipUnknownIndexer},
		{"block policy known publishes", UnknownBlock, Exclusions{}, "IndexerOne", "Movies", SkipNone},
	}
	for _, tc := range cases {
		e := newEnv(t)
		e.cfg.Unknown = tc.policy
		e.excl = tc.excl
		in := info()
		in.Indexer, in.Category = tc.indexer, tc.category
		outs, err := e.publisher(t).Publish(context.Background(), healthy(), in)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if tc.want == SkipNone {
			if len(e.sink.got) != 2 {
				t.Errorf("%s: expected publication, outcomes %+v", tc.name, outs)
			}
			continue
		}
		if len(e.sink.got) != 0 || e.key.calls != 0 || len(outs) != 1 || outs[0].Skip != tc.want {
			t.Errorf("%s: sunk=%d outs=%+v", tc.name, len(e.sink.got), outs)
		}
	}
}

func TestAttestationSkipPublishesTheReleaseRecordOnly(t *testing.T) {
	e := newEnv(t)
	res := healthy()
	res.AttestationSkip = SkipReason("some_reason")
	outs, err := e.publisher(t).Publish(context.Background(), res, info())
	if err != nil {
		t.Fatal(err)
	}
	rel, att := kinds(t, e.sink)
	if rel != 1 || att != 0 {
		t.Fatalf("%d release / %d attestation", rel, att)
	}
	found := false
	for _, o := range outs {
		if o.Kind == records.KindAttestation && o.Skip == "some_reason" {
			found = true
		}
	}
	if !found {
		t.Fatalf("no attestation skip with the given reason: %+v", outs)
	}
}

func TestPatchedFilesAreNeverAttested(t *testing.T) {
	for _, v := range []Verdict{VerdictHealthy, VerdictUnhealthy} {
		e := newEnv(t)
		e.cfg.PublishUnhealthy = true
		res := healthy()
		res.PatchedByPAR2 = true
		if v == VerdictUnhealthy {
			res.Verdict, res.MissingCount = v, 2
		}
		outs, err := e.publisher(t).Publish(context.Background(), res, info())
		if err != nil {
			t.Fatal(err)
		}
		rel, att := kinds(t, e.sink)
		if att != 0 {
			t.Errorf("verdict %v: patched file attested", v)
		}
		if v == VerdictHealthy && rel != 1 {
			t.Errorf("release record should still be published, got %d", rel)
		}
		found := false
		for _, o := range outs {
			if o.Kind == records.KindAttestation && o.Skip == SkipPatched {
				found = true
			}
		}
		if !found {
			t.Errorf("verdict %v: no SkipPatched outcome: %+v", v, outs)
		}
	}
}

func TestBackboneRules(t *testing.T) {
	for _, tc := range []struct {
		name     string
		provider []string
		known    []string
		wantAtt  string // "" means no attestation
	}{
		{"normalised and filtered", []string{" OMICRON ", "Made Up Net", ""}, []string{"omicron", "Eweka"}, "omicron"},
		{"unknown label only", []string{"Made Up Net"}, []string{"Omicron"}, ""},
		{"no providers", nil, []string{"Omicron"}, ""},
		{"known list unavailable fails closed", []string{"Omicron"}, nil, ""},
		{"invalid label chars are dropped", []string{"Om/icron"}, []string{"Om/icron"}, ""},
		{"duplicates collapse", []string{"Omicron", "omicron", "OMICRON"}, []string{"Omicron"}, "omicron"},
		{"spaces become dashes on both sides", []string{"Eweka Internet Services"}, []string{"eweka   internet services"}, "eweka-internet-services"},
	} {
		e := newEnv(t)
		e.bb = fakeBackbones{provider: tc.provider, known: tc.known}
		e.cfg.Backbones = e.bb
		outs, err := e.publisher(t).Publish(context.Background(), healthy(), info())
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		rel, att := kinds(t, e.sink)
		if rel != 1 {
			t.Errorf("%s: release record must not depend on backbones", tc.name)
		}
		if tc.wantAtt == "" {
			if att != 0 {
				t.Errorf("%s: attested without a recognised backbone", tc.name)
			}
			if published(outs, records.KindAttestation) != 0 {
				t.Errorf("%s: outcome claims publication", tc.name)
			}
			continue
		}
		for _, s := range e.sink.got {
			if a, err := records.DecodeHealthAttestation(s.enc); err == nil {
				if strings.Join(a.Backbones, ",") != tc.wantAtt {
					t.Errorf("%s: backbones %v", tc.name, a.Backbones)
				}
			}
		}
	}
}

func TestBackboneCountIsCapped(t *testing.T) {
	e := newEnv(t)
	var names []string
	for i := 0; i < records.MaxBackbones+5; i++ {
		names = append(names, "bb"+string(rune('a'+i)))
	}
	e.bb = fakeBackbones{provider: names, known: names}
	e.cfg.Backbones = e.bb
	if _, err := e.publisher(t).Publish(context.Background(), healthy(), info()); err != nil {
		t.Fatal(err)
	}
	for _, s := range e.sink.got {
		if a, err := records.DecodeHealthAttestation(s.enc); err == nil && len(a.Backbones) != records.MaxBackbones {
			t.Fatalf("backbones %d", len(a.Backbones))
		}
	}
}

func TestTitleIsCleanedAndNeverAPath(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"Some.Movie.2020.1080p", "Some.Movie.2020.1080p"},
		{"/mnt/media/Movies/Some Movie (2020)/Some.Movie.2020.nzb", "Some.Movie.2020"},
		{`C:\Users\bob\Downloads\Some.Movie.2020.NZB`, "Some.Movie.2020"},
		{"Some.Movie.2020.nzb", "Some.Movie.2020"},
		{"Some\x00.Movie\n.2020\u202e", "Some .Movie .2020"},
		{"dir/", ""},
		{"   ", ""},
	} {
		if got := releaseTitle(tc.in); got != tc.want {
			t.Errorf("releaseTitle(%q) = %q want %q", tc.in, got, tc.want)
		}
	}
}

func TestNothingLocalLeaksIntoRecords(t *testing.T) {
	e := newEnv(t)
	e.bb = fakeBackbones{provider: []string{"Omicron"}, known: []string{"Omicron"}}
	e.cfg.Backbones = e.bb
	in := info()
	in.Title = "/srv/secret-library/radarr-4k/Some.Movie.2020.nzb"
	in.Indexer = "PrivateIndexerName"
	in.Category = "mycategory-xyz"
	if _, err := e.publisher(t).Publish(context.Background(), healthy(), in); err != nil {
		t.Fatal(err)
	}
	if len(e.sink.got) != 2 {
		t.Fatalf("sunk %d", len(e.sink.got))
	}
	for _, s := range e.sink.got {
		for _, bad := range []string{"srv", "secret", "radarr", "PrivateIndexerName", "mycategory", "xyz"} {
			if bytes.Contains(s.enc, []byte(bad)) {
				t.Errorf("record leaks %q", bad)
			}
		}
	}
}

func TestEmptyTitleIsAnError(t *testing.T) {
	e := newEnv(t)
	in := info()
	in.Title = "/only/a/dir/"
	_, err := e.publisher(t).Publish(context.Background(), healthy(), in)
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("err = %v", err)
	}
	if len(e.sink.got) != 0 {
		t.Fatal("something was published")
	}
}

func TestInvalidInputs(t *testing.T) {
	for name, mut := range map[string]func(*CheckResult, *ReleaseInfo){
		"zero release":    func(r *CheckResult, _ *ReleaseInfo) { r.ReleaseID = records.ReleaseID{} },
		"zero file":       func(r *CheckResult, _ *ReleaseInfo) { r.FileID = records.FileID{} },
		"zero checked at": func(r *CheckResult, _ *ReleaseInfo) { r.CheckedAt = time.Time{} },
		"no verdict":      func(r *CheckResult, _ *ReleaseInfo) { r.Verdict = 0 },
		"zero files":      func(_ *CheckResult, i *ReleaseInfo) { i.FileCount = 0 },
		"negative size":   func(_ *CheckResult, i *ReleaseInfo) { i.TotalSize = -1 },
	} {
		e := newEnv(t)
		res, in := healthy(), info()
		mut(&res, &in)
		_, err := e.publisher(t).Publish(context.Background(), res, in)
		if !errors.Is(err, ErrInvalidInput) {
			t.Errorf("%s: err = %v", name, err)
		}
		if len(e.sink.got) != 0 {
			t.Errorf("%s: published", name)
		}
	}
}

func TestInvalidOptionalDataIsDroppedNotFatal(t *testing.T) {
	e := newEnv(t)
	in := info()
	in.Media = records.MediaIDs{IMDb: "not-an-imdb-id"}
	in.PostDate = time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, err := e.publisher(t).Publish(context.Background(), healthy(), in); err != nil {
		t.Fatal(err)
	}
	for _, s := range e.sink.got {
		if r, err := records.DecodeReleaseRecord(s.enc); err == nil {
			if r.Media.IMDb != "" || !r.PostDate.IsZero() {
				t.Fatalf("invalid optional data kept: %+v", r)
			}
			return
		}
	}
	t.Fatal("no release record published")
}

func TestUnhealthyPolicy(t *testing.T) {
	unhealthy := func() CheckResult {
		r := healthy()
		r.Verdict, r.MissingCount, r.MediaHeaderVerified = VerdictUnhealthy, 3, false
		return r
	}
	cases := []struct {
		name    string
		enable  bool
		mut     func(*CheckResult)
		wantAtt int
		skip    SkipReason
	}{
		{"off by default", false, func(*CheckResult) {}, 0, SkipUnhealthyNotShared},
		{"enabled and observed", true, func(*CheckResult) {}, 1, SkipNone},
		{"enabled but inconclusive", true, func(r *CheckResult) { r.Verdict = VerdictInconclusive }, 0, SkipInconclusive},
		{"inconclusive never published even with misses", true, func(r *CheckResult) { r.Verdict = VerdictInconclusive; r.MissingCount = 1 }, 0, SkipInconclusive},
		{"unhealthy claim without observed misses", true, func(r *CheckResult) { r.MissingCount = 0 }, 0, SkipInconclusive},
		{"inconclusive default", false, func(r *CheckResult) { r.Verdict = VerdictInconclusive }, 0, SkipInconclusive},
	}
	for _, tc := range cases {
		e := newEnv(t)
		e.cfg.PublishUnhealthy = tc.enable
		res := unhealthy()
		tc.mut(&res)
		outs, err := e.publisher(t).Publish(context.Background(), res, info())
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		rel, att := kinds(t, e.sink)
		if rel != 0 {
			t.Errorf("%s: release record published for an unhealthy file", tc.name)
		}
		if att != tc.wantAtt {
			t.Errorf("%s: attestations %d want %d (%+v)", tc.name, att, tc.wantAtt, outs)
		}
		if tc.wantAtt == 0 {
			got := SkipNone
			for _, o := range outs {
				got = o.Skip
			}
			if got != tc.skip {
				t.Errorf("%s: skip %v want %v", tc.name, got, tc.skip)
			}
		}
		if tc.wantAtt == 1 {
			for _, s := range e.sink.got {
				a, _ := records.DecodeHealthAttestation(s.enc)
				if a.Healthy() || a.MissingCount != 3 {
					t.Errorf("%s: %+v", tc.name, a)
				}
			}
		}
	}
}

func TestPeerBundleOriginNeedsFullCheck(t *testing.T) {
	for _, tc := range []struct {
		name string
		mut  func(*CheckResult)
		want int
	}{
		{"not full checked", func(r *CheckResult) { r.FromPeerBundle = true }, 0},
		{"flag set but only sampled", func(r *CheckResult) { r.FromPeerBundle, r.FullCheckPassed = true, true }, 0},
		{"full check passed", func(r *CheckResult) {
			r.FromPeerBundle, r.FullCheckPassed, r.SegmentsChecked = true, true, r.TotalSegments
		}, 2},
		{"full flag but not healthy", func(r *CheckResult) {
			r.FromPeerBundle, r.FullCheckPassed, r.SegmentsChecked, r.Verdict = true, true, r.TotalSegments, VerdictInconclusive
		}, 0},
		{"local import unaffected", func(*CheckResult) {}, 2},
	} {
		e := newEnv(t)
		res := healthy()
		tc.mut(&res)
		outs, err := e.publisher(t).Publish(context.Background(), res, info())
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if len(e.sink.got) != tc.want {
			t.Errorf("%s: sunk %d want %d (%+v)", tc.name, len(e.sink.got), tc.want, outs)
		}
		if tc.want == 0 && e.key.calls != 0 {
			t.Errorf("%s: signed something", tc.name)
		}
	}
}

func TestRateControl(t *testing.T) {
	e := newEnv(t)
	p := e.publisher(t)
	ctx := context.Background()
	if _, err := p.Publish(ctx, healthy(), info()); err != nil {
		t.Fatal(err)
	}
	if len(e.sink.got) != 2 {
		t.Fatalf("first publish sunk %d", len(e.sink.got))
	}

	// Same verdict, new check an hour later: too soon.
	e.now = t0.Add(time.Hour)
	res := healthy()
	res.CheckedAt = e.now.Add(-time.Second)
	outs, _ := p.Publish(ctx, res, info())
	if len(e.sink.got) != 2 {
		t.Fatalf("identical verdict re-published within the minimum interval")
	}
	for _, o := range outs {
		if o.Skip != SkipNotDue {
			t.Errorf("skip %v", o.Skip)
		}
	}

	// Same check data again, after the interval: nothing new to say.
	e.now = t0.Add(25 * time.Hour)
	if _, err := p.Publish(ctx, healthy(), info()); err != nil {
		t.Fatal(err)
	}
	if len(e.sink.got) != 2 {
		t.Fatal("re-published a check that is not newer than the last published one")
	}

	// A newer check after the interval: attestation due, release record not.
	res.CheckedAt = e.now.Add(-time.Minute)
	if _, err := p.Publish(ctx, res, info()); err != nil {
		t.Fatal(err)
	}
	if rel, att := kinds(t, e.sink); rel != 1 || att != 2 {
		t.Fatalf("rel=%d att=%d", rel, att)
	}

	// Verdict change publishes immediately.
	e.cfg.PublishUnhealthy = true
	p2 := e.publisher(t)
	e.now = e.now.Add(time.Minute)
	bad := healthy()
	bad.Verdict, bad.MissingCount, bad.CheckedAt = VerdictUnhealthy, 1, e.now.Add(-time.Second)
	if _, err := p2.Publish(ctx, bad, info()); err != nil {
		t.Fatal(err)
	}
	if _, att := kinds(t, e.sink); att != 3 {
		t.Fatalf("verdict change not published, att=%d", att)
	}

	// The release record is re-announced before it expires.
	e.now = t0.Add(21 * 24 * time.Hour)
	res.CheckedAt = e.now.Add(-time.Minute)
	if _, err := p2.Publish(ctx, res, info()); err != nil {
		t.Fatal(err)
	}
	if rel, _ := kinds(t, e.sink); rel != 2 {
		t.Fatalf("release record not re-announced, rel=%d", rel)
	}
}

func TestSinkFailureDoesNotMarkPublishedAndOtherNetworksProceed(t *testing.T) {
	e := newEnv(t)
	e.nets = []Network{{ID: openNet, Level: 2}, {ID: privNet, Level: 2}}
	boom := errors.New("sink down")
	e.sink.fail[openNet] = boom
	outs, err := e.publisher(t).Publish(context.Background(), healthy(), info())
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	for _, s := range e.sink.got {
		if s.nw != privNet {
			t.Fatal("record reached a failing network")
		}
	}
	if len(e.sink.got) != 2 {
		t.Fatalf("private network not served: %d", len(e.sink.got))
	}
	for k := range e.store.m {
		if k.nw == openNet {
			t.Fatal("state recorded for failed network")
		}
	}
	failed := 0
	for _, o := range outs {
		if o.Err != nil {
			failed++
		}
	}
	if failed != 2 {
		t.Fatalf("failed outcomes %d", failed)
	}
}

func TestStoreErrors(t *testing.T) {
	e := newEnv(t)
	e.store.readErr = errors.New("read broke")
	if _, err := e.publisher(t).Publish(context.Background(), healthy(), info()); err == nil {
		t.Fatal("expected read error")
	}
	if len(e.sink.got) != 0 {
		t.Fatal("published without knowing the last state")
	}
	e = newEnv(t)
	e.store.markErr = errors.New("mark broke")
	if _, err := e.publisher(t).Publish(context.Background(), healthy(), info()); err == nil {
		t.Fatal("expected mark error")
	}
}

func TestContextCancelled(t *testing.T) {
	e := newEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := e.publisher(t).Publish(ctx, healthy(), info())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if len(e.sink.got) != 0 {
		t.Fatal("published on a cancelled context")
	}
}

func TestNilKeyFromSource(t *testing.T) {
	e := newEnv(t)
	e.key.k = nil
	if _, err := e.publisher(t).Publish(context.Background(), healthy(), info()); err == nil {
		t.Fatal("expected error when key is unavailable")
	}
	if len(e.sink.got) != 0 {
		t.Fatal("published without a key")
	}
}

func TestReannounceReleasePublishesOnlyTheReleaseRecordWhenDue(t *testing.T) {
	e := newEnv(t)
	p := e.publisher(t)
	// First: published normally together with an attestation.
	if _, err := p.Publish(context.Background(), healthy(), info()); err != nil {
		t.Fatal(err)
	}
	if rel, att := kinds(t, e.sink); rel != 1 || att != 1 {
		t.Fatalf("setup: %d/%d", rel, att)
	}

	// Not yet due: nothing is sent.
	e.now = t0.Add(DefaultRecordRefresh - time.Hour)
	o := p.ReannounceRelease(context.Background(), openNet, relID, info())
	if o.Skip != SkipNotDue || o.Err != nil || len(e.sink.got) != 2 {
		t.Fatalf("outcome %+v, sunk %d", o, len(e.sink.got))
	}

	// Due: a fresh release record, no attestation, state moves forward.
	e.now = t0.Add(DefaultRecordRefresh + time.Hour)
	o = p.ReannounceRelease(context.Background(), openNet, relID, info())
	if o.Skip != SkipNone || o.Err != nil || o.Kind != records.KindRelease {
		t.Fatalf("outcome %+v", o)
	}
	if rel, att := kinds(t, e.sink); rel != 2 || att != 1 {
		t.Fatalf("after re-announce: %d release / %d attestation records", rel, att)
	}
	st, _, _ := e.store.LastPublished(context.Background(), openNet, records.KindRelease, relID, records.FileID{})
	if !st.PublishedAt.Equal(e.now) {
		t.Fatalf("state not advanced: %v", st.PublishedAt)
	}
}

func TestReannounceReleaseHonoursLevelExclusionsAndNeverBuiltBefore(t *testing.T) {
	e := newEnv(t)
	e.excl = Exclusions{Categories: []string{"movies"}}
	p := e.publisher(t)
	e.now = t0.Add(DefaultRecordRefresh + time.Hour)
	if o := p.ReannounceRelease(context.Background(), openNet, relID, info()); o.Skip != SkipExcludedCategory || e.key.calls != 0 {
		t.Fatalf("an excluded release must not be re-announced: %+v calls=%d", o, e.key.calls)
	}

	e.excl = Exclusions{}
	e.nets = []Network{{ID: openNet, Level: 1}}
	if o := p.ReannounceRelease(context.Background(), openNet, relID, info()); o.Skip != SkipLevel || e.key.calls != 0 {
		t.Fatalf("level 1 must not publish: %+v", o)
	}

	// A network this node does not publish to at all.
	e.nets = []Network{{ID: openNet, Level: 2}}
	if o := p.ReannounceRelease(context.Background(), privNet, relID, info()); o.Skip != SkipLevel {
		t.Fatalf("unknown network: %+v", o)
	}
	if len(e.sink.got) != 0 {
		t.Fatal("nothing may be sent")
	}
}

func TestReleaseTitleStripsPasswordForms(t *testing.T) {
	for in, want := range map[string]string{
		"Name{{x}}.nzb":           "Name",
		"Name{{ x }}.nzb":         "Name",
		"Name/password:x.nzb":     "Name",
		"/q/12/Name{{x}}.nzb":     "Name",
		"Name {{unterminated.nzb": "Name unterminated",
	} {
		if got := releaseTitle(in); got != want {
			t.Errorf("releaseTitle(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestTitleLeaks(t *testing.T) {
	for _, c := range []struct {
		title   string
		secrets []string
		want    bool
	}{
		{"Plain.Title", nil, false},
		{"Has {{ marker", nil, true},
		{"Has }} marker", nil, true},
		{"Title With HUNTER2pw inside", []string{"hunter2PW"}, true},
		{"Title", []string{"abc"}, false}, // below the minimum length
		{"Plain.Title", []string{"other-secret"}, false},
	} {
		if got := titleLeaks(c.title, c.secrets); got != c.want {
			t.Errorf("titleLeaks(%q,%v)=%v", c.title, c.secrets, got)
		}
	}
}
