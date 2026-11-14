package bridge

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/importer"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/peernet/bundle"
	"github.com/kipsilabs/altmount/internal/peernet/bundleimport"
	"github.com/kipsilabs/altmount/internal/peernet/contentid"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeAcceptor struct {
	reqs []bundleimport.Request
	res  *bundleimport.Result
	err  error
}

func (a *fakeAcceptor) Accept(_ context.Context, r bundleimport.Request) (*bundleimport.Result, error) {
	a.reqs = append(a.reqs, r)
	return a.res, a.err
}
func (a *fakeAcceptor) QueueHasPending(context.Context, int64) bool        { return true }
func (a *fakeAcceptor) AttachQueue(context.Context, []string, int64) error { return nil }

type blockingFetcher struct {
	b     *bundle.Bundle
	err   error
	calls atomic.Int32
	ids   []string
	block bool
}

func (f *blockingFetcher) FetchBundle(ctx context.Context, id string) (*bundle.Bundle, bundleimport.Origin, error) {
	f.calls.Add(1)
	f.ids = append(f.ids, id)
	if f.block {
		<-ctx.Done()
		return nil, bundleimport.Origin{}, ctx.Err()
	}
	return f.b, bundleimport.Origin{NetworkID: "n", PublisherID: "p"}, f.err
}

func nzbStore() *metapb.NzbStore {
	return &metapb.NzbStore{Files: []*metapb.NzbFileEntry{
		{Segments: []*metapb.NzbSeg{{Id: "a@x"}, {Id: "<b@x>"}}},
		{Segments: []*metapb.NzbSeg{{Id: "c@x"}}},
	}}
}

func hook(t *testing.T, a *fakeAcceptor, f *blockingFetcher, enabled bool, timeout time.Duration) importer.PeerBundleFastPath {
	t.Helper()
	h := NewPeerBundleFastPath(FastPathConfig{Acceptor: a, Fetcher: f, Enabled: func() bool { return enabled }, FetchTimeout: timeout})
	require.NotNil(t, h)
	return h
}

func matching(t *testing.T) *bundle.Bundle {
	rid, err := contentid.ReleaseID(nzbStore())
	require.NoError(t, err)
	return &bundle.Bundle{Scheme: bundle.SchemeNZB, ReleaseID: rid}
}

func req() importer.PeerBundleRequest {
	return importer.PeerBundleRequest{NZB: nzbStore(), QueueID: 3, TargetDir: "movies/R", Category: "movies", SkipArr: true}
}

func TestFastPath_DisabledDoesNothing(t *testing.T) {
	f := &blockingFetcher{b: matching(t)}
	_, ok := hook(t, &fakeAcceptor{}, f, false, time.Second).TryBundle(context.Background(), req())
	assert.False(t, ok)
	assert.Zero(t, f.calls.Load())
}

func TestFastPath_NilCollaboratorsYieldNoHook(t *testing.T) {
	assert.Nil(t, NewPeerBundleFastPath(FastPathConfig{Enabled: func() bool { return true }}))
}

func TestFastPath_TakenAsksForTheExactReleaseAndCrossChecksTheNZB(t *testing.T) {
	a := &fakeAcceptor{res: &bundleimport.Result{Dir: "movies/R", Paths: []string{"movies/R/a.mkv"}, TotalSize: 5}}
	f := &blockingFetcher{b: matching(t)}
	out, ok := hook(t, a, f, true, time.Second).TryBundle(context.Background(), req())
	require.True(t, ok)
	assert.Equal(t, importer.PeerBundleOutcome{ResultPath: "movies/R", WrittenPaths: []string{"movies/R/a.mkv"}, TotalSize: 5}, out)
	rid, _ := contentid.ReleaseID(nzbStore())
	assert.Equal(t, []string{rid.String()}, f.ids)
	require.Len(t, a.reqs, 1)
	assert.Equal(t, [][]string{{"a@x", "b@x"}, {"c@x"}}, a.reqs[0].ExpectedNZB, "the user's own list, canonical")
	assert.Equal(t, bundleimport.Target{Dir: "movies/R", Category: "movies", QueueID: 3, SkipArr: true}, a.reqs[0].Target)
}

func TestFastPath_FailsOpen(t *testing.T) {
	other := matching(t)
	other.ReleaseID[0] ^= 1
	files := matching(t)
	files.Scheme = bundle.SchemeFiles
	for name, tc := range map[string]struct {
		f *blockingFetcher
		a *fakeAcceptor
	}{
		"fetch error":      {&blockingFetcher{err: errors.New("no peers")}, &fakeAcceptor{}},
		"nothing found":    {&blockingFetcher{}, &fakeAcceptor{}},
		"other release":    {&blockingFetcher{b: other}, &fakeAcceptor{}},
		"files scheme":     {&blockingFetcher{b: files}, &fakeAcceptor{}},
		"accept rejection": {&blockingFetcher{b: matching(t)}, &fakeAcceptor{err: errors.New("rejected")}},
	} {
		t.Run(name, func(t *testing.T) {
			_, ok := hook(t, tc.a, tc.f, true, time.Second).TryBundle(context.Background(), req())
			assert.False(t, ok)
		})
	}
}

func TestFastPath_SlowFetchIsCutOffAtTheDeadline(t *testing.T) {
	f := &blockingFetcher{block: true}
	start := time.Now()
	_, ok := hook(t, &fakeAcceptor{}, f, true, 50*time.Millisecond).TryBundle(context.Background(), req())
	assert.False(t, ok)
	assert.Less(t, time.Since(start), 2*time.Second, "never slower than the deadline")
}

func TestReleaseProberMapsErrors(t *testing.T) {
	for in, want := range map[error]error{
		nil:                               nil,
		importer.ErrPeerProbeMissing:      bundleimport.ErrProbeMissing,
		importer.ErrPeerProbeInconclusive: bundleimport.ErrProbeInconclusive,
		errors.New("anything else"):       bundleimport.ErrProbeInconclusive,
	} {
		err := NewReleaseProber(probeFn(func() error { return in })).ProbeRelease(context.Background(), []bundleimport.ProbeFile{{Name: "a", SegmentIDs: []string{"x"}}})
		assert.Equal(t, want, err)
	}
	_ = strings.Repeat
}

type probeFn func() error

func (p probeFn) ProbePeerRelease(context.Context, []importer.PeerProbeList) error { return p() }

type fakeBundleService struct {
	b      *bundle.Bundle
	src    service.FetchSource
	err    error
	noted  []string
	gotRel string
}

func (f *fakeBundleService) FetchBundle(_ context.Context, rel string) (*bundle.Bundle, service.FetchSource, error) {
	f.gotRel = rel
	return f.b, f.src, f.err
}
func (f *fakeBundleService) NoteBundleFullCheck(n, p string, passed bool) {
	f.noted = append(f.noted, n+"/"+p+"/"+map[bool]string{true: "pass", false: "fail"}[passed])
}

func TestBundleFetcherAdapterMapsSourceToOrigin(t *testing.T) {
	nw, pub := netid.Open(), identity.NodeID{1}
	svc := &fakeBundleService{b: &bundle.Bundle{}, src: service.FetchSource{Network: nw, Publisher: pub}}
	b, o, err := NewBundleFetcher(svc).FetchBundle(context.Background(), "rel")
	require.NoError(t, err)
	assert.Same(t, svc.b, b)
	assert.Equal(t, bundleimport.Origin{NetworkID: nw.String(), PublisherID: pub.String(), Open: true}, o)
	assert.Equal(t, "rel", svc.gotRel)
	svc.err = errors.New("no offers")
	_, _, err = NewBundleFetcher(svc).FetchBundle(context.Background(), "rel")
	assert.Error(t, err)
	assert.Nil(t, NewBundleFetcher(nil))
}

func TestBundleReporterAdapterPassesFailureAndSuccess(t *testing.T) {
	svc := &fakeBundleService{}
	rep := NewBundleReporter(svc)
	o := bundleimport.Origin{NetworkID: "n", PublisherID: "p"}
	rep.ReportBadBundle(context.Background(), o, "rel")
	rep.(bundleimport.BundleVerifiedReporter).ReportVerifiedBundle(context.Background(), o, "rel")
	assert.Equal(t, []string{"n/p/fail", "n/p/pass"}, svc.noted)
	assert.Nil(t, NewBundleReporter(nil))
}

type originFetcher struct {
	b *bundle.Bundle
	o bundleimport.Origin
}

func (f originFetcher) FetchBundle(context.Context, string) (*bundle.Bundle, bundleimport.Origin, error) {
	return f.b, f.o, nil
}

func TestFastPath_OpenNetworkNeedsTheSwitchAndTwoCorroboratingPublishers(t *testing.T) {
	cases := []struct {
		name   string
		origin bundleimport.Origin
		on     bool
		taken  bool
	}{
		{"private network, switch off", bundleimport.Origin{NetworkID: "p", Open: false}, false, true},
		{"open network, switch off", bundleimport.Origin{NetworkID: "o", Open: true, Corroborators: 5}, false, false},
		{"open network, switch on, one publisher", bundleimport.Origin{NetworkID: "o", Open: true, Corroborators: 1}, true, false},
		{"open network, switch on, two publishers", bundleimport.Origin{NetworkID: "o", Open: true, Corroborators: 2}, true, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			a := &fakeAcceptor{res: &bundleimport.Result{Dir: "movies/R", Paths: []string{"movies/R/a.mkv"}}}
			h := NewPeerBundleFastPath(FastPathConfig{Acceptor: a, Fetcher: originFetcher{b: matching(t), o: c.origin},
				Enabled: func() bool { return true }, OpenFastPath: func() bool { return c.on }})
			_, ok := h.TryBundle(context.Background(), req())
			assert.Equal(t, c.taken, ok)
			assert.Equal(t, c.taken, len(a.reqs) == 1, "an open offer that fails the policy is never even accepted")
		})
	}
}

type slowAcceptor struct{ fakeAcceptor }

func (a *slowAcceptor) Accept(ctx context.Context, r bundleimport.Request) (*bundleimport.Result, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestFastPath_VerificationThatDoesNotFitDeclinesInsteadOfStallingTheImport(t *testing.T) {
	h := NewPeerBundleFastPath(FastPathConfig{Acceptor: &slowAcceptor{}, Fetcher: originFetcher{b: matching(t), o: bundleimport.Origin{NetworkID: "p"}},
		Enabled: func() bool { return true }, AcceptTimeout: 60 * time.Millisecond})
	start := time.Now()
	_, ok := h.TryBundle(context.Background(), req())
	assert.False(t, ok)
	assert.Less(t, time.Since(start), 2*time.Second)
}
