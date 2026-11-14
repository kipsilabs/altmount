package cmd

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/kipsilabs/altmount/internal/api"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/health"
	"github.com/kipsilabs/altmount/internal/importer"
	"github.com/kipsilabs/altmount/internal/peernet/bundle"
	"github.com/kipsilabs/altmount/internal/peernet/bundleimport"
	"github.com/kipsilabs/altmount/internal/peernet/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recFastPath struct{ cur importer.PeerBundleFastPath }

func (r *recFastPath) SetPeerBundleFastPath(h importer.PeerBundleFastPath) { r.cur = h }

type recGate struct{ cur health.PeerBundleGate }

func (r *recGate) SetPeerBundleGate(g health.PeerBundleGate) { r.cur = g }

type recLookup struct{ cur api.PeerBundleLookup }

func (r *recLookup) SetPeerBundleLookup(l api.PeerBundleLookup) { r.cur = l }

type okFetcher struct{}

func (okFetcher) FetchBundle(context.Context, string) (*bundle.Bundle, bundleimport.Origin, error) {
	return nil, bundleimport.Origin{}, nil
}

func bundleHostFixture(t *testing.T) (*peernetBundleHost, *recFastPath, *recGate, *recLookup, *int) {
	t.Helper()
	fp, g, l, built := &recFastPath{}, &recGate{}, &recLookup{}, new(int)
	h := &peernetBundleHost{
		importerSvc: fp, health: g, api: l, seams: &peernetBundleSeams{}, receiving: func() bool { return true },
		build: func() *bundleimport.Importer { *built++; return bundleimport.New(bundleimport.Config{}) },
	}
	return h, fp, g, l, built
}

func enabledCfg(on bool) *config.Config {
	cfg := config.DefaultConfig()
	cfg.Peernet.Enabled = &on
	return cfg
}

func TestBundleHost_FeatureOffInstallsNothingAndBuildsNothing(t *testing.T) {
	h, fp, g, l, built := bundleHostFixture(t)
	h.apply(enabledCfg(false))
	assert.Nil(t, fp.cur)
	assert.Nil(t, g.cur)
	assert.Nil(t, l.cur)
	assert.Zero(t, *built, "no importer, no database access, nothing")
	assert.Nil(t, h.Importer())
}

func TestBundleHost_EnabledWithoutFetcherInstallsGateAndBadgesOnly(t *testing.T) {
	h, fp, g, l, built := bundleHostFixture(t)
	h.lookup = nil
	h.apply(enabledCfg(true))
	assert.NotNil(t, g.cur, "pending peer imports are honoured even before a fetcher exists")
	assert.Nil(t, fp.cur, "no fast path without a fetcher")
	_ = l
	assert.Equal(t, 1, *built)
}

func TestBundleHost_FetcherInstallsFastPathAndDisableRemovesEverything(t *testing.T) {
	h, fp, g, l, built := bundleHostFixture(t)
	h.seams.SetFetcher(okFetcher{})
	h.apply(enabledCfg(true))
	require.NotNil(t, fp.cur)
	assert.NotNil(t, g.cur)

	h.apply(enabledCfg(true))
	assert.Equal(t, 1, *built, "the importer is built once and reused")

	h.apply(enabledCfg(false))
	assert.Nil(t, fp.cur)
	assert.Nil(t, g.cur)
	assert.Nil(t, l.cur)
}

func TestBundleSeams_DefaultsAreNoOps(t *testing.T) {
	s := &peernetBundleSeams{}
	_, _, err := s.FetchBundle(context.Background(), "x")
	assert.ErrorIs(t, err, errNoBundleFetcher)
	s.ReportBadBundle(context.Background(), bundleimport.Origin{}, "x") // must not panic
}

type fakeBundleSvc struct{ noted []string }

func (fakeBundleSvc) FetchBundle(context.Context, string) (*bundle.Bundle, service.FetchSource, error) {
	return nil, service.FetchSource{}, nil
}
func (f *fakeBundleSvc) NoteBundleFullCheck(n, p string, passed bool) {
	f.noted = append(f.noted, n+"/"+p+"/"+map[bool]string{true: "pass", false: "fail"}[passed])
}

func TestConnectBundleSeams_FastPathOnlyWhenFeatureEnabled(t *testing.T) {
	h, fp, g, _, built := bundleHostFixture(t)
	svc := &fakeBundleSvc{}
	connectPeernetBundleSeams(h, svc, enabledCfg(false))
	assert.Nil(t, fp.cur, "feature off: nothing installed")
	assert.Nil(t, g.cur)
	assert.Zero(t, *built)

	connectPeernetBundleSeams(h, svc, enabledCfg(true))
	require.NotNil(t, fp.cur, "feature on and fetcher connected")
	assert.NotNil(t, g.cur)

	h.seams.ReportBadBundle(context.Background(), bundleimport.Origin{NetworkID: "n", PublisherID: "p"}, "r")
	h.seams.ReportVerifiedBundle(context.Background(), bundleimport.Origin{NetworkID: "n", PublisherID: "p"}, "r")
	assert.Equal(t, []string{"n/p/fail", "n/p/pass"}, svc.noted)
}

type stubBundleState struct{ bundleimport.State }

func (stubBundleState) Get(_ context.Context, p string) (database.PeernetBundleImport, bool, error) {
	if p == "peer.mkv" {
		return database.PeernetBundleImport{State: "pending"}, true, nil
	}
	return database.PeernetBundleImport{}, false, nil
}

func TestPeerOriginBinder_FailsClosedUntilTheImporterExists(t *testing.T) {
	b := &peerOriginBinder{}
	_, _, err := b.PeerBundleStatus(context.Background(), "peer.mkv")
	require.Error(t, err, "unbound")

	h, _, _, _, _ := bundleHostFixture(t)
	h.build = func() *bundleimport.Importer { return bundleimport.New(bundleimport.Config{State: stubBundleState{}}) }
	b.Bind(h)
	_, _, err = b.PeerBundleStatus(context.Background(), "peer.mkv")
	require.Error(t, err, "importer not built yet (feature not applied)")

	h.apply(enabledCfg(true))
	from, passed, err := b.PeerBundleStatus(context.Background(), "peer.mkv")
	require.NoError(t, err)
	assert.True(t, from)
	assert.False(t, passed)
	from, _, err = b.PeerBundleStatus(context.Background(), "local.mkv")
	require.NoError(t, err)
	assert.False(t, from)
}

func TestPeernetPatches_NilStoreMeansNoPatches(t *testing.T) {
	require.NotNil(t, peernetPatches(nil))
	assert.False(t, peernetPatches(nil).Has("x@y"))
}

func TestPeerOriginBinder_ImportStatusMapsCounters(t *testing.T) {
	b := &peerOriginBinder{}
	assert.Equal(t, service.BundleImportStatus{}, b.ImportStatus(), "unbound: zero")
	h, _, _, _, _ := bundleHostFixture(t)
	b.Bind(h)
	h.apply(enabledCfg(true))
	st := b.ImportStatus()
	assert.NotNil(t, st.Rejected)
	assert.Zero(t, st.Accepted)
}

type recPwTarget struct{ cur importer.NZBPasswordRecorder }

func (r *recPwTarget) SetNZBPasswordRecorder(x importer.NZBPasswordRecorder) { r.cur = x }

type noopPwRecorder struct{}

func (noopPwRecorder) RecordNZBPasswordFiles(context.Context, []string) {}

func TestBundleHost_PasswordRecorderOnlyWhileTheFeatureIsOn(t *testing.T) {
	h, _, _, _, _ := bundleHostFixture(t)
	target := &recPwTarget{}
	h.pwTarget, h.pwRecorder = target, noopPwRecorder{}
	h.apply(enabledCfg(false))
	assert.Nil(t, target.cur, "feature off: the importer has no hook")
	h.apply(enabledCfg(true))
	assert.NotNil(t, target.cur)
	h.apply(enabledCfg(false))
	assert.Nil(t, target.cur)
}

type recPlayTarget struct{ cur api.PeerPlayback }

func (r *recPlayTarget) SetPeerPlayback(p api.PeerPlayback) { r.cur = p }

type nopPlayback struct{}

func (nopPlayback) Candidates(context.Context, string, *int, *int) []service.PeerCandidate {
	return nil
}
func (nopPlayback) HasOffer(context.Context, string) bool { return false }
func (nopPlayback) Play(context.Context, api.PeerPlayRequest) (api.PeerPlayResult, error) {
	return api.PeerPlayResult{}, nil
}

func TestBundleHost_StremioPlaybackNeedsTheFeatureAndAFetcher(t *testing.T) {
	h, _, _, _, _ := bundleHostFixture(t)
	target := &recPlayTarget{}
	h.play, h.peerPlay = target, nopPlayback{}
	h.apply(enabledCfg(true))
	assert.Nil(t, target.cur, "no fetcher: nothing to play from")
	h.seams.SetFetcher(okFetcher{})
	h.apply(enabledCfg(true))
	assert.NotNil(t, target.cur)
	h.apply(enabledCfg(false))
	assert.Nil(t, target.cur, "feature off: the stream list and the route are as before")
}

type fakePeerSvc struct{ offers []service.Offer }

func (fakePeerSvc) PeerCandidates(context.Context, string, *int, *int) []service.PeerCandidate {
	return nil
}
func (f fakePeerSvc) BundleOffers(context.Context, string) ([]service.Offer, error) {
	return f.offers, nil
}

type noImporter struct{}

func (noImporter) Importer() *bundleimport.Importer { return nil }

type dirsFake struct{}

func (dirsFake) PeerImportDir(c, title string) string { return "/complete/" + c + "/" + title }

func TestStremioPeerPlayback_OffersAndNotReady(t *testing.T) {
	p := stremioPeerPlayback{svc: fakePeerSvc{}, host: noImporter{}, dirs: dirsFake{}}
	assert.False(t, p.HasOffer(context.Background(), "x"))
	p.svc = fakePeerSvc{offers: []service.Offer{{Own: true, Fetchable: true}, {Fetchable: false}}}
	assert.False(t, p.HasOffer(context.Background(), "x"), "own or undialable offers do not count")
	p.svc = fakePeerSvc{offers: []service.Offer{{Fetchable: true}}}
	assert.True(t, p.HasOffer(context.Background(), "x"))
	_, err := p.Play(context.Background(), api.PeerPlayRequest{ReleaseID: "x"})
	assert.ErrorIs(t, err, errPeerImporterNotReady)
}

func TestBundleHost_FeatureOffKeepsTheGateWhilePeerImportsArePending(t *testing.T) {
	h, fp, g, _, built := bundleHostFixture(t)
	pending := true
	h.hasPending = func() bool { return pending }
	h.seams.SetFetcher(okFetcher{})
	h.apply(enabledCfg(false))
	assert.NotNil(t, g.cur, "pending imports still get their forced full check and the ARR hold is released on pass")
	assert.Nil(t, fp.cur, "no new fast-path imports while the feature is off")
	assert.Equal(t, 1, *built)

	pending = false
	h.apply(enabledCfg(false))
	assert.Nil(t, g.cur, "nothing pending: the gate is removed again")
}

func TestPeernetStagingRoot_IsAlwaysAbsoluteAndNeverTheWorkingDirectory(t *testing.T) {
	for _, dbPath := range []string{"", "altmount.db", "data/altmount.db", "/var/lib/altmount/altmount.db"} {
		root := peernetStagingRoot(dbPath)
		assert.True(t, filepath.IsAbs(root), "%q gave %q", dbPath, root)
		assert.NotEqual(t, ".peernet-staging", root)
		assert.Contains(t, filepath.Base(root), "peernet-staging")
	}
	assert.Equal(t, "/var/lib/altmount/.peernet-staging", peernetStagingRoot("/var/lib/altmount/altmount.db"))
}
