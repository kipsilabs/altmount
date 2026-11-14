package service

import (
	"context"
	"errors"
	"testing"
	"time"
)

const mediaMetadata = `{"imdb_id":"tt0133093","type":"series","season":2,"episode":5,"tmdbId":603}`

type fakePeerOrigin struct {
	from, passed bool
	err          error
	asked        []string
}

func (f *fakePeerOrigin) PeerBundleStatus(_ context.Context, p string) (bool, bool, error) {
	f.asked = append(f.asked, p)
	return f.from, f.passed, f.err
}

func originFixture(t *testing.T, po *fakePeerOrigin, extra ...func(*Deps)) *pubFixture {
	t.Helper()
	f := newPubInstance(t, level(2), func(d *Deps) {
		d.PeerOrigin = po
		for _, m := range extra {
			m(d)
		}
	})
	f.seedFile(t)
	f.start(t)
	return f
}

func TestPublishPeerBundleFileWaitsForItsFullCheck(t *testing.T) {
	for _, tc := range []struct {
		name         string
		po           fakePeerOrigin
		published    bool
		skip         string
		releaseState bool
	}{
		{name: "local import", po: fakePeerOrigin{}, published: true},
		{name: "peer import, check pending", po: fakePeerOrigin{from: true}, skip: "peer_bundle_not_fully_checked"},
		{name: "peer import, check passed", po: fakePeerOrigin{from: true, passed: true}, published: true},
		{name: "status unreadable", po: fakePeerOrigin{err: errors.New("db down")}, skip: "peer_status_unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			po := tc.po
			f := originFixture(t, &po)
			f.svc.NotifyHealthy(healthyNotice())
			f.drain(t)
			got := f.status(t).Publish
			if tc.published && got.Published["release"] == 0 {
				t.Fatalf("not published: %+v", got)
			}
			if !tc.published && (len(got.Published) != 0 || got.Skipped[tc.skip] != 1) {
				t.Fatalf("want one skip %q and nothing published: %+v", tc.skip, got)
			}
		})
	}
}

func TestReannounceSkipsPeerBundleFilesUntilVerified(t *testing.T) {
	po := &fakePeerOrigin{passed: true, from: true}
	lh := &fakeLocalHealth{statuses: map[string]string{pubPath: "healthy"}}
	f := originFixture(t, po, func(d *Deps) { d.Health = lh })
	f.svc.NotifyHealthy(healthyNotice())
	f.drain(t)
	awaitFirstUpkeep(t, f)
	ageRelease(t, f, pubRelease)
	po.from, po.passed = true, false
	f.node(t).pipe.reannounce(context.Background(), 10)
	if got := f.status(t).Publish.Reannounced; got != 0 {
		t.Fatalf("re-announced a peer import that is not verified: %d", got)
	}
	po.err = errors.New("db down")
	f.node(t).pipe.reannounce(context.Background(), 10)
	if got := f.status(t).Publish.Reannounced; got != 0 {
		t.Fatalf("re-announced with unreadable peer state: %d", got)
	}
	po.err, po.passed = nil, true
	f.node(t).pipe.clearFailure(openID, pubRelease) // skip the back-off of the refused passes
	f.node(t).pipe.reannounce(context.Background(), 10)
	if got := f.status(t).Publish.Reannounced; got != 1 {
		t.Fatalf("a verified peer import must re-announce: %d", got)
	}
}

// A peer's unverified media claim must not be signed by this node: whatever
// the full-check state, a peer-bundle file publishes its release without
// media identifiers; a local import keeps them.
func TestPublishNeverRepublishesPeerBundleMediaIDs(t *testing.T) {
	for _, tc := range []struct {
		name      string
		po        fakePeerOrigin
		wantMedia bool
	}{
		{name: "local import", po: fakePeerOrigin{}, wantMedia: true},
		{name: "peer import, check passed", po: fakePeerOrigin{from: true, passed: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			po := tc.po
			f := newPubInstance(t, level(2), func(d *Deps) { d.PeerOrigin = &po })
			f.seedFile(t, func(s *seed) { s.metadata = mediaMetadata })
			f.start(t)
			f.svc.NotifyHealthy(healthyNotice())
			f.drain(t)
			rel, _ := ownRecords(t, f)
			if rel == nil {
				t.Fatalf("no release record: %+v", f.status(t).Publish)
			}
			if got := rel.Media.IMDb != "" || rel.Media.TMDB != 0 || rel.Media.Season != nil || rel.Media.Episode != nil; got != tc.wantMedia {
				t.Fatalf("media published = %v, want %v: %+v", got, tc.wantMedia, rel.Media)
			}
		})
	}
}

func TestReannounceNeverRepublishesPeerBundleMediaIDs(t *testing.T) {
	po := &fakePeerOrigin{}
	lh := &fakeLocalHealth{statuses: map[string]string{pubPath: "healthy"}}
	f := newPubInstance(t, level(2), func(d *Deps) { d.PeerOrigin, d.Health = po, lh })
	f.seedFile(t, func(s *seed) { s.metadata = mediaMetadata })
	f.start(t)
	f.svc.NotifyHealthy(healthyNotice())
	f.drain(t)
	awaitFirstUpkeep(t, f)
	if rel, _ := ownRecords(t, f); rel == nil || rel.Media.IMDb == "" {
		t.Fatalf("a local import must publish its media ids: %+v", rel)
	}
	ageRelease(t, f, pubRelease)
	time.Sleep(1100 * time.Millisecond) // record times have second precision: the new record must be newer
	po.from, po.passed = true, true
	f.node(t).pipe.reannounce(context.Background(), 10)
	if got := f.status(t).Publish.Reannounced; got != 1 {
		t.Fatalf("not re-announced: %d %+v", got, f.status(t).Publish)
	}
	if rel, _ := ownRecords(t, f); rel == nil || rel.Media.IMDb != "" || rel.Media.TMDB != 0 {
		t.Fatalf("re-announce republished a peer's media ids: %+v", rel)
	}
}
