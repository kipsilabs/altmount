package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/peernet/bundle"
	"github.com/kipsilabs/altmount/internal/peernet/bundlex"
	"github.com/kipsilabs/altmount/internal/peernet/contentid"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/publish"
	"github.com/kipsilabs/altmount/internal/peernet/records"
)

const bundleSegSize = 716800

var bundleMarkers = []string{"/local/store/path", "RCLONE-PW-MARK", "RCLONE-SALT-MARK", "dl-id-MARK", "IndexerMARK",
	"CategoryMARK", "fingerprint-MARK", "health-MARK"}

type allHealthy struct{}

func (allHealthy) Statuses(_ context.Context, paths []string) (map[string]string, error) {
	out := map[string]string{}
	for _, p := range paths {
		out[p] = string(database.HealthStatusHealthy)
	}
	return out, nil
}

// bundleSrc is the fake full-metadata reader of the builder.
type bundleSrc struct {
	meta map[string]*metapb.FileMetadata
	nzb  [][]string
}

func (s *bundleSrc) ReadFile(p string) (*metapb.FileMetadata, error) { return s.meta[p], nil }
func (s *bundleSrc) NZBSegmentIDs(string) ([][]string, error)        { return s.nzb, nil }
func (s *bundleSrc) NZBPassword(string, *metapb.FileMetadata) (string, bool) {
	return "", false
}

func bundleMeta(segs []string) *metapb.FileMetadata {
	m := &metapb.FileMetadata{FileSize: int64(len(segs)) * bundleSegSize, SourceNzbPath: "/local/store/path/x.nzbz",
		StoreRef: "/local/store/path/ref.nzbz", Status: metapb.FileStatus_FILE_STATUS_HEALTHY, ReleaseDate: 1793491200,
		OriginalFilename: "movie.mkv", NzbdavId: "dl-id-MARK", Password: "RCLONE-PW-MARK", Salt: "RCLONE-SALT-MARK",
		HoleProviderFingerprint: "fingerprint-MARK"}
	for i := range segs {
		m.SegmentData = append(m.SegmentData, &metapb.SegmentData{Id: segs[i], SegmentSize: bundleSegSize, EndOffset: bundleSegSize - 1})
	}
	return m
}

type bundlePub struct {
	*pubFixture
	src *bundleSrc
	rel string
}

// newBundlePublisher is a listening node at the given open-network level whose
// library holds one releasable file with real IDs.
func newBundlePublisher(t *testing.T, lvl int, mut func(*Deps)) *bundlePub {
	t.Helper()
	segs := make([]string, 12)
	for i := range segs {
		segs[i] = fmt.Sprintf("seg-%03d@news.test", i)
	}
	par := []string{"par-000@news.test"}
	meta := bundleMeta(segs)
	fid, err := contentid.FileID(meta)
	if err != nil {
		t.Fatal(err)
	}
	nzb := [][]string{segs, par}
	store := &metapb.NzbStore{}
	for _, f := range nzb {
		fe := &metapb.NzbFileEntry{}
		for _, id := range f {
			fe.Segments = append(fe.Segments, &metapb.NzbSeg{Id: id})
		}
		store.Files = append(store.Files, fe)
	}
	rid, err := contentid.ReleaseID(store)
	if err != nil {
		t.Fatal(err)
	}
	src := &bundleSrc{meta: map[string]*metapb.FileMetadata{pubPath: meta}, nzb: nzb}
	set := Settings{Enabled: true, AcceptIncoming: true, OpenLevel: lvl}
	f := newPubInstance(t, set, func(d *Deps) {
		d.Health, d.BundleSource, d.Patches = allHealthy{}, src, bundlex.NoPatches{}
		if mut != nil {
			mut(d)
		}
	})
	f.seedFile(t, func(s *seed) {
		s.release, s.file, s.category, s.indexer, s.downloadID = rid.String(), fid.String(), "CategoryMARK", "IndexerMARK", "dl-id-MARK"
		s.size = meta.FileSize
	})
	f.start(t)
	if n := f.node(t); lvl > 0 && (n.builder == nil || n.bsrv == nil || n.pipe == nil) {
		t.Fatalf("bundle wiring missing: builder=%v server=%v pipeline=%v", n.builder != nil, n.bsrv != nil, n.pipe != nil)
	}
	set.AdvertiseAddress, set.ListenPort = f.addr(t), f.port(t)
	f.svc.Apply(context.Background(), set)
	return &bundlePub{pubFixture: f, src: src, rel: rid.String()}
}

func (p *bundlePub) publishHealthy(t *testing.T) {
	t.Helper()
	p.svc.NotifyHealthy(healthyNotice())
	p.drain(t)
}

// bundleReceiver is a level-1 node connected to a.
func bundleReceiver(t *testing.T, a *bundlePub) *instance {
	t.Helper()
	b, bset := listeningInstance(t, Settings{OpenLevel: 1})
	bset.OpenPeers = []string{a.addr(t)}
	b.svc.Apply(context.Background(), bset)
	connect(t, b, a.instance, netid.Open())
	return b
}

func releaseBundleIDs(t *testing.T, i *instance, local bool) []records.BundleID {
	t.Helper()
	var out []records.BundleID
	for _, row := range i.stored(t, netid.Open()) {
		env, err := records.Peek(row.Payload)
		if err != nil || env.Kind != records.KindRelease {
			continue
		}
		r, err := records.DecodeReleaseRecord(row.Payload)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, r.BundleID)
	}
	return out
}

func TestBundleEndToEndLevelThreePublishesAndPeerFetches(t *testing.T) {
	a := newBundlePublisher(t, 3, nil)
	a.publishHealthy(t)
	if a.status(t).Bundles.Announced != 1 {
		t.Fatalf("bundles: %+v", a.status(t).Bundles)
	}
	b := bundleReceiver(t, a)
	if res := b.syncRound(t); res.Stored != 2 {
		t.Fatalf("sync: %+v", res)
	}
	offers, err := b.svc.BundleOffers(context.Background(), a.rel)
	if err != nil || len(offers) != 1 || !offers[0].Fetchable {
		t.Fatalf("offers %+v %v", offers, err)
	}
	got, src, err := b.svc.FetchBundle(context.Background(), a.rel)
	if err != nil {
		t.Fatal(err)
	}
	if src.Network != netid.Open() || src.Publisher.String() != a.id(t).String() {
		t.Fatalf("source %+v", src)
	}
	built, err := a.node(t).builder.Build(context.Background(), a.rel, "Some.Movie.2020.1080p", time.Unix(1793491200, 0))
	if err != nil {
		t.Fatal(err)
	}
	rid, _ := contentid.ParseID(a.rel)
	want, err := bundle.Verify(built.Wire, built.ID, rid)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("the fetched bundle differs from the one the publisher built")
	}
	if s := a.status(t).Bundles; s.Served != 1 || s.ServedBytes != int64(len(built.Wire)) || s.Built != 1 {
		t.Fatalf("publisher bundle status %+v", s)
	}
	if s := b.statusOf(t).Bundles; s.Fetched != 1 {
		t.Fatalf("receiver bundle status %+v", s)
	}
}

func (i *instance) statusOf(t *testing.T) Status {
	t.Helper()
	st, err := i.svc.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestBundleLevelTwoPublisherAnnouncesNoBundleAndRefuses(t *testing.T) {
	a := newBundlePublisher(t, 2, nil)
	a.publishHealthy(t)
	b := bundleReceiver(t, a)
	b.syncRound(t)
	for _, id := range releaseBundleIDs(t, b, false) {
		if id != (records.BundleID{}) {
			t.Fatal("a level 2 record carries a bundle id")
		}
	}
	if _, _, err := b.svc.FetchBundle(context.Background(), a.rel); !errors.Is(err, bundlex.ErrNoOffers) {
		t.Fatalf("fetch with no offer: %v", err)
	}
	if a.node(t).builder.Status().Built != 0 {
		t.Fatal("a bundle was built at level 2")
	}
}

func TestBundleAnnouncedThenLevelDroppedIsRefusedImmediately(t *testing.T) {
	a := newBundlePublisher(t, 3, nil)
	a.publishHealthy(t)
	b := bundleReceiver(t, a)
	b.syncRound(t)
	set := a.svc.currentSettings()
	set.OpenLevel = 2
	a.svc.Apply(context.Background(), set)
	_, _, err := b.svc.FetchBundle(context.Background(), a.rel)
	if !errors.Is(err, bundlex.ErrAllFailed) {
		t.Fatalf("fetch after the publisher dropped to level 2: %v", err)
	}
	if got := a.status(t).Bundles.ServeRefused["level"]; got != 1 {
		t.Fatalf("refusals %+v", a.status(t).Bundles.ServeRefused)
	}
}

func TestBundleForgedBytesAreRejectedAndPublisherIsStruckWithoutHurtingRecordSync(t *testing.T) {
	a := newBundlePublisher(t, 3, func(d *Deps) {
		d.wrapProvider = func(p bundlex.Provider) bundlex.Provider { return forger{inner: p} }
	})
	a.publishHealthy(t)
	b := bundleReceiver(t, a)
	b.syncRound(t)
	if _, _, err := b.svc.FetchBundle(context.Background(), a.rel); !errors.Is(err, bundlex.ErrAllFailed) {
		t.Fatalf("forged bundle accepted: %v", err)
	}
	st := b.statusOf(t).Bundles
	if st.FetchFailed["verification_failed"] != 1 || st.FetchBackedOff != 1 || st.Fetched != 0 {
		t.Fatalf("status %+v", st)
	}
	if _, _, err := b.svc.FetchBundle(context.Background(), a.rel); !errors.Is(err, bundlex.ErrNoOffers) {
		t.Fatalf("struck publisher asked again: %v", err)
	}
	// Record sync with the same peer is unaffected by the bundle strike.
	a.publish(t, netid.Open(), 77)
	if res := b.syncRound(t); res.Stored != 1 {
		t.Fatalf("record sync after a bundle strike: %+v", res)
	}
}

type forger struct{ inner bundlex.Provider }

func (f forger) Bundle(ctx context.Context, rel string) (*bundlex.Built, error) {
	real, err := f.inner.Bundle(ctx, rel)
	if err != nil {
		return nil, err
	}
	bad := *real
	bad.Wire = bytes.Repeat([]byte{0x99}, len(real.Wire))
	return &bad, nil
}

func TestBundleExclusionAddedAfterAnnouncementIsRefusedAndWithdrawn(t *testing.T) {
	a := newBundlePublisher(t, 3, nil)
	a.publishHealthy(t)
	b := bundleReceiver(t, a)
	b.syncRound(t)
	before := a.node(t).builder.Status()
	a.excl.set(publish.Exclusions{Indexers: []string{"indexermark"}})
	if _, _, err := b.svc.FetchBundle(context.Background(), a.rel); !errors.Is(err, bundlex.ErrAllFailed) {
		t.Fatalf("excluded release served: %v", err)
	}
	if after := a.node(t).builder.Status(); after.Built != before.Built || after.CacheHits != before.CacheHits {
		t.Fatalf("the excluded release was looked up again: %+v -> %+v", before, after)
	}
	if a.status(t).Bundles.Served != 0 {
		t.Fatal("bytes were sent")
	}
	if n := a.openNetStatus(t).Records.Releases.Local; n != 0 {
		t.Fatalf("record not withdrawn: %d", n)
	}
}

func TestBundleExcludedReleaseBuildsAndAnnouncesNothing(t *testing.T) {
	a := newBundlePublisher(t, 3, nil)
	a.excl.set(publish.Exclusions{Categories: []string{"CategoryMARK"}})
	a.publishHealthy(t)
	if a.node(t).builder.Status().Built != 0 || a.status(t).Bundles.Announced != 0 {
		t.Fatalf("%+v", a.status(t).Bundles)
	}
}

func TestBundleBytesServedContainNoLocalData(t *testing.T) {
	a := newBundlePublisher(t, 3, nil)
	a.publishHealthy(t)
	built, err := a.node(t).builder.Build(context.Background(), a.rel, "Some.Movie.2020.1080p", time.Unix(1793491200, 0))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := bundle.Decompress(built.Wire)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range bundleMarkers {
		if bytes.Contains(raw, []byte(m)) || bytes.Contains(built.Wire, []byte(m)) {
			t.Errorf("bundle contains %q", m)
		}
	}
	if strings.Contains(string(raw), pubPath) {
		t.Error("bundle contains a virtual path")
	}
}

func TestBundleNotShareableReasonIsCountedAndRecordStillPublished(t *testing.T) {
	a := newBundlePublisher(t, 3, func(d *Deps) { d.Patches = nil })
	a.publishHealthy(t)
	st := a.status(t)
	if st.Bundles.NotShareable["patch_status_unknown"] != 1 || st.Bundles.Announced != 0 {
		t.Fatalf("%+v", st.Bundles)
	}
	if a.openNetStatus(t).Records.Releases.Local != 1 {
		t.Fatal("the release record must still be published")
	}
}

func TestBundleLevelChangesSupersedeTheRecord(t *testing.T) {
	ctx := context.Background()
	a := newBundlePublisher(t, 2, nil)
	a.publishHealthy(t)
	awaitFirstUpkeep(t, a.pubFixture)
	bundleOf := func() records.BundleID {
		ids := releaseBundleIDs(t, a.instance, true)
		if len(ids) != 1 {
			t.Fatalf("%d release records", len(ids))
		}
		return ids[0]
	}
	if bundleOf() != (records.BundleID{}) {
		t.Fatal("level 2 record has a bundle id")
	}
	set := a.svc.currentSettings()
	set.OpenLevel = 3
	a.svc.Apply(ctx, set)
	time.Sleep(1100 * time.Millisecond) // records are stamped with whole seconds
	a.node(t).maintainPublishing(ctx, false)
	if bundleOf() == (records.BundleID{}) {
		t.Fatal("2 -> 3 did not supersede the record with one carrying the bundle id")
	}
	set.OpenLevel = 2
	a.svc.Apply(ctx, set)
	time.Sleep(1100 * time.Millisecond)
	a.node(t).maintainPublishing(ctx, false)
	if bundleOf() != (records.BundleID{}) {
		t.Fatal("3 -> 2 left the bundle id announced")
	}
}

func TestBundleFullCheckFailureBacksPublisherOffForFetchesOnly(t *testing.T) {
	a := newBundlePublisher(t, 3, nil)
	a.publishHealthy(t)
	b := bundleReceiver(t, a)
	b.syncRound(t)
	if _, _, err := b.svc.FetchBundle(context.Background(), a.rel); err != nil {
		t.Fatal(err)
	}
	before := len(b.stored(t, netid.Open()))
	b.svc.NoteBundleFullCheck(netid.Open().String(), a.id(t).String(), false)
	if _, _, err := b.svc.FetchBundle(context.Background(), a.rel); !errors.Is(err, bundlex.ErrNoOffers) {
		t.Fatalf("a publisher whose bundle failed the full check was asked again: %v", err)
	}
	if s := b.statusOf(t).Bundles; s.FullCheckReports != 1 || s.FullCheckBackedOff != 1 {
		t.Fatalf("status %+v", s)
	}
	// Record sync and stored records are untouched.
	b.syncRound(t)
	if len(b.stored(t, netid.Open())) != before {
		t.Fatal("the penalty changed the stored records")
	}
	if offers, err := b.svc.BundleOffers(context.Background(), a.rel); err != nil || len(offers) != 1 {
		t.Fatalf("offers must stay listed: %+v %v", offers, err)
	}
	// A later verified bundle from the same publisher clears it.
	b.svc.NoteBundleFullCheck(netid.Open().String(), a.id(t).String(), true)
	if _, _, err := b.svc.FetchBundle(context.Background(), a.rel); err != nil {
		t.Fatalf("not cleared: %v", err)
	}
	// Garbage identifiers are ignored.
	b.svc.NoteBundleFullCheck("zz", "yy", false)
}
