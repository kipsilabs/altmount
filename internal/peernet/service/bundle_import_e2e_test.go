package service

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/javi11/nntppool/v5"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/metadata"
	"github.com/kipsilabs/altmount/internal/peernet/bundle"
	"github.com/kipsilabs/altmount/internal/peernet/bundleimport"
	"github.com/kipsilabs/altmount/internal/peernet/bundlex"
	"github.com/kipsilabs/altmount/internal/peernet/contentid"
	"github.com/kipsilabs/altmount/internal/pool"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
)

// e2ePool serves one fake NNTP client; a method the stack should not call
// panics (the embedded interface is nil).
type e2ePool struct {
	pool.Manager
	client pool.NntpClient
}

func (m *e2ePool) GetPool() (pool.NntpClient, error)           { return m.client, nil }
func (m *e2ePool) HasPool() bool                               { return true }
func (m *e2ePool) IncArticlesDownloaded()                      {}
func (m *e2ePool) UpdateDownloadProgress(_ string, _ int64)    {}
func (m *e2ePool) SpeculativeBudget() *pool.SpeculativeBudget  { return nil }
func (m *e2ePool) NotifyStreamChange()                         {}
func (m *e2ePool) SetStreamSource(_ pool.StreamActivitySource) {}
func (m *e2ePool) AcquireImportSlot(context.Context) (func(), error) {
	return func() {}, nil
}
func (m *e2ePool) AcquireImportConnection(context.Context) (func(), error) {
	return func() {}, nil
}
func (m *e2ePool) StatSweepConcurrency(c int) int { return c }

type e2eProber struct{}

func (e2eProber) ProbeRelease(context.Context, []bundleimport.ProbeFile) error { return nil }

type e2eAnnouncer struct {
	mu  sync.Mutex
	got []bundleimport.Announcement
}

func (a *e2eAnnouncer) AnnounceVerifiedImport(_ context.Context, an bundleimport.Announcement) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.got = append(a.got, an)
	return nil
}
func (a *e2eAnnouncer) count() int { a.mu.Lock(); defer a.mu.Unlock(); return len(a.got) }

type e2eRecorder struct {
	mu   sync.Mutex
	recs []bundleimport.ImportRecord
}

func (r *e2eRecorder) RecordPeerImport(_ context.Context, rec bundleimport.ImportRecord) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recs = append(r.recs, rec)
	return int64(100 + len(r.recs)), nil
}

// serviceFetcher is the bridge adapter (bridge imports this package, so the
// test cannot use it): FetchSource becomes the importer's Origin.
type serviceFetcher struct{ svc *Service }

func (f serviceFetcher) FetchBundle(ctx context.Context, rel string) (*bundle.Bundle, bundleimport.Origin, error) {
	b, src, err := f.svc.FetchBundle(ctx, rel)
	if err != nil {
		return nil, bundleimport.Origin{}, err
	}
	return b, bundleimport.Origin{NetworkID: src.Network.String(), PublisherID: src.Publisher.String()}, nil
}

type serviceReporter struct{ svc *Service }

func (r serviceReporter) ReportBadBundle(_ context.Context, o bundleimport.Origin, _ string) {
	r.svc.NoteBundleFullCheck(o.NetworkID, o.PublisherID, false)
}
func (r serviceReporter) ReportVerifiedBundle(_ context.Context, o bundleimport.Origin, _ string) {
	r.svc.NoteBundleFullCheck(o.NetworkID, o.PublisherID, true)
}

type e2eReceiver struct {
	*instance
	imp  *bundleimport.Importer
	lib  *metadata.MetadataService
	ann  *e2eAnnouncer
	rec  *e2eRecorder
	idx  *database.PeernetContentRepository
	pool *fakepool.Client
}

// mkvFile is a structurally valid Matroska file of size bytes: EBML header, a
// Segment ending at the file end, a SeekHead pointing at Info and at Cues (the
// last element), and a Cluster of filler between them.
func mkvFile(size int) []byte {
	u32 := func(v int) []byte { return []byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)} }
	seek := func(id []byte, pos int) []byte {
		body := append(append([]byte{0x53, 0xAB, 0x84}, id...), append([]byte{0x53, 0xAC, 0x84}, u32(pos)...)...)
		return append([]byte{0x4D, 0xBB, byte(0x80 | len(body))}, body...)
	}
	info := []byte{0x15, 0x49, 0xA9, 0x66}
	cues := []byte{0x1C, 0x53, 0xBB, 0x6B}
	ebml := append([]byte{0x1A, 0x45, 0xDF, 0xA3, 0x8B, 0x42, 0x82, 0x88}, "matroska"...)
	segStart := len(ebml) + 12
	seekHead := append([]byte{0x11, 0x4D, 0x9B, 0x74, byte(0x80 | 34)}, append(seek(info, 39), seek(cues, size-segStart-9)...)...)
	out := append([]byte{}, ebml...)
	segSize := size - segStart
	out = append(out, 0x18, 0x53, 0x80, 0x67, 0x01, 0, byte(segSize>>40), byte(segSize>>32), byte(segSize>>24), byte(segSize>>16), byte(segSize>>8), byte(segSize))
	out = append(out, seekHead...)
	out = append(out, append(append([]byte{}, info...), 0x84, 0, 0, 0, 0)...)
	// A Cluster whose 8-byte size covers the filler.
	fill := size - len(out) - 12 - 9
	out = append(out, 0x1F, 0x43, 0xB6, 0x75, 0x01, 0, byte(fill>>40), byte(fill>>32), byte(fill>>24), byte(fill>>16), byte(fill>>8), byte(fill))
	out = append(out, simpleBlocks(fill)...) // a real Cluster body
	return append(out, append(append([]byte{}, cues...), 0x84, 0, 0, 0, 0)...)
}

// newE2EReceiver wires a level-1 node's bundle importer over real sqlite, a
// real metadata service in a temp dir and a fake NNTP pool that serves node
// A's articles.
func newE2EReceiver(t *testing.T, b *instance, serve bool) *e2eReceiver {
	t.Helper()
	fp := fakepool.New()
	if serve {
		file := mkvFile(12 * bundleSegSize)
		for i := 0; i < 12; i++ {
			fp.SetBehavior(strings.Replace("seg-NNN@news.test", "NNN", pad3(i), 1), fakepool.SegmentBehavior{
				Bytes: file[i*bundleSegSize : (i+1)*bundleSegSize],
				YEnc:  nntppool.YEncMeta{PartBegin: int64(i * bundleSegSize), PartSize: bundleSegSize, FileSize: int64(len(file))}})
		}
	}
	lib := metadata.NewMetadataService(t.TempDir())
	conn, d := b.db.Connection(), b.db.Dialect()
	r := &e2eReceiver{instance: b, lib: lib, ann: &e2eAnnouncer{}, rec: &e2eRecorder{}, idx: database.NewPeernetContentRepository(conn, d), pool: fp}
	cfg := config.DefaultConfig()
	r.imp = bundleimport.New(bundleimport.Config{
		Library: lib,
		Headers: bundleimport.NewPoolHeaderReader(&e2ePool{client: fp}),
		Stager:  bundleimport.NewFSStager(bundleimport.FSStagerDeps{Pool: &e2ePool{client: fp}, Config: func() *config.Config { return cfg }, Root: t.TempDir()}),
		Prober:  e2eProber{}, State: database.NewPeernetBundleImportRepository(conn, d), Index: r.idx,
		Fetcher: serviceFetcher{b.svc}, Recorder: r.rec, Announcer: r.ann, Reporter: serviceReporter{b.svc},
		HealthEnabled: func() bool { return true },
	})
	return r
}

func pad3(i int) string {
	return string([]byte{byte('0' + i/100), byte('0' + i/10%10), byte('0' + i%10)})
}

func TestBundleImportEndToEnd_FullCheckPassesAnnouncesOnceAndBecomesPublishEligible(t *testing.T) {
	ctx := context.Background()
	a := newBundlePublisher(t, 3, nil)
	a.publishHealthy(t)
	b := bundleReceiver(t, a)
	b.syncRound(t)
	r := newE2EReceiver(t, b, true)

	res, err := r.imp.ImportRelease(ctx, a.rel, bundleimport.Target{Category: "movies"})
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if len(res.Paths) != 1 || !strings.HasPrefix(res.Paths[0], "movies/") {
		t.Fatalf("paths %v", res.Paths)
	}
	path := res.Paths[0]
	// In B's library, playable, with a verified release identity in the index.
	m, err := r.lib.ReadFileMetadata(path)
	if err != nil || m == nil || m.FileSize != 12*bundleSegSize {
		t.Fatalf("file not in the library: %v %+v", err, m)
	}
	row, ok, err := r.idx.GetByPath(ctx, path)
	if err != nil || !ok || row.ReleaseID != a.rel || !contentid.IsVerifiedBundleRow(row) {
		t.Fatalf("index row %+v ok=%v err=%v", row, ok, err)
	}
	// Peer-origin pending: not published, not announced, held at the ARRs.
	if from, passed, _ := r.imp.PeerBundleStatus(ctx, path); !from || passed {
		t.Fatalf("pending state wrong: from=%v passed=%v", from, passed)
	}
	if r.ann.count() != 0 || !r.imp.QueueHasPending(ctx, 101) {
		t.Fatal("the ARRs must not be told before the full check")
	}
	if len(r.rec.recs) != 1 || r.rec.recs[0].ReleaseID != a.rel || r.rec.recs[0].Title == "" {
		t.Fatalf("recorded import %+v", r.rec.recs)
	}

	// The health worker's forced full check passes.
	r.imp.FullCheckPassed(ctx, path)
	r.imp.FullCheckPassed(ctx, path) // idempotent
	if r.ann.count() != 1 {
		t.Fatalf("announced %d times, want exactly once", r.ann.count())
	}
	if from, passed, err := r.imp.PeerBundleStatus(ctx, path); err != nil || !from || !passed {
		t.Fatalf("not publish-eligible after the pass: %v %v %v", from, passed, err)
	}
	if c := r.imp.Counters(); c.Accepted != 1 || c.FullCheckPassed != 1 || c.Announced != 1 || c.BadBundleReports != 0 {
		t.Fatalf("counters %+v", c)
	}
	// A later verified bundle is not penalised: A is still fetchable.
	if _, _, err := b.svc.FetchBundle(ctx, a.rel); err != nil {
		t.Fatalf("publisher wrongly penalised: %v", err)
	}
}

func TestBundleImportEndToEnd_FullCheckFailureBacksThePublisherOffForFetches(t *testing.T) {
	ctx := context.Background()
	a := newBundlePublisher(t, 3, nil)
	a.publishHealthy(t)
	b := bundleReceiver(t, a)
	b.syncRound(t)
	r := newE2EReceiver(t, b, true)

	res, err := r.imp.ImportRelease(ctx, a.rel, bundleimport.Target{Category: "movies"})
	if err != nil {
		t.Fatal(err)
	}
	path := res.Paths[0]
	// Only a contradiction between the layout and an article's own header is
	// the publisher's doing (missing articles can be propagation): find one in
	// the sampled headers, as the forced full check does.
	id := "seg-005@news.test"
	beh := r.pool.BehaviorFor(id)
	beh.YEnc.PartBegin += 3
	r.pool.SetBehavior(id, beh)
	if err := r.imp.VerifyPeerHeaders(ctx, path); err == nil {
		t.Fatal("the header sample must contradict the layout")
	}
	r.imp.FullCheckFailed(ctx, path)
	r.imp.FullCheckFailed(ctx, path) // reported once per release

	if r.ann.count() != 0 {
		t.Fatal("a failed import is never announced")
	}
	// The failed import is removed with its state: nothing of it can be published.
	if _, passed, _ := r.imp.PeerBundleStatus(ctx, path); passed {
		t.Fatal("a failed file must never become publish-eligible")
	}
	if r.lib.FileExists(path) {
		t.Fatal("a failed import must not stay in the library")
	}
	if _, _, err := b.svc.FetchBundle(ctx, a.rel); !errors.Is(err, bundlex.ErrNoOffers) {
		t.Fatalf("the publisher must be backed off for fetches: %v", err)
	}
	st := b.statusOf(t).Bundles
	if st.FullCheckReports != 1 || st.FullCheckBackedOff != 1 {
		t.Fatalf("status %+v", st)
	}
	if c := r.imp.Counters(); c.FullCheckFailed != 1 || c.BadBundleReports != 1 {
		t.Fatalf("counters %+v", c)
	}
	// Record sync is unaffected: the offer is still known.
	if offers, err := b.svc.BundleOffers(ctx, a.rel); err != nil || len(offers) != 1 {
		t.Fatalf("offers %+v %v", offers, err)
	}
}

func TestBundleImportEndToEnd_ArticlesMissingOnTheReceiversProvidersRejectsAndLeavesNothing(t *testing.T) {
	ctx := context.Background()
	a := newBundlePublisher(t, 3, nil)
	a.publishHealthy(t)
	b := bundleReceiver(t, a)
	b.syncRound(t)
	r := newE2EReceiver(t, b, false) // B's providers hold none of A's articles

	if _, err := r.imp.ImportRelease(ctx, a.rel, bundleimport.Target{Category: "movies"}); err == nil {
		t.Fatal("import must be rejected: the media head is not on B's providers")
	}
	if got, err := r.lib.ReadFileMetadata("movies/Some.Movie.2020.1080p/movie.mkv"); err == nil && got != nil {
		t.Fatal("a rejected import left a file behind")
	}
	if _, ok, _ := r.idx.GetByPath(ctx, "movies/Some.Movie.2020.1080p/movie.mkv"); ok {
		t.Fatal("a rejected import left an index row")
	}
	if len(r.rec.recs) != 0 || r.ann.count() != 0 {
		t.Fatal("a rejected import must not be recorded or announced")
	}
}

// simpleBlocks returns exactly n bytes (n >= 7) of consecutive Matroska
// SimpleBlock elements, what the body of a real Cluster looks like.
func simpleBlocks(n int) []byte {
	out := make([]byte, 0, n)
	for k := 0; len(out) < n; k++ {
		remaining := n - len(out)
		want := min(remaining, 90+(k*37)%300)
		if remaining-want < 7 {
			want = remaining
		}
		body := want - 3
		out = append(out, 0xA3, 0x40|byte(body>>8), byte(body), 0x81, byte(k>>8), byte(k), 0x80)
		for j := 0; j < body-4; j++ {
			out = append(out, byte(1+(len(out)*7+k)%250))
		}
	}
	return out
}
