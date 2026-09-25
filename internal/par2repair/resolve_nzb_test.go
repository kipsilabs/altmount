package par2repair

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/javi11/nzbparser"

	"github.com/kipsilabs/altmount/internal/metadata"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/testsupport/par2gen"
)

// nzbFromFixture rebuilds the same release as an *nzbparser.Nzb, so the two
// resolvers can be compared on identical input.
func nzbFromFixture(fm *metapb.FileMetadata, store *metapb.NzbStore) *nzbparser.Nzb {
	par2ByID := map[string]string{}
	for _, p := range fm.Par2Files {
		for _, seg := range p.SegmentData {
			par2ByID[normalizeMsgID(seg.Id)] = p.Filename
		}
	}

	n := &nzbparser.Nzb{}
	for _, f := range store.Files {
		file := nzbparser.NzbFile{Subject: f.Subject}
		// Name the file the way the importer would: PAR2 entries by their
		// recorded filename, content entries from the subject's quoted name.
		if len(f.Segments) > 0 {
			if name, ok := par2ByID[normalizeMsgID(f.Segments[0].Id)]; ok {
				file.Filename = name
			}
		}
		if file.Filename == "" {
			file.Filename = subjectFilename(f.Subject)
		}
		for _, s := range f.Segments {
			file.Segments = append(file.Segments, nzbparser.NzbSegment{
				ID:     s.Id,
				Number: int(s.Number),
				Bytes:  int(s.Bytes),
			})
		}
		file.TotalSegments = len(file.Segments)
		n.Files = append(n.Files, file)
	}
	n.TotalFiles = len(n.Files)
	return n
}

// The NZB-mode resolver must plan the same repair as the metadata-mode
// resolver for the same release — same missing slices, same dead articles.
func TestResolveFromNzbMatchesMetadataResolver(t *testing.T) {
	fm, store, fetch, _, deadID := mkResolveFixture(t, false)
	caps := Caps{MaxRepairRatio: 0.5, MaxMemoryBytes: 64 << 20}

	fromMeta, err := Resolve(context.Background(), fm, store, []string{deadID}, fetch, caps, testLogger(), nil)
	if err != nil {
		t.Fatalf("metadata resolve: %v", err)
	}

	n := nzbFromFixture(fm, store)
	fromNzb, err := ResolveFromNzb(context.Background(), n, []string{deadID}, fetch, caps, testLogger(), nil)
	if err != nil {
		t.Fatalf("nzb resolve: %v", err)
	}

	if !equalInts(fromNzb.Plan.Missing, fromMeta.Plan.Missing) {
		t.Fatalf("Missing = %v, want %v", fromNzb.Plan.Missing, fromMeta.Plan.Missing)
	}
	if fromNzb.Plan.SliceSize != fromMeta.Plan.SliceSize {
		t.Fatalf("SliceSize = %d, want %d", fromNzb.Plan.SliceSize, fromMeta.Plan.SliceSize)
	}
	if fromNzb.Plan.GlobalSlices != fromMeta.Plan.GlobalSlices {
		t.Fatalf("GlobalSlices = %d, want %d", fromNzb.Plan.GlobalSlices, fromMeta.Plan.GlobalSlices)
	}
	if len(fromNzb.Plan.DeadArticles) != 1 || fromNzb.Plan.DeadArticles[0].MessageID != normalizeMsgID(deadID) {
		t.Fatalf("DeadArticles = %+v", fromNzb.Plan.DeadArticles)
	}
}

// An NZB-mode repair produces the same byte-exact patch as metadata mode.
func TestResolveFromNzbRepairsEndToEnd(t *testing.T) {
	fm, store, fetch, contents, deadID := mkResolveFixture(t, false)
	n := nzbFromFixture(fm, store)

	res, err := ResolveFromNzb(context.Background(), n, []string{deadID}, fetch,
		Caps{MaxRepairRatio: 0.5, MaxMemoryBytes: 64 << 20}, testLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ps := NewPatchStore(t.TempDir())
	if err := RunJob(context.Background(), res.Plan, res.Index, res.Par2Files, fetch, ps, testLogger()); err != nil {
		t.Fatal(err)
	}
	got, ok := ps.Get(normalizeMsgID(deadID))
	if !ok || string(got) != string(contents["a.rar"][2048:4096]) {
		t.Fatal("NZB-mode repair did not reproduce the dead article byte-exactly")
	}
}

// mkEncodedNzbFixture builds a realistic NZB-mode release: yEnc-style
// ENCODED declared segment sizes (larger than the decoded payloads the
// fetcher returns), PAR2 files split into par2ArtSize-byte articles, and —
// with packVolumes — every recovery slice concatenated into ONE volume file,
// the way real posts pack "+NN" volumes. Article 1 of a.rar is dead.
func mkEncodedNzbFixture(t testing.TB, par2ArtSize int, packVolumes bool) (*nzbparser.Nzb, *fakeFetcher, map[string][]byte, string) {
	t.Helper()
	rng := rand.New(rand.NewSource(23))
	mk := func(n int) []byte {
		b := make([]byte, n)
		rng.Read(b)
		return b
	}
	contents := map[string][]byte{
		"a.rar": mk(8192),
		"b.rar": mk(8192),
	}
	set := par2gen.BuildFull(1024, []par2gen.FileEntry{
		{Name: "a.rar", Content: contents["a.rar"]},
		{Name: "b.rar", Content: contents["b.rar"]},
	}, 8)

	fetch := &fakeFetcher{articles: map[string][]byte{}}
	n := &nzbparser.Nzb{}
	const artSize = 2048
	// yEnc overhead: every declared segment size is larger than the payload.
	const inflate = 200

	deadID := "a.rar-1@test"
	for _, name := range []string{"a.rar", "b.rar"} {
		file := nzbparser.NzbFile{
			Filename: name,
			Subject:  fmt.Sprintf(`"%s" yEnc (1/4)`, name),
		}
		content := contents[name]
		for off, i := 0, 0; off < len(content); off, i = off+artSize, i+1 {
			id := fmt.Sprintf("%s-%d@test", name, i)
			file.Segments = append(file.Segments, nzbparser.NzbSegment{
				ID: id, Number: i + 1, Bytes: artSize + inflate,
			})
			if id != deadID {
				fetch.articles[id] = content[off : off+artSize]
			}
		}
		n.Files = append(n.Files, file)
	}

	par2Payloads := [][]byte{set.Index}
	if packVolumes {
		var packed []byte
		for _, v := range set.Volumes {
			packed = append(packed, v...)
		}
		par2Payloads = append(par2Payloads, packed)
	} else {
		par2Payloads = append(par2Payloads, set.Volumes...)
	}
	for i, p := range par2Payloads {
		name := fmt.Sprintf("rel.vol%02d.par2", i)
		file := nzbparser.NzbFile{
			Filename: name,
			Subject:  fmt.Sprintf(`"%s" yEnc (1/1)`, name),
		}
		for off, j := 0, 0; off < len(p); off, j = off+par2ArtSize, j+1 {
			end := min(off+par2ArtSize, len(p))
			id := fmt.Sprintf("par2-%d-%d@test", i, j)
			fetch.articles[id] = p[off:end]
			file.Segments = append(file.Segments, nzbparser.NzbSegment{
				ID: id, Number: j + 1, Bytes: (end - off) + inflate,
			})
		}
		n.Files = append(n.Files, file)
	}
	n.TotalFiles = len(n.Files)
	return n, fetch, contents, deadID
}

// Real posts split PAR2 volumes into many articles, and the NZB declares
// yEnc-ENCODED segment sizes — a few percent above the decoded payloads the
// fetcher returns. The resolver must derive the PAR2 files' decoded article
// sizes by probing, or every stream offset drifts at each article boundary
// and every recovery payload fails its packet MD5 (burning all spares).
func TestResolveFromNzbMultiArticlePar2WithEncodedSizes(t *testing.T) {
	n, fetch, contents, deadID := mkEncodedNzbFixture(t, 600, false)

	res, err := ResolveFromNzb(context.Background(), n, []string{deadID}, fetch,
		Caps{MaxRepairRatio: 0.5, MaxMemoryBytes: 64 << 20}, testLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ps := NewPatchStore(t.TempDir())
	if err := RunJob(context.Background(), res.Plan, res.Index, res.Par2Files, fetch, ps, testLogger()); err != nil {
		t.Fatal(err)
	}
	got, ok := ps.Get(normalizeMsgID(deadID))
	if !ok || string(got) != string(contents["a.rar"][2048:4096]) {
		t.Fatal("repair with encoded-size PAR2 articles did not reproduce the dead article byte-exactly")
	}
}

// Planning must pipeline its article fetches instead of paying one full
// round-trip per article in sequence: the parser's recovery headers sit at a
// uniform article stride (warmed ahead once the stride stabilizes) and member
// sizing probes are warmed as a batch. The singleflight cache keeps the
// pipelining free of duplicate downloads.
func TestPlanningPipelinesArticleFetches(t *testing.T) {
	n, fetch, _, deadID := mkEncodedNzbFixture(t, 512, true)
	cf := &concurrencyFetcher{inner: fetch}

	_, err := ResolveFromNzb(context.Background(), n, []string{deadID}, cf,
		Caps{MaxRepairRatio: 0.5, MaxMemoryBytes: 64 << 20}, testLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}

	cf.mu.Lock()
	peak := cf.maxInFlight
	cf.mu.Unlock()
	if peak < 2 {
		t.Fatalf("peak in-flight fetches = %d, want >= 2 (planning must not fetch strictly sequentially)", peak)
	}

	// Waste guard: pipelining must never download the same article twice.
	counts := map[string]int{}
	fetch.mu.Lock()
	for _, id := range fetch.fetched {
		counts[id]++
	}
	fetch.mu.Unlock()
	for id, c := range counts {
		if c > 1 {
			t.Fatalf("article %s fetched %d times, want 1 (singleflight)", id, c)
		}
	}
}

// fetchCached must singleflight concurrent callers of the same article: one
// download, everyone gets the payload.
func TestFetchCachedSingleflight(t *testing.T) {
	var calls atomic.Int32
	slow := fetchFunc(func(ctx context.Context, msgID string) ([]byte, error) {
		calls.Add(1)
		time.Sleep(20 * time.Millisecond)
		return []byte("payload-" + msgID), nil
	})
	cache := newArticleCache(4)

	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data, err := fetchCached(context.Background(), slow, "<x@test>", cache)
			if err != nil || string(data) != "payload-<x@test>" {
				t.Errorf("fetchCached = %q, %v", data, err)
			}
		}()
	}
	wg.Wait()
	if got := calls.Load(); got != 1 {
		t.Fatalf("fetcher called %d times, want 1", got)
	}
}

// fetchFunc adapts a function to ArticleFetcher.
type fetchFunc func(ctx context.Context, msgID string) ([]byte, error)

func (f fetchFunc) Fetch(ctx context.Context, msgID string) ([]byte, error) { return f(ctx, msgID) }

// latencyFetcher simulates the per-article round-trip planning lives under,
// counting fetches so the benchmark can report the sequential baseline.
type latencyFetcher struct {
	inner *fakeFetcher
	delay time.Duration
	calls atomic.Int64
}

func (l *latencyFetcher) Fetch(ctx context.Context, msgID string) ([]byte, error) {
	l.calls.Add(1)
	time.Sleep(l.delay)
	return l.inner.Fetch(ctx, msgID)
}

// BenchmarkPlanningWithLatency measures NZB-mode planning wall clock under a
// simulated per-article round-trip — the regime real repairs live in
// (planning is latency-bound, not bandwidth-bound). Compare ns/op against
// calls × delay: the gap is what stride warming and probe prewarming save.
func BenchmarkPlanningWithLatency(b *testing.B) {
	const delay = 5 * time.Millisecond
	var calls int64
	for b.Loop() {
		b.StopTimer()
		n, fetch, _, deadID := mkEncodedNzbFixture(b, 512, true)
		lf := &latencyFetcher{inner: fetch, delay: delay}
		b.StartTimer()

		if _, err := ResolveFromNzb(context.Background(), n, []string{deadID}, lf,
			Caps{MaxRepairRatio: 0.5, MaxMemoryBytes: 64 << 20}, testLogger(), nil); err != nil {
			b.Fatal(err)
		}
		calls = lf.calls.Load()
	}
	b.ReportMetric(float64(calls), "fetches/op")
	b.ReportMetric(float64(calls)*float64(delay)/float64(time.Millisecond), "sequential-baseline-ms")
}

func TestResolveFromNzbWithoutPar2Files(t *testing.T) {
	fm, store, fetch, _, _ := mkResolveFixture(t, false)
	n := nzbFromFixture(fm, store)
	// Drop every PAR2 file from the NZB.
	var kept []nzbparser.NzbFile
	for _, f := range n.Files {
		if !isPar2Filename(f.Filename) {
			kept = append(kept, f)
		}
	}
	n.Files = kept

	_, err := ResolveFromNzb(context.Background(), n, nil, fetch, Caps{}, testLogger(), nil)
	if err == nil {
		t.Fatal("want error when the NZB carries no PAR2 files")
	}
}

// Stored NZB entries retain encoded sizes, including for multipart PAR2 volumes.
// Archive metadata can omit Par2Files even though these entries are available.
func TestResolveFallsBackToStoredNzb(t *testing.T) {
	for _, packed := range []bool{false, true} {
		t.Run(fmt.Sprintf("packed=%v", packed), func(t *testing.T) {
			n, fetch, contents, deadID := mkEncodedNzbFixture(t, 700, packed)
			store, _ := metadata.BuildStore(n)
			fm := &metapb.FileMetadata{}
			res, err := Resolve(context.Background(), fm, store, []string{deadID}, fetch,
				Caps{MaxRepairRatio: 0.5, MaxMemoryBytes: 64 << 20}, testLogger(), nil)
			if err != nil {
				t.Fatal(err)
			}
			ps := NewPatchStore(t.TempDir())
			if err := RunJob(context.Background(), res.Plan, res.Index, res.Par2Files, fetch, ps, testLogger()); err != nil {
				t.Fatal(err)
			}
			got, ok := ps.Get(normalizeMsgID(deadID))
			if !ok || string(got) != string(contents["a.rar"][2048:4096]) {
				t.Fatal("stored-NZB repair did not reproduce the dead article byte-exactly")
			}
			if len(fm.Par2Files) != 0 {
				t.Fatal("fallback mutated metadata")
			}
		})
	}
}
