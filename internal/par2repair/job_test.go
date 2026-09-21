package par2repair

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/javi11/nntppool/v5"

	"github.com/kipsilabs/altmount/internal/importer/parser/par2"
	"github.com/kipsilabs/altmount/internal/testsupport/par2gen"
)

// fakeFetcher serves article payloads from a map; absent keys are dead.
// Concurrency-safe like the production fetcher must be.
type fakeFetcher struct {
	mu       sync.Mutex
	articles map[string][]byte
	fetched  []string
}

func (f *fakeFetcher) Fetch(_ context.Context, messageID string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, ok := f.articles[messageID]
	if !ok {
		return nil, nntppool.ErrArticleNotFound
	}
	f.fetched = append(f.fetched, messageID)
	return data, nil
}

// repairFixture wires a complete fake release: two content files split into
// articles, a full PAR2 set split into one article per par2 file, and the
// SetFile/article maps for both.
type repairFixture struct {
	idx        *par2.Index
	files      []SetFile
	par2Files  []SetFile
	fetch      *fakeFetcher
	contents   map[string][]byte // by name
	deadMsgID  string
	deadOrig   []byte // the dead article's true payload
	deadOffset int64
}

func mkRepairFixture(t *testing.T, sliceSize int, artSize int64, numRecovery int, deadArt int) *repairFixture {
	t.Helper()
	rng := rand.New(rand.NewSource(7))
	mk := func(n int) []byte {
		b := make([]byte, n)
		rng.Read(b)
		return b
	}
	contents := map[string][]byte{
		"a.rar": mk(8192),
		"b.rar": mk(8192),
	}
	set := par2gen.BuildFull(sliceSize, []par2gen.FileEntry{
		{Name: "a.rar", Content: contents["a.rar"]},
		{Name: "b.rar", Content: contents["b.rar"]},
	}, numRecovery)

	streams := []io.Reader{bytes.NewReader(set.Index)}
	for _, v := range set.Volumes {
		streams = append(streams, bytes.NewReader(v))
	}
	idx, err := par2.ParseIndex(streams)
	if err != nil {
		t.Fatal(err)
	}

	fetch := &fakeFetcher{articles: map[string][]byte{}}
	fx := &repairFixture{idx: idx, fetch: fetch, contents: contents}

	// Content files -> articles. Article deadArt of a.rar is dead.
	for _, name := range []string{"a.rar", "b.rar"} {
		content := contents[name]
		var fileID [16]byte
		for id, fd := range idx.Files {
			if fd.Name == name {
				fileID = id
			}
		}
		sf := SetFile{FileID: fileID, Length: uint64(len(content))}
		for off, i := int64(0), 0; off < int64(len(content)); off, i = off+artSize, i+1 {
			sz := min(artSize, int64(len(content))-off)
			msgID := fmt.Sprintf("<%s-%d@test>", name, i)
			payload := content[off : off+sz]
			dead := name == "a.rar" && i == deadArt
			if dead {
				fx.deadMsgID = msgID
				fx.deadOrig = payload
				fx.deadOffset = off
			} else {
				fetch.articles[msgID] = payload
			}
			sf.Articles = append(sf.Articles, Article{MessageID: msgID, Size: sz, Dead: dead})
		}
		fx.files = append(fx.files, sf)
	}

	// PAR2 files -> one article each, matching ParseIndex stream order.
	par2Payloads := append([][]byte{set.Index}, set.Volumes...)
	for i, p := range par2Payloads {
		msgID := fmt.Sprintf("<par2-%d@test>", i)
		fetch.articles[msgID] = p
		fx.par2Files = append(fx.par2Files, SetFile{
			Length:   uint64(len(p)),
			Articles: []Article{{MessageID: msgID, Size: int64(len(p))}},
		})
	}
	return fx
}

func testLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// A provider serving a truncated article must not panic the range read: the
// short tail reads as zeros (the payload then fails its checksum and follows
// the normal corrupt/dead paths), mirroring lazyFileReader's semantics.
func TestReadRangeFromShortArticleZeroFills(t *testing.T) {
	f := SetFile{
		Length: 4096,
		Articles: []Article{
			{MessageID: "<a-0@test>", Size: 2048},
			{MessageID: "<a-1@test>", Size: 2048},
		},
	}
	get := func(id string) ([]byte, error) {
		if id == "<a-0@test>" {
			return bytes.Repeat([]byte{0x11}, 1000), nil // truncated: declared 2048
		}
		return bytes.Repeat([]byte{0x22}, 2048), nil
	}
	out, err := readRangeFrom(get, f, 0, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if out[0] != 0x11 || out[999] != 0x11 {
		t.Fatal("delivered bytes of the short article lost")
	}
	for i := 1000; i < 2048; i++ {
		if out[i] != 0 {
			t.Fatalf("byte %d = %#x, want zero fill for the truncated tail", i, out[i])
		}
	}
	if out[2048] != 0x22 {
		t.Fatal("following article misplaced")
	}
}

func TestRunJobRepairsDeadArticle(t *testing.T) {
	fx := mkRepairFixture(t, 1024, 2048, 6, 1)
	plan, err := BuildPlan(fx.idx, fx.files, Caps{MaxRepairRatio: 0.5, MaxMemoryBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}

	store := NewPatchStore(t.TempDir())
	var stages []Stage
	progress := WithProgress(func(stage Stage, done, total int) {
		if n := len(stages); n == 0 || stages[n-1] != stage {
			stages = append(stages, stage)
		}
		if done < 0 || done > total {
			t.Errorf("progress %d/%d out of range in stage %q", done, total, stage)
		}
	})
	if err := RunJob(context.Background(), plan, fx.idx, fx.par2Files, fx.fetch, store, testLogger(), progress); err != nil {
		t.Fatal(err)
	}
	if want := []Stage{StageDownloading, StageRepairing}; !slices.Equal(stages, want) {
		t.Fatalf("progress stages = %v, want %v", stages, want)
	}

	got, ok := store.Get(fx.deadMsgID)
	if !ok {
		t.Fatal("no patch stored for dead article")
	}
	if !bytes.Equal(got, fx.deadOrig) {
		t.Fatalf("patch mismatch: got %d bytes, want %d byte-exact", len(got), len(fx.deadOrig))
	}
}

// concurrencyFetcher records the peak number of Fetch calls in flight.
type concurrencyFetcher struct {
	inner *fakeFetcher

	mu          sync.Mutex
	inFlight    int
	maxInFlight int
}

func (c *concurrencyFetcher) Fetch(ctx context.Context, messageID string) ([]byte, error) {
	c.mu.Lock()
	c.inFlight++
	c.maxInFlight = max(c.maxInFlight, c.inFlight)
	c.mu.Unlock()
	time.Sleep(time.Millisecond) // give overlapping fetches a chance to pile up
	defer func() {
		c.mu.Lock()
		c.inFlight--
		c.mu.Unlock()
	}()
	return c.inner.Fetch(ctx, messageID)
}

// The sweep of a 900MB release must not download one article at a time: RunJob
// is expected to keep several fetches in flight while folding slices in order.
func TestRunJobFetchesArticlesConcurrently(t *testing.T) {
	// Small articles (512B) over 2x8192B files -> 32 sweep articles.
	fx := mkRepairFixture(t, 1024, 512, 6, 1)
	fetch := &concurrencyFetcher{inner: fx.fetch}

	plan, err := BuildPlan(fx.idx, fx.files, Caps{MaxRepairRatio: 0.5, MaxMemoryBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	store := NewPatchStore(t.TempDir())
	if err := RunJob(context.Background(), plan, fx.idx, fx.par2Files, fetch, store, testLogger()); err != nil {
		t.Fatal(err)
	}

	got, ok := store.Get(fx.deadMsgID)
	if !ok || !bytes.Equal(got, fx.deadOrig) {
		t.Fatal("parallel sweep must still produce a byte-exact patch")
	}
	if fetch.maxInFlight < 2 {
		t.Fatalf("max in-flight fetches = %d, want concurrent fetching", fetch.maxInFlight)
	}
}

// overlapFetcher records whether fetches from two different files were ever
// in flight together.
type overlapFetcher struct {
	inner *fakeFetcher

	mu       sync.Mutex
	inFlight map[string]int // file prefix -> count
	crossed  bool
}

func filePrefixOf(messageID string) string {
	return messageID[:strings.IndexByte(messageID, '-')]
}

func (o *overlapFetcher) Fetch(ctx context.Context, messageID string) ([]byte, error) {
	p := filePrefixOf(messageID)
	o.mu.Lock()
	o.inFlight[p]++
	for other, n := range o.inFlight {
		if other != p && n > 0 {
			o.crossed = true
		}
	}
	o.mu.Unlock()
	time.Sleep(2 * time.Millisecond)
	defer func() {
		o.mu.Lock()
		o.inFlight[p]--
		o.mu.Unlock()
	}()
	return o.inner.Fetch(ctx, messageID)
}

// The sweep's prefetch must not drain at recovery-set file boundaries: the
// first articles of the next file must already be in flight while the last
// articles of the previous one are still downloading.
func TestRunJobPrefetchSpansFileBoundaries(t *testing.T) {
	fx := mkRepairFixture(t, 1024, 512, 6, 1) // 16 articles per file
	fetch := &overlapFetcher{inner: fx.fetch, inFlight: map[string]int{}}

	plan, err := BuildPlan(fx.idx, fx.files, Caps{MaxRepairRatio: 0.5, MaxMemoryBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	store := NewPatchStore(t.TempDir())
	if err := RunJob(context.Background(), plan, fx.idx, fx.par2Files, fetch, store, testLogger(), WithConcurrency(8)); err != nil {
		t.Fatal(err)
	}
	got, ok := store.Get(fx.deadMsgID)
	if !ok || !bytes.Equal(got, fx.deadOrig) {
		t.Fatal("continuous sweep must still produce a byte-exact patch")
	}
	if !fetch.crossed {
		t.Fatal("no fetch of the second file overlapped a fetch of the first: the pipeline drained at the file boundary")
	}
}

// Files whose length is not a multiple of the slice size end in a partial
// slice that PAR2 defines as zero-padded; the sweep must fold it padded, and
// the repair must still be byte-exact.
func TestRunJobRepairsWithPartialFinalSlice(t *testing.T) {
	// One recovery slice for one missing slice: no margin row may quietly
	// absorb a mis-padded tail as a corrupt slice.
	fx := mkRepairFixture(t, 1020, 510, 1, 1) // 8192 % 1020 = 32-byte tail; dead article inside slice 0
	plan, err := BuildPlan(fx.idx, fx.files, Caps{MaxRepairRatio: 0.5, MaxMemoryBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	store := NewPatchStore(t.TempDir())
	if err := RunJob(context.Background(), plan, fx.idx, fx.par2Files, fx.fetch, store, testLogger()); err != nil {
		t.Fatal(err)
	}
	got, ok := store.Get(fx.deadMsgID)
	if !ok || !bytes.Equal(got, fx.deadOrig) {
		t.Fatal("partial-final-slice release must produce a byte-exact patch")
	}
}

// A plan over the memory budget must still repair, backed by disk instead of
// heap accumulators. maxInFlight and byte-exactness must both hold.
func TestRunJobSpillToDiskRepairs(t *testing.T) {
	fx := mkRepairFixture(t, 1024, 2048, 6, 1)
	plan, err := BuildPlan(fx.idx, fx.files, Caps{MaxRepairRatio: 0.5, MaxMemoryBytes: 1}) // force spill
	if err != nil {
		t.Fatal(err)
	}
	if !plan.SpillToDisk {
		t.Fatal("fixture must produce a spill plan")
	}

	store := NewPatchStore(t.TempDir())
	if err := RunJob(context.Background(), plan, fx.idx, fx.par2Files, fx.fetch, store, testLogger()); err != nil {
		t.Fatal(err)
	}
	got, ok := store.Get(fx.deadMsgID)
	if !ok || !bytes.Equal(got, fx.deadOrig) {
		t.Fatal("disk-backed repair must produce a byte-exact patch")
	}
}

// Spill mode must absorb a corrupt present slice on its margin rows, all on
// disk-backed buffers.
func TestRunJobSpillToDiskSurvivesCorruptReplan(t *testing.T) {
	fx := mkRepairFixture(t, 1024, 2048, 6, 1)
	corruptID := "<b.rar-0@test>"
	corrupted := bytes.Clone(fx.fetch.articles[corruptID])
	corrupted[10] ^= 0xFF
	fx.fetch.articles[corruptID] = corrupted

	plan, err := BuildPlan(fx.idx, fx.files, Caps{MaxRepairRatio: 0.5, MaxMemoryBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	store := NewPatchStore(t.TempDir())
	if err := RunJob(context.Background(), plan, fx.idx, fx.par2Files, fx.fetch, store, testLogger()); err != nil {
		t.Fatal(err)
	}
	got, ok := store.Get(fx.deadMsgID)
	if !ok || !bytes.Equal(got, fx.deadOrig) {
		t.Fatal("disk-backed replan must produce a byte-exact patch")
	}
}

func TestRunJobRepairsDespiteCorruptPresentSlice(t *testing.T) {
	fx := mkRepairFixture(t, 1024, 2048, 6, 1)
	// Corrupt a present content article (same length, one flipped byte): the
	// swept slice fails its IFSC CRC32 and must be reclassified as missing,
	// consuming a spare recovery slice.
	corruptID := "<b.rar-0@test>"
	corrupted := bytes.Clone(fx.fetch.articles[corruptID])
	corrupted[10] ^= 0xFF
	fx.fetch.articles[corruptID] = corrupted

	plan, err := BuildPlan(fx.idx, fx.files, Caps{MaxRepairRatio: 0.5, MaxMemoryBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	store := NewPatchStore(t.TempDir())
	if err := RunJob(context.Background(), plan, fx.idx, fx.par2Files, fx.fetch, store, testLogger()); err != nil {
		t.Fatal(err)
	}

	got, ok := store.Get(fx.deadMsgID)
	if !ok {
		t.Fatal("no patch stored for dead article")
	}
	if !bytes.Equal(got, fx.deadOrig) {
		t.Fatalf("patch mismatch: got %d bytes, want %d byte-exact", len(got), len(fx.deadOrig))
	}
	// The corrupt article's wire copy is bad — readers must get corrected
	// bytes, so it is patched too, byte-exact.
	gotCorrupt, ok := store.Get(corruptID)
	if !ok {
		t.Fatal("no patch stored for the corrupt present article")
	}
	if !bytes.Equal(gotCorrupt, fx.contents["b.rar"][:2048]) {
		t.Fatal("corrupt article patch not byte-exact")
	}
}

func TestRunJobUnrepairableWhenCorruptAndNoSpares(t *testing.T) {
	// numRecovery=2 exactly covers the dead article's 2 missing slices
	// (slice size 1024, article size 2048), leaving zero spares.
	fx := mkRepairFixture(t, 1024, 2048, 2, 1)
	corruptID := "<b.rar-0@test>"
	corrupted := bytes.Clone(fx.fetch.articles[corruptID])
	corrupted[10] ^= 0xFF
	fx.fetch.articles[corruptID] = corrupted

	plan, err := BuildPlan(fx.idx, fx.files, Caps{MaxRepairRatio: 0.5, MaxMemoryBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	store := NewPatchStore(t.TempDir())
	err = RunJob(context.Background(), plan, fx.idx, fx.par2Files, fx.fetch, store, testLogger())
	if !errors.Is(err, ErrUnrepairable) {
		t.Fatalf("err = %v, want ErrUnrepairable", err)
	}
	if store.Has(fx.deadMsgID) {
		t.Fatal("patch must not be stored after an aborted job")
	}
}

func TestRunJobFallsBackToSpareOnDeadRecoveryArticle(t *testing.T) {
	fx := mkRepairFixture(t, 1024, 2048, 6, 1)
	plan, err := BuildPlan(fx.idx, fx.files, Caps{MaxRepairRatio: 0.5, MaxMemoryBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	// Kill the par2 volume article that carries the first chosen recovery slice.
	firstRef := plan.Recovery[0]
	deadPar2 := fx.par2Files[firstRef.FileIndex].Articles[0].MessageID
	delete(fx.fetch.articles, deadPar2)

	store := NewPatchStore(t.TempDir())
	if err := RunJob(context.Background(), plan, fx.idx, fx.par2Files, fx.fetch, store, testLogger()); err != nil {
		t.Fatal(err)
	}
	got, ok := store.Get(fx.deadMsgID)
	if !ok || !bytes.Equal(got, fx.deadOrig) {
		t.Fatal("repair via spare recovery slice failed")
	}
}

func TestRunJobUnrepairableWhenAllRecoveryDead(t *testing.T) {
	fx := mkRepairFixture(t, 1024, 2048, 2, 1)
	plan, err := BuildPlan(fx.idx, fx.files, Caps{MaxRepairRatio: 0.5, MaxMemoryBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	// Kill every par2 volume (streams 1..N; stream 0 is the index file).
	for i := 1; i < len(fx.par2Files); i++ {
		delete(fx.fetch.articles, fx.par2Files[i].Articles[0].MessageID)
	}
	store := NewPatchStore(t.TempDir())
	err = RunJob(context.Background(), plan, fx.idx, fx.par2Files, fx.fetch, store, testLogger())
	if !errors.Is(err, ErrUnrepairable) {
		t.Fatalf("err = %v, want ErrUnrepairable", err)
	}
}

// countFetches returns how many times messageID was successfully fetched.
func (f *fakeFetcher) countFetches(messageID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, id := range f.fetched {
		if id == messageID {
			n++
		}
	}
	return n
}

// An article the plan thought live but that dies before the sweep reaches it
// must be absorbed by the plan's margin recovery rows: the job completes in
// one sweep and patches the newly dead article too.
func TestRunJobAbsorbsMidSweepDeadArticle(t *testing.T) {
	fx := mkRepairFixture(t, 1024, 2048, 6, 1)
	plan, err := BuildPlan(fx.idx, fx.files, Caps{MaxRepairRatio: 0.5, MaxMemoryBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Recovery) <= len(plan.Missing) {
		t.Fatal("fixture must produce a plan with margin rows")
	}

	// Kill a b.rar article after planning: the plan still marks it live.
	lateDead := "<b.rar-1@test>"
	lateOrig := fx.fetch.articles[lateDead]
	delete(fx.fetch.articles, lateDead)

	store := NewPatchStore(t.TempDir())
	if err := RunJob(context.Background(), plan, fx.idx, fx.par2Files, fx.fetch, store, testLogger()); err != nil {
		t.Fatalf("margin rows must absorb a mid-sweep dead article, got %v", err)
	}

	got, ok := store.Get(fx.deadMsgID)
	if !ok || !bytes.Equal(got, fx.deadOrig) {
		t.Fatal("planned dead article not repaired byte-exact")
	}
	got, ok = store.Get(lateDead)
	if !ok || !bytes.Equal(got, lateOrig) {
		t.Fatal("mid-sweep dead article not repaired byte-exact")
	}
	// One sweep only: no present article was read twice.
	if n := fx.fetch.countFetches("<a.rar-0@test>"); n != 1 {
		t.Fatalf("article <a.rar-0@test> fetched %d times, want 1 (single sweep)", n)
	}
}

// A mid-sweep dead article with no margin rows left must still surface
// SweepDeadArticleError so the service can persist the discovery and replan.
func TestRunJobMidSweepDeadWithoutMarginSurfacesError(t *testing.T) {
	// numRecovery=2 exactly covers the dead article's 2 missing slices:
	// no margin, no spares.
	fx := mkRepairFixture(t, 1024, 2048, 2, 1)
	plan, err := BuildPlan(fx.idx, fx.files, Caps{MaxRepairRatio: 0.5, MaxMemoryBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	delete(fx.fetch.articles, "<b.rar-1@test>")

	store := NewPatchStore(t.TempDir())
	err = RunJob(context.Background(), plan, fx.idx, fx.par2Files, fx.fetch, store, testLogger())
	var dead *SweepDeadArticleError
	if !errors.As(err, &dead) || dead.MessageID != "<b.rar-1@test>" {
		t.Fatalf("err = %v, want SweepDeadArticleError for <b.rar-1@test>", err)
	}
}

// When the margin runs out, the articles the sweep ALREADY proved dead and
// absorbed must ride out on the error alongside the one that broke the margin.
// Dropping them makes the retry re-derive facts this sweep already paid for:
// each attempt would learn exactly one dead article while re-parsing the PAR2
// set and re-reading the release, so a release with more dead articles than
// margin rows exhausts maxJobAttempts instead of converging.
func TestSweepDeadArticleErrorCarriesAbsorbedDiscoveries(t *testing.T) {
	// deadArt=1 => k=2 missing slices; numRecovery=6 => 4 margin rows, so two
	// 2-slice articles absorb and the third has nowhere to go.
	fx := mkRepairFixture(t, 1024, 2048, 6, 1)
	plan, err := BuildPlan(fx.idx, fx.files, Caps{MaxRepairRatio: 0.9, MaxMemoryBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"<b.rar-0@test>", "<b.rar-1@test>", "<b.rar-2@test>"} {
		delete(fx.fetch.articles, id)
	}

	store := NewPatchStore(t.TempDir())
	err = RunJob(context.Background(), plan, fx.idx, fx.par2Files, fx.fetch, store, testLogger())

	var dead *SweepDeadArticleError
	if !errors.As(err, &dead) {
		t.Fatalf("err = %v, want SweepDeadArticleError", err)
	}
	// Every article this sweep proved dead must be reported, in one set, so
	// the next plan starts from all of them.
	got := map[string]bool{dead.MessageID: true}
	for _, id := range dead.Absorbed {
		got[id] = true
	}
	for _, want := range []string{"<b.rar-0@test>", "<b.rar-1@test>", "<b.rar-2@test>"} {
		if !got[want] {
			t.Errorf("dead article %s missing from error (MessageID=%q, Absorbed=%v)",
				want, dead.MessageID, dead.Absorbed)
		}
	}
	// The planned-dead article was already known; it must not be re-reported.
	if got[fx.deadMsgID] {
		t.Errorf("planned-dead %s must not be reported as a new discovery", fx.deadMsgID)
	}
}

// A corrupt present slice must be absorbed by a margin row in the same sweep,
// not trigger a second full read of the release.
func TestRunJobAbsorbsCorruptSliceWithoutResweep(t *testing.T) {
	fx := mkRepairFixture(t, 1024, 2048, 6, 1)
	corruptID := "<b.rar-0@test>"
	corrupted := bytes.Clone(fx.fetch.articles[corruptID])
	corrupted[10] ^= 0xFF
	fx.fetch.articles[corruptID] = corrupted

	plan, err := BuildPlan(fx.idx, fx.files, Caps{MaxRepairRatio: 0.5, MaxMemoryBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	store := NewPatchStore(t.TempDir())
	if err := RunJob(context.Background(), plan, fx.idx, fx.par2Files, fx.fetch, store, testLogger()); err != nil {
		t.Fatal(err)
	}
	got, ok := store.Get(fx.deadMsgID)
	if !ok || !bytes.Equal(got, fx.deadOrig) {
		t.Fatal("repair with absorbed corrupt slice not byte-exact")
	}
	if n := fx.fetch.countFetches("<a.rar-0@test>"); n != 1 {
		t.Fatalf("article <a.rar-0@test> fetched %d times, want 1 (corrupt slice must not force a re-sweep)", n)
	}
}

// A verify sweep: the plan carries no missing slices — the trigger knows the
// release is damaged (corrupt articles broke import analysis) but not where.
// The sweep's CRC verification must find the corrupt slice, absorb it onto a
// margin row, and store a byte-exact patch for the corrupt ARTICLE — the whole
// point of the sweep is that the resumed import reads corrected bytes. The
// corrupt article spans a good slice too, so the patch splices recovered bytes
// over the corrupt slice and wire bytes over the rest.
func TestRunJobVerifySweepPatchesCorruptArticle(t *testing.T) {
	fx := mkRepairFixture(t, 1024, 2048, 6, -1) // nothing dead
	corruptID := "<b.rar-1@test>"
	orig := bytes.Clone(fx.fetch.articles[corruptID])
	corrupted := bytes.Clone(orig)
	corrupted[10] ^= 0xFF // damages the article's first slice only
	fx.fetch.articles[corruptID] = corrupted

	caps := Caps{MaxRepairRatio: 0.5, MaxMemoryBytes: 64 << 20, VerifySweep: true}
	plan, err := BuildPlan(fx.idx, fx.files, caps)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Missing) != 0 {
		t.Fatalf("verify plan Missing = %v, want none", plan.Missing)
	}

	store := NewPatchStore(t.TempDir())
	if err := RunJob(context.Background(), plan, fx.idx, fx.par2Files, fx.fetch, store, testLogger()); err != nil {
		t.Fatal(err)
	}

	got, ok := store.Get(corruptID)
	if !ok {
		t.Fatal("no patch stored for the corrupt article")
	}
	if !bytes.Equal(got, orig) {
		t.Fatalf("patch not byte-exact: got %d bytes, want %d", len(got), len(orig))
	}
}

// A verify sweep over an intact release finds nothing to fix. It must surface
// ErrNothingToRepair so the service fails the parked import instead of
// resuming it — resuming would re-fail analysis and re-defer forever.
func TestRunJobVerifySweepCleanReleaseReportsNothingToRepair(t *testing.T) {
	fx := mkRepairFixture(t, 1024, 2048, 6, -1)

	caps := Caps{MaxRepairRatio: 0.5, MaxMemoryBytes: 64 << 20, VerifySweep: true}
	plan, err := BuildPlan(fx.idx, fx.files, caps)
	if err != nil {
		t.Fatal(err)
	}
	store := NewPatchStore(t.TempDir())
	err = RunJob(context.Background(), plan, fx.idx, fx.par2Files, fx.fetch, store, testLogger())
	if !errors.Is(err, ErrNothingToRepair) {
		t.Fatalf("err = %v, want ErrNothingToRepair for a clean verify sweep", err)
	}
}

// More corrupt slices than margin rows must fall back to the replan path:
// one spare per overflow slice and a fresh sweep, whose recovery payloads are
// refetched (the previous attempt's fold consumed the donated buffers).
func TestRunJobCorruptOverflowFallsBackToReplan(t *testing.T) {
	// sliceSize == artSize == 512: one slice per article. The dead a.rar
	// article is 1 missing slice, so the plan carries 1+8 recovery rows;
	// 12 volumes leave 3 spares.
	fx := mkRepairFixture(t, 512, 512, 12, 1)
	// Corrupt 10 b.rar articles: 8 absorbed on margin rows, 2 via replans.
	for i := range 10 {
		id := fmt.Sprintf("<b.rar-%d@test>", i)
		corrupted := bytes.Clone(fx.fetch.articles[id])
		corrupted[3] ^= 0xFF
		fx.fetch.articles[id] = corrupted
	}

	plan, err := BuildPlan(fx.idx, fx.files, Caps{MaxRepairRatio: 0.5, MaxMemoryBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Recovery) != 9 || len(plan.SpareRecovery) != 3 {
		t.Fatalf("recovery split = %d/%d, want 9/3", len(plan.Recovery), len(plan.SpareRecovery))
	}
	store := NewPatchStore(t.TempDir())
	if err := RunJob(context.Background(), plan, fx.idx, fx.par2Files, fx.fetch, store, testLogger()); err != nil {
		t.Fatal(err)
	}
	got, ok := store.Get(fx.deadMsgID)
	if !ok || !bytes.Equal(got, fx.deadOrig) {
		t.Fatal("repair across corrupt-overflow replans not byte-exact")
	}
	// The replans really happened: present articles were swept more than once.
	if n := fx.fetch.countFetches("<a.rar-0@test>"); n < 2 {
		t.Fatalf("article <a.rar-0@test> fetched %d times, want >=2 (replan re-sweeps)", n)
	}
}

// A recovery payload that arrives corrupt (its bytes no longer match the
// RecvSlic packet's MD5) must never seed an accumulator: a poisoned row makes
// every rebuilt slice fail verification and the whole repair is discarded.
// The load must detect the mismatch and fall back exactly like a dead
// recovery article — margin row dropped or spare swapped in — and the repair
// must still produce a byte-exact patch.
func TestRunJobSwapsOutCorruptRecoveryPayload(t *testing.T) {
	fx := mkRepairFixture(t, 1024, 2048, 6, 1)
	plan, err := BuildPlan(fx.idx, fx.files, Caps{MaxRepairRatio: 0.5, MaxMemoryBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt the payload bytes of the first chosen recovery slice inside its
	// backing PAR2 article (copied: the fixture shares the volume buffers).
	ref := plan.Recovery[0]
	artID := fx.par2Files[ref.FileIndex].Articles[0].MessageID
	art := append([]byte(nil), fx.fetch.articles[artID]...)
	art[ref.BodyOffset+7] ^= 0xFF
	fx.fetch.articles[artID] = art

	store := NewPatchStore(t.TempDir())
	if err := RunJob(context.Background(), plan, fx.idx, fx.par2Files, fx.fetch, store, testLogger()); err != nil {
		t.Fatalf("RunJob must survive a corrupt recovery payload: %v", err)
	}
	got, ok := store.Get(fx.deadMsgID)
	if !ok || !bytes.Equal(got, fx.deadOrig) {
		t.Fatal("repair with a corrupt recovery payload must still produce a byte-exact patch")
	}
}

// The sweep's fetch concurrency must follow the configured repair connection
// count, not a hard-coded depth: WithConcurrency(2) keeps at most 2 fetches
// in flight.
func TestRunJobHonorsConcurrencyOption(t *testing.T) {
	fx := mkRepairFixture(t, 1024, 512, 6, 1) // 32 sweep articles
	fetch := &concurrencyFetcher{inner: fx.fetch}

	plan, err := BuildPlan(fx.idx, fx.files, Caps{MaxRepairRatio: 0.5, MaxMemoryBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	store := NewPatchStore(t.TempDir())
	if err := RunJob(context.Background(), plan, fx.idx, fx.par2Files, fetch, store, testLogger(), WithConcurrency(2)); err != nil {
		t.Fatal(err)
	}

	got, ok := store.Get(fx.deadMsgID)
	if !ok || !bytes.Equal(got, fx.deadOrig) {
		t.Fatal("bounded sweep must still produce a byte-exact patch")
	}
	if fetch.maxInFlight > 2 {
		t.Fatalf("max in-flight fetches = %d, want <= 2 (WithConcurrency must bound the sweep)", fetch.maxInFlight)
	}
}

// blockingFetcher blocks every Fetch until told to complete one, so tests can
// observe in-flight counts deterministically.
type blockingFetcher struct {
	mu       sync.Mutex
	inFlight int
	proceed  chan struct{} // one receive releases one blocked fetch
}

func (b *blockingFetcher) Fetch(ctx context.Context, messageID string) ([]byte, error) {
	b.mu.Lock()
	b.inFlight++
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		b.inFlight--
		b.mu.Unlock()
	}()
	select {
	case <-b.proceed:
		return []byte{0}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (b *blockingFetcher) current() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.inFlight
}

// waitFor polls until cond holds or the deadline passes.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// The prefetch pipeline reads its depth live: raising the configured repair
// connection count mid-run must let a running sweep launch more concurrent
// fetches as slots churn, without restarting the job.
func TestPrefetchArticlesHonorsLiveDepthRaise(t *testing.T) {
	fetch := &blockingFetcher{proceed: make(chan struct{})}
	var limit atomic.Int64
	limit.Store(1)

	arts := make([]Article, 8)
	for i := range arts {
		arts[i] = Article{MessageID: fmt.Sprintf("<a%d@test>", i), Size: 1}
	}
	slots, stop := prefetchArticles(context.Background(), fetch, arts, func() int { return int(limit.Load()) })
	defer stop()

	waitFor(t, "first fetch in flight", func() bool { return fetch.current() == 1 })
	time.Sleep(10 * time.Millisecond) // depth 1: no second fetch may start
	if got := fetch.current(); got != 1 {
		t.Fatalf("in-flight = %d, want 1 while depth is 1", got)
	}

	limit.Store(3)
	fetch.proceed <- struct{}{} // one completion churns a slot at the new depth
	waitFor(t, "three fetches in flight after raise", func() bool { return fetch.current() == 3 })

	// Drain so the pipeline goroutine exits.
	for range arts {
		select {
		case fetch.proceed <- struct{}{}:
		default:
		}
	}
	stop()
	for range slots {
	}
}

// The pool's background lane owns the connection share now: while playback
// streams, the fetch depth stays at the configured bound instead of collapsing
// to a caller-side guess, and only the solver's fold width still yields CPU.
func TestFetchDepthIgnoresStreamActivity(t *testing.T) {
	active := true
	var o jobOptions
	for _, opt := range []JobOption{
		WithLiveConcurrency(func() int { return 20 }),
		WithYieldToStreams(func() bool { return active }),
	} {
		opt(&o)
	}
	depth := o.fetchDepth()
	if got := depth(); got != 20 {
		t.Fatalf("streams active: depth = %d, want the configured 20", got)
	}
	if limit := o.foldWorkerLimit(); limit == nil || limit() != yieldFoldWorkers {
		t.Fatal("streams active: fold width must still yield to playback")
	}
	active = false
	if got := depth(); got != 20 {
		t.Fatalf("streams idle: depth = %d, want 20", got)
	}
}

// WithLiveConcurrency plumbs a live getter through RunJob down to the sweep's
// fetch pipeline: the bound must be honored end to end.
func TestRunJobHonorsLiveConcurrencyGetter(t *testing.T) {
	fx := mkRepairFixture(t, 1024, 512, 6, 1) // 32 sweep articles
	fetch := &concurrencyFetcher{inner: fx.fetch}

	plan, err := BuildPlan(fx.idx, fx.files, Caps{MaxRepairRatio: 0.5, MaxMemoryBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	store := NewPatchStore(t.TempDir())
	err = RunJob(context.Background(), plan, fx.idx, fx.par2Files, fetch, store, testLogger(),
		WithLiveConcurrency(func() int { return 2 }))
	if err != nil {
		t.Fatal(err)
	}
	got, ok := store.Get(fx.deadMsgID)
	if !ok || !bytes.Equal(got, fx.deadOrig) {
		t.Fatal("bounded sweep must still produce a byte-exact patch")
	}
	if fetch.maxInFlight > 2 {
		t.Fatalf("max in-flight fetches = %d, want <= 2 (live getter must bound the sweep)", fetch.maxInFlight)
	}
}
