package par2repair

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"testing"

	"github.com/javi11/nntppool/v5"

	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/testsupport/par2gen"
)

// mkResolveFixture builds metadata + NzbStore + fake articles for a release
// of two content files with a full PAR2 set. Subjects optionally obfuscated.
func mkResolveFixture(t *testing.T, obfuscated bool) (*metapb.FileMetadata, *metapb.NzbStore, *fakeFetcher, map[string][]byte, string) {
	t.Helper()
	rng := rand.New(rand.NewSource(11))
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
	}, 6)

	fetch := &fakeFetcher{articles: map[string][]byte{}}
	store := &metapb.NzbStore{}
	const artSize = 2048

	// Content files: 4 articles each. Article 1 of a.rar is dead.
	deadID := "a.rar-1@test"
	for _, name := range []string{"a.rar", "b.rar"} {
		subject := fmt.Sprintf(`"%s" yEnc (1/4)`, name)
		if obfuscated {
			subject = "aGVsbG8gd29ybGQ" + name // no filename visible
		}
		entry := &metapb.NzbFileEntry{Subject: subject}
		content := contents[name]
		for off, i := 0, 0; off < len(content); off, i = off+artSize, i+1 {
			id := fmt.Sprintf("%s-%d@test", name, i)
			entry.Segments = append(entry.Segments, &metapb.NzbSeg{Id: id, Number: int32(i + 1), Bytes: artSize + 200})
			if id != deadID {
				fetch.articles[id] = content[off : off+artSize]
			}
		}
		store.Files = append(store.Files, entry)
	}

	// PAR2 files: index + volumes, one article each; recorded in metadata.
	fm := &metapb.FileMetadata{}
	par2Payloads := append([][]byte{set.Index}, set.Volumes...)
	for i, p := range par2Payloads {
		id := fmt.Sprintf("par2-%d@test", i)
		fetch.articles[id] = p
		store.Files = append(store.Files, &metapb.NzbFileEntry{
			Subject:  fmt.Sprintf(`"rel.vol%02d.par2" yEnc (1/1)`, i),
			Segments: []*metapb.NzbSeg{{Id: id, Number: 1, Bytes: int64(len(p))}},
		})
		fm.Par2Files = append(fm.Par2Files, &metapb.Par2FileReference{
			Filename: fmt.Sprintf("rel.vol%02d.par2", i),
			FileSize: int64(len(p)),
			SegmentData: []*metapb.SegmentData{
				{Id: "<" + id + ">", SegmentSize: int64(len(p))},
			},
		})
	}
	return fm, store, fetch, contents, deadID
}

func TestResolveByFilename(t *testing.T) {
	fm, store, fetch, _, deadID := mkResolveFixture(t, false)

	res, err := Resolve(context.Background(), fm, store, []string{"<" + deadID + ">"}, fetch,
		Caps{MaxRepairRatio: 0.5, MaxMemoryBytes: 64 << 20}, testLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(res.Plan.Missing); got != 2 {
		t.Fatalf("Missing = %v", res.Plan.Missing)
	}
	if len(res.Plan.DeadArticles) != 1 || res.Plan.DeadArticles[0].MessageID != deadID {
		t.Fatalf("DeadArticles = %+v", res.Plan.DeadArticles)
	}
	// Article sizing: 4 articles of 2048 exactly.
	for _, f := range res.Plan.Files {
		if len(f.Articles) != 4 {
			t.Fatalf("articles = %d", len(f.Articles))
		}
		for _, a := range f.Articles {
			if a.Size != 2048 {
				t.Fatalf("article size = %d", a.Size)
			}
		}
	}
}

func TestResolveObfuscatedFallsBackToHash16k(t *testing.T) {
	fm, store, fetch, _, deadID := mkResolveFixture(t, true)

	res, err := Resolve(context.Background(), fm, store, []string{deadID}, fetch,
		Caps{MaxRepairRatio: 0.5, MaxMemoryBytes: 64 << 20}, testLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Plan.DeadArticles) != 1 {
		t.Fatalf("DeadArticles = %+v", res.Plan.DeadArticles)
	}
}

func TestResolveEndToEndWithRunJob(t *testing.T) {
	fm, store, fetch, contents, deadID := mkResolveFixture(t, false)

	res, err := Resolve(context.Background(), fm, store, []string{deadID}, fetch,
		Caps{MaxRepairRatio: 0.5, MaxMemoryBytes: 64 << 20}, testLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ps := NewPatchStore(t.TempDir())
	if err := RunJob(context.Background(), res.Plan, res.Index, res.Par2Files, fetch, ps, testLogger()); err != nil {
		t.Fatal(err)
	}
	got, ok := ps.Get(deadID)
	if !ok || !bytes.Equal(got, contents["a.rar"][2048:4096]) {
		t.Fatal("resolved repair did not reproduce the dead article payload")
	}
}

// mkVolumeFixture builds a release of two volumes with a full PAR2 set and no
// dead articles; the caller decides which message IDs to remove from the
// fetcher. Volume sizes differ so a part size borrowed from one file is
// genuinely validated against the other's length.
func mkVolumeFixture(t *testing.T) (*metapb.FileMetadata, *metapb.NzbStore, *fakeFetcher, map[string][]byte) {
	t.Helper()
	rng := rand.New(rand.NewSource(29))
	mk := func(n int) []byte {
		b := make([]byte, n)
		rng.Read(b)
		return b
	}
	contents := map[string][]byte{
		"vol1.rar": mk(4096),
		"vol2.rar": mk(8192),
	}
	set := par2gen.BuildFull(1024, []par2gen.FileEntry{
		{Name: "vol1.rar", Content: contents["vol1.rar"]},
		{Name: "vol2.rar", Content: contents["vol2.rar"]},
	}, 8)

	fetch := &fakeFetcher{articles: map[string][]byte{}}
	store := &metapb.NzbStore{}
	const artSize = 2048
	for _, name := range []string{"vol1.rar", "vol2.rar"} {
		entry := &metapb.NzbFileEntry{Subject: fmt.Sprintf(`"%s" yEnc (1/4)`, name)}
		content := contents[name]
		for off, i := 0, 0; off < len(content); off, i = off+artSize, i+1 {
			id := fmt.Sprintf("%s-%d@test", name, i)
			entry.Segments = append(entry.Segments, &metapb.NzbSeg{Id: id, Number: int32(i + 1), Bytes: artSize + 200})
			fetch.articles[id] = content[off : off+artSize]
		}
		store.Files = append(store.Files, entry)
	}

	fm := &metapb.FileMetadata{}
	for i, p := range append([][]byte{set.Index}, set.Volumes...) {
		id := fmt.Sprintf("par2-%d@test", i)
		fetch.articles[id] = p
		store.Files = append(store.Files, &metapb.NzbFileEntry{
			Subject:  fmt.Sprintf(`"rel.vol%02d.par2" yEnc (1/1)`, i),
			Segments: []*metapb.NzbSeg{{Id: id, Number: 1, Bytes: int64(len(p))}},
		})
		fm.Par2Files = append(fm.Par2Files, &metapb.Par2FileReference{
			Filename:    fmt.Sprintf("rel.vol%02d.par2", i),
			FileSize:    int64(len(p)),
			SegmentData: []*metapb.SegmentData{{Id: "<" + id + ">", SegmentSize: int64(len(p))}},
		})
	}
	return fm, store, fetch, contents
}

// A volume whose articles are ALL missing is the canonical thing PAR2 repair
// exists to rebuild. Its decoded part size cannot be probed from its own
// articles, so it must be borrowed from a sibling volume of the same release.
func TestResolveFullyMissingVolume(t *testing.T) {
	fm, store, fetch, contents := mkVolumeFixture(t)

	dead := []string{"vol1.rar-0@test", "vol1.rar-1@test"}
	for _, id := range dead {
		delete(fetch.articles, id)
	}

	res, err := Resolve(context.Background(), fm, store, dead, fetch,
		Caps{MaxRepairRatio: 1.0, MaxMemoryBytes: 64 << 20}, testLogger(), nil)
	if err != nil {
		t.Fatalf("fully missing volume must still be planned: %v", err)
	}
	if len(res.Plan.DeadArticles) != 2 {
		t.Fatalf("DeadArticles = %+v", res.Plan.DeadArticles)
	}
	for _, f := range res.Plan.Files {
		var sum int64
		for _, a := range f.Articles {
			if a.Size != 2048 {
				t.Fatalf("borrowed part size wrong: article size = %d", a.Size)
			}
			sum += a.Size
		}
		if sum != int64(f.Length) {
			t.Fatalf("article sizes sum to %d, want %d", sum, f.Length)
		}
	}

	// The layout must be right, not merely plausible: rebuild and compare.
	ps := NewPatchStore(t.TempDir())
	if err := RunJob(context.Background(), res.Plan, res.Index, res.Par2Files, fetch, ps, testLogger()); err != nil {
		t.Fatalf("RunJob: %v", err)
	}
	for i, id := range dead {
		got, ok := ps.Get(id)
		if !ok {
			t.Fatalf("no patch stored for %s", id)
		}
		want := contents["vol1.rar"][i*2048 : (i+1)*2048]
		if !bytes.Equal(got, want) {
			t.Fatalf("patch for %s does not match the original bytes", id)
		}
	}
}

// statFetcher wraps fakeFetcher with the ArticleStater capability, answering
// liveness from the articles map like a StatMany sweep would.
type statFetcher struct{ *fakeFetcher }

func (s statFetcher) StatIDs(_ context.Context, ids []string, onResult func(done int)) (map[string]bool, error) {
	missing := map[string]bool{}
	for i, id := range ids {
		if _, ok := s.articles[id]; !ok {
			missing[id] = true
		}
		if onResult != nil {
			onResult(i + 1)
		}
	}
	return missing, nil
}

// A stat-capable fetcher lets Resolve discover dead articles that nobody
// declared, so the plan covers the release's full damage instead of tripping
// over surprises during the sweep.
func TestResolveStatSweepFindsUndeclaredDead(t *testing.T) {
	fm, store, fetch, _, deadID := mkResolveFixture(t, false)
	surprise := "b.rar-2@test"
	delete(fetch.articles, surprise)

	res, err := Resolve(context.Background(), fm, store, []string{deadID}, statFetcher{fetch},
		Caps{MaxRepairRatio: 1.0, MaxMemoryBytes: 64 << 20}, testLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, da := range res.Plan.DeadArticles {
		got[da.MessageID] = true
	}
	if len(got) != 2 || !got[deadID] || !got[surprise] {
		t.Fatalf("DeadArticles = %+v, want %s and %s", res.Plan.DeadArticles, deadID, surprise)
	}
}

// When the stat sweep shows more damage than the recovery set can fix, the
// verdict must land at plan time — before any payload is downloaded.
func TestResolveUnrepairableFastWhenDamageExceedsRecovery(t *testing.T) {
	fm, store, fetch, _, deadID := mkResolveFixture(t, false)
	// 4 dead articles x 2 slices = 8 missing slices vs 6 recovery slices.
	// Only deadID is declared; the rest must come from the stat sweep.
	for _, id := range []string{"a.rar-2@test", "a.rar-3@test", "b.rar-2@test"} {
		delete(fetch.articles, id)
	}

	_, err := Resolve(context.Background(), fm, store, []string{deadID}, statFetcher{fetch},
		Caps{MaxRepairRatio: 1.0, MaxMemoryBytes: 64 << 20}, testLogger(), nil)
	if !errors.Is(err, ErrUnrepairable) {
		t.Fatalf("err = %v, want ErrUnrepairable", err)
	}
}

// A recovery slice whose payload tail lives in a dead article parses fine
// (ParseIndex seeks past payloads without fetching them), so the plan must
// drop it up front instead of discovering the hole mid-job.
func TestResolveDropsRecoverySlicesBackedByDeadArticles(t *testing.T) {
	fm, store, fetch, _, deadID := mkResolveFixture(t, false)

	// Re-split one PAR2 volume into two articles: a live head carrying the
	// packet header + exponent, and a dead tail carrying most of the payload.
	const volID = "par2-1@test"
	vol := fetch.articles[volID]
	const split = 128
	fetch.articles["par2-1a@test"] = vol[:split]
	delete(fetch.articles, volID) // tail article par2-1b is never added: dead
	for _, ref := range fm.Par2Files {
		if len(ref.SegmentData) == 1 && ref.SegmentData[0].Id == "<"+volID+">" {
			ref.SegmentData = []*metapb.SegmentData{
				{Id: "<par2-1a@test>", SegmentSize: split},
				{Id: "<par2-1b@test>", SegmentSize: int64(len(vol) - split)},
			}
		}
	}

	res, err := Resolve(context.Background(), fm, store, []string{deadID}, statFetcher{fetch},
		Caps{MaxRepairRatio: 1.0, MaxMemoryBytes: 64 << 20}, testLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(res.Plan.Recovery) + len(res.Plan.SpareRecovery); got != 5 {
		t.Fatalf("planned recovery slices = %d, want 5 (dead-backed slice dropped)", got)
	}
}

// noBodyFetcher is stat-capable but fails the test if any article body is
// actually downloaded.
type noBodyFetcher struct {
	t *testing.T
	statFetcher
}

func (f noBodyFetcher) Fetch(_ context.Context, messageID string) ([]byte, error) {
	f.t.Errorf("article body %s was fetched; the verdict must come from STATs alone", messageID)
	return nil, nntppool.ErrArticleNotFound
}

// Damage far above the repair-ratio cap must be rejected right after the
// liveness sweep — before the PAR2 set is parsed or any body is downloaded.
func TestResolveRatioCapRejectsEarlyWithoutBodyFetches(t *testing.T) {
	fm, store, fetch, _, deadID := mkResolveFixture(t, false)
	// Kill 6 of 8 content articles (75% of the release) against a 5% cap.
	for _, id := range []string{"a.rar-2@test", "a.rar-3@test", "b.rar-0@test", "b.rar-1@test", "b.rar-3@test"} {
		delete(fetch.articles, id)
	}

	_, err := Resolve(context.Background(), fm, store, []string{deadID},
		noBodyFetcher{t: t, statFetcher: statFetcher{fetch}},
		Caps{MaxRepairRatio: 0.05, MaxMemoryBytes: 64 << 20}, testLogger(), nil)
	if !errors.Is(err, ErrUnrepairable) {
		t.Fatalf("err = %v, want ErrUnrepairable", err)
	}
	if !strings.Contains(err.Error(), "max_repair_ratio") {
		t.Fatalf("err = %v, want a damage-ratio verdict", err)
	}
}

// The resolve-phase article cache must stay bounded: a 372-volume PAR2 set
// would otherwise pin hundreds of MB of header articles on the heap.
func TestArticleCacheEvictsOldestBeyondCap(t *testing.T) {
	c := newArticleCache(2)
	c.put("a", []byte("1"))
	c.put("b", []byte("2"))
	c.put("c", []byte("3"))
	if _, ok := c.get("a"); ok {
		t.Fatal("oldest entry must be evicted past the cap")
	}
	if _, ok := c.get("b"); !ok {
		t.Fatal("entry within cap must remain")
	}
	if got, ok := c.get("c"); !ok || string(got) != "3" {
		t.Fatal("newest entry must remain")
	}
}

func TestResolveNoPar2Files(t *testing.T) {
	_, store, fetch, _, _ := mkResolveFixture(t, false)
	store.Files = store.Files[:2] // Only content files; no PAR2 in either source.
	fm := &metapb.FileMetadata{}
	_, err := Resolve(context.Background(), fm, store, nil, fetch, Caps{}, testLogger(), nil)
	if err == nil {
		t.Fatal("want error without par2 files")
	}
}

func TestResolvePar2FromStoreRepairsEndToEnd(t *testing.T) {
	n, fetch, contents, deadID := mkEncodedNzbFixture(t, 700, true)
	store := &metapb.NzbStore{}
	for _, f := range n.Files {
		entry := &metapb.NzbFileEntry{Subject: f.Subject}
		for _, s := range f.Segments {
			entry.Segments = append(entry.Segments, &metapb.NzbSeg{
				Id: s.ID, Number: int32(s.Number), Bytes: int64(s.Bytes),
			})
		}
		store.Files = append(store.Files, entry)
	}
	// Archive imports record no PAR2 references; the release store retains them.
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
	if !ok || !bytes.Equal(got, contents["a.rar"][2048:4096]) {
		t.Fatal("store-based repair did not reproduce the dead article byte-exactly")
	}
}

// sweepStater is a stat-only fetcher recording each StatIDs call, answering
// liveness from its missing set.
type sweepStater struct {
	ArticleFetcher
	missing map[string]bool
	calls   [][]string
}

func (s *sweepStater) StatIDs(_ context.Context, ids []string, onResult func(done int)) (map[string]bool, error) {
	s.calls = append(s.calls, ids)
	out := map[string]bool{}
	for i, id := range ids {
		if s.missing[id] {
			out[id] = true
		}
		if onResult != nil {
			onResult(i + 1)
		}
	}
	return out, nil
}

// uniformBudget is a damage budget over ids of equal size, for sweep tests.
func uniformBudget(ids []string, maxRatio float64) *damageBudget {
	b := &damageBudget{bytes: make(map[string]int64, len(ids)), maxRatio: maxRatio}
	for _, id := range ids {
		b.bytes[id] = 1000
		b.total += 1000
	}
	return b
}

// When the sample alone proves the damage exceeds the repair cap, the sweep
// stops there: STATing the remaining thousands of articles could only confirm
// a verdict that is already certain, and costs minutes of a provider's time.
func TestStatSweepSampleProvesUnrepairable(t *testing.T) {
	ids := make([]string, statSampleSize*8)
	missing := map[string]bool{}
	for i := range ids {
		ids[i] = fmt.Sprintf("a-%d@test", i)
		missing[ids[i]] = true // total loss
	}
	st := &sweepStater{missing: missing}
	dead := map[string]bool{}
	_, err := statSweepBudgeted(context.Background(), st, ids, dead, uniformBudget(ids, 0.02), nil)
	if !errors.Is(err, ErrUnrepairable) {
		t.Fatalf("err = %v, want ErrUnrepairable from the sample alone", err)
	}
	if len(st.calls) != 1 || len(st.calls[0]) != statSampleSize {
		t.Fatalf("StatIDs calls = %d, want exactly the %d-article sample", len(st.calls), statSampleSize)
	}
}

// Damage near the cap is not provable from a sample, so the full census still
// runs and the exact check decides.
func TestStatSweepInconclusiveSampleRunsCensus(t *testing.T) {
	ids := make([]string, statSampleSize*8)
	missing := map[string]bool{}
	for i := range ids {
		ids[i] = fmt.Sprintf("a-%d@test", i)
		if i%20 == 0 { // 5% dead against a 6% cap: the sample cannot prove the excess
			missing[ids[i]] = true
		}
	}
	st := &sweepStater{missing: missing}
	dead := map[string]bool{}
	if _, err := statSweepBudgeted(context.Background(), st, ids, dead, uniformBudget(ids, 0.06), nil); err != nil {
		t.Fatalf("err = %v, want the census to run to completion", err)
	}
	if len(st.calls) != 2 {
		t.Fatalf("StatIDs calls = %d, want sample + full census", len(st.calls))
	}
	if len(dead) != len(missing) {
		t.Fatalf("dead = %d, want %d after a full census", len(dead), len(missing))
	}
}

// The budget counts content bytes only: PAR2 articles are neither damage nor
// denominator, since the ratio is about the bytes being streamed.
func TestNewDamageBudgetExcludesPar2(t *testing.T) {
	store := &metapb.NzbStore{Files: []*metapb.NzbFileEntry{
		{Segments: []*metapb.NzbSeg{{Id: "<c1@x>", Bytes: 700}, {Id: "<c2@x>", Bytes: 300}}},
		{Segments: []*metapb.NzbSeg{{Id: "<p1@x>", Bytes: 5000}}},
	}}
	par2 := []SetFile{{Articles: []Article{{MessageID: "p1@x", Size: 5000}}}}
	b := newDamageBudget(store.Files, par2, Caps{MaxRepairRatio: 0.02})
	if b == nil || b.total != 1000 || len(b.bytes) != 2 {
		t.Fatalf("budget = %+v, want total 1000 over 2 content articles", b)
	}
	if newDamageBudget(store.Files, par2, Caps{}) != nil {
		t.Fatal("no cap configured: budget must be nil so the sweep never aborts")
	}
}

// A clean sample skips the full-release STAT sweep: the plan trusts the known
// holes and the payload sweep's margin rows absorb any stragglers.
func TestStatSweepCleanSampleSkipsFullSweep(t *testing.T) {
	ids := make([]string, statSampleSize*4)
	for i := range ids {
		ids[i] = fmt.Sprintf("a-%d@test", i)
	}
	st := &sweepStater{}
	dead := map[string]bool{ids[0]: true} // known hole: excluded from the sample
	var stages []Stage
	var lastDone, lastTotal int
	hidden, err := statSweep(context.Background(), st, ids, dead, func(stage Stage, done, total int) {
		stages = append(stages, stage)
		lastDone, lastTotal = done, total
	})
	if err != nil {
		t.Fatal(err)
	}
	if hidden != 0 {
		t.Fatalf("hidden estimate = %d, want 0 for a clean sample", hidden)
	}
	if len(st.calls) != 1 || len(st.calls[0]) != statSampleSize {
		t.Fatalf("StatIDs calls = %d (first %d ids), want 1 call of %d sampled ids",
			len(st.calls), len(st.calls[0]), statSampleSize)
	}
	for _, id := range st.calls[0] {
		if id == ids[0] {
			t.Fatal("sample must exclude already-known dead articles")
		}
	}
	if len(dead) != 1 {
		t.Fatalf("dead = %d entries, want the 1 known hole only", len(dead))
	}
	for _, stage := range stages {
		if stage != StageChecking {
			t.Fatalf("stage = %q, want %q", stage, StageChecking)
		}
	}
	if lastDone != statSampleSize || lastTotal != statSampleSize {
		t.Fatalf("final progress = %d/%d, want %d/%d", lastDone, lastTotal, statSampleSize, statSampleSize)
	}
}

// Heavy hidden damage — more than margin rows could absorb — still escalates
// to a full-release sweep, so the plan sees the whole damage instead of a
// lucky subset.
func TestStatSweepHeavyDamageEscalatesToFullSweep(t *testing.T) {
	ids := make([]string, statSampleSize*4)
	missing := map[string]bool{}
	for i := range ids {
		ids[i] = fmt.Sprintf("a-%d@test", i)
		missing[ids[i]] = true // total loss: any sample finds damage
	}
	st := &sweepStater{missing: missing}
	dead := map[string]bool{}
	var lastDone, lastTotal int
	hidden, err := statSweep(context.Background(), st, ids, dead, func(_ Stage, done, total int) {
		lastDone, lastTotal = done, total
	})
	if err != nil {
		t.Fatal(err)
	}
	if hidden != 0 {
		t.Fatalf("hidden estimate = %d, want 0 after a full sweep (nothing left unchecked)", hidden)
	}
	if len(st.calls) != 2 {
		t.Fatalf("StatIDs calls = %d, want 2 (sample then rest)", len(st.calls))
	}
	if got := len(st.calls[0]) + len(st.calls[1]); got != len(ids) {
		t.Fatalf("statted %d ids across both calls, want all %d", got, len(ids))
	}
	if len(dead) != len(ids) {
		t.Fatalf("dead = %d entries, want all %d", len(dead), len(ids))
	}
	if lastDone != len(ids) || lastTotal != len(ids) {
		t.Fatalf("final progress = %d/%d, want %d/%d", lastDone, lastTotal, len(ids), len(ids))
	}
}

// Light hidden damage must NOT trigger the full-release sweep: STATing tens of
// thousands of articles takes minutes, while the payload sweep verifies every
// slice anyway. The sample's damage estimate is returned instead, so the plan
// can provision extra margin rows for the stragglers.
func TestStatSweepLightDamageSkipsFullSweep(t *testing.T) {
	ids := make([]string, statSampleSize*4)
	missing := map[string]bool{}
	for i := range ids {
		ids[i] = fmt.Sprintf("a-%d@test", i)
		if i%64 == 0 { // ~1.6% damage: well inside what margin rows absorb
			missing[ids[i]] = true
		}
	}
	st := &sweepStater{missing: missing}
	dead := map[string]bool{}
	hidden, err := statSweep(context.Background(), st, ids, dead, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.calls) != 1 {
		t.Fatalf("StatIDs calls = %d, want 1 (sample only; light damage must not escalate)", len(st.calls))
	}
	if len(dead) > 0 && hidden < 1 {
		t.Fatalf("hidden estimate = %d, want >= 1 when the sample found damage", hidden)
	}
	if hidden > maxHiddenAbsorbArticles {
		t.Fatalf("hidden estimate = %d exceeds the absorb threshold %d yet no escalation happened",
			hidden, maxHiddenAbsorbArticles)
	}
}

// A release smaller than the sample size is statted in full in one pass.
func TestStatSweepSmallReleaseStatsEverything(t *testing.T) {
	ids := []string{"a@test", "b@test", "c@test"}
	st := &sweepStater{missing: map[string]bool{"b@test": true}}
	dead := map[string]bool{}
	hidden, err := statSweep(context.Background(), st, ids, dead, nil)
	if err != nil {
		t.Fatal(err)
	}
	if hidden != 0 {
		t.Fatalf("hidden estimate = %d, want 0 when everything was statted", hidden)
	}
	if len(st.calls) != 1 || len(st.calls[0]) != len(ids) {
		t.Fatalf("StatIDs calls = %+v, want one call covering all ids", st.calls)
	}
	if !dead["b@test"] || len(dead) != 1 {
		t.Fatalf("dead = %v, want b@test only", dead)
	}
}

// A PAR2 volume with a dead article must still be parseable past the hole:
// the lazy reader serves zeros for the missing span (whether the article was
// known dead up front or discovered dead on fetch) and the packet parser's
// resync recovers the packets behind it.
func TestLazyFileReaderZeroFillsDeadArticles(t *testing.T) {
	part1 := bytes.Repeat([]byte{0x11}, 100)
	part3 := bytes.Repeat([]byte{0x33}, 100)
	fetch := &fakeFetcher{articles: map[string][]byte{
		"a1@test": part1,
		"a3@test": part3,
	}}

	tests := []struct {
		name string
		dead bool // middle article flagged dead up front vs discovered on fetch
	}{
		{"known dead", true},
		{"discovered dead", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sf := SetFile{Length: 300, Articles: []Article{
				{MessageID: "a1@test", Size: 100},
				{MessageID: "a2@test", Size: 100, Dead: tt.dead},
				{MessageID: "a3@test", Size: 100},
			}}
			r := newLazyFileReader(context.Background(), fetch, sf, newArticleCache(4))
			got, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("Read must zero-fill dead articles, got error: %v", err)
			}
			want := append(append(append([]byte(nil), part1...), make([]byte, 100)...), part3...)
			if !bytes.Equal(got, want) {
				t.Fatal("dead article span must read as zeros with neighbours intact")
			}
		})
	}
}

// Between the liveness check and the recovery download the resolver parses
// the PAR2 set and matches files — minutes of article fetches on a large
// release. It must report a planning stage there, or the UI keeps showing
// the finished liveness check as if the job were stuck.
func TestResolveReportsPlanningStage(t *testing.T) {
	fm, store, fetch, _, deadID := mkResolveFixture(t, false)
	var stages []Stage
	_, err := Resolve(context.Background(), fm, store, []string{deadID}, fetch,
		Caps{MaxRepairRatio: 0.5, MaxMemoryBytes: 64 << 20}, testLogger(),
		func(stage Stage, done, total int) {
			if n := len(stages); n == 0 || stages[n-1] != stage {
				stages = append(stages, stage)
			}
		})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, s := range stages {
		if s == StagePlanning {
			found = true
		}
	}
	if !found {
		t.Fatalf("stages = %v, want %q reported during resolve", stages, StagePlanning)
	}
}

// Planning is minutes of article fetches on a large release (parsing every
// PAR2 volume, matching and sizing every member). It must report unit counts
// — not sit on a bare spinner — so the UI shows movement the whole way.
func TestResolvePlanningProgressCounts(t *testing.T) {
	fm, store, fetch, _, deadID := mkResolveFixture(t, false)
	type event struct{ done, total int }
	var planning []event
	_, err := Resolve(context.Background(), fm, store, []string{deadID}, fetch,
		Caps{MaxRepairRatio: 0.5, MaxMemoryBytes: 64 << 20}, testLogger(),
		func(stage Stage, done, total int) {
			if stage == StagePlanning {
				planning = append(planning, event{done, total})
			}
		})
	if err != nil {
		t.Fatal(err)
	}
	if len(planning) < 2 {
		t.Fatalf("planning events = %d, want progress reported as the work advances", len(planning))
	}
	last := planning[len(planning)-1]
	if last.total <= 0 {
		t.Fatalf("final planning total = %d, want real unit counts", last.total)
	}
	if last.done != last.total {
		t.Fatalf("final planning progress = %d/%d, want the stage reported complete", last.done, last.total)
	}
	for i := 1; i < len(planning); i++ {
		if planning[i].done < planning[i-1].done {
			t.Fatalf("planning progress went backwards: %+v", planning)
		}
	}
}

// Release-size bounds are a policy cap: a repair downloads the whole release,
// so users can exclude releases too large (bandwidth) or too small (not worth
// it). Zero bounds allow everything.
func TestReleaseSizePrecheck(t *testing.T) {
	files := []*metapb.NzbFileEntry{
		{Segments: []*metapb.NzbSeg{{Id: "c1@x", Bytes: 6 << 20}, {Id: "c2@x", Bytes: 6 << 20}}},
		{Segments: []*metapb.NzbSeg{{Id: "p1@x", Bytes: 100 << 20}}}, // PAR2 file: excluded
	}
	par2Files := []SetFile{{Articles: []Article{{MessageID: "p1@x", Size: 100 << 20}}}}
	// Content size: 12 MB.

	tests := []struct {
		name     string
		min, max int64
		wantErr  bool
	}{
		{"unbounded allows all", 0, 0, false},
		{"inside range", 1 << 20, 100 << 20, false},
		{"below min", 50 << 20, 0, true},
		{"above max", 0, 10 << 20, true},
		{"min only, above it", 10 << 20, 0, false},
		{"max only, below it", 0, 50 << 20, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			caps := Caps{MinReleaseSizeBytes: tt.min, MaxReleaseSizeBytes: tt.max}
			err := releaseSizePrecheck(files, par2Files, caps)
			if tt.wantErr {
				if !errors.Is(err, ErrUnrepairable) {
					t.Fatalf("err = %v, want ErrUnrepairable", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("err = %v, want nil", err)
			}
		})
	}
}

// A release outside the configured size range must be refused before ANY
// network work: no STATs, no article fetches — the verdict is free.
func TestResolveRejectsReleaseOutsideSizeRange(t *testing.T) {
	fm, store, fetch, _, deadID := mkResolveFixture(t, false)

	_, err := Resolve(context.Background(), fm, store, []string{deadID}, fetch,
		Caps{MaxRepairRatio: 0.5, MaxMemoryBytes: 64 << 20, MinReleaseSizeBytes: 1 << 40},
		testLogger(), nil)
	if !errors.Is(err, ErrUnrepairable) {
		t.Fatalf("err = %v, want ErrUnrepairable for a release below the minimum size", err)
	}
	if n := len(fetch.fetched); n != 0 {
		t.Fatalf("fetched %d articles, want 0 (size verdict must be free)", n)
	}

	_, err = Resolve(context.Background(), fm, store, []string{deadID}, fetch,
		Caps{MaxRepairRatio: 0.5, MaxMemoryBytes: 64 << 20, MaxReleaseSizeBytes: 1}, testLogger(), nil)
	if !errors.Is(err, ErrUnrepairable) {
		t.Fatalf("err = %v, want ErrUnrepairable for a release above the maximum size", err)
	}
}
