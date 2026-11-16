package bundleimport

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/javi11/nntppool/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kipsilabs/altmount/internal/peernet/bundle"
)

func TestAccept_SampledYencHeadersMustAgreeWithTheLayout(t *testing.T) {
	lies := map[string]func(i int, m nntppool.YEncMeta) nntppool.YEncMeta{
		"part size":   func(_ int, m nntppool.YEncMeta) nntppool.YEncMeta { m.PartSize++; return m },
		"part offset": func(_ int, m nntppool.YEncMeta) nntppool.YEncMeta { m.PartBegin += 7; return m },
		"file total":  func(_ int, m nntppool.YEncMeta) nntppool.YEncMeta { m.FileSize *= 2; return m },
	}
	for name, lie := range lies {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			b := e.mkBundle("yl", mkvSpec("movie.mkv"))
			for i, id := range b.Pool {
				beh := e.fp.BehaviorFor(id)
				beh.YEnc = lie(i, beh.YEnc)
				e.fp.SetBehavior(id, beh)
			}
			_, err := e.imp.Accept(context.Background(), e.req(b))
			require.Error(t, err)
			assert.Equal(t, ReasonYencMismatch, ReasonOf(err))
			assert.Len(t, e.rep.got, 1, "the publisher is reported as having served a bad bundle")
			e.assertNothingBehind()
		})
	}
}

func TestAccept_NoHeaderReaderMeansNoAccept(t *testing.T) {
	e := newEnv(t)
	e.imp.cfg.Headers = nil
	_, err := e.imp.Accept(context.Background(), e.req(e.mkBundle("nh", mkvSpec("movie.mkv"))))
	require.Error(t, err)
	e.assertNothingBehind()
}

type countingHeaders struct {
	SegmentHeaders
	n atomic.Int32
}

func (c *countingHeaders) SegmentHeader(ctx context.Context, id string) (YencHeader, error) {
	c.n.Add(1)
	return c.SegmentHeaders.SegmentHeader(ctx, id)
}

func TestAccept_YencBudgetTooSmallForTheTablesRejects(t *testing.T) {
	e := newEnv(t)
	files := []fileSpec{}
	for i := 0; i < 10; i++ {
		files = append(files, mkvSpec(fmt.Sprintf("m%d.mkv", i)))
	}
	b := e.mkBundle("cap", files...)
	ch := &countingHeaders{SegmentHeaders: e.imp.cfg.Headers}
	e.imp.cfg.Headers = ch
	e.imp.cfg.Policy.MaxYencSamples = 6 // ten tables need at least 30
	_, err := e.imp.Accept(context.Background(), e.req(b))
	require.Error(t, err)
	assert.Equal(t, ReasonTooManyTables, ReasonOf(err))
	assert.Zero(t, ch.n.Load(), "nothing is read when the budget cannot cover the tables")
	e.assertNothingBehind()
}

func TestAccept_InteriorLieIsFound(t *testing.T) {
	for _, idx := range []int{1, 2} {
		e := newEnv(t)
		b := e.mkBundle(fmt.Sprintf("in%d", idx), mkvSpec("movie.mkv"))
		beh := e.fp.BehaviorFor(b.Pool[idx])
		beh.YEnc.PartBegin += 3
		e.fp.SetBehavior(b.Pool[idx], beh)
		_, err := e.imp.Accept(context.Background(), e.req(b))
		require.Error(t, err)
		assert.Equal(t, ReasonYencMismatch, ReasonOf(err), "interior article %d", idx)
		e.assertNothingBehind()
	}
}

func TestAccept_FirstArticleNameMustMatchTheNZBEntry(t *testing.T) {
	e := newEnv(t)
	b := e.mkBundle("nm", mkvSpec("movie.mkv"))
	beh := e.fp.BehaviorFor(b.Pool[0])
	beh.YEnc.FileName = "a1b2c3.bin"
	e.fp.SetBehavior(b.Pool[0], beh)
	req := e.req(b)
	req.ExpectedNZB = [][]string{b.Pool}
	req.ExpectedNZBNames = []string{"other-entry.bin"}
	_, err := e.imp.Accept(context.Background(), req)
	require.Error(t, err)
	assert.Equal(t, ReasonYencMismatch, ReasonOf(err), "%v", err)
	assert.Empty(t, e.rep.got, "a disagreement with the local NZB is not the publisher's fault")
	e.assertNothingBehind()

	req.ExpectedNZBNames = []string{"A1B2C3.bin"} // obfuscated names bind too
	_, err = e.imp.Accept(context.Background(), req)
	require.NoError(t, err)
}

func TestSubjectFilename(t *testing.T) {
	assert.Equal(t, "My.File.rar", SubjectFilename(`[01/10] - "My.File.rar" yEnc (1/50)`))
	assert.Equal(t, "", SubjectFilename(`no quotes here`))
	assert.Equal(t, "", SubjectFilename(`"unterminated`))
}

func mkTable(n int, main bool) sampleTable {
	t := sampleTable{ids: make([]string, n), sizes: make([]int64, n), prefix: make([]int64, n), main: main}
	for i := range t.sizes {
		t.sizes[i] = 100
		t.prefix[i] = int64(i) * 100
		t.total += 100
	}
	return t
}

// bigTable is a table of n articles of 768,000 bytes.
func bigTable(n int, main bool) sampleTable {
	t := mkTable(n, main)
	t.total = 0
	for i := range t.sizes {
		t.sizes[i] = 768_000
		t.prefix[i] = t.total
		t.total += 768_000
	}
	return t
}

func planBytes(tables []sampleTable, plan []sample) (n int, bytes int64) {
	for _, s := range plan {
		bytes += tables[s.table].sizes[s.pos]
	}
	return len(plan), bytes
}

// Every table's LAST article is always read (a size lie anywhere in a table
// shifts the offsets after it, so the last article shows it); the first and a
// random interior one are read when the budget allows.
func TestPlanSamples_EveryTableGetsItsLastArticle(t *testing.T) {
	tables := make([]sampleTable, 50)
	for i := range tables {
		tables[i] = mkTable(10, false)
	}
	plan, err := planSamples(tables, 600, 1<<30)
	require.NoError(t, err)
	per := map[int][]int{}
	for _, s := range plan {
		per[s.table] = append(per[s.table], s.pos)
	}
	require.Len(t, per, 50, "no table is left unsampled")
	for _, ps := range per {
		assert.Contains(t, ps, 9, "the last article of every table")
		assert.Contains(t, ps, 0, "a roomy budget also reads the first")
		assert.Len(t, ps, 3, "and one interior article")
	}
}

func TestPlanSamples_MediaTablesAreSampledDenserAndTheCapHolds(t *testing.T) {
	plan, err := planSamples([]sampleTable{mkTable(5000, true)}, 600, 1<<30)
	require.NoError(t, err)
	assert.Len(t, plan, 2+10, "first, last and one interior per 500 segments")
	// Scarce article budget: the main file's tables are still read in full at
	// that rate; the rest of the release gets only what is left.
	plan, err = planSamples([]sampleTable{mkTable(5000, true), mkTable(10, false), mkTable(10, false)}, 14, 1<<30)
	require.NoError(t, err)
	assert.LessOrEqual(t, len(plan), 14)
	main := 0
	for _, s := range plan {
		if s.table == 0 {
			main++
		}
	}
	assert.Equal(t, 12, main)
}

// A release with many small volumes is not rejected: the last article of each
// is read, and the byte cap is raised to fit them (it only ever refuses beyond
// the absolute ceiling).
func TestPlanSamples_ManyVolumesAreThinnedNotRejected(t *testing.T) {
	tables := make([]sampleTable, 300)
	for i := range tables {
		tables[i] = bigTable(20, false) // 15 MB volumes: a 4.6 GB release
	}
	plan, err := planSamples(tables, 2000, 1<<30)
	require.NoError(t, err, "300 volumes must not be rejected")
	n, bytes := planBytes(tables, plan)
	assert.Equal(t, 300, n, "the last article of every volume, and nothing else fits")
	assert.EqualValues(t, 300*768_000, bytes)
	for _, s := range plan {
		assert.Equal(t, 19, s.pos)
	}
}

func TestPlanSamples_RejectsOnlyBeyondTheCeiling(t *testing.T) {
	tables := make([]sampleTable, 1500) // 1500 last articles of 768 KB are 1.15 GB
	for i := range tables {
		tables[i] = bigTable(20, false)
	}
	_, err := planSamples(tables, 5000, 1<<30)
	require.ErrorIs(t, err, errTooManyTables)
	_, err = planSamples(tables[:1000], 5000, 1<<30)
	require.NoError(t, err)
	_, err = planSamples(tables[:100], 50, 1<<30) // article ceiling
	require.ErrorIs(t, err, errTooManyTables)
}

// When the budget covers the last articles but not everything, the tables that
// also get their first and an interior article are chosen at random.
func TestPlanSamples_OptionalSamplesGoToRandomTables(t *testing.T) {
	tables := make([]sampleTable, 100)
	for i := range tables {
		tables[i] = bigTable(120, false) // 100 x 92 MB: a 9.2 GB release, 1% = 92 MB
	}
	chosen := map[int]int{}
	for range 30 {
		plan, err := planSamples(tables, 5000, 1<<30)
		require.NoError(t, err)
		n, bytes := planBytes(tables, plan)
		assert.GreaterOrEqual(t, n, 100)
		assert.LessOrEqual(t, bytes, int64(92_160_000), "within 1 percent of the release")
		for _, s := range plan {
			if s.pos == 0 {
				chosen[s.table]++
			}
		}
	}
	assert.Greater(t, len(chosen), 60, "which tables get the optional articles is random, so every table is eventually covered")
}

// Two tables that start with the same article are two tables (they used to
// collapse into one when keyed by their first message-id).
func TestTablesOf_KeysByTheWholeTable(t *testing.T) {
	seg := func(r int64) bundle.Segment { return bundle.Segment{Ref: r, Size: 10, Start: 0, End: 9} }
	b := &bundle.Bundle{Pool: []string{"a@x", "b@x", "c@x"}, Files: []bundle.File{
		{Name: "A.mkv", Segments: []bundle.Segment{seg(0), seg(1)}},
		{Name: "B.mkv", Segments: []bundle.Segment{seg(0), seg(2)}},
	}}
	assert.Len(t, tablesOf(b, nil), 2)
}

// The cost of the accept-time header sampling for typical release shapes (768 KB
// articles, 100 MB volumes). Logged for the documentation; the bounds are
// asserted.
func TestSamplingCostByReleaseShape(t *testing.T) {
	volumes := func(n, articles int) []sampleTable {
		out := make([]sampleTable, n)
		for i := range out {
			out[i] = bigTable(articles, false)
		}
		return out
	}
	for name, tc := range map[string]struct {
		tables   []sampleTable
		maxBytes int64
	}{
		"1 file (1.1 GB mkv)":  {[]sampleTable{bigTable(1500, true)}, 8 << 20},
		"50 volumes (100 MB)":  {volumes(50, 140), 70_000_000},
		"200 volumes (100 MB)": {volumes(200, 140), 220_000_000},
		"300 volumes (50 MB)":  {volumes(300, 70), 250 << 20},
		"500 volumes (100 MB)": {volumes(500, 140), 540_000_000},
	} {
		plan, err := planSamples(tc.tables, 2000, 1<<30)
		require.NoError(t, err, name)
		n, bytes := planBytes(tc.tables, plan)
		t.Logf("%s: %d articles, %.0f MB read", name, n, float64(bytes)/1e6)
		assert.LessOrEqual(t, bytes, tc.maxBytes, name)
	}
}

// slowHeaders answers instantly for the articles in fast and blocks the rest
// until the sampling time is over.
type slowHeaders struct {
	SegmentHeaders
	fast map[string]bool
}

func (s slowHeaders) SegmentHeader(ctx context.Context, id string) (YencHeader, error) {
	if s.fast[id] {
		return s.SegmentHeaders.SegmentHeader(ctx, id)
	}
	<-ctx.Done()
	return YencHeader{}, ctx.Err()
}

// The sampling is capped by time, not rejected: optional samples that do not
// answer in time are dropped, while every mandatory one still has to answer.
func TestAccept_OptionalSamplesRunningOutOfTimeAreDropped(t *testing.T) {
	e := newEnv(t)
	b := e.mkBundle("slow", fileSpec{name: "main.mkv", plain: mkvPlain(512), segSize: 128}, fileSpec{name: "side.mkv", plain: mkvPlain(256), segSize: 64})
	fast := map[string]bool{}
	var sideLast string
	for _, f := range b.Files {
		last := b.Pool[f.Segments[len(f.Segments)-1].Ref]
		if f.Name == "side.mkv" {
			sideLast = last
		}
		if f.Name == "main.mkv" {
			for _, s := range f.Segments {
				fast[b.Pool[s.Ref]] = true // everything of the main file
			}
		}
	}
	fast[sideLast] = true // only the last article of the other file
	e.imp.cfg.Headers = slowHeaders{SegmentHeaders: e.imp.cfg.Headers, fast: fast}
	e.imp.cfg.Policy.YencTimeout = 200 * time.Millisecond
	_, err := e.imp.Accept(context.Background(), e.req(b))
	require.NoError(t, err, "%v", err)

	delete(fast, sideLast) // now a mandatory sample does not answer
	e2 := newEnv(t)
	b2 := e2.mkBundle("slow2", fileSpec{name: "main.mkv", plain: mkvPlain(512), segSize: 128})
	e2.imp.cfg.Headers = slowHeaders{SegmentHeaders: e2.imp.cfg.Headers, fast: map[string]bool{}}
	e2.imp.cfg.Policy.YencTimeout = 100 * time.Millisecond
	_, err = e2.imp.Accept(context.Background(), e2.req(b2))
	require.Error(t, err)
	assert.Equal(t, ReasonProbeInconclusive, ReasonOf(err))
}

// The parts of the main media file are sampled like one table: one interior
// article per 500 segments of the whole file (at least three), spread over the
// parts at random, plus the last article of every part and the first article of
// every part that gets an archive header check.
func TestPlanMust_MainFilePartsAreSampledAsOneTable(t *testing.T) {
	parts := func(n, articles int) []sampleTable {
		out := make([]sampleTable, n)
		for i := range out {
			out[i] = bigTable(articles, false)
			out[i].part, out[i].mainPart, out[i].index, out[i].need0 = true, true, i, true
		}
		return out
	}
	for _, tc := range []struct{ n, articles, interior int }{{50, 140, 14}, {200, 140, 56}, {3, 100, 3}} {
		tables := parts(tc.n, tc.articles)
		must, _ := planMust(tables)
		last, first, interior := 0, 0, 0
		for _, s := range must {
			switch s.pos {
			case tc.articles - 1:
				last++
			case 0:
				first++
			default:
				interior++
			}
		}
		assert.Equal(t, tc.n, last, "the last article of every part")
		assert.Equal(t, tc.n, first, "the first article of every part that is bound")
		assert.Equal(t, tc.interior, interior, "%d parts: one interior article per 500 segments of the file, at least 3", tc.n)
	}
}

// What the multi-part checks cost for stored sets of 100 MB volumes at 768 KB
// articles (140 per volume): the mandatory samples only, because the header walk
// of every part reads the first article the yEnc sample already fetched.
func TestSamplingCostOfMultiVolumeSets(t *testing.T) {
	for _, tc := range []struct {
		volumes         int
		articles        int
		maxMB           float64
		wantArticlesMax int
	}{{50, 150, 130, 170}, {200, 150, 380, 500}, {600, 150, 1100, 1400}} {
		tables := make([]sampleTable, tc.volumes)
		for i := range tables {
			tables[i] = bigTable(tc.articles, false)
			tables[i].part, tables[i].mainPart, tables[i].index = true, true, i
			tables[i].need0 = true
		}
		must, mustBytes := planMust(tables)
		mb := float64(mustBytes) / 1e6
		t.Logf("%d volumes: %d mandatory samples (every part checked), %.0f MB", tc.volumes, len(must), mb)
		assert.LessOrEqual(t, mb, tc.maxMB)
		assert.LessOrEqual(t, len(must), tc.wantArticlesMax)
	}
}

func TestReadableName(t *testing.T) {
	assert.True(t, readableName("Release.part01.rar"))
	assert.True(t, readableName("a8.mkv"))
	assert.False(t, readableName("3f9a1c7e5b2d4a6c8e0f1a2b"), "random, no extension")
	assert.False(t, readableName("3f9a1c7e5b2d4a6c8e0f1a2b.rar"), "random stem")
	assert.False(t, readableName(""))
	assert.False(t, readableName("name."))
}
