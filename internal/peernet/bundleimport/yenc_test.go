package bundleimport

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

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
	}
	return t
}

func TestPlanSamples_EveryTableGetsFirstLastAndAnInterior(t *testing.T) {
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
	seen := map[int]struct{}{}
	for _, ps := range per {
		require.Len(t, ps, 3)
		assert.Contains(t, ps, 0)
		assert.Contains(t, ps, 9)
		for _, p := range ps {
			if p != 0 && p != 9 {
				seen[p] = struct{}{}
			}
		}
	}
	assert.Greater(t, len(seen), 3, "interior picks vary")
}

func TestPlanSamples_MediaTablesAreSampledDenserAndTheCapHolds(t *testing.T) {
	plan, err := planSamples([]sampleTable{mkTable(5000, true)}, 600, 1<<30)
	require.NoError(t, err)
	assert.Len(t, plan, 2+10, "first, last and one interior per 500 segments")
	plan, err = planSamples([]sampleTable{mkTable(5000, true), mkTable(10, false)}, 8, 1<<30)
	require.NoError(t, err)
	assert.LessOrEqual(t, len(plan), 8)
}

func TestPlanSamples_RejectsWhenTheMinimumDoesNotFit(t *testing.T) {
	tables := make([]sampleTable, 20)
	for i := range tables {
		tables[i] = mkTable(10, false)
	}
	_, err := planSamples(tables, 59, 1<<30)
	require.ErrorIs(t, err, errTooManyTables)
	_, err = planSamples(tables, 600, 100) // bytes
	require.ErrorIs(t, err, errTooManyTables)
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
// articles). Logged for the documentation; the bounds are asserted.
func TestSamplingCostByReleaseShape(t *testing.T) {
	pol := DefaultPolicy()
	table := func(n int, main bool) sampleTable {
		tb := mkTable(n, main)
		for i := range tb.sizes {
			tb.sizes[i] = 768_000
		}
		return tb
	}
	volumes := func(n int) []sampleTable {
		out := make([]sampleTable, n)
		for i := range out {
			out[i] = table(140, false) // a 100 MB volume
		}
		return out
	}
	for name, tc := range map[string]struct {
		tables  []sampleTable
		wantMax int
	}{
		"1 file (1.1 GB mkv)": {[]sampleTable{table(1500, true)}, 5},
		"50 volumes":          {volumes(50), 150},
		"200 volumes":         {volumes(200), 600},
	} {
		plan, err := planSamples(tc.tables, pol.YencSamples(), pol.YencBytes())
		require.NoError(t, err, name)
		var bytes int64
		for _, s := range plan {
			bytes += tc.tables[s.table].sizes[s.pos]
		}
		t.Logf("%s: %d articles, %.0f MB read", name, len(plan), float64(bytes)/1e6)
		assert.LessOrEqual(t, len(plan), tc.wantMax, name)
	}
	_, err := planSamples(volumes(201), pol.YencSamples(), pol.YencBytes())
	require.ErrorIs(t, err, errTooManyTables, "201 volumes exceed the budget and are rejected, not skipped")
}
