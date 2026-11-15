package bundleimport

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tenLyingFiles is a bundle of forty media files whose every article says a
// different size in its yEnc header than the layout declares.
func tenLyingFiles(t *testing.T, e *env, tag string) ([]string, *countingHeaders) {
	t.Helper()
	files := []fileSpec{}
	for i := range 40 {
		files = append(files, mkvSpec(fmt.Sprintf("m%d.mkv", i)))
	}
	b := e.mkBundle(tag, files...)
	for _, id := range b.Pool {
		beh := e.fp.BehaviorFor(id)
		beh.YEnc.PartSize += 1000
		e.fp.SetBehavior(id, beh)
	}
	ch := &countingHeaders{SegmentHeaders: e.imp.cfg.Headers}
	e.imp.cfg.Headers = ch
	_, err := e.imp.Accept(context.Background(), e.req(b))
	require.Error(t, err)
	assert.Equal(t, ReasonYencMismatch, ReasonOf(err))
	return b.Pool, ch
}

// The first contradicting sample ends the sampling: the rest of the plan is
// never fetched (only the reads already in flight finish).
func TestVerify_FirstContradictionStopsTheRemainingSamples(t *testing.T) {
	e := newEnv(t)
	_, ch := tenLyingFiles(t, e, "stop")
	assert.LessOrEqual(t, int(ch.n.Load()), 2*sampleConcurrency, "reads after the first contradiction")
}

// Bytes are charged as they are read, not as declared: articles that deliver
// far more than the layout says, behind headers that agree with the layout,
// use the budget up and are a contradiction of the bundle's numbers.
func TestVerify_ArticlesLargerThanDeclaredExhaustTheBudget(t *testing.T) {
	e := newEnv(t)
	files := []fileSpec{}
	for i := range 40 {
		files = append(files, mkvSpec(fmt.Sprintf("m%d.mkv", i)))
	}
	b := e.mkBundle("big", files...)
	for _, id := range b.Pool {
		beh := e.fp.BehaviorFor(id)
		beh.Bytes = append(beh.Bytes, make([]byte, 40_000)...) // 40 KB behind a 128-byte claim
		e.fp.SetBehavior(id, beh)
	}
	ch := &countingHeaders{SegmentHeaders: e.imp.cfg.Headers}
	e.imp.cfg.Headers = ch
	e.imp.cfg.Policy.MaxYencBytes = 9_000 // fits the declared samples, not 40 KB articles
	_, err := e.imp.Accept(context.Background(), e.req(b))
	require.Error(t, err)
	assert.Equal(t, ReasonYencMismatch, ReasonOf(err), "%v", err)
	assert.LessOrEqual(t, int(ch.n.Load()), 2*sampleConcurrency)
	e.assertNothingBehind()
}

// The evidence of a contradiction is the contradicting article's message-ID,
// so two attempts that hit the same article count once.
func TestVerify_ContradictionEvidenceIsTheArticleMessageID(t *testing.T) {
	e := newEnv(t)
	b := e.mkBundle("ev", mkvSpec("movie.mkv"))
	beh := e.fp.BehaviorFor(b.Pool[3])
	beh.YEnc.PartSize += 5
	e.fp.SetBehavior(b.Pool[3], beh)
	req := e.req(b)
	req.Origin.BundleID = "bundle-ev"
	_, err := e.imp.Accept(context.Background(), req)
	require.Error(t, err)
	assert.Equal(t, ReasonYencMismatch, ReasonOf(err))
	require.Len(t, e.imp.strikes.m["bundle-ev"], 1)
	assert.Contains(t, e.imp.strikes.m["bundle-ev"], b.Pool[3])
}

type stubHeaders struct{ h YencHeader }

func (s stubHeaders) SegmentHeader(context.Context, string) (YencHeader, error) { return s.h, nil }

// Only an article that itself delivers more than the layout declares is
// evidence against the publisher. A budget that other reads (the staged
// excess) had already spent is this node's accounting: an honest article that
// merely tips it over is not a contradiction.
func TestReadSample_BudgetSpentByOtherReadsIsNotAttributable(t *testing.T) {
	e := newEnv(t)
	tbl := sampleTable{name: "f", ids: []string{"a@x"}, sizes: []int64{100}, prefix: []int64{0}, total: 100}
	e.imp.cfg.Headers = stubHeaders{YencHeader{PartSize: 100, PartBegin: 0, FileSize: 100, Bytes: 100}}
	bud := newCostBudget(1000)
	bud.used = 950 // spent by staged reads
	v := e.imp.readSample(context.Background(), tbl, sample{table: 0, pos: 0, must: true}, bud)
	assert.NotEmpty(t, v.reason)
	assert.False(t, v.attributable, "an honest article that tips a spent budget over is not the publisher's doing")

	e.imp.cfg.Headers = stubHeaders{YencHeader{PartSize: 100, PartBegin: 0, FileSize: 100, Bytes: 5000}}
	v = e.imp.readSample(context.Background(), tbl, sample{table: 0, pos: 0, must: true}, newCostBudget(1000))
	assert.Equal(t, ReasonYencMismatch, v.reason)
	assert.True(t, v.attributable, "an article larger than declared is a contradiction")
}

// The node-wide meter is charged what the whole accept really read, from the
// first read to the last: the media probe and the structure reads included, and
// nothing is charged twice or from a stale reservation.
func TestAccept_TheNodeWideMeterShowsWhatTheWholeAcceptReallyRead(t *testing.T) {
	e := newEnv(t)
	now := time.Unix(1_700_000_000, 0)
	e.imp.cfg.Verify = NewVerifyLimiter(VerifyLimits{}, func() time.Time { return now })
	b := e.mkBundle("mt", mkvSpec("movie.mkv"))
	res, err := e.imp.Accept(context.Background(), e.req(b))
	require.NoError(t, err)
	u := e.imp.VerifyUsage()
	assert.Zero(t, u.Active, "the lease ended with the accept")
	assert.Positive(t, u.UsedBytes)
	assert.EqualValues(t, res.Cost.Bytes, u.UsedBytes, "the meter and the status agree on the bytes read")
}

// A media file's structure reads stop when they have downloaded more than their
// allowance, whatever the number of reads.
func TestStructureReads_AreCappedByDownloadedBytes(t *testing.T) {
	data := mkvPlain(4096)
	fs := afero.NewMemMapFs()
	require.NoError(t, afero.WriteFile(fs, "m.mkv", data, 0o644))
	err := verifyStructureMode(context.Background(), memOpener{fs}, "m.mkv", int64(len(data)), structLoose, func() bool { return true })
	require.Error(t, err)
	assert.ErrorIs(t, err, errCost)
	assert.NoError(t, verifyStructureMode(context.Background(), memOpener{fs}, "m.mkv", int64(len(data)), structLoose, func() bool { return false }))
}
