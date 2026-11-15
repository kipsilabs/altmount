package bundleimport

import (
	"context"
	"fmt"
	"testing"

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
