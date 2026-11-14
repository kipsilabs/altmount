package bundleimport

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVerifyPeerHeaders_AgreeingSamplePasses(t *testing.T) {
	e := newEnv(t)
	_, res := e.accept("vh1", mkvSpec("a.mkv"))
	require.NoError(t, e.imp.VerifyPeerHeaders(context.Background(), res.Paths[0]))
}

func TestVerifyPeerHeaders_LayoutLieFailsAndIsAttributable(t *testing.T) {
	e := newEnv(t)
	b, res := e.accept("vh2", mkvSpec("a.mkv"))
	// The lie sits in an interior article the accept-time sample may have
	// missed in a bigger release; here the whole file is sampled.
	beh := e.fp.BehaviorFor(b.Pool[2])
	beh.YEnc.PartBegin += 9
	e.fp.SetBehavior(b.Pool[2], beh)
	ctx := context.Background()

	err := e.imp.VerifyPeerHeaders(ctx, res.Paths[0])
	var lm *LayoutMismatchError
	require.ErrorAs(t, err, &lm)

	e.imp.FullCheckFailed(ctx, res.Paths[0])
	assert.Equal(t, []string{strings.Repeat("d", 64) + "/" + b.ReleaseID.String()}, e.rep.got, "a header contradiction is the publisher's")
}

func TestVerifyPeerHeaders_NotAPendingImportIsIgnored(t *testing.T) {
	e := newEnv(t)
	require.NoError(t, e.imp.VerifyPeerHeaders(context.Background(), "movies/none/x.mkv"))
}

func TestHeaderSampleSize(t *testing.T) {
	assert.Equal(t, 5, headerSampleSize(5))
	assert.Equal(t, 20, headerSampleSize(1000))
	assert.Equal(t, 100, headerSampleSize(10000))
	assert.Equal(t, 500, headerSampleSize(400000))
}

// A degraded verdict fails the import at once; with the user's NZB at hand
// the queue item is put back for a normal import, after the files are gone.
type requeueAnn struct {
	*failAnn
	requeued      []int64
	fileGoneAtReq bool
	can           bool
	path          string
	e             *env
}

func (a *requeueAnn) RequeueForNormalImport(_ context.Context, id int64) (bool, error) {
	a.requeued = append(a.requeued, id)
	a.fileGoneAtReq = !a.e.lib.FileExists(a.path)
	return a.can, nil
}

func TestDegraded_FastPathItemIsRequeuedForANormalImportAfterRemoval(t *testing.T) {
	e := newEnv(t)
	base := e.withFailAnn()
	_, res := e.accept("dg1", mkvSpec("a.mkv"))
	ann := &requeueAnn{failAnn: base, can: true, path: res.Paths[0], e: e}
	e.imp.cfg.Announcer = ann
	ctx := context.Background()

	e.imp.FullCheckDegraded(ctx, res.Paths[0])

	assert.Equal(t, []int64{11}, ann.requeued)
	assert.True(t, ann.fileGoneAtReq, "the normal import must not race the removal")
	assert.Empty(t, ann.failed, "the item is not marked failed: the user still gets their release")
	assert.Zero(t, e.rowCount(res.Paths...))
	assert.Empty(t, e.rep.got, "missing articles are not the publisher's fault")
	assert.Empty(t, ann.got, "never announced")
}

func TestDegraded_DirectImportIsFailedNotRequeued(t *testing.T) {
	e := newEnv(t)
	base := e.withFailAnn()
	_, res := e.accept("dg2", mkvSpec("a.mkv"))
	ann := &requeueAnn{failAnn: base, can: false, path: res.Paths[0], e: e}
	e.imp.cfg.Announcer = ann

	e.imp.FullCheckDegraded(context.Background(), res.Paths[0])

	assert.Equal(t, failReasonDegraded, ann.failed[11])
	assert.False(t, e.lib.FileExists(res.Paths[0]))
}

// A layout that failed local verification goes on the negative cache under its
// Bundle ID, so the same layout from another publisher is skipped.
func TestNegativeCache_AcceptTimeContradictionAndFullCheckMismatchAreReported(t *testing.T) {
	bid := strings.Repeat("e", 64)

	e := newEnv(t)
	b := e.mkBundle("nc1", mkvSpec("movie.mkv"))
	beh := e.fp.BehaviorFor(b.Pool[1])
	beh.YEnc.PartSize++
	e.fp.SetBehavior(b.Pool[1], beh)
	req := e.req(b)
	req.Origin.BundleID = bid
	_, err := e.imp.Accept(context.Background(), req)
	require.Error(t, err)
	assert.Equal(t, []string{bid + "/" + b.ReleaseID.String()}, e.rep.failedBundles)

	e2 := newEnv(t)
	b2, res := e2.accept("nc2", mkvSpec("a.mkv"))
	_ = b2
	require.NoError(t, e2.repo.SetBundleID(context.Background(), res.Paths, bid))
	beh = e2.fp.BehaviorFor(b2.Pool[2])
	beh.YEnc.PartBegin += 3
	e2.fp.SetBehavior(b2.Pool[2], beh)
	require.Error(t, e2.imp.VerifyPeerHeaders(context.Background(), res.Paths[0]))
	e2.imp.FullCheckFailed(context.Background(), res.Paths[0])
	assert.Equal(t, []string{bid + "/" + b2.ReleaseID.String()}, e2.rep.failedBundles)

	// A missing-only failure does not blacklist the layout.
	e3 := newEnv(t)
	_, res3 := e3.accept("nc3", mkvSpec("a.mkv"))
	require.NoError(t, e3.repo.SetBundleID(context.Background(), res3.Paths, bid))
	e3.imp.FullCheckFailed(context.Background(), res3.Paths[0])
	assert.Empty(t, e3.rep.failedBundles)
}
