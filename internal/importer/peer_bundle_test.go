package importer

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/javi11/nntppool/v5"
	"github.com/javi11/nzbparser"
	"github.com/kipsilabs/altmount/internal/database"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/kipsilabs/altmount/internal/testsupport/nzbbuild"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeFastPath struct {
	mu        sync.Mutex
	reqs      []PeerBundleRequest
	take      bool
	panics    bool
	env       *batteryEnv
	hold      map[int64]bool
	attach    error
	noOffers  bool
	offerAsks atomic.Int32
	// onAttach runs while the queue row exists but the import is unfinished.
	onAttach func(queueID int64)
}

func (f *fakeFastPath) TryBundle(_ context.Context, req PeerBundleRequest) (PeerBundleOutcome, bool) {
	f.mu.Lock()
	f.reqs = append(f.reqs, req)
	f.mu.Unlock()
	if f.panics {
		panic("boom")
	}
	if !f.take {
		return PeerBundleOutcome{}, false
	}
	p := req.TargetDir + "/movie.mkv"
	_ = f.env.svc.WriteFileMetadata(p, &metapb.FileMetadata{FileSize: 10, Status: metapb.FileStatus_FILE_STATUS_HEALTHY})
	return PeerBundleOutcome{ResultPath: req.TargetDir, WrittenPaths: []string{p}, TotalSize: 10}, true
}

func (f *fakeFastPath) MayHaveOffers(context.Context) bool {
	f.offerAsks.Add(1)
	return !f.noOffers
}

func (f *fakeFastPath) AttachQueueItem(_ context.Context, _ []string, id int64) error {
	if f.onAttach != nil {
		f.onAttach(id)
	}
	return f.attach
}

func (f *fakeFastPath) HoldQueueItem(_ context.Context, id int64) bool { return f.hold[id] }

type memRecorder struct{ rows []*database.ImportHistory }

func (r *memRecorder) AddImportHistory(_ context.Context, h *database.ImportHistory) error {
	r.rows = append(r.rows, h)
	return nil
}

func missingBehavior() fakepool.SegmentBehavior {
	return fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound}
}

// registerContentWithLatency is registerContent with a per-article delay.
func (e *batteryEnv) registerContentWithLatency(prefix string, content []byte, partSize int, latency time.Duration) []nzbbuild.Segment {
	segs := e.registerContent(prefix, content, partSize, 1.0, nil)
	for i, s := range segs {
		end := min((i+1)*partSize, len(content))
		e.client.SetBehavior(s.ID, fakepool.SegmentBehavior{Bytes: content[i*partSize : end], Latency: latency})
	}
	return segs
}

func cleanNZB(e *batteryEnv) (nzbPath string) {
	content := make([]byte, 4096)
	for i := range content {
		content[i] = byte(i % 251)
	}
	segs := e.registerContent("fp", content, 1024, 1.0, nil)
	return nzbbuild.WriteTemp(e.t, nzbbuild.Build(nzbbuild.File{Subject: "movie.mkv", Segments: segs}), "Some.Release")
}

func TestPeerFastPath_TakenSkipsProbeAndAnalysisAndRecordsHistory(t *testing.T) {
	e := newBatteryEnv(t)
	rec := &memRecorder{}
	e.proc.SetRecorder(rec)
	fp := &fakeFastPath{take: true, env: e}
	e.proc.SetPeerBundleFastPath(fp)
	nzbPath := cleanNZB(e)

	result, written, err := e.proc.ProcessNzbFile(context.Background(), nzbPath, filepath.Dir(nzbPath), 5, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "/Some.Release", result)
	assert.Equal(t, []string{"/Some.Release/movie.mkv"}, written)
	assert.Zero(t, e.client.StatCalls(), "no release probe")
	assert.Zero(t, e.client.BodyCalls(), "no article download")
	require.Len(t, fp.reqs, 1)
	assert.Equal(t, 5, fp.reqs[0].QueueID)
	assert.Equal(t, "/Some.Release", fp.reqs[0].TargetDir)
	require.Len(t, fp.reqs[0].NZB.Files, 1)
	assert.Len(t, fp.reqs[0].NZB.Files[0].Segments, 4, "the parsed NZB's segment list is handed over")
	require.Len(t, rec.rows, 1)
	assert.Equal(t, "Some.Release.nzb", rec.rows[0].NzbName)
	assert.Equal(t, "/Some.Release", rec.rows[0].VirtualPath)
}

// The normal import result must be identical whether no fast path is installed
// or one is installed that declines, errors out or panics.
func TestPeerFastPath_FallbackLeavesTheNormalImportIdentical(t *testing.T) {
	run := func(install func(e *batteryEnv)) (string, []string, []byte) {
		e := newBatteryEnv(t)
		install(e)
		nzbPath := cleanNZB(e)
		res, written, err := e.proc.ProcessNzbFile(context.Background(), nzbPath, filepath.Dir(nzbPath), 5, nil, nil, nil, nil, nil, nil)
		require.NoError(t, err)
		meta, err := e.svc.ReadFileMetadata(written[0])
		require.NoError(t, err)
		return res, written, []byte(meta.OriginalFilename + "|" + filepath.Base(written[0]))
	}
	baseRes, baseWritten, baseMeta := run(func(*batteryEnv) {})
	for name, install := range map[string]func(e *batteryEnv){
		"declines":  func(e *batteryEnv) { e.proc.SetPeerBundleFastPath(&fakeFastPath{env: e}) },
		"panics":    func(e *batteryEnv) { e.proc.SetPeerBundleFastPath(&fakeFastPath{env: e, panics: true}) },
		"no offers": func(e *batteryEnv) { e.proc.SetPeerBundleFastPath(&fakeFastPath{env: e, noOffers: true}) },
	} {
		t.Run(name, func(t *testing.T) {
			res, written, meta := run(install)
			assert.Equal(t, baseRes, res)
			assert.Equal(t, baseWritten, written)
			assert.Equal(t, baseMeta, meta)
		})
	}
}

// With no offer on this node the import asks one cheap question and does
// nothing else for the peer network: no store is built for it, nothing is handed
// to the hook.
func TestPeerFastPath_NoOffersMeansNoPeerWork(t *testing.T) {
	e := newBatteryEnv(t)
	fp := &fakeFastPath{take: true, noOffers: true, env: e}
	e.proc.SetPeerBundleFastPath(fp)
	nzbPath := cleanNZB(e)
	n := parseTestNzb(t, nzbPath)

	result, written, taken, prebuilt := e.proc.tryPeerBundle(context.Background(), n, nzbPath, 5, "/", nil, nil, nil)
	assert.False(t, taken)
	assert.Empty(t, result)
	assert.Empty(t, written)
	assert.Nil(t, prebuilt, "no store was built")
	assert.Equal(t, int32(1), fp.offerAsks.Load())
	assert.Empty(t, fp.reqs, "the hook never saw the NZB")

	// Control: with offers the store is built once and kept for the parse.
	fp.noOffers, fp.take = false, false
	_, _, taken, prebuilt = e.proc.tryPeerBundle(context.Background(), n, nzbPath, 5, "/", nil, nil, nil)
	assert.False(t, taken)
	require.NotNil(t, prebuilt)
	assert.Same(t, prebuilt.store, fp.reqs[0].NZB, "the store handed to the hook is the one kept for the parse")
}

func TestPeerFastPath_RemovedHookIsNeverCalled(t *testing.T) {
	e := newBatteryEnv(t)
	fp := &fakeFastPath{take: true, env: e}
	e.proc.SetPeerBundleFastPath(fp)
	e.proc.SetPeerBundleFastPath(nil)
	nzbPath := cleanNZB(e)
	_, _, err := e.proc.ProcessNzbFile(context.Background(), nzbPath, filepath.Dir(nzbPath), 5, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	assert.Empty(t, fp.reqs)
}

func TestProbePeerRelease(t *testing.T) {
	e := newBatteryEnv(t)
	ids := []string{"a1@x", "a2@x", "a3@x", "a4@x", "a5@x", "a6@x"}
	require.NoError(t, e.proc.ProbePeerRelease(context.Background(), []PeerProbeList{{Name: "f.mkv", SegmentIDs: ids}}))
	assert.Positive(t, e.client.StatCalls(), "a STAT sample was taken")
	assert.Zero(t, e.client.BodyCalls(), "never a body")

	e2 := newBatteryEnv(t)
	e2.client.SetBehavior("a6@x", missingBehavior())
	err := e2.proc.ProbePeerRelease(context.Background(), []PeerProbeList{{Name: "f.mkv", SegmentIDs: ids}})
	assert.ErrorIs(t, err, ErrPeerProbeMissing, "the last segment of every file is always checked")
}

// The saved time on a synthetic archive import: every article costs 15 ms, as
// on a real provider. The fast path skips the probe, the first-segment warm-up
// and the archive analysis.
func TestPeerFastPath_TimeSavedOnArchiveImport(t *testing.T) {
	timed := func(withFastPath bool) time.Duration {
		e := newBatteryEnv(t)
		rarBytes := loadFixture(t, filepath.Join("rar_single", "archive.rar"))
		segs := e.registerContentWithLatency("tm", rarBytes, archivePartSize, 15*time.Millisecond)
		nzbPath := nzbbuild.WriteTemp(t, nzbbuild.Build(nzbbuild.File{Subject: "archive.rar", Segments: segs}), "archive")
		if withFastPath {
			e.proc.SetPeerBundleFastPath(&fakeFastPath{take: true, env: e})
		}
		start := time.Now()
		_, _, err := e.proc.ProcessNzbFile(context.Background(), nzbPath, filepath.Dir(nzbPath), 1, nil, nil, nil, nil, nil, nil)
		require.NoError(t, err)
		return time.Since(start)
	}
	normal, fast := timed(false), timed(true)
	t.Logf("archive import with 15ms/article: normal %v, bundle fast path %v (saved %v)", normal, fast, normal-fast)
	assert.Less(t, fast, normal)
}

func parseTestNzb(t *testing.T, path string) *nzbparser.Nzb {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	n, err := nzbparser.Parse(f)
	require.NoError(t, err)
	return n
}
