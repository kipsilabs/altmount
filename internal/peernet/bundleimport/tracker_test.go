package bundleimport

import (
	"context"
	"errors"
	"os/exec"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/peernet/bundle"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (e *env) accept(tag string, specs ...fileSpec) (*bundle.Bundle, *Result) {
	b := e.mkBundle(tag, specs...)
	res, err := e.imp.Accept(context.Background(), e.req(b))
	require.NoError(e.t, err)
	return b, res
}

func TestTracker_PendingAndPublishEligibility(t *testing.T) {
	e := newEnv(t)
	_, res := e.accept("tr", mkvSpec("movie.mkv"))
	ctx := context.Background()
	p := res.Paths[0]

	from, passed, err := e.imp.PeerBundleStatus(ctx, p)
	require.NoError(t, err)
	assert.True(t, from)
	assert.False(t, passed, "not publishable until the full check passed")
	assert.Contains(t, e.imp.PendingFullCheck(ctx, []string{p, "other.mkv"}), p)
	assert.NotContains(t, e.imp.PendingFullCheck(ctx, []string{"other.mkv"}), "other.mkv")

	from, passed, err = e.imp.PeerBundleStatus(ctx, "movies/local-import.mkv")
	require.NoError(t, err)
	assert.False(t, from, "a normal import is not from a peer")
	assert.False(t, passed)

	e.imp.FullCheckPassed(ctx, p)
	from, passed, _ = e.imp.PeerBundleStatus(ctx, p)
	assert.True(t, from)
	assert.True(t, passed, "publish-eligible once passed")
	assert.Empty(t, e.imp.PendingFullCheck(ctx, []string{p}))
}

func TestTracker_ARRAnnouncedOnceAfterLastFilePasses(t *testing.T) {
	e := newEnv(t)
	_, res := e.accept("arr", mkvSpec("a.mkv"), mkvSpec("b.mkv"))
	ctx := context.Background()
	require.Len(t, res.Paths, 2)

	e.imp.FullCheckPassed(ctx, res.Paths[0])
	assert.Empty(t, e.ann.got, "one file still pending: nothing announced")
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); e.imp.FullCheckPassed(ctx, res.Paths[1]) }()
	}
	wg.Wait()
	e.imp.FullCheckPassed(ctx, res.Paths[1])
	require.Len(t, e.ann.got, 1, "exactly once")
	assert.Equal(t, int64(11), e.ann.got[0].QueueID)
	assert.Equal(t, "movies", e.ann.got[0].Category)
	assert.Equal(t, "movies/Test.Release", e.ann.got[0].ResultingPath)
}

func TestTracker_GateFailsClosedAcrossRestart(t *testing.T) {
	e := newEnv(t)
	_, res := e.accept("restart", mkvSpec("a.mkv"))
	ctx := context.Background()

	// "Restart": a brand new importer over the same database.
	imp2 := New(Config{Library: e.lib, State: e.repo, Announcer: e.ann, HealthEnabled: func() bool { return true }})
	assert.Contains(t, imp2.PendingFullCheck(ctx, res.Paths), res.Paths[0], "still pending after restart")
	assert.True(t, imp2.QueueHasPending(ctx, 11))
	imp2.ResumeAnnouncements(ctx)
	assert.Empty(t, e.ann.got, "no announcement while pending")

	// The pass was recorded but the process died before the ARRs were told.
	_, err := e.repo.MarkPassed(ctx, res.Paths[0])
	require.NoError(t, err)
	imp3 := New(Config{Library: e.lib, State: e.repo, Announcer: e.ann})
	imp3.ResumeAnnouncements(ctx)
	imp3.ResumeAnnouncements(ctx)
	assert.Len(t, e.ann.got, 1, "delivered once after restart")
}

func TestTracker_FailedAnnouncementIsRetried(t *testing.T) {
	e := newEnv(t)
	_, res := e.accept("retry", mkvSpec("a.mkv"))
	var calls atomic.Int32
	e.imp.cfg.Announcer = announcerFunc(func(Announcement) error {
		if calls.Add(1) == 1 {
			return errors.New("arr down")
		}
		return nil
	})
	ctx := context.Background()
	e.imp.FullCheckPassed(ctx, res.Paths[0])
	e.imp.ResumeAnnouncements(ctx)
	assert.Equal(t, int32(2), calls.Load())
	e.imp.ResumeAnnouncements(ctx)
	assert.Equal(t, int32(2), calls.Load(), "no third delivery")
}

type announcerFunc func(Announcement) error

func (f announcerFunc) AnnounceVerifiedImport(_ context.Context, a Announcement) error { return f(a) }

func TestTracker_FailedFullCheckReportsPublisherAndNeverAnnounces(t *testing.T) {
	e := newEnv(t)
	b, res := e.accept("bad", mkvSpec("a.mkv"), mkvSpec("b.mkv"))
	ctx := context.Background()
	e.imp.FullCheckPassed(ctx, res.Paths[0])
	e.imp.FullCheckFailed(ctx, res.Paths[1])
	e.imp.FullCheckFailed(ctx, res.Paths[0]) // passed already: no-op

	assert.Equal(t, []string{strings.Repeat("d", 64) + "/" + b.ReleaseID.String()}, e.rep.got)
	assert.Empty(t, e.ann.got)
	_, passed, _ := e.imp.PeerBundleStatus(ctx, res.Paths[1])
	assert.False(t, passed, "a failed file never becomes publish-eligible")
	c := e.imp.Counters()
	assert.Equal(t, uint64(1), c.FullCheckFailed)
	assert.Equal(t, uint64(1), c.FullCheckPassed)
	assert.Equal(t, uint64(1), c.BadBundleReports)
}

func TestTracker_CallerSuppressedARRIsNeverAnnounced(t *testing.T) {
	e := newEnv(t)
	b := e.mkBundle("skip", mkvSpec("a.mkv"))
	req := e.req(b)
	req.Target.SkipArr = true
	res, err := e.imp.Accept(context.Background(), req)
	require.NoError(t, err)
	e.imp.FullCheckPassed(context.Background(), res.Paths[0])
	assert.Empty(t, e.ann.got)
}

type fakeFetcher struct {
	b     *bundle.Bundle
	err   error
	calls atomic.Int32
	gate  chan struct{}
}

func (f *fakeFetcher) FetchBundle(ctx context.Context, _ string) (*bundle.Bundle, Origin, error) {
	f.calls.Add(1)
	if f.gate != nil {
		<-f.gate
	}
	return f.b, Origin{NetworkID: strings.Repeat("c", 64), PublisherID: strings.Repeat("d", 64)}, f.err
}

type fakeRecorder struct {
	mu   sync.Mutex
	recs []ImportRecord
}

func (r *fakeRecorder) RecordPeerImport(_ context.Context, rec ImportRecord) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recs = append(r.recs, rec)
	return 77, nil
}

func TestImportRelease_FetchAcceptAndRecord(t *testing.T) {
	e := newEnv(t)
	b := e.mkBundle("ir", mkvSpec("movie.mkv"))
	rec := &fakeRecorder{}
	e.imp.cfg.Fetcher = &fakeFetcher{b: b}
	e.imp.cfg.Recorder = rec
	res, err := e.imp.ImportRelease(context.Background(), b.ReleaseID.String(), Target{Category: "movies"})
	require.NoError(t, err)
	assert.Equal(t, "movies/Test Release ir", res.Dir, "folder is category plus the sanitised bundle title")
	require.Len(t, rec.recs, 1)
	assert.Equal(t, "Test Release ir", rec.recs[0].Title)
	assert.Equal(t, res.Paths, rec.recs[0].WrittenPaths)
	assert.Equal(t, b.ReleaseID.String(), rec.recs[0].ReleaseID)
	assert.True(t, e.imp.QueueHasPending(context.Background(), 77), "rows attached to the new queue item")
}

func TestImportRelease_IdempotentAndCoalesced(t *testing.T) {
	e := newEnv(t)
	b := e.mkBundle("co", mkvSpec("movie.mkv"))
	f := &fakeFetcher{b: b, gate: make(chan struct{})}
	rec := &fakeRecorder{}
	e.imp.cfg.Fetcher, e.imp.cfg.Recorder = f, rec
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := e.imp.ImportRelease(context.Background(), b.ReleaseID.String(), Target{Category: "movies"})
			assert.NoError(t, err)
		}()
	}
	for f.calls.Load() == 0 {
	}
	close(f.gate)
	wg.Wait()
	assert.Equal(t, int32(1), f.calls.Load(), "concurrent requests coalesce into one fetch")
	assert.Len(t, rec.recs, 1)

	res, err := e.imp.ImportRelease(context.Background(), b.ReleaseID.String(), Target{Category: "movies"})
	require.NoError(t, err)
	assert.True(t, res.AlreadyImported)
	assert.Len(t, rec.recs, 1, "an already imported release is not recorded twice")
}

func TestImportRelease_FailuresLeaveNoRecord(t *testing.T) {
	e := newEnv(t)
	b := e.mkBundle("fail", mkvSpec("movie.mkv"))
	rec := &fakeRecorder{}
	e.imp.cfg.Recorder = rec
	e.imp.cfg.Fetcher = &fakeFetcher{err: errors.New("no peer")}
	_, err := e.imp.ImportRelease(context.Background(), b.ReleaseID.String(), Target{Category: "movies"})
	assert.Equal(t, ReasonUnavailable, ReasonOf(err))

	e.imp.cfg.Fetcher = nil
	_, err = e.imp.ImportRelease(context.Background(), b.ReleaseID.String(), Target{Category: "movies"})
	assert.Equal(t, ReasonNoNetwork, ReasonOf(err))

	e.prober.err = ErrProbeMissing
	e.imp.cfg.Fetcher = &fakeFetcher{b: b}
	_, err = e.imp.ImportRelease(context.Background(), b.ReleaseID.String(), Target{Category: "movies"})
	assert.Equal(t, ReasonProbeMissing, ReasonOf(err))
	assert.Empty(t, rec.recs)
	e.assertNothingBehind()
}

func TestImportRelease_RejectsABundleOfAnotherRelease(t *testing.T) {
	e := newEnv(t)
	b := e.mkBundle("one", mkvSpec("movie.mkv"))
	other := e.mkBundle("two", mkvSpec("movie.mkv"))
	e.imp.cfg.Fetcher = &fakeFetcher{b: other}
	e.imp.cfg.Recorder = &fakeRecorder{}
	_, err := e.imp.ImportRelease(context.Background(), b.ReleaseID.String(), Target{Category: "movies"})
	assert.Equal(t, ReasonInvalid, ReasonOf(err))
	e.assertNothingBehind()
}

// No archive analysis and no key derivation may be reachable from this
// package: the layout is already resolved. The serving stack's rclone cipher
// does link a KDF, but only for rclone-encrypted files, which a bundle cannot
// carry; so the KDF ban is on this package's own imports.
func TestPackageNeverImportsArchiveAnalysisOrKDF(t *testing.T) {
	deps, err := exec.Command("go", "list", "-deps", ".").Output()
	require.NoError(t, err)
	for _, dep := range strings.Fields(string(deps)) {
		for _, banned := range []string{"internal/importer/archive", "rardecode", "sevenzip", "internal/importer/multifile", "internal/importer/singlefile"} {
			assert.NotContains(t, dep, banned)
		}
	}
	direct, err := exec.Command("go", "list", "-f", "{{join .Imports \"\\n\"}}", ".").Output()
	require.NoError(t, err)
	for _, imp := range strings.Fields(string(direct)) {
		assert.NotContains(t, imp, "x/crypto")
		assert.NotContains(t, imp, "pbkdf2")
		assert.NotContains(t, imp, "scrypt")
	}
}

func TestTracker_VerifiedBundleIsReportedOnceEveryFilePassed(t *testing.T) {
	e := newEnv(t)
	b, res := e.accept("good", mkvSpec("a.mkv"), mkvSpec("b.mkv"))
	ctx := context.Background()
	e.imp.FullCheckPassed(ctx, res.Paths[0])
	assert.Empty(t, e.rep.verified, "one file still pending")
	e.imp.FullCheckPassed(ctx, res.Paths[1])
	assert.Equal(t, []string{strings.Repeat("d", 64) + "/" + b.ReleaseID.String()}, e.rep.verified)
	assert.Empty(t, e.rep.got)
}

func TestImportRelease_PassesMediaMetadataAndDownloadIDToTheRecorder(t *testing.T) {
	e := newEnv(t)
	b := e.mkBundle("md", mkvSpec("movie.mkv"))
	rec := &fakeRecorder{}
	e.imp.cfg.Fetcher, e.imp.cfg.Recorder = &fakeFetcher{b: b}, rec
	_, err := e.imp.ImportRelease(context.Background(), b.ReleaseID.String(),
		Target{Category: "movies", Metadata: `{"imdb_id":"tt1"}`, DownloadID: "stremio:x"})
	require.NoError(t, err)
	require.Len(t, rec.recs, 1)
	assert.Equal(t, `{"imdb_id":"tt1"}`, rec.recs[0].Metadata)
	assert.Equal(t, "stremio:x", rec.recs[0].DownloadID)
}

func TestImportRelease_DirFuncChoosesTheFolderFromTheSanitisedTitle(t *testing.T) {
	e := newEnv(t)
	b := e.mkBundle("df", mkvSpec("movie.mkv"))
	b.Title = "Some Title (2020)"
	e.imp.cfg.Fetcher, e.imp.cfg.Recorder = &fakeFetcher{b: b}, &fakeRecorder{}
	var got string
	res, err := e.imp.ImportRelease(context.Background(), b.ReleaseID.String(), Target{Category: "movies",
		DirFunc: func(title string) string { got = title; return "complete/movies/" + title }})
	require.NoError(t, err)
	assert.Equal(t, bundle.SanitizeFilename(b.Title), got, "the function only ever sees the sanitised title")
	assert.Equal(t, "complete/movies/"+got, res.Dir)
}

func TestImportRelease_HostileTitlesNeverEscapeTheCategoryFolder(t *testing.T) {
	for _, title := range []string{"...mkv", ".mkv", ".hidden", "patches"} {
		e := newEnv(t)
		b := e.mkBundle("ht", mkvSpec("movie.mkv"))
		b.Title = title
		e.imp.cfg.Fetcher, e.imp.cfg.Recorder = &fakeFetcher{b: b}, &fakeRecorder{}
		// Without a DirFunc the default folder is category plus title.
		res, err := e.imp.ImportRelease(context.Background(), b.ReleaseID.String(), Target{Category: "movies"})
		if err == nil {
			assert.Equal(t, "movies", path.Dir(res.Dir), "title %q: %q", title, res.Dir)
			assert.NotContains(t, []string{".", "..", ""}, path.Base(res.Dir), title)
		}
		require.Error(t, err, "title %q must be refused", title)
		assert.Equal(t, ReasonBadTarget, ReasonOf(err), title)
		// A DirFunc that refuses the title (returns "") never falls back.
		e2 := newEnv(t)
		b2 := e2.mkBundle("ht2", mkvSpec("movie.mkv"))
		b2.Title = title
		e2.imp.cfg.Fetcher, e2.imp.cfg.Recorder = &fakeFetcher{b: b2}, &fakeRecorder{}
		_, err = e2.imp.ImportRelease(context.Background(), b2.ReleaseID.String(), Target{Category: "movies", DirFunc: func(string) string { return "" }})
		assert.Equal(t, ReasonBadTarget, ReasonOf(err), title)
	}
}

func TestCleanTargetRefusesReservedAndCategoryOnlyFolders(t *testing.T) {
	for _, dir := range []string{"movies", "complete/.nzbs/x", "movies/patches/x", "movies/.peernet-staging", "movies/Corrupted_Metadata"} {
		_, err := cleanTarget(dir)
		assert.Error(t, err, dir)
	}
	got, err := cleanTarget("complete/movies/Some Movie")
	require.NoError(t, err)
	assert.Equal(t, "complete/movies/Some Movie", got)
}

func TestEscalateStale_FailsOldPendingImportsAndNeverAnnounces(t *testing.T) {
	e := newEnv(t)
	_, res := e.accept("st", mkvSpec("a.mkv"))
	ctx := context.Background()
	require.NoError(t, e.repo.SetQueue(ctx, res.Paths, 11))

	pending, stale := e.imp.PendingStatus(ctx)
	assert.Equal(t, 1, pending)
	assert.Zero(t, stale, "a fresh import is not stale")

	e.imp.cfg.Now = func() time.Time { return time.Now().Add(MaxPendingAge + time.Hour) }
	_, stale = e.imp.PendingStatus(ctx)
	assert.Equal(t, 1, stale)
	assert.Equal(t, 1, e.imp.EscalateStale(ctx))

	_, ok, _ := e.repo.Get(ctx, res.Paths[0])
	assert.False(t, ok, "a failed import is removed with its state rows")
	assert.False(t, e.lib.FileExists(res.Paths[0]), "and so are its files")
	assert.Empty(t, e.ann.got, "an unverified import is never announced")
	assert.Empty(t, e.imp.PendingFullCheck(ctx, res.Paths), "no longer pending")
	assert.Empty(t, e.rep.got, "the publisher is not blamed for a check that never ran")
}

func TestImportRelease_FirstCallerLeavingDoesNotFailTheOthers(t *testing.T) {
	e := newEnv(t)
	b := e.mkBundle("sf", mkvSpec("movie.mkv"))
	gate := make(chan struct{})
	e.imp.cfg.Fetcher, e.imp.cfg.Recorder = &fakeFetcher{b: b, gate: gate}, &fakeRecorder{}
	first, leave := context.WithCancel(context.Background())
	errFirst := make(chan error, 1)
	go func() {
		_, err := e.imp.ImportRelease(first, b.ReleaseID.String(), Target{Category: "movies"})
		errFirst <- err
	}()
	time.Sleep(50 * time.Millisecond)
	type out struct {
		res *Result
		err error
	}
	second := make(chan out, 1)
	go func() {
		r, err := e.imp.ImportRelease(context.Background(), b.ReleaseID.String(), Target{Category: "movies"})
		second <- out{r, err}
	}()
	time.Sleep(50 * time.Millisecond)
	leave()
	close(gate)
	o := <-second
	require.NoError(t, o.err, "a follower must not inherit the first caller's cancellation")
	require.NotNil(t, o.res)
	<-errFirst
}
