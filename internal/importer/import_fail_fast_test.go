package importer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/javi11/nntppool/v5"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/importer/multifile"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
)

// failFastConfig is a config with import.fail_fast_on_missing_articles on, a
// 100 % sample, and no damage acceptable.
func failFastConfig() *config.Config {
	cfg := config.DefaultConfig()
	on := true
	cfg.Import.FailFastOnMissingArticles = &on
	cfg.Import.SegmentSamplePercentage = 100
	cfg.Health.AcceptableMissingSegmentsPercentage = 0
	return cfg
}

func failFastProc(client *fakepool.Client) *Processor {
	return &Processor{
		poolManager:       processorTestPoolManager{client: client},
		validationTimeout: 100 * time.Millisecond,
	}
}

func TestImportFailFastRejectsReleaseWithMissingMedia(t *testing.T) {
	client := fakepool.New()
	client.SetBehavior("movie-missing", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	n := buildTestNzb([]testNzbFile{
		{name: "Movie.2010.mkv", segID: "movie-missing"},
		{name: "Movie.2010.par2", segID: "par2-segment"},
	})
	cfg := failFastConfig()

	_, _, _, err := failFastProc(client).preParseFastFail(context.Background(), n, cfg, 1, nil, nil)
	if !errors.Is(err, multifile.ErrNoFilesProcessed) {
		t.Fatalf("err = %v, want ErrNoFilesProcessed straight from the probe", err)
	}
}

// A damaged episode fails the whole pack (no partial import) with fail-fast on.
func TestImportFailFastRejectsPackWithOneMissingEpisode(t *testing.T) {
	client := fakepool.New()
	client.SetBehavior("missing-segment", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	n := buildTestNzb([]testNzbFile{
		{name: "Show.S01E01.mkv", segID: "healthy-segment"},
		{name: "Show.S01E02.mkv", segID: "missing-segment"},
	})
	cfg := failFastConfig()

	_, _, _, err := failFastProc(client).preParseFastFail(context.Background(), n, cfg, 1, nil, nil)
	if !errors.Is(err, multifile.ErrNoFilesProcessed) {
		t.Fatalf("err = %v, want ErrNoFilesProcessed", err)
	}
}

// Only a sidecar missed: the media is re-checked and imports, minus the sidecar.
func TestImportFailFastDropsOnlyMissingSidecar(t *testing.T) {
	client := fakepool.New()
	client.SetBehavior("nfo-missing", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	n := buildTestNzb([]testNzbFile{
		{name: "Movie.2010.nfo", segID: "nfo-missing"},
		{name: "Movie.2010.mkv", segID: "movie-ok"},
	})
	cfg := failFastConfig()

	brokenIdx, _, _, err := failFastProc(client).preParseFastFail(context.Background(), n, cfg, 1, nil, nil)
	if err != nil {
		t.Fatalf("err = %v, want the media to import", err)
	}
	if _, ok := brokenIdx[0]; !ok || len(brokenIdx) != 1 {
		t.Fatalf("brokenIdx = %v, want only the .nfo (index 0)", brokenIdx)
	}
}

// Sidecar and media both missing: the re-check of the media fails the release.
func TestImportFailFastSidecarRecheckStillCatchesMedia(t *testing.T) {
	client := fakepool.New()
	client.SetBehavior("nfo-missing", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	client.SetBehavior("movie-missing", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	n := buildTestNzb([]testNzbFile{
		{name: "Movie.2010.nfo", segID: "nfo-missing"},
		{name: "Movie.2010.mkv", segID: "movie-missing"},
	})
	cfg := failFastConfig()

	_, _, _, err := failFastProc(client).preParseFastFail(context.Background(), n, cfg, 1, nil, nil)
	if !errors.Is(err, multifile.ErrNoFilesProcessed) {
		t.Fatalf("err = %v, want ErrNoFilesProcessed", err)
	}
}

func TestImportFailFastHealthyReleasePasses(t *testing.T) {
	client := fakepool.New()
	n := buildTestNzb([]testNzbFile{
		{name: "Movie.2010.mkv", segID: "movie-ok"},
		{name: "Movie.2010.nfo", segID: "nfo-ok"},
	})
	cfg := failFastConfig()

	brokenIdx, missing, _, err := failFastProc(client).preParseFastFail(context.Background(), n, cfg, 1, nil, nil)
	if err != nil || len(brokenIdx) != 0 || len(missing) != 0 {
		t.Fatalf("got (%v, %v, %v), want a clean pass", brokenIdx, missing, err)
	}
}

// With some damage tolerated, fail-fast stays out of the way (upstream path).
func TestImportFailFastOffWhenDamageIsTolerated(t *testing.T) {
	client := fakepool.New()
	client.SetBehavior("missing-segment", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	n := buildTestNzb([]testNzbFile{
		{name: "Show.S01E01.mkv", segID: "healthy-segment"},
		{name: "Show.S01E02.mkv", segID: "missing-segment"},
	})
	cfg := failFastConfig()
	cfg.Health.AcceptableMissingSegmentsPercentage = 2

	_, _, _, err := failFastProc(client).preParseFastFail(context.Background(), n, cfg, 1, nil, nil)
	if err != nil {
		t.Fatalf("err = %v, want the upstream partial import", err)
	}
}

// Off by default: a damaged pack keeps the partial import of its healthy files.
func TestImportFailFastIsOffByDefault(t *testing.T) {
	client := fakepool.New()
	client.SetBehavior("missing-segment", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	n := buildTestNzb([]testNzbFile{
		{name: "Show.S01E01.mkv", segID: "healthy-segment"},
		{name: "Show.S01E02.mkv", segID: "missing-segment"},
	})
	cfg := config.DefaultConfig()
	cfg.Import.SegmentSamplePercentage = 100
	cfg.Health.AcceptableMissingSegmentsPercentage = 0

	_, _, _, err := failFastProc(client).preParseFastFail(context.Background(), n, cfg, 1, nil, nil)
	if errors.Is(err, multifile.ErrNoFilesProcessed) {
		t.Fatalf("err = %v with fail_fast_on_missing_articles unset, want the partial-import path", err)
	}
}
