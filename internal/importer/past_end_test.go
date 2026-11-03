package importer

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/javi11/nntppool/v5"
	"github.com/javi11/nzbparser"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/importer/parser"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
)

// pastEndRelease is a two-volume set plus PAR2. Each volume needs three
// articles; the first also lists two more, which are dead.
func pastEndRelease(client *fakepool.Client) *nzbparser.Nzb {
	head := fakepool.SegmentBehavior{YEnc: nntppool.YEncMeta{PartSize: 1000, FileSize: 3000}}
	var files []nzbparser.NzbFile
	for v, listed := range []int{5, 3} {
		file := nzbparser.NzbFile{Filename: fmt.Sprintf("Show.S01E01.part0%d.rar", v+1)}
		for i := range listed {
			id := fmt.Sprintf("v%d-%d", v, i)
			file.Segments = append(file.Segments, nzbparser.NzbSegment{ID: id, Number: i + 1, Bytes: 1100})
			client.SetBehavior(id, head)
		}
		files = append(files, file)
	}
	client.SetBehavior("v0-3", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	client.SetBehavior("v0-4", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	files = append(files, nzbparser.NzbFile{
		Filename: "Show.S01E01.par2",
		Segments: nzbparser.NzbSegments{{ID: "p1", Number: 1, Bytes: 100}},
	})
	return &nzbparser.Nzb{Files: files}
}

func pastEndProcessor(client *fakepool.Client, cfg *config.Config) *Processor {
	manager := processorTestPoolManager{client: client}
	return &Processor{
		poolManager:       manager,
		parser:            parser.NewParser(manager, func() *config.Config { return cfg }),
		validationTimeout: time.Second,
	}
}

func pastEndConfig() *config.Config {
	on := true
	cfg := config.DefaultConfig()
	cfg.Import.SegmentSamplePercentage = 100
	cfg.Par2Repair.Enabled = &on
	cfg.Par2Repair.RepairOnImport = &on
	return cfg
}

// A complete release whose only dead articles lie past the end of a file must
// import rather than wait for a PAR2 repair that has nothing to do.
func TestPreParseFastFailImportsReleaseWithDeadArticlesPastEnd(t *testing.T) {
	client := fakepool.New()
	n := pastEndRelease(client)
	cfg := pastEndConfig()
	proc := pastEndProcessor(client, cfg)

	_, _, _, err := proc.preParseFastFail(context.Background(), n, cfg, 1, nil, nil)
	var pastEnd *articlesPastEndError
	if !errors.As(err, &pastEnd) {
		t.Fatalf("err = %v, want articlesPastEndError", err)
	}
	proc.parser.TrimArticles(context.Background(), n.Files, pastEnd.keep)

	brokenIdx, missingIDs, _, err := proc.preParseFastFail(context.Background(), n, cfg, 1, nil, nil)
	if err != nil {
		t.Fatalf("after trimming: %v", err)
	}
	if len(brokenIdx) != 0 || len(missingIDs) != 0 {
		t.Fatalf("broken = %v, missing = %v, want a clean release", brokenIdx, missingIDs)
	}
}

// Real damage must still defer for repair once the extras are out of the way.
func TestPreParseFastFailStillDefersRealDamageAfterTrimming(t *testing.T) {
	client := fakepool.New()
	n := pastEndRelease(client)
	client.SetBehavior("v1-1", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	cfg := pastEndConfig()
	proc := pastEndProcessor(client, cfg)

	_, _, _, err := proc.preParseFastFail(context.Background(), n, cfg, 1, nil, nil)
	var pastEnd *articlesPastEndError
	if !errors.As(err, &pastEnd) {
		t.Fatalf("err = %v, want articlesPastEndError", err)
	}
	proc.parser.TrimArticles(context.Background(), n.Files, pastEnd.keep)

	_, _, _, err = proc.preParseFastFail(context.Background(), n, cfg, 1, nil, nil)
	var deferred *DeferredRepairError
	if !errors.As(err, &deferred) {
		t.Fatalf("err = %v, want DeferredRepairError", err)
	}
	if deferred.FirstMissingSegmentID != "v1-1" {
		t.Fatalf("first missing = %q, want v1-1", deferred.FirstMissingSegmentID)
	}
}
