package parser

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/javi11/nntppool/v5"
	"github.com/javi11/nzbparser"

	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
)

const pastEndPartSize = 1000

// pastEndFile lists five articles for a file that needs three.
func pastEndFile(fp *fakepool.Client, name string, fileSize int64) nzbparser.NzbFile {
	file := nzbparser.NzbFile{Filename: name}
	for i := range 5 {
		id := fmt.Sprintf("%s-%d", name, i)
		file.Segments = append(file.Segments, nzbparser.NzbSegment{ID: id, Number: i + 1, Bytes: 1100})
		file.Bytes += 1100
		fp.SetBehavior(id, fakepool.SegmentBehavior{YEnc: nntppool.YEncMeta{PartSize: pastEndPartSize, FileSize: fileSize}})
	}
	return file
}

func setDead(fp *fakepool.Client, ids ...string) {
	for _, id := range ids {
		fp.SetBehavior(id, fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	}
}

func TestFindDeadArticlesPastEnd(t *testing.T) {
	tests := []struct {
		name     string
		fileSize int64
		dead     []string
		wantKeep int // 0 means the file must be left alone
	}{
		{"every extra is dead", 3000, []string{"f-3", "f-4"}, 3},
		{"short last part still needs its article", 2500, []string{"f-3", "f-4"}, 3},
		{"an extra is alive", 3000, []string{"f-4"}, 0},
		{"no extras", 5000, []string{"f-4"}, 0},
		{"unknown file size", 0, []string{"f-3", "f-4"}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fp := fakepool.New()
			files := []nzbparser.NzbFile{pastEndFile(fp, "f", tt.fileSize)}
			setDead(fp, tt.dead...)
			p := NewParser(newFakeFullPoolManager(fp), stormConfigGetter(4))

			keep := p.FindDeadArticlesPastEnd(context.Background(), files)
			if got := keep[0]; got != tt.wantKeep {
				t.Fatalf("keep = %d, want %d", got, tt.wantKeep)
			}
			if len(files[0].Segments) != 5 {
				t.Fatal("finding must not change the file: the warm-up may be reading it")
			}
		})
	}
}

func TestTrimArticlesDropsExtrasAndTheirBytes(t *testing.T) {
	fp := fakepool.New()
	files := []nzbparser.NzbFile{pastEndFile(fp, "f", 3000)}
	p := NewParser(newFakeFullPoolManager(fp), stormConfigGetter(4))

	p.TrimArticles(context.Background(), files, map[int]int{0: 3})

	if len(files[0].Segments) != 3 || files[0].Bytes != 3300 {
		t.Fatalf("segments = %d, bytes = %d, want 3 and 3300", len(files[0].Segments), files[0].Bytes)
	}
}

// The sampled availability check can miss the extras, so parsing drops them too.
func TestParseTrimDropsExtrasTheSampleMissed(t *testing.T) {
	fp := fakepool.New()
	file := pastEndFile(fp, "f", 3000)
	setDead(fp, "f-3", "f-4")
	p := NewParser(newFakeFullPoolManager(fp), stormConfigGetter(4))
	sizes := map[string]firstSegmentYencInfo{"f-0": {PartSize: pastEndPartSize, FileSize: 3000}}

	p.trimDeadArticlesPastEnd(context.Background(), []*FirstSegmentData{{File: &file}}, sizes, time.Time{})

	if len(file.Segments) != 3 {
		t.Fatalf("segments = %d, want 3", len(file.Segments))
	}
}

// A release can list hundreds of dead extras and a 430 is slow to answer, so
// checking them one at a time can take minutes. They must go out together.
func TestParseTrimChecksExtrasInParallel(t *testing.T) {
	fp := fakepool.New()
	var cache []*FirstSegmentData
	sizes := map[string]firstSegmentYencInfo{}
	for v := range 4 {
		file := pastEndFile(fp, fmt.Sprintf("v%d", v), 3000)
		for _, i := range []int{3, 4} {
			fp.SetBehavior(fmt.Sprintf("v%d-%d", v, i), fakepool.SegmentBehavior{
				Err: nntppool.ErrArticleNotFound, Latency: 20 * time.Millisecond,
			})
		}
		sizes[file.Segments[0].ID] = firstSegmentYencInfo{PartSize: pastEndPartSize, FileSize: 3000}
		cache = append(cache, &FirstSegmentData{File: &file})
	}
	p := NewParser(newFakeFullPoolManager(fp), stormConfigGetter(4))

	p.trimDeadArticlesPastEnd(context.Background(), cache, sizes, time.Time{})

	for _, data := range cache {
		if len(data.File.Segments) != 3 {
			t.Fatalf("%s has %d segments, want 3", data.File.Filename, len(data.File.Segments))
		}
	}
	if got := fp.MaxInFlight(); got < 2 {
		t.Fatalf("max in flight = %d, want extras checked in parallel", got)
	}
}

// unansweredStatClient leaves every check unanswered for its first few sweeps.
type unansweredStatClient struct {
	*fakepool.Client
	unansweredSweeps int
	sweeps           atomic.Int32
}

func (c *unansweredStatClient) ExistsMany(ctx context.Context, ids []string, opts nntppool.ManyOptions) <-chan nntppool.ExistsResult {
	if int(c.sweeps.Add(1)) > c.unansweredSweeps {
		return c.Client.ExistsMany(ctx, ids, opts)
	}
	out := make(chan nntppool.ExistsResult, len(ids))
	for _, id := range ids {
		out <- nntppool.ExistsResult{MessageID: id, Err: errors.New("attempt expired awaiting response")}
	}
	close(out)
	return out
}

// A STAT that expires unanswered says nothing about the article: it must be asked
// again rather than leaving the file untrimmed, and an article still unknown after
// every attempt must keep its file untouched.
func TestConfirmDeadPastEndRetriesUnansweredChecks(t *testing.T) {
	backoff := pastEndRetryBackoff
	pastEndRetryBackoff = time.Millisecond
	t.Cleanup(func() { pastEndRetryBackoff = backoff })

	tests := []struct {
		name       string
		unanswered int
		wantSegs   int
	}{
		{"answered on retry", 2, 3},
		{"never answered", pastEndStatAttempts, 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fp := fakepool.New()
			file := pastEndFile(fp, "f", 3000)
			setDead(fp, "f-3", "f-4")
			client := &unansweredStatClient{Client: fp, unansweredSweeps: tt.unanswered}
			p := NewParser(newFakeFullPoolManager(client), stormConfigGetter(4))
			sizes := map[string]firstSegmentYencInfo{"f-0": {PartSize: pastEndPartSize, FileSize: 3000}}

			p.trimDeadArticlesPastEnd(context.Background(), []*FirstSegmentData{{File: &file}}, sizes, time.Time{})

			if len(file.Segments) != tt.wantSegs {
				t.Fatalf("segments = %d, want %d", len(file.Segments), tt.wantSegs)
			}
		})
	}
}

// concurrencyRecordingClient records the width of each ExistsMany sweep.
type concurrencyRecordingClient struct {
	*fakepool.Client
	widths []int
}

func (c *concurrencyRecordingClient) ExistsMany(ctx context.Context, ids []string, opts nntppool.ManyOptions) <-chan nntppool.ExistsResult {
	c.widths = append(c.widths, opts.Concurrency)
	return c.Client.ExistsMany(ctx, ids, opts)
}

// A batch wider than the connections can serve makes most checks expire.
func TestConfirmDeadPastEndSizesBatchToConnections(t *testing.T) {
	fp := fakepool.New()
	file := pastEndFile(fp, "f", 3000)
	setDead(fp, "f-3", "f-4")
	client := &concurrencyRecordingClient{Client: fp}
	p := NewParser(newFakeFullPoolManager(client), stormConfigGetter(4))
	sizes := map[string]firstSegmentYencInfo{"f-0": {PartSize: pastEndPartSize, FileSize: 3000}}

	p.trimDeadArticlesPastEnd(context.Background(), []*FirstSegmentData{{File: &file}}, sizes, time.Time{})

	want := 4 * pastEndStatsPerConnection
	if len(client.widths) != 1 || client.widths[0] > want {
		t.Fatalf("sweep widths = %v, want one sweep no wider than %d", client.widths, want)
	}
}
