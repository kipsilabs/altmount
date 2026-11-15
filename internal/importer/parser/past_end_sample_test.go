package parser

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/javi11/nntppool/v5"

	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
)

// Past pastEndSampleSize extras only a sample is STAT-checked: every sampled
// extra dead trims every file, anything else trims none.

// manyVolumes builds `volumes` files of five articles each that need three, so
// every file lists two extras (2 × volumes in total).
func manyVolumes(fp *fakepool.Client, volumes int, dead bool) ([]*FirstSegmentData, map[string]firstSegmentYencInfo) {
	var cache []*FirstSegmentData
	sizes := map[string]firstSegmentYencInfo{}
	for v := range volumes {
		file := pastEndFile(fp, fmt.Sprintf("vol%03d", v), 3000)
		if dead {
			setDead(fp, file.Segments[3].ID, file.Segments[4].ID)
		}
		sizes[file.Segments[0].ID] = firstSegmentYencInfo{PartSize: pastEndPartSize, FileSize: 3000}
		cache = append(cache, &FirstSegmentData{File: &file})
	}
	return cache, sizes
}

func segmentsLeft(cache []*FirstSegmentData) map[int]int {
	out := map[int]int{}
	for _, d := range cache {
		out[len(d.File.Segments)]++
	}
	return out
}

func TestPastEndSampleTrimsAllWhenSampleIsDead(t *testing.T) {
	fp := fakepool.New()
	cache, sizes := manyVolumes(fp, 100, true) // 200 extras
	p := NewParser(newFakeFullPoolManager(fp), stormConfigGetter(50))

	p.trimDeadArticlesPastEnd(context.Background(), cache, sizes, time.Time{})

	if got := segmentsLeft(cache); got[3] != 100 {
		t.Fatalf("segments per file = %v, want all 100 files trimmed to 3", got)
	}
	if got := fp.StatCalls(); got > int64(pastEndSampleSize) {
		t.Fatalf("STAT calls = %d, want at most the %d-article sample", got, pastEndSampleSize)
	}
}

func TestPastEndSampleTrimsNothingWhenExtrasAreAlive(t *testing.T) {
	fp := fakepool.New()
	cache, sizes := manyVolumes(fp, 100, false) // extras alive: the size was wrong
	p := NewParser(newFakeFullPoolManager(fp), stormConfigGetter(50))

	p.trimDeadArticlesPastEnd(context.Background(), cache, sizes, time.Time{})

	if got := segmentsLeft(cache); got[5] != 100 {
		t.Fatalf("segments per file = %v, want every file left with its 5 articles", got)
	}
	if got := fp.StatCalls(); got > int64(pastEndSampleSize) {
		t.Fatalf("STAT calls = %d, want at most the %d-article sample", got, pastEndSampleSize)
	}
}

// A sample that cannot be answered inside its budget trims nothing, fast.
func TestPastEndSampleIsBoundedInTime(t *testing.T) {
	prev := pastEndSampleBudget
	pastEndSampleBudget = 300 * time.Millisecond
	t.Cleanup(func() { pastEndSampleBudget = prev })
	fp := fakepool.New()
	cache, sizes := manyVolumes(fp, 100, false)
	for v := range 100 {
		for _, i := range []int{3, 4} {
			fp.SetBehavior(fmt.Sprintf("vol%03d-%d", v, i), fakepool.SegmentBehavior{
				Err: nntppool.ErrArticleNotFound, Latency: 5 * time.Second,
			})
		}
	}
	p := NewParser(newFakeFullPoolManager(fp), stormConfigGetter(50))

	start := time.Now()
	p.trimDeadArticlesPastEnd(context.Background(), cache, sizes, time.Time{})
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("took %s, want the sample bounded by its 300ms budget", elapsed)
	}
	if got := segmentsLeft(cache); got[5] != 100 {
		t.Fatalf("segments per file = %v, want nothing trimmed on an unanswered sample", got)
	}
}

// Few extras keep the exact per-file check (upstream behaviour).
func TestPastEndSmallReleaseChecksEveryExtra(t *testing.T) {
	fp := fakepool.New()
	cache, sizes := manyVolumes(fp, 10, true) // 20 extras, within the sample size
	// one file's extras alive: only that file must be left alone
	fp.SetBehavior("vol004-3", fakepool.SegmentBehavior{YEnc: nntppool.YEncMeta{PartSize: pastEndPartSize, FileSize: 3000}})
	p := NewParser(newFakeFullPoolManager(fp), stormConfigGetter(50))

	p.trimDeadArticlesPastEnd(context.Background(), cache, sizes, time.Time{})

	if got := segmentsLeft(cache); got[3] != 9 || got[5] != 1 {
		t.Fatalf("segments per file = %v, want 9 trimmed and 1 (vol004) untouched", got)
	}
}
