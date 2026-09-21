package parser

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/kipsilabs/altmount/internal/testsupport/par2gen"
	"github.com/javi11/nntppool/v5"
	"github.com/javi11/nzbparser"
)

// A retried or repair-resumed import re-parses the same NZB. Everything the
// first pass learned from the wire — first-segment heads, yEnc headers, the
// PAR2 index — is immutable per message-id, so the second pass must not fetch
// any of it again.
func TestParseNzbReusesFetchedHeadsAcrossParses(t *testing.T) {
	content := bytes.Repeat([]byte("F"), 32768)
	par2Bytes := par2gen.Build(par2gen.FileEntry{Name: "Real.Movie.2024.mkv", Content: content})

	fp := fakepool.New()
	fp.SetBehavior("vid-0", fakepool.SegmentBehavior{
		Bytes: content[:16384],
		YEnc:  nntppool.YEncMeta{FileName: "a3f9c1d2e4b5a6f7c8d9e0f1a2b3c4d5", FileSize: 32768, Part: 1, PartBegin: 0, PartSize: 16384},
	})
	fp.SetBehavior("vid-1", fakepool.SegmentBehavior{
		Bytes: content[16384:],
		YEnc:  nntppool.YEncMeta{FileSize: 32768, Part: 2, PartBegin: 16384, PartSize: 16384},
	})
	fp.SetBehavior("p2-0", fakepool.SegmentBehavior{
		Bytes: par2Bytes,
		YEnc:  nntppool.YEncMeta{FileName: "release.par2", FileSize: int64(len(par2Bytes)), PartSize: int64(len(par2Bytes))},
	})

	newNzb := func() *nzbparser.Nzb {
		return &nzbparser.Nzb{Files: nzbparser.NzbFiles{
			{Filename: "a3f9c1d2e4b5a6f7c8d9e0f1a2b3c4d5", Segments: nzbparser.NzbSegments{
				{Bytes: 16800, Number: 1, ID: "vid-0"},
				{Bytes: 16800, Number: 2, ID: "vid-1"},
			}},
			{Filename: "release.par2", Segments: nzbparser.NzbSegments{
				{Bytes: len(par2Bytes) + 100, Number: 1, ID: "p2-0"},
			}},
		}}
	}

	p := NewParser(newFakeFullPoolManager(fp), stormConfigGetter(4))

	videoOf := func(parsed *ParsedNzb) ParsedFile {
		for _, f := range parsed.Files {
			if !f.IsPar2Archive {
				return f
			}
		}
		t.Fatalf("no video file in %+v", parsed.Files)
		return ParsedFile{}
	}

	first, err := p.ParseNzb(context.Background(), newNzb(), "release.nzb", nil, ParseOptions{})
	if err != nil {
		t.Fatalf("first ParseNzb error = %v", err)
	}
	if got := videoOf(first).Filename; got != "Real.Movie.2024.mkv" {
		t.Fatalf("first parse Filename = %q, want PAR2 name", got)
	}
	calls := map[string]int64{}
	for _, id := range []string{"vid-0", "vid-1", "p2-0"} {
		calls[id] = fp.PerMessageCalls(id)
	}
	if calls["p2-0"] == 0 || calls["vid-0"] == 0 {
		t.Fatalf("first parse issued no fetches: %+v", calls)
	}

	second, err := p.ParseNzb(context.Background(), newNzb(), "release.nzb", nil, ParseOptions{})
	if err != nil {
		t.Fatalf("second ParseNzb error = %v", err)
	}
	if videoOf(second).Filename != videoOf(first).Filename || videoOf(second).Size != videoOf(first).Size {
		t.Errorf("second parse differs: %+v vs %+v", videoOf(second), videoOf(first))
	}
	for id, n := range calls {
		if got := fp.PerMessageCalls(id); got != n {
			t.Errorf("%s fetched %d times after second parse, want %d (served from cache)", id, got, n)
		}
	}
}

func TestHeadCacheEvictsOldestWhenOverBudget(t *testing.T) {
	c := newHeadCache(200, time.Hour)
	c.put("a", articleHead{bytes: make([]byte, 60)})
	c.put("b", articleHead{bytes: make([]byte, 60)})
	if _, ok := c.get("a"); ok {
		t.Error("a should have been evicted to make room for b")
	}
	if _, ok := c.get("b"); !ok {
		t.Error("b should be present")
	}
	// An entry larger than the whole budget is not cached at all.
	c.put("huge", articleHead{bytes: make([]byte, 400)})
	if _, ok := c.get("huge"); ok {
		t.Error("oversized entry must not be cached")
	}
}

func TestHeadCacheExpiresEntries(t *testing.T) {
	now := time.Now()
	c := newHeadCache(1<<20, time.Minute)
	c.now = func() time.Time { return now }
	c.put("a", articleHead{meta: nntppool.YEncMeta{PartSize: 1}})
	now = now.Add(2 * time.Minute)
	if _, ok := c.get("a"); ok {
		t.Error("expired entry must not be returned")
	}
}

// A header-only entry must not overwrite one that already carries bytes.
func TestHeadCacheKeepsBytesOverHeaderOnly(t *testing.T) {
	c := newHeadCache(1<<20, time.Hour)
	c.put("a", articleHead{meta: nntppool.YEncMeta{PartSize: 5}, bytes: []byte("hello")})
	c.put("a", articleHead{meta: nntppool.YEncMeta{PartSize: 5}})
	got, _ := c.get("a")
	if string(got.bytes) != "hello" {
		t.Errorf("bytes = %q, want hello", got.bytes)
	}
}
