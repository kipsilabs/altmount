package parser

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/javi11/nntppool/v5"
	"github.com/javi11/nzbparser"
	"github.com/kipsilabs/altmount/internal/importer/parser/par2"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/kipsilabs/altmount/internal/testsupport/par2gen"
)

type recordingStore struct {
	mu   sync.Mutex
	puts map[string][]byte
}

func (s *recordingStore) Get(id string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.puts[id]
	return d, ok
}
func (s *recordingStore) Put(id string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.puts == nil {
		s.puts = map[string][]byte{}
	}
	s.puts[id] = append([]byte(nil), data...)
	return nil
}

func (s *recordingStore) get(id string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.puts[id]
	return d, ok
}

// The first article of every file is fetched at import anyway; handing the
// whole decoded body to the streaming segment store means the cold open a
// moment later is served from memory instead of a second provider round trip.
func TestWarmFirstSegmentsPublishesWholeArticleToSegmentStore(t *testing.T) {
	article := bytes.Repeat([]byte("A"), 700*1024)
	fp := fakepool.New()
	fp.SetBehavior("vid-0", fakepool.SegmentBehavior{
		Bytes: article,
		YEnc:  nntppool.YEncMeta{FileName: "Real.Movie.2024.mkv", FileSize: int64(len(article)) * 2, Part: 1, PartSize: int64(len(article))},
	})
	nzb := &nzbparser.Nzb{Files: nzbparser.NzbFiles{
		{Filename: "Real.Movie.2024.mkv", Segments: nzbparser.NzbSegments{
			{Bytes: 720000, Number: 1, ID: "vid-0"},
			{Bytes: 720000, Number: 2, ID: "vid-1"},
		}},
	}}
	store := &recordingStore{}
	p := NewParser(newFakeFullPoolManager(fp), stormConfigGetter(4))
	p.SetSegmentStore(func() SegmentStore { return store })

	p.WarmFirstSegments(context.Background(), nzb.Files)

	got, ok := store.get("vid-0")
	if !ok {
		t.Fatal("warm-up did not put vid-0 into the segment store")
	}
	if !bytes.Equal(got, article) {
		t.Fatalf("stored %d bytes, want the whole %d-byte article (not the clipped head)", len(got), len(article))
	}
	if _, ok := store.get("vid-1"); ok {
		t.Fatal("warm-up stored vid-1, which it never fetched")
	}
}

func TestWarmFirstSegmentsWithoutStoreStillWarmsHeads(t *testing.T) {
	fp := fakepool.New()
	fp.SetBehavior("vid-0", fakepool.SegmentBehavior{Bytes: []byte("x"), YEnc: nntppool.YEncMeta{FileSize: 1, PartSize: 1}})
	nzb := &nzbparser.Nzb{Files: nzbparser.NzbFiles{
		{Filename: "Real.Movie.2024.mkv", Segments: nzbparser.NzbSegments{{Bytes: 10, Number: 1, ID: "vid-0"}}},
	}}
	p := NewParser(newFakeFullPoolManager(fp), stormConfigGetter(4))
	p.SetSegmentStore(func() SegmentStore { return nil })

	p.WarmFirstSegments(context.Background(), nzb.Files)
	if got := fp.PerMessageCalls("vid-0"); got != 1 {
		t.Fatalf("warm-up fetched vid-0 %d times, want 1", got)
	}
}


// waitForStore waits for a detached store-only warm fetch to land.
func waitForStore(t *testing.T, store *recordingStore, id string) []byte {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for {
		if d, ok := store.get(id); ok {
			return d
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s never landed in the segment store", id)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func cleanVideos() nzbparser.NzbFiles {
	seg := func(prefix string) nzbparser.NzbSegments {
		return nzbparser.NzbSegments{
			{Bytes: 720000, Number: 1, ID: prefix + "-0"},
			{Bytes: 720000, Number: 2, ID: prefix + "-1"},
			{Bytes: 720000, Number: 3, ID: prefix + "-2"},
		}
	}
	return nzbparser.NzbFiles{
		{Filename: "Show.S01E01.1080p.WEB-DL.mkv", Bytes: 4 << 30, Segments: seg("e1")},
		{Filename: "Show.S01E02.1080p.WEB-DL.mkv", Bytes: 6 << 30, Segments: seg("e2")},
	}
}

// Clean-named videos skip their first-segment fetch to save bandwidth, but the
// largest one is what a player opens first: with a segment store to keep the
// article in, warming just that file turns the cold open into a cache hit.
func TestWarmFirstSegmentsWarmsLargestVideoWhenStoreIsWired(t *testing.T) {
	fp := fakepool.New()
	fp.SetDefaultBehavior(fakepool.SegmentBehavior{Bytes: []byte("v"), YEnc: nntppool.YEncMeta{FileSize: 1, PartSize: 1}})
	store := &recordingStore{}
	p := NewParser(newFakeFullPoolManager(fp), stormConfigGetter(4))
	p.SetSegmentStore(func() SegmentStore { return store })

	p.WarmFirstSegments(context.Background(), cleanVideos())

	waitForStore(t, store, "e2-0")
	if got := fp.PerMessageCalls("e2-0"); got != 1 {
		t.Fatalf("largest video first segment fetched %d times, want 1", got)
	}
	if got := fp.PerMessageCalls("e1-0"); got != 0 {
		t.Fatalf("smaller video first segment fetched %d times, want 0: only the largest is warmed", got)
	}
}

func TestWarmFirstSegmentsKeepsSkippingCleanVideosWithoutStore(t *testing.T) {
	fp := fakepool.New()
	fp.SetDefaultBehavior(fakepool.SegmentBehavior{Bytes: []byte("v"), YEnc: nntppool.YEncMeta{FileSize: 1, PartSize: 1}})
	p := NewParser(newFakeFullPoolManager(fp), stormConfigGetter(4))

	p.WarmFirstSegments(context.Background(), cleanVideos())

	if got := fp.PerMessageCalls("e1-0") + fp.PerMessageCalls("e2-0"); got != 0 {
		t.Fatalf("clean-named videos fetched %d first segments without a store, want 0", got)
	}
}

func sevenZipVolumes() nzbparser.NzbFiles {
	seg := func(prefix string) nzbparser.NzbSegments {
		return nzbparser.NzbSegments{
			{Bytes: 720000, Number: 1, ID: prefix + "-0"},
			{Bytes: 720000, Number: 2, ID: prefix + "-1"},
			{Bytes: 720000, Number: 3, ID: prefix + "-2"},
			{Bytes: 120000, Number: 4, ID: prefix + "-3"},
		}
	}
	return nzbparser.NzbFiles{
		{Filename: "Movie.7z.001", Bytes: 2 << 30, Segments: seg("v1")},
		{Filename: "Movie.7z.002", Bytes: 2 << 30, Segments: seg("v2")},
		{Filename: "Movie.7z.003", Bytes: 1 << 30, Segments: seg("v3")},
	}
}

// 7z analysis reads the first volume's head and the last volume's tail, each
// a cold provider round trip today. With a store to keep them in, warm-up
// fetches the last volume's last two articles alongside the heads.
func TestWarmFirstSegmentsWarmsLastSevenZipVolumeTail(t *testing.T) {
	fp := fakepool.New()
	fp.SetDefaultBehavior(fakepool.SegmentBehavior{Bytes: []byte("z"), YEnc: nntppool.YEncMeta{FileSize: 1, PartSize: 1}})
	store := &recordingStore{}
	p := NewParser(newFakeFullPoolManager(fp), stormConfigGetter(4))
	p.SetSegmentStore(func() SegmentStore { return store })

	p.WarmFirstSegments(context.Background(), sevenZipVolumes())

	for _, id := range []string{"v3-3", "v3-2"} {
		waitForStore(t, store, id)
		if got := fp.PerMessageCalls(id); got != 1 {
			t.Errorf("tail article %s fetched %d times, want 1", id, got)
		}
	}
	if got := fp.PerMessageCalls("v2-3") + fp.PerMessageCalls("v1-3") + fp.PerMessageCalls("v3-1"); got != 0 {
		t.Fatalf("unrelated articles fetched %d times, want 0", got)
	}
}

func TestWarmFirstSegmentsSkipsSevenZipTailWithoutStore(t *testing.T) {
	fp := fakepool.New()
	fp.SetDefaultBehavior(fakepool.SegmentBehavior{Bytes: []byte("z"), YEnc: nntppool.YEncMeta{FileSize: 1, PartSize: 1}})
	p := NewParser(newFakeFullPoolManager(fp), stormConfigGetter(4))

	p.WarmFirstSegments(context.Background(), sevenZipVolumes())

	if got := fp.PerMessageCalls("v3-3") + fp.PerMessageCalls("v3-2"); got != 0 {
		t.Fatalf("tail articles fetched %d times without a store, want 0", got)
	}
}

// The parse pass fetches one "representative" middle segment's yEnc header to
// learn the release-wide part size; started only after every first segment is
// in, it is a whole provider round trip on the critical path. Warm-up runs
// alongside the fast-fail probe, so fetching it there makes the parse a cache hit.
func TestWarmFirstSegmentsPrefetchesRepresentativeMiddleHeader(t *testing.T) {
	fp := fakepool.New()
	fp.SetDefaultBehavior(fakepool.SegmentBehavior{Bytes: []byte("v"), YEnc: nntppool.YEncMeta{FileSize: 3, PartSize: 1, Part: 2}})
	files := nzbparser.NzbFiles{
		{Filename: "Some.Release-GRP.r00", Segments: nzbparser.NzbSegments{
			{Bytes: 720000, Number: 1, ID: "r00-0"}, {Bytes: 720000, Number: 2, ID: "r00-1"}, {Bytes: 720000, Number: 3, ID: "r00-2"},
		}},
	}
	p := NewParser(newFakeFullPoolManager(fp), stormConfigGetter(4))

	p.WarmFirstSegments(context.Background(), files)

	if got := fp.PerMessageCalls("r00-1"); got != 1 {
		t.Fatalf("representative middle segment fetched %d times during warm-up, want 1", got)
	}
	if head, ok := p.heads.get("r00-1"); !ok || head.meta.PartSize != 1 {
		t.Fatalf("representative header not cached (ok=%v, %+v)", ok, head.meta)
	}
}

// Fetches that exist only to fill the segment store (the largest video's first
// article, the 7z tail) must not hold up the parse: WarmFirstSegments returns
// once the heads the parse needs are in, and those fetches finish behind it.
func TestWarmFirstSegmentsDoesNotWaitForStoreOnlyFetches(t *testing.T) {
	fp := fakepool.New()
	fp.SetDefaultBehavior(fakepool.SegmentBehavior{Bytes: []byte("v"), YEnc: nntppool.YEncMeta{FileSize: 1, PartSize: 1}})
	fp.SetBehavior("e2-0", fakepool.SegmentBehavior{Bytes: []byte("slow"), YEnc: nntppool.YEncMeta{FileSize: 1, PartSize: 1}, Latency: 1500 * time.Millisecond})
	store := &recordingStore{}
	p := NewParser(newFakeFullPoolManager(fp), stormConfigGetter(4))
	p.SetSegmentStore(func() SegmentStore { return store })

	start := time.Now()
	p.WarmFirstSegments(context.Background(), cleanVideos())
	if elapsed := time.Since(start); elapsed > 700*time.Millisecond {
		t.Fatalf("WarmFirstSegments blocked %s on a store-only fetch", elapsed)
	}
	deadline := time.Now().Add(4 * time.Second)
	for {
		if _, ok := store.get("e2-0"); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("store-only fetch never landed in the segment store")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// The PAR2 index's first segment is fetched by the warm-up and published to the
// segment store; descriptor extraction reading it back from there costs no
// provider round trip.
func TestGetFileDescriptorsReadsIndexFromSegmentStore(t *testing.T) {
	set := par2gen.BuildFull(1024, []par2gen.FileEntry{
		{Name: "Movie.mkv", Content: bytes.Repeat([]byte("m"), 4096)},
	}, 1)
	fp := fakepool.New() // no behaviour for idx-0: a fetch would fail
	store := &recordingStore{}
	_ = store.Put("idx-0", set.Index)
	file := &nzbparser.NzbFile{Filename: "Movie.par2", Segments: nzbparser.NzbSegments{{Bytes: len(set.Index), Number: 1, ID: "idx-0"}}}
	cache := []*par2.FirstSegmentData{{File: file, RawBytes: set.Index[:min(len(set.Index), 16*1024)]}}

	descriptors, err := par2.GetFileDescriptors(context.Background(), cache, newFakeFullPoolManager(fp), store)
	if err != nil {
		t.Fatalf("GetFileDescriptors error = %v", err)
	}
	if len(descriptors) != 1 {
		t.Fatalf("descriptors = %d, want 1", len(descriptors))
	}
	if got := fp.PerMessageCalls("idx-0"); got != 0 {
		t.Fatalf("index segment fetched %d times, want 0: it was in the segment store", got)
	}
}
