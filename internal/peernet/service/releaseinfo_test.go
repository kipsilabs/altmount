package service

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
)

func TestMediaFromMetadata(t *testing.T) {
	u32 := func(v uint32) *uint32 { return &v }
	for _, tc := range []struct {
		name, raw        string
		imdb             string
		tmdb, tvdb       uint32
		season, episode  *uint32
		wantEmptyOverall bool
	}{
		{name: "series", raw: `{"content_key":"k","release_key":"r","imdb_id":"tt0944947","type":"series","season":2,"episode":5,"tmdbId":1399,"tvdbId":121361}`,
			imdb: "tt0944947", tmdb: 1399, tvdb: 121361, season: u32(2), episode: u32(5)},
		{name: "movie has no season or episode", raw: `{"imdb_id":"tt0133093","type":"movie","tmdbId":603}`, imdb: "tt0133093", tmdb: 603},
		{name: "not json", raw: `not json`, wantEmptyOverall: true},
		{name: "empty", raw: ``, wantEmptyOverall: true},
		{name: "other json shape", raw: `{"source":"nzbdav","x":[1,2]}`, wantEmptyOverall: true},
		{name: "bad imdb dropped, ids kept", raw: `{"imdb_id":"../../etc","tmdbId":5}`, tmdb: 5},
		{name: "wrong types are dropped wholesale", raw: `{"imdb_id":"tt1","season":"two"}`, wantEmptyOverall: true},
		{name: "negative and huge numbers ignored", raw: `{"imdb_id":"tt1","tmdbId":-4,"tvdbId":99999999999,"season":-1}`, imdb: "tt1"},
		{name: "oversized", raw: `{"imdb_id":"tt1","pad":"` + strings.Repeat("x", 70_000) + `"}`, wantEmptyOverall: true},
		{name: "series without episode keeps season only", raw: `{"imdb_id":"tt1","type":"series","season":3}`, imdb: "tt1", season: u32(3)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := mediaFromMetadata(tc.raw)
			if tc.wantEmptyOverall {
				if m.IMDb != "" || m.TMDB != 0 || m.TVDB != 0 || m.Season != nil || m.Episode != nil {
					t.Fatalf("want empty, got %+v", m)
				}
				return
			}
			if m.IMDb != tc.imdb || m.TMDB != tc.tmdb || m.TVDB != tc.tvdb {
				t.Fatalf("got %+v", m)
			}
			eq := func(a, b *uint32) bool { return (a == nil) == (b == nil) && (a == nil || *a == *b) }
			if !eq(m.Season, tc.season) || !eq(m.Episode, tc.episode) {
				t.Fatalf("season/episode: %+v", m)
			}
		})
	}
}

func TestReleaseInfoFromMetadataAndHistory(t *testing.T) {
	f := newPubInstance(t, level(2), nil)
	f.seedFile(t, func(s *seed) {
		s.metadata = `{"imdb_id":"tt0133093","type":"movie","tmdbId":603}`
	})
	// A second file of the same release.
	second := "movies/Some.Movie.2020/extras.mkv"
	if err := f.content.Upsert(context.Background(), database.PeernetContentEntry{FilePath: second, FileID: strings.Repeat("c", 64), ReleaseID: pubRelease}); err != nil {
		t.Fatal(err)
	}
	f.meta.files[second] = &metapb.FileMetadata{FileSize: 500, ReleaseDate: time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC).Unix()}
	f.start(t)

	info, err := f.node(t).pipe.info.forRelease(context.Background(), pubRelease, pubPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Title != "Some.Movie.2020.1080p" || info.Category != "movies" || info.Indexer != "IndexerOne" {
		t.Fatalf("origin fields: %+v", info)
	}
	if info.FileCount != 2 || info.TotalSize != (1<<30)+500 {
		t.Fatalf("size/count: %+v", info)
	}
	if !info.PostDate.Equal(time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("post date: %v", info.PostDate)
	}
	if info.Media.IMDb != "tt0133093" || info.Media.TMDB != 603 {
		t.Fatalf("media: %+v", info.Media)
	}
}

func TestReleaseInfoSurvivesHistoryBeingCleared(t *testing.T) {
	f := newPubInstance(t, level(2), nil)
	f.seedFile(t, func(s *seed) { s.nzbName, s.category, s.indexer = "", "", "" }) // no history row at all
	f.start(t)
	info, err := f.node(t).pipe.info.forRelease(context.Background(), pubRelease, pubPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Title != "Some.Movie.2020.1080p" || info.Category != "" || info.Indexer != "" {
		t.Fatalf("title falls back to the stored NZB name; category and indexer stay unknown: %+v", info)
	}
	if info.Media.IMDb != "" {
		t.Fatalf("media ids must be absent: %+v", info.Media)
	}
}

// An NZB named after its media file ends up as the fallback title; the file
// extension is not part of a release title.
func TestReleaseInfoFallbackTitleDropsAMediaExtension(t *testing.T) {
	f := newPubInstance(t, level(2), nil)
	f.seedFile(t, func(s *seed) {
		s.nzbName, s.category, s.indexer = "", "", "" // no history row
		s.sourceNzb = "/nzbs/944-finding.you.2021.german.dl.2160p.web.x265-w4k.mkv.nzbz"
	})
	f.start(t)
	info, err := f.node(t).pipe.info.forRelease(context.Background(), pubRelease, pubPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Title != "finding.you.2021.german.dl.2160p.web.x265-w4k" {
		t.Fatalf("title = %q", info.Title)
	}
}

func TestReleaseInfoNeedsSomethingToPublish(t *testing.T) {
	f := newPubInstance(t, level(2), nil)
	f.seedFile(t)
	f.start(t)
	f.meta.mu.Lock()
	delete(f.meta.files, pubPath) // metadata gone
	f.meta.mu.Unlock()
	if _, err := f.node(t).pipe.info.forRelease(context.Background(), pubRelease, pubPath); err == nil {
		t.Fatal("a release with no readable file has no info")
	}
	if _, err := f.node(t).pipe.info.forRelease(context.Background(), strings.Repeat("d", 64), ""); err == nil {
		t.Fatal("an unknown release has no info")
	}
}

func TestReleaseInfoIsCached(t *testing.T) {
	f := newPubInstance(t, level(2), nil)
	f.seedFile(t)
	f.start(t)
	src := f.node(t).pipe.info
	a, err := src.forRelease(context.Background(), pubRelease, pubPath)
	if err != nil {
		t.Fatal(err)
	}
	f.meta.mu.Lock()
	delete(f.meta.files, pubPath)
	f.meta.mu.Unlock()
	b, err := src.forRelease(context.Background(), pubRelease, pubPath)
	if err != nil || a.Title != b.Title {
		t.Fatalf("second lookup within the TTL should be served from memory: %v %+v", err, b)
	}
}

func TestReleaseInfoIsAllOrNothing(t *testing.T) {
	f := newPubInstance(t, level(2), nil)
	f.seedFile(t)
	// A second indexed file whose metadata cannot be read: a partial listing
	// would publish an understated count and size.
	missing := "movies/Some.Movie.2020/gone.mkv"
	if err := f.content.Upsert(context.Background(), database.PeernetContentEntry{FilePath: missing, FileID: strings.Repeat("c", 64), ReleaseID: pubRelease}); err != nil {
		t.Fatal(err)
	}
	f.start(t)
	if info, err := f.node(t).pipe.info.forRelease(context.Background(), pubRelease, pubPath); err == nil {
		t.Fatalf("partial release info returned: %+v", info)
	}
}
