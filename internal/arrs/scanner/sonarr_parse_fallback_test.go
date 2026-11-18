package scanner

import (
	"testing"

	"github.com/kipsilabs/altmount/internal/config"
	"golift.io/starr/sonarr"
)

// The cases below follow the shapes seen on a library that symlinks into the
// mount: the release names the repair gave up on, and what Sonarr held for the
// episode. Names and ids are made up.

func TestReleaseNameForPath(t *testing.T) {
	cfg := &config.Config{MountPath: "/mnt/altmount"}
	cfg.SABnzbd.CompleteDir = "complete"
	cfg.SABnzbd.Categories = []config.SABnzbdCategory{{Name: "shows"}, {Name: "shows-DUTCH"}}

	cases := []struct{ path, want string }{
		// A file directly in a category folder: the file name is the release.
		{"/mnt/altmount/complete/shows-DUTCH/Some.Show.S02E04.1080p.WEB-DL.H.264-GRP.mkv",
			"Some.Show.S02E04.1080p.WEB-DL.H.264-GRP"},
		// A file in a release folder with an obfuscated name: the folder is the release.
		{"/mnt/altmount/complete/shows/Other.Show.s03e17.DVDRip.x264-GRP/6f315be508b82c911eef7438869aa6de.mkv",
			"Other.Show.s03e17.DVDRip.x264-GRP"},
		{"/mnt/altmount/complete/shows/Third.Show.1997.S02E09.2160p.WEB-DL.H265-GRP/Third.Show.1997.S02E09.mkv",
			"Third.Show.1997.S02E09.2160p.WEB-DL.H265-GRP"},
	}
	for _, c := range cases {
		if got := releaseNameForPath(cfg, c.path); got != c.want {
			t.Errorf("releaseNameForPath(%q) = %q, want %q", c.path, got, c.want)
		}
	}
	// Without config only the file name is trusted.
	if got := releaseNameForPath(nil, "/x/y/Rel.Name/file.mkv"); got != "file" {
		t.Errorf("nil config: got %q, want %q", got, "file")
	}
}

func TestDecideSonarrParseFallback(t *testing.T) {
	ep := func(id, series, file int64) *sonarr.Episode {
		return &sonarr.Episode{ID: id, SeriesID: series, EpisodeFileID: file, HasFile: file > 0}
	}
	file := func(id int64, scene string) *sonarr.EpisodeFile {
		return &sonarr.EpisodeFile{ID: id, SceneName: scene}
	}

	t.Run("Sonarr's file is this release: repair it", func(t *testing.T) {
		rel := "Some.Show.S02E04.1080p.WEB-DL.H.264-GRP"
		d := decideSonarrParseFallback(rel,
			[]*sonarr.Episode{ep(11, 7, 501)},
			[]*sonarr.EpisodeFile{file(500, "other"), file(501, "some.show.S02E04.1080p.WEB-DL.H.264-GRP")})
		if d.kind != parseFallbackCurrentFile || d.episodeFileID != 501 || d.seriesID != 7 {
			t.Fatalf("got %+v, want current file 501 of series 7", d)
		}
	})

	t.Run("Sonarr moved on to another release: leave it", func(t *testing.T) {
		d := decideSonarrParseFallback("Fourth.Show.S09E01.1080p.BluRay.x264-GRP",
			[]*sonarr.Episode{ep(21, 8, 601)},
			[]*sonarr.EpisodeFile{file(601, "Fourth.Show.S09E01.Episode.Title.1080p.BluRay.Remux.H.264-OTHER")})
		if d.kind != parseFallbackNone {
			t.Fatalf("got %+v, want none", d)
		}
	})

	t.Run("Sonarr's file has no scene name: leave it", func(t *testing.T) {
		d := decideSonarrParseFallback("Other.Show.s03e17.DVDRip.x264-GRP",
			[]*sonarr.Episode{ep(31, 9, 701)},
			[]*sonarr.EpisodeFile{file(701, "")})
		if d.kind != parseFallbackNone {
			t.Fatalf("got %+v, want none", d)
		}
	})

	t.Run("no file for the episodes: search them", func(t *testing.T) {
		d := decideSonarrParseFallback("Fifth.Show.S01E26E27.DUTCH.DVDRip.x264-GRP",
			[]*sonarr.Episode{ep(41, 10, 0), ep(42, 10, 0)}, nil)
		if d.kind != parseFallbackMissing || len(d.episodeIDs) != 2 || d.seriesID != 10 {
			t.Fatalf("got %+v, want missing with 2 episodes of series 10", d)
		}
	})

	t.Run("hasFile without a file id: leave it", func(t *testing.T) {
		odd := &sonarr.Episode{ID: 45, SeriesID: 10, EpisodeFileID: 0, HasFile: true}
		d := decideSonarrParseFallback("X.S01E01", []*sonarr.Episode{odd}, nil)
		if d.kind != parseFallbackNone {
			t.Fatalf("got %+v, want none", d)
		}
	})

	t.Run("episodes link to two files: leave it", func(t *testing.T) {
		d := decideSonarrParseFallback("X.S01E01E02",
			[]*sonarr.Episode{ep(51, 11, 801), ep(52, 11, 802)},
			[]*sonarr.EpisodeFile{file(801, "X.S01E01E02"), file(802, "X.S01E01E02")})
		if d.kind != parseFallbackNone {
			t.Fatalf("got %+v, want none", d)
		}
	})

	t.Run("one episode with a file and one without: repair the file only when it is this release", func(t *testing.T) {
		d := decideSonarrParseFallback("X.S01E01E02",
			[]*sonarr.Episode{ep(61, 12, 901), ep(62, 12, 0)},
			[]*sonarr.EpisodeFile{file(901, "X.S01E01E02")})
		if d.kind != parseFallbackCurrentFile || d.episodeFileID != 901 {
			t.Fatalf("got %+v, want current file 901", d)
		}
	})

	t.Run("episodes of two series: leave it", func(t *testing.T) {
		d := decideSonarrParseFallback("X", []*sonarr.Episode{ep(71, 13, 0), ep(72, 14, 0)}, nil)
		if d.kind != parseFallbackNone {
			t.Fatalf("got %+v, want none", d)
		}
	})

	t.Run("parse found nothing: leave it", func(t *testing.T) {
		if d := decideSonarrParseFallback("X", nil, nil); d.kind != parseFallbackNone {
			t.Fatalf("got %+v, want none", d)
		}
		if d := decideSonarrParseFallback("", []*sonarr.Episode{ep(1, 1, 0)}, nil); d.kind != parseFallbackNone {
			t.Fatalf("empty release: got %+v, want none", d)
		}
	})

	t.Run("file id not in the series' files: leave it", func(t *testing.T) {
		d := decideSonarrParseFallback("X", []*sonarr.Episode{ep(81, 15, 999)}, []*sonarr.EpisodeFile{file(1, "X")})
		if d.kind != parseFallbackNone {
			t.Fatalf("got %+v, want none", d)
		}
	})
}
