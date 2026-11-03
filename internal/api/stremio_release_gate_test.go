package api

import (
	"context"
	"testing"
)

// Real release names captured from a live addon query for
// tt5537002 (Killers of the Flower Moon), plus the canonical metadata
// Cinemeta resolves for that IMDb ID.
const kotfmCanonicalTitle = "Killers of the Flower Moon"
const kotfmCanonicalYear = "2023"

func TestReleaseMatchesContent_KotFMLive(t *testing.T) {
	tests := []struct {
		name   string
		title  string
		want   bool
		reason string
	}{
		{
			name:  "library remux (CiNEPHiLES)",
			title: "Killers of the Flower Moon (2023) - [Remux-2160p][TrueHD Atmos 7.1][DV HDR10][HEVC]-CiNEPHiLES",
			want:  true,
		},
		{
			name:  "standard PROPER BluRay encode",
			title: "Killers.of.the.Flower.Moon.2023.PROPER.BluRay.1080p.TrueHD.Atmos.7.1.AVC.REPACK-iRO",
			want:  true,
		},
		{
			name:  "bilingual Turkish release carrying full English title",
			title: "Killers.of.the.Flower.Moon.2023.Dolunay.Katilleri.ATVP.WEB-DL.1080p.H.264.DDP5.1-TURG",
			want:  true,
		},
		{
			name:  "unusual tag-first ordering rescued by token coverage",
			title: "REMUX.2160p.Killers.of.the.Flower.Moon.TrueHD.Atmos.7.1.HEVC.DV-CiNEPHiLES",
			want:  true,
		},
		{
			name:   "wrong movie entirely (althub indexer mislabel)",
			title:  "American.Dilemma.2023.1080p.WEB.h264-BAE",
			want:   false,
			reason: "zero title overlap",
		},
		{
			name:   "typo'd title missing 'Flower'",
			title:  "Killers.Of.The.Moon.2023.1080p.BluRay.REMUX.AVC.TrueHD.7.1-UnKn0wn",
			want:   false,
			reason: "coverage 2/3 below floor and clean-title inequality",
		},
		{
			name:   "extras pack",
			title:  "Killers.Of.The.Flower.Moon.2023.EXTRAS.COMPLETE.BLURAY-BAKED",
			want:   false,
			reason: "junk pattern",
		},
		{
			name:   "full disc package",
			title:  "Killers.of.the.Flower.Moon.2023.COMPLETE.BLURAY-UNTOUCHED",
			want:   false,
			reason: "junk pattern",
		},
		{
			name:   "clearly conflicting year",
			title:  "Killers.of.the.Flower.Moon.2019.1080p.BluRay.x264-FAKE",
			want:   false,
			reason: "year off by more than one",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := releaseMatchesContent("movie", tt.title, kotfmCanonicalTitle, kotfmCanonicalYear, 0, 0)
			if got != tt.want {
				t.Errorf("releaseMatchesContent(%q) = %v, want %v (%s)", tt.title, got, tt.want, tt.reason)
			}
		})
	}
}

func TestIsJunkRelease(t *testing.T) {
	junk := []string{
		"Show.2023.EXTRAS.COMPLETE.BLURAY-BAKED",
		"Movie.2023.COMPLETE.BLURAY-UNTOUCHED",
		"Movie.2023.PROPER.COMPLETE.BLURAY-BAKED",
		"Movie.2023.Sample.a.b.c",
		"EXTRAS-Movie.2023",
	}
	for _, name := range junk {
		if !isJunkRelease(name) {
			t.Errorf("isJunkRelease(%q) = false, want true", name)
		}
	}
	clean := []string{
		"The.Complete.Series.2020.S01.1080p", // "complete" alone without bluray is fine
		"Sampling.The.Past.2021.1080p.WEB-DL",
	}
	for _, name := range clean {
		if isJunkRelease(name) {
			t.Errorf("isJunkRelease(%q) = true, want false", name)
		}
	}
}

func TestReleaseMatchesContent_FailOpenWithoutMetadata(t *testing.T) {
	if !releaseMatchesContent("movie", "Anything.At.All.2023.1080p", "", "", 0, 0) {
		t.Fatal("gate must fail open when canonical metadata is unavailable")
	}
}

func TestReleaseMatchesContentRejectsConflicts(t *testing.T) {
	for _, tc := range []struct {
		name, kind, release, title, year string
		season, episode                  int
		want                             bool
	}{
		{"unrelated spinoff", "series", "Fear.The.Walking.Dead.S01E01.1080p", "The Walking Dead", "", 1, 1, false},
		{"missing series title", "series", "S02E01.1080p", "Black Clover", "", 2, 1, false},
		{"missing movie title", "movie", "2023.1080p", kotfmCanonicalTitle, kotfmCanonicalYear, 0, 0, false},
		{"numeric movie title", "movie", "1917.2019.1080p", "1917", "2019", 0, 0, true},
		{"numeric movie title without year", "movie", "2012.1080p", "2012", "2009", 0, 0, true},
		{"numeric series title", "series", "1923.S01E01.1080p", "1923", "", 1, 1, true},
		{"different numeric title", "movie", "1917.2019.1080p", "2012", "2009", 0, 0, false},
		{"plain movie filename", "movie", "Alien.mkv", "Alien", "1979", 0, 0, true},
		{"plain nzb filename", "movie", "Alien.nzb.gz", "Alien", "1979", 0, 0, true},
		{"different plain movie filename", "movie", "Alien.Covenant.mkv", "Alien", "1979", 0, 0, false},
		{"wrong episode", "series", "Black.Clover.S02E02.1080p", "Black Clover", "", 2, 1, false},
		{"wrong season", "series", "Black.Clover.S03E01.1080p", "Black Clover", "", 2, 1, false},
		{"matching multi season pack", "series", "Black.Clover.S01-S03.1080p", "Black Clover", "", 2, 19, true},
		{"outside multi season pack", "series", "Black.Clover.S01-S03.1080p", "Black Clover", "", 4, 1, false},
		{"partial word is not alias", "series", "Dark.S01E01.1080p", "The Ark", "", 1, 1, false},
		{"wrong season pack", "series", "Black.Clover.S03.1080p", "Black Clover", "", 2, 1, false},
		{"different show sharing title", "series", "ARK.The.Animated.Series.S01E01.1080p", "The Ark", "", 1, 1, false},
		{"different movie sharing title", "movie", "Alien.Covenant.2017.1080p", "Alien", "", 0, 0, false},
		{"matching episode", "series", "Black.Clover.S02E01.1080p", "Black Clover", "", 2, 1, true},
		{"matching season pack", "series", "Black.Clover.S02.1080p", "Black Clover", "", 2, 19, true},
		{"matching episode range", "series", "Black.Clover.S02E01-E03.1080p", "Black Clover", "", 2, 2, true},
		{"outside episode range", "series", "Black.Clover.S02E01-E03.1080p", "Black Clover", "", 2, 4, false},
		{"tagged wrong episode", "series", "[Group] Black.Clover.S02E02.1080p", "Black Clover", "", 2, 1, false},
		{"tagged correct episode", "series", "[Group] Black.Clover.S02E01.1080p", "Black Clover", "", 2, 1, true},
		{"tagged conflicting year", "movie", "REMUX.2160p.Killers.of.the.Flower.Moon.2019.1080p", kotfmCanonicalTitle, kotfmCanonicalYear, 0, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := releaseMatchesContent(tc.kind, tc.release, tc.title, tc.year, tc.season, tc.episode); got != tc.want {
				t.Fatalf("releaseMatchesContent(%q) = %v, want %v", tc.release, got, tc.want)
			}
		})
	}
}

func TestReleaseMatchesContentAnimeAliasesAndAbsolute(t *testing.T) {
	romaji := "Meitantei Conan"
	aliases := []string{"Detective Conan", "Case Closed"}
	ctx := releaseMatchContext{aliases: aliases, isAnime: true}
	absCtx := releaseMatchContext{
		aliases:     aliases,
		isAnime:     true,
		episodeMeta: seriesEpisodeMeta{absolute: map[int]map[int]int{1: {1: 1, 670: 670}, 5: {23: 523}, 99: {1210: 1210}}},
	}

	cases := []struct {
		name    string
		release string
		season  int
		episode int
		ctx     releaseMatchContext
		want    bool
	}{
		{"fansub absolute matches catalog absolute", "[SubsPlease] Detective Conan - 1210 (1080p)", 99, 1210, absCtx, true},
		{"erai-raws absolute matches catalog absolute", "[Erai-raws] Detective Conan - 1210 [1080p CR WEB-DL AVC AAC]", 99, 1210, absCtx, true},
		{"toonshub EP prefix matches catalog absolute", "Detective.Conan.EP1210.Episode.1210.1080p.NF.WEB-DL.JPN.AAC2.0.H.264.MSubs-ToonsHub", 99, 1210, absCtx, true},
		{"english alias title match via standard path", "Detective.Conan.S01E01.The.Roller.Coaster.Murder.Case.1080p.CR.WEB-DL.VARYG", 1, 1, ctx, true},
		{"legacy baseline without aliases or anime flag rejects", "[SubsPlease] Detective Conan - 1210 (1080p)", 99, 1210, releaseMatchContext{}, false},
		{"alias matches absolute episode", "[SubsPlease] Detective Conan - 1210 (1080p)", 99, 1210, ctx, true},
		{"wrong absolute episode", "[SubsPlease] Detective Conan - 1211 (1080p)", 99, 1210, absCtx, false},
		{"relative episode is not franchise absolute", "[SubsPlease] Detective Conan - 23 (1080p)", 5, 23, absCtx, false},
		{"EP prefix wrong absolute", "Detective.Conan.EP1211.Episode.1211.1080p", 99, 1210, absCtx, false},
		{"unrelated series sharing S/E rejected", "One.Piece.S01E01.1080p.WEB-DL.GROUP", 1, 1, absCtx, false},
		{"ARK vs The Ark regression guarded", "ARK.The.Animated.Series.S01E01.1080p.WEB-DL.DDP5.1.H.264-GROUP", 1, 1, releaseMatchContext{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := releaseMatchesContent("series", tc.release, romaji, "", tc.season, tc.episode, tc.ctx); got != tc.want {
				t.Fatalf("releaseMatchesContent(%q) = %v, want %v", tc.release, got, tc.want)
			}
		})
	}
}

func TestResolveSeriesTitleAliasesCacheNilSafe(t *testing.T) {
	if resolveSeriesTitleAliases(context.Background(), "", "test-agent") != nil {
		t.Fatal("empty imdb must yield nil aliases")
	}
}

func TestReleaseMatchesContentKnownCreatorAlias(t *testing.T) {
	ctx := releaseMatchContext{aliases: []string{"I Will Find You", "Harlan Cobens I Will Find You"}}
	for _, name := range []string{"I.Will.Find.You.S01E01.1080p", "Harlan.Cobens.I.Will.Find.You.S01E01.1080p"} {
		if !releaseMatchesContent("series", name, "Harlan Coben's I Will Find You", "", 1, 1, ctx) {
			t.Fatalf("known alias rejected: %s", name)
		}
	}
}
