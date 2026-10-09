package prowlarr

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMatchKeywordOrPattern_TS_NoFalsePositiveOnDTS(t *testing.T) {
	releaseTitle := "Infidelity.in.Suburbia.2017.1080p.BluRay.REMUX.AVC.DTS-HD.MA.5.1-EPSiLON"

	// "TS" should not match DTS or DTS-HD
	assert.False(t, MatchKeywordOrPattern(releaseTitle, "TS"), "TS keyword should not match DTS-HD")
	assert.False(t, MatchKeywordOrPattern(releaseTitle, "CAM"), "CAM keyword should not match unrelated title")
	assert.False(t, MatchesExcludeKeywords(releaseTitle, []string{"CAM", "TeleSync", "TS", ".sample", "sample", "Korean Dub", "HDCAM", "HDTS"}),
		"DTS-HD REMUX release should not be blacklisted by default exclude keywords")
}

func TestMatchKeywordOrPattern_LegitimateMatches(t *testing.T) {
	tests := []struct {
		title   string
		keyword string
		want    bool
	}{
		// TS / Telesync tests
		{"Movie.2024.TS.1080p.x264", "TS", true},
		{"Movie_2024_TS_1080p", "TS", true},
		{"Movie-2024-TS-1080p", "TS", true},
		{"Movie 2024 [TS] 1080p", "TS", true},
		{"TS.Movie.2024", "TS", true},
		{"Movie.2024.TS", "TS", true},
		{"Movie.2024.TELESYNC.1080p", "TeleSync", true},
		{"Movie.2024.HDTS.1080p", "HDTS", true},

		// Words containing 'ts' substring - must NOT match 'TS' keyword
		{"Infidelity.in.Suburbia.2017.1080p.BluRay.REMUX.AVC.DTS-HD.MA.5.1-EPSiLON", "TS", false},
		{"Streets.Of.Fire.1984.1080p.BluRay.DTS-HD.MA.5.1", "TS", false},
		{"Knights.Of.The.Zodiac.2023.1080p.BluRay.x264", "TS", false},
		{"Cats.2019.1080p.BluRay", "TS", false},
		{"Drafts.2020.1080p.WEB-DL", "TS", false},
		{"The.Ghosts.Of.Monday.2022.1080p", "TS", false},
		{"Two.Tickets.To.Greece.2022.1080p", "TS", false},

		// CAM tests
		{"Movie.2024.CAM.1080p", "CAM", true},
		{"Movie.2024.HDCAM.1080p", "HDCAM", true},
		{"Became.Human.2020.1080p", "CAM", false},
		{"Scam.1992.S01.1080p", "CAM", false},
		{"Camille.2008.1080p", "CAM", false},
		{"Dreamcatcher.2003.1080p", "CAM", false},

		// Multi-word phrase tests
		{"Squid.Game.S01.Korean.Dub.1080p", "Korean Dub", true},
		{"Squid.Game.S01.Korean Dub.1080p", "Korean Dub", true},
		{"Squid.Game.S01.Korean-Dub.1080p", "Korean Dub", true},
		{"Squid.Game.S01.Korean_Dub.1080p", "Korean Dub", true},

		// Sample tests
		{"Movie.2024.1080p.sample.mkv", "sample", true},
		{"Movie.2024.1080p.sample.mkv", ".sample", true},
		{"Movie.2024.1080p-sample.mkv", "sample", true},

		// Explicit regex patterns
		{"Movie.2024.TS.1080p", `\b(cam|ts)\b`, true},
		{"Infidelity.in.Suburbia.2017.1080p.BluRay.REMUX.AVC.DTS-HD.MA.5.1-EPSiLON", `\b(cam|ts)\b`, false},

		// Slash-delimited regex with flags
		{"Movie.2024.CAM.1080p", `/cam/i`, true},
		{"Movie.2024.CAM.1080p", `/^movie/i`, true},
		{"Movie.2024.CAM.1080p", `/^cam/`, false},
		{"Movie.2024.CAM.1080p", `/cam/m`, true},

		// Invalid slash-delimited regex must fail closed (no literal fallback)
		{"Movie.2024.[unclosed.1080p", `/[unclosed/`, false},
	}

	for _, tt := range tests {
		t.Run(tt.title+"_"+tt.keyword, func(t *testing.T) {
			got := MatchKeywordOrPattern(tt.title, tt.keyword)
			assert.Equal(t, tt.want, got, "MatchKeywordOrPattern(%q, %q)", tt.title, tt.keyword)
		})
	}
}

// TestMatchKeywordOrPattern_BracketedKeywordsAreLiteral pins that release-name
// punctuation in a plain keyword is treated literally. Release groups and years
// routinely appear as "[Group]" or "(2024)"; compiling those as regexes turns
// "[SubsPlease]" into a character class that matches almost every title, which
// would silently blacklist a user's entire result set.
func TestMatchKeywordOrPattern_BracketedKeywordsAreLiteral(t *testing.T) {
	tests := []struct {
		name    string
		title   string
		keyword string
		want    bool
	}{
		{"bracket group matches its own release", "[SubsPlease] Show - 01 (1080p).mkv", "[SubsPlease]", true},
		{"bracket group does not match other releases", "Movie.2024.1080p.BluRay.x264-GROUP", "[SubsPlease]", false},
		{"bracket group is not a character class", "A.Plain.Title.Ends.With.S.2024", "[SubsPlease]", false},
		{"parenthesised year matches literally", "Show (2020) - 35 (1080p).mkv", "(2020)", true},
		{"parenthesised year does not match other years", "Show (2019) - 35 (1080p).mkv", "(2020)", false},
		{"braces are literal", "Movie.{Extended}.2024", "{Extended}", true},

		// Unambiguous regex directives must still be honoured.
		{"alternation is still regex", "Movie.2024.TS.1080p", "(cam|ts)", true},
		{"alternation still excludes non-matches", "Movie.2024.WEB.1080p", "(cam|ts)", false},
		{"word boundary is still regex", "Movie.2024.TS.1080p", `\bts\b`, true},
		{"anchor is still regex", "Movie.2024.TS.1080p", "^Movie", true},
		{"anchor still excludes non-matches", "Not.Movie.2024", "^Movie", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MatchKeywordOrPattern(tt.title, tt.keyword)
			assert.Equal(t, tt.want, got, "MatchKeywordOrPattern(%q, %q)", tt.title, tt.keyword)
		})
	}
}

// TestSlashPatternFlagSemantics documents the flag contract shared with the
// frontend preview (frontend/src/components/config/stremio/scoringPresets.ts,
// matchKeywordOrPattern). Only Go's inline flags i/m/s are honoured, matching is
// always case-insensitive, and unsupported flag letters are ignored rather than
// making the pattern fail.
func TestSlashPatternFlagSemantics(t *testing.T) {
	tests := []struct {
		name    string
		title   string
		pattern string
		want    bool
	}{
		{"no flags is case-insensitive", "Movie.2024.CAM", "/cam/", true},
		{"explicit i flag", "Movie.2024.CAM", "/cam/i", true},
		{"s flag honoured", "Movie.2024.CAM", "/movie.*cam/s", true},
		{"unsupported g flag ignored, still matches", "Movie.2024.CAM", "/cam/g", true},
		{"unsupported u flag ignored, still matches", "Movie.2024.CAM", "/cam/u", true},
		{"unsupported x flag ignored, still matches", "Movie.2024.CAM", "/cam/x", true},
		{"unsupported flags do not force a match", "Movie.2024.WEB", "/cam/gux", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MatchKeywordOrPattern(tt.title, tt.pattern)
			assert.Equal(t, tt.want, got, "MatchKeywordOrPattern(%q, %q)", tt.title, tt.pattern)
		})
	}
}

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestDownloadNZB_ProwlarrRedirect(t *testing.T) {
	const prowlarrHost = "http://prowlarr.local:9696"
	const prowlarrAPIKey = "prowlarr-secret-key"
	const directIndexerURL = "https://indexer.example.com/api?t=get&id=123&apikey=indexerkey"
	const nzbContent = "<nzb>test content</nzb>"

	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.String() {
		case prowlarrHost + "/1/api?t=get&id=123":
			// Must include Prowlarr API key
			if req.Header.Get("X-Api-Key") != prowlarrAPIKey {
				return &http.Response{
					StatusCode: http.StatusUnauthorized,
					Body:       io.NopCloser(strings.NewReader("unauthorized")),
				}, nil
			}
			// Respond with redirect to indexer
			header := make(http.Header)
			header.Set("Location", directIndexerURL)
			return &http.Response{
				StatusCode: http.StatusFound,
				Header:     header,
				Body:       io.NopCloser(strings.NewReader("")),
			}, nil

		case directIndexerURL:
			// Must NOT include Prowlarr API key on cross-host request
			if req.Header.Get("X-Api-Key") != "" {
				return &http.Response{
					StatusCode: http.StatusBadRequest,
					Body:       io.NopCloser(strings.NewReader("prowlarr key leaked")),
				}, nil
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(nzbContent)),
			}, nil

		default:
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Body:       io.NopCloser(strings.NewReader("not found")),
			}, nil
		}
	})

	httpClient := &http.Client{Transport: transport}
	client := NewClient(prowlarrHost, prowlarrAPIKey, httpClient)

	t.Run("prowlarr redirect to indexer succeeds", func(t *testing.T) {
		data, err := client.DownloadNZB(context.Background(), prowlarrHost+"/1/api?t=get&id=123")
		assert.NoError(t, err)
		assert.Equal(t, nzbContent, string(data))
	})

	t.Run("direct indexer url succeeds without prowlarr key", func(t *testing.T) {
		data, err := client.DownloadNZB(context.Background(), directIndexerURL)
		assert.NoError(t, err)
		assert.Equal(t, nzbContent, string(data))
	})

	t.Run("direct loopback url is rejected as SSRF", func(t *testing.T) {
		_, err := client.DownloadNZB(context.Background(), "http://127.0.0.1:8080/secret.nzb")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "private address")
	})
}

func TestInferReleaseMetaOverlappingTitleMetadata(t *testing.T) {
	var meta ReleaseMeta
	require.NotPanics(t, func() {
		meta = InferReleaseMeta("[ABC 1080p] Black Clover S02E01 Japanese")
	})
	require.Equal(t, "Japanese", meta.Language)
	require.Equal(t, "🇯🇵", meta.FlagEmoji)
	require.Equal(t, "Jpn", meta.LangCode)
}

func TestInferReleaseMetaNormalTitle(t *testing.T) {
	meta := InferReleaseMeta("Black.Clover.S02E01.1080p.WEB-DL.x264.AAC.Japanese")
	require.Equal(t, "Black Clover", meta.ParsedTitle)
	require.Equal(t, "1080p", meta.Resolution)
	require.Equal(t, "FHD", meta.QualityLabel)
	require.Equal(t, "WEB-DL", meta.Quality)
	require.Equal(t, "x264", meta.Codec)
	require.Equal(t, "AAC", meta.Audio)
	require.Equal(t, "Japanese", meta.Language)
}
