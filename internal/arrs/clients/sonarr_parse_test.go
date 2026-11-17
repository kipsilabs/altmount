package clients

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"golift.io/starr"
	"golift.io/starr/sonarr"
)

// The parse fallback in the scanner reads the episodes a release name maps to:
// their id, seriesId, episodeFileId and hasFile. Both adapters must return
// those the same way, from each version's own response shape.

// sonarrV3ParseResponse follows ParseResource in src/Sonarr.Api.V3/openapi.json:
// parsedEpisodeInfo.seasonNumber is a single number.
const sonarrV3ParseResponse = `{
  "title": "Some.Show.S02E04E05.1080p.WEB-DL.H.264-GRP",
  "parsedEpisodeInfo": {"releaseTitle": "Some.Show.S02E04E05.1080p.WEB-DL.H.264-GRP", "seriesTitle": "Some Show",
    "seasonNumber": 2, "episodeNumbers": [4, 5], "languages": [{"id": 1, "name": "English"}],
    "quality": {"quality": {"id": 3, "name": "WEBDL-1080p", "source": "web", "resolution": 1080}, "revision": {"version": 1, "real": 0, "isRepack": false}},
    "releaseGroup": "GRP", "releaseType": "multiEpisode"},
  "series": {"id": 7, "title": "Some Show", "path": "/tv/Some Show"},
  "episodes": [
    {"id": 11, "seriesId": 7, "tvdbId": 1001, "episodeFileId": 501, "seasonNumber": 2, "episodeNumber": 4, "hasFile": true, "monitored": true},
    {"id": 12, "seriesId": 7, "tvdbId": 1002, "episodeFileId": 0, "seasonNumber": 2, "episodeNumber": 5, "hasFile": false, "monitored": true}
  ],
  "languages": [{"id": 1, "name": "English"}],
  "customFormats": [],
  "customFormatScore": 0
}`

// sonarrV5ParseResponse follows ParseResource in src/Sonarr.Api.V5/openapi.json
// (develop, and the revision pinned in testdata/sonarr): parsedEpisodeInfo has
// seasonNumbers as a list and new fields such as isSeasonTitle, and the
// episodes are v5 EpisodeResources.
const sonarrV5ParseResponse = `{
  "id": 0,
  "title": "Some.Show.S02E04E05.1080p.WEB-DL.H.264-GRP",
  "parsedEpisodeInfo": {"releaseTitle": "Some.Show.S02E04E05.1080p.WEB-DL.H.264-GRP", "seriesTitle": "Some Show",
    "seriesTitleInfo": {"title": "Some Show", "titleWithoutYear": "Some Show", "year": 0, "allTitles": ["Some Show"]},
    "quality": {"quality": {"id": 3, "name": "WEBDL-1080p", "source": "web", "resolution": 1080}, "revision": {"version": 1, "real": 0, "isRepack": false}},
    "seasonNumbers": [2], "episodeNumbers": [4, 5], "absoluteEpisodeNumbers": [], "specialAbsoluteEpisodeNumbers": [],
    "airDate": null, "languages": [{"id": 1, "name": "English"}], "fullSeason": false, "isPartialSeason": false,
    "isMultiSeason": false, "isSeasonExtra": false, "isSeasonTitle": false, "isSplitEpisode": false, "isMiniSeries": false,
    "special": false, "releaseGroup": "GRP", "releaseHash": "", "seasonPart": 0, "releaseTokens": ".1080p.WEB-DL.H.264-GRP",
    "dailyPart": null, "isDaily": false, "isAbsoluteNumbering": false, "isPossibleSpecialEpisode": false,
    "isPossibleSceneSeasonSpecial": false, "releaseType": "multiEpisode"},
  "series": {"id": 7, "title": "Some Show", "path": "/tv/Some Show", "seasonTypes": [], "folders": []},
  "episodes": [
    {"id": 11, "seriesId": 7, "tvdbId": 1001, "episodeFileId": 501, "seasonNumber": 2, "episodeNumber": 4,
      "title": "Four", "finaleType": null, "hasFile": true, "monitored": true, "unverifiedSceneNumbering": false},
    {"id": 12, "seriesId": 7, "tvdbId": 1002, "episodeFileId": 0, "seasonNumber": 2, "episodeNumber": 5,
      "title": "Five", "finaleType": null, "hasFile": false, "monitored": true, "unverifiedSceneNumbering": false}
  ],
  "languages": [{"id": 1, "name": "English"}],
  "customFormats": [],
  "customFormatScore": 0
}`

// Sonarr answers a title it cannot map to a series with the title alone.
const sonarrUnknownParseResponse = `{"title": "Unknown.Show.S01E01-GRP"}`

func TestSonarrParseAcrossAdapters(t *testing.T) {
	ctx := context.Background()
	adapters := []struct {
		name, prefix, known string
		newClient           func(*starr.Config) SonarrClient
	}{
		{name: "legacy v3", prefix: "/api/v3", known: sonarrV3ParseResponse, newClient: NewLegacySonarr},
		{name: "native v5", prefix: "/api/v5", known: sonarrV5ParseResponse, newClient: newNativeSonarr},
	}
	for _, a := range adapters {
		t.Run(a.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, http.MethodGet, r.Method)
				require.Equal(t, a.prefix+"/parse", r.URL.Path)
				require.Equal(t, "key", r.Header.Get("X-Api-Key"))
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Query().Get("title") == "Unknown.Show.S01E01-GRP" {
					_, _ = w.Write([]byte(sonarrUnknownParseResponse))
					return
				}
				require.Equal(t, "Some.Show.S02E04E05.1080p.WEB-DL.H.264-GRP", r.URL.Query().Get("title"))
				_, _ = w.Write([]byte(a.known))
			}))
			defer srv.Close()
			c := a.newClient(&starr.Config{URL: srv.URL, APIKey: "key", Client: srv.Client()})

			out, err := c.ParseContext(ctx, &sonarr.ParseInput{Title: "Some.Show.S02E04E05.1080p.WEB-DL.H.264-GRP"})
			require.NoError(t, err)
			require.NotNil(t, out)
			require.Equal(t, "Some.Show.S02E04E05.1080p.WEB-DL.H.264-GRP", out.Title)
			require.Len(t, out.Episodes, 2)
			require.Equal(t, int64(11), out.Episodes[0].ID)
			require.Equal(t, int64(7), out.Episodes[0].SeriesID)
			require.Equal(t, int64(501), out.Episodes[0].EpisodeFileID)
			require.True(t, out.Episodes[0].HasFile)
			require.Equal(t, int64(12), out.Episodes[1].ID)
			require.Equal(t, int64(7), out.Episodes[1].SeriesID)
			require.Zero(t, out.Episodes[1].EpisodeFileID)
			require.False(t, out.Episodes[1].HasFile)

			unknown, err := c.ParseContext(ctx, &sonarr.ParseInput{Title: "Unknown.Show.S01E01-GRP"})
			require.NoError(t, err)
			require.NotNil(t, unknown)
			require.Empty(t, unknown.Episodes)
		})
	}
}

func TestNativeSonarrParseRequest(t *testing.T) {
	ctx := context.Background()
	called := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called++
		require.Equal(t, "/api/v5/parse", r.URL.Path)
		// A path-only parse sends no empty title, and the other way round.
		_, hasTitle := r.URL.Query()["title"]
		require.False(t, hasTitle)
		require.Equal(t, "/downloads/Some.Show.S02E04-GRP/file.mkv", r.URL.Query().Get("path"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"title":"file"}`))
	}))
	defer srv.Close()
	c := newNativeSonarr(&starr.Config{URL: srv.URL, APIKey: "key", Client: srv.Client()})

	_, err := c.ParseContext(ctx, &sonarr.ParseInput{Path: "/downloads/Some.Show.S02E04-GRP/file.mkv"})
	require.NoError(t, err)
	require.Equal(t, 1, called)

	// Nothing to parse: refuse without a request rather than ask Sonarr about "".
	_, err = c.ParseContext(ctx, &sonarr.ParseInput{Title: "  "})
	require.ErrorIs(t, err, starr.ErrRequestError)
	_, err = c.ParseContext(ctx, nil)
	require.ErrorIs(t, err, starr.ErrRequestError)
	require.Equal(t, 1, called)

	// An HTTP error is returned as an error, so the scanner keeps its old behaviour.
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer failing.Close()
	fc := newNativeSonarr(&starr.Config{URL: failing.URL, APIKey: "key", Client: failing.Client()})
	_, err = fc.ParseContext(ctx, &sonarr.ParseInput{Title: "Some.Show.S02E04-GRP"})
	require.Error(t, err)
}
