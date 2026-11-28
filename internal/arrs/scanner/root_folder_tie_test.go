package scanner

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kipsilabs/altmount/internal/arrs/clients"
	"github.com/kipsilabs/altmount/internal/arrs/data"
	"github.com/kipsilabs/altmount/internal/arrs/instances"
	"github.com/kipsilabs/altmount/internal/config"
)

// fakeLibrary is what a fake arr returns besides its root folder.
type fakeLibrary struct {
	movies       string            // body of /api/v3/movie
	series       string            // body of /api/v3/series
	episodeFiles map[string]string // body of /api/v3/episodeFile by seriesId
}

// fakeArrWith serves one root folder and the given library. Endpoints it does
// not know answer with an empty list.
func fakeArrWith(t *testing.T, root string, lib fakeLibrary) string {
	t.Helper()
	writeJSON := func(w http.ResponseWriter, body string) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}
	orEmpty := func(s string) string {
		if s == "" {
			return "[]"
		}
		return s
	}
	mux := http.NewServeMux()
	// The Sonarr client probes the version before it is handed out.
	mux.HandleFunc("/api/v3/system/status", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, `{"version":"4.0.0"}`)
	})
	mux.HandleFunc("/api/v3/rootFolder", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, `[{"id":1,"path":"`+root+`","accessible":true}]`)
	})
	mux.HandleFunc("/api/v3/movie", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, orEmpty(lib.movies))
	})
	mux.HandleFunc("/api/v3/series", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, orEmpty(lib.series))
	})
	mux.HandleFunc("/api/v3/episodeFile", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, orEmpty(lib.episodeFiles[r.URL.Query().Get("seriesId")]))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, "[]")
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv.URL
}

// downArr answers every request with a 500, like an arr that is not running.
func downArr(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func tieManager(cfg *config.Config) *Manager {
	getter := func() *config.Config { return cfg }
	return NewManager(getter, instances.NewManager(getter, nil), clients.NewManager(nil), data.NewManager(), nil, nil)
}

const (
	tieMovieRoot = "/media/movies"
	tieMovieFile = tieMovieRoot + "/Some Film (2001)/Some.Film.2001.mkv"
	tieTVRoot    = "/media/tv"
	tieTVFile    = tieTVRoot + "/Some Show/Season 01/Some.Show.S01E01.mkv"
)

var (
	tieMovieWithFile = `[{"id":1,"title":"Some Film","path":"` + tieMovieRoot + `/Some Film (2001)","hasFile":true,"movieFile":{"id":10,"path":"` + tieMovieFile + `"}}]`
	tieMovieNoFile   = `[{"id":1,"title":"Some Film","path":"` + tieMovieRoot + `/Some Film (2001)","hasFile":false}]`
	tieShowSeries    = `[{"id":5,"title":"Some Show","path":"` + tieTVRoot + `/Some Show"}]`
	tieShowEpisode   = map[string]string{"5": `[{"id":50,"seriesId":5,"path":"` + tieTVFile + `"}]`}
)

// Two Radarr instances share a root folder. The movie is in both libraries,
// but only the second one listed has the file. The tie goes to that one.
func TestFindInstanceForFilePath_SharedRootGoesToTheInstanceHoldingTheFile(t *testing.T) {
	enabled := true
	cfg := &config.Config{Arrs: config.ArrsConfig{RadarrInstances: []config.ArrsInstanceConfig{
		{Name: "radarr-a", Enabled: &enabled, APIKey: "k", URL: fakeArrWith(t, tieMovieRoot, fakeLibrary{movies: tieMovieNoFile})},
		{Name: "radarr-b", Enabled: &enabled, APIKey: "k", URL: fakeArrWith(t, tieMovieRoot, fakeLibrary{movies: tieMovieWithFile})},
	}}}

	gotType, gotName, err := tieManager(cfg).findInstanceForFilePath(context.Background(), tieMovieFile, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotType != "radarr" || gotName != "radarr-b" {
		t.Fatalf("got %s/%s, want radarr/radarr-b", gotType, gotName)
	}
}

// When none of the tied instances has the file, the lookup is refused
// instead of going to the first one listed.
func TestFindInstanceForFilePath_SharedRootWithoutAHolderIsRefused(t *testing.T) {
	enabled := true
	cfg := &config.Config{Arrs: config.ArrsConfig{RadarrInstances: []config.ArrsInstanceConfig{
		{Name: "radarr-a", Enabled: &enabled, APIKey: "k", URL: fakeArrWith(t, tieMovieRoot, fakeLibrary{movies: tieMovieNoFile})},
		{Name: "radarr-b", Enabled: &enabled, APIKey: "k", URL: fakeArrWith(t, tieMovieRoot, fakeLibrary{movies: tieMovieNoFile})},
	}}}

	_, _, err := tieManager(cfg).findInstanceForFilePath(context.Background(), tieMovieFile, "")
	if !errors.Is(err, errAmbiguousInstance) {
		t.Fatalf("err = %v, want errAmbiguousInstance", err)
	}
}

// Two Sonarr instances share a root folder and both have a series in the
// same folder, but only the second one has an episode file at this path.
// A series folder alone must not count as holding the file.
func TestFindInstanceForFilePath_SharedSonarrRootGoesToTheEpisodeFileOwner(t *testing.T) {
	enabled := true
	cfg := &config.Config{Arrs: config.ArrsConfig{SonarrInstances: []config.ArrsInstanceConfig{
		{Name: "sonarr-a", Enabled: &enabled, APIKey: "k", URL: fakeArrWith(t, tieTVRoot, fakeLibrary{series: tieShowSeries})},
		{Name: "sonarr-b", Enabled: &enabled, APIKey: "k", URL: fakeArrWith(t, tieTVRoot, fakeLibrary{series: tieShowSeries, episodeFiles: tieShowEpisode})},
	}}}

	_, gotName, err := tieManager(cfg).findInstanceForFilePath(context.Background(), tieTVFile, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotName != "sonarr-b" {
		t.Fatalf("got %s, want sonarr-b", gotName)
	}
}

// A tied instance whose library cannot be read might be the real holder, so
// the other instance must not win the tie by default.
func TestFindInstanceForFilePath_SharedRootWithAFailedLookupIsRefused(t *testing.T) {
	enabled := true

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/rootFolder", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"id":1,"path":"` + tieMovieRoot + `","accessible":true}]`))
	})
	mux.HandleFunc("/api/v3/movie", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	broken := httptest.NewServer(mux)
	t.Cleanup(broken.Close)

	cfg := &config.Config{Arrs: config.ArrsConfig{RadarrInstances: []config.ArrsInstanceConfig{
		{Name: "radarr-a", Enabled: &enabled, APIKey: "k", URL: fakeArrWith(t, tieMovieRoot, fakeLibrary{movies: tieMovieWithFile})},
		{Name: "radarr-b", Enabled: &enabled, APIKey: "k", URL: broken.URL},
	}}}

	_, _, err := tieManager(cfg).findInstanceForFilePath(context.Background(), tieMovieFile, "")
	if !errors.Is(err, errAmbiguousInstance) {
		t.Fatalf("err = %v, want errAmbiguousInstance", err)
	}
}

// A Sonarr instance that is down gets no client, so its root folders are
// unknown and it may share the root of the instance that answered. The file
// must not go to the instance that answered by default.
func TestFindInstanceForFilePath_UnreachableSonarrOfTheSameTypeIsRefused(t *testing.T) {
	enabled := true
	cfg := &config.Config{Arrs: config.ArrsConfig{SonarrInstances: []config.ArrsInstanceConfig{
		{Name: "sonarr-a", Enabled: &enabled, APIKey: "k", URL: fakeArrWith(t, tieTVRoot, fakeLibrary{series: tieShowSeries})},
		{Name: "sonarr-b", Enabled: &enabled, APIKey: "k", URL: downArr(t)},
	}}}

	_, gotName, err := tieManager(cfg).findInstanceForFilePath(context.Background(), tieTVFile, "")
	if !errors.Is(err, errAmbiguousInstance) {
		t.Fatalf("got %q, err = %v, want errAmbiguousInstance", gotName, err)
	}
}

// Same for a Radarr instance whose client exists but whose root folders
// cannot be read.
func TestFindInstanceForFilePath_RadarrWithUnreadableRootFoldersIsRefused(t *testing.T) {
	enabled := true
	cfg := &config.Config{Arrs: config.ArrsConfig{RadarrInstances: []config.ArrsInstanceConfig{
		{Name: "radarr-a", Enabled: &enabled, APIKey: "k", URL: fakeArrWith(t, tieMovieRoot, fakeLibrary{movies: tieMovieWithFile})},
		{Name: "radarr-b", Enabled: &enabled, APIKey: "k", URL: downArr(t)},
	}}}

	_, gotName, err := tieManager(cfg).findInstanceForFilePath(context.Background(), tieMovieFile, "")
	if !errors.Is(err, errAmbiguousInstance) {
		t.Fatalf("got %q, err = %v, want errAmbiguousInstance", gotName, err)
	}
}

// An instance of another type that is down does not block: with one Sonarr
// and one Radarr, a Sonarr file still goes to the Sonarr while the Radarr is
// unreachable.
func TestFindInstanceForFilePath_UnreachableInstanceOfAnotherTypeDoesNotBlock(t *testing.T) {
	enabled := true
	cfg := &config.Config{Arrs: config.ArrsConfig{
		SonarrInstances: []config.ArrsInstanceConfig{
			{Name: "sonarr", Enabled: &enabled, APIKey: "k", URL: fakeArrWith(t, tieTVRoot, fakeLibrary{series: tieShowSeries, episodeFiles: tieShowEpisode})},
		},
		RadarrInstances: []config.ArrsInstanceConfig{
			{Name: "radarr", Enabled: &enabled, APIKey: "k", URL: downArr(t)},
		},
	}}

	gotType, gotName, err := tieManager(cfg).findInstanceForFilePath(context.Background(), tieTVFile, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotType != "sonarr" || gotName != "sonarr" {
		t.Fatalf("got %s/%s, want sonarr/sonarr", gotType, gotName)
	}
}

// The relative-path fallback runs when no root folder matches. A Sonarr that
// is down must not let another Sonarr win there either.
func TestFindInstanceForFilePath_RelativePathWithUnreachableSonarrIsRefused(t *testing.T) {
	enabled := true
	file := "/elsewhere/Some Show/Season 01/Some.Show.S01E01.mkv"
	cfg := &config.Config{Arrs: config.ArrsConfig{SonarrInstances: []config.ArrsInstanceConfig{
		{Name: "sonarr-a", Enabled: &enabled, APIKey: "k", URL: downArr(t)},
		{Name: "sonarr-b", Enabled: &enabled, APIKey: "k", URL: fakeArrWith(t, tieTVRoot, fakeLibrary{series: tieShowSeries})},
	}}}

	_, gotName, err := tieManager(cfg).findInstanceForFilePath(context.Background(), file, "Some Show/Season 01/Some.Show.S01E01.mkv")
	if !errors.Is(err, errAmbiguousInstance) {
		t.Fatalf("got %q, err = %v, want errAmbiguousInstance", gotName, err)
	}
}

// With every instance readable, the relative-path fallback still finds the
// series owner.
func TestFindInstanceForFilePath_RelativePathStillResolves(t *testing.T) {
	enabled := true
	file := "/elsewhere/Some Show/Season 01/Some.Show.S01E01.mkv"
	cfg := &config.Config{Arrs: config.ArrsConfig{SonarrInstances: []config.ArrsInstanceConfig{
		{Name: "sonarr-a", Enabled: &enabled, APIKey: "k", URL: fakeArrWith(t, tieTVRoot, fakeLibrary{})},
		{Name: "sonarr-b", Enabled: &enabled, APIKey: "k", URL: fakeArrWith(t, tieTVRoot, fakeLibrary{series: tieShowSeries})},
	}}}

	_, gotName, err := tieManager(cfg).findInstanceForFilePath(context.Background(), file, "Some Show/Season 01/Some.Show.S01E01.mkv")
	if err != nil || gotName != "sonarr-b" {
		t.Fatalf("got %q, err = %v, want sonarr-b", gotName, err)
	}
}

// A relative path that matches the libraries of two instances is refused
// instead of going to the first one listed.
func TestFindInstanceForFilePath_RelativePathMatchingTwoInstancesIsRefused(t *testing.T) {
	enabled := true
	file := "/elsewhere/Some Show/Season 01/Some.Show.S01E01.mkv"
	cfg := &config.Config{Arrs: config.ArrsConfig{SonarrInstances: []config.ArrsInstanceConfig{
		{Name: "sonarr-a", Enabled: &enabled, APIKey: "k", URL: fakeArrWith(t, tieTVRoot, fakeLibrary{series: tieShowSeries})},
		{Name: "sonarr-b", Enabled: &enabled, APIKey: "k", URL: fakeArrWith(t, tieTVRoot, fakeLibrary{series: tieShowSeries})},
	}}}

	_, gotName, err := tieManager(cfg).findInstanceForFilePath(context.Background(), file, "Some Show/Season 01/Some.Show.S01E01.mkv")
	if !errors.Is(err, errAmbiguousInstance) {
		t.Fatalf("got %q, err = %v, want errAmbiguousInstance", gotName, err)
	}
}
