package scanner

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/kipsilabs/altmount/internal/arrs/clients"
	"github.com/kipsilabs/altmount/internal/arrs/model"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/stretchr/testify/require"
	"golift.io/starr"
	"golift.io/starr/radarr"
)

func categoryRootTestManager() *Manager {
	cfg := &config.Config{
		MountPath: "/mnt/altmount",
		SABnzbd: config.SABnzbdConfig{
			CompleteDir: "/complete",
			Categories:  []config.SABnzbdCategory{{Name: "movies"}, {Name: "tv"}},
		},
	}
	return NewManager(func() *config.Config { return cfg }, nil, nil, nil, nil, nil)
}

// radarrQueueServer serves one Radarr queue record and records queue deletes.
func radarrQueueServer(t *testing.T, outputPath string) (*radarr.Radarr, func() []string) {
	t.Helper()
	var (
		mu      sync.Mutex
		deletes []string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v3/queue" && r.Method == http.MethodGet:
			_ = json.NewEncoder(w).Encode(radarr.Queue{TotalRecords: 1, Records: []*radarr.QueueRecord{
				{ID: 42, MovieID: 7, DownloadClient: "AltMount", OutputPath: outputPath},
			}})
		case strings.HasPrefix(r.URL.Path, "/api/v3/queue/") && r.Method == http.MethodDelete:
			mu.Lock()
			deletes = append(deletes, r.URL.RequestURI())
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	client := radarr.New(&starr.Config{URL: srv.URL, APIKey: "test", Client: srv.Client()})
	return client, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), deletes...)
	}
}

// A queue item that reports the category folder as its output matched every
// file in that category by prefix, and deleting it with RemoveFromClient makes
// Radarr delete the whole category folder (#1029).
func TestFailRadarrQueueItemByPath_SkipsCategoryRootOutput(t *testing.T) {
	client, deletes := radarrQueueServer(t, "/mnt/altmount/complete/movies")
	err := categoryRootTestManager().failRadarrQueueItemByPath(context.Background(), client, "/mnt/altmount/complete/movies/Some.Movie.2020/movie.mkv")
	require.Error(t, err)
	require.Empty(t, deletes(), "a queue item whose output is a category folder must not be deleted")
}

func TestFailRadarrQueueItemByPath_ReleaseFolderStillMatches(t *testing.T) {
	client, deletes := radarrQueueServer(t, "/mnt/altmount/complete/movies/Some.Movie.2020")
	err := categoryRootTestManager().failRadarrQueueItemByPath(context.Background(), client, "/mnt/altmount/complete/movies/Some.Movie.2020/movie.mkv")
	require.NoError(t, err)
	require.Len(t, deletes(), 1)
	require.Contains(t, deletes()[0], "removeFromClient=true")
}

// The prefix match had no path-component boundary for Radarr, so a release
// folder matched a sibling whose name only starts the same way.
func TestFailRadarrQueueItemByPath_PrefixNeedsComponentBoundary(t *testing.T) {
	client, deletes := radarrQueueServer(t, "/mnt/altmount/complete/movies/Some.Movie")
	err := categoryRootTestManager().failRadarrQueueItemByPath(context.Background(), client, "/mnt/altmount/complete/movies/Some.Movie.2020/movie.mkv")
	require.Error(t, err)
	require.Empty(t, deletes())
}

func TestFailSonarrQueueItemByPath_SkipsCategoryRootOutput(t *testing.T) {
	stub := newArrStub()
	stub.queueEpisodeIDs = []int64{10, 11}
	stub.queueDownloadID = "pack"
	stub.queueClient = "AltMount"
	stub.queueOutputPath = "/mnt/altmount/complete/tv"
	srv := stub.serve()
	defer srv.Close()
	mgr := categoryRootTestManager()
	called := false
	mgr.SetSonarrQueueRepair(func(ctx context.Context, instance string, c clients.SonarrClient, g model.SonarrDownloadGroup) error {
		called = true
		return nil
	})
	err := mgr.failSonarrQueueItemByPath(context.Background(), sonarrClient(t, srv), "sonarr", "/mnt/altmount/complete/tv/Some.Show.S01/episode.mkv")
	require.Error(t, err)
	require.False(t, called, "a queue item whose output is a category folder must not be repaired (removed from the client)")
}
