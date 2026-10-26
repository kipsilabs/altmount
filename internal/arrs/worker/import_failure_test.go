package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/kipsilabs/altmount/internal/arrs/clients"
	"github.com/kipsilabs/altmount/internal/arrs/instances"
	"github.com/kipsilabs/altmount/internal/config"
)

// fakeSonarrQueue serves a fixed Sonarr queue and records deletes/unmonitors.
type fakeSonarrQueue struct {
	native         bool
	mu             sync.Mutex
	records        []map[string]any
	queueGets      int
	deletes        []string  // raw query of each queue DELETE
	unmonitored    [][]int64 // episodeIds of each monitored=false PUT
	episodeFileIDs map[int64]int64
	monitorFails   bool
	searches       [][]int64
}

func (f *fakeSonarrQueue) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if f.native {
			if r.URL.Path == "/api" {
				_, _ = w.Write([]byte(`{"current":"v5"}`))
				return
			}
			if strings.HasSuffix(r.URL.Path, "/system/status") {
				_, _ = w.Write([]byte(`{"appName":"Sonarr","version":"5.0.0"}`))
				return
			}
			// Reuse fixture behavior while retaining native queue/removal payloads.
			r.URL.Path = strings.Replace(r.URL.Path, "/api/v5/", "/api/v3/", 1)
		}

		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/episode":
			f.mu.Lock()
			defer f.mu.Unlock()
			var episodes []map[string]any
			seen := map[int64]bool{}
			for _, value := range r.URL.Query()["episodeIds"] {
				id, _ := strconv.ParseInt(value, 10, 64)
				if !seen[id] {
					episodes = append(episodes, map[string]any{"id": id, "monitored": true, "episodeFileId": f.episodeFileIDs[id], "hasFile": f.episodeFileIDs[id] > 0})
					seen[id] = true
				}
			}
			_ = json.NewEncoder(w).Encode(episodes)
		case r.Method == http.MethodPost && r.URL.Path == "/api/v3/command":
			var body struct {
				EpisodeIDs []int64 `json:"episodeIds"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			f.mu.Lock()
			f.searches = append(f.searches, body.EpisodeIDs)
			f.mu.Unlock()
			_, _ = w.Write([]byte(`{"id":99,"name":"EpisodeSearch","trigger":"manual"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/queue":
			f.mu.Lock()
			f.queueGets++
			recs := f.records
			f.mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{
				"page": 1, "pageSize": 500, "totalRecords": len(recs), "records": recs,
			})
		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/api/v3/queue/"):
			f.mu.Lock()
			f.deletes = append(f.deletes, r.URL.RawQuery)
			f.mu.Unlock()
			_, _ = w.Write([]byte(`{}`))
		case r.Method == http.MethodPut && r.URL.Path == "/api/v3/episode/monitor":
			if f.monitorFails {
				http.Error(w, "failed to unmonitor", 500)
				return
			}
			var body struct {
				EpisodeIDs []int64 `json:"episodeIds"`
				Monitored  bool    `json:"monitored"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			f.mu.Lock()
			if !body.Monitored {
				f.unmonitored = append(f.unmonitored, body.EpisodeIDs)
			}
			f.mu.Unlock()
			_, _ = w.Write([]byte(`[]`))
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	})
}

func queueRecord(id, episodeID int64, downloadID, downloadClient string) map[string]any {
	return map[string]any{
		"id": id, "episodeId": episodeID, "seriesId": 7,
		"title":          "Test.Show.S02E03.REMUX",
		"downloadId":     downloadID,
		"downloadClient": downloadClient,
		"protocol":       "usenet",
	}
}

func newImportFailureTestWorker(serverURL string, maxFailures int) *Worker {
	enabled := true
	cfg := &config.Config{}
	cfg.Arrs.Enabled = &enabled
	cfg.Arrs.QueueCleanupMaxFailures = maxFailures
	cfg.Arrs.SonarrInstances = []config.ArrsInstanceConfig{
		{Name: "test-sonarr", URL: serverURL, APIKey: "key", Category: "tv", Enabled: &enabled},
	}
	configGetter := func() *config.Config { return cfg }
	return NewWorker(configGetter, instances.NewManager(configGetter, nil), clients.NewManager(nil), nil, nil)
}

func TestHandleImportFailure_CountsBelowThreshold(t *testing.T) {
	fake := &fakeSonarrQueue{records: []map[string]any{
		queueRecord(101, 42, "SABnzbd_nzo_abc", "AltMount"),
	}}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	w := newImportFailureTestWorker(srv.URL, 2)

	w.HandleImportFailure(context.Background(), "SABnzbd_nzo_abc", "tv")

	if fake.queueGets != 1 {
		t.Fatalf("queue fetches = %d, want 1", fake.queueGets)
	}
	if len(fake.deletes) != 0 || len(fake.unmonitored) != 0 {
		t.Fatalf("below threshold must only count; deletes=%v unmonitored=%v", fake.deletes, fake.unmonitored)
	}
}

func TestHandleImportFailure_GivesUpAtThreshold(t *testing.T) {
	fake := &fakeSonarrQueue{records: []map[string]any{
		queueRecord(101, 42, "SABnzbd_nzo_abc", "AltMount"),
	}}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	w := newImportFailureTestWorker(srv.URL, 2)

	// First failure: counted only. Second failure (re-grab also failed): give up.
	w.HandleImportFailure(context.Background(), "SABnzbd_nzo_abc", "tv")
	fake.mu.Lock()
	fake.records[0]["downloadId"] = "replacement-download"
	fake.mu.Unlock()
	w.HandleImportFailure(context.Background(), "replacement-download", "tv") // a new failed download generation

	if len(fake.unmonitored) != 1 || len(fake.unmonitored[0]) != 1 || fake.unmonitored[0][0] != 42 {
		t.Fatalf("unmonitored = %v, want [[42]]", fake.unmonitored)
	}
	if len(fake.deletes) != 1 {
		t.Fatalf("queue deletes = %v, want exactly one", fake.deletes)
	}
	q := fake.deletes[0]
	if !strings.Contains(q, "blocklist=true") || !strings.Contains(q, "skipRedownload=true") {
		t.Fatalf("give-up delete must blocklist without re-search, got query %q", q)
	}
}

func TestHandleImportFailure_PackUnmonitorsAllTrippedEpisodes(t *testing.T) {
	fake := &fakeSonarrQueue{records: []map[string]any{
		queueRecord(101, 42, "SABnzbd_nzo_pack", "AltMount"),
		queueRecord(102, 43, "SABnzbd_nzo_pack", "AltMount"),
	}}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	w := newImportFailureTestWorker(srv.URL, 1) // first failure trips

	w.HandleImportFailure(context.Background(), "SABnzbd_nzo_pack", "tv")

	if len(fake.unmonitored) != 1 || len(fake.unmonitored[0]) != 2 {
		t.Fatalf("unmonitored = %v, want one PUT covering both episodes", fake.unmonitored)
	}
	// One delete is enough: removing any record of the download removes the pack.
	if len(fake.deletes) != 1 {
		t.Fatalf("queue deletes = %v, want exactly one (cascades to siblings)", fake.deletes)
	}
}

func TestHandleImportFailure_Gates(t *testing.T) {
	t.Run("foreign download client untouched", func(t *testing.T) {
		fake := &fakeSonarrQueue{records: []map[string]any{
			queueRecord(101, 42, "SABnzbd_nzo_abc", "RealSABnzbd"),
		}}
		srv := httptest.NewServer(fake.handler())
		defer srv.Close()
		w := newImportFailureTestWorker(srv.URL, 1)
		w.HandleImportFailure(context.Background(), "SABnzbd_nzo_abc", "tv")
		if len(fake.deletes) != 0 || len(fake.unmonitored) != 0 {
			t.Fatalf("foreign client item must not be acted on; deletes=%v unmonitored=%v", fake.deletes, fake.unmonitored)
		}
	})

	t.Run("breaker disabled: no arr requests", func(t *testing.T) {
		fake := &fakeSonarrQueue{}
		srv := httptest.NewServer(fake.handler())
		defer srv.Close()
		w := newImportFailureTestWorker(srv.URL, 0)
		w.HandleImportFailure(context.Background(), "SABnzbd_nzo_abc", "tv")
		if fake.queueGets != 0 {
			t.Fatalf("disabled breaker must not query the arr, got %d queue fetches", fake.queueGets)
		}
	})

	t.Run("unknown download id: counted nothing, deleted nothing", func(t *testing.T) {
		fake := &fakeSonarrQueue{records: []map[string]any{
			queueRecord(101, 42, "SABnzbd_nzo_other", "AltMount"),
		}}
		srv := httptest.NewServer(fake.handler())
		defer srv.Close()
		w := newImportFailureTestWorker(srv.URL, 1)
		w.HandleImportFailure(context.Background(), "SABnzbd_nzo_missing", "tv")
		if len(fake.deletes) != 0 || len(fake.unmonitored) != 0 {
			t.Fatalf("no matching download must mean no actions; deletes=%v unmonitored=%v", fake.deletes, fake.unmonitored)
		}
	})
}
