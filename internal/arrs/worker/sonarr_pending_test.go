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
	"time"

	"github.com/kipsilabs/altmount/internal/arrs/failures"
	"github.com/stretchr/testify/require"
)

type pendingSonarrFixture struct {
	mu                                                         sync.Mutex
	removed, failReadAfterRemove, failDelete, ambiguousCommand bool
	deletes, searches                                          int
	monitored                                                  map[int64]bool
	commandAt                                                  time.Time
}

func (f *pendingSonarrFixture) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/system/status"):
			_, _ = w.Write([]byte(`{"appName":"Sonarr","version":"4.0.0"}`))
		case r.URL.Path == "/api":
			_, _ = w.Write([]byte(`{"current":"v3"}`))
		case r.Method == "GET" && r.URL.Path == "/api/v3/queue":
			rows := []map[string]any{}
			if !f.removed {
				rows = []map[string]any{queueRecord(1, 42, "failed", "AltMount"), queueRecord(2, 43, "failed", "AltMount")}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"totalRecords": len(rows), "records": rows})
		case r.Method == "GET" && r.URL.Path == "/api/v3/episode":
			if f.removed && f.failReadAfterRemove {
				f.failReadAfterRemove = false
				w.WriteHeader(500)
				return
			}
			rows := []map[string]any{}
			for _, raw := range r.URL.Query()["episodeIds"] {
				id, _ := strconv.ParseInt(raw, 10, 64)
				monitored := true
				if v, ok := f.monitored[id]; ok {
					monitored = v
				}
				rows = append(rows, map[string]any{"id": id, "monitored": monitored, "episodeFileId": 0})
			}
			_ = json.NewEncoder(w).Encode(rows)
		case r.Method == "PUT" && r.URL.Path == "/api/v3/episode/monitor":
			var b struct {
				IDs []int64 `json:"episodeIds"`
			}
			_ = json.NewDecoder(r.Body).Decode(&b)
			if f.monitored == nil {
				f.monitored = map[int64]bool{}
			}
			for _, id := range b.IDs {
				f.monitored[id] = false
			}
			_, _ = w.Write([]byte(`[]`))
		case r.Method == "DELETE" && strings.HasPrefix(r.URL.Path, "/api/v3/queue/"):
			f.deletes++
			if f.failDelete {
				f.failDelete = false
				w.WriteHeader(500)
				return
			}
			f.removed = true
			w.WriteHeader(204)
		case r.Method == "POST" && r.URL.Path == "/api/v3/command":
			f.searches++
			f.commandAt = time.Now().UTC()
			if f.ambiguousCommand {
				f.ambiguousCommand = false
				w.WriteHeader(504)
				return
			}
			_, _ = w.Write([]byte(`{"id":10}`))
		case r.Method == "GET" && r.URL.Path == "/api/v3/command":
			if f.commandAt.IsZero() {
				_, _ = w.Write([]byte(`[]`))
				return
			}
			_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 10, "name": "EpisodeSearch", "status": "completed", "trigger": "manual", "queued": f.commandAt, "body": map[string]any{"episodeIds": []int64{43}}}})
		default:
			http.NotFound(w, r)
		}
	})
}
func TestPendingSonarrSearchSurvivesRemovedGroupAndReadFailure(t *testing.T) {
	f := &pendingSonarrFixture{failReadAfterRemove: true}
	srv := httptest.NewServer(f.handler())
	defer srv.Close()
	w := newImportFailureTestWorker(srv.URL, 2)
	w.breaker.Bump(failures.EpisodeKey("test-sonarr", 42))
	w.HandleImportFailure(context.Background(), "failed", "tv")
	require.Equal(t, 1, f.deletes)
	require.Zero(t, f.searches)
	w.retryPendingSonarrActions(context.Background())
	require.Equal(t, 1, f.deletes)
	require.Equal(t, 1, f.searches)
	w.retryPendingSonarrActions(context.Background())
	require.Equal(t, 1, f.searches)
}
func TestPendingSonarrCommandTimeoutReconcilesAcceptedSearch(t *testing.T) {
	f := &pendingSonarrFixture{ambiguousCommand: true}
	srv := httptest.NewServer(f.handler())
	defer srv.Close()
	w := newImportFailureTestWorker(srv.URL, 2)
	w.breaker.Bump(failures.EpisodeKey("test-sonarr", 42))
	w.HandleImportFailure(context.Background(), "failed", "tv")
	require.Equal(t, 1, f.deletes)
	require.Equal(t, 1, f.searches)
	w.retryPendingSonarrActions(context.Background())
	w.retryPendingSonarrActions(context.Background())
	require.Equal(t, 1, f.deletes)
	require.Equal(t, 1, f.searches)
}
func TestPendingSonarrDeleteFailureRetainsUnmonitoredIntent(t *testing.T) {
	f := &pendingSonarrFixture{failDelete: true}
	srv := httptest.NewServer(f.handler())
	defer srv.Close()
	w := newImportFailureTestWorker(srv.URL, 2)
	w.breaker.Bump(failures.EpisodeKey("test-sonarr", 42))
	w.HandleImportFailure(context.Background(), "failed", "tv")
	require.False(t, f.monitored[42])
	require.False(t, f.removed)
	w.retryPendingSonarrActions(context.Background())
	require.True(t, f.removed)
	require.Equal(t, 1, f.searches)
	require.Equal(t, 2, f.deletes, "a failed removal is retried only after rereading the original queue")
}
func TestPendingSonarrRecoveryRunsWithCleanupDisabled(t *testing.T) {
	f := &pendingSonarrFixture{failReadAfterRemove: true}
	srv := httptest.NewServer(f.handler())
	defer srv.Close()
	w := newImportFailureTestWorker(srv.URL, 2)
	disabled := false
	w.configGetter().Arrs.QueueCleanupEnabled = &disabled
	w.breaker.Bump(failures.EpisodeKey("test-sonarr", 42))
	w.HandleImportFailure(context.Background(), "failed", "tv")
	w.workerCtx = context.Background()
	w.safeCleanup()
	require.Equal(t, 1, f.searches)
	require.NoError(t, w.Start(context.Background()))
	require.True(t, w.workerRunning)
	w.Stop(context.Background())
}

func TestPendingSonarrRecoveryRespectsDisabledInstance(t *testing.T) {
	f := &pendingSonarrFixture{failReadAfterRemove: true}
	srv := httptest.NewServer(f.handler())
	defer srv.Close()
	w := newImportFailureTestWorker(srv.URL, 2)
	w.breaker.Bump(failures.EpisodeKey("test-sonarr", 42))
	w.HandleImportFailure(context.Background(), "failed", "tv")
	require.Equal(t, 1, f.deletes)
	require.Zero(t, f.searches)
	disabled := false
	w.configGetter().Arrs.SonarrInstances[0].Enabled = &disabled
	w.retryPendingSonarrActions(context.Background())
	require.Zero(t, f.searches)
	enabled := true
	w.configGetter().Arrs.SonarrInstances[0].Enabled = &enabled
	w.retryPendingSonarrActions(context.Background())
	require.Equal(t, 1, f.searches)
	require.Equal(t, 1, f.deletes)
}
