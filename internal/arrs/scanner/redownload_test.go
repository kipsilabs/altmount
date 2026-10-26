package scanner

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/kipsilabs/altmount/internal/arrs/clients"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/config"
	"golift.io/starr"
	"golift.io/starr/radarr"
	"golift.io/starr/sonarr"
)

func testManager(autoSearchWaitSeconds, fileDeleteConfirmSeconds int) *Manager {
	cfg := &config.Config{}
	cfg.Health.Repair.AutoSearchWaitSeconds = autoSearchWaitSeconds
	cfg.Health.Repair.FileDeleteConfirmSeconds = fileDeleteConfirmSeconds
	return NewManager(func() *config.Config { return cfg }, nil, nil, nil, nil, nil)
}

// arrStub is a minimal Sonarr/Radarr API double: it serves the command,
// episode, movie, queue and file-delete endpoints the repair coordination
// touches and records the order in which they were hit.
type arrStub struct {
	episodeStates   []*sonarr.Episode
	unmonitored     [][]int64
	failHistoryOnce bool
	historyFailed   bool
	mu              sync.Mutex

	events       []string
	postedSonarr []sonarrSearchBody
	postedRadarr []radarrSearchBody
	commandLists int
	deletes      int

	commands    []arrCommandResponse
	statuses    map[int64][]string
	postReplies []arrCommandResponse

	episodeFileID        int64
	extraEpisodeID       int64
	primaryEpisodeID     int64
	failedHistory        int
	clearOnDelete        bool
	failQueueAfterDelete bool

	queueEpisodeIDs []int64
	queueDownloadID string
	queueClient     string
	queueOutputPath string
	queueMovieIDs   []int64

	movieFileID int64
}

func newArrStub() *arrStub {
	return &arrStub{statuses: map[int64][]string{}, clearOnDelete: true}
}

func (s *arrStub) record(event string) {
	s.events = append(s.events, event)
}

func (s *arrStub) snapshotEvents() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.events...)
}

// nextStatus pops the scripted status for a command, repeating the last one
// once the script is exhausted.
func (s *arrStub) nextStatus(id int64) string {
	script := s.statuses[id]
	if len(script) == 0 {
		return "completed"
	}
	status := script[0]
	if len(script) > 1 {
		s.statuses[id] = script[1:]
	}
	return status
}

func (s *arrStub) serve() *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()

		path := r.URL.Path
		w.Header().Set("Content-Type", "application/json")

		switch {
		case path == "/api/v3/command" && r.Method == http.MethodGet:
			s.commandLists++
			s.record("commands")
			_ = json.NewEncoder(w).Encode(s.commands)

		case path == "/api/v3/command" && r.Method == http.MethodPost:
			s.record("post")
			var raw map[string]any
			_ = json.NewDecoder(r.Body).Decode(&raw)
			if _, ok := raw["episodeIds"]; ok {
				var body sonarrSearchBody
				remarshal(raw, &body)
				s.postedSonarr = append(s.postedSonarr, body)
			} else {
				var body radarrSearchBody
				remarshal(raw, &body)
				s.postedRadarr = append(s.postedRadarr, body)
			}
			reply := arrCommandResponse{ID: 99, Name: fmt.Sprint(raw["name"]), Status: "queued", Trigger: manualTrigger}
			if len(s.postReplies) > 0 {
				reply = s.postReplies[0]
				if len(s.postReplies) > 1 {
					s.postReplies = s.postReplies[1:]
				}
			}
			_ = json.NewEncoder(w).Encode(reply)

		case strings.HasPrefix(path, "/api/v3/command/"):
			id := int64(0)
			_, _ = fmt.Sscanf(strings.TrimPrefix(path, "/api/v3/command/"), "%d", &id)
			status := s.nextStatus(id)
			s.record("status:" + status)
			_ = json.NewEncoder(w).Encode(arrCommandResponse{ID: id, Name: "EpisodeSearch", Status: status, Trigger: "unspecified"})

		case path == "/api/v3/history":
			if s.historyFailed {
				_, _ = w.Write([]byte(`{"records":[{"id":51,"eventType":"downloadFailed","downloadId":"one-pack"},{"id":50,"eventType":"grabbed","downloadId":"one-pack"}],"totalRecords":2}`))
				return
			}
			_, _ = w.Write([]byte(`{"records":[{"id":50,"sourceTitle":"broken","eventType":"grabbed","downloadId":"one-pack"}],"totalRecords":1}`))
		case path == "/api/v3/history/failed/50" && r.Method == http.MethodPost:
			s.failedHistory++
			if s.failHistoryOnce {
				s.failHistoryOnce = false
				s.historyFailed = true
				http.Error(w, "response lost after accepted failure", http.StatusGatewayTimeout)
				return
			}
			_, _ = w.Write([]byte(`{}`))
		case path == "/api/v3/episode":
			if s.episodeStates != nil {
				var rows []*sonarr.Episode
				for _, ep := range s.episodeStates {
					include := len(r.URL.Query()["episodeIds"]) == 0
					for _, raw := range r.URL.Query()["episodeIds"] {
						if raw == fmt.Sprint(ep.ID) {
							include = true
						}
					}
					if include {
						rows = append(rows, ep)
					}
				}
				_ = json.NewEncoder(w).Encode(rows)
				return
			}

			s.record(fmt.Sprintf("episodes:%d", s.episodeFileID))
			primaryID := s.primaryEpisodeID
			if primaryID == 0 {
				primaryID = 10
			}
			eps := []*sonarr.Episode{{ID: primaryID, EpisodeFileID: s.episodeFileID, Monitored: true}}
			if s.extraEpisodeID > 0 {
				eps = append(eps, &sonarr.Episode{ID: s.extraEpisodeID, EpisodeFileID: s.episodeFileID, Monitored: true})
			}
			_ = json.NewEncoder(w).Encode(eps)

		case path == "/api/v3/episode/monitor" && r.Method == http.MethodPut:
			var body struct {
				IDs       []int64 `json:"episodeIds"`
				Monitored bool    `json:"monitored"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			if !body.Monitored {
				s.unmonitored = append(s.unmonitored, body.IDs)
			}
			for _, ep := range s.episodeStates {
				for _, id := range body.IDs {
					if ep.ID == id {
						ep.Monitored = body.Monitored
					}
				}
			}
			_, _ = w.Write([]byte(`[]`))
		case strings.HasPrefix(path, "/api/v3/episodeFile/") && r.Method == http.MethodDelete:
			s.deletes++
			s.record("delete")
			if s.clearOnDelete {
				s.episodeFileID = 0
				var removed int64
				_, _ = fmt.Sscanf(strings.TrimPrefix(path, "/api/v3/episodeFile/"), "%d", &removed)
				for _, ep := range s.episodeStates {
					if ep.EpisodeFileID == removed {
						ep.EpisodeFileID = 0
					}
				}
			}
			w.WriteHeader(http.StatusOK)

		case path == "/api/v3/moviefile/bulk" && r.Method == http.MethodDelete:
			s.deletes++
			s.record("delete")
			if s.clearOnDelete {
				s.movieFileID = 0
			}
			w.WriteHeader(http.StatusOK)

		case strings.HasPrefix(path, "/api/v3/movie/"):
			s.record(fmt.Sprintf("movie:%d", s.movieFileID))
			movie := radarr.Movie{ID: 7, HasFile: s.movieFileID != 0}
			if s.movieFileID != 0 {
				movie.MovieFile = &radarr.MovieFile{ID: s.movieFileID}
			}
			_ = json.NewEncoder(w).Encode(movie)

		case path == "/api/v3/queue":
			if s.failQueueAfterDelete && s.deletes > 0 {
				s.failQueueAfterDelete = false
				http.Error(w, "temporary queue error", 500)
				return
			}
			s.record("queue")
			sonarrRecords := make([]*sonarr.QueueRecord, 0, len(s.queueEpisodeIDs))
			for _, id := range s.queueEpisodeIDs {
				sonarrRecords = append(sonarrRecords, &sonarr.QueueRecord{EpisodeID: id, DownloadID: s.queueDownloadID, DownloadClient: s.queueClient, OutputPath: s.queueOutputPath, Status: "downloading"})
			}
			radarrRecords := make([]*radarr.QueueRecord, 0, len(s.queueMovieIDs))
			for _, id := range s.queueMovieIDs {
				radarrRecords = append(radarrRecords, &radarr.QueueRecord{MovieID: id})
			}
			if len(radarrRecords) > 0 {
				_ = json.NewEncoder(w).Encode(radarr.Queue{TotalRecords: len(radarrRecords), Records: radarrRecords})
				return
			}
			_ = json.NewEncoder(w).Encode(sonarr.Queue{TotalRecords: len(sonarrRecords), Records: sonarrRecords})

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	return httptest.NewServer(mux)
}

func remarshal(in any, out any) {
	data, _ := json.Marshal(in)
	_ = json.Unmarshal(data, out)
}

func sonarrClient(t *testing.T, srv *httptest.Server) clients.SonarrClient {
	t.Helper()
	return clients.NewLegacySonarr(&starr.Config{URL: srv.URL, APIKey: "test", Client: srv.Client()})
}

func radarrClient(t *testing.T, srv *httptest.Server) *radarr.Radarr {
	t.Helper()
	return radarr.New(&starr.Config{URL: srv.URL, APIKey: "test", Client: srv.Client()})
}

func autoEpisodeSearch(id int64, episodeIDs ...float64) arrCommandResponse {
	ids := make([]any, 0, len(episodeIDs))
	for _, epID := range episodeIDs {
		ids = append(ids, epID)
	}
	return arrCommandResponse{
		ID:      id,
		Name:    "EpisodeSearch",
		Status:  "started",
		Trigger: "unspecified",
		Queued:  time.Now(),
		Body:    map[string]any{"episodeIds": ids},
	}
}

func indexOf(events []string, want string) int {
	for i, e := range events {
		if e == want {
			return i
		}
	}
	return -1
}

func lastIndexOf(events []string, want string) int {
	for i := len(events) - 1; i >= 0; i-- {
		if events[i] == want {
			return i
		}
	}
	return -1
}

func TestSettleSonarrAutoRedownload_WaitsForAutoSearchThenDeletesAndSearchesOnce(t *testing.T) {
	stub := newArrStub()
	stub.episodeFileID = 500
	stub.commands = []arrCommandResponse{autoEpisodeSearch(1, 10)}
	stub.statuses[1] = []string{"started", "completed"}

	srv := stub.serve()
	defer srv.Close()

	client := sonarrClient(t, srv)
	m := testManager(10, 5)

	auto, state, err := m.settleSonarrAutoRedownload(context.Background(), client, "sonarr", sonarrRedownloadTarget{
		seriesID:      3,
		episodeIDs:    []int64{10},
		episodeFileID: 500,
	}, time.Now().Add(-time.Second))
	if err != nil {
		t.Fatalf("settle returned error: %v", err)
	}
	if state != redownloadProceed {
		t.Fatalf("state = %v; want redownloadProceed", state)
	}
	if auto == nil || auto.ID != 1 {
		t.Fatalf("auto command not detected: %+v", auto)
	}

	if _, err := m.sendSonarrSearch(context.Background(), client, "sonarr", []int64{10}, auto); err != nil {
		t.Fatalf("sendSonarrSearch returned error: %v", err)
	}

	events := stub.snapshotEvents()
	if len(stub.postedSonarr) != 1 {
		t.Fatalf("expected exactly one search command, got %d", len(stub.postedSonarr))
	}
	if stub.postedSonarr[0].Trigger != manualTrigger {
		t.Errorf("search trigger = %q; want %q", stub.postedSonarr[0].Trigger, manualTrigger)
	}
	if stub.deletes != 1 {
		t.Errorf("delete count = %d; want 1", stub.deletes)
	}

	deleteAt := indexOf(events, "delete")
	postAt := indexOf(events, "post")
	clearedAt := lastIndexOf(events, "episodes:0")
	statusAt := lastIndexOf(events, "status:completed")
	if statusAt == -1 || deleteAt == -1 || postAt == -1 || clearedAt == -1 {
		t.Fatalf("missing expected events: %v", events)
	}
	if statusAt >= deleteAt || deleteAt >= clearedAt || clearedAt >= postAt {
		t.Errorf("unexpected ordering %v (status=%d delete=%d cleared=%d post=%d)",
			events, statusAt, deleteAt, clearedAt, postAt)
	}
}

func TestSettleSonarrAutoRedownload_QueueAlreadyHoldsReplacement(t *testing.T) {
	stub := newArrStub()
	stub.episodeFileID = 500
	stub.commands = []arrCommandResponse{autoEpisodeSearch(1, 10)}
	stub.statuses[1] = []string{"completed"}
	stub.queueEpisodeIDs = []int64{10}

	srv := stub.serve()
	defer srv.Close()

	m := testManager(10, 5)
	_, state, err := m.settleSonarrAutoRedownload(context.Background(), sonarrClient(t, srv), "sonarr", sonarrRedownloadTarget{
		seriesID:      3,
		episodeIDs:    []int64{10},
		episodeFileID: 500,
	}, time.Now().Add(-time.Second))
	if err != nil {
		t.Fatalf("settle returned error: %v", err)
	}
	if state != redownloadHandled {
		t.Fatalf("state = %v; want redownloadHandled", state)
	}
	if stub.deletes != 0 {
		t.Errorf("delete count = %d; want 0", stub.deletes)
	}
	if len(stub.postedSonarr) != 0 {
		t.Errorf("expected no search command, got %d", len(stub.postedSonarr))
	}
}

func TestSendSonarrSearch_RetriesOnceWhenDeduplicated(t *testing.T) {
	stub := newArrStub()
	stub.episodeFileID = 0
	stub.statuses[1] = []string{"completed"}
	stub.postReplies = []arrCommandResponse{
		{ID: 1, Name: "EpisodeSearch", Status: "started", Trigger: "unspecified"},
		{ID: 2, Name: "EpisodeSearch", Status: "queued", Trigger: manualTrigger},
	}

	srv := stub.serve()
	defer srv.Close()

	m := testManager(10, 5)
	resp, err := m.sendSonarrSearch(context.Background(), sonarrClient(t, srv), "sonarr", []int64{10}, nil)
	if err != nil {
		t.Fatalf("sendSonarrSearch returned error: %v", err)
	}
	if resp == nil || resp.ID != 2 {
		t.Fatalf("expected the retry response, got %+v", resp)
	}
	if len(stub.postedSonarr) != 2 {
		t.Fatalf("expected exactly two search commands (one retry), got %d", len(stub.postedSonarr))
	}

	events := stub.snapshotEvents()
	firstPost := indexOf(events, "post")
	statusAt := indexOf(events, "status:completed")
	lastPost := lastIndexOf(events, "post")
	if statusAt == -1 || firstPost >= statusAt || statusAt >= lastPost {
		t.Errorf("retry was not issued after the deduplicated command finished: %v", events)
	}
}

func TestSettleSonarrAutoRedownload_BudgetExpiryDefersDestructiveRepair(t *testing.T) {
	stub := newArrStub()
	stub.episodeFileID = 500
	stub.commands = []arrCommandResponse{autoEpisodeSearch(1, 10)}
	stub.statuses[1] = []string{"started"}
	srv := stub.serve()
	defer srv.Close()
	_, _, err := testManager(1, 1).settleSonarrAutoRedownload(context.Background(), sonarrClient(t, srv), "sonarr", sonarrRedownloadTarget{seriesID: 3, episodeIDs: []int64{10}, episodeFileID: 500}, time.Now().Add(-time.Second))
	if err == nil {
		t.Fatal("expected deferred repair while automatic search is active")
	}
	if stub.deletes != 0 || len(stub.postedSonarr) > 0 {
		t.Fatalf("must not mutate after wait expiry; deletes=%d", stub.deletes)
	}
}

func TestSettleSonarrAutoRedownload_ZeroWaitKeepsImmediateBehaviour(t *testing.T) {
	stub := newArrStub()
	stub.episodeFileID = 500
	stub.commands = []arrCommandResponse{autoEpisodeSearch(1, 10)}
	stub.postReplies = []arrCommandResponse{{ID: 1, Name: "EpisodeSearch", Status: "started", Trigger: "unspecified"}}

	srv := stub.serve()
	defer srv.Close()

	client := sonarrClient(t, srv)
	m := testManager(0, 15)

	auto, state, err := m.settleSonarrAutoRedownload(context.Background(), client, "sonarr", sonarrRedownloadTarget{
		seriesID:      3,
		episodeIDs:    []int64{10},
		episodeFileID: 500,
	}, time.Now().Add(-time.Second))
	if err != nil {
		t.Fatalf("settle returned error: %v", err)
	}
	if state != redownloadProceed || auto != nil {
		t.Fatalf("state = %v, auto = %+v; want proceed with no auto command", state, auto)
	}
	if stub.deletes != 1 {
		t.Errorf("delete count = %d; want 1", stub.deletes)
	}
	if stub.commandLists != 0 {
		t.Errorf("command list polled %d times; want 0 with the wait disabled", stub.commandLists)
	}

	if _, err := m.sendSonarrSearch(context.Background(), client, "sonarr", []int64{10}, auto); err != nil {
		t.Fatalf("sendSonarrSearch returned error: %v", err)
	}
	if len(stub.postedSonarr) != 1 {
		t.Fatalf("expected exactly one search command with no dedup retry, got %d", len(stub.postedSonarr))
	}

	events := stub.snapshotEvents()
	if indexOf(events, "queue") == -1 {
		t.Errorf("queue coverage must be refreshed even with command waiting disabled: %v", events)
	}
}

func TestSettleRadarrAutoRedownload_WaitsThenDeletesAndSearches(t *testing.T) {
	stub := newArrStub()
	stub.movieFileID = 800
	stub.commands = []arrCommandResponse{{
		ID:      4,
		Name:    "MoviesSearch",
		Status:  "queued",
		Trigger: "unspecified",
		Queued:  time.Now(),
		Body:    map[string]any{"movieIds": []any{float64(7)}},
	}}
	stub.statuses[4] = []string{"started", "completed"}

	srv := stub.serve()
	defer srv.Close()

	client := radarrClient(t, srv)
	m := testManager(10, 5)

	auto, state, err := m.settleRadarrAutoRedownload(context.Background(), client, "radarr", radarrRedownloadTarget{
		movieID:     7,
		movieFileID: 800,
	}, time.Now().Add(-time.Second))
	if err != nil {
		t.Fatalf("settle returned error: %v", err)
	}
	if state != redownloadProceed {
		t.Fatalf("state = %v; want redownloadProceed", state)
	}
	if auto == nil || auto.ID != 4 {
		t.Fatalf("auto command not detected: %+v", auto)
	}
	if stub.deletes != 1 {
		t.Errorf("delete count = %d; want 1", stub.deletes)
	}

	if _, err := m.sendRadarrSearch(context.Background(), client, "radarr", 7, auto); err != nil {
		t.Fatalf("sendRadarrSearch returned error: %v", err)
	}
	if len(stub.postedRadarr) != 1 {
		t.Fatalf("expected exactly one search command, got %d", len(stub.postedRadarr))
	}
	if stub.postedRadarr[0].Trigger != manualTrigger {
		t.Errorf("search trigger = %q; want %q", stub.postedRadarr[0].Trigger, manualTrigger)
	}

	events := stub.snapshotEvents()
	deleteAt := indexOf(events, "delete")
	postAt := indexOf(events, "post")
	clearedAt := lastIndexOf(events, "movie:0")
	if deleteAt == -1 || postAt == -1 || clearedAt == -1 || deleteAt >= clearedAt || clearedAt >= postAt {
		t.Errorf("unexpected ordering %v", events)
	}
}

func TestBodyInt64Set(t *testing.T) {
	tests := []struct {
		name string
		body map[string]any
		key  string
		want []int64
	}{
		{
			name: "json numbers decode as float64",
			body: map[string]any{"episodeIds": []any{float64(12), float64(13)}},
			key:  "episodeIds",
			want: []int64{12, 13},
		},
		{
			name: "scalar body value",
			body: map[string]any{"seriesId": float64(41), "seasonNumber": float64(2)},
			key:  "seriesId",
			want: []int64{41},
		},
		{
			name: "missing key yields empty set",
			body: map[string]any{"movieIds": []any{float64(9)}},
			key:  "episodeIds",
			want: nil,
		},
		{
			name: "nil body yields empty set",
			body: nil,
			key:  "episodeIds",
			want: nil,
		},
		{
			name: "numeric strings are parsed",
			body: map[string]any{"movieIds": []any{"5", "nope"}},
			key:  "movieIds",
			want: []int64{5},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := bodyInt64Set(tt.body, tt.key)
			if len(got) != len(tt.want) {
				t.Fatalf("got %v; want %v", got, tt.want)
			}
			for _, id := range tt.want {
				if _, ok := got[id]; !ok {
					t.Errorf("missing id %d in %v", id, got)
				}
			}
		})
	}
}

func TestFindAutoRedownloadCommand(t *testing.T) {
	since := time.Now()
	episodeTargets := map[string][]int64{"episodeIds": {12}, "seriesId": {41}}

	episodeSearch := &arrCommand{
		ID: 1, Name: "EpisodeSearch", Status: "queued", Trigger: "unspecified", Queued: since.Add(time.Second),
		Body: map[string]any{"episodeIds": []any{float64(12)}},
	}
	seasonSearch := &arrCommand{
		ID: 2, Name: "SeasonSearch", Status: "started", Trigger: "unspecified", Queued: since.Add(time.Second),
		Body: map[string]any{"seriesId": float64(41), "seasonNumber": float64(2)},
	}
	otherSeries := &arrCommand{
		ID: 3, Name: "EpisodeSearch", Status: "queued", Queued: since.Add(time.Second),
		Body: map[string]any{"episodeIds": []any{float64(77)}},
	}
	stale := &arrCommand{
		ID: 4, Name: "EpisodeSearch", Status: "queued", Queued: since.Add(-time.Minute),
		Body: map[string]any{"episodeIds": []any{float64(12)}},
	}
	finished := &arrCommand{
		ID: 5, Name: "EpisodeSearch", Status: "completed", Queued: since.Add(time.Second),
		Body: map[string]any{"episodeIds": []any{float64(12)}},
	}
	unrelatedName := &arrCommand{
		ID: 6, Name: "RefreshSeries", Status: "queued", Queued: since.Add(time.Second),
		Body: map[string]any{"seriesId": float64(41)},
	}

	tests := []struct {
		name   string
		list   []*arrCommand
		want   int64
		wantOk bool
	}{
		{name: "matches an episode search", list: []*arrCommand{otherSeries, episodeSearch}, want: 1, wantOk: true},
		{name: "matches a season search by series id", list: []*arrCommand{seasonSearch}, want: 2, wantOk: true},
		{name: "ignores other targets", list: []*arrCommand{otherSeries}},
		{name: "ignores commands queued before the blocklist", list: []*arrCommand{stale}},
		{name: "ignores terminal commands", list: []*arrCommand{finished}},
		{name: "ignores unrelated command names", list: []*arrCommand{unrelatedName}},
		{name: "ignores nil entries", list: []*arrCommand{nil}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := findAutoRedownloadCommand(tt.list, []string{"EpisodeSearch", "SeasonSearch"}, episodeTargets, since)
			if !tt.wantOk {
				if got != nil {
					t.Fatalf("got command %d; want none", got.ID)
				}
				return
			}
			if got == nil || got.ID != tt.want {
				t.Fatalf("got %+v; want command %d", got, tt.want)
			}
		})
	}
}

func TestCommandWasDeduplicated(t *testing.T) {
	auto := &arrCommand{ID: 1}
	tests := []struct {
		name string
		resp *arrCommand
		auto *arrCommand
		want bool
	}{
		{name: "manual trigger is ours", resp: &arrCommand{ID: 9, Trigger: manualTrigger}, auto: auto},
		{name: "foreign trigger means deduplicated", resp: &arrCommand{ID: 9, Trigger: "unspecified"}, auto: auto, want: true},
		{name: "matching auto id means deduplicated", resp: &arrCommand{ID: 1, Trigger: manualTrigger}, auto: auto, want: true},
		{name: "empty trigger with no auto command is ours", resp: &arrCommand{ID: 9}},
		{name: "nil response", resp: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := commandWasDeduplicated(tt.resp, tt.auto); got != tt.want {
				t.Errorf("got %v; want %v", got, tt.want)
			}
		})
	}
}
