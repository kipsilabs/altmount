package scanner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/kipsilabs/altmount/internal/arrs/clients"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/kipsilabs/altmount/internal/arrs/model"
	"golift.io/starr"
	"golift.io/starr/radarr"
	"golift.io/starr/sonarr"
)

const (
	manualTrigger = "manual"
	commandURI    = "v3/command"

	autoSearchDetectWindow   = 5 * time.Second
	autoSearchDetectInterval = 500 * time.Millisecond
	commandPollInitial       = 500 * time.Millisecond
	commandPollMax           = 5 * time.Second
	fileClearPollInterval    = 300 * time.Millisecond
)

// arrCommand is the subset of a Sonarr/Radarr command record the repair
// coordination needs, normalised across both apps.
type arrCommand struct {
	ID      int64
	Name    string
	Status  string
	Trigger string
	Queued  time.Time
	Body    map[string]any
}

// arrCommandResponse mirrors the /api/v3/command payload. It is declared here
// rather than reusing starr's per-app response types so one decoder serves both
// Sonarr and Radarr.
type arrCommandResponse struct {
	ID      int64          `json:"id"`
	Name    string         `json:"name"`
	Status  string         `json:"status"`
	Trigger string         `json:"trigger"`
	Queued  time.Time      `json:"queued"`
	Body    map[string]any `json:"body"`
}

func (r *arrCommandResponse) command() *arrCommand {
	if r == nil {
		return nil
	}
	return &arrCommand{ID: r.ID, Name: r.Name, Status: r.Status, Trigger: r.Trigger, Queued: r.Queued, Body: r.Body}
}

// sonarrSearchBody and radarrSearchBody carry the manual trigger that starr's
// CommandRequest types cannot express.
type sonarrSearchBody struct {
	Name       string  `json:"name"`
	EpisodeIDs []int64 `json:"episodeIds,omitempty"`
	Trigger    string  `json:"trigger"`
}

type radarrSearchBody struct {
	Name     string  `json:"name"`
	MovieIDs []int64 `json:"movieIds,omitempty"`
	Trigger  string  `json:"trigger"`
}

var terminalCommandStatuses = map[string]struct{}{
	"completed": {},
	"failed":    {},
	"aborted":   {},
	"cancelled": {},
	"orphaned":  {},
}

func commandIsTerminal(cmd *arrCommand) bool {
	if cmd == nil {
		return false
	}
	_, ok := terminalCommandStatuses[strings.ToLower(cmd.Status)]
	return ok
}

// commandGetter fetches one command by id. Sonarr and Radarr differ only in how
// the request is issued, so every wait helper takes this closure.
type commandGetter func(context.Context, int64) (*arrCommand, error)

func sonarrCommandByID(ctx context.Context, client clients.SonarrClient, id int64) (*arrCommand, error) {
	resp, err := client.GetCommandStatusContext(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("failed to read Sonarr command %d: %w", id, err)
	}
	return (&arrCommandResponse{
		ID:      resp.ID,
		Name:    resp.Name,
		Status:  resp.Status,
		Trigger: resp.Trigger,
		Queued:  resp.Queued,
		Body:    resp.Body,
	}).command(), nil
}

// radarrCommandByID issues a raw GET because starr's Radarr client exposes no
// single-command status endpoint.
func radarrCommandByID(ctx context.Context, client *radarr.Radarr, id int64) (*arrCommand, error) {
	var out arrCommandResponse
	req := starr.Request{URI: commandURI + "/" + strconv.FormatInt(id, 10)}
	if err := client.GetInto(ctx, req, &out); err != nil {
		return nil, fmt.Errorf("failed to read Radarr command %d: %w", id, err)
	}
	return out.command(), nil
}

func sonarrCommands(ctx context.Context, client clients.SonarrClient) ([]*arrCommand, error) {
	list, err := client.GetCommandsContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list Sonarr commands: %w", err)
	}
	out := make([]*arrCommand, 0, len(list))
	for _, c := range list {
		if c == nil {
			continue
		}
		out = append(out, (&arrCommandResponse{
			ID:      c.ID,
			Name:    c.Name,
			Status:  c.Status,
			Trigger: c.Trigger,
			Queued:  c.Queued,
			Body:    c.Body,
		}).command())
	}
	return out, nil
}

func radarrCommands(ctx context.Context, client *radarr.Radarr) ([]*arrCommand, error) {
	list, err := client.GetCommandsContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list Radarr commands: %w", err)
	}
	out := make([]*arrCommand, 0, len(list))
	for _, c := range list {
		if c == nil {
			continue
		}
		out = append(out, (&arrCommandResponse{
			ID:      c.ID,
			Name:    c.Name,
			Status:  c.Status,
			Trigger: c.Trigger,
			Queued:  c.Queued,
			Body:    c.Body,
		}).command())
	}
	return out, nil
}

// bodyInt64Set collects the integer ids stored under key in a command body.
// Command bodies arrive as map[string]any, so every number is a float64.
func bodyInt64Set(body map[string]any, key string) map[int64]struct{} {
	out := make(map[int64]struct{})
	raw, ok := body[key]
	if !ok {
		return out
	}
	add := func(v any) {
		switch n := v.(type) {
		case float64:
			out[int64(n)] = struct{}{}
		case float32:
			out[int64(n)] = struct{}{}
		case int:
			out[int64(n)] = struct{}{}
		case int64:
			out[n] = struct{}{}
		case json.Number:
			if parsed, err := n.Int64(); err == nil {
				out[parsed] = struct{}{}
			}
		case string:
			if parsed, err := strconv.ParseInt(n, 10, 64); err == nil {
				out[parsed] = struct{}{}
			}
		}
	}
	if list, ok := raw.([]any); ok {
		for _, v := range list {
			add(v)
		}
		return out
	}
	add(raw)
	return out
}

// findAutoRedownloadCommand picks out the search the ARR queued for itself right
// after a release was blocklisted. targets maps a command-body key to the ids a
// matching command must reference, so a Sonarr caller can match both the
// per-episode EpisodeSearch and the whole-season SeasonSearch without confusing
// series ids for episode ids.
func findAutoRedownloadCommand(list []*arrCommand, names []string, targets map[string][]int64, since time.Time) *arrCommand {
	for _, cmd := range list {
		if cmd == nil || !containsFold(names, cmd.Name) {
			continue
		}
		status := strings.ToLower(cmd.Status)
		if status != "queued" && status != "started" {
			continue
		}
		if !cmd.Queued.IsZero() && cmd.Queued.Before(since) {
			continue
		}
		for key, ids := range targets {
			found := bodyInt64Set(cmd.Body, key)
			for _, id := range ids {
				if _, ok := found[id]; ok {
					return cmd
				}
			}
		}
	}
	return nil
}

func containsFold(values []string, want string) bool {
	for _, v := range values {
		if strings.EqualFold(v, want) {
			return true
		}
	}
	return false
}

// waitCommandTerminal polls a command until it reaches a terminal status. A
// budget expiry is not an error: the caller degrades to the unsynchronised
// ordering rather than abandoning the repair.
func waitCommandTerminal(ctx context.Context, get commandGetter, id int64, budget time.Duration) (*arrCommand, error) {
	deadline := time.Now().Add(budget)
	delay := commandPollInitial

	var last *arrCommand
	var lastErr error

	for {
		cmd, err := get(ctx, id)
		if err != nil {
			lastErr = err
		} else {
			last = cmd
			lastErr = nil
			if commandIsTerminal(cmd) {
				return cmd, nil
			}
		}

		if time.Now().After(deadline) {
			break
		}

		select {
		case <-ctx.Done():
			return last, ctx.Err()
		case <-time.After(delay):
		}

		if delay < commandPollMax {
			delay *= 2
			if delay > commandPollMax {
				delay = commandPollMax
			}
		}
	}

	if last == nil && lastErr != nil {
		return nil, lastErr
	}
	slog.WarnContext(ctx, "ARR automatic redownload search did not finish within the configured budget, proceeding anyway",
		"command_id", id, "budget", budget, "status", commandStatus(last))
	return last, nil
}

func commandStatus(cmd *arrCommand) string {
	if cmd == nil {
		return "unknown"
	}
	return cmd.Status
}

// waitEpisodeFileCleared polls until no target episode still points at a file
// record, so the search that follows cannot race the deletion.
func waitEpisodeFileCleared(ctx context.Context, client clients.SonarrClient, episodeIDs []int64, budget time.Duration, originalFileIDs ...int64) bool {
	if len(episodeIDs) == 0 || budget <= 0 {
		return false
	}
	deadline := time.Now().Add(budget)
	for {
		episodes, err := client.GetSeriesEpisodesContext(ctx, &sonarr.GetEpisode{EpisodeIDs: episodeIDs})
		if err == nil {
			cleared := true
			for _, ep := range episodes {
				if ep != nil && ep.EpisodeFileID != 0 {
					cleared = false
					break
				}
			}
			if cleared {
				return true
			}
		}

		if time.Now().After(deadline) {
			slog.WarnContext(ctx, "Sonarr still reports the original episode file after deletion, deferring search",
				"episode_ids", episodeIDs, "budget", budget)
			return false
		}

		select {
		case <-ctx.Done():
			return false
		case <-time.After(fileClearPollInterval):
		}
	}
}

// waitMovieFileCleared is the Radarr twin of waitEpisodeFileCleared.
func waitMovieFileCleared(ctx context.Context, client *radarr.Radarr, movieID int64, budget time.Duration) bool {
	if movieID == 0 || budget <= 0 {
		return false
	}
	deadline := time.Now().Add(budget)
	for {
		movie, err := client.GetMovieByIDContext(ctx, movieID)
		if err == nil && movieFileID(movie) == 0 {
			return true
		}

		if time.Now().After(deadline) {
			slog.WarnContext(ctx, "Radarr still reports a movie file after deletion, proceeding with the search anyway",
				"movie_id", movieID, "budget", budget)
			return false
		}

		select {
		case <-ctx.Done():
			return false
		case <-time.After(fileClearPollInterval):
		}
	}
}

func movieFileID(movie *radarr.Movie) int64 {
	if movie == nil || movie.MovieFile == nil {
		return 0
	}
	return movie.MovieFile.ID
}

// sendSearchCommandWithTrigger posts a search command carrying an explicit
// manual trigger, which starr's CommandRequest types cannot express. The manual
// trigger is what makes the ARR treat the search as user-invoked and bypass the
// release delay profile.
func sendSearchCommandWithTrigger(ctx context.Context, api starr.APIer, body any) (*arrCommand, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("failed to encode search command: %w", err)
	}

	var out arrCommandResponse
	req := starr.Request{URI: commandURI, Body: bytes.NewReader(payload)}
	if err := api.PostInto(ctx, req, &out); err != nil {
		return nil, fmt.Errorf("failed to send search command: %w", err)
	}
	return out.command(), nil
}

// commandWasDeduplicated reports whether the ARR handed back a pre-existing
// command instead of queueing ours. Its command queue compares only the command
// payload — the trigger is not part of that comparison — so a search issued
// while the ARR's own automatic search is still queued silently collapses into
// it. A response trigger other than manual, or an id matching the automatic
// search, is therefore proof that our search was never queued.
func commandWasDeduplicated(resp, auto *arrCommand) bool {
	if resp == nil {
		return false
	}
	if resp.Trigger != "" && !strings.EqualFold(resp.Trigger, manualTrigger) {
		return true
	}
	return auto != nil && resp.ID == auto.ID
}

type redownloadState int

const (
	// redownloadProceed: delete confirmed (or not needed); issue the search.
	redownloadProceed redownloadState = iota
	// redownloadHandled: the ARR already grabbed a replacement.
	redownloadHandled
	// redownloadSatisfied: a different file is already linked to the target.
	redownloadSatisfied
)

func (m *Manager) autoSearchWaitBudget() time.Duration {
	return m.configGetter().GetRepairAutoSearchWait()
}

func (m *Manager) fileDeleteConfirmBudget() time.Duration {
	return m.configGetter().GetRepairFileDeleteConfirm()
}

type sonarrRedownloadTarget struct {
	seriesID      int64
	episodeIDs    []int64
	episodeFileID int64
	downloadID    string
	seasonNumbers []int64
}

// settleSonarrAutoRedownload runs between blocklisting a release and searching
// for its replacement.
//
// Blocklisting makes Sonarr queue an automatic redownload search of its own.
// Deleting the episode file while that search is running leaves each candidate
// evaluation holding an episode whose EpisodeFileId is still set but whose lazy
// EpisodeFile reference no longer resolves, which the delay specification
// dereferences unconditionally — every candidate is then rejected and the search
// reports nothing downloaded. Letting the automatic search finish first closes
// that window.
func (m *Manager) settleSonarrAutoRedownload(
	ctx context.Context,
	client clients.SonarrClient,
	instanceName string,
	target sonarrRedownloadTarget,
	failAt time.Time,
) (*arrCommand, redownloadState, error) {
	budget := m.autoSearchWaitBudget()
	initial, err := client.GetSeriesEpisodesContext(ctx, &sonarr.GetEpisode{EpisodeIDs: target.episodeIDs})
	if err != nil {
		return nil, redownloadProceed, fmt.Errorf("snapshot Sonarr repair targets: %w", err)
	}
	baseline := map[int64]int64{}
	for _, ep := range initial {
		if ep != nil {
			baseline[ep.ID] = ep.EpisodeFileID
			found := false
			for _, n := range target.seasonNumbers {
				if n == int64(ep.SeasonNumber) {
					found = true
				}
			}
			if !found {
				target.seasonNumbers = append(target.seasonNumbers, int64(ep.SeasonNumber))
			}
		}
	}
	var auto *arrCommand
	if budget > 0 {
		auto = m.detectSonarrAutoSearch(ctx, client, target, failAt)
		if auto != nil {
			// Snapshot all matching active commands before waiting; a pack may
			// have separate replacement searches for several subsets.
			list, listErr := sonarrCommands(ctx, client)
			if listErr != nil {
				return nil, redownloadProceed, listErr
			}
			matches := findSonarrAutoCommands(list, target, failAt)
			if len(matches) == 0 {
				matches = []*arrCommand{auto}
			}
			deadline := time.Now().Add(budget)
			for _, cmd := range matches {
				remaining := time.Until(deadline)
				if remaining <= 0 {
					return nil, redownloadProceed, fmt.Errorf("sonarr automatic replacement still active; deferring repair")
				}
				last, waitErr := waitCommandTerminal(ctx, sonarrCommandGetter(client), cmd.ID, remaining)
				if waitErr != nil {
					return nil, redownloadProceed, waitErr
				}
				if !commandIsTerminal(last) {
					return nil, redownloadProceed, fmt.Errorf("sonarr automatic replacement %d still active; deferring repair", cmd.ID)
				}
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, redownloadProceed, err
	}

	queue, err := client.GetQueueContext(ctx, 0, 500)
	if err != nil {
		return nil, redownloadProceed, fmt.Errorf("refresh Sonarr queue before repair: %w", err)
	}
	episodes, err := client.GetSeriesEpisodesContext(ctx, &sonarr.GetEpisode{EpisodeIDs: target.episodeIDs})
	if err != nil {
		return nil, redownloadProceed, fmt.Errorf("refresh Sonarr episodes before repair: %w", err)
	}
	queued, replaced, present := map[int64]bool{}, map[int64]bool{}, map[int64]bool{}
	for _, row := range queue.Records {
		if model.SonarrQueueIsReplacement(row, target.downloadID) {
			queued[row.EpisodeID] = true
		}
	}
	for _, ep := range episodes {
		if ep == nil {
			continue
		}
		present[ep.ID] = true
		old := baseline[ep.ID]
		replaced[ep.ID] = ep.EpisodeFileID > 0 && ep.EpisodeFileID != old && ep.EpisodeFileID != target.episodeFileID
	}
	allCovered, allReplaced := len(target.episodeIDs) > 0, len(target.episodeIDs) > 0
	for _, id := range target.episodeIDs {
		if !present[id] {
			return nil, redownloadProceed, fmt.Errorf("sonarr omitted episode %d; deferring repair", id)
		}
		allCovered = allCovered && (queued[id] || replaced[id])
		allReplaced = allReplaced && replaced[id]
	}
	if allReplaced {
		return nil, redownloadSatisfied, nil
	}
	if allCovered {
		return nil, redownloadHandled, nil
	}
	if target.episodeFileID > 0 && !anyEpisodeLinkedTo(episodes, target.episodeFileID) {
		return auto, redownloadProceed, nil
	}

	if target.episodeFileID > 0 {
		if err := m.deleteSonarrEpisodeFile(ctx, client, instanceName, target.episodeFileID); err != nil {
			return nil, redownloadProceed, err
		}
		if budget := m.fileDeleteConfirmBudget(); budget > 0 && !waitEpisodeFileCleared(ctx, client, target.episodeIDs, budget, target.episodeFileID) {
			return nil, redownloadProceed, fmt.Errorf("sonarr still links the removed file; deferring search")
		}
	}

	return auto, redownloadProceed, nil
}

func (m *Manager) detectSonarrAutoSearch(
	ctx context.Context,
	client clients.SonarrClient,
	target sonarrRedownloadTarget,
	failAt time.Time,
) *arrCommand {

	deadline := time.Now().Add(autoSearchDetectWindow)
	for {
		list, err := sonarrCommands(ctx, client)
		if err != nil {
			slog.DebugContext(ctx, "Failed to list Sonarr commands while detecting the automatic redownload search", "error", err)
		} else if matches := findSonarrAutoCommands(list, target, failAt); len(matches) > 0 {
			return matches[0]
		}

		if time.Now().After(deadline) {
			return nil
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(autoSearchDetectInterval):
		}
	}
}

func (m *Manager) deleteSonarrEpisodeFile(ctx context.Context, client clients.SonarrClient, instanceName string, episodeFileID int64) error {
	if episodeFileID <= 0 {
		return nil
	}
	err := client.DeleteEpisodeFileContext(ctx, episodeFileID)
	var request *starr.ReqError
	if errors.As(err, &request) && request.Code == 404 {
		return nil
	}
	if err != nil {
		return fmt.Errorf("delete original Sonarr episode file: %w", err)
	}
	return nil
}

// sendSonarrSearch issues the targeted replacement search and retries once if
// Sonarr collapsed it into an existing command.
func (m *Manager) sendSonarrSearch(ctx context.Context, client clients.SonarrClient, instanceName string, searchIDs []int64, auto *arrCommand) (*arrCommand, error) {
	body := sonarrSearchBody{Name: "EpisodeSearch", EpisodeIDs: searchIDs, Trigger: manualTrigger}

	resp, err := sendSearchCommandWithTrigger(ctx, client, body)
	if err != nil {
		return nil, fmt.Errorf("failed to trigger Sonarr episode search: %w", err)
	}

	budget := m.autoSearchWaitBudget()
	if budget <= 0 || !commandWasDeduplicated(resp, auto) {
		return resp, nil
	}

	slog.InfoContext(ctx, "Sonarr merged the repair search into a running command, retrying once it finishes",
		"instance", instanceName, "command_id", resp.ID, "trigger", resp.Trigger)
	last, waitErr := waitCommandTerminal(ctx, sonarrCommandGetter(client), resp.ID, budget)
	if waitErr != nil {
		return nil, waitErr
	}
	if !commandIsTerminal(last) {
		return nil, fmt.Errorf("sonarr search is still active; deferring duplicate submission")
	}

	resp, err = sendSearchCommandWithTrigger(ctx, client, body)
	if err != nil {
		return nil, fmt.Errorf("failed to re-trigger Sonarr episode search: %w", err)
	}
	return resp, nil
}

func sonarrCommandGetter(client clients.SonarrClient) commandGetter {
	return func(ctx context.Context, id int64) (*arrCommand, error) {
		return sonarrCommandByID(ctx, client, id)
	}
}

func anyEpisodeLinkedTo(episodes []*sonarr.Episode, episodeFileID int64) bool {
	for _, ep := range episodes {
		if ep != nil && ep.EpisodeFileID == episodeFileID {
			return true
		}
	}
	return false
}

type radarrRedownloadTarget struct {
	movieID     int64
	movieFileID int64
}

// settleRadarrAutoRedownload mirrors settleSonarrAutoRedownload. Radarr's delay
// specification guards the lazy file reference so it does not throw, but its
// automatic post-blocklist search is still delay-gated and still absorbs the
// repair search through command deduplication.
func (m *Manager) settleRadarrAutoRedownload(
	ctx context.Context,
	client *radarr.Radarr,
	instanceName string,
	target radarrRedownloadTarget,
	failAt time.Time,
) (*arrCommand, redownloadState, error) {
	budget := m.autoSearchWaitBudget()
	if budget <= 0 {
		m.deleteRadarrMovieFile(ctx, client, instanceName, target)
		return nil, redownloadProceed, nil
	}

	baseline := snapshotMovieFileID(ctx, client, target.movieID)

	auto := m.detectRadarrAutoSearch(ctx, client, target, failAt)
	if auto != nil {
		slog.InfoContext(ctx, "Radarr queued its own redownload search after the blocklist, waiting for it to finish",
			"instance", instanceName, "command_id", auto.ID, "command", auto.Name, "trigger", auto.Trigger)
		if _, err := waitCommandTerminal(ctx, radarrCommandGetter(client), auto.ID, budget); err != nil {
			return nil, redownloadProceed, err
		}
	}

	if queue, err := client.GetQueueContext(ctx, 0, 500); err != nil {
		slog.WarnContext(ctx, "Failed to read Radarr queue while checking for an automatic replacement",
			"instance", instanceName, "error", err)
	} else if queueHasMovie(queue, target.movieID) {
		slog.InfoContext(ctx, "Radarr's automatic redownload already grabbed a replacement, skipping delete and search",
			"instance", instanceName, "movie_id", target.movieID)
		return nil, redownloadHandled, nil
	}

	movie, err := client.GetMovieByIDContext(ctx, target.movieID)
	if err != nil {
		slog.WarnContext(ctx, "Failed to re-read the Radarr movie after the automatic redownload search",
			"instance", instanceName, "movie_id", target.movieID, "error", err)
	} else {
		current := movieFileID(movie)
		if current != 0 && current != baseline {
			slog.InfoContext(ctx, "Radarr's automatic redownload already imported a replacement file, skipping repair",
				"instance", instanceName, "movie_id", target.movieID, "movie_file_id", current)
			return nil, redownloadSatisfied, nil
		}
		if target.movieFileID > 0 && current != target.movieFileID {
			return auto, redownloadProceed, nil
		}
	}

	if target.movieFileID > 0 {
		m.deleteRadarrMovieFile(ctx, client, instanceName, target)
		waitMovieFileCleared(ctx, client, target.movieID, m.fileDeleteConfirmBudget())
	}

	return auto, redownloadProceed, nil
}

func (m *Manager) detectRadarrAutoSearch(
	ctx context.Context,
	client *radarr.Radarr,
	target radarrRedownloadTarget,
	failAt time.Time,
) *arrCommand {
	targets := map[string][]int64{"movieIds": {target.movieID}}

	deadline := time.Now().Add(autoSearchDetectWindow)
	for {
		list, err := radarrCommands(ctx, client)
		if err != nil {
			slog.DebugContext(ctx, "Failed to list Radarr commands while detecting the automatic redownload search", "error", err)
		} else if cmd := findAutoRedownloadCommand(list, []string{"MoviesSearch"}, targets, failAt); cmd != nil {
			return cmd
		}

		if time.Now().After(deadline) {
			return nil
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(autoSearchDetectInterval):
		}
	}
}

func (m *Manager) deleteRadarrMovieFile(ctx context.Context, client *radarr.Radarr, instanceName string, target radarrRedownloadTarget) {
	if target.movieFileID <= 0 {
		return
	}
	if err := client.DeleteMovieFilesContext(ctx, target.movieFileID); err != nil {
		slog.WarnContext(ctx, "Failed to delete movie file from Radarr, continuing with search",
			"instance", instanceName, "movie_id", target.movieID, "file_id", target.movieFileID, "error", err)
	}
}

// sendRadarrSearch issues the targeted replacement search and retries once if
// Radarr collapsed it into an existing command.
func (m *Manager) sendRadarrSearch(ctx context.Context, client *radarr.Radarr, instanceName string, movieID int64, auto *arrCommand) (*arrCommand, error) {
	body := radarrSearchBody{Name: "MoviesSearch", MovieIDs: []int64{movieID}, Trigger: manualTrigger}

	resp, err := sendSearchCommandWithTrigger(ctx, client, body)
	if err != nil {
		return nil, fmt.Errorf("failed to trigger Radarr search for movie ID %d: %w", movieID, err)
	}

	budget := m.autoSearchWaitBudget()
	if budget <= 0 || !commandWasDeduplicated(resp, auto) {
		return resp, nil
	}

	slog.InfoContext(ctx, "Radarr merged the repair search into a running command, retrying once it finishes",
		"instance", instanceName, "command_id", resp.ID, "trigger", resp.Trigger)
	if _, err := waitCommandTerminal(ctx, radarrCommandGetter(client), resp.ID, budget); err != nil {
		return nil, err
	}

	resp, err = sendSearchCommandWithTrigger(ctx, client, body)
	if err != nil {
		return nil, fmt.Errorf("failed to re-trigger Radarr search for movie ID %d: %w", movieID, err)
	}
	return resp, nil
}

func radarrCommandGetter(client *radarr.Radarr) commandGetter {
	return func(ctx context.Context, id int64) (*arrCommand, error) {
		return radarrCommandByID(ctx, client, id)
	}
}

func snapshotMovieFileID(ctx context.Context, client *radarr.Radarr, movieID int64) int64 {
	movie, err := client.GetMovieByIDContext(ctx, movieID)
	if err != nil {
		return 0
	}
	return movieFileID(movie)
}

func queueHasMovie(queue *radarr.Queue, movieID int64) bool {
	if queue == nil {
		return false
	}
	for _, record := range queue.Records {
		if record != nil && record.MovieID == movieID {
			return true
		}
	}
	return false
}
