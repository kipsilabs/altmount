package worker

import (
	"context"
	"fmt"
	"github.com/kipsilabs/altmount/internal/arrs/clients"
	"log/slog"
	"strings"
	"time"

	"github.com/kipsilabs/altmount/internal/arrs/failures"
	"github.com/kipsilabs/altmount/internal/arrs/model"
	"github.com/kipsilabs/altmount/internal/arrs/registrar"
	"github.com/kipsilabs/altmount/internal/config"
	"golift.io/starr"
	"golift.io/starr/radarr"
)

// stuckItem is a normalized view of an *arr queue record across all client types,
// holding only the fields the stuck-import detection needs.
type stuckItem struct {
	ConfirmGhost          func(context.Context) (bool, error)
	ObservationKey        string // stable grouped-download identity for grace tracking
	ID                    int64
	Title                 string
	TrackedDownloadStatus string
	TrackedDownloadState  string // empty when the *arr does not report it (e.g. Lidarr)
	DownloadClient        string
	OutputPath            string // download path; used for ghost detection (may be empty)
	Messages              []string

	// BreakerKey is a stable per-target identity (e.g. a Radarr movie or Sonarr
	// episode) that survives re-grabs of different releases for the same item. It
	// keys the failure circuit breaker. Empty when the *arr type does not support
	// the breaker, which disables it for that item.
	BreakerKey string
	// Unmonitor stops the *arr from automatically re-grabbing this target. It is
	// invoked once the breaker trips. Nil when the *arr type has no unmonitor
	// wrapper (the breaker still blocklists-without-search in that case).
	Unmonitor func(ctx context.Context) error
}

// CleanupStuckQueue scans every enabled *arr instance for items AltMount sent that
// are stuck importing for a known reason, then removes and blocklists them so the
// release is not grabbed again and the *arr searches for a replacement.
//
// An item is only acted on after it has been continuously observed stuck for the
// configured grace period (transient errors that the *arr resolves on its own are
// left alone); ghost/empty-folder items are removed grace-free. The periodic run is
// gated by IsQueueCleanupEnabled at the caller (the worker tick); this method itself
// only requires arrs to be enabled.
func (w *Worker) CleanupStuckQueue(ctx context.Context) error {
	cfg := w.configGetter()

	if cfg.Arrs.Enabled == nil || !*cfg.Arrs.Enabled {
		return nil
	}

	totalBlocked := 0
	for _, instance := range w.instances.GetAllInstances() {
		if instance == nil || !instance.Enabled {
			continue
		}

		var blocked int
		var err error
		switch instance.Type {
		case "radarr":
			blocked, err = w.cleanupStuckRadarr(ctx, instance, cfg)
		case "sonarr":
			blocked, err = w.cleanupStuckSonarr(ctx, instance, cfg, false)
		case "whisparr":
			blocked, err = w.cleanupStuckSonarr(ctx, instance, cfg, true)
		case "lidarr":
			blocked, err = w.cleanupStuckLidarr(ctx, instance, cfg)
		case "readarr":
			blocked, err = w.cleanupStuckReadarr(ctx, instance, cfg)
		case "sportarr":
			blocked, err = w.cleanupStuckSportarr(ctx, instance, cfg)
		default:
			continue
		}

		if err != nil {
			slog.WarnContext(ctx, "Failed to clean up stuck imports",
				"instance", instance.Name, "type", instance.Type, "error", err)
		}
		totalBlocked += blocked
	}

	if totalBlocked > 0 {
		slog.InfoContext(ctx, "Stuck import cleanup acted on releases", "count", totalBlocked)
	}
	return nil
}

// stuckAction is a queue item selected for cleanup plus how to act on it
// (one of the config.StuckAction* values).
type stuckAction struct {
	Ghost  bool
	ID     int64
	Action string
	// Unmonitor is set only when the failure breaker has tripped for this item: it
	// is invoked after the queue delete succeeds to stop the *arr re-grabbing the
	// target. Nil otherwise.
	Unmonitor func(ctx context.Context) error
}

// matchStuckRule returns the first enabled rule whose message matches any of the
// item's status messages (case-insensitive substring), or nil when none match.
func matchStuckRule(messages []string, rules []config.StuckCleanupRule) *config.StuckCleanupRule {
	if len(messages) == 0 {
		return nil
	}
	joined := strings.ToLower(strings.Join(messages, " "))
	for i := range rules {
		r := &rules[i]
		if !r.Enabled || r.Message == "" {
			continue
		}
		if strings.Contains(joined, strings.ToLower(r.Message)) {
			return r
		}
	}
	return nil
}

// stuckRuleFor returns the cleanup rule for an item, or nil if it is not stuck.
// An item must be flagged with a warning by the *arr and match an enabled rule.
func stuckRuleFor(item stuckItem, cfg *config.Config) *config.StuckCleanupRule {
	if !strings.EqualFold(item.TrackedDownloadStatus, "warning") {
		return nil
	}
	return matchStuckRule(item.Messages, cfg.Arrs.QueueCleanupRules)
}

// selectStuckActions filters AltMount-owned queue items to those that should be
// cleaned now, carrying each item's action. Ghost/empty-folder items (already
// imported, or source path gone) are removed first, grace-free. The remainder are
// matched against the message rules: an item must have been observed stuck for the
// configured grace period before it is acted on (or immediately when no grace period
// is configured). First observations and items the *arr has since resolved are
// tracked/cleared via the shared firstSeen map.
func (w *Worker) selectStuckActions(ctx context.Context, instance *model.ConfigInstance, cfg *config.Config, items []stuckItem) []stuckAction {
	var actions []stuckAction
	gracePeriod := time.Duration(cfg.Arrs.QueueCleanupGracePeriodMinutes) * time.Minute

	maxFailures := cfg.Arrs.QueueCleanupMaxFailures

	for _, item := range items {
		// Only ever touch items owned by AltMount's download client — other
		// clients may reference paths AltMount cannot see (see issue #523).
		if !registrar.IsAltmountDownloadClient(item.DownloadClient) {
			continue
		}

		key := fmt.Sprintf("stuck|%s|%d", instance.Name, item.ID)
		if item.ObservationKey != "" {
			key = "stuck|" + instance.Name + "|" + item.ObservationKey
		}
		clearFirstSeen := func() {
			w.firstSeenMu.Lock()
			delete(w.firstSeen, key)
			w.firstSeenMu.Unlock()
		}
		// emitAction records the cleanup action for this item, applying the failure
		// breaker, and clears its grace tracking.
		emitAction := func(baseAction string) {
			action, unmonitor := w.applyBreaker(ctx, item, baseAction, maxFailures, instance.Name)
			actions = append(actions, stuckAction{ID: item.ID, Action: action, Unmonitor: unmonitor})
			clearFirstSeen()
		}

		// Ghost detection runs before rule matching and is grace-free. A confirmed
		// healthy import (file is in the library per import history) both removes the
		// stale queue entry and resets the failure breaker for that target. A source
		// path that has gone away is also a ghost; isGhostByPathGone keeps its own
		// observation window via firstSeen. Ghosts are always removed (never
		// blocklisted) and never count toward the breaker — the release was not bad,
		// the queue entry is just stale.
		healthyGhost := w.checkGhostByImportHistory(ctx, item.OutputPath, cfg, instance.Name, item.Title)
		if healthyGhost && item.ConfirmGhost != nil {
			var err error
			healthyGhost, err = item.ConfirmGhost(ctx)
			if err != nil {
				continue
			}
		}
		if healthyGhost {
			w.resetBreaker(item.BreakerKey)
			actions = append(actions, stuckAction{ID: item.ID, Action: config.StuckActionRemove, Ghost: true})
			clearFirstSeen()
			continue
		}
		if w.isGhostByPathGone(ctx, item.OutputPath, item.ID, cfg, instance.Name, item.Title) {
			actions = append(actions, stuckAction{ID: item.ID, Action: config.StuckActionRemove, Ghost: true})
			clearFirstSeen()
			continue
		}

		rule := stuckRuleFor(item, cfg)
		if rule == nil {
			clearFirstSeen()
			continue
		}

		if gracePeriod <= 0 {
			emitAction(rule.Action)
			continue
		}

		w.firstSeenMu.Lock()
		seenTime, exists := w.firstSeen[key]
		if !exists {
			w.firstSeen[key] = time.Now()
			w.firstSeenMu.Unlock()
			slog.DebugContext(ctx, "First saw stuck import, starting grace period",
				"title", item.Title, "instance", instance.Name)
			continue
		}
		w.firstSeenMu.Unlock()

		if time.Since(seenTime) < gracePeriod {
			continue
		}

		emitAction(rule.Action)
	}

	return actions
}

// applyBreaker records one failure against the item's target and decides the
// final action. Below the configured threshold (or when the breaker is disabled
// or the item has no breaker key) it returns the rule's base action unchanged.
// Once the threshold is reached it escalates to a give-up: blocklist the release
// without re-searching and return the item's unmonitor closure so the *arr stops
// automatically re-grabbing a target that keeps failing.
func (w *Worker) applyBreaker(ctx context.Context, item stuckItem, baseAction string, maxFailures int, instanceName string) (string, func(ctx context.Context) error) {
	if maxFailures <= 0 || item.BreakerKey == "" {
		return baseAction, nil
	}

	failures := w.bumpBreaker(item.BreakerKey)
	if failures < maxFailures {
		return baseAction, nil
	}

	slog.WarnContext(ctx, "Failure threshold reached, giving up on target (blocklist without re-search + unmonitor)",
		"title", item.Title, "instance", instanceName, "failures", failures, "max_failures", maxFailures)
	return config.StuckActionBlocklist, item.Unmonitor
}

// starrDeleteOpts maps a stuck action to starr queue-delete options. The item is
// always removed from AltMount's download client. blocklist blocks the release;
// SkipRedownload is false only for blocklist_search so the *arr re-searches.
func starrDeleteOpts(action string) *starr.QueueDeleteOpts {
	removeFromClient := true
	blocklist := action == config.StuckActionBlocklist || action == config.StuckActionBlocklistSearch
	search := action == config.StuckActionBlocklistSearch
	return &starr.QueueDeleteOpts{
		RemoveFromClient: &removeFromClient,
		BlockList:        blocklist,
		SkipRedownload:   !search,
	}
}

// runUnmonitor invokes a give-up unmonitor closure (set only when the failure
// breaker tripped) after the queue item was removed, logging the outcome. A nil
// closure — breaker not tripped, or an *arr type without unmonitor support — is a
// no-op.
func (w *Worker) runUnmonitor(ctx context.Context, instance *model.ConfigInstance, a stuckAction) {
	if a.Unmonitor == nil {
		return
	}
	if err := a.Unmonitor(ctx); err != nil {
		slog.WarnContext(ctx, "Failed to unmonitor target after failure threshold",
			"instance", instance.Name, "id", a.ID, "error", err)
		return
	}
	slog.InfoContext(ctx, "Unmonitored target after repeated import failures",
		"instance", instance.Name, "id", a.ID)
}

// unmonitorRadarrMovie returns a closure that unmonitors a Radarr movie so the
// *arr stops automatically re-searching for it. Returns nil when there is no
// usable movie ID.
func unmonitorRadarrMovie(client *radarr.Radarr, movieID int64) func(ctx context.Context) error {
	if movieID <= 0 {
		return nil
	}
	return func(ctx context.Context) error {
		movie, err := client.GetMovieByIDContext(ctx, movieID)
		if err != nil {
			return err
		}
		if !movie.Monitored {
			return nil
		}
		movie.Monitored = false
		_, err = client.UpdateMovieContext(ctx, movieID, movie, false)
		return err
	}
}

// deleteStarrQueue runs the per-item starr delete (per its action) and counts
// successes, tolerating already-removed (404) items.
func (w *Worker) deleteStarrQueue(ctx context.Context, instance *model.ConfigInstance, actions []stuckAction, del func(ctx context.Context, id int64, opts *starr.QueueDeleteOpts) error) int {
	cleaned := 0
	for _, a := range actions {
		if err := del(ctx, a.ID, starrDeleteOpts(a.Action)); err != nil {
			if strings.Contains(err.Error(), "404") {
				slog.DebugContext(ctx, "Stuck queue item already removed", "instance", instance.Name, "id", a.ID)
				continue
			}
			slog.ErrorContext(ctx, "Failed to clean stuck queue item",
				"instance", instance.Name, "id", a.ID, "action", a.Action, "error", err)
			continue
		}
		cleaned++
		w.runUnmonitor(ctx, instance, a)
	}
	if cleaned > 0 {
		slog.InfoContext(ctx, "Cleaned stuck imports",
			"instance", instance.Name, "type", instance.Type, "count", cleaned)
	}
	return cleaned
}

// flattenStarrMessages collapses starr status messages (title + lines) into a flat
// slice for pattern matching.
func flattenStarrMessages(msgs []*starr.StatusMessage) []string {
	out := make([]string, 0, len(msgs))
	for _, m := range msgs {
		if m == nil {
			continue
		}
		if m.Title != "" {
			out = append(out, m.Title)
		}
		out = append(out, m.Messages...)
	}
	return out
}

func (w *Worker) cleanupStuckRadarr(ctx context.Context, instance *model.ConfigInstance, cfg *config.Config) (int, error) {
	client, err := w.clients.GetOrCreateRadarrClient(instance.Name, instance.URL, instance.APIKey)
	if err != nil {
		return 0, fmt.Errorf("failed to get Radarr client: %w", err)
	}
	queue, err := client.GetQueueContext(ctx, 0, 500)
	if err != nil {
		return 0, fmt.Errorf("failed to get Radarr queue: %w", err)
	}

	items := make([]stuckItem, 0, len(queue.Records))
	for _, q := range queue.Records {
		item := stuckItem{
			ID:                    q.ID,
			Title:                 q.Title,
			TrackedDownloadStatus: q.TrackedDownloadStatus,
			TrackedDownloadState:  q.TrackedDownloadState,
			DownloadClient:        q.DownloadClient,
			OutputPath:            q.OutputPath,
			Messages:              flattenStarrMessages(q.StatusMessages),
		}
		// Breaker target: the movie. Stable across re-grabs of different releases.
		if q.MovieID > 0 {
			item.BreakerKey = failures.MovieKey(instance.Name, q.MovieID)
			item.Unmonitor = unmonitorRadarrMovie(client, q.MovieID)
		}
		items = append(items, item)
	}

	actions := w.selectStuckActions(ctx, instance, cfg, items)
	return w.deleteStarrQueue(ctx, instance, actions, client.DeleteQueueContext), nil
}

// cleanupStuckSonarr handles Sonarr and Whisparr (both use the Sonarr client).
func (w *Worker) cleanupStuckSonarr(ctx context.Context, instance *model.ConfigInstance, cfg *config.Config, whisparr bool) (int, error) {
	var (
		client clients.SonarrClient
		err    error
	)
	if whisparr {
		client, err = w.clients.GetOrCreateWhisparrClient(instance.Name, instance.URL, instance.APIKey)
	} else {
		client, err = w.clients.GetOrCreateSonarrClientContext(ctx, instance.Name, instance.URL, instance.APIKey, instance.APIVersion)
	}
	if err != nil {
		return 0, fmt.Errorf("failed to get Sonarr client: %w", err)
	}
	groups, err := client.QueueGroups(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get Sonarr queue: %w", err)
	}
	items := make([]stuckItem, 0, len(groups))
	byID := map[int64]model.SonarrDownloadGroup{}
	for _, group := range groups {
		q := group.Record
		if q == nil {
			continue
		}
		items = append(items, stuckItem{ID: q.ID, Title: q.Title, TrackedDownloadStatus: q.TrackedDownloadStatus, TrackedDownloadState: q.TrackedDownloadState, DownloadClient: q.DownloadClient, OutputPath: q.OutputPath, Messages: flattenStarrMessages(q.StatusMessages), ObservationKey: group.Key(), ConfirmGhost: func(ctx context.Context) (bool, error) {
			return w.confirmSonarrPackImported(ctx, client, instance.Name, group.EpisodeIDs)
		}})
		byID[q.ID] = group
	}
	actions := w.selectStuckActions(ctx, instance, cfg, items)
	acted := 0
	for _, action := range actions {
		ok, err := w.actOnSonarrPack(ctx, instance, client, byID[action.ID], action.Action, cfg.Arrs.QueueCleanupMaxFailures, false, action.Ghost)
		if err != nil {
			return acted, err
		}
		if ok {
			acted++
		}
	}
	return acted, nil
}

func (w *Worker) cleanupStuckLidarr(ctx context.Context, instance *model.ConfigInstance, cfg *config.Config) (int, error) {
	client, err := w.clients.GetOrCreateLidarrClient(instance.Name, instance.URL, instance.APIKey)
	if err != nil {
		return 0, fmt.Errorf("failed to get Lidarr client: %w", err)
	}
	queue, err := client.GetQueueContext(ctx, 0, 500)
	if err != nil {
		return 0, fmt.Errorf("failed to get Lidarr queue: %w", err)
	}

	items := make([]stuckItem, 0, len(queue.Records))
	for _, q := range queue.Records {
		// Lidarr's queue record has no trackedDownloadState; leave it empty (it is
		// not used for gating — only trackedDownloadStatus and the rule match are).
		item := stuckItem{
			ID:                    q.ID,
			Title:                 q.Title,
			TrackedDownloadStatus: q.TrackedDownloadStatus,
			DownloadClient:        q.DownloadClient,
			OutputPath:            q.OutputPath,
			Messages:              flattenStarrMessages(q.StatusMessages),
		}
		// Breaker target: the album. Lidarr has no unmonitor wrapper yet, so the
		// breaker only forces blocklist-without-search once tripped.
		if q.AlbumID > 0 {
			item.BreakerKey = failures.AlbumKey(instance.Name, q.AlbumID)
		}
		items = append(items, item)
	}

	actions := w.selectStuckActions(ctx, instance, cfg, items)
	return w.deleteStarrQueue(ctx, instance, actions, client.DeleteQueueContext), nil
}

func (w *Worker) cleanupStuckReadarr(ctx context.Context, instance *model.ConfigInstance, cfg *config.Config) (int, error) {
	client, err := w.clients.GetOrCreateReadarrClient(instance.Name, instance.URL, instance.APIKey)
	if err != nil {
		return 0, fmt.Errorf("failed to get Readarr client: %w", err)
	}
	queue, err := client.GetQueueContext(ctx, 0, 500)
	if err != nil {
		return 0, fmt.Errorf("failed to get Readarr queue: %w", err)
	}

	items := make([]stuckItem, 0, len(queue.Records))
	for _, q := range queue.Records {
		item := stuckItem{
			ID:                    q.ID,
			Title:                 q.Title,
			TrackedDownloadStatus: q.TrackedDownloadStatus,
			TrackedDownloadState:  q.TrackedDownloadState,
			DownloadClient:        q.DownloadClient,
			OutputPath:            q.OutputPath,
			Messages:              flattenStarrMessages(q.StatusMessages),
		}
		// Breaker target: the book. Readarr has no unmonitor wrapper yet, so the
		// breaker only forces blocklist-without-search once tripped.
		if q.BookID > 0 {
			item.BreakerKey = failures.BookKey(instance.Name, q.BookID)
		}
		items = append(items, item)
	}

	actions := w.selectStuckActions(ctx, instance, cfg, items)
	return w.deleteStarrQueue(ctx, instance, actions, client.DeleteQueueContext), nil
}

// cleanupStuckSportarr mirrors the starr path but uses Sportarr's native client,
// which is not starr-compatible.
func (w *Worker) cleanupStuckSportarr(ctx context.Context, instance *model.ConfigInstance, cfg *config.Config) (int, error) {
	client, err := w.clients.GetOrCreateSportarrClient(instance.Name, instance.URL, instance.APIKey)
	if err != nil {
		return 0, fmt.Errorf("failed to get Sportarr client: %w", err)
	}
	queue, err := client.GetQueue(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get Sportarr queue: %w", err)
	}

	items := make([]stuckItem, 0, len(queue))
	for _, q := range queue {
		// Capture the indexer from Sportarr's native queue. Sportarr is not
		// starr-compatible, so AltMount cannot auto-register its Grab/Import webhook
		// (the path that supplies the indexer for Radarr/Sonarr/etc.). Its native queue
		// record is the only place the indexer is exposed, so persist it here against
		// the download ID — otherwise these imports show up in indexer health as
		// "Unknown".
		if q.DownloadID != "" && q.Indexer != "" {
			w.captureSportarrIndexer(ctx, q.DownloadID, q.Indexer)
		}

		var messages []string
		for _, m := range q.StatusMessages {
			if m.Title != "" {
				messages = append(messages, m.Title)
			}
			messages = append(messages, m.Messages...)
		}
		items = append(items, stuckItem{
			ID:                    q.ID,
			Title:                 q.Title,
			TrackedDownloadStatus: q.TrackedDownloadStatus,
			TrackedDownloadState:  q.TrackedDownloadState,
			DownloadClient:        q.DownloadClient.Name,
			OutputPath:            q.OutputPath,
			Messages:              messages,
		})
	}

	actions := w.selectStuckActions(ctx, instance, cfg, items)
	cleaned := 0
	for _, a := range actions {
		var err error
		// Sportarr's native API has no skipRedownload flag, so blocklist and
		// blocklist_search both map to a blocklisting delete.
		if a.Action == config.StuckActionBlocklist || a.Action == config.StuckActionBlocklistSearch {
			err = client.DeleteQueueItemBlocklist(ctx, a.ID)
		} else {
			err = client.DeleteQueueItem(ctx, a.ID)
		}
		if err != nil {
			if strings.Contains(err.Error(), "404") {
				slog.DebugContext(ctx, "Stuck queue item already removed from Sportarr", "instance", instance.Name, "id", a.ID)
				continue
			}
			slog.ErrorContext(ctx, "Failed to clean stuck Sportarr queue item",
				"instance", instance.Name, "id", a.ID, "action", a.Action, "error", err)
			continue
		}
		cleaned++
	}
	if cleaned > 0 {
		slog.InfoContext(ctx, "Cleaned stuck Sportarr imports", "instance", instance.Name, "count", cleaned)
	}
	return cleaned, nil
}
