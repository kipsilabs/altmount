package worker

import (
	"context"
	"fmt"
	"github.com/kipsilabs/altmount/internal/arrs/clients"
	"log/slog"
	"strings"

	"github.com/kipsilabs/altmount/internal/arrs/failures"
	"github.com/kipsilabs/altmount/internal/arrs/model"
	"github.com/kipsilabs/altmount/internal/arrs/registrar"
	"github.com/kipsilabs/altmount/internal/config"
)

// HandleImportFailure runs the importer-side failure breaker for one
// permanently failed *arr-originated download. It closes the re-grab loop the
// queue-cleanup breaker can never see: AltMount fast-fails an import (e.g. dead
// articles), reports Failed via the SABnzbd API, and the *arr's failed-download
// handling blocklists only that one release and instantly re-searches — finding
// the same dead release on the next indexer, forever.
//
// While the failed download is still in the *arr's queue (it stays there until
// the *arr's next completed-download poll), the owning instance is found by
// downloadID, every stable target in the download (episode/movie/album/book) is
// counted against the shared failure tracker, and targets at the
// queue_cleanup_max_failures threshold are given up on: unmonitored first, then
// the queue record is removed with blocklist-without-re-search — so the *arr
// neither auto-re-searches nor re-grabs the target.
//
// Races: if the *arr's poll wins and the queue record is already gone, the
// count was still recorded and the next failure trips the give-up. Below the
// threshold nothing but counting happens — the *arr's normal
// blocklist-and-re-search failure handling is desirable for transient bad
// releases.
func (w *Worker) HandleImportFailure(ctx context.Context, downloadID, category string) {
	if downloadID == "" {
		return
	}

	cfg := w.configGetter()
	if cfg.Arrs.Enabled == nil || !*cfg.Arrs.Enabled {
		return
	}
	w.retryPendingSonarrActions(ctx)
	maxFailures := cfg.Arrs.QueueCleanupMaxFailures
	if maxFailures <= 0 {
		return
	}

	// Prefer instances whose category matches the download's; fall back to the
	// rest. The downloadID match below is the authoritative filter — category
	// only avoids needless queue fetches.
	all := w.instances.GetAllInstances()
	ordered := make([]*model.ConfigInstance, 0, len(all))
	for _, inst := range all {
		if inst != nil && inst.Enabled && category != "" && strings.EqualFold(inst.Category, category) {
			ordered = append(ordered, inst)
		}
	}
	for _, inst := range all {
		if inst != nil && inst.Enabled && (category == "" || !strings.EqualFold(inst.Category, category)) {
			ordered = append(ordered, inst)
		}
	}

	for _, instance := range ordered {
		var (
			handled bool
			err     error
		)
		switch instance.Type {
		case "radarr":
			handled, err = w.importFailureRadarr(ctx, instance, downloadID, maxFailures)
		case "sonarr":
			handled, err = w.importFailureSonarr(ctx, instance, downloadID, maxFailures, false)
		case "whisparr":
			handled, err = w.importFailureSonarr(ctx, instance, downloadID, maxFailures, true)
		case "lidarr":
			handled, err = w.importFailureLidarr(ctx, instance, downloadID, maxFailures)
		case "readarr":
			handled, err = w.importFailureReadarr(ctx, instance, downloadID, maxFailures)
		default:
			continue
		}
		if err != nil {
			slog.WarnContext(ctx, "Import-failure breaker: failed to inspect instance queue",
				"instance", instance.Name, "type", instance.Type, "download_id", downloadID, "error", err)
			continue
		}
		if handled {
			return
		}
	}
}

// importFailureSonarr counts a failed import against every episode of the
// download in a Sonarr/Whisparr queue and, for episodes at the threshold,
// unmonitors them and removes the download with blocklist-without-re-search.
// Returns handled=true when the download was found in this instance's queue.
func (w *Worker) importFailureSonarr(ctx context.Context, instance *model.ConfigInstance, downloadID string, maxFailures int, whisparr bool) (bool, error) {
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
		return false, fmt.Errorf("failed to get Sonarr client: %w", err)
	}
	groups, err := client.QueueGroups(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to get Sonarr queue: %w", err)
	}
	found := false
	for _, group := range groups {
		if group.Record == nil || !strings.EqualFold(group.Record.DownloadID, downloadID) || !registrar.IsAltmountDownloadClient(group.Record.DownloadClient) {
			continue
		}
		found = true
		if _, err := w.actOnSonarrPack(ctx, instance, client, group, config.StuckActionBlocklist, maxFailures, true); err != nil {
			return true, err
		}
	}
	return found, nil
}

// importFailureRadarr is the Radarr counterpart of importFailureSonarr.
func (w *Worker) importFailureRadarr(ctx context.Context, instance *model.ConfigInstance, downloadID string, maxFailures int) (bool, error) {
	client, err := w.clients.GetOrCreateRadarrClient(instance.Name, instance.URL, instance.APIKey)
	if err != nil {
		return false, fmt.Errorf("failed to get Radarr client: %w", err)
	}
	queue, err := client.GetQueueContext(ctx, 0, 500)
	if err != nil {
		return false, fmt.Errorf("failed to get Radarr queue: %w", err)
	}

	for _, q := range queue.Records {
		if !strings.EqualFold(q.DownloadID, downloadID) || !registrar.IsAltmountDownloadClient(q.DownloadClient) {
			continue
		}
		if q.MovieID > 0 && w.bumpBreaker(failures.MovieKey(instance.Name, q.MovieID)) >= maxFailures {
			slog.WarnContext(ctx, "Failure threshold reached on import failure, giving up (unmonitor + blocklist without re-search)",
				"instance", instance.Name, "title", q.Title, "movie_id", q.MovieID, "max_failures", maxFailures)
			if err := unmonitorRadarrMovie(client, q.MovieID)(ctx); err != nil {
				slog.WarnContext(ctx, "Failed to unmonitor movie after failure threshold",
					"instance", instance.Name, "movie_id", q.MovieID, "error", err)
			}
			w.deleteStarrQueue(ctx, instance, []stuckAction{{ID: q.ID, Action: config.StuckActionBlocklist}}, client.DeleteQueueContext)
		} else {
			slog.InfoContext(ctx, "Import failure counted toward failure limit",
				"instance", instance.Name, "download_id", downloadID, "max_failures", maxFailures)
		}
		return true, nil
	}
	return false, nil
}

// importFailureLidarr counts failed imports per album. Lidarr has no unmonitor
// wrapper (matching the queue-cleanup breaker), so the give-up is
// blocklist-without-re-search only.
func (w *Worker) importFailureLidarr(ctx context.Context, instance *model.ConfigInstance, downloadID string, maxFailures int) (bool, error) {
	client, err := w.clients.GetOrCreateLidarrClient(instance.Name, instance.URL, instance.APIKey)
	if err != nil {
		return false, fmt.Errorf("failed to get Lidarr client: %w", err)
	}
	queue, err := client.GetQueueContext(ctx, 0, 500)
	if err != nil {
		return false, fmt.Errorf("failed to get Lidarr queue: %w", err)
	}

	for _, q := range queue.Records {
		if !strings.EqualFold(q.DownloadID, downloadID) || !registrar.IsAltmountDownloadClient(q.DownloadClient) {
			continue
		}
		if q.AlbumID > 0 && w.bumpBreaker(failures.AlbumKey(instance.Name, q.AlbumID)) >= maxFailures {
			slog.WarnContext(ctx, "Failure threshold reached on import failure, giving up (blocklist without re-search)",
				"instance", instance.Name, "title", q.Title, "album_id", q.AlbumID, "max_failures", maxFailures)
			w.deleteStarrQueue(ctx, instance, []stuckAction{{ID: q.ID, Action: config.StuckActionBlocklist}}, client.DeleteQueueContext)
		} else {
			slog.InfoContext(ctx, "Import failure counted toward failure limit",
				"instance", instance.Name, "download_id", downloadID, "max_failures", maxFailures)
		}
		return true, nil
	}
	return false, nil
}

// importFailureReadarr counts failed imports per book; give-up is
// blocklist-without-re-search only (no unmonitor wrapper, like Lidarr).
func (w *Worker) importFailureReadarr(ctx context.Context, instance *model.ConfigInstance, downloadID string, maxFailures int) (bool, error) {
	client, err := w.clients.GetOrCreateReadarrClient(instance.Name, instance.URL, instance.APIKey)
	if err != nil {
		return false, fmt.Errorf("failed to get Readarr client: %w", err)
	}
	queue, err := client.GetQueueContext(ctx, 0, 500)
	if err != nil {
		return false, fmt.Errorf("failed to get Readarr queue: %w", err)
	}

	for _, q := range queue.Records {
		if !strings.EqualFold(q.DownloadID, downloadID) || !registrar.IsAltmountDownloadClient(q.DownloadClient) {
			continue
		}
		if q.BookID > 0 && w.bumpBreaker(failures.BookKey(instance.Name, q.BookID)) >= maxFailures {
			slog.WarnContext(ctx, "Failure threshold reached on import failure, giving up (blocklist without re-search)",
				"instance", instance.Name, "title", q.Title, "book_id", q.BookID, "max_failures", maxFailures)
			w.deleteStarrQueue(ctx, instance, []stuckAction{{ID: q.ID, Action: config.StuckActionBlocklist}}, client.DeleteQueueContext)
		} else {
			slog.InfoContext(ctx, "Import failure counted toward failure limit",
				"instance", instance.Name, "download_id", downloadID, "max_failures", maxFailures)
		}
		return true, nil
	}
	return false, nil
}
