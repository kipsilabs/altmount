package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/kipsilabs/altmount/internal/arrs/clients"
	"github.com/kipsilabs/altmount/internal/arrs/model"
	"github.com/kipsilabs/altmount/internal/arrs/registrar"
	"github.com/kipsilabs/altmount/internal/config"
	"golift.io/starr"
	"golift.io/starr/sonarr"
)

const sonarrPendingLifetime = 24 * time.Hour
const sonarrCommandReconcileDelay = 30 * time.Second

// Each stage retains its original adapter and download identity. A successful
// removal is never repeated because a later state read or command submission failed.
type sonarrPendingAction struct {
	Instance             model.ConfigInstance
	Client               clients.SonarrClient
	Group                model.SonarrDownloadGroup
	GiveUp, Search       []int64
	Blocklist            bool
	Created              time.Time
	Unmonitored, Removed bool
	RemovalAttempted     bool
	CommandAttempted     time.Time
}

func (w *Worker) getSonarrPending(event string) *sonarrPendingAction {
	w.sonarrPendingMu.Lock()
	defer w.sonarrPendingMu.Unlock()
	return w.sonarrPending[event]
}
func (w *Worker) completeSonarrPending(event string) {
	w.sonarrPendingMu.Lock()
	delete(w.sonarrPending, event)
	w.sonarrPendingMu.Unlock()
}

func (w *Worker) retryPendingSonarrActions(ctx context.Context) {
	if ctx == nil {
		return
	}
	cfg := w.configGetter()
	if cfg.Arrs.Enabled == nil || !*cfg.Arrs.Enabled {
		return
	}
	w.sonarrPendingMu.Lock()
	events := make([]string, 0, len(w.sonarrPending))
	for event := range w.sonarrPending {
		events = append(events, event)
	}
	w.sonarrPendingMu.Unlock()
	for _, event := range events {
		if ctx.Err() != nil {
			return
		}
		unlock, err := w.breaker.LockAction(ctx, event)
		if err != nil {
			return
		}
		pending := w.getSonarrPending(event)
		if pending != nil {
			// A pending operation keeps its adapter/queue IDs, but must not keep
			// mutating an instance the user disabled, removed or repointed.
			currentMatches := false
			for _, current := range w.instances.GetAllInstances() {
				if current != nil && current.Enabled && current.Name == pending.Instance.Name && current.Type == pending.Instance.Type && current.URL == pending.Instance.URL && current.APIKey == pending.Instance.APIKey && current.APIVersion == pending.Instance.APIVersion {
					currentMatches = true
					break
				}
			}
			if !currentMatches {
				unlock()
				continue
			}

			if time.Since(pending.Created) > sonarrPendingLifetime {
				w.completeSonarrPending(event)
				slog.ErrorContext(ctx, "Sonarr pending recovery expired; unresolved targets require inspection", "instance", pending.Instance.Name, "download_id", pending.Group.Record.DownloadID, "episode_ids", pending.Search)
			} else {
				if _, err = w.resumeSonarrPendingLocked(ctx, event, pending); err != nil {
					slog.WarnContext(ctx, "Sonarr staged recovery deferred", "instance", pending.Instance.Name, "error", err)
				}
			}
		}
		unlock()
	}
}

// Caller holds the shared per-download action lock, including immediate execution.
func (w *Worker) resumeSonarrPendingLocked(ctx context.Context, event string, p *sonarrPendingAction) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if !p.Unmonitored {
		var ids []int64
		if len(p.GiveUp) > 0 {
			unresolved, err := unresolvedSonarrQueueEpisodes(ctx, p.Client, p.GiveUp)
			if err != nil {
				return false, err
			}
			ids = unresolved
		}
		if len(ids) > 0 {
			if _, err := p.Client.MonitorEpisodeContext(ctx, ids, false); err != nil {
				return false, fmt.Errorf("unmonitor exhausted pack episodes: %w", err)
			}
		}
		p.Unmonitored = true
	}
	if !p.Removed {
		// Reconcile an ambiguous prior DELETE from current queue state. The search
		// stage survives even if Sonarr already removed all original queue records.
		groups, err := p.Client.QueueGroups(ctx)
		if err != nil {
			return false, err
		}
		present := false
		for _, g := range groups {
			if g.Record != nil && g.Key() == p.Group.Key() {
				present = true
				break
			}
		}
		if !present {
			p.Removed = true
		} else {
			p.RemovalAttempted = true
			if err = p.Client.RemoveDownload(ctx, p.Group, model.SonarrRemovalOptions{RemoveFromClient: true, Blocklist: p.Blocklist}); err != nil {
				return false, err
			}
			p.Removed = true
		}
	}
	if len(p.Search) == 0 {
		w.completeSonarrPending(event)
		return true, nil
	}
	unresolved, err := unresolvedSonarrQueueEpisodes(ctx, p.Client, p.Search)
	if err != nil {
		return true, err
	}
	groups, err := p.Client.QueueGroups(ctx)
	if err != nil {
		return true, err
	}
	covered := map[int64]bool{}
	for _, g := range groups {
		if !model.SonarrQueueIsReplacement(g.Record, p.Group.Record.DownloadID) {
			continue
		}
		for _, id := range g.EpisodeIDs {
			covered[id] = true
		}
	}
	if !p.CommandAttempted.IsZero() {
		commands, readErr := p.Client.GetCommandsContext(ctx)
		if readErr != nil {
			return true, readErr
		}
		for _, command := range commands {
			if command == nil || !strings.EqualFold(command.Name, "EpisodeSearch") || !strings.EqualFold(command.Trigger, "manual") || command.Queued.Before(p.CommandAttempted.Add(-2*time.Second)) {
				continue
			}
			if strings.EqualFold(command.Status, "failed") || strings.EqualFold(command.Status, "aborted") {
				continue
			}
			for _, id := range sonarrCommandEpisodes(command.Body) {
				covered[id] = true
			}
		}
	}
	remaining := make([]int64, 0, len(unresolved))
	for _, id := range unresolved {
		if !covered[id] {
			remaining = append(remaining, id)
		}
	}
	if len(remaining) == 0 {
		w.completeSonarrPending(event)
		return true, nil
	}
	if !p.CommandAttempted.IsZero() && time.Since(p.CommandAttempted) < sonarrCommandReconcileDelay {
		return true, fmt.Errorf("waiting to reconcile uncertain Sonarr search submission")
	}
	payload, err := json.Marshal(struct {
		Name       string  `json:"name"`
		EpisodeIDs []int64 `json:"episodeIds"`
		Trigger    string  `json:"trigger"`
	}{"EpisodeSearch", remaining, "manual"})
	if err != nil {
		return true, err
	}
	if err = ctx.Err(); err != nil {
		return true, err
	}
	p.CommandAttempted = time.Now()
	var command sonarr.CommandResponse
	if err = p.Client.PostInto(ctx, starr.Request{URI: "v3/command", Body: bytes.NewReader(payload)}, &command); err != nil {
		return true, err
	}
	w.completeSonarrPending(event)
	return true, nil
}

func sonarrCommandEpisodes(body map[string]any) []int64 {
	encoded, _ := json.Marshal(body)
	var value struct {
		IDs []int64 `json:"episodeIds"`
		ID  int64   `json:"episodeId"`
	}
	if json.Unmarshal(encoded, &value) != nil {
		return nil
	}
	if value.ID > 0 {
		value.IDs = append(value.IDs, value.ID)
	}
	return value.IDs
}

// RepairSonarrDownload shares staged group recovery with scanner queue fallback.
func (w *Worker) RepairSonarrDownload(ctx context.Context, instanceName string, client clients.SonarrClient, group model.SonarrDownloadGroup) error {
	if group.Record == nil || !registrar.IsAltmountDownloadClient(group.Record.DownloadClient) {
		return fmt.Errorf("sonarr download is not owned by AltMount")
	}
	for _, instance := range w.instances.GetAllInstances() {
		if instance != nil && instance.Enabled && instance.Name == instanceName && (instance.Type == "sonarr" || instance.Type == "whisparr") {
			_, err := w.actOnSonarrPack(ctx, instance, client, group, config.StuckActionBlocklistSearch, w.configGetter().Arrs.QueueCleanupMaxFailures, false)
			return err
		}
	}
	return fmt.Errorf("enabled Sonarr instance %q not found", instanceName)
}
