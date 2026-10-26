package scanner

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/kipsilabs/altmount/internal/arrs/clients"
	"github.com/kipsilabs/altmount/internal/arrs/failures"
	"github.com/kipsilabs/altmount/internal/arrs/model"
	"golift.io/starr/sonarr"
)

func planSonarrPackRepair(snapshot model.SonarrRepairSnapshot) model.SonarrRepairPlan {
	var plan model.SonarrRepairPlan
	seen, files := map[int64]bool{}, map[int64]bool{}
	for _, ep := range snapshot.Episodes {
		if ep.ID <= 0 || seen[ep.ID] {
			continue
		}
		seen[ep.ID] = true
		switch {
		case !ep.Known:
			plan.Unknown = append(plan.Unknown, ep.ID)
		case ep.Healthy:
			plan.Satisfied = append(plan.Satisfied, ep.ID)
		case !ep.Monitored:
			plan.Ignored = append(plan.Ignored, ep.ID)
		case ep.Exhausted:
			plan.GiveUp = append(plan.GiveUp, ep.ID)
		case ep.ReplacementPending:
			plan.Covered = append(plan.Covered, ep.ID)
		default:
			plan.Search = append(plan.Search, ep.ID)
			if ep.FileID > 0 && !files[ep.FileID] {
				files[ep.FileID] = true
				plan.FileIDs = append(plan.FileIDs, ep.FileID)
			}
		}
	}
	return plan
}

// Recheck every target after automatic redownload: one replaced/queued episode
// cannot satisfy all siblings. New file identities are preserved.
func unresolvedSonarrRepairEpisodes(ctx context.Context, client clients.SonarrClient, ids []int64, brokenFileID int64, failedDownloadIDs ...string) ([]int64, error) {
	episodes, err := client.GetSeriesEpisodesContext(ctx, &sonarr.GetEpisode{EpisodeIDs: ids})
	if err != nil {
		return nil, fmt.Errorf("refresh Sonarr repair targets: %w", err)
	}
	queue, err := client.QueueGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("refresh Sonarr replacement coverage: %w", err)
	}
	failedID := ""
	if len(failedDownloadIDs) > 0 {
		failedID = failedDownloadIDs[0]
	}
	covered := map[int64]bool{}
	for _, group := range queue {
		if !model.SonarrQueueIsReplacement(group.Record, failedID) {
			continue
		}
		for _, id := range group.EpisodeIDs {
			covered[id] = true
		}
	}
	byID := map[int64]*sonarr.Episode{}
	for _, ep := range episodes {
		if ep != nil {
			byID[ep.ID] = ep
		}
	}
	var snapshot model.SonarrRepairSnapshot
	for _, id := range ids {
		state := model.SonarrEpisodeRepairState{ID: id}
		if ep := byID[id]; ep != nil {
			state.Known, state.Monitored, state.FileID = true, ep.Monitored, ep.EpisodeFileID
			// A different current file is a newer import. Its independent health
			// check owns it; never delete it as part of repairing the old file.
			state.Healthy = ep.EpisodeFileID > 0 && ep.EpisodeFileID != brokenFileID
			state.ReplacementPending = covered[id]
		}
		snapshot.Episodes = append(snapshot.Episodes, state)
	}
	plan := planSonarrPackRepair(snapshot)
	if len(plan.Unknown) > 0 {
		return nil, fmt.Errorf("sonarr omitted repair targets %v; deferring", plan.Unknown)
	}
	return plan.Search, nil
}

// Match command-specific identities. Series alone cannot identify a season
// search: another season of the same show must not delay or suppress repair.
func findSonarrAutoCommands(list []*arrCommand, target sonarrRedownloadTarget, since time.Time) []*arrCommand {
	var matches []*arrCommand
	for _, cmd := range list {
		if cmd == nil {
			continue
		}
		if strings.EqualFold(cmd.Name, "EpisodeSearch") {
			if findAutoRedownloadCommand([]*arrCommand{cmd}, []string{"EpisodeSearch"}, map[string][]int64{"episodeIds": target.episodeIDs}, since) != nil {
				matches = append(matches, cmd)
			}
			continue
		}
		if !strings.EqualFold(cmd.Name, "SeasonSearch") {
			continue
		}
		if findAutoRedownloadCommand([]*arrCommand{cmd}, []string{"SeasonSearch"}, map[string][]int64{"seriesId": {target.seriesID}}, since) == nil {
			continue
		}
		seasons := bodyInt64Set(cmd.Body, "seasonNumber")
		for _, season := range target.seasonNumbers {
			if _, ok := seasons[season]; ok {
				matches = append(matches, cmd)
				break
			}
		}
	}
	return matches
}

func (m *Manager) sonarrRepairEvent(ctx context.Context, instance, path string, fileID int64) string {
	if m.repo != nil && path != "" {
		if history, err := m.repo.GetImportHistoryByPath(ctx, path); err == nil && history != nil && history.DownloadID != nil && *history.DownloadID != "" {
			return failures.SonarrDownloadKey(instance, *history.DownloadID)
		}
	}
	return failures.SonarrDownloadKey(instance, fmt.Sprintf("file:%d:%s", fileID, path))
}

func (m *Manager) blocklistSonarrRepairOnce(ctx context.Context, client clients.SonarrClient, event string, seriesID, fileID int64, path string, ids []int64, scene string, sourceIDs ...string) error {
	finish, acquired := m.failures.BeginAction(event + "|blocklist")
	if !acquired {
		return nil
	}
	downloadID := failures.SonarrDownloadID(event)
	if len(sourceIDs) > 0 {
		downloadID = sourceIDs[0]
	}
	if strings.HasPrefix(downloadID, "file:") {
		downloadID = ""
	}
	stage, finishStage := m.startSonarrRepairStage(event + "|history-failure")
	err := m.blocklistSonarrEpisodeFile(ctx, client, seriesID, fileID, path, ids, scene, &stage.CommandAttempted, downloadID)
	finishStage(err == nil)
	finish(err == nil)
	return err
}

func (m *Manager) sonarrSourceDownloadID(ctx context.Context, path string, metadata *model.WebhookMetadata) string {
	if metadata != nil && strings.TrimSpace(metadata.DownloadID) != "" {
		return strings.TrimSpace(metadata.DownloadID)
	}
	if m.repo != nil && path != "" {
		if history, err := m.repo.GetImportHistoryByPath(ctx, path); err == nil && history != nil && history.DownloadID != nil {
			return strings.TrimSpace(*history.DownloadID)
		}
	}
	return ""
}
