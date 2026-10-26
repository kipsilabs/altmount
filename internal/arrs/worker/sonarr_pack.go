package worker

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/kipsilabs/altmount/internal/arrs/clients"
	"github.com/kipsilabs/altmount/internal/arrs/failures"
	"github.com/kipsilabs/altmount/internal/arrs/model"
	"github.com/kipsilabs/altmount/internal/config"
	"golift.io/starr/sonarr"
)

// Queue import failures concern unresolved targets. Already linked imports are
// preserved; health failures of imported files use the scanner's explicit file
// identity rather than inferring corruption from a stale queue warning.
func unresolvedSonarrQueueEpisodes(ctx context.Context, client clients.SonarrClient, ids []int64) ([]int64, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	episodes, err := client.GetSeriesEpisodesContext(ctx, &sonarr.GetEpisode{EpisodeIDs: ids})
	if err != nil {
		return nil, fmt.Errorf("refresh pack episodes: %w", err)
	}
	found := map[int64]*sonarr.Episode{}
	for _, ep := range episodes {
		if ep != nil {
			found[ep.ID] = ep
		}
	}
	var pending []int64
	seen := map[int64]bool{}
	for _, id := range ids {
		if id <= 0 || seen[id] {
			continue
		}
		seen[id] = true
		ep := found[id]
		if ep == nil {
			return nil, fmt.Errorf("sonarr omitted pack episode %d; deferring", id)
		}
		if ep.Monitored && ep.EpisodeFileID == 0 {
			pending = append(pending, id)
		}
	}
	return pending, nil
}

func sonarrGroupEvent(instance string, g model.SonarrDownloadGroup) string {
	if g.Record != nil && strings.TrimSpace(g.Record.DownloadID) != "" {
		return failures.SonarrDownloadKey(instance, g.Record.DownloadID)
	}
	return failures.SonarrDownloadKey(instance, g.Key())
}

// actOnSonarrPack performs a single download mutation with per-episode decisions.
// Import callbacks only remove a group when at least one target is exhausted;
// cleanup rules can act below the threshold according to their configured policy.
func (w *Worker) actOnSonarrPack(ctx context.Context, instance *model.ConfigInstance, client clients.SonarrClient, g model.SonarrDownloadGroup, action string, maxFailures int, onlyExhausted bool, ghost ...bool) (acted bool, err error) {
	if g.Record == nil {
		return false, nil
	}
	event := sonarrGroupEvent(instance.Name, g)
	unlock, err := w.breaker.LockAction(ctx, event)
	if err != nil {
		return false, err
	}
	defer unlock()
	if pending := w.getSonarrPending(event); pending != nil {
		return w.resumeSonarrPendingLocked(ctx, event, pending)
	}
	finish, acquired := w.breaker.BeginAction(event + "|queue")
	if !acquired {
		return false, nil
	}
	staged := false
	defer func() { finish(staged) }()
	if err = ctx.Err(); err != nil {
		return false, err
	}
	isGhost := len(ghost) > 0 && ghost[0]
	var unresolved []int64
	if !isGhost && (action != config.StuckActionRemove || onlyExhausted || maxFailures > 0) {
		unresolved, err = unresolvedSonarrQueueEpisodes(ctx, client, g.EpisodeIDs)
		if err != nil {
			return false, err
		}
	}
	var search, giveUp []int64
	for _, id := range unresolved {
		if maxFailures > 0 && w.breaker.BumpOnce(failures.EpisodeKey(instance.Name, id), event) >= maxFailures {
			giveUp = append(giveUp, id)
		} else {
			search = append(search, id)
		}
	}
	if onlyExhausted && len(giveUp) == 0 {
		return false, nil
	}
	blocklist := action == config.StuckActionBlocklist || action == config.StuckActionBlocklistSearch || len(giveUp) > 0
	if len(g.EpisodeIDs) > 0 && len(unresolved) == 0 {
		blocklist = false
	}
	if action != config.StuckActionBlocklistSearch && (!onlyExhausted || len(giveUp) == 0) {
		search = nil
	}
	instanceCopy := *instance
	pending := &sonarrPendingAction{Instance: instanceCopy, Client: client, Group: g, GiveUp: giveUp, Search: search, Blocklist: blocklist, Created: time.Now()}
	w.sonarrPendingMu.Lock()
	if w.sonarrPending == nil {
		w.sonarrPending = map[string]*sonarrPendingAction{}
	}
	w.sonarrPending[event] = pending
	w.sonarrPendingMu.Unlock()
	staged = true
	return w.resumeSonarrPendingLocked(ctx, event, pending)
}

// An imported first file cannot make an entire partially imported pack a ghost.
// Reset only independently confirmed imported targets; preserve sibling counters.
func (w *Worker) confirmSonarrPackImported(ctx context.Context, client clients.SonarrClient, instance string, ids []int64) (bool, error) {
	if len(ids) == 0 {
		return false, nil
	}
	episodes, err := client.GetSeriesEpisodesContext(ctx, &sonarr.GetEpisode{EpisodeIDs: ids})
	if err != nil {
		return false, err
	}
	byID := map[int64]*sonarr.Episode{}
	for _, ep := range episodes {
		if ep != nil {
			byID[ep.ID] = ep
		}
	}
	all := true
	for _, id := range ids {
		ep := byID[id]
		if ep == nil {
			return false, fmt.Errorf("sonarr omitted episode %d while confirming import", id)
		}
		if ep.EpisodeFileID > 0 {
			w.resetBreaker(failures.EpisodeKey(instance, id))
		} else {
			all = false
		}
	}
	return all, nil
}
