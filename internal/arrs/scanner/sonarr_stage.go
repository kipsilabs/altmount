package scanner

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/kipsilabs/altmount/internal/arrs/clients"
)

// Retain target identity across deletion, failed reads and uncertain submissions.
// Access to a stage is serialized by the shared download lock.
type sonarrRepairStage struct {
	CleanupStarted   bool
	Created          time.Time
	Active           bool
	EpisodeIDs       []int64
	CommandAttempted time.Time
}

func (m *Manager) startSonarrRepairStage(key string) (*sonarrRepairStage, func(bool)) {
	m.sonarrStagesMu.Lock()
	if m.sonarrStages == nil {
		m.sonarrStages = map[string]*sonarrRepairStage{}
	}
	now := time.Now()
	for k, v := range m.sonarrStages {
		if !v.Active && now.Sub(v.Created) > 24*time.Hour {
			delete(m.sonarrStages, k)
		}
	}
	if len(m.sonarrStages) >= 10000 {
		oldest := ""
		var at time.Time
		for k, v := range m.sonarrStages {
			if !v.Active && (oldest == "" || v.Created.Before(at)) {
				oldest, at = k, v.Created
			}
		}
		if oldest != "" {
			delete(m.sonarrStages, oldest)
		}
	}
	stage := m.sonarrStages[key]
	if stage == nil {
		stage = &sonarrRepairStage{Created: now}
		m.sonarrStages[key] = stage
	}
	stage.Active = true
	m.sonarrStagesMu.Unlock()
	return stage, func(success bool) {
		m.sonarrStagesMu.Lock()
		stage.Active = false
		if success {
			delete(m.sonarrStages, key)
		}
		m.sonarrStagesMu.Unlock()
	}
}

// Accepted-but-unacknowledged searches are reconciled from command history.
// If the server has not exposed a definitive outcome yet, defer rather than
// immediately resubmitting the same manual search.
func reconcileSonarrSearch(ctx context.Context, client clients.SonarrClient, stage *sonarrRepairStage, ids []int64) ([]int64, error) {
	if stage.CommandAttempted.IsZero() {
		return ids, nil
	}
	commands, err := client.GetCommandsContext(ctx)
	if err != nil {
		return nil, err
	}
	covered := map[int64]bool{}
	for _, cmd := range commands {
		if cmd == nil || !strings.EqualFold(cmd.Name, "EpisodeSearch") || !strings.EqualFold(cmd.Trigger, manualTrigger) || cmd.Queued.Before(stage.CommandAttempted.Add(-2*time.Second)) {
			continue
		}
		if strings.EqualFold(cmd.Status, "failed") || strings.EqualFold(cmd.Status, "aborted") || strings.EqualFold(cmd.Status, "cancelled") {
			continue
		}
		for id := range bodyInt64Set(cmd.Body, "episodeIds") {
			covered[id] = true
		}
	}
	var remaining []int64
	for _, id := range ids {
		if !covered[id] {
			remaining = append(remaining, id)
		}
	}
	if len(remaining) > 0 && time.Since(stage.CommandAttempted) < 30*time.Second {
		return nil, fmt.Errorf("waiting to reconcile uncertain Sonarr episode search")
	}
	return remaining, nil
}
