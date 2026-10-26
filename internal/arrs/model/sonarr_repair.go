package model

import (
	"golift.io/starr/sonarr"
	"strings"
)

// SonarrQueueIsReplacement requires an active, distinct download. A lingering
// failed source row or another failed import is not replacement coverage.
func SonarrQueueIsReplacement(row *sonarr.QueueRecord, failedDownloadID string) bool {
	if row == nil {
		return false
	}
	if failedDownloadID != "" && strings.EqualFold(strings.TrimSpace(row.DownloadID), strings.TrimSpace(failedDownloadID)) {
		return false
	}
	if row.ErrorMessage != "" || strings.EqualFold(row.TrackedDownloadStatus, "warning") || strings.EqualFold(row.TrackedDownloadStatus, "error") || strings.Contains(strings.ToLower(row.TrackedDownloadState), "failed") {
		return false
	}
	switch strings.ToLower(row.Status) {
	case "queued", "paused", "downloading", "completed", "delay":
		return true
	}
	return false
}

// SonarrEpisodeRepairState describes one refreshed target. Healthy must be
// supplied by the caller; a pack's aggregate imported count cannot establish it.
type SonarrEpisodeRepairState struct {
	ID, FileID                                               int64
	Known, Monitored, Healthy, ReplacementPending, Exhausted bool
}

type SonarrRepairSnapshot struct{ Episodes []SonarrEpisodeRepairState }

type SonarrRepairPlan struct {
	Satisfied, Covered, Search, GiveUp, Unknown, Ignored, FileIDs []int64
}
