package model

import (
	"fmt"
	"strings"

	"golift.io/starr/sonarr"
)

// SonarrDownloadGroup retains all episode targets and source queue identities.
// Optional pack hints deliberately distinguish absent metadata from false/zero.
type SonarrDownloadGroup struct {
	Record                 *sonarr.QueueRecord
	EpisodeIDs             []int64
	QueueIDs               []int64
	SeasonNumbers          []int
	IsFullSeason           *bool
	EpisodesWithFilesCount *int
}

// Key is instance-local; callers must also include their instance identity.
func (g SonarrDownloadGroup) Key() string {
	if g.Record == nil {
		return ""
	}
	if strings.TrimSpace(g.Record.DownloadID) == "" {
		return fmt.Sprintf("queue:%d", g.Record.ID)
	}
	return fmt.Sprintf("%d:%q:%q", g.Record.SeriesID, strings.ToLower(strings.TrimSpace(g.Record.DownloadClient)), strings.ToLower(strings.TrimSpace(g.Record.DownloadID)))
}

type SonarrRemovalOptions struct {
	RemoveFromClient bool
	ChangeCategory   bool
	Blocklist        bool
	Search           bool
}

func (o SonarrRemovalOptions) Validate() error {
	if o.RemoveFromClient && o.ChangeCategory {
		return fmt.Errorf("remove-from-client and change-category are mutually exclusive")
	}
	if o.Search && !o.Blocklist {
		return fmt.Errorf("queue replacement search requires blocklisting")
	}
	return nil
}
