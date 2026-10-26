package clients

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/kipsilabs/altmount/internal/arrs/model"
	"golift.io/starr"
	"golift.io/starr/sonarr"
)

type legacySonarr struct{ *sonarr.Sonarr }

func NewLegacySonarr(c *starr.Config) SonarrClient { return &legacySonarr{sonarr.New(c)} }
func (*legacySonarr) APIVersion() string           { return "v3" }
func (s *legacySonarr) QueueGroups(ctx context.Context) ([]model.SonarrDownloadGroup, error) {
	queue, err := s.GetQueueContext(ctx, 0, 1000)
	if err != nil {
		return nil, err
	}
	groups := []model.SonarrDownloadGroup{}
	index := map[string]int{}
	for _, r := range queue.Records {
		if r == nil {
			continue
		}
		g := model.SonarrDownloadGroup{Record: r}
		key := g.Key()
		// Invalid/missing IDs remain independent rather than collapsing unrelated rows.
		pos, ok := index[key]
		if !ok || strings.TrimSpace(r.DownloadID) == "" {
			pos = len(groups)
			groups = append(groups, g)
			index[key] = pos
		}
		groups[pos].EpisodeIDs = appendUniquePositive(groups[pos].EpisodeIDs, r.EpisodeID)
		groups[pos].QueueIDs = appendUniquePositive(groups[pos].QueueIDs, r.ID)
	}
	return groups, nil
}
func appendUniquePositive(ids []int64, id int64) []int64 {
	if id <= 0 {
		return ids
	}
	for _, v := range ids {
		if v == id {
			return ids
		}
	}
	return append(ids, id)
}
func removeSonarrDownload(ctx context.Context, g model.SonarrDownloadGroup, o model.SonarrRemovalOptions, remove func(context.Context, int64, *starr.QueueDeleteOpts) error) error {
	if err := o.Validate(); err != nil {
		return err
	}
	ids := g.QueueIDs
	if len(ids) == 0 && g.Record != nil {
		ids = []int64{g.Record.ID}
	}
	if len(ids) == 0 || ids[0] <= 0 {
		return fmt.Errorf("download has no valid queue identity")
	}
	// A single queue deletion reconciles all sibling records for the same tracked download.
	err := remove(ctx, ids[0], &starr.QueueDeleteOpts{RemoveFromClient: &o.RemoveFromClient, ChangeCategory: o.ChangeCategory, BlockList: o.Blocklist, SkipRedownload: !o.Search})
	var requestErr *starr.ReqError
	if errors.As(err, &requestErr) && requestErr.Code == 404 {
		return nil
	}
	return err
}
func (s *legacySonarr) RemoveDownload(ctx context.Context, g model.SonarrDownloadGroup, o model.SonarrRemovalOptions) error {
	return removeSonarrDownload(ctx, g, o, s.DeleteQueueContext)
}
