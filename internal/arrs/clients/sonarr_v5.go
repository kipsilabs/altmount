package clients

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"strings"

	"github.com/kipsilabs/altmount/internal/arrs/model"
	"golift.io/starr"
	"golift.io/starr/sonarr"
)

type nativeSonarr struct{ starr.APIer }

func newNativeSonarr(c *starr.Config) SonarrClient {
	return &nativeSonarr{sonarr.New(c).APIer}
}
func (*nativeSonarr) APIVersion() string { return "v5" }
func (s *nativeSonarr) GetSystemStatus() (*sonarr.SystemStatus, error) {
	return s.GetSystemStatusContext(context.Background())
}
func (s *nativeSonarr) GetQueue(records, perPage int) (*sonarr.Queue, error) {
	return s.GetQueueContext(context.Background(), records, perPage)
}

type nativeQueueRecord struct {
	sonarr.QueueRecord
	EpisodeIDs             []int64 `json:"episodeIds"`
	SeasonNumbers          []int   `json:"seasonNumbers"`
	IsFullSeason           *bool   `json:"isFullSeason"`
	EpisodesWithFilesCount *int    `json:"episodesWithFilesCount"`
}

func (s *nativeSonarr) queueGroups(ctx context.Context, perPage int) ([]model.SonarrDownloadGroup, error) {
	if perPage <= 0 {
		perPage = 1000
	}
	groups := []model.SonarrDownloadGroup{}
	seen := map[int64]bool{}
	count := 0
	for page := 1; ; page++ {
		var result struct {
			Page         int                 `json:"page"`
			TotalRecords int                 `json:"totalRecords"`
			Records      []nativeQueueRecord `json:"records"`
		}
		q := url.Values{"page": {starr.Str(page)}, "pageSize": {starr.Str(perPage)}, "includeUnknownSeriesItems": {"true"}, "sortKey": {"timeleft"}}
		if err := s.GetInto(ctx, starr.Request{URI: "/v5/queue", Query: q}, &result); err != nil {
			return nil, err
		}
		count += len(result.Records)
		for _, r := range result.Records {
			if r.ID > 0 && seen[r.ID] {
				continue
			}
			seen[r.ID] = true
			g := model.SonarrDownloadGroup{Record: &r.QueueRecord, QueueIDs: appendUniquePositive(nil, r.ID), SeasonNumbers: r.SeasonNumbers, IsFullSeason: r.IsFullSeason, EpisodesWithFilesCount: r.EpisodesWithFilesCount}
			for _, id := range r.EpisodeIDs {
				g.EpisodeIDs = appendUniquePositive(g.EpisodeIDs, id)
			}
			groups = append(groups, g)
		}
		if count >= result.TotalRecords || len(result.Records) == 0 || (result.Page > 0 && result.Page < page) {
			break
		}
	}
	return groups, nil
}
func (s *nativeSonarr) QueueGroups(ctx context.Context) ([]model.SonarrDownloadGroup, error) {
	return s.queueGroups(ctx, 1000)
}
func (s *nativeSonarr) GetQueueContext(ctx context.Context, records, perPage int) (*sonarr.Queue, error) {
	groups, err := s.queueGroups(ctx, perPage)
	if err != nil {
		return nil, err
	}
	q := &sonarr.Queue{}
	for _, g := range groups {
		if len(g.EpisodeIDs) == 0 {
			q.Records = append(q.Records, g.Record)
			continue
		}
		for _, id := range g.EpisodeIDs {
			r := *g.Record
			r.EpisodeID = id
			q.Records = append(q.Records, &r)
		}
	}
	q.TotalRecords = len(q.Records)
	q.PageSize = q.TotalRecords
	return q, nil
}
func (s *nativeSonarr) DeleteQueueContext(ctx context.Context, id int64, o *starr.QueueDeleteOpts) error {
	if id <= 0 {
		return fmt.Errorf("invalid queue identity")
	}
	remove := true
	category, blocklist, search := false, false, false
	if o != nil {
		if o.RemoveFromClient != nil {
			remove = *o.RemoveFromClient
		}
		category = o.ChangeCategory
		blocklist = o.BlockList
		search = blocklist && !o.SkipRedownload
	}
	opts := model.SonarrRemovalOptions{RemoveFromClient: remove, ChangeCategory: category, Blocklist: blocklist, Search: search}
	if err := opts.Validate(); err != nil {
		return err
	}
	removalMethod := "ignore"
	if remove {
		removalMethod = "removeFromClient"
	} else if category {
		removalMethod = "changeCategory"
	}
	blocklistMethod := "doNotBlocklist"
	if blocklist {
		blocklistMethod = "blocklistOnly"
		if search {
			blocklistMethod = "blocklistAndSearch"
		}
	}
	return s.DeleteAny(ctx, starr.Request{URI: path.Join("/v5/queue", starr.Str(id)), Query: url.Values{"removalMethod": {removalMethod}, "blocklistMethod": {blocklistMethod}}})
}
func (s *nativeSonarr) RemoveDownload(ctx context.Context, g model.SonarrDownloadGroup, o model.SonarrRemovalOptions) error {
	return removeSonarrDownload(ctx, g, o, s.DeleteQueueContext)
}

// nativeRequest explicitly adapts the two generic endpoints used by shared
// command/health helpers. Other legacy generic paths are rejected, never sent.
func nativeRequest(r starr.Request) (starr.Request, error) {
	uri := strings.TrimPrefix(strings.TrimPrefix(r.URI, "/"), "api/")
	if strings.HasPrefix(uri, "v5/") {
		r.URI = "/" + uri
		return r, nil
	}
	if uri == "health" || uri == "v3/health" {
		r.URI = "/v5/health"
		return r, nil
	}
	if uri == "v3/command" || strings.HasPrefix(uri, "v3/command/") {
		r.URI = "/v5/" + strings.TrimPrefix(uri, "v3/")
		return r, nil
	}
	return r, fmt.Errorf("unsupported generic native Sonarr endpoint %q", r.URI)
}
func (s *nativeSonarr) GetInto(ctx context.Context, r starr.Request, out any) error {
	r, err := nativeRequest(r)
	if err != nil {
		return err
	}
	return sanitizeSonarrError(s.APIer.GetInto(ctx, r, out))
}
func (s *nativeSonarr) PostInto(ctx context.Context, r starr.Request, out any) error {
	r, err := nativeRequest(r)
	if err != nil {
		return err
	}
	return sanitizeSonarrError(s.APIer.PostInto(ctx, r, out))
}
func (s *nativeSonarr) PutInto(ctx context.Context, r starr.Request, out any) error {
	r, err := nativeRequest(r)
	if err != nil {
		return err
	}
	return sanitizeSonarrError(s.APIer.PutInto(ctx, r, out))
}
func (s *nativeSonarr) DeleteAny(ctx context.Context, r starr.Request) error {
	r, err := nativeRequest(r)
	if err != nil {
		return err
	}
	return sanitizeSonarrError(s.APIer.DeleteAny(ctx, r))
}

// nativeParseResource is the part of Sonarr v5's ParseResource
// (GET /api/v5/parse) that callers read: the title and the episodes the title
// maps to. parsedEpisodeInfo, languages and customFormats are left out on
// purpose: v5 already reshaped parsedEpisodeInfo (seasonNumbers is a list),
// and decoding only what is read keeps a later change there from failing
// the parse.
type nativeParseResource struct {
	ID       int64             `json:"id"`
	Title    string            `json:"title"`
	Episodes []*sonarr.Episode `json:"episodes"`
}

// ParseContext asks Sonarr v5 which series and episodes a release name or path
// is (GET /api/v5/parse). The returned ParseOutput carries ID, Title and
// Episodes only; ParsedEpisodeInfo is always nil. A title Sonarr cannot map to
// a series gives no episodes, not an error.
func (s *nativeSonarr) ParseContext(ctx context.Context, input *sonarr.ParseInput) (*sonarr.ParseOutput, error) {
	if input == nil || (strings.TrimSpace(input.Title) == "" && strings.TrimSpace(input.Path) == "") {
		return nil, fmt.Errorf("%w: parse needs a title or a path", starr.ErrRequestError)
	}
	q := url.Values{}
	if input.Title != "" {
		q.Set("title", input.Title)
	}
	if input.Path != "" {
		q.Set("path", input.Path)
	}
	var res nativeParseResource
	req := starr.Request{URI: "/v5/parse", Query: q}
	if err := s.GetInto(ctx, req, &res); err != nil {
		return nil, fmt.Errorf("api.Get(%s): %w", &req, err)
	}
	return &sonarr.ParseOutput{ID: res.ID, Title: res.Title, Episodes: res.Episodes}, nil
}
