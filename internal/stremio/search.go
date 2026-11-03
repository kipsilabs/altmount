package stremio

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/kipsilabs/altmount/internal/newsnab"
	"github.com/kipsilabs/altmount/internal/prowlarr"
)

// SearchParams contains the query parameters for a Stremio stream search.
type SearchParams struct {
	Type         string   // "movie" or "series"
	IMDBID       string   // "tt1234567"
	Title        string   // "Arcane"
	TitleAliases []string // Known alternative titles from metadata.
	Season       int      // 1
	Episode      int      // 1
	TVDBID       string   // optional
	TimeoutMS    int      // e.g. 3500
	CustomUA     string   // optional
}

// CoordinatorConfig defines the search provider configuration.
type CoordinatorConfig struct {
	Provider        string                  `yaml:"provider" mapstructure:"provider" json:"provider"`                      // "prowlarr", "newsnab", "both"
	UserAgentMode   string                  `yaml:"user_agent_mode" mapstructure:"user_agent_mode" json:"user_agent_mode"` // "auto", "custom"
	CustomUserAgent string                  `yaml:"custom_user_agent" mapstructure:"custom_user_agent" json:"custom_user_agent"`
	ProwlarrHost    string                  `yaml:"prowlarr_host" mapstructure:"prowlarr_host" json:"prowlarr_host"`
	ProwlarrKey     string                  `yaml:"prowlarr_key" mapstructure:"prowlarr_key" json:"prowlarr_key"`
	ProwlarrCats    []int                   `yaml:"prowlarr_categories" mapstructure:"prowlarr_categories" json:"prowlarr_categories"`
	ProwlarrIdxs    []int                   `yaml:"prowlarr_indexers" mapstructure:"prowlarr_indexers" json:"prowlarr_indexers"`
	NewsnabIndexers []newsnab.IndexerConfig `yaml:"newsnab_indexers" mapstructure:"newsnab_indexers" json:"newsnab_indexers"`
	Scoring         StreamScoringConfig     `yaml:"scoring" mapstructure:"scoring" json:"scoring"`
	// PreferredIndexers are indexer ids or names (legacy Prowlarr settings)
	// whose releases rank ahead of every other indexer's.
	PreferredIndexers []string `yaml:"preferred_indexers" mapstructure:"preferred_indexers" json:"preferred_indexers,omitempty"`
}

// SearchCoordinator coordinates multi-provider indexer searches and ranking.
type SearchCoordinator struct {
	prowlarrClient *prowlarr.Client
	newsnabClients []*newsnab.Client
	uaManager      *UserAgentManager
	config         CoordinatorConfig
	httpClient     *http.Client
}

// newsnabClientPool caches newsnab clients per indexer configuration so the
// in-process caps cache and the learned identifier-support negative cache
// survive across search requests. A coordinator is created per stream request;
// without the pool every request would re-fetch caps and replay the full
// identifier degradation ladder against indexers that cannot resolve
// identifiers, burning most of the search budget before the title pass runs.
var newsnabClientPool sync.Map

func cachedNewsnabClient(cfg newsnab.IndexerConfig, httpClient *http.Client) *newsnab.Client {
	key := fmt.Sprintf("%s|%s|%s|%s|%d|%v", cfg.ID, cfg.Name, cfg.URL, cfg.APIKey, cfg.TimeoutSeconds, cfg.Categories)
	if v, ok := newsnabClientPool.Load(key); ok {
		return v.(*newsnab.Client)
	}
	client := newsnab.NewClient(cfg, httpClient)
	actual, _ := newsnabClientPool.LoadOrStore(key, client)
	return actual.(*newsnab.Client)
}

// NewSearchCoordinator creates a new search coordinator.
func NewSearchCoordinator(cfg CoordinatorConfig, httpClient *http.Client) *SearchCoordinator {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}

	var prowlarrClient *prowlarr.Client
	if cfg.ProwlarrHost != "" && cfg.ProwlarrKey != "" {
		prowlarrClient = prowlarr.NewClient(cfg.ProwlarrHost, cfg.ProwlarrKey, httpClient)
	}

	newsnabClients := make([]*newsnab.Client, 0, len(cfg.NewsnabIndexers))
	for _, nCfg := range cfg.NewsnabIndexers {
		if nCfg.Enabled && nCfg.URL != "" {
			newsnabClients = append(newsnabClients, cachedNewsnabClient(nCfg, httpClient))
		}
	}

	return &SearchCoordinator{
		prowlarrClient: prowlarrClient,
		newsnabClients: newsnabClients,
		uaManager:      GetUserAgentManager(),
		config:         cfg,
		httpClient:     httpClient,
	}
}

// SearchInspectResult contains evaluation diagnostics for all releases found during an indexer query.
type SearchInspectResult struct {
	TotalResults     int             `json:"total_results"`
	ActiveResults    int             `json:"active_results"`
	DiscardedResults int             `json:"discarded_results"`
	Releases         []ScoredRelease `json:"releases"`
}

// Search executes concurrent queries across all enabled providers, deduplicates and ranks results.
func (sc *SearchCoordinator) Search(ctx context.Context, params SearchParams) ([]ScoredRelease, error) {
	inspect, err := sc.SearchInspect(ctx, params)
	if err != nil {
		return nil, err
	}

	// Filter down to active (non-excluded) releases only
	active := make([]ScoredRelease, 0, inspect.ActiveResults)
	for _, rel := range inspect.Releases {
		if !rel.Excluded {
			active = append(active, rel)
		}
	}
	return active, nil
}

// searchQuery is one indexer query dispatched during a search fan-out. Each
// query runs in its own goroutine; results are merged under a shared mutex.
type searchQuery struct {
	// label identifies the query in diagnostics, e.g. "prowlarr movie id".
	label string
	// indexer names the specific indexer for per-indexer queries; empty for
	// aggregators like Prowlarr that report their own indexer per result.
	indexer string
	// run performs the query and returns provider-agnostic results.
	run func(context.Context) ([]SearchResult, error)
}

// runSearchQueries dispatches every query concurrently and returns the merged
// results. A query that fails is logged and contributes nothing; one bad
// indexer never fails the whole search.
func runSearchQueries(ctx context.Context, queries []searchQuery) []SearchResult {
	var (
		wg         sync.WaitGroup
		mu         sync.Mutex
		aggregated []SearchResult
	)

	for _, q := range queries {
		q := q
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := q.run(ctx)
			if err != nil {
				slog.WarnContext(ctx, "Indexer search failed",
					"query", q.label, "indexer", q.indexer, "error", err)
				return
			}
			slog.DebugContext(ctx, "Indexer search returned results",
				"query", q.label, "indexer", q.indexer, "count", len(res))
			if len(res) == 0 {
				return
			}
			mu.Lock()
			aggregated = append(aggregated, res...)
			mu.Unlock()
		}()
	}

	wg.Wait()
	return aggregated
}

// newsnabMovieCategories are the Newznab movie categories queried when falling
// back to a free-text title search, which carries no category of its own.
var newsnabMovieCategories = []int{2000, 2010, 2030, 2040, 2045, 2060}

// idQueries builds the ID-based queries (IMDb / TVDB) for the enabled providers.
// ID searches are the precise form and are always tried first.
func (sc *SearchCoordinator) idQueries(params SearchParams, provider, userAgent string) []searchQuery {
	var queries []searchQuery
	isMovie := strings.EqualFold(params.Type, "movie")

	if (provider == "prowlarr" || provider == "both") && sc.prowlarrClient != nil {
		if isMovie {
			if params.IMDBID != "" {
				queries = append(queries, searchQuery{label: "prowlarr movie imdb", run: func(ctx context.Context) ([]SearchResult, error) {
					return toSearchResults(sc.prowlarrClient.Search(ctx, params.IMDBID, "movie", sc.config.ProwlarrCats, sc.config.ProwlarrIdxs, 0, 0))
				}})
			}
		} else {
			if params.TVDBID != "" {
				queries = append(queries, searchQuery{label: "prowlarr tv tvdb", run: func(ctx context.Context) ([]SearchResult, error) {
					return toSearchResults(sc.prowlarrClient.SearchByTVDB(ctx, params.TVDBID, "tvsearch", sc.config.ProwlarrCats, sc.config.ProwlarrIdxs, params.Season, params.Episode))
				}})
			}
			if params.IMDBID != "" {
				queries = append(queries, searchQuery{label: "prowlarr tv imdb", run: func(ctx context.Context) ([]SearchResult, error) {
					return toSearchResults(sc.prowlarrClient.Search(ctx, params.IMDBID, "tvsearch", sc.config.ProwlarrCats, sc.config.ProwlarrIdxs, params.Season, params.Episode))
				}})
			}
		}
	}

	if (provider == "newsnab" || provider == "both") && len(sc.newsnabClients) > 0 {
		for _, client := range sc.newsnabClients {
			c := client
			if isMovie {
				if params.IMDBID != "" {
					queries = append(queries, searchQuery{label: "newsnab movie imdb", indexer: c.Name(), run: func(ctx context.Context) ([]SearchResult, error) {
						return toSearchResults(c.SearchMovie(ctx, params.IMDBID, params.Title, nil, userAgent))
					}})
				}
			} else if params.IMDBID != "" || params.TVDBID != "" {
				queries = append(queries, searchQuery{label: "newsnab tv id", indexer: c.Name(), run: func(ctx context.Context) ([]SearchResult, error) {
					return toSearchResults(c.SearchTV(ctx, params.IMDBID, params.TVDBID, "", params.Season, params.Episode, nil, userAgent))
				}})
			}
		}
	}

	return queries
}

// titleQueries builds the free-text title queries for the enabled providers.
// These are a fallback: they are broader, and running them alongside the ID
// searches would double every indexer's request volume on every request.
func (sc *SearchCoordinator) titleQueries(params SearchParams, provider, userAgent string) []searchQuery {
	if params.Title == "" {
		return nil
	}

	var queries []searchQuery
	isMovie := strings.EqualFold(params.Type, "movie")
	searchType := "tvsearch"
	if isMovie {
		searchType = "movie"
	}

	if (provider == "prowlarr" || provider == "both") && sc.prowlarrClient != nil {
		season, episode := params.Season, params.Episode
		if isMovie {
			season, episode = 0, 0
		}
		queries = append(queries, searchQuery{label: "prowlarr title", run: func(ctx context.Context) ([]SearchResult, error) {
			return toSearchResults(sc.prowlarrClient.SearchByQuery(ctx, params.Title, searchType, sc.config.ProwlarrCats, sc.config.ProwlarrIdxs, season, episode))
		}})
	}

	if (provider == "newsnab" || provider == "both") && len(sc.newsnabClients) > 0 {
		for _, client := range sc.newsnabClients {
			c := client
			if isMovie {
				queries = append(queries, searchQuery{label: "newsnab movie title", indexer: c.Name(), run: func(ctx context.Context) ([]SearchResult, error) {
					return toSearchResults(c.SearchGeneral(ctx, params.Title, newsnabMovieCategories, userAgent))
				}})
			} else {
				queries = append(queries, searchQuery{label: "newsnab tv title", indexer: c.Name(), run: func(ctx context.Context) ([]SearchResult, error) {
					return toSearchResults(c.SearchTV(ctx, "", "", params.Title, params.Season, params.Episode, nil, userAgent))
				}})
			}
		}
	}

	return queries
}

// dedupeResults removes duplicate releases, keying on download URL first and
// then on the release's full provider identity.
func dedupeResults(aggregated []SearchResult) []SearchResult {
	unique := make([]SearchResult, 0, len(aggregated))
	seenURLs := make(map[string]struct{}, len(aggregated))
	seenReleases := make(map[string]struct{}, len(aggregated))

	for _, res := range aggregated {
		if res.DownloadURL != "" {
			if _, exists := seenURLs[res.DownloadURL]; exists {
				continue
			}
			seenURLs[res.DownloadURL] = struct{}{}
		}
		identity := res.Source + "|" + res.IndexerID + "|" + res.GUID + "|" + res.DownloadURL
		if _, exists := seenReleases[identity]; exists {
			continue
		}
		seenReleases[identity] = struct{}{}

		unique = append(unique, res)
	}

	return unique
}

// mediaMismatchReason reports why a release does not belong to the requested
// media, or "" when it matches (or when no title is available to check).
// Releases the indexer matched via an identifier query (imdbid/tvdbid) skip
// the gate: the indexer resolved the ID itself, so release names differing
// from the metadata title (foreign/alternative titles like tt23648788
// "Contraataque" vs "Counterattack") are still valid — the same trust model
// Prowlarr/Sonarr/Radarr apply to identifier matches.
func mediaMismatchReason(res SearchResult, params SearchParams) string {
	if params.Title == "" || res.ByIDSearch {
		return ""
	}
	switch {
	case strings.EqualFold(params.Type, "series"):
		for _, title := range append([]string{params.Title}, params.TitleAliases...) {
			if strings.TrimSpace(title) != "" && MatchesSeries(res.Title, title, params.Season, params.Episode, 0) {
				return ""
			}
		}
		return "Does not match requested series or episode"
	case strings.EqualFold(params.Type, "movie"):
		if !MatchesMovie(res.Title, params.Title, 0) {
			return "Does not match requested movie title"
		}
	}
	return ""
}

// SearchInspect queries all enabled providers, evaluates every release against
// scoring/exclusion rules, and returns both active and discarded releases with
// diagnostic reasons.
//
// ID-based searches run concurrently first; the broader free-text title
// searches are dispatched only when the ID pass came back empty, so a typical
// request costs one round of indexer queries rather than two.
func (sc *SearchCoordinator) SearchInspect(ctx context.Context, params SearchParams) (*SearchInspectResult, error) {
	timeout := time.Duration(params.TimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = 5000 * time.Millisecond
	}

	searchCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	userAgent := sc.uaManager.GetUserAgent(params.Type, sc.config.CustomUserAgent)
	if params.CustomUA != "" {
		userAgent = params.CustomUA
	}

	provider := strings.ToLower(sc.config.Provider)
	if provider == "" {
		provider = "both"
	}

	slog.DebugContext(searchCtx, "Stremio search starting",
		"provider", provider,
		"has_prowlarr", sc.prowlarrClient != nil,
		"newsnab_count", len(sc.newsnabClients),
		"type", params.Type,
		"title", params.Title,
		"imdb_id", params.IMDBID)

	indexerWeights := make(map[string]int)
	for _, n := range sc.config.NewsnabIndexers {
		if n.Weight != 0 {
			indexerWeights[n.Name] = n.Weight
			indexerWeights[n.ID] = n.Weight
		}
	}

	aggregated := runSearchQueries(searchCtx, sc.idQueries(params, provider, userAgent))
	tagProwlarrProvenance(aggregated, true)

	// Check if ID search yielded any active (non-excluded) results
	hasActiveIDResults := false
	for _, rel := range dedupeResults(aggregated) {
		eval := EvaluateRelease(rel.Title, &sc.config.Scoring)
		if !eval.Excluded && mediaMismatchReason(rel, params) == "" {
			hasActiveIDResults = true
			break
		}
	}

	if !hasActiveIDResults {
		if titleQueries := sc.titleQueries(params, provider, userAgent); len(titleQueries) > 0 {
			slog.DebugContext(searchCtx, "ID search returned no valid matching results, falling back to title search",
				"title", params.Title)
			titleResults := runSearchQueries(searchCtx, titleQueries)
			tagProwlarrProvenance(titleResults, false)
			aggregated = append(aggregated, titleResults...)
		}
	}

	uniqueResults := dedupeResults(aggregated)

	activeList := make([]ScoredRelease, 0, len(uniqueResults))
	discardedList := make([]ScoredRelease, 0, len(uniqueResults))

	for _, rel := range uniqueResults {
		eval := EvaluateRelease(rel.Title, &sc.config.Scoring)
		eval.SearchResult = rel

		if reason := mediaMismatchReason(rel, params); reason != "" {
			eval.Excluded = true
			if eval.ExcludeReason != "" {
				eval.ExcludeReason = reason + "; " + eval.ExcludeReason
			} else {
				eval.ExcludeReason = reason
			}
		}

		if eval.Excluded {
			discardedList = append(discardedList, eval)
		} else {
			applyIndexerBonus(&eval, rel, indexerWeights)
			applyPreferredIndexerBonus(&eval, rel, sc.config.PreferredIndexers)
			activeList = append(activeList, eval)
		}
	}

	sortByScoreDesc(activeList)
	sortByDateDesc(discardedList)

	allEvaluated := slices.Concat(activeList, discardedList)

	return &SearchInspectResult{
		TotalResults:     len(allEvaluated),
		ActiveResults:    len(activeList),
		DiscardedResults: len(discardedList),
		Releases:         allEvaluated,
	}, nil
}

// toSearchResults adapts a provider-specific result slice (and its error) into
// the common SearchResult form, so every query in a fan-out has one signature.
func toSearchResults[T prowlarr.NZBResult | newsnab.Result](res []T, err error) ([]SearchResult, error) {
	if err != nil {
		return nil, err
	}
	out := make([]SearchResult, 0, len(res))
	for _, r := range res {
		switch v := any(r).(type) {
		case prowlarr.NZBResult:
			out = append(out, prowlarrToSearchResult(v))
		case newsnab.Result:
			out = append(out, newsnabToSearchResult(v))
		}
	}
	return out, nil
}

// tagProwlarrProvenance marks Prowlarr results with the query form that
// produced them (identifier vs free-text pass). Newsnab results are skipped:
// their provenance is set per query inside the client itself, which knows
// when caps forced an identifier query to degrade into a keyword one.
func tagProwlarrProvenance(res []SearchResult, byID bool) {
	for i := range res {
		if res[i].Source == "prowlarr" {
			res[i].ByIDSearch = byID
		}
	}
}

func prowlarrToSearchResult(r prowlarr.NZBResult) SearchResult {
	return SearchResult{
		Title:       r.Title,
		DownloadURL: r.DownloadURL,
		Size:        r.Size,
		PublishDate: r.PublishDate,
		Indexer:     r.Indexer,
		IndexerID:   fmt.Sprintf("%d", r.IndexerID),
		Source:      "prowlarr",
		GUID:        r.GUID,
	}
}

func newsnabToSearchResult(r newsnab.Result) SearchResult {
	return SearchResult{
		Title:       r.Title,
		DownloadURL: r.DownloadURL,
		Size:        r.Size,
		PublishDate: r.PublishDate,
		Indexer:     r.Indexer,
		IndexerID:   r.IndexerID,
		GUID:        r.GUID,
		Source:      "newsnab",
		ByIDSearch:  r.ByIDSearch,
	}
}
