package api

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/kipsilabs/altmount/internal/auth"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/httpclient"
	"github.com/kipsilabs/altmount/internal/stremio"
)

// InspectSearchRequest represents a payload to inspect/rank indexer search results in real time.
type InspectSearchRequest struct {
	Query     string                       `json:"query"`
	Type      string                       `json:"type"` // "movie" or "series"
	IMDbID    string                       `json:"imdb_id,omitempty"`
	TVDBID    string                       `json:"tvdb_id,omitempty"`
	Season    int                          `json:"season,omitempty"`
	Episode   int                          `json:"episode,omitempty"`
	TimeoutMS int                          `json:"timeout_ms,omitempty"`
	Scoring   *stremio.StreamScoringConfig `json:"scoring,omitempty"`
}

// resolveInspectTarget normalizes the request in place: Stremio content IDs
// (tt1234567:S:E) are split into their IMDb / season / episode parts and the
// stream type is inferred from them. It returns the free-text title to search
// with, which is empty when the caller only supplied an ID.
func resolveInspectTarget(req *InspectSearchRequest) string {
	rawTarget := req.IMDbID
	if rawTarget == "" && strings.HasPrefix(strings.ToLower(req.Query), "tt") {
		rawTarget = req.Query
	}

	if rawTarget != "" && strings.HasPrefix(strings.ToLower(rawTarget), "tt") {
		parsedIMDb, parsedSeason, parsedEpisode := parseStremioContentID(rawTarget)
		req.IMDbID = parsedIMDb
		if parsedSeason > 0 && req.Season == 0 {
			req.Season = parsedSeason
			req.Type = "series"
		}
		if parsedEpisode > 0 && req.Episode == 0 {
			req.Episode = parsedEpisode
			req.Type = "series"
		}
	}

	if req.Type == "" {
		req.Type = "movie"
	}

	searchTitle := req.Query
	if strings.HasPrefix(strings.ToLower(searchTitle), "tt") {
		searchTitle = ""
	}
	return searchTitle
}

// inspectMetadata is the canonical metadata resolved from an IMDb ID, used both
// to align the indexer query with addon stream behavior and to match local
// library files.
type inspectMetadata struct {
	TVDBID  string
	TmdbID  int
	Title   string
	Year    string
	Search  string
	Applied bool
}

// resolveInspectMetadata looks up canonical metadata for the request's IMDb ID.
// A provider outage is not fatal: the caller falls back to the supplied title.
func (s *Server) resolveInspectMetadata(ctx context.Context, req *InspectSearchRequest, searchTitle string) inspectMetadata {
	meta := inspectMetadata{Search: searchTitle}
	if req.IMDbID == "" {
		return meta
	}

	if req.Type == "series" {
		tvdbID, title, err := resolveSeriesMetadataFromIMDb(ctx, req.IMDbID, s.configManager.GetConfig().GetUserAgent())
		if err != nil {
			slog.WarnContext(ctx, "Failed to resolve series metadata from IMDb ID", "error", err, "imdb_id", req.IMDbID)
			return meta
		}
		meta.TVDBID = tvdbID
		meta.Title = title
		if req.TVDBID == "" && tvdbID != "" {
			req.TVDBID = tvdbID
		}
		if meta.Search == "" && title != "" {
			meta.Search = title
		}
		return meta
	}

	tmdbID, movieTitle, movieYear, err := resolveMovieMetadataFromIMDb(ctx, req.IMDbID, s.configManager.GetConfig().GetUserAgent())
	if err != nil {
		slog.WarnContext(ctx, "Failed to resolve movie metadata from IMDb ID", "error", err, "imdb_id", req.IMDbID)
		return meta
	}
	meta.TmdbID = tmdbID
	meta.Title = movieTitle
	meta.Year = movieYear
	if meta.Search == "" && movieTitle != "" {
		meta.Search = movieTitle
	}
	return meta
}

// libraryReleaseSink collects local library files as scored releases,
// deduplicating by release title and stamping the library source/boost so
// instant streams outrank remote results.
type libraryReleaseSink struct {
	seen        map[string]struct{}
	out         []stremio.ScoredRelease
	baseURL     string
	downloadKey string
}

func newLibraryReleaseSink(baseURL, downloadKey string, alreadySeen []stremio.ScoredRelease) *libraryReleaseSink {
	seen := make(map[string]struct{}, len(alreadySeen))
	for _, r := range alreadySeen {
		seen[normalizeReleaseTitleKey(r.Title)] = struct{}{}
	}
	return &libraryReleaseSink{seen: seen, baseURL: baseURL, downloadKey: downloadKey}
}

func (l *libraryReleaseSink) add(eval stremio.ScoredRelease, filePath string) {
	titleKey := normalizeReleaseTitleKey(eval.Title)
	if titleKey == "" {
		return
	}
	if _, dup := l.seen[titleKey]; dup {
		return
	}
	l.seen[titleKey] = struct{}{}
	eval.Score += libraryScoreBoost
	eval.Excluded = false
	eval.Source = "library"
	eval.Indexer = "Local Library"
	eval.IndexerID = "library"
	eval.DownloadURL = l.baseURL + "/api/files/stream?path=" + url.QueryEscape(filePath) +
		"&download_key=" + url.QueryEscape(l.downloadKey)
	l.out = append(l.out, eval)
}

// libraryMatchPath prefers the library path, which carries the human-readable
// name, over the raw storage path.
func libraryMatchPath(h *database.FileHealth) string {
	if h.LibraryPath != nil && *h.LibraryPath != "" {
		return *h.LibraryPath
	}
	return h.FilePath
}

// isPlayableLibraryFile reports whether a health record points at a real media
// file rather than a sample or non-media artifact.
func isPlayableLibraryFile(h *database.FileHealth) bool {
	return h != nil && h.FilePath != "" &&
		isMediaExtension(filepath.Ext(h.FilePath)) && !isSampleFile(h.FilePath)
}

// inspectLibraryReleases returns local library files matching the request as
// scored releases. downloadKey authenticates the generated stream URLs; when it
// is empty the lookup is skipped rather than emitting links that would 401.
func (s *Server) inspectLibraryReleases(
	ctx context.Context,
	req *InspectSearchRequest,
	meta inspectMetadata,
	scoring *stremio.StreamScoringConfig,
	baseURL string,
	downloadKey string,
	alreadySeen []stremio.ScoredRelease,
) []stremio.ScoredRelease {
	if downloadKey == "" || s.healthRepo == nil {
		return nil
	}

	sink := newLibraryReleaseSink(baseURL, downloadKey, alreadySeen)
	title := meta.Title
	if title == "" {
		title = meta.Search
	}

	if req.Type == "movie" {
		s.collectLibraryMovies(ctx, title, meta, scoring, sink)
	} else {
		s.collectLibraryEpisodes(ctx, title, meta, req, scoring, sink)
	}
	return sink.out
}

// collectLibraryMovies appends healthy library files matching a movie title.
func (s *Server) collectLibraryMovies(
	ctx context.Context,
	title string,
	meta inspectMetadata,
	scoring *stremio.StreamScoringConfig,
	sink *libraryReleaseSink,
) {
	healthyFiles, err := s.healthRepo.FindHealthyFilesForMovie(ctx, title, meta.Year, meta.TmdbID)
	if err != nil {
		slog.WarnContext(ctx, "Library lookup for movie failed", "error", err, "title", title)
		return
	}

	yearNum, _ := strconv.Atoi(meta.Year)
	for _, h := range healthyFiles {
		if !isPlayableLibraryFile(h) {
			continue
		}
		matchPath := libraryMatchPath(h)
		if title != "" &&
			!stremio.MatchesMovie(filepath.Base(matchPath), title, yearNum) &&
			!stremio.MatchesMovie(matchPath, title, yearNum) {
			continue
		}
		sink.add(stremio.EvaluateRelease(filepath.Base(matchPath), scoring), h.FilePath)
	}
}

// collectLibraryEpisodes appends healthy library files matching a series title
// and the requested season/episode.
func (s *Server) collectLibraryEpisodes(
	ctx context.Context,
	title string,
	meta inspectMetadata,
	req *InspectSearchRequest,
	scoring *stremio.StreamScoringConfig,
	sink *libraryReleaseSink,
) {
	tvdbID, _ := strconv.Atoi(meta.TVDBID)
	healthyFiles, err := s.healthRepo.FindHealthyFilesForSeries(ctx, title, tvdbID)
	if err != nil {
		slog.WarnContext(ctx, "Library lookup for series failed", "error", err, "title", title)
		return
	}

	matchCtx := releaseMatchContext{}
	if req.IMDbID != "" && s.configManager != nil {
		matchCtx.aliases = resolveSeriesTitleAliases(ctx, req.IMDbID, s.configManager.GetConfig().GetUserAgent())
	}
	selector := &stremioEpisodeSelector{Season: req.Season, Episode: req.Episode}
	for _, h := range healthyFiles {
		if !isPlayableLibraryFile(h) {
			continue
		}
		matchPath := libraryMatchPath(h)
		if !selector.matches(matchPath) && !selector.matches(h.FilePath) {
			continue
		}
		if title != "" &&
			!releaseMatchesContent("series", filepath.Base(matchPath), title, "", req.Season, req.Episode, matchCtx) &&
			!releaseMatchesContent("series", matchPath, title, "", req.Season, req.Episode, matchCtx) {
			continue
		}
		sink.add(stremio.EvaluateRelease(filepath.Base(matchPath), scoring), h.FilePath)
	}
}

// EvaluateReleaseRequest is the body of POST /api/stremio/scoring/evaluate.
type EvaluateReleaseRequest struct {
	Title   string                       `json:"title"`
	Scoring *stremio.StreamScoringConfig `json:"scoring,omitempty"`
}

// EvaluatedFormat is a custom format that matched, with the score it added.
type EvaluatedFormat struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Score int    `json:"score"`
}

// EvaluateReleaseResponse is how the stream ranking scores one release title.
type EvaluateReleaseResponse struct {
	Score            int               `json:"score"`
	Excluded         bool              `json:"excluded"`
	ExcludeReason    string            `json:"exclude_reason,omitempty"`
	MatchedLanguages []string          `json:"matched_languages"`
	LanguageBonus    int               `json:"language_bonus"`
	Formats          []EvaluatedFormat `json:"formats"`
}

// handleEvaluateStremioRelease handles POST /api/stremio/scoring/evaluate:
// scores one release title with the same scorer that ranks Stremio streams,
// against the stored rules or a draft from the request.
func (s *Server) handleEvaluateStremioRelease(c *fiber.Ctx) error {
	var req EvaluateReleaseRequest
	if err := c.BodyParser(&req); err != nil {
		return RespondBadRequest(c, "Invalid request body", err.Error())
	}
	req.Title = strings.TrimSpace(req.Title)
	if req.Title == "" {
		return RespondBadRequest(c, "Release title is required", "")
	}
	if err := validateInspectScoring(req.Scoring); err != nil {
		return RespondValidationError(c, "Invalid scoring configuration", err.Error())
	}

	scoring := s.buildStremioCoordinatorConfig(s.configManager.GetConfig()).Scoring
	if req.Scoring != nil {
		scoring = *req.Scoring
	}

	eval := stremio.EvaluateRelease(req.Title, &scoring)
	resp := EvaluateReleaseResponse{
		Score:            eval.Score,
		Excluded:         eval.Excluded,
		ExcludeReason:    eval.ExcludeReason,
		MatchedLanguages: eval.MatchedLanguages,
		LanguageBonus:    eval.LanguageBonus,
		Formats:          make([]EvaluatedFormat, 0, len(eval.MatchedFormats)),
	}
	for _, name := range eval.MatchedFormats {
		for _, f := range scoring.CustomFormats {
			if f.Name == name {
				resp.Formats = append(resp.Formats, EvaluatedFormat{ID: f.ID, Name: f.Name, Score: f.Score})
				break
			}
		}
	}
	return RespondSuccess(c, resp)
}

// handleInspectStremioSearch handles POST /api/stremio/search/inspect
func (s *Server) handleInspectStremioSearch(c *fiber.Ctx) error {
	ctx := c.Context()
	var req InspectSearchRequest
	if err := c.BodyParser(&req); err != nil {
		return RespondBadRequest(c, "Invalid request body", err.Error())
	}

	req.Query = strings.TrimSpace(req.Query)
	req.IMDbID = strings.TrimSpace(req.IMDbID)

	if req.Query == "" && req.IMDbID == "" {
		return RespondBadRequest(c, "Query title or IMDb ID is required", "")
	}
	if err := validateInspectScoring(req.Scoring); err != nil {
		return RespondValidationError(c, "Invalid scoring configuration", err.Error())
	}

	searchTitle := resolveInspectTarget(&req)
	meta := s.resolveInspectMetadata(ctx, &req, searchTitle)

	slog.DebugContext(ctx, "Inspect search executing",
		"imdb_id", req.IMDbID, "search_title", meta.Search, "type", req.Type)

	cfg := s.configManager.GetConfig()
	coordCfg := s.buildStremioCoordinatorConfig(cfg)

	// A draft scoring config in the request overrides the stored one so the
	// sandbox can evaluate unsaved rules.
	if req.Scoring != nil {
		coordCfg.Scoring = *req.Scoring
	}

	coordinator := stremio.NewSearchCoordinator(coordCfg, httpclient.NewForExternal(cfg.Network, 30*time.Second))

	var titleAliases []string
	if req.Type == "series" && req.IMDbID != "" {
		titleAliases = resolveSeriesTitleAliases(ctx, req.IMDbID, cfg.GetUserAgent())
	}
	res, err := coordinator.SearchInspect(ctx, stremio.SearchParams{
		Type:         req.Type,
		IMDBID:       req.IMDbID,
		Title:        meta.Search,
		TitleAliases: titleAliases,
		TVDBID:       req.TVDBID,
		Season:       req.Season,
		Episode:      req.Episode,
		TimeoutMS:    clampInspectTimeoutMS(req.TimeoutMS),
	})
	if err != nil {
		return RespondInternalError(c, "Failed to execute indexer search inspection", err.Error())
	}

	if cfg.Stremio.EffectiveIncludeLibraryStreams() {
		// Stream URLs are authenticated with the requesting user's own download
		// key — never another account's.
		downloadKey := ""
		if rawKey := s.getAPIKeyForConfig(c); rawKey != "" {
			downloadKey = auth.HashAPIKey(rawKey)
		}
		libraryReleases := s.inspectLibraryReleases(
			ctx, &req, meta, &coordCfg.Scoring,
			resolveBaseURL(c, cfg.Stremio.BaseURL), downloadKey, res.Releases,
		)
		if len(libraryReleases) > 0 {
			res.Releases = append(libraryReleases, res.Releases...)
			res.ActiveResults += len(libraryReleases)
			res.TotalResults += len(libraryReleases)
		}
	}

	// Indexer download URLs embed indexer credentials (e.g. Newznab apikey
	// query parameters). The inspector only surfaces diagnostics — it never
	// downloads — so strip everything after the path before responding.
	for i := range res.Releases {
		if res.Releases[i].Source != "library" {
			res.Releases[i].DownloadURL = redactExternalDownloadURL(res.Releases[i].DownloadURL)
		}
	}

	return RespondSuccess(c, res)
}

const (
	defaultInspectTimeoutMS = 5000
	maxInspectTimeoutMS     = 15000

	// libraryScoreBoost ranks local library files above any remote release so
	// instant streams always surface first in inspection results.
	libraryScoreBoost = 10000

	// maxInspectCustomFormats and maxInspectPatternLen bound the regex work a
	// single inspect request can trigger. Draft configs hold dozens of rules, so
	// these limits are far above any legitimate use while stopping a caller from
	// making one request compile thousands of large patterns.
	maxInspectCustomFormats = 512
	maxInspectPatternLen    = 1024
)

// clampInspectTimeoutMS bounds the client-supplied search timeout to keep each
// inspect request's indexer fan-out short-lived.
func clampInspectTimeoutMS(timeoutMS int) int {
	if timeoutMS <= 0 {
		return defaultInspectTimeoutMS
	}
	if timeoutMS > maxInspectTimeoutMS {
		return maxInspectTimeoutMS
	}
	return timeoutMS
}

// validateInspectScoring bounds and validates a caller-supplied draft scoring
// config. Every pattern here is compiled during evaluation, so the request must
// not be able to dictate unbounded regex work, and an invalid pattern is
// reported rather than silently never matching.
func validateInspectScoring(scoring *stremio.StreamScoringConfig) error {
	if scoring == nil {
		return nil
	}

	if len(scoring.CustomFormats) > maxInspectCustomFormats {
		return fmt.Errorf("custom_formats has %d entries, limit is %d",
			len(scoring.CustomFormats), maxInspectCustomFormats)
	}
	if len(scoring.ExcludeKeywords) > maxInspectCustomFormats {
		return fmt.Errorf("exclude_keywords has %d entries, limit is %d",
			len(scoring.ExcludeKeywords), maxInspectCustomFormats)
	}

	patterns := make([]string, 0, len(scoring.CustomFormats)+len(scoring.ExcludeKeywords)+1)
	for _, f := range scoring.CustomFormats {
		patterns = append(patterns, f.Pattern)
	}
	patterns = append(patterns, scoring.ExcludeKeywords...)
	patterns = append(patterns, scoring.ExcludeRegex)

	for _, p := range patterns {
		if len(p) > maxInspectPatternLen {
			return fmt.Errorf("pattern of %d bytes exceeds the %d byte limit",
				len(p), maxInspectPatternLen)
		}
	}

	if expr := strings.TrimSpace(scoring.ExcludeRegex); expr != "" {
		if _, err := regexp.Compile(expr); err != nil {
			return fmt.Errorf("exclude_regex is invalid: %w", err)
		}
	}
	for i, f := range scoring.CustomFormats {
		if !f.Enabled || f.PatternType == "token" || strings.TrimSpace(f.Pattern) == "" {
			continue
		}
		if _, err := regexp.Compile(f.Pattern); err != nil {
			return fmt.Errorf("custom_formats[%d] pattern is invalid: %w", i, err)
		}
	}

	return nil
}

// normalizeReleaseTitleKey builds a case-insensitive dedupe key for release titles.
func normalizeReleaseTitleKey(title string) string {
	return strings.ToLower(strings.TrimSpace(title))
}

// redactExternalDownloadURL removes query parameters and user info from an
// indexer-provided download URL, preventing embedded API keys from leaking
// through diagnostic API responses. Unparseable URLs are dropped entirely.
func redactExternalDownloadURL(raw string) string {
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return ""
	}
	u.RawQuery = ""
	u.Fragment = ""
	u.User = nil
	return u.String()
}
