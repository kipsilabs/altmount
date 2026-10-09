package api

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/kipsilabs/altmount/internal/auth"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/httpclient"
	"github.com/kipsilabs/altmount/internal/newsnab"
	"github.com/kipsilabs/altmount/internal/prowlarr"
	"github.com/kipsilabs/altmount/internal/stremio"
)

// stremioDownloadIDPrefix marks queue items originating from the Stremio addon.
// Used by the cleanup service to identify and expire those items.
const stremioDownloadIDPrefix = "stremio:"

// stremioManifest is the Stremio addon manifest response.
type stremioManifest struct {
	ID          string   `json:"id"`
	Version     string   `json:"version"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Resources   []string `json:"resources"`
	Types       []string `json:"types"`
	Catalogs    []any    `json:"catalogs"`
	IDPrefixes  []string `json:"idPrefixes"`
}

// emptyStreamsResponse returns the Stremio-protocol response for "no streams available".
func emptyStreamsResponse(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{"streams": []any{}})
}

// resolveBaseURL returns the public base URL for building stream links.
// Uses the configured base_url if set, otherwise auto-detects from the request.
func resolveBaseURL(c *fiber.Ctx, configuredURL string) string {
	baseURL := strings.TrimRight(configuredURL, "/")
	if baseURL == "" {
		baseURL = c.Protocol() + "://" + c.Hostname()
	}
	return baseURL
}

// isStremioEnabled reports whether the Stremio integration is active.
func isStremioEnabled(cfg *config.Config) bool {
	return cfg.Stremio.Enabled != nil && *cfg.Stremio.Enabled
}

// isProwlarrEnabled reports whether the Prowlarr search is active.
func isProwlarrEnabled(cfg *config.Config) bool {
	return cfg.Stremio.Prowlarr.Enabled != nil && *cfg.Stremio.Prowlarr.Enabled
}

func stremioProviderAvailable(cfg *config.Config) bool {
	provider := strings.ToLower(strings.TrimSpace(cfg.Stremio.Indexers.Provider))
	if provider == "" {
		provider = "prowlarr"
	}
	prowlarrReady := isProwlarrEnabled(cfg) && cfg.Stremio.Prowlarr.Host != "" && cfg.Stremio.Prowlarr.APIKey != ""
	if cfg.Stremio.Indexers.Prowlarr.Host != "" && cfg.Stremio.Indexers.Prowlarr.APIKey != "" {
		prowlarrReady = true
	}
	newsnabReady := false
	for _, indexer := range cfg.Stremio.Indexers.Newsnab {
		if indexer.Enabled && indexer.URL != "" && indexer.APIKey != "" {
			newsnabReady = true
			break
		}
	}
	if cfg.Stremio.EffectiveIncludeLibraryStreams() {
		return true
	}
	switch provider {
	case "prowlarr":
		return prowlarrReady
	case "newsnab":
		return newsnabReady
	case "both":
		return prowlarrReady || newsnabReady
	default:
		return false
	}
}

func resultSource(result prowlarr.NZBResult) string {
	if result.Source != "" {
		return result.Source
	}
	return "prowlarr"
}

func downloadStremioNZB(ctx context.Context, cfg *config.Config, candidate stremioPlayCandidate) ([]byte, error) {
	client := httpclient.NewForExternal(cfg.Network, httpclient.LongTimeout)
	parsed, err := url.Parse(candidate.DownloadURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Hostname() == "" {
		return nil, fmt.Errorf("invalid download URL")
	}

	if candidate.Source == "newsnab" {
		for _, indexer := range cfg.Stremio.Indexers.Newsnab {
			if !indexer.Enabled || (!strings.EqualFold(indexer.Name, candidate.Indexer) && !strings.EqualFold(indexer.ID, candidate.Indexer)) {
				continue
			}
			return newsnab.NewClient(newsIndexConfig(indexer), client).DownloadNZB(ctx, candidate.DownloadURL, "altmount-stremio")
		}
		return newsnab.NewClient(newsnab.IndexerConfig{Name: candidate.Indexer}, client).DownloadNZB(ctx, candidate.DownloadURL, "altmount-stremio")
	}

	prowlarrCfg := cfg.Stremio.Indexers.Prowlarr
	if prowlarrCfg.Host == "" {
		prowlarrCfg = cfg.Stremio.Prowlarr
	}
	return prowlarr.NewClient(prowlarrCfg.Host, prowlarrCfg.APIKey, client).DownloadNZB(ctx, candidate.DownloadURL)
}

func newsIndexConfig(indexer config.NewsnabIndexerConfig) newsnab.IndexerConfig {
	return newsnab.IndexerConfig{ID: indexer.ID, Name: indexer.Name, URL: indexer.URL, APIKey: indexer.APIKey, Categories: indexer.Categories, Weight: indexer.Weight, TimeoutSeconds: indexer.TimeoutSeconds, Enabled: indexer.Enabled}
}

// handleStremioManifest handles GET /stremio/:key/manifest.json
// Returns the Stremio addon manifest for addon installation.
//
//	@Summary		Stremio addon manifest
//	@Description	Returns the Stremio addon manifest JSON for installation. The key authenticates the addon.
//	@Tags			Stremio
//	@Produce		json
//	@Param			key	path		string	true	"Download key (SHA256 of API key)"
//	@Success		200	{object}	stremioManifest
//	@Failure		401	{object}	APIResponse
//	@Router			/stremio/{key}/manifest.json [get]
func (s *Server) handleStremioManifest(c *fiber.Ctx) error {
	ctx := c.Context()

	if s.configManager == nil {
		return RespondServiceUnavailable(c, "Configuration not available", "")
	}
	cfg := s.configManager.GetConfig()
	if !isStremioEnabled(cfg) {
		return RespondNotFound(c, "Stremio endpoint", "Stremio integration is disabled")
	}

	key := c.Params("key")
	if !s.validateDownloadKey(ctx, key) {
		return RespondUnauthorized(c, "Invalid key", "")
	}

	slog.InfoContext(ctx, "Stremio addon manifest requested")

	return c.JSON(stremioManifest{
		ID:          "community.altmount",
		Version:     "1.1.0",
		Name:        "AltMount Usenet",
		Description: "Stream from Usenet via Prowlarr",
		Resources:   []string{"stream"},
		Types:       []string{"movie", "series"},
		Catalogs:    []any{},
		IDPrefixes:  []string{"tt"},
	})
}

// handleStremioAddonStream handles GET /stremio/:key/stream/:type/:id.json
// Searches Prowlarr and returns play-URL options -- no NZB download or queuing at this stage.
//
//	@Summary		Stremio stream handler
//	@Description	Searches Prowlarr for matching NZBs and returns Stremio-compatible stream URL options.
//	@Tags			Stremio
//	@Produce		json
//	@Param			key		path		string	true	"Download key"
//	@Param			type	path		string	true	"Content type (movie or series)"
//	@Param			id		path		string	true	"Stremio content ID (e.g. tt1234567)"
//	@Success		200		{object}	APIResponse
//	@Failure		401		{object}	APIResponse
//	@Router			/stremio/{key}/stream/{type}/{id}.json [get]
func (s *Server) handleStremioAddonStream(c *fiber.Ctx) error {
	ctx := c.Context()

	key := c.Params("key")

	if s.configManager == nil {
		return emptyStreamsResponse(c)
	}
	cfg := s.configManager.GetConfig()
	if !isStremioEnabled(cfg) || !stremioProviderAvailable(cfg) {
		return emptyStreamsResponse(c)
	}

	if !s.validateDownloadKey(ctx, key) {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "invalid key"})
	}

	streamType := c.Params("type")
	if streamType != "movie" && streamType != "series" {
		return emptyStreamsResponse(c)
	}

	rawID, _ := url.PathUnescape(c.Params("id"))

	// Parse Stremio ID: tt1234567 (movie) or tt1234567:season:episode (series)
	imdbID, season, episode := parseStremioContentID(rawID)

	if !strings.HasPrefix(imdbID, "tt") {
		return emptyStreamsResponse(c)
	}

	baseURL := resolveBaseURL(c, cfg.Stremio.BaseURL)
	libraryStreams := s.findHealthyLibraryStreams(ctx, cfg, streamType, imdbID, baseURL, key, season, episode)

	// Map Stremio type to Prowlarr search type
	prowlarrType := "search"
	switch streamType {
	case "movie":
		prowlarrType = "movie"
	case "series":
		prowlarrType = "tvsearch"
	}

	slog.InfoContext(ctx, "Stremio addon stream request",
		"type", streamType, "id", rawID, "imdb_id", imdbID, "library_matches", len(libraryStreams))

	results, cachedItems, err := s.searchStremioReleases(ctx, cfg, streamType, prowlarrType, imdbID, season, episode)
	if err != nil && len(libraryStreams) == 0 {
		return emptyStreamsResponse(c)
	}

	showCachedIndicator := cfg.Stremio.ShowCachedIndicator == nil || *cfg.Stremio.ShowCachedIndicator
	entries := buildStremioStreamEntries(results, cachedItems, cfg.Stremio.NzbTTLHours, time.Now(),
		baseURL, key, streamType, season, episode, imdbID, cfg.Stremio.Prowlarr,
		s.storeStremioReleaseRef)
	if !showCachedIndicator {
		for i := range entries {
			entries[i].Name = strings.TrimPrefix(entries[i].Name, "⚡ Cached · ")
		}
	}

	streams := make([]fiber.Map, 0, len(libraryStreams)+len(entries))
	streams = append(streams, libraryStreams...)

	for _, e := range entries {
		streams = append(streams, fiber.Map{
			"name":  e.Name,
			"title": e.Title,
			"url":   e.URL,
		})
	}

	if len(streams) == 0 {
		return emptyStreamsResponse(c)
	}

	return c.JSON(fiber.Map{"streams": streams})
}

func (s *Server) findHealthyLibraryStreams(ctx context.Context, cfg *config.Config, streamType, imdbID, baseURL, key string, season, episode int) []fiber.Map {
	if !cfg.Stremio.EffectiveIncludeLibraryStreams() || s.healthRepo == nil || imdbID == "" {
		return nil
	}
	selector := &stremioEpisodeSelector{Season: season, Episode: episode}
	var libraryStreams []fiber.Map
	if streamType == "movie" {
		tmdbID, movieTitle, movieYear, _ := resolveMovieMetadataFromIMDb(ctx, imdbID, cfg.GetUserAgent())
		yearNum, _ := strconv.Atoi(movieYear)
		if healthyFiles, err := s.healthRepo.FindHealthyFilesForMovie(ctx, movieTitle, movieYear, tmdbID); err == nil {
			for _, h := range healthyFiles {
				if h != nil && h.FilePath != "" && isMediaExtension(filepath.Ext(h.FilePath)) && !isSampleFile(h.FilePath) {
					matchPath := h.FilePath
					if h.LibraryPath != nil && *h.LibraryPath != "" {
						matchPath = *h.LibraryPath
					}
					// Verify movie title matches if movieTitle is known (prevents substring false positives)
					if movieTitle != "" && !stremio.MatchesMovie(filepath.Base(matchPath), movieTitle, yearNum) && !stremio.MatchesMovie(matchPath, movieTitle, yearNum) {
						continue
					}
					streamURL := baseURL + "/api/files/stream?path=" +
						url.QueryEscape(h.FilePath) + "&download_key=" + url.QueryEscape(key)
					libraryStreams = append(libraryStreams, fiber.Map{
						"name":  formatLibraryStreamName(h),
						"title": formatLibraryStreamTitle(h),
						"url":   streamURL,
					})
				}
			}
		}
	} else if streamType == "series" {
		tvdbIDStr, seriesTitle, _ := resolveSeriesMetadataFromIMDb(ctx, imdbID, cfg.GetUserAgent())
		tvdbID, _ := strconv.Atoi(tvdbIDStr)
		if healthyFiles, err := s.healthRepo.FindHealthyFilesForSeries(ctx, seriesTitle, tvdbID); err == nil {
			for _, h := range healthyFiles {
				if h != nil && h.FilePath != "" && isMediaExtension(filepath.Ext(h.FilePath)) && !isSampleFile(h.FilePath) {
					matchPath := h.FilePath
					if h.LibraryPath != nil && *h.LibraryPath != "" {
						matchPath = *h.LibraryPath
					}
					if selector == nil || selector.matches(matchPath) || selector.matches(h.FilePath) {
						// Verify series title matches if seriesTitle is known (prevents cross-show false positives)
						if seriesTitle != "" && !stremio.MatchesSeries(filepath.Base(matchPath), seriesTitle, season, episode, 0) && !stremio.MatchesSeries(matchPath, seriesTitle, season, episode, 0) {
							continue
						}
						streamURL := baseURL + "/api/files/stream?path=" +
							url.QueryEscape(h.FilePath) + "&download_key=" + url.QueryEscape(key)
						libraryStreams = append(libraryStreams, fiber.Map{
							"name":  formatLibraryStreamName(h),
							"title": formatLibraryStreamTitle(h),
							"url":   streamURL,
						})
					}
				}
			}
		}
	}
	return libraryStreams
}

func formatLibraryStreamTitle(h *database.FileHealth) string {
	rawName := ""
	if h.LibraryPath != nil && *h.LibraryPath != "" {
		rawName = filepath.Base(*h.LibraryPath)
	}
	if rawName == "" || rawName == "." {
		rawName = filepath.Base(h.FilePath)
	}
	rawName = strings.TrimSuffix(rawName, filepath.Ext(rawName))

	return fmt.Sprintf("%s\n💾 Local Library • ⚡ Instant 0s Playback", rawName)
}

func formatLibraryStreamName(h *database.FileHealth) string {
	name := filepath.Base(h.FilePath)
	if h.LibraryPath != nil && *h.LibraryPath != "" {
		name = filepath.Base(*h.LibraryPath)
	}
	nameLower := strings.ToLower(name)
	res := ""
	if strings.Contains(nameLower, "2160p") || strings.Contains(nameLower, "4k") || strings.Contains(nameLower, "uhd") {
		res = "2160p"
	} else if strings.Contains(nameLower, "1080p") || strings.Contains(nameLower, "fhd") {
		res = "1080p"
	} else if strings.Contains(nameLower, "720p") || strings.Contains(nameLower, "hd") {
		res = "720p"
	} else if strings.Contains(nameLower, "480p") || strings.Contains(nameLower, "sd") {
		res = "480p"
	}

	if res != "" {
		return fmt.Sprintf("⚡ Altmount Library\n%s", res)
	}
	return "⚡ Altmount Library"
}

func isSampleFile(path string) bool {
	lower := strings.ToLower(path)
	if strings.Contains(lower, "/sample/") || strings.Contains(lower, "\\sample\\") {
		return true
	}
	base := strings.ToLower(filepath.Base(path))
	return strings.Contains(base, "sample.") || strings.HasPrefix(base, "sample") || strings.HasSuffix(base, "-sample") || strings.HasSuffix(base, "_sample")
}

// stremioPlayCandidate is one release a /play request may attempt. Candidates are
// produced in the same order the stream list presented, so the fallback chain and the
// list agree on what "the next release" means.
type stremioPlayCandidate struct {
	Title       string // raw Prowlarr title
	SafeTitle   string // sanitizeFilename(Title) -- the release key
	DownloadURL string
	Indexer     string
	Source      string
	IndexerID   string
	GUID        string
}

type stremioReleaseRef struct {
	Candidate stremioPlayCandidate
	ExpiresAt time.Time
}

func (s *Server) storeStremioReleaseRef(candidate stremioPlayCandidate) (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate Stremio release reference: %w", err)
	}
	token := fmt.Sprintf("%x", raw[:])
	s.stremioReleaseMu.Lock()
	defer s.stremioReleaseMu.Unlock()
	if s.stremioReleaseRefs == nil {
		s.stremioReleaseRefs = make(map[string]stremioReleaseRef)
	}
	now := time.Now()
	for key, ref := range s.stremioReleaseRefs {
		if !ref.ExpiresAt.After(now) {
			delete(s.stremioReleaseRefs, key)
		}
	}
	s.stremioReleaseRefs[token] = stremioReleaseRef{Candidate: candidate, ExpiresAt: now.Add(15 * time.Minute)}
	return token, nil
}

func (s *Server) consumeStremioReleaseRef(token string) (stremioPlayCandidate, bool) {
	s.stremioReleaseMu.Lock()
	defer s.stremioReleaseMu.Unlock()
	ref, ok := s.stremioReleaseRefs[token]
	if !ok || !ref.ExpiresAt.After(time.Now()) {
		if ok {
			delete(s.stremioReleaseRefs, token)
		}
		return stremioPlayCandidate{}, false
	}
	return ref.Candidate, true
}

type stremioQueueMetadata struct {
	ContentKey string `json:"content_key"`
	ReleaseKey string `json:"release_key"`
	IMDbID     string `json:"imdb_id,omitempty"`
	Type       string `json:"type,omitempty"`
	Season     int    `json:"season,omitempty"`
	Episode    int    `json:"episode,omitempty"`
	TMDBID     int    `json:"tmdbId,omitempty"`
	TVDBID     int    `json:"tvdbId,omitempty"`
}

type stremioContentInFlightError struct {
	itemID int64
}

func (e *stremioContentInFlightError) Error() string {
	return fmt.Sprintf("another release for this content is already processing (queue item %d)", e.itemID)
}

func stremioContentKey(streamType, imdbID string, selector *stremioEpisodeSelector) string {
	if imdbID == "" {
		return ""
	}
	season, episode := 0, 0
	if selector != nil {
		season, episode = selector.Season, selector.Episode
	}
	if strings.EqualFold(streamType, "series") && (season <= 0 || episode <= 0) {
		return ""
	}
	return fmt.Sprintf("%s:%s:%d:%d", strings.ToLower(streamType), strings.ToLower(imdbID), season, episode)
}

func stremioReleaseKey(candidate stremioPlayCandidate) string {
	identity := candidate.GUID
	if identity == "" {
		identity = candidate.IndexerID
	}
	if identity == "" {
		identity = candidate.DownloadURL
	}
	hash := sha256.Sum256([]byte(identity))
	return fmt.Sprintf("%s:%s:%x", strings.ToLower(candidate.Source), strings.ToLower(candidate.Indexer), hash[:16])
}

func encodeStremioQueueMetadata(metadata stremioQueueMetadata) (*string, error) {
	data, err := json.Marshal(metadata)
	if err != nil {
		return nil, err
	}
	value := string(data)
	return &value, nil
}

func stremioQueueMetadataMatches(item *database.ImportQueueItem, contentKey, releaseKey string) bool {
	if item == nil || item.Metadata == nil || contentKey == "" || releaseKey == "" {
		return false
	}
	var metadata stremioQueueMetadata
	if json.Unmarshal([]byte(*item.Metadata), &metadata) != nil {
		return false
	}
	return metadata.ContentKey == contentKey && metadata.ReleaseKey == releaseKey
}

func (s *Server) findActiveStremioContent(ctx context.Context, contentKey string) (*database.ImportQueueItem, error) {
	if s.queueRepo == nil || contentKey == "" {
		return nil, nil
	}
	items, err := s.queueRepo.ListActiveQueueItems(ctx, "", "", 1000, 0, "updated_at", "desc")
	if err != nil {
		return nil, err
	}
	for _, item := range items {
		if item == nil || item.DownloadID == nil || !strings.HasPrefix(*item.DownloadID, stremioDownloadIDPrefix) {
			continue
		}
		if item.Metadata != nil {
			var metadata stremioQueueMetadata
			if json.Unmarshal([]byte(*item.Metadata), &metadata) == nil && metadata.ContentKey == contentKey {
				return item, nil
			}
		}
	}
	return nil, nil
}

// normalizeTitleForMatching extracts an alphanumeric canonical representation of a title
// or file path, ignoring case, separators (dots, underscores, dashes, spaces), brackets,
// and file extensions (.nzb, .nzb.gz, .mkv, .mp4, etc.).
func normalizeTitleForMatching(s string) string {
	s = strings.ReplaceAll(s, "\\", "/")
	s = filepath.Base(s)
	for {
		orig := s
		s = strings.TrimSuffix(s, ".gz")
		s = strings.TrimSuffix(s, ".nzb")
		s = strings.TrimSuffix(s, ".mkv")
		s = strings.TrimSuffix(s, ".mp4")
		s = strings.TrimSuffix(s, ".avi")
		s = strings.TrimSuffix(s, ".iso")
		s = strings.TrimSuffix(s, ".rar")
		if s == orig {
			break
		}
	}

	s = strings.ToLower(s)
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// stremioNzbPathMatches checks if an NZB path matches a candidate title regardless of
// case, punctuation, extensions, or directory wrappers using exact normalized equality.
func stremioNzbPathMatches(nzbPath, safeTitle string) bool {
	normA := normalizeTitleForMatching(nzbPath)
	normB := normalizeTitleForMatching(safeTitle)
	if normA == "" || normB == "" {
		return false
	}
	return normA == normB
}

// stremioCachedPredicate returns a test for "already imported and still usable":
// the item must have a storage path and, when a TTL is configured, have completed
// within it. Uses exact alphanumeric normalized matching to eliminate false positives.
func stremioCachedPredicate(cached []*database.ImportQueueItem, ttlHours int, now time.Time) func(safeTitle string) bool {
	normalizedCache := make(map[string]struct{}, len(cached)*3)
	for _, item := range cached {
		if item == nil || item.StoragePath == nil || *item.StoragePath == "" {
			continue
		}
		if ttlHours > 0 {
			if item.CompletedAt == nil || now.Sub(*item.CompletedAt) >= time.Duration(ttlHours)*time.Hour {
				continue
			}
		}
		if norm := normalizeTitleForMatching(item.NzbPath); norm != "" {
			normalizedCache[norm] = struct{}{}
		}
		if item.StoragePath != nil {
			if norm := normalizeTitleForMatching(*item.StoragePath); norm != "" {
				normalizedCache[norm] = struct{}{}
			}
		}
		if item.RelativePath != nil && *item.RelativePath != "" {
			if norm := normalizeTitleForMatching(*item.RelativePath); norm != "" {
				normalizedCache[norm] = struct{}{}
			}
		}
	}

	return func(safeTitle string) bool {
		target := normalizeTitleForMatching(safeTitle)
		if target == "" {
			return false
		}
		_, exists := normalizedCache[target]
		return exists
	}
}

// half (failed import_queue rows) with the process-local half (failures that leave no
// failed row -- see stremioFailureCache).
//
// Unlike the cached predicate this keys off UpdatedAt, because UpdateQueueItemStatus
// does not set completed_at on failure, and it does not require a storage path.
func stremioFailedPredicate(
	failed []*database.ImportQueueItem,
	memKeys map[string]struct{},
	ttlHours int,
	now time.Time,
) func(safeTitle string) bool {
	paths := make([]string, 0, len(failed))
	for _, item := range failed {
		if item == nil {
			continue
		}
		if ttlHours > 0 && now.Sub(item.UpdatedAt) >= time.Duration(ttlHours)*time.Hour {
			continue
		}
		paths = append(paths, item.NzbPath)
	}

	return func(safeTitle string) bool {
		if _, ok := memKeys[safeTitle]; ok {
			return true
		}
		for _, p := range paths {
			if stremioNzbPathMatches(p, safeTitle) {
				return true
			}
		}
		return false
	}
}

// filterStremioResults drops releases failing the configured language/quality/exclude filters,
// and those already known to have failed. Excluding failed releases here -- rather than
// demoting them -- is what stops a bad release being offered, and re-picked, forever.
func filterStremioResults(
	results []prowlarr.NZBResult,
	languages, qualities, excludeKeywords []string,
	isFailed func(safeTitle string) bool,
) []prowlarr.NZBResult {
	filtered := make([]prowlarr.NZBResult, 0, len(results))
	for _, r := range results {
		if !prowlarr.MatchesLanguage(r.Title, languages) {
			continue
		}
		if !prowlarr.MatchesQuality(r.Title, qualities) {
			continue
		}
		if prowlarr.MatchesExcludeKeywords(r.Title, excludeKeywords) {
			continue
		}
		if isFailed != nil && isFailed(sanitizeFilename(r.Title)) {
			continue
		}
		filtered = append(filtered, r)
	}
	return filtered
}

func calculateStremioReleaseScore(r prowlarr.NZBResult, prowlarrCfg config.ProwlarrConfig) int {
	score := 0
	if len(prowlarrCfg.PreferredIndexers) > 0 && r.IndexerID > 0 {
		for _, id := range prowlarrCfg.PreferredIndexers {
			if id == r.IndexerID {
				score += 10000
				break
			}
		}
	}
	if len(prowlarrCfg.PreferredIndexerNames) > 0 && r.Indexer != "" {
		indexerLower := strings.ToLower(r.Indexer)
		for _, name := range prowlarrCfg.PreferredIndexerNames {
			name = strings.TrimSpace(name)
			if name != "" && strings.Contains(indexerLower, strings.ToLower(name)) {
				score += 10000
				break
			}
		}
	}
	if len(prowlarrCfg.PreferredLanguages) > 0 {
		if prowlarr.MatchesLanguage(r.Title, prowlarrCfg.PreferredLanguages) {
			score += 5000
		}
	}
	if len(prowlarrCfg.CustomScores) > 0 {
		for pattern, val := range prowlarrCfg.CustomScores {
			pattern = strings.TrimSpace(pattern)
			if pattern == "" {
				continue
			}
			if prowlarr.MatchKeywordOrPattern(r.Title, pattern) {
				score += val
			}
		}
	}
	return score
}

// orderStremioResults stably reorders results so cached releases come first,
// followed by highest TRaSH/custom score, preserving Prowlarr's publish date within equal groups.
func orderStremioResults(
	results []prowlarr.NZBResult,
	isCached func(safeTitle string) bool,
	prowlarrCfg config.ProwlarrConfig,
) []prowlarr.NZBResult {
	ordered := make([]prowlarr.NZBResult, len(results))
	copy(ordered, results)
	scores := make(map[string]int, len(results))
	for _, r := range results {
		scores[r.Title] = calculateStremioReleaseScore(r, prowlarrCfg)
	}

	sort.SliceStable(ordered, func(i, j int) bool {
		cI := isCached(sanitizeFilename(ordered[i].Title))
		cJ := isCached(sanitizeFilename(ordered[j].Title))
		if cI != cJ {
			return cI && !cJ
		}
		sI := scores[ordered[i].Title]
		sJ := scores[ordered[j].Title]
		if sI != sJ {
			return sI > sJ
		}
		return ordered[i].PublishDate.After(ordered[j].PublishDate)
	})
	return ordered
}

// stremioCandidates converts ordered Prowlarr results into play candidates.
func stremioCandidates(results []prowlarr.NZBResult, fallbackID string) []stremioPlayCandidate {
	cands := make([]stremioPlayCandidate, 0, len(results))
	for _, r := range results {
		safeTitle := sanitizeFilename(r.Title)
		if safeTitle == "" {
			safeTitle = fallbackID
		}
		cands = append(cands, stremioPlayCandidate{
			Title:       r.Title,
			SafeTitle:   safeTitle,
			DownloadURL: r.DownloadURL,
			Indexer:     r.Indexer,
			Source:      r.Source,
			IndexerID:   strconv.Itoa(r.IndexerID),
			GUID:        r.GUID,
		})
	}
	return cands
}

// nextStremioCandidate returns the index of the release to try after currentSafeTitle.
// When the current release is absent -- the Prowlarr list can change between /stream and
// /play -- it starts from the top rather than giving up. Reports false when exhausted.
func nextStremioCandidate(cands []stremioPlayCandidate, currentSafeTitle string, startAfter int) (int, bool) {
	if len(cands) == 0 {
		return 0, false
	}

	if startAfter < 0 {
		startAfter = -1
		for i, c := range cands {
			if c.SafeTitle == currentSafeTitle {
				startAfter = i
				break
			}
		}
		if startAfter < 0 {
			return 0, true
		}
	}

	next := startAfter + 1
	if next >= len(cands) {
		return 0, false
	}
	return next, true
}

func (s *Server) buildStremioCoordinatorConfig(cfg *config.Config) stremio.CoordinatorConfig {
	stremioCfg := cfg.Stremio

	// Determine provider
	provider := stremioCfg.Indexers.Provider
	if provider == "" {
		provider = "prowlarr"
	}

	// Prowlarr config (merging from Indexers.Prowlarr or fallback to Stremio.Prowlarr)
	pHost := stremioCfg.Indexers.Prowlarr.Host
	pKey := stremioCfg.Indexers.Prowlarr.APIKey
	pCats := stremioCfg.Indexers.Prowlarr.Categories
	pIdxs := stremioCfg.Indexers.Prowlarr.Indexers

	if pHost == "" && stremioCfg.Prowlarr.Host != "" {
		pHost = stremioCfg.Prowlarr.Host
		pKey = stremioCfg.Prowlarr.APIKey
		pCats = stremioCfg.Prowlarr.Categories
		pIdxs = stremioCfg.Prowlarr.Indexers
	}

	// Convert newsnab indexers
	newsnabList := make([]newsnab.IndexerConfig, 0, len(stremioCfg.Indexers.Newsnab))
	for _, n := range stremioCfg.Indexers.Newsnab {
		newsnabList = append(newsnabList, newsnab.IndexerConfig{
			ID:             n.ID,
			Name:           n.Name,
			URL:            n.URL,
			APIKey:         n.APIKey,
			Categories:     n.Categories,
			Weight:         n.Weight,
			TimeoutSeconds: n.TimeoutSeconds,
			Enabled:        n.Enabled,
		})
	}

	// Convert scoring config
	customFormats := make([]stremio.TrashCustomFormat, 0, len(stremioCfg.Scoring.CustomFormats))
	for _, f := range stremioCfg.Scoring.CustomFormats {
		customFormats = append(customFormats, stremio.TrashCustomFormat{
			ID:          f.ID,
			Name:        f.Name,
			Category:    f.Category,
			Pattern:     f.Pattern,
			PatternType: f.PatternType,
			Score:       f.Score,
			Enabled:     f.Enabled,
			IsCustom:    f.IsCustom,
			Invert:      f.Invert,
		})
	}

	// If no custom formats configured yet, convert from legacy Prowlarr.CustomScores
	if len(customFormats) == 0 && len(stremioCfg.Prowlarr.CustomScores) > 0 {
		for pat, score := range stremioCfg.Prowlarr.CustomScores {
			customFormats = append(customFormats, stremio.TrashCustomFormat{
				ID:          pat,
				Name:        pat,
				Pattern:     pat,
				PatternType: "regex",
				Score:       score,
				Enabled:     true,
			})
		}
	}

	excludeKeywords := stremioCfg.Scoring.ExcludeKeywords
	if len(excludeKeywords) == 0 && len(stremioCfg.Prowlarr.ExcludeKeywords) > 0 {
		excludeKeywords = stremioCfg.Prowlarr.ExcludeKeywords
	}

	preferredLangs := stremioCfg.Scoring.PreferredLanguages
	if len(preferredLangs) == 0 && len(stremioCfg.Prowlarr.PreferredLanguages) > 0 {
		preferredLangs = stremioCfg.Prowlarr.PreferredLanguages
	}

	return stremio.CoordinatorConfig{
		Provider:        provider,
		UserAgentMode:   stremioCfg.Indexers.UserAgentMode,
		CustomUserAgent: stremioCfg.Indexers.CustomUserAgent,
		ProwlarrHost:    pHost,
		ProwlarrKey:     pKey,
		ProwlarrCats:    pCats,
		ProwlarrIdxs:    pIdxs,
		NewsnabIndexers: newsnabList,
		Scoring: stremio.StreamScoringConfig{
			Preset:                   stremioCfg.Scoring.Preset,
			CustomFormats:            customFormats,
			ExcludeKeywords:          excludeKeywords,
			ExcludeRegex:             stremioCfg.Scoring.ExcludeRegex,
			PreferredLanguages:       preferredLangs,
			RequirePreferredLanguage: stremioCfg.Scoring.RequirePreferredLanguage,
		},
	}
}

// searchStremioReleases runs multi-provider search for a Stremio content id, applies the
// language/quality filters and TRaSH scoring, drops failed releases, and orders cached-first.
func (s *Server) searchStremioReleases(
	ctx context.Context,
	cfg *config.Config,
	streamType, prowlarrType, imdbID string,
	season, episode int,
) ([]prowlarr.NZBResult, []*database.ImportQueueItem, error) {
	coordCfg := s.buildStremioCoordinatorConfig(cfg)
	coordinator := stremio.NewSearchCoordinator(coordCfg, httpclient.NewForExternal(cfg.Network, 30*time.Second))

	var tvdbID, title string
	expectedTitle := ""
	expectedYear := ""
	seriesMatchCtx := releaseMatchContext{}
	if streamType == "series" {
		var err error
		tvdbID, title, err = resolveSeriesMetadataFromIMDb(ctx, imdbID, cfg.GetUserAgent())
		if err != nil {
			slog.WarnContext(ctx, "Failed to resolve series metadata from IMDb ID", "error", err, "imdb_id", imdbID)
		}
		expectedTitle = title
		// Anime releases use franchise-absolute episode numbers and localized
		// titles; resolve both once per request so the relevance gate can
		// accept e.g. "[SubsPlease] Detective Conan - 1210" for catalog
		// SxxEyy entries.
		meta := resolveSeriesEpisodeMeta(ctx, imdbID, cfg.GetUserAgent())
		seriesMatchCtx = releaseMatchContext{
			aliases:     resolveSeriesTitleAliases(ctx, imdbID, cfg.GetUserAgent()),
			episodeMeta: meta,
			isAnime:     meta.isAnimation,
		}
	} else if imdbID != "" {
		_, movieTitle, movieYear, mErr := resolveMovieMetadataFromIMDb(ctx, imdbID, cfg.GetUserAgent())
		if mErr != nil {
			slog.WarnContext(ctx, "Failed to resolve movie metadata from IMDb ID", "error", mErr, "imdb_id", imdbID)
		}
		title = movieTitle
		expectedTitle = movieTitle
		expectedYear = movieYear
	}

	timeoutMs := cfg.Stremio.FallbackTimeoutMs
	if timeoutMs <= 0 {
		timeoutMs = 3500
	}

	scoredReleases, err := coordinator.Search(ctx, stremio.SearchParams{
		Type:      streamType,
		IMDBID:    imdbID,
		Title:     title,
		Season:    season,
		Episode:   episode,
		TVDBID:    tvdbID,
		TimeoutMS: timeoutMs,
	})
	if err != nil {
		slog.WarnContext(ctx, "Stremio search coordinator error", "error", err, "imdb_id", imdbID)
	}

	if len(scoredReleases) == 0 {
		slog.InfoContext(ctx, "No Stremio releases found across configured providers", "imdb_id", imdbID)
		return nil, nil, nil
	}

	now := time.Now()
	cachedItems, failedItems := s.loadStremioQueueState(ctx, cfg.Stremio.EffectiveIncludeLibraryStreams(), cfg.Stremio.NzbTTLHours)
	isFailed := stremioFailedPredicate(failedItems, s.stremioFailures.Keys(stremioFailedTTL(cfg)), cfg.Stremio.FailedReleaseTTLHours, now)
	isCached := stremioCachedPredicate(cachedItems, cfg.Stremio.NzbTTLHours, now)

	// Filter out failed releases and convert to prowlarr.NZBResult
	var filtered []prowlarr.NZBResult
	for _, sr := range scoredReleases {
		safeTitle := sanitizeFilename(sr.Title)
		if isFailed != nil && isFailed(safeTitle) {
			continue
		}
		idxID, _ := strconv.Atoi(sr.IndexerID)
		filtered = append(filtered, prowlarr.NZBResult{
			Title:       sr.Title,
			DownloadURL: sr.DownloadURL,
			Size:        sr.Size,
			PublishDate: sr.PublishDate,
			Indexer:     sr.Indexer,
			IndexerID:   idxID,
			Source:      sr.Source,
			GUID:        sr.GUID,
		})
	}

	// Reorder with cached items first, then preserving scored order. Cached
	// releases were already content-matched at import time, so they bypass the
	// relevance gate; fresh search results must plausibly belong to the
	// requested content before they can reach any downstream consumer.
	ordered := make([]prowlarr.NZBResult, 0, len(filtered))
	var uncached []prowlarr.NZBResult
	var droppedNames []string
	for _, r := range filtered {
		if isCached(sanitizeFilename(r.Title)) {
			ordered = append(ordered, r)
			continue
		}
		if !releaseMatchesContent(streamType, r.Title, expectedTitle, expectedYear, season, episode, seriesMatchCtx) {
			droppedNames = append(droppedNames, fmt.Sprintf("%q (%s)", r.Title, r.Indexer))
			continue
		}
		uncached = append(uncached, r)
	}
	ordered = append(ordered, uncached...)

	if len(droppedNames) > 0 {
		slog.InfoContext(ctx, "Dropped irrelevant Stremio releases",
			"imdb_id", imdbID,
			"dropped_count", len(droppedNames),
			"dropped_sample", droppedNames,
			"kept", len(ordered)+len(uncached)-len(droppedNames))
	}

	return ordered, cachedItems, nil
}

// stremioFailedTTL is the age after which a recorded failure stops excluding a release.
func stremioFailedTTL(cfg *config.Config) time.Duration {
	return time.Duration(cfg.Stremio.FailedReleaseTTLHours) * time.Hour
}

// loadStremioQueueState fetches the cached and failed Stremio queue items. Both lookups
// are best-effort: a DB hiccup degrades badges and exclusion rather than failing search.
func (s *Server) loadStremioQueueState(ctx context.Context, reuseLibrary bool, ttlHours int) (cached, failed []*database.ImportQueueItem) {
	if s.queueRepo == nil {
		return nil, nil
	}

	if items, err := s.queueRepo.GetCachedStremioQueueItems(ctx, reuseLibrary, ttlHours); err != nil {
		slog.WarnContext(ctx, "Failed to load cached Stremio items; continuing without cache badges",
			"error", err)
	} else {
		cached = items
	}

	if items, err := s.queueRepo.GetFailedStremioQueueItems(ctx); err != nil {
		slog.WarnContext(ctx, "Failed to load failed Stremio items; continuing without exclusion",
			"error", err)
	} else {
		failed = items
	}

	return cached, failed
}

// stremioStreamEntry is a single Stremio stream option produced from a Prowlarr
// result, together with whether the underlying release is already cached/imported
// in AltMount.
type stremioStreamEntry struct {
	Name   string
	Title  string
	URL    string
	Cached bool
}

// buildStremioStreamEntries converts filtered Prowlarr results into ordered
// Stremio stream entries. Results whose release is already imported and still
// fresh (per the cached queue items and TTL) are badged "⚡ Cached" and stably
// sorted ahead of the rest, mirroring the aiostreams/Torrentio UX.
//
// The match/TTL logic mirrors the short-circuit in handleStremioAddonPlay so the
// badge is truthful: a cached entry is one /play resolves to an instant redirect.
// now is injected for deterministic TTL testing.
func buildStremioStreamEntries(
	results []prowlarr.NZBResult,
	cached []*database.ImportQueueItem,
	ttlHours int,
	now time.Time,
	baseURL, key, streamType string,
	season, episode int,
	fallbackID string,
	prowlarrCfg config.ProwlarrConfig,
	releaseStores ...func(stremioPlayCandidate) (string, error),
) []stremioStreamEntry {
	isCached := stremioCachedPredicate(cached, ttlHours, now)

	// Order first, then build, so entries come out in final order without a second sort.
	results = orderStremioResults(results, isCached, prowlarrCfg)

	entries := make([]stremioStreamEntry, 0, len(results))
	for _, r := range results {
		safeTitle := sanitizeFilename(r.Title)
		if safeTitle == "" {
			safeTitle = fallbackID
		}
		cachedHit := isCached(safeTitle)
		candidate := stremioPlayCandidate{
			Title: r.Title, SafeTitle: safeTitle, DownloadURL: r.DownloadURL,
			Indexer: r.Indexer, IndexerID: strconv.Itoa(r.IndexerID), GUID: r.GUID,
			Source: resultSource(r),
		}
		releaseToken := ""
		if len(releaseStores) > 0 {
			var err error
			releaseToken, err = releaseStores[0](candidate)
			if err != nil {
				continue
			}
		}

		playURL := baseURL + "/stremio/" + key + "/play"
		if releaseToken != "" {
			playURL += "?release=" + url.QueryEscape(releaseToken)
		} else {
			playURL += "?url=" + url.QueryEscape(r.DownloadURL) + "&title=" + url.QueryEscape(safeTitle)
		}
		playURL += "&type=" + url.QueryEscape(streamType)
		if r.Indexer != "" {
			playURL += "&indexer=" + url.QueryEscape(r.Indexer)
		}
		if source := resultSource(r); source != "" {
			playURL += "&source=" + url.QueryEscape(source)
		}
		if fallbackID != "" {
			// The content id lets /play re-derive the candidate list to fall back on.
			playURL += "&id=" + url.QueryEscape(fallbackID)
		}
		if streamType == "series" && season > 0 && episode > 0 {
			playURL += "&season=" + url.QueryEscape(strconv.Itoa(season)) +
				"&episode=" + url.QueryEscape(strconv.Itoa(episode))
		}

		sizeGB := float64(r.Size) / 1e9
		indexerLabel := r.Indexer
		if indexerLabel == "" {
			indexerLabel = "Unknown"
		}

		meta := prowlarr.InferReleaseMeta(r.Title)

		// Badge: "AltMount 🇪🇸 4K"
		badge := "AltMount"
		if meta.FlagEmoji != "" {
			badge += " " + meta.FlagEmoji
		}
		if meta.QualityLabel != "" {
			badge += " " + meta.QualityLabel
		}

		// Content info: "La película (2024) [2160p][Esp]"
		contentTitle := meta.ParsedTitle
		if contentTitle == "" {
			contentTitle = r.Title
		}
		if meta.Year > 0 {
			contentTitle += fmt.Sprintf(" (%d)", meta.Year)
		}
		if meta.Resolution != "" {
			contentTitle += " [" + meta.Resolution + "]"
		}
		if meta.LangCode != "" {
			contentTitle += "[" + meta.LangCode + "]"
		}

		streamName := badge
		if contentTitle != "" {
			streamName += " - " + contentTitle
		}
		if cachedHit {
			// Prefix marks instantly-playable releases, aiostreams-style.
			streamName = "⚡ Cached · " + streamName
		}

		metaLine := fmt.Sprintf("💾 %.2f GB 🌐 %s", sizeGB, indexerLabel)
		entries = append(entries, stremioStreamEntry{
			Name:   streamName,
			Title:  fmt.Sprintf("%s\n%s", r.Title, metaLine),
			URL:    playURL,
			Cached: cachedHit,
		})
	}

	return entries
}

// handleStremioAddonPlay handles GET /stremio/:key/play
// Downloads the NZB from Prowlarr, queues it with high priority, waits for completion,
// then 302-redirects to the first media stream URL.
//
//	@Summary		Play Stremio NZB stream
//	@Description	Downloads the NZB from Prowlarr by URL, queues it with high priority, waits for download completion, then redirects (302) to the first media stream URL.
//	@Tags			Stremio
//	@Produce		json
//	@Param			key		path	string	true	"Download key (SHA256 of API key)"
//	@Param			url		query	string	true	"Prowlarr NZB download URL"
//	@Param			title	query	string	false	"Safe filename title for the NZB"
//	@Param			season	query	int		false	"Season number for selecting one episode from a season pack"
//	@Param			episode	query	int		false	"Episode number for selecting one episode from a season pack"
//	@Success		302	{string}	string	"Redirects to media stream URL"
//	@Failure		400	{object}	APIResponse
//	@Failure		401	{object}	APIResponse
//	@Failure		503	{object}	APIResponse
//	@Router			/stremio/{key}/play [get]
func (s *Server) handleStremioAddonPlay(c *fiber.Ctx) error {
	ctx := c.Context()

	if s.configManager == nil {
		return RespondServiceUnavailable(c, "Configuration not available", "")
	}
	cfg := s.configManager.GetConfig()
	if !isStremioEnabled(cfg) {
		return RespondNotFound(c, "Stremio endpoint", "Stremio integration is disabled")
	}
	if !stremioProviderAvailable(cfg) {
		return RespondServiceUnavailable(c, "No Stremio search provider is configured", "")
	}

	key := c.Params("key")
	if !s.validateDownloadKey(ctx, key) {
		return RespondUnauthorized(c, "Invalid key", "")
	}

	releaseToken := c.Query("release")
	if releaseToken == "" {
		return RespondBadRequest(c, "Missing release parameter", "")
	}
	cand, ok := s.consumeStremioReleaseRef(releaseToken)
	if !ok {
		return RespondBadRequest(c, "Invalid or expired release parameter", "")
	}
	imdbID := c.Query("id")
	streamType := c.Query("type")

	baseURL := resolveBaseURL(c, cfg.Stremio.BaseURL)
	selector := stremioEpisodeSelectorFromRequest(c)

	// Short-circuit: return cached stream if already processed within TTL or present in library. This runs
	// before the failed-release check so a genuinely playable file always wins over a
	// stale failure record.
	ttlHours := cfg.Stremio.NzbTTLHours
	season, episode := 0, 0
	if selector != nil {
		season, episode = selector.Season, selector.Episode
	}
	normTarget := normalizeTitleForMatching(cand.SafeTitle)
	if libraryStreams := s.findHealthyLibraryStreams(ctx, cfg, streamType, imdbID, baseURL, key, season, episode); len(libraryStreams) > 0 {
		for _, lib := range libraryStreams {
			if libURL, ok := lib["url"].(string); ok && libURL != "" {
				libTitle, _ := lib["title"].(string)
				normLibrary := normalizeTitleForMatching(libTitle)
				if cand.SafeTitle == "" || normTarget == "" || normLibrary == normTarget || (normLibrary != "" && strings.Contains(normTarget, normLibrary)) {
					slog.InfoContext(ctx, "Returning local library Stremio stream",
						"nzb_name", cand.SafeTitle, "library_url", libURL)
					return c.Redirect(libURL, fiber.StatusFound)
				}
			}
		}
	}
	if normTarget != "" {
		if cachedItems, err := s.queueRepo.GetCachedStremioQueueItems(ctx, cfg.Stremio.EffectiveIncludeLibraryStreams(), cfg.Stremio.NzbTTLHours); err == nil {
			for _, prev := range cachedItems {
				if prev == nil || prev.StoragePath == nil || *prev.StoragePath == "" {
					continue
				}
				if ttlHours > 0 && prev.CompletedAt != nil && time.Since(*prev.CompletedAt) >= time.Duration(ttlHours)*time.Hour {
					continue
				}
				normNzb := normalizeTitleForMatching(prev.NzbPath)
				normStorage := ""
				if prev.StoragePath != nil {
					normStorage = normalizeTitleForMatching(*prev.StoragePath)
				}
				normRel := ""
				if prev.RelativePath != nil {
					normRel = normalizeTitleForMatching(*prev.RelativePath)
				}

				if normNzb == normTarget || (normStorage != "" && normStorage == normTarget) || (normRel != "" && normRel == normTarget) {
					if streams, err := s.buildStremioStreams(ctx, prev, baseURL, key, cand.SafeTitle, selector); err == nil && len(streams) > 0 {
						slog.InfoContext(ctx, "Returning cached Stremio stream",
							"nzb_name", cand.SafeTitle, "matched_path", prev.NzbPath, "indexer", cand.Indexer)
						return c.Redirect(streams[0].URL, fiber.StatusFound)
					}
				}
			}
		}
	}

	return s.playStremioWithFallback(c, cfg, cand, playRequest{
		baseURL:         baseURL,
		key:             key,
		streamType:      streamType,
		imdbID:          imdbID,
		selector:        selector,
		explicitRelease: true,
	})
}

// playRequest carries the per-request context the fallback loop needs, keeping the
// loop signature readable.
type playRequest struct {
	baseURL         string
	key             string
	streamType      string
	imdbID          string
	selector        *stremioEpisodeSelector
	explicitRelease bool
}

const (
	// stremioPlayBudget is the total wall clock a /play request may spend across all
	// attempts. Unchanged from the original single-attempt timeout, so client
	// behaviour is unaffected.
	stremioPlayBudget = 300 * time.Second
	// stremioMinAttemptBudget is the minimum time left worth starting another attempt.
	stremioMinAttemptBudget = 20 * time.Second
)

// playStremioWithFallback queues the requested release and waits for it, advancing to
// the next candidate release when one fails. Without this, a release that fails is
// simply re-downloaded and re-queued on every play, forever.
func (s *Server) playStremioWithFallback(
	c *fiber.Ctx,
	cfg *config.Config,
	cand stremioPlayCandidate,
	req playRequest,
) error {
	ctx := c.Context()

	// Candidates are resolved lazily: the happy path must not pay for a second
	// Prowlarr search.
	var (
		cands       []stremioPlayCandidate
		candsLoaded bool
		curIdx      = -1
	)
	loadCandidates := func() {
		if candsLoaded {
			return
		}
		candsLoaded = true
		if req.imdbID == "" {
			return
		}
		prowlarrType := "search"
		switch req.streamType {
		case "movie":
			prowlarrType = "movie"
		case "series":
			prowlarrType = "tvsearch"
		}
		season, episode := 0, 0
		if req.selector != nil {
			season, episode = req.selector.Season, req.selector.Episode
		}
		results, _, err := s.searchStremioReleases(ctx, cfg, req.streamType, prowlarrType, req.imdbID, season, episode)
		if err != nil {
			slog.WarnContext(ctx, "Stremio fallback search failed", "error", err, "imdb_id", req.imdbID)
			return
		}
		cands = stremioCandidates(results, req.imdbID)
	}

	// advance moves to the next candidate, returning false when fallback is disabled,
	// unavailable, or exhausted.
	advance := func() bool {
		if cfg.Stremio.EffectiveMaxFallbackReleases() == 0 || req.imdbID == "" {
			return false
		}
		loadCandidates()
		next, ok := nextStremioCandidate(cands, cand.SafeTitle, curIdx)
		if !ok {
			return false
		}
		curIdx = next
		cand = cands[next]
		// Only the release selected by the client bypasses content-level
		// coalescing. Releases reached through fallback must wait for another
		// active release of the same movie/episode to finish.
		req.explicitRelease = false
		slog.InfoContext(ctx, "Falling back to next Stremio release",
			"imdb_id", req.imdbID, "title", cand.SafeTitle)
		return true
	}

	deadline := time.Now().Add(stremioPlayBudget)
	maxAttempts := 1 + cfg.Stremio.EffectiveMaxFallbackReleases()
	attempts := 0

	// A stale client-cached stream list can point straight at a release we already know
	// is bad. Skip the download entirely and start from the next candidate.
	if s.stremioIsFailed(ctx, cfg, cand.SafeTitle) {
		slog.InfoContext(ctx, "Skipping known-failed Stremio release", "title", cand.SafeTitle)
		if !advance() {
			season, episode := 0, 0
			if req.selector != nil {
				season, episode = req.selector.Season, req.selector.Episode
			}
			if libraryStreams := s.findHealthyLibraryStreams(ctx, cfg, req.streamType, req.imdbID, req.baseURL, req.key, season, episode); len(libraryStreams) > 0 {
				if libURL, ok := libraryStreams[0]["url"].(string); ok && libURL != "" {
					slog.InfoContext(ctx, "Falling back to healthy local library stream",
						"imdb_id", req.imdbID, "library_url", libURL)
					return c.Redirect(libURL, fiber.StatusFound)
				}
			}
			return RespondServiceUnavailable(c, "No playable release found", "release previously failed")
		}
	}

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		remaining := time.Until(deadline)
		if attempt > 1 && remaining < stremioMinAttemptBudget {
			break
		}
		attempts++

		itemID, err := s.enqueueStremioRelease(ctx, cfg, cand, req.streamType, req.imdbID, req.selector, req.explicitRelease)
		if err != nil {
			var inFlightErr *stremioContentInFlightError
			if errors.As(err, &inFlightErr) {
				out := s.waitForStream(ctx, inFlightErr.itemID, req.baseURL, req.key, cand.SafeTitle, req.selector, remaining)
				switch out.Kind {
				case streamOutcomeReady:
					// The in-flight item for this content finished successfully and is ready to stream.
					s.stremioFailures.Forget(cand.SafeTitle)
					return c.Redirect(out.StreamURL, fiber.StatusFound)
				case streamOutcomeFailed, streamOutcomeNoStreams:
					// The active release is definitively unusable. The requested
					// release may now be queued on the next loop iteration.
					continue
				default:
					return respondStreamOutcome(c, out)
				}
			}
			// Pre-queue failure (dead Prowlarr URL, malformed NZB): no queue row will
			// ever exist, so the in-memory cache is the only place this can be recorded.
			slog.WarnContext(ctx, "Failed to prepare Stremio NZB stream",
				"error", err, "title", cand.SafeTitle)
			s.stremioFailures.Record(cand.SafeTitle)
			if !advance() {
				season, episode := 0, 0
				if req.selector != nil {
					season, episode = req.selector.Season, req.selector.Episode
				}
				if libraryStreams := s.findHealthyLibraryStreams(ctx, cfg, req.streamType, req.imdbID, req.baseURL, req.key, season, episode); len(libraryStreams) > 0 {
					if libURL, ok := libraryStreams[0]["url"].(string); ok && libURL != "" {
						slog.InfoContext(ctx, "Falling back to healthy local library stream",
							"imdb_id", req.imdbID, "library_url", libURL)
						return c.Redirect(libURL, fiber.StatusFound)
					}
				}
				return RespondServiceUnavailable(c, "Failed to prepare NZB stream", err.Error())
			}
			continue
		}

		out := s.waitForStream(ctx, itemID, req.baseURL, req.key, cand.SafeTitle, req.selector, remaining)
		switch out.Kind {
		case streamOutcomeReady:
			// Self-healing: a release that plays is no longer excluded.
			s.stremioFailures.Forget(cand.SafeTitle)
			return c.Redirect(out.StreamURL, fiber.StatusFound)

		case streamOutcomeNoStreams:
			// The row is 'completed', so GetFailedStremioQueueItems will never see it
			// and it would otherwise keep its "⚡ Cached" badge.
			s.stremioFailures.Record(cand.SafeTitle)
			if !advance() {
				season, episode := 0, 0
				if req.selector != nil {
					season, episode = req.selector.Season, req.selector.Episode
				}
				if libraryStreams := s.findHealthyLibraryStreams(ctx, cfg, req.streamType, req.imdbID, req.baseURL, req.key, season, episode); len(libraryStreams) > 0 {
					if libURL, ok := libraryStreams[0]["url"].(string); ok && libURL != "" {
						slog.InfoContext(ctx, "Falling back to healthy local library stream",
							"imdb_id", req.imdbID, "library_url", libURL)
						return c.Redirect(libURL, fiber.StatusFound)
					}
				}
				return respondStreamOutcome(c, out)
			}

		case streamOutcomeFailed:
			// The failed import_queue row already records this durably.
			if !advance() {
				season, episode := 0, 0
				if req.selector != nil {
					season, episode = req.selector.Season, req.selector.Episode
				}
				if libraryStreams := s.findHealthyLibraryStreams(ctx, cfg, req.streamType, req.imdbID, req.baseURL, req.key, season, episode); len(libraryStreams) > 0 {
					if libURL, ok := libraryStreams[0]["url"].(string); ok && libURL != "" {
						slog.InfoContext(ctx, "Falling back to healthy local library stream",
							"imdb_id", req.imdbID, "library_url", libURL)
						return c.Redirect(libURL, fiber.StatusFound)
					}
				}
				return respondStreamOutcome(c, out)
			}

		default:
			// Ambiguous, timeout and unavailable are not evidence the release is bad.
			return respondStreamOutcome(c, out)
		}
	}

	season, episode := 0, 0
	if req.selector != nil {
		season, episode = req.selector.Season, req.selector.Episode
	}
	if libraryStreams := s.findHealthyLibraryStreams(ctx, cfg, req.streamType, req.imdbID, req.baseURL, req.key, season, episode); len(libraryStreams) > 0 {
		if libURL, ok := libraryStreams[0]["url"].(string); ok && libURL != "" {
			slog.InfoContext(ctx, "Falling back to healthy local library stream",
				"imdb_id", req.imdbID, "library_url", libURL)
			return c.Redirect(libURL, fiber.StatusFound)
		}
	}

	slog.WarnContext(ctx, "No playable Stremio release found",
		"imdb_id", req.imdbID, "attempts", attempts, "candidates", len(cands))
	return RespondServiceUnavailable(c, "No playable release found",
		fmt.Sprintf("tried %d release(s), none playable", attempts))
}

// stremioIsFailed reports whether a release is currently excluded, consulting both the
// durable (import_queue) and process-local halves of the exclusion set.
func (s *Server) stremioIsFailed(ctx context.Context, cfg *config.Config, safeTitle string) bool {
	if s.stremioFailures.Has(safeTitle, stremioFailedTTL(cfg)) {
		return true
	}
	if s.queueRepo == nil {
		return false
	}
	failed, err := s.queueRepo.GetFailedStremioQueueItems(ctx)
	if err != nil {
		return false
	}
	return stremioFailedPredicate(failed, nil, cfg.Stremio.FailedReleaseTTLHours, time.Now())(safeTitle)
}

// enqueueStremioRelease downloads the NZB and adds it to the import queue, coalescing
// concurrent callers for the same release so it is downloaded once.
//
// It returns as soon as the queue item exists. The wait deliberately stays outside the
// singleflight group: holding the group for the whole import would serialize every
// concurrent viewer of the release behind one request.
func (s *Server) enqueueStremioRelease(
	ctx context.Context,
	cfg *config.Config,
	cand stremioPlayCandidate,
	streamType string,
	imdbID string,
	selector *stremioEpisodeSelector,
	explicitRelease bool,
) (int64, error) {
	safeFilename := cand.SafeTitle + ".nzb"
	ttlHours := cfg.Stremio.NzbTTLHours
	contentKey := stremioContentKey(streamType, imdbID, selector)
	releaseKey := stremioReleaseKey(cand)

	var indexerPtr *string
	if cand.Indexer != "" {
		indexer := cand.Indexer
		indexerPtr = &indexer
	}

	queue := func() (interface{}, error) {
		if contentKey != "" {
			active, findErr := s.findActiveStremioContent(ctx, contentKey)
			if findErr != nil {
				return nil, fmt.Errorf("find active Stremio content: %w", findErr)
			}
			if active != nil {
				if stremioQueueMetadataMatches(active, contentKey, releaseKey) {
					return active.ID, nil
				}
				return nil, &stremioContentInFlightError{itemID: active.ID}
			}
		}

		v, err, _ := s.stremioPlayGroup.Do(releaseKey, func() (interface{}, error) {
			// Serialized per release: reuse an in-flight or TTL-fresh import instead of re-downloading.
			if items, e := s.queueRepo.ListQueueItems(ctx, nil, safeFilename, "", 1, 0, "updated_at", "desc"); e == nil && len(items) > 0 {
				it := items[0]
				// Current Stremio rows carry a release fingerprint. Never attach a
				// request for a different indexer URL merely because both releases
				// sanitize to the same filename.
				if it.Metadata != nil {
					var metadata stremioQueueMetadata
					if json.Unmarshal([]byte(*it.Metadata), &metadata) == nil && metadata.ReleaseKey != "" && metadata.ReleaseKey != releaseKey {
						it = nil
					}
				} else {
					// Legacy rows have no trustworthy release identity. Do not attach
					// a new Stremio request to one based on its sanitized filename.
					it = nil
				}
				if it == nil {
					// Continue with a new queue item for the explicitly selected release.
				} else {
					switch it.Status {
					case database.QueueStatusPending, database.QueueStatusProcessing, database.QueueStatusPaused:
						return it.ID, nil
					case database.QueueStatusCompleted:
						reusable := it.StoragePath != nil && *it.StoragePath != ""
						if reusable && ttlHours > 0 && it.CompletedAt != nil {
							reusable = time.Since(*it.CompletedAt) < time.Duration(ttlHours)*time.Hour
						}
						if reusable {
							return it.ID, nil
						}
					}
				}
			}

			if s.importerService == nil {
				return nil, fmt.Errorf("importer service not available")
			}

			// Detach from the caller's request so one client disconnecting won't abort shared work.
			workCtx := context.WithoutCancel(ctx)

			// Unique per-request staging dir so concurrent plays never share a temp file.
			uploadDir := filepath.Join(os.TempDir(), "altmount-uploads")
			if err := os.MkdirAll(uploadDir, 0755); err != nil {
				return nil, fmt.Errorf("failed to create upload directory: %w", err)
			}
			stageDir, err := os.MkdirTemp(uploadDir, "play-*")
			if err != nil {
				return nil, fmt.Errorf("failed to create staging directory: %w", err)
			}
			// Importer moves the NZB out on success; this clears the staged file / empty dir.
			defer os.RemoveAll(stageDir)
			tempPath := filepath.Join(stageDir, safeFilename)

			nzbData, err := downloadStremioNZB(workCtx, cfg, cand)
			if err != nil {
				return nil, fmt.Errorf("failed to download NZB: %w", err)
			}
			if err := os.WriteFile(tempPath, nzbData, 0644); err != nil {
				return nil, fmt.Errorf("failed to write NZB temp file: %w", err)
			}

			var basePath *string
			if completeDir := cfg.SABnzbd.CompleteDir; completeDir != "" {
				basePath = &completeDir
			}

			priority := database.QueuePriorityHigh
			// Map Stremio stream type to Newznab category name so downloads land in the
			// correct folder (matches default SABnzbd category config).
			category := "Movies"
			tmdbID := 0
			tvdbID := 0
			switch streamType {
			case "series":
				category = "TV"
				if tvdbIDStr, _, _ := resolveSeriesMetadataFromIMDb(workCtx, imdbID, cfg.GetUserAgent()); tvdbIDStr != "" {
					tvdbID, _ = strconv.Atoi(tvdbIDStr)
				}
			case "movie":
				tmdbID, _, _, _ = resolveMovieMetadataFromIMDb(workCtx, imdbID, cfg.GetUserAgent())
			}
			stremioDownloadID := stremioDownloadIDPrefix + uuid.NewString()
			metaJSONPtr, metadataErr := encodeStremioQueueMetadata(stremioQueueMetadata{
				ContentKey: contentKey,
				ReleaseKey: releaseKey,
				IMDbID:     imdbID,
				Type:       streamType,
				TMDBID:     tmdbID,
				TVDBID:     tvdbID,
			})
			if metadataErr != nil {
				return nil, fmt.Errorf("failed to encode Stremio queue metadata: %w", metadataErr)
			}
			if selector != nil {
				var metadata stremioQueueMetadata
				if err := json.Unmarshal([]byte(*metaJSONPtr), &metadata); err != nil {
					return nil, err
				}
				metadata.Season = selector.Season
				metadata.Episode = selector.Episode
				metaJSONPtr, metadataErr = encodeStremioQueueMetadata(metadata)
				if metadataErr != nil {
					return nil, fmt.Errorf("failed to encode Stremio episode metadata: %w", metadataErr)
				}
			}
			item, err := s.importerService.AddToQueue(workCtx, tempPath, basePath, &category, &priority, metaJSONPtr, &stremioDownloadID, indexerPtr)
			if err != nil {
				return nil, fmt.Errorf("failed to add NZB to queue: %w", err)
			}

			slog.InfoContext(ctx, "Stremio stream NZB queued for play",
				"queue_id", item.ID, "title", cand.SafeTitle, "indexer", cand.Indexer)
			return item.ID, nil
		})
		return v, err
	}

	var v interface{}
	var err error
	if contentKey != "" {
		v, err, _ = s.stremioContentGroup.Do(contentKey, queue)
	} else {
		v, err = queue()
	}
	if err != nil {
		return 0, err
	}

	itemID, ok := v.(int64)
	if !ok {
		return 0, fmt.Errorf("unexpected play result type %T", v)
	}
	if contentKey != "" {
		item, lookupErr := s.queueRepo.GetQueueItem(ctx, itemID)
		if lookupErr == nil && item != nil && item.Status != database.QueueStatusCompleted && item.Status != database.QueueStatusFailed && !stremioQueueMetadataMatches(item, contentKey, releaseKey) {
			return 0, &stremioContentInFlightError{itemID: item.ID}
		}
	}
	return itemID, nil
}

// streamOutcomeKind classifies how a wait for a queue item ended. The distinction
// drives fallback: only definitive, release-specific failures justify trying another
// release.
type streamOutcomeKind int

const (
	// streamOutcomeReady: a playable stream URL is available.
	streamOutcomeReady streamOutcomeKind = iota
	// streamOutcomeFailed: the import failed. A 'failed' row now exists in
	// import_queue, so exclusion is already durably recorded.
	streamOutcomeFailed
	// streamOutcomeNoStreams: the import completed but produced nothing playable.
	// The row is 'completed', so this must be recorded in the in-memory cache.
	streamOutcomeNoStreams
	// streamOutcomeAmbiguous: a multi-episode pack with no episode selector. A client
	// problem, not a bad release -- never falls back, never recorded.
	streamOutcomeAmbiguous
	// streamOutcomeTimeout: still importing. Never falls back (the item is still
	// running and a second high-priority import would contend for the same connection
	// pool) and never recorded.
	streamOutcomeTimeout
	// streamOutcomeUnavailable: the wait could not be performed at all.
	streamOutcomeUnavailable
)

// streamOutcome is the result of waiting on a queue item.
type streamOutcome struct {
	Kind      streamOutcomeKind
	StreamURL string
	Detail    string
}

// waitForStream waits for a queue item to become streamable and classifies the result.
// Unlike waitAndRedirectToStream it never writes to the HTTP response, so the caller
// can decide between redirecting and falling back to another release.
func (s *Server) waitForStream(
	ctx context.Context,
	itemID int64,
	baseURL, downloadKey, nzbName string,
	selector *stremioEpisodeSelector,
	timeout time.Duration,
) streamOutcome {
	// Subscribe before reading status so an event fired between the two is not missed.
	subID, ch := s.progressBroadcaster.Subscribe()
	defer s.progressBroadcaster.Unsubscribe(subID)

	current, err := s.queueRepo.GetQueueItem(ctx, itemID)
	if err != nil || current == nil {
		return streamOutcome{Kind: streamOutcomeUnavailable, Detail: "queue item not found"}
	}

	firstStream := func(item *database.ImportQueueItem) streamOutcome {
		streams, err := s.buildStremioStreams(ctx, item, baseURL, downloadKey, nzbName, selector)
		if err != nil {
			if errors.Is(err, errStremioEpisodeAmbiguous) {
				return streamOutcome{Kind: streamOutcomeAmbiguous}
			}
			return streamOutcome{Kind: streamOutcomeNoStreams, Detail: err.Error()}
		}
		if len(streams) == 0 {
			return streamOutcome{Kind: streamOutcomeNoStreams, Detail: "no media files in release"}
		}
		return streamOutcome{Kind: streamOutcomeReady, StreamURL: streams[0].URL}
	}

	switch current.Status {
	case database.QueueStatusCompleted:
		return firstStream(current)
	case database.QueueStatusFailed:
		detail := ""
		if current.ErrorMessage != nil {
			detail = *current.ErrorMessage
		}
		return streamOutcome{Kind: streamOutcomeFailed, Detail: detail}
	default:
		// If the item is already processing and has a storage path, the streamable
		// event fired before we subscribed — use it immediately.
		if current.StoragePath != nil && *current.StoragePath != "" {
			if out := firstStream(current); out.Kind == streamOutcomeReady {
				return out
			}
		}
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case update, ok := <-ch:
			if !ok {
				return streamOutcome{Kind: streamOutcomeUnavailable, Detail: "progress channel closed"}
			}
			if update.QueueID != int(itemID) {
				continue
			}
			switch update.Status {
			case "streamable":
				// Serve as soon as the file is accessible in the VFS — before post-processing.
				if update.StoragePath != "" {
					fakeItem := &database.ImportQueueItem{ID: itemID, StoragePath: &update.StoragePath}
					if out := firstStream(fakeItem); out.Kind == streamOutcomeReady {
						return out
					}
				}
				// No media files yet — fall through to wait for completed.
			case "completed":
				item, err := s.queueRepo.GetQueueItem(ctx, itemID)
				if err != nil {
					return streamOutcome{Kind: streamOutcomeUnavailable, Detail: "failed to fetch queue item"}
				}
				return firstStream(item)
			case "failed":
				return streamOutcome{Kind: streamOutcomeFailed}
			}
		case <-timer.C:
			return streamOutcome{
				Kind:   streamOutcomeTimeout,
				Detail: fmt.Sprintf("did not complete within %s", timeout),
			}
		}
	}
}

// respondStreamOutcome writes the HTTP response for a non-ready outcome.
func respondStreamOutcome(c *fiber.Ctx, out streamOutcome) error {
	switch out.Kind {
	case streamOutcomeReady:
		return c.Redirect(out.StreamURL, fiber.StatusFound)
	case streamOutcomeAmbiguous:
		return respondEpisodeAmbiguous(c)
	case streamOutcomeFailed:
		return RespondServiceUnavailable(c, "NZB processing failed", out.Detail)
	case streamOutcomeNoStreams:
		return RespondServiceUnavailable(c, "No streams available", out.Detail)
	case streamOutcomeTimeout:
		return RespondServiceUnavailable(c, "Processing timed out", out.Detail)
	default:
		return RespondServiceUnavailable(c, "Failed to prepare NZB stream", out.Detail)
	}
}

// validateDownloadKey returns true if key matches any user's hashed API key.
func (s *Server) validateDownloadKey(ctx context.Context, key string) bool {
	if s.userRepo == nil || key == "" {
		return false
	}
	users, err := s.userRepo.GetAllUsers(ctx)
	if err != nil {
		return false
	}
	for _, user := range users {
		if user.APIKey == nil || *user.APIKey == "" {
			continue
		}
		if subtle.ConstantTimeCompare([]byte(auth.HashAPIKey(*user.APIKey)), []byte(key)) == 1 {
			return true
		}
	}
	return false
}

// prowlarrIndexersRequest carries optional connection overrides so the config UI
// can list indexers before the Prowlarr settings are saved.
type prowlarrIndexersRequest struct {
	Host   string `json:"host"`
	APIKey string `json:"api_key"`
}

// handleListProwlarrIndexers returns the usenet indexers configured in Prowlarr
// so the Stremio config UI can present them for selection. Host and API key may
// be supplied in the body (to test unsaved values) or fall back to saved config.
//
//	@Summary		List Prowlarr indexers
//	@Description	Returns the usenet indexers configured in Prowlarr for the Stremio addon.
//	@Tags			Config
//	@Accept			json
//	@Produce		json
//	@Param			body	body		prowlarrIndexersRequest	false	"Optional Prowlarr connection overrides"
//	@Success		200		{object}	APIResponse
//	@Failure		400		{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/prowlarr/indexers [post]
func (s *Server) handleListProwlarrIndexers(c *fiber.Ctx) error {
	if s.configManager == nil {
		return RespondServiceUnavailable(c, "Configuration not available", "")
	}

	var req prowlarrIndexersRequest
	// Body is optional; ignore parse errors and fall back to saved config.
	_ = c.BodyParser(&req)

	cfg := s.configManager.GetConfig()
	host := strings.TrimSpace(req.Host)
	apiKey := strings.TrimSpace(req.APIKey)
	// Fall back to the stored Prowlarr configuration so masked credentials
	// work without re-entry: the per-indexer section is authoritative, the
	// legacy stremio.prowlarr section is the fallback.
	if host == "" {
		if host = cfg.Stremio.Indexers.Prowlarr.Host; host == "" {
			host = cfg.Stremio.Prowlarr.Host
		}
	}
	if apiKey == "" {
		if apiKey = cfg.Stremio.Indexers.Prowlarr.APIKey; apiKey == "" {
			apiKey = cfg.Stremio.Prowlarr.APIKey
		}
	}

	if host == "" || apiKey == "" {
		return RespondValidationError(c, "Prowlarr host and API key are required", "")
	}

	client := prowlarr.NewClient(host, apiKey, httpclient.NewForExternal(cfg.Network, 30*time.Second))
	indexers, err := client.GetIndexers(c.Context())
	if err != nil {
		return RespondBadRequest(c, "Failed to list Prowlarr indexers", err.Error())
	}

	return RespondSuccess(c, indexers)
}

type newsnabTestRequest struct {
	ID     string `json:"id"`
	URL    string `json:"url"`
	APIKey string `json:"api_key"`
}

func (s *Server) handleTestNewsnabIndexer(c *fiber.Ctx) error {
	var req newsnabTestRequest
	if err := c.BodyParser(&req); err != nil {
		return RespondBadRequest(c, "Invalid request payload", err.Error())
	}

	reqURL := strings.TrimSpace(req.URL)

	// A masked stored API key arrives empty; when the request identifies a
	// configured indexer, fall back to its stored credentials so testing an
	// existing indexer does not require re-typing the key.
	if strings.TrimSpace(req.APIKey) == "" && strings.TrimSpace(req.ID) != "" {
		for _, n := range s.configManager.GetConfig().Stremio.Indexers.Newsnab {
			if n.ID == strings.TrimSpace(req.ID) {
				if reqURL == "" {
					reqURL = n.URL
				}
				req.APIKey = n.APIKey
				break
			}
		}
	}

	if reqURL == "" || req.APIKey == "" {
		return RespondValidationError(c, "Indexer URL and API Key are required", "")
	}

	cfg := s.configManager.GetConfig()
	client := newsnab.NewClient(newsnab.IndexerConfig{
		Name:           "Test Indexer",
		URL:            reqURL,
		APIKey:         req.APIKey,
		TimeoutSeconds: 6,
		Enabled:        true,
	}, httpclient.NewForExternal(cfg.Network, 10*time.Second))

	ua := stremio.GetUserAgentManager().GetUserAgent("movie", cfg.Stremio.Indexers.CustomUserAgent)
	caps, err := client.CheckCaps(c.Context(), ua)
	if err != nil {
		return RespondBadRequest(c, "Failed to connect to Newsnab indexer", err.Error())
	}

	return RespondSuccess(c, fiber.Map{
		"server_name": caps.ServerName,
		"categories":  caps.Categories,
		"status":      "ok",
		"capabilities": fiber.Map{
			"search":         caps.Search,
			"movie_search":   caps.MovieSearch,
			"tv_search":      caps.TVSearch,
			"search_params":  caps.SearchParams,
			"movie_params":   caps.MovieParams,
			"tv_params":      caps.TVParams,
			"category_count": len(caps.Categories),
		},
	})
}

func (s *Server) handleGetStremioUserAgents(c *fiber.Ctx) error {
	mgr := stremio.GetUserAgentManager()
	return RespondSuccess(c, mgr.GetInfo())
}

func (s *Server) handleRefreshStremioUserAgents(c *fiber.Ctx) error {
	mgr := stremio.GetUserAgentManager()
	ctx := c.Context()

	cfg := s.configManager.GetConfig()
	if cfg != nil {
		_ = mgr.FetchLatestFromGitHub(ctx, cfg.GetUserAgent())
		sonarrURL, sonarrKey := firstEnabledARR(cfg.Arrs.SonarrInstances)
		radarrURL, radarrKey := firstEnabledARR(cfg.Arrs.RadarrInstances)
		_ = mgr.CheckLocalARRs(ctx, sonarrURL, sonarrKey, radarrURL, radarrKey)
	} else {
		_ = mgr.FetchLatestFromGitHub(ctx, "")
	}

	return RespondSuccess(c, mgr.GetInfo())
}

func firstEnabledARR(instances []config.ArrsInstanceConfig) (string, string) {
	for _, instance := range instances {
		if instance.Enabled != nil && !*instance.Enabled {
			continue
		}
		if instance.URL != "" && instance.APIKey != "" {
			return instance.URL, instance.APIKey
		}
	}
	return "", ""
}
