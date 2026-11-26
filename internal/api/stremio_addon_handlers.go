package api

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
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
	if cfg.Stremio.SkipSearchWhenCached {
		cachedStreams := s.findCachedStremioStreams(ctx, cfg, streamType, imdbID, baseURL, key, season, episode)
		seen := make(map[string]bool, len(libraryStreams))
		for _, stream := range libraryStreams {
			streamURL, _ := stream["url"].(string)
			seen[streamURL] = true
		}
		for _, stream := range cachedStreams {
			if !seen[stream.URL] {
				libraryStreams = append(libraryStreams, fiber.Map{"name": stream.Name, "title": stream.Title, "url": stream.URL})
				seen[stream.URL] = true
			}
		}
		if len(libraryStreams) > 0 {
			return c.JSON(fiber.Map{"streams": s.labelNetworkHealthy(ctx, libraryStreams)})
		}
	}
	libraryStreams = s.labelNetworkHealthy(ctx, libraryStreams)
	// Releases other nodes announced for this content with a bundle this node
	// can fetch (nil with the peer network off: nothing is added or asked).
	// They are looked up while the indexer search runs, never before it.
	waitPeerStreams := s.startPeerCandidates(ctx, cfg, streamType, imdbID, baseURL, key, season, episode)

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
	peerStreams := waitPeerStreams()
	if err != nil && len(libraryStreams) == 0 && len(peerStreams) == 0 {
		return emptyStreamsResponse(c)
	}

	entries := buildStremioStreamEntries(results, cachedItems, cfg.Stremio.NzbTTLHours, time.Now(),
		baseURL, key, streamType, season, episode, imdbID,
		resolveStreamDisplay(cfg.Stremio), s.storeStremioReleaseRef)

	streams := assembleStremioStreams(libraryStreams, peerStreams, entries)

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
	display := resolveStreamDisplay(cfg.Stremio)
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
					name, title := s.libraryStreamLabels(display, h)
					libraryStreams = append(libraryStreams, fiber.Map{
						"name":  name,
						"title": title,
						"url":   streamURL,
					})
				}
			}
		}
	} else if streamType == "series" {
		tvdbIDStr, seriesTitle, _ := resolveSeriesMetadataFromIMDb(ctx, imdbID, cfg.GetUserAgent())
		tvdbID, _ := strconv.Atoi(tvdbIDStr)
		matchCtx := releaseMatchContext{aliases: resolveSeriesTitleAliases(ctx, imdbID, cfg.GetUserAgent())}
		if healthyFiles, err := s.healthRepo.FindHealthyFilesForSeries(ctx, seriesTitle, tvdbID); err == nil {
			for _, h := range healthyFiles {
				if h != nil && h.FilePath != "" && isMediaExtension(filepath.Ext(h.FilePath)) && !isSampleFile(h.FilePath) {
					matchPath := h.FilePath
					if h.LibraryPath != nil && *h.LibraryPath != "" {
						matchPath = *h.LibraryPath
					}
					if selector == nil || selector.matches(matchPath) || selector.matches(h.FilePath) {
						// Verify series title matches if seriesTitle is known (prevents cross-show false positives)
						if seriesTitle != "" && !releaseMatchesContent("series", filepath.Base(matchPath), seriesTitle, "", season, episode, matchCtx) && !releaseMatchesContent("series", matchPath, seriesTitle, "", season, episode, matchCtx) {
							continue
						}
						streamURL := baseURL + "/api/files/stream?path=" +
							url.QueryEscape(h.FilePath) + "&download_key=" + url.QueryEscape(key)
						name, title := s.libraryStreamLabels(display, h)
						libraryStreams = append(libraryStreams, fiber.Map{
							"name":  name,
							"title": title,
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
	Size        int64 // indexer-reported size, used only when there is no GUID
}

type stremioReleaseRef struct {
	Candidate stremioPlayCandidate
	ExpiresAt time.Time
}

// stremioReleaseToken derives a play link's release token from the release
// itself instead of from the listing that offered it. Every listing, every
// reopened link and every process therefore presents the same token for the
// same release, so a client that saved one play link keeps naming that release.
//
// The indexer and its GUID identify the release. An indexer that reports no
// GUID falls back to the normalized release name and size. Tokens minted before
// this scheme were random; they still resolve through the stored reference, and
// a link whose token is unknown takes the same recovery path either way.
func stremioReleaseToken(candidate stremioPlayCandidate) string {
	identity := strings.ToLower(strings.TrimSpace(candidate.Source)) + "\x00" +
		strings.ToLower(strings.TrimSpace(candidate.Indexer)) + "\x00"
	if guid := strings.TrimSpace(candidate.GUID); guid != "" {
		identity += strings.ToLower(guid)
	} else {
		identity += normalizeTitleForMatching(candidate.SafeTitle) + "\x00" + strconv.FormatInt(candidate.Size, 10)
	}
	sum := sha256.Sum256([]byte(identity))
	return fmt.Sprintf("r2-%x", sum[:16])
}

func (s *Server) storeStremioReleaseRef(candidate stremioPlayCandidate) (string, error) {
	token := stremioReleaseToken(candidate)
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
	s.stremioReleaseRefs[token] = stremioReleaseRef{Candidate: candidate, ExpiresAt: now.Add(s.stremioReleaseLifetime())}
	return token, nil
}

// minStremioReleaseLifetime keeps play links usable through a film even when
// stream_ttl_seconds is set very low.
const minStremioReleaseLifetime = time.Hour

// stremioReleaseLifetime is how long a play link's release token stays valid:
// as long as Stremio may keep showing the stream list it came from
// (stream_ttl_seconds, default 24h). Players re-request the play link when
// they seek or reconnect, so a short lifetime broke rewinding mid-film.
func (s *Server) stremioReleaseLifetime() time.Duration {
	lifetime := 24 * time.Hour
	if s.configManager != nil {
		if ttl := s.configManager.GetConfig().Stremio.StreamTTLSeconds; ttl > 0 {
			lifetime = time.Duration(ttl) * time.Second
		}
	}
	return max(lifetime, minStremioReleaseLifetime)
}

// consumeStremioReleaseRef resolves a play link's release token. A token
// that is used is kept alive for another full lifetime, so playback that
// re-requests the link keeps working however long it runs.
func (s *Server) consumeStremioReleaseRef(token string) (stremioPlayCandidate, bool) {
	lifetime := s.stremioReleaseLifetime()
	s.stremioReleaseMu.Lock()
	defer s.stremioReleaseMu.Unlock()
	ref, ok := s.stremioReleaseRefs[token]
	now := time.Now()
	if !ok || !ref.ExpiresAt.After(now) {
		if ok {
			delete(s.stremioReleaseRefs, token)
		}
		return stremioPlayCandidate{}, false
	}
	ref.ExpiresAt = now.Add(lifetime)
	s.stremioReleaseRefs[token] = ref
	return ref.Candidate, true
}

type stremioQueueMetadata struct {
	ContentKey string `json:"content_key"`
	ReleaseKey string `json:"release_key"`
	// ReleaseToken is the stable play-link token derived from the release. It
	// outlives the listing's URL, so a play link whose URL-derived release key
	// no longer matches can still find the import it names.
	ReleaseToken string `json:"release_token,omitempty"`
	IMDbID       string `json:"imdb_id,omitempty"`
	Type         string `json:"type,omitempty"`
	Season       int    `json:"season,omitempty"`
	Episode      int    `json:"episode,omitempty"`
	TMDBID       int    `json:"tmdbId,omitempty"`
	TVDBID       int    `json:"tvdbId,omitempty"`
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
	// The download URL identifies the exact NZB bytes the indexer serves. GUIDs
	// and indexer IDs are reused across mirrored releases, so they only identify
	// a release when no URL is available.
	identity := candidate.DownloadURL
	if identity == "" {
		identity = candidate.IndexerID
	}
	if identity == "" {
		identity = candidate.GUID
	}
	hash := sha256.Sum256([]byte(identity))
	return fmt.Sprintf("%s:%s:%x", strings.ToLower(candidate.Source), strings.ToLower(candidate.Indexer), hash[:16])
}

// stremioPlayNzbFilename names the NZB a Stremio play queues. The URL hash keeps
// releases that sanitize to the same title on separate queue paths, so one
// release can never reuse the other's imported file.
func stremioPlayNzbFilename(candidate stremioPlayCandidate) string {
	hash := sha256.Sum256([]byte(candidate.DownloadURL))
	return fmt.Sprintf("%s-%x.nzb", candidate.SafeTitle, hash[:8])
}

// stremioQueueItemReleaseKey returns the release fingerprint stored on a queue
// item, when the row has one.
func stremioQueueItemReleaseKey(item *database.ImportQueueItem) (string, bool) {
	if item == nil || item.Metadata == nil || *item.Metadata == "" {
		return "", false
	}
	var metadata stremioQueueMetadata
	if json.Unmarshal([]byte(*item.Metadata), &metadata) != nil || metadata.ReleaseKey == "" {
		return "", false
	}
	return metadata.ReleaseKey, true
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

// stremioQueueItemReleaseToken returns the stable release token stored on a
// queue item, when the row has one. Rows written before the token was stored
// carry only the URL-derived release key and report false.
func stremioQueueItemReleaseToken(item *database.ImportQueueItem) (string, bool) {
	if item == nil || item.Metadata == nil || *item.Metadata == "" {
		return "", false
	}
	var metadata stremioQueueMetadata
	if json.Unmarshal([]byte(*item.Metadata), &metadata) != nil || metadata.ReleaseToken == "" {
		return "", false
	}
	return metadata.ReleaseToken, true
}

// stremioSelectTokenRelease returns the completed row an exact stable release
// token names. A stable token is derived from the release itself, so every
// listing of that release produces the same one; several rows can share it
// (for example a re-import), and the newest is the answer.
func stremioSelectTokenRelease(rows []*database.ImportQueueItem, releaseToken string, ttlHours int) *database.ImportQueueItem {
	if releaseToken == "" {
		return nil
	}
	matches := make([]*database.ImportQueueItem, 0, 1)
	for _, item := range rows {
		if item == nil || !reusableCompletedImport(item, ttlHours) {
			continue
		}
		storedToken, ok := stremioQueueItemReleaseToken(item)
		if !ok || storedToken != releaseToken {
			continue
		}
		matches = append(matches, item)
	}
	if len(matches) == 0 {
		return nil
	}
	return stremioNewestCompleted(matches)
}

// stremioQueueItemNameMatches reports whether a completed row holds the release
// whose normalized name is normTarget. A recovered play link that carries only
// the release name can still resolve to the bytes already imported under that
// name instead of searching and grabbing the release a second time.
//
// The importer's URL-hash suffix is removed from each basename first, so rows
// written before the stable token existed -- whose nzb, target and storage names
// all end in "-<url-hash>" -- are matched by the title the play link carries.
func stremioQueueItemNameMatches(item *database.ImportQueueItem, normTarget string) bool {
	if item == nil || normTarget == "" {
		return false
	}
	names := []string{item.NzbPath}
	for _, path := range []*string{item.TargetPath, item.StoragePath, item.RelativePath} {
		if path != nil && *path != "" {
			names = append(names, *path)
		}
	}
	for _, name := range names {
		if normalizeReleaseNameForMatching(name) == normTarget {
			return true
		}
	}
	return false
}

// stremioCompletedRowTimestamp is the timestamp that orders completed rows:
// when the import finished, falling back to its last update or creation for
// rows that predate completed_at. It matches the ordering used by the
// duplicate-lookup queries.
func stremioCompletedRowTimestamp(item *database.ImportQueueItem) time.Time {
	if item.CompletedAt != nil {
		return *item.CompletedAt
	}
	if !item.UpdatedAt.IsZero() {
		return item.UpdatedAt
	}
	return item.CreatedAt
}

// stremioNewestCompleted returns the completed row that finished last. The
// queue id breaks ties so the choice is deterministic when rows share a
// timestamp.
func stremioNewestCompleted(items []*database.ImportQueueItem) *database.ImportQueueItem {
	var newest *database.ImportQueueItem
	for _, item := range items {
		if newest == nil {
			newest = item
			continue
		}
		itemAt, newestAt := stremioCompletedRowTimestamp(item), stremioCompletedRowTimestamp(newest)
		if itemAt.After(newestAt) || (itemAt.Equal(newestAt) && item.ID > newest.ID) {
			newest = item
		}
	}
	return newest
}

// stremioIndexerAgrees reports whether the request and the completed row can be
// the same release by indexer. A side that does not know its indexer cannot
// disagree.
func stremioIndexerAgrees(item *database.ImportQueueItem, requestIndexer string) bool {
	requestIndexer = strings.TrimSpace(requestIndexer)
	if requestIndexer == "" || item.Indexer == nil {
		return true
	}
	rowIndexer := strings.TrimSpace(*item.Indexer)
	return rowIndexer == "" || strings.EqualFold(rowIndexer, requestIndexer)
}

// stremioSelectRecoveredRelease picks the completed row a play link recovered
// from its release name alone resolves to. It returns nil when the request does
// not name exactly one release, and the caller searches instead.
//
// One completed row with the normalized name is that release. With several,
// only rows carrying a content identity can be verified: they must all agree on
// one identity, and the newest of them wins. Distinct identities, or rows with
// no identity at all, mean the name could refer to any of them, so the match is
// ambiguous and is never guessed. A known indexer that disagrees with a known
// row indexer excludes that row for the same reason.
func stremioSelectRecoveredRelease(rows []*database.ImportQueueItem, normTarget, requestIndexer string, requestSize int64, ttlHours int) *database.ImportQueueItem {
	matches := make([]*database.ImportQueueItem, 0, len(rows))
	for _, item := range rows {
		if item == nil || !reusableCompletedImport(item, ttlHours) {
			continue
		}
		if !stremioQueueItemNameMatches(item, normTarget) || !stremioIndexerAgrees(item, requestIndexer) {
			continue
		}
		if requestSize > 0 && item.FileSize != nil && *item.FileSize != requestSize {
			continue
		}
		matches = append(matches, item)
	}

	switch len(matches) {
	case 0:
		return nil
	case 1:
		return matches[0]
	}

	var identity string
	verified := make([]*database.ImportQueueItem, 0, len(matches))
	for _, item := range matches {
		if item.ContentIdentity == nil || *item.ContentIdentity == "" {
			continue
		}
		if identity == "" {
			identity = *item.ContentIdentity
		} else if *item.ContentIdentity != identity {
			return nil
		}
		verified = append(verified, item)
	}
	if len(verified) == 0 {
		return nil
	}
	return stremioNewestCompleted(verified)
}

// findActiveStremioRelease returns the active Stremio import of contentKey that
// is the release the request names, matched by its release key or its stable
// token. A different active release of the same content is never returned: the
// caller queues the requested release instead, so /play can only redirect to
// the bytes the client asked for.
func (s *Server) findActiveStremioRelease(ctx context.Context, contentKey, releaseKey, releaseToken string) (*database.ImportQueueItem, error) {
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
		if item.Metadata == nil {
			continue
		}
		var metadata stremioQueueMetadata
		if json.Unmarshal([]byte(*item.Metadata), &metadata) != nil || metadata.ContentKey != contentKey {
			continue
		}
		if releaseKey != "" && metadata.ReleaseKey == releaseKey {
			return item, nil
		}
		if releaseToken != "" && metadata.ReleaseToken == releaseToken {
			return item, nil
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
	s = stripMediaExtensions(s)

	s = strings.ToLower(s)
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// matchingExtensions are the wrappers normalizeTitleForMatching removes,
// innermost last, so "release.nzb.gz" normalises to "release".
var matchingExtensions = []string{".gz", ".nzb", ".mkv", ".mp4", ".avi", ".iso", ".rar"}

func stripMediaExtensions(s string) string {
	for {
		orig := s
		for _, ext := range matchingExtensions {
			s = strings.TrimSuffix(s, ext)
		}
		if s == orig {
			return s
		}
	}
}

// stremioImporterHashSuffix matches the "-<hex>" suffix stremioPlayNzbFilename
// appends to a queued NZB (8 bytes of SHA-256 rendered as 16 hex digits), on a
// basename whose media extension was already removed. The 8..64 length window
// accepts the importer's hash and older or future hash lengths while refusing to
// strip a short hex run that belongs to the release name, such as "-2024" or
// "-1080". A non-hex or separately punctuated suffix is not the importer's.
var stremioImporterHashSuffix = regexp.MustCompile(`-[0-9a-fA-F]{8,64}$`)

// normalizeReleaseNameForMatching normalises a stored path for comparison with
// a recovered play link's release name.
//
// Legacy Stremio rows were named "<title>-<url-hash>.nzb", and their target and
// storage basenames repeated the hashed name. normalizeTitleForMatching keeps
// that hash as trailing alphanumerics, so the name can never equal the title the
// play link carries. Removing the importer's "-<hex>" suffix first makes those
// rows findable without loosening the strict name-and-identity rule.
func normalizeReleaseNameForMatching(s string) string {
	s = strings.ReplaceAll(s, "\\", "/")
	s = filepath.Base(s)
	s = stripMediaExtensions(s)
	s = stremioImporterHashSuffix.ReplaceAllString(s, "")
	return normalizeTitleForMatching(s)
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

// cachedFirst stably moves releases that are already imported (and so play
// instantly) ahead of the rest, keeping the search's score order within each
// group. The input slice is not modified: the stream list and the play
// fallback chain both order the same search results.
func cachedFirst(results []prowlarr.NZBResult, isCached func(safeTitle string) bool) []prowlarr.NZBResult {
	ordered := make([]prowlarr.NZBResult, len(results))
	copy(ordered, results)
	sort.SliceStable(ordered, func(i, j int) bool {
		return isCached(sanitizeFilename(ordered[i].Title)) && !isCached(sanitizeFilename(ordered[j].Title))
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
			Size:        r.Size,
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

	// Legacy Prowlarr preferred indexers (by id or name) still rank first.
	var preferredIndexers []string
	for _, id := range stremioCfg.Prowlarr.PreferredIndexers {
		preferredIndexers = append(preferredIndexers, strconv.Itoa(id))
	}
	preferredIndexers = append(preferredIndexers, stremioCfg.Prowlarr.PreferredIndexerNames...)

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
		PreferredIndexers: preferredIndexers,
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
		Type:         streamType,
		IMDBID:       imdbID,
		Title:        title,
		TitleAliases: seriesMatchCtx.aliases,
		Season:       season,
		Episode:      episode,
		TVDBID:       tvdbID,
		TimeoutMS:    timeoutMs,
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

	// Cache availability does not establish relevance to this request.
	// Validate every release before promoting cached results.
	ordered := make([]prowlarr.NZBResult, 0, len(filtered))
	var uncached []prowlarr.NZBResult
	var droppedNames []string
	for _, r := range filtered {
		if !releaseMatchesContent(streamType, r.Title, expectedTitle, expectedYear, season, episode, seriesMatchCtx) {
			droppedNames = append(droppedNames, fmt.Sprintf("%q (%s)", r.Title, r.Indexer))
			continue
		}
		if isCached(sanitizeFilename(r.Title)) {
			ordered = append(ordered, r)
		} else {
			uncached = append(uncached, r)
		}
	}
	ordered = append(ordered, uncached...)

	if len(droppedNames) > 0 {
		slog.InfoContext(ctx, "Dropped irrelevant Stremio releases",
			"imdb_id", imdbID,
			"dropped_count", len(droppedNames),
			"dropped_sample", droppedNames,
			"kept", len(ordered))
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
	Name          string
	Title         string
	URL           string
	Cached        bool
	BehaviorHints stremioStreamBehaviorHints `json:"behaviorHints"`
}

// Explicit hints let addon aggregators identify the service and parse the full
// release title without depending on AltMount's human-readable display badges.
type stremioStreamBehaviorHints struct {
	Service  string `json:"service"`
	Cached   bool   `json:"cached"`
	Filename string `json:"filename"`
	Indexer  string `json:"indexer,omitempty"`
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
	display streamDisplay,
	releaseStores ...func(stremioPlayCandidate) (string, error),
) []stremioStreamEntry {
	isCached := stremioCachedPredicate(cached, ttlHours, now)

	// Results arrive in the scorer's order (the same score the config page's
	// testing view shows); only already-imported releases move ahead.
	results = cachedFirst(results, isCached)

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
			Source: resultSource(r), Size: r.Size,
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
			// The release name is not secret (the token hides the indexer
			// URL); it lets /play recover the release once the token expires.
			playURL += "?release=" + url.QueryEscape(releaseToken) + "&title=" + url.QueryEscape(safeTitle)
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

		name, description := formatStremioStream(display, r, cachedHit)
		entries = append(entries, stremioStreamEntry{
			Name:   name,
			Title:  description,
			URL:    playURL,
			Cached: cachedHit,
			BehaviorHints: stremioStreamBehaviorHints{
				Service:  "altmount",
				Cached:   cachedHit,
				Filename: r.Title,
				Indexer:  r.Indexer,
			},
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
	imdbID := c.Query("id")
	streamType := c.Query("type")
	cand, ok := s.consumeStremioReleaseRef(releaseToken)
	// A link recovered from its release name alone carries no release
	// identity, only content and the name the release was listed under.
	recoveredByName := false
	// recoveredToken is the stable token a recovered link still carries. A
	// stable token is deterministic, so it names the release across a restart
	// even though the in-memory reference is gone. A token minted by the old
	// random scheme is not deterministic and falls through to name matching.
	recoveredToken := ""
	if !ok {
		// Stremio saves the stream a title was played with and replays that
		// link on resume, long after its release token expired. Recover from
		// the content id, and from the NZB URL when the link still carries one,
		// instead of failing.
		if imdbID == "" {
			return RespondBadRequest(c, "Invalid or expired release parameter", "")
		}
		if strings.HasPrefix(releaseToken, "r2-") {
			recoveredToken = releaseToken
		}
		cand = stremioPlayCandidate{}
		if title := c.Query("title"); title != "" {
			cand.SafeTitle = sanitizeFilename(title)
		}
		// A recovered link that still carries the indexer URL keeps release
		// identity. Filling it in here keys the release key, queue filename,
		// singleflight key and cached-row match off the URL exactly as an
		// explicit request does, so the recovery can never resolve to a
		// different release that merely shares the title.
		if downloadURL := strings.TrimSpace(c.Query("url")); downloadURL != "" {
			cand.DownloadURL = downloadURL
			cand.Indexer = c.Query("indexer")
			cand.Source = c.Query("source")
		} else {
			recoveredByName = cand.SafeTitle != ""
		}
		slog.InfoContext(ctx, "Recovering Stremio play link with an expired release token",
			"imdb_id", imdbID, "title", cand.SafeTitle, "has_release_url", cand.DownloadURL != "")
	}

	baseURL := resolveBaseURL(c, cfg.Stremio.BaseURL)
	selector := stremioEpisodeSelectorFromRequest(c)

	// A live release token names one exact release, and so does a recovered
	// link that still carries its NZB URL. A link recovered from the release
	// name alone names the release by name; the cached-row lookup below matches
	// that name exactly after normalisation instead of treating the title as a
	// release key.
	explicitRelease := cand.DownloadURL != ""
	releaseKey := stremioReleaseKey(cand)

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
				if cand.SafeTitle == "" || (normTarget != "" && normLibrary == normTarget) {
					slog.InfoContext(ctx, "Returning local library Stremio stream",
						"nzb_name", cand.SafeTitle, "library_url", libURL)
					return c.Redirect(libURL, fiber.StatusFound)
				}
			}
		}
	}
	// The request's indexer is the live release's own when the token is live or
	// the link still carries its NZB URL, and the query parameter otherwise.
	requestIndexer := strings.TrimSpace(cand.Indexer)
	if requestIndexer == "" {
		requestIndexer = strings.TrimSpace(c.Query("indexer"))
	}
	// A stable token names the release across listings even when the URL that
	// produced its release key has changed.
	stableReleaseToken := stremioReleaseToken(cand)
	tryServe := func(prev *database.ImportQueueItem, matchedBy string) (bool, error) {
		if prev == nil {
			return false, nil
		}
		streams, err := s.buildStremioStreams(ctx, prev, baseURL, key, cand.SafeTitle, selector)
		if err != nil || len(streams) == 0 {
			return false, nil
		}
		slog.InfoContext(ctx, "Returning cached Stremio stream",
			"nzb_name", cand.SafeTitle, "matched_path", prev.NzbPath, "indexer", prev.Indexer,
			"matched_by", matchedBy, "recovered_by_name", matchedBy == "release_name")
		return true, c.Redirect(streams[0].URL, fiber.StatusFound)
	}

	// Reuse cached bytes only for the release a request names. A live token or
	// a recovered link carrying its NZB URL names an exact release through its
	// release key. A recovered link's own stable token names the release across
	// a restart even when its URL key no longer matches. A link recovered from
	// the release name alone names the release by name, so a completed row is
	// served only when that name and its verified identity resolve to one
	// release; an ambiguous name still searches instead of guessing between
	// same-titled rows.
	if recoveredToken != "" || (normTarget != "" && (explicitRelease || recoveredByName)) {
		if cachedItems, err := s.queueRepo.GetCachedStremioQueueItems(ctx, cfg.Stremio.EffectiveIncludeLibraryStreams(), cfg.Stremio.NzbTTLHours); err == nil {
			if recoveredToken != "" {
				// The saved link still names its release. Check the token
				// before the name rule: a legacy row's hashed basenames defeat
				// the name, while a token minted by the old random scheme simply
				// misses here and falls through.
				prev := stremioSelectTokenRelease(cachedItems, recoveredToken, ttlHours)
				if served, err := tryServe(prev, "release_token"); served {
					return err
				}
			}
			if recoveredByName {
				prev := stremioSelectRecoveredRelease(cachedItems, normTarget,
					requestIndexer, cand.Size, ttlHours)
				if served, err := tryServe(prev, "release_name"); served {
					return err
				}
			} else if explicitRelease {
				keyMatched := false
				for _, prev := range cachedItems {
					if prev == nil || !reusableCompletedImport(prev, ttlHours) {
						continue
					}
					// Rows written before release keys existed cannot prove
					// which release they hold, so they are skipped as well.
					storedKey, keyed := stremioQueueItemReleaseKey(prev)
					if !keyed || storedKey != releaseKey {
						continue
					}
					keyMatched = true
					if served, err := tryServe(prev, "release_key"); served {
						return err
					}
				}
				if !keyMatched {
					// The URL changed while the release did not. Try the stable
					// token first, then the strict name-and-identity rule, so an
					// already-completed release is not fetched again.
					prev := stremioSelectTokenRelease(cachedItems, stableReleaseToken, ttlHours)
					if served, err := tryServe(prev, "release_token"); served {
						return err
					}
					prev = stremioSelectRecoveredRelease(cachedItems, normTarget,
						requestIndexer, cand.Size, ttlHours)
					if served, err := tryServe(prev, "release_name"); served {
						return err
					}
				}
			}
		}
		if recoveredToken != "" {
			// An import already running for this exact release is awaited
			// instead of being queued a second time.
			contentKey := stremioContentKey(streamType, imdbID, selector)
			active, err := s.findActiveStremioRelease(ctx, contentKey, "", recoveredToken)
			if err != nil {
				slog.WarnContext(ctx, "Failed to look up the recovered release's active import",
					"error", err, "release_token", recoveredToken)
			} else if active != nil {
				out := s.waitForStream(ctx, active.ID, baseURL, key, cand.SafeTitle, selector, stremioPlayBudget)
				switch out.Kind {
				case streamOutcomeReady:
					return c.Redirect(out.StreamURL, fiber.StatusFound)
				case streamOutcomeFailed, streamOutcomeNoStreams:
					// The active import ended without playable bytes; search
					// again instead of attaching to a dead row.
				default:
					return respondStreamOutcome(c, out)
				}
			}
		}
	}

	if cand.DownloadURL == "" {
		// Nothing completed matches the recovered release name: find the
		// release again, by its stable token when it has one, else by name.
		found, _ := s.findStremioCandidate(ctx, cfg, streamType, imdbID, selector, cand.SafeTitle, releaseToken)
		if found == nil {
			return RespondServiceUnavailable(c, "No playable release found", "the play link expired and a new search found nothing")
		}
		cand = *found
	}

	return s.playStremioWithFallback(c, cfg, cand, playRequest{
		baseURL:    baseURL,
		key:        key,
		streamType: streamType,
		imdbID:     imdbID,
		selector:   selector,
	})
}

// findStremioCandidate searches again for a content id and returns the
// release named safeTitle when the search still offers it (matched true),
// otherwise the top-ranked release, or nil when the search finds nothing.
// releaseToken, when it belongs to the stable token scheme, wins over the name:
// it identifies the release even if the indexer renamed it between listings.
func (s *Server) findStremioCandidate(
	ctx context.Context,
	cfg *config.Config,
	streamType, imdbID string,
	selector *stremioEpisodeSelector,
	safeTitle, releaseToken string,
) (cand *stremioPlayCandidate, matched bool) {
	prowlarrType := "search"
	switch streamType {
	case "movie":
		prowlarrType = "movie"
	case "series":
		prowlarrType = "tvsearch"
	}
	season, episode := 0, 0
	if selector != nil {
		season, episode = selector.Season, selector.Episode
	}
	results, _, err := s.searchStremioReleases(ctx, cfg, streamType, prowlarrType, imdbID, season, episode)
	if err != nil {
		slog.WarnContext(ctx, "Stremio search for an expired play link failed", "error", err, "imdb_id", imdbID)
	}
	return pickStremioCandidate(stremioCandidates(results, imdbID), safeTitle, releaseToken)
}

// pickStremioCandidate returns the release the play link names, then the
// release named safeTitle, then the first release, else nil.
func pickStremioCandidate(cands []stremioPlayCandidate, safeTitle, releaseToken string) (*stremioPlayCandidate, bool) {
	if releaseToken != "" && strings.HasPrefix(releaseToken, "r2-") {
		for i := range cands {
			if stremioReleaseToken(cands[i]) == releaseToken {
				return &cands[i], true
			}
		}
	}
	if safeTitle != "" {
		for i := range cands {
			if cands[i].SafeTitle == safeTitle {
				return &cands[i], true
			}
		}
	}
	if len(cands) == 0 {
		return nil, false
	}
	return &cands[0], false
}

// playRequest carries the per-request context the fallback loop needs, keeping the
// loop signature readable.
type playRequest struct {
	baseURL    string
	key        string
	streamType string
	imdbID     string
	selector   *stremioEpisodeSelector
}

const (
	// stremioPlayBudget is the total wall clock a /play request may spend across all
	// attempts. Unchanged from the original single-attempt timeout, so client
	// behaviour is unaffected.
	stremioPlayBudget = 300 * time.Second
	// stremioMinAttemptBudget is the minimum time left worth starting another attempt.
	stremioMinAttemptBudget = 20 * time.Second
)

// stremioPlayAttempts records the release tokens one /play request has already
// tried to fetch. Its scope is one playStremioWithFallback call -- one fallback
// burst -- so a fallback list that repeats a release fetches it once. A later,
// separate /play request starts empty and may try that release once more; the
// active and completed row lookups decide whether that becomes another fetch.
type stremioPlayAttempts map[string]struct{}

// begin reports whether the token may be fetched and records the attempt. An
// empty token cannot identify a release, so it is never deduplicated.
func (a stremioPlayAttempts) begin(token string) bool {
	if token == "" {
		return true
	}
	if _, tried := a[token]; tried {
		return false
	}
	a[token] = struct{}{}
	return true
}

// stremioServeCompletedToken redirects to a completed import that already holds
// the candidate's stable release token. It runs before each fallback attempt, so
// a candidate whose import finished while an earlier candidate was being waited
// on is served from storage instead of being fetched a second time.
func (s *Server) stremioServeCompletedToken(
	c *fiber.Ctx,
	cfg *config.Config,
	cand stremioPlayCandidate,
	req playRequest,
) (bool, error) {
	ctx := c.Context()
	if s.queueRepo == nil {
		return false, nil
	}
	token := stremioReleaseToken(cand)
	if token == "" {
		return false, nil
	}
	rows, err := s.queueRepo.GetCachedStremioQueueItems(ctx,
		cfg.Stremio.EffectiveIncludeLibraryStreams(), cfg.Stremio.NzbTTLHours)
	if err != nil {
		return false, nil
	}
	prev := stremioSelectTokenRelease(rows, token, cfg.Stremio.NzbTTLHours)
	if prev == nil {
		return false, nil
	}
	streams, err := s.buildStremioStreams(ctx, prev, req.baseURL, req.key, cand.SafeTitle, req.selector)
	if err != nil || len(streams) == 0 {
		return false, nil
	}
	slog.InfoContext(ctx, "Returning completed Stremio release instead of fetching the release again",
		"title", cand.SafeTitle, "queue_id", prev.ID, "matched_by", "release_token")
	s.stremioFailures.Forget(cand.SafeTitle)
	return true, c.Redirect(streams[0].URL, fiber.StatusFound)
}

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
		slog.InfoContext(ctx, "Falling back to next Stremio release",
			"imdb_id", req.imdbID, "title", cand.SafeTitle)
		return true
	}

	deadline := time.Now().Add(stremioPlayBudget)
	maxAttempts := 1 + cfg.Stremio.EffectiveMaxFallbackReleases()
	attempts := 0
	// attempted lives for this one /play request: a fallback burst fetches each
	// release token at most once, while a later request starts a new burst.
	attempted := make(stremioPlayAttempts)

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

		// A candidate whose import already completed under its stable token is
		// served, never fetched again. A candidate this burst already tried is
		// skipped: retrying the same release inside one play cannot succeed when
		// the request that just tried it did not, and a fallback list can repeat
		// the same release.
		if served, err := s.stremioServeCompletedToken(c, cfg, cand, req); served || err != nil {
			return err
		}
		if !attempted.begin(stremioReleaseToken(cand)) {
			slog.InfoContext(ctx, "Skipping a Stremio release already attempted in this play",
				"imdb_id", req.imdbID, "title", cand.SafeTitle)
			if !advance() {
				break
			}
			continue
		}
		attempts++

		itemID, err := s.enqueueStremioRelease(ctx, cfg, cand, req.streamType, req.imdbID, req.selector)
		if err != nil {
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
) (int64, error) {
	safeFilename := stremioPlayNzbFilename(cand)
	ttlHours := cfg.Stremio.NzbTTLHours
	contentKey := stremioContentKey(streamType, imdbID, selector)
	releaseKey := stremioReleaseKey(cand)
	releaseToken := stremioReleaseToken(cand)

	var indexerPtr *string
	if cand.Indexer != "" {
		indexer := cand.Indexer
		indexerPtr = &indexer
	}

	queue := func() (interface{}, error) {
		if contentKey != "" {
			active, findErr := s.findActiveStremioRelease(ctx, contentKey, releaseKey, releaseToken)
			if findErr != nil {
				return nil, fmt.Errorf("find active Stremio release: %w", findErr)
			}
			if active != nil {
				return active.ID, nil
			}
			// Another release of this content is importing. Never attach this
			// request to it: queue the requested release instead, so /play can
			// only redirect to the bytes the client asked for.
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
						if reusableCompletedImport(it, ttlHours) {
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
				ContentKey:   contentKey,
				ReleaseKey:   releaseKey,
				ReleaseToken: releaseToken,
				IMDbID:       imdbID,
				Type:         streamType,
				TMDBID:       tmdbID,
				TVDBID:       tvdbID,
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

	// The group key includes the release key: concurrent plays of the same
	// release share one download, while two different releases for the same
	// content never share a queue item.
	groupKey := contentKey
	if groupKey != "" {
		groupKey += "|" + releaseKey
	}
	var v interface{}
	var err error
	if groupKey != "" {
		v, err, _ = s.stremioContentGroup.Do(groupKey, queue)
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
func isSameOrigin(urlStr1, urlStr2 string) bool {
	u1, err1 := url.Parse(strings.TrimSpace(urlStr1))
	u2, err2 := url.Parse(strings.TrimSpace(urlStr2))
	if err1 != nil || err2 != nil || u1.Host == "" || u2.Host == "" {
		return false
	}
	return strings.EqualFold(u1.Scheme, u2.Scheme) && strings.EqualFold(u1.Host, u2.Host)
}

// @Summary		List available Prowlarr indexers
// @Description	Queries the configured Prowlarr instance and returns all enabled indexers
// @Tags			Stremio
// @Accept			json
// @Produce		json
// @Param			request	body		prowlarrIndexersRequest	false	"Optional host/api_key override"
// @Success		200		{object}	APIResponse
// @Failure		400		{object}	APIResponse
// @Failure		500		{object}	APIResponse
// @Failure		503		{object}	APIResponse
// @Security		BearerAuth
// @Router			/prowlarr/indexers [post]
func (s *Server) handleListProwlarrIndexers(c *fiber.Ctx) error {
	if s.configManager == nil {
		return RespondServiceUnavailable(c, "Configuration not available", "")
	}

	var req prowlarrIndexersRequest
	// Body is optional; ignore parse errors and fall back to saved config.
	_ = c.BodyParser(&req)

	cfg := s.configManager.GetConfig()
	storedHost := cfg.Stremio.Indexers.Prowlarr.Host
	if storedHost == "" {
		storedHost = cfg.Stremio.Prowlarr.Host
	}
	storedKey := cfg.Stremio.Indexers.Prowlarr.APIKey
	if storedKey == "" {
		storedKey = cfg.Stremio.Prowlarr.APIKey
	}

	host := strings.TrimSpace(req.Host)
	apiKey := strings.TrimSpace(req.APIKey)

	// Fall back to the stored Prowlarr configuration when fields are empty or
	// when host matches the stored host (for masked credentials).
	if host == "" {
		host = storedHost
	}
	if apiKey == "" && (req.Host == "" || isSameOrigin(req.Host, storedHost)) {
		apiKey = storedKey
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
	// configured indexer, fall back to its stored credentials only if the URL
	// is empty or matches the stored indexer origin.
	if strings.TrimSpace(req.APIKey) == "" && strings.TrimSpace(req.ID) != "" {
		for _, n := range s.configManager.GetConfig().Stremio.Indexers.Newsnab {
			if n.ID == strings.TrimSpace(req.ID) {
				if reqURL == "" {
					reqURL = n.URL
					req.APIKey = n.APIKey
				} else if isSameOrigin(reqURL, n.URL) {
					req.APIKey = n.APIKey
				}
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
