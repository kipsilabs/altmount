package api

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/kipsilabs/altmount/internal/auth"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/httpclient"
	"github.com/kipsilabs/altmount/internal/importer/parser/fileinfo"
	"github.com/kipsilabs/altmount/internal/importer/utils/nzbtrim"
	"github.com/javi11/nzbparser"
)

// mediaExtensions lists common video/media file extensions for Stremio stream filtering.
var mediaExtensions = map[string]bool{
	".mkv":  true,
	".mp4":  true,
	".avi":  true,
	".ts":   true,
	".m2ts": true,
	".mov":  true,
	".wmv":  true,
	".flv":  true,
	".m4v":  true,
	".mpeg": true,
	".mpg":  true,
	".vob":  true,
	".webm": true,
	".ogv":  true,
	".iso":  true,
}

var (
	stremioSeasonEpisodePattern = regexp.MustCompile(`(?i)s0*(\d{1,2})((?:[ ._-]*e0*\d{1,3})+)`)
	stremioEpisodeOnlyPattern   = regexp.MustCompile(`(?i)e0*(\d{1,3})`)
	stremioXEpisodePattern      = regexp.MustCompile(`(?i)(?:^|[^0-9])0*(\d{1,2})x0*(\d{1,3})(?:[^0-9]|$)`)
	// Looser, boundary-anchored season/episode markers used only as a fallback
	// when a filename (including its parent directory) carries no explicit
	// combined SxxExx / NxNN token — e.g. "Season 01/Show E05.mkv".
	stremioSeasonContextPattern  = regexp.MustCompile(`(?i)\b(?:season|series|s)[ ._-]*0*(\d{1,2})`)
	stremioEpisodeContextPattern = regexp.MustCompile(`(?i)\b(?:episode|ep|e)[ ._-]*0*(\d{1,3})`)
)

// errStremioEpisodeAmbiguous is returned by buildStremioStreams when a
// multi-episode release is resolved without any episode context. Callers turn
// it into a clear client error instead of silently serving the first episode.
var errStremioEpisodeAmbiguous = errors.New("stremio: episode not specified for a multi-episode release")

type stremioEpisodeSelector struct {
	Season  int
	Episode int
}

// StremioStream represents a single stream entry in the Stremio addon format.
type StremioStream struct {
	URL   string `json:"url"`
	Title string `json:"title"`
	Name  string `json:"name"`
}

// StremioStreamsResponse is the response returned by the Stremio stream endpoint.
// The _queue_item_id, _queue_status, and _cached fields are AltMount extensions that Stremio ignores.
type StremioStreamsResponse struct {
	Streams     []StremioStream `json:"streams"`
	QueueItemID int64           `json:"_queue_item_id"`
	QueueStatus string          `json:"_queue_status"`
	// Cached is true when streams were served from an already-completed queue item
	// without re-processing. Callers such as AIOStreams can use this to show an
	// "instant" indicator to the user.
	Cached bool `json:"_cached"`
}

// handleNzbStreams handles POST /api/nzb/streams.
// Public endpoint — authenticated via the download_key form field (SHA256 of the user's API key).
// Accepts an NZB file, adds it to the import queue with high priority, and waits synchronously
// for processing to complete before returning Stremio-compatible stream URLs for all media files
// found in the NZB output.
//
//	@Summary		Get Stremio streams for NZB file
//	@Description	Accepts an NZB file (upload or URL), queues it with high priority, and returns Stremio-compatible stream URLs as soon as the file is accessible via VFS. Auth: download_key form field (SHA256 of API key) or X-Api-Key header (raw API key). Returns _cached=true when served from an already-completed item.
//	@Tags			Stremio
//	@Accept			multipart/form-data
//	@Produce		json
//	@Param			file			formData	file	false	"NZB file to process (mutually exclusive with nzb_url)"
//	@Param			nzb_url			formData	string	false	"URL to download the NZB from (mutually exclusive with file)"
//	@Param			download_key	formData	string	false	"SHA256 hash of the user's API key (alternative: X-Api-Key header)"
//	@Param			X-Api-Key		header		string	false	"Raw API key (alternative to download_key form field)"
//	@Param			category		formData	string	false	"Queue category (default: stremio)"
//	@Param			timeout			formData	int		false	"Processing timeout in seconds (default: 300)"
//	@Param			season			formData	int		false	"Season number for selecting one episode from a season pack"
//	@Param			episode			formData	int		false	"Episode number for selecting one episode from a season pack"
//	@Param			id				formData	string	false	"Stremio content id (e.g. tt1234567:1:5) as an alternative to season/episode"
//	@Success		200	{object}	StremioStreamsResponse
//	@Failure		400	{object}	APIResponse
//	@Failure		401	{object}	APIResponse
//	@Failure		503	{object}	APIResponse
//	@Router			/nzb/streams [post]
func (s *Server) handleNzbStreams(c *fiber.Ctx) error {
	ctx := c.Context()

	// --- Gate on Stremio enabled flag ---
	if s.configManager == nil {
		return RespondServiceUnavailable(c, "Configuration not available", "")
	}
	cfg := s.configManager.GetConfig()
	if !isStremioEnabled(cfg) {
		return RespondNotFound(c, "Stremio endpoint", "Stremio integration is disabled in configuration")
	}

	// --- Authenticate via download_key form field, X-Api-Key header, or query param ---
	// download_key: SHA256 hash of the API key (used by the Stremio addon play path)
	// X-Api-Key:    raw API key (used by AIOStreams and other integrations)
	downloadKey := c.FormValue("download_key")
	if downloadKey == "" {
		downloadKey = c.Query("download_key")
	}
	if downloadKey == "" {
		// Accept raw API key via X-Api-Key header (AIOStreams sends altmountApiKey here).
		// Hash it so validateDownloadKey can compare against the stored hash.
		if rawKey := c.Get("X-Api-Key"); rawKey != "" {
			if s.validateAPIKey(c, rawKey) {
				downloadKey = auth.HashAPIKey(rawKey)
			} else {
				slog.WarnContext(ctx, "Stremio stream endpoint: authentication failed - invalid X-Api-Key")
				return RespondUnauthorized(c, "Invalid X-Api-Key", "")
			}
		}
	}
	if downloadKey == "" {
		return RespondUnauthorized(c, "Authentication required", "Provide download_key (SHA256 of API key) or X-Api-Key header")
	}
	if !s.validateDownloadKey(ctx, downloadKey) {
		slog.WarnContext(ctx, "Stremio stream endpoint: authentication failed - invalid download_key")
		return RespondUnauthorized(c, "Invalid download_key", "")
	}

	// --- Accept NZB as file upload or by URL ---
	// nzb_url allows callers (e.g. AIOStreams) to pass the NZB download URL directly
	// instead of uploading the file bytes, avoiding an extra round-trip.
	nzbURL := c.FormValue("nzb_url")

	var nzbFilename string
	var nzbData []byte

	if nzbURL != "" {
		// Download NZB from the provided URL
		const maxNzbFetchSize = 100 * 1024 * 1024 // 100 MB
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, nzbURL, nil)
		if err != nil {
			return RespondBadRequest(c, "Invalid nzb_url", err.Error())
		}
		// Some indexers (e.g. NZBHydra2) reject requests without a browser-like User-Agent.
		req.Header.Set("User-Agent", cfg.GetUserAgent())
		resp, err := httpclient.NewForExternal(cfg.Network, httpclient.LongTimeout).Do(req) //nolint:gosec // URL is provided by an authenticated caller
		if err != nil {
			return RespondBadRequest(c, "Failed to fetch NZB from URL", err.Error())
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return RespondBadRequest(c, "Failed to fetch NZB from URL", fmt.Sprintf("HTTP %d", resp.StatusCode))
		}
		nzbData, err = io.ReadAll(io.LimitReader(resp.Body, maxNzbFetchSize))
		if err != nil {
			return RespondBadRequest(c, "Failed to read NZB from URL", err.Error())
		}
		// Derive filename from URL path
		nzbFilename = filepath.Base(nzbURL)
		if idx := strings.Index(nzbFilename, "?"); idx >= 0 {
			nzbFilename = nzbFilename[:idx]
		}
		if !nzbtrim.HasNzbExtension(nzbFilename) {
			nzbFilename = nzbFilename + ".nzb"
		}
	} else {
		// Require file upload
		file, err := c.FormFile("file")
		if err != nil {
			return RespondBadRequest(c, "No file provided", "Upload a .nzb file or provide nzb_url")
		}
		if !nzbtrim.HasNzbExtension(file.Filename) {
			return RespondValidationError(c, "Invalid file type", "Only .nzb or .nzb.gz files are allowed")
		}
		const maxUploadSize = 100 * 1024 * 1024 // 100 MB
		if file.Size > maxUploadSize {
			return RespondValidationError(c, "File too large", "File size must be less than 100MB")
		}
		nzbFilename = file.Filename
		// Read file bytes for saving below
		f, err := file.Open()
		if err != nil {
			return RespondInternalError(c, "Failed to open uploaded file", err.Error())
		}
		defer f.Close()
		nzbData, err = io.ReadAll(f)
		if err != nil {
			return RespondInternalError(c, "Failed to read uploaded file", err.Error())
		}
	}

	// --- Recover a real release name when the transport name is generic ---
	// AIOStreams uploads/links the NZB as "download.nzb" (multipart part name or a
	// download-endpoint URL like ".../download?..."), which loses the real title. That
	// generic name would then (1) rename the imported media file to "download.mp4" via
	// RenameToNzbName and (2) collide across releases in the per-filename dedup/cache
	// below, serving one movie's stream for a different request. When the incoming name
	// looks obfuscated/generic, derive a meaningful, unique name from the NZB contents.
	if fileinfo.IsProbablyObfuscated(nzbtrim.TrimNzbExtension(filepath.Base(nzbFilename))) {
		if derived := deriveNzbNameFromContent(nzbData); derived != "" {
			nzbFilename = derived + ".nzb"
		}
	}

	// --- Resolve base URL ---
	baseURL := resolveBaseURL(c, cfg.Stremio.BaseURL)
	selector := stremioEpisodeSelectorFromRequest(c)

	category := c.FormValue("category")

	timeoutSecs := 300
	if ts := c.FormValue("timeout"); ts != "" {
		if n, err := strconv.Atoi(ts); err == nil && n > 0 {
			timeoutSecs = n
		}
	}

	// --- Derive stable names before touching the filesystem ---
	uploadDir := filepath.Join(os.TempDir(), "altmount-uploads")
	safeFilename := filepath.Base(nzbFilename)
	nzbName := nzbtrim.TrimNzbExtension(safeFilename)
	tempPath := filepath.Join(uploadDir, safeFilename)

	// --- Find or add NZB to queue, deduplicated per filename ---
	// stremioPlayGroup serialises all callers with the same safeFilename: the first
	// one runs the full find-or-add path; concurrent duplicates (e.g. two users
	// requesting the same release simultaneously) wait and share the returned queue
	// item ID, preventing the TOCTOU race that previously created duplicate entries.
	ttlHours := cfg.Stremio.NzbTTLHours

	rawID, sfErr, _ := s.stremioPlayGroup.Do(safeFilename, func() (interface{}, error) {
		// Detach from the caller's request context so one client disconnecting
		// does not abort work that other concurrent callers are also waiting on.
		workCtx := context.WithoutCancel(ctx)

		completedStatus := database.QueueStatusCompleted

		// Check completed cache inside the critical section so two concurrent
		// callers can't both miss it and both enqueue the same NZB.
		if existing, e := s.queueRepo.ListQueueItems(workCtx, &completedStatus, safeFilename, "", 1, 0, "updated_at", "desc"); e == nil && len(existing) > 0 {
			prev := existing[0]
			cacheValid := prev.StoragePath != nil && *prev.StoragePath != ""
			if cacheValid && ttlHours > 0 && prev.CompletedAt != nil {
				cacheValid = time.Since(*prev.CompletedAt) < time.Duration(ttlHours)*time.Hour
			}
			if cacheValid {
				slog.InfoContext(workCtx, "Returning cached Stremio streams for already-processed NZB",
					"nzb_name", nzbName, "queue_id", prev.ID)
				return prev.ID, nil
			}
		}

		// Join an existing active queue item instead of re-adding.
		if activeItems, e := s.queueRepo.ListQueueItems(workCtx, nil, safeFilename, "", 1, 0, "updated_at", "desc"); e == nil && len(activeItems) > 0 {
			it := activeItems[0]
			switch it.Status {
			case database.QueueStatusPending, database.QueueStatusProcessing, database.QueueStatusPaused:
				return it.ID, nil
			}
		}

		// --- Save NZB to temp directory and add to queue ---
		if s.importerService == nil {
			return nil, fmt.Errorf("importer service not available")
		}

		if err := os.MkdirAll(uploadDir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create upload directory: %w", err)
		}
		if err := os.WriteFile(tempPath, nzbData, 0644); err != nil {
			return nil, fmt.Errorf("failed to save NZB file: %w", err)
		}

		if category == "" {
			category = "stremio"
		}
		var basePath *string
		if completeDir := cfg.SABnzbd.CompleteDir; completeDir != "" {
			basePath = &completeDir
		}

		priority := database.QueuePriorityHigh
		item, err := s.importerService.AddToQueue(workCtx, tempPath, basePath, &category, &priority, nil, nil, nil)
		if err != nil {
			os.Remove(tempPath)
			return nil, fmt.Errorf("failed to add NZB to queue: %w", err)
		}

		slog.InfoContext(workCtx, "NZB added to queue for Stremio stream processing",
			"queue_id", item.ID,
			"nzb_path", tempPath,
			"timeout_secs", timeoutSecs)

		return item.ID, nil
	})
	if sfErr != nil {
		return RespondInternalError(c, "Failed to process NZB", sfErr.Error())
	}

	return s.waitAndRespond(c, rawID.(int64), baseURL, downloadKey, nzbName, selector, timeoutSecs)
}

// waitAndRespond subscribes to the progress broadcaster and waits for the queue item to
// reach a terminal state (completed or failed), then returns the appropriate Stremio response.
// This avoids polling by using an event-driven approach via the ProgressBroadcaster.
func (s *Server) waitAndRespond(c *fiber.Ctx, itemID int64, baseURL, downloadKey, nzbName string, selector *stremioEpisodeSelector, timeoutSecs int) error {
	ctx := c.Context()

	// Subscribe before the status check to eliminate the race between AddToQueue and the event.
	subID, ch := s.progressBroadcaster.Subscribe()
	defer s.progressBroadcaster.Unsubscribe(subID)

	// Single upfront check — the item may have already reached a terminal state.
	current, err := s.queueRepo.GetQueueItem(ctx, itemID)
	if err != nil {
		return RespondInternalError(c, "Failed to check queue status", err.Error())
	}
	if current == nil {
		return RespondInternalError(c, "Queue item not found", fmt.Sprintf("item ID %d", itemID))
	}

	switch current.Status {
	case database.QueueStatusCompleted:
		streams, err := s.buildStremioStreams(ctx, current, baseURL, downloadKey, nzbName, selector)
		if err != nil {
			if errors.Is(err, errStremioEpisodeAmbiguous) {
				return respondEpisodeAmbiguous(c)
			}
			return RespondInternalError(c, "Failed to list output media files", err.Error())
		}
		return c.JSON(StremioStreamsResponse{
			Streams:     streams,
			QueueItemID: current.ID,
			QueueStatus: string(current.Status),
			Cached:      true,
		})
	case database.QueueStatusFailed:
		errMsg := ""
		if current.ErrorMessage != nil {
			errMsg = *current.ErrorMessage
		}
		return RespondInternalError(c, "NZB processing failed", errMsg)
	default:
		// If the item is already processing and has a storage path, the streamable
		// event fired before we subscribed — return the streams immediately.
		if current.StoragePath != nil && *current.StoragePath != "" {
			if streams, err := s.buildStremioStreams(ctx, current, baseURL, downloadKey, nzbName, selector); err == nil && len(streams) > 0 {
				return c.JSON(StremioStreamsResponse{
					Streams:     streams,
					QueueItemID: current.ID,
					QueueStatus: "streamable",
				})
			}
		}
	}

	// Wait for a streamable or completion event from the broadcaster.
	timer := time.NewTimer(time.Duration(timeoutSecs) * time.Second)
	defer timer.Stop()

	for {
		select {
		case update, ok := <-ch:
			if !ok {
				return RespondInternalError(c, "Progress broadcaster closed unexpectedly", "")
			}
			if update.QueueID != int(itemID) {
				continue
			}
			switch update.Status {
			case "streamable":
				// Return streams as soon as the file is accessible in the VFS — before
				// post-processing (symlinks, STRM, health scheduling) completes.
				if update.StoragePath != "" {
					fakeItem := &database.ImportQueueItem{ID: itemID, StoragePath: &update.StoragePath}
					if streams, err := s.buildStremioStreams(ctx, fakeItem, baseURL, downloadKey, nzbName, selector); err == nil && len(streams) > 0 {
						return c.JSON(StremioStreamsResponse{
							Streams:     streams,
							QueueItemID: itemID,
							QueueStatus: "streamable",
						})
					}
				}
				// StoragePath empty or no media files yet — fall through to wait for completed.
			case "completed":
				item, err := s.queueRepo.GetQueueItem(ctx, itemID)
				if err != nil {
					return RespondInternalError(c, "Failed to fetch completed item", err.Error())
				}
				streams, err := s.buildStremioStreams(ctx, item, baseURL, downloadKey, nzbName, selector)
				if err != nil {
					if errors.Is(err, errStremioEpisodeAmbiguous) {
						return respondEpisodeAmbiguous(c)
					}
					return RespondInternalError(c, "Failed to list output media files", err.Error())
				}
				return c.JSON(StremioStreamsResponse{
					Streams:     streams,
					QueueItemID: item.ID,
					QueueStatus: string(item.Status),
				})
			case "failed":
				item, _ := s.queueRepo.GetQueueItem(ctx, itemID)
				errMsg := "Processing failed"
				if item != nil && item.ErrorMessage != nil {
					errMsg = *item.ErrorMessage
				}
				return RespondInternalError(c, errMsg, "")
			}
		case <-timer.C:
			return RespondError(c, fiber.StatusRequestTimeout, "TIMEOUT",
				"NZB processing timed out",
				fmt.Sprintf("Processing did not complete within %d seconds (queue_item_id: %d)", timeoutSecs, itemID))
		}
	}
}

// buildStremioStreams resolves the virtual paths from a completed queue item and
// returns Stremio stream objects for the media files in the NZB output. When a
// selector is provided, only files matching the requested episode are returned.
// When no selector is provided and the output is a multi-episode pack, it returns
// errStremioEpisodeAmbiguous so callers can respond with a clear error instead of
// silently serving the first episode.
func (s *Server) buildStremioStreams(ctx context.Context, item *database.ImportQueueItem, baseURL, downloadKey, nzbName string, selector *stremioEpisodeSelector) ([]StremioStream, error) {
	if item.StoragePath == nil || *item.StoragePath == "" {
		return nil, fmt.Errorf("completed queue item %d has no storage path", item.ID)
	}

	storagePath := filepath.ToSlash(*item.StoragePath)

	// If the storage path already points to a media file, return it directly.
	if isMediaExtension(filepath.Ext(storagePath)) {
		baseName := filepath.Base(storagePath)
		match := selector == nil || selector.matches(baseName)
		if !match && selector != nil && !selector.hasConflictingMarker(baseName) {
			if selector.matches(nzbName) || (item.NzbPath != "" && selector.matches(item.NzbPath)) || (item.RelativePath != nil && selector.matches(*item.RelativePath)) {
				match = true
			}
		}
		if !match {
			return []StremioStream{}, nil
		}
		return []StremioStream{stremioStreamFromPath(storagePath, baseURL, downloadKey)}, nil
	}

	// Otherwise treat it as a virtual directory and list its media files.
	if s.metadataService == nil {
		return nil, fmt.Errorf("metadata service not available")
	}

	files, err := s.listStremioMediaFiles(storagePath)
	if err != nil {
		return nil, fmt.Errorf("failed to list directory %q: %w", storagePath, err)
	}

	// Without episode context, refuse to guess which episode of a multi-episode
	// pack to serve — returning the first file would silently play the wrong one.
	if selector == nil && countDistinctEpisodes(files) > 1 {
		slog.WarnContext(ctx, "Stremio release is a multi-episode pack but no episode was specified",
			"nzb_name", nzbName, "queue_id", item.ID, "file_count", len(files))
		return nil, errStremioEpisodeAmbiguous
	}

	streams := make([]StremioStream, 0, len(files))
	for _, name := range files {
		match := selector == nil || selector.matches(name)
		if !match && selector != nil && len(files) == 1 && !selector.hasConflictingMarker(name) {
			if selector.matches(nzbName) || (item.NzbPath != "" && selector.matches(item.NzbPath)) || (item.RelativePath != nil && selector.matches(*item.RelativePath)) || countDistinctEpisodes(files) <= 1 {
				match = true
			}
		}
		if !match {
			continue
		}
		virtualPath := filepath.ToSlash(filepath.Join(storagePath, filepath.FromSlash(name)))
		streams = append(streams, stremioStreamFromPath(virtualPath, baseURL, downloadKey))
	}

	if selector != nil && len(streams) == 0 {
		slog.WarnContext(ctx, "No media file in Stremio release matched the requested episode",
			"nzb_name", nzbName, "queue_id", item.ID,
			"season", selector.Season, "episode", selector.Episode,
			"available_files", files)
	}

	return streams, nil
}

// respondEpisodeAmbiguous returns a clear client error when a multi-episode
// release is requested without episode context, so the caller does not silently
// play the first episode.
func respondEpisodeAmbiguous(c *fiber.Ctx) error {
	return RespondBadRequest(c, "Episode not specified",
		"This release is a multi-episode pack; provide season and episode (or a Stremio id like tt1234567:1:5).")
}

func (s *Server) listStremioMediaFiles(storagePath string) ([]string, error) {
	dirs, files, err := s.metadataService.ListDirectoryAll(storagePath)
	if err != nil {
		return nil, err
	}

	mediaFiles := make([]string, 0, len(files))
	for _, name := range files {
		if isMediaExtension(filepath.Ext(name)) {
			mediaFiles = append(mediaFiles, name)
		}
	}

	for _, dir := range dirs {
		if dir == nil {
			continue
		}
		children, err := s.listStremioMediaFiles(filepath.ToSlash(filepath.Join(storagePath, dir.Name())))
		if err != nil {
			return nil, err
		}
		for _, child := range children {
			mediaFiles = append(mediaFiles, filepath.ToSlash(filepath.Join(dir.Name(), filepath.FromSlash(child))))
		}
	}

	return mediaFiles, nil
}

// stremioStreamFromPath creates a StremioStream for a given virtual file path.
func stremioStreamFromPath(virtualPath, baseURL, downloadKey string) StremioStream {
	streamURL := baseURL + "/api/files/stream?path=" +
		url.QueryEscape(virtualPath) + "&download_key=" + url.QueryEscape(downloadKey)
	filename := filepath.Base(virtualPath)
	return StremioStream{
		URL:   streamURL,
		Title: filename,
		Name:  filename,
	}
}

// isMediaExtension reports whether ext is a common video/media file extension.
func isMediaExtension(ext string) bool {
	return mediaExtensions[strings.ToLower(ext)]
}

// deriveNzbNameFromContent inspects raw NZB bytes for a meaningful release name so the
// downstream filename, dedup key, and import output do not fall back to a generic transport
// name (e.g. "download"). It parses locally and never touches the network.
//
// Preference order:
//  1. the <head><meta type="name"> value, when present and not obfuscated;
//  2. otherwise the largest media <file> subject's release stem (extension stripped).
//
// Returns "" when nothing trustworthy is found, so the caller keeps its existing fallback.
func deriveNzbNameFromContent(nzbData []byte) string {
	nzb, err := nzbparser.Parse(bytes.NewReader(nzbData))
	if err != nil {
		return ""
	}

	// 1. Explicit release name in NZB metadata.
	if name := strings.TrimSpace(nzb.Meta["name"]); name != "" && !fileinfo.IsProbablyObfuscated(name) {
		if safe := sanitizeFilename(name); safe != "" {
			return safe
		}
	}

	// 2. Largest media file's subject name.
	bestName := ""
	var bestBytes int64 = -1
	for i := range nzb.Files {
		f := nzb.Files[i]
		if !isMediaExtension(filepath.Ext(f.Filename)) {
			continue
		}
		stem := strings.TrimSuffix(f.Filename, filepath.Ext(f.Filename))
		if stem == "" || fileinfo.IsProbablyObfuscated(stem) {
			continue
		}
		if f.Bytes > bestBytes {
			bestBytes = f.Bytes
			bestName = stem
		}
	}
	if bestName == "" {
		return ""
	}
	return sanitizeFilename(bestName)
}

// parseStremioContentID parses a Stremio content id of the form "tt1234567"
// (movie) or "tt1234567:1:5" (series, season:episode). It returns the imdb id
// and the season/episode numbers, which are 0 when absent or unparseable.
func parseStremioContentID(rawID string) (imdbID string, season, episode int) {
	parts := strings.SplitN(rawID, ":", 3)
	imdbID = strings.TrimSpace(parts[0])
	if len(parts) >= 2 {
		if v, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
			season = v
		}
	}
	if len(parts) >= 3 {
		if v, err := strconv.Atoi(strings.TrimSpace(parts[2])); err == nil {
			episode = v
		}
	}
	return imdbID, season, episode
}

// stremioEpisodeSelectorFromRequest derives the requested season/episode from
// the request, trying several sources so proxies (e.g. AIOStreams) can forward
// episode context in whatever form they support. Priority:
//  1. explicit `season` + `episode` (form or query);
//  2. a combined Stremio id via `id` / `stremio_id` (form or query), e.g. tt123:1:5;
//  3. `season`/`episode` embedded in the `nzb_url` query string.
//
// Returns nil only when none of the sources yield a positive season+episode.
func stremioEpisodeSelectorFromRequest(c *fiber.Ctx) *stremioEpisodeSelector {
	// 1. Explicit discrete fields.
	if season, okSeason := positiveIntFormOrQuery(c, "season"); okSeason {
		if episode, okEpisode := positiveIntFormOrQuery(c, "episode"); okEpisode {
			return &stremioEpisodeSelector{Season: season, Episode: episode}
		}
	}

	// 2. Combined Stremio content id.
	for _, key := range []string{"id", "stremio_id"} {
		if raw := formOrQuery(c, key); raw != "" {
			if _, season, episode := parseStremioContentID(raw); season > 0 && episode > 0 {
				return &stremioEpisodeSelector{Season: season, Episode: episode}
			}
		}
	}

	// 3. season/episode embedded in the nzb_url query string.
	if nzbURL := formOrQuery(c, "nzb_url"); nzbURL != "" {
		if u, err := url.Parse(nzbURL); err == nil {
			q := u.Query()
			if season, sErr := strconv.Atoi(q.Get("season")); sErr == nil && season > 0 {
				if episode, eErr := strconv.Atoi(q.Get("episode")); eErr == nil && episode > 0 {
					return &stremioEpisodeSelector{Season: season, Episode: episode}
				}
			}
		}
	}

	return nil
}

// formOrQuery returns the value for key from the request form, falling back to
// the URL query string.
func formOrQuery(c *fiber.Ctx, key string) string {
	if v := c.FormValue(key); v != "" {
		return v
	}
	return c.Query(key)
}

func positiveIntFormOrQuery(c *fiber.Ctx, key string) (int, bool) {
	value := formOrQuery(c, key)
	if value == "" {
		return 0, false
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, false
	}
	return parsed, true
}

// parseEpisodeFromName extracts a (season, episode) pair from an explicit
// combined marker (SxxExx or NxNN) in name. It returns ok=false when the name
// carries no such marker.
func parseEpisodeFromName(name string) (season, episode int, ok bool) {
	if m := stremioSeasonEpisodePattern.FindStringSubmatch(name); m != nil {
		s, err := strconv.Atoi(m[1])
		if err == nil {
			if em := stremioEpisodeOnlyPattern.FindStringSubmatch(m[2]); em != nil {
				if e, err := strconv.Atoi(em[1]); err == nil {
					return s, e, true
				}
			}
		}
	}
	if m := stremioXEpisodePattern.FindStringSubmatch(name); m != nil {
		s, sErr := strconv.Atoi(m[1])
		e, eErr := strconv.Atoi(m[2])
		if sErr == nil && eErr == nil {
			return s, e, true
		}
	}
	return 0, 0, false
}

// countDistinctEpisodes returns how many of the given media files parse to a
// distinct season+episode via an explicit marker. Files with no recognizable
// marker are ignored, so a movie (with optional samples/extras) counts as 0 or 1.
func countDistinctEpisodes(files []string) int {
	seen := make(map[[2]int]struct{})
	for _, name := range files {
		if season, episode, ok := parseEpisodeFromName(name); ok {
			seen[[2]int{season, episode}] = struct{}{}
		}
	}
	return len(seen)
}

func (s *stremioEpisodeSelector) matches(filename string) bool {
	if s == nil || s.Season <= 0 || s.Episode <= 0 {
		return true
	}

	// Primary: explicit combined SxxExx / SxxExxExx markers.
	hasExplicitMarker := false
	for _, match := range stremioSeasonEpisodePattern.FindAllStringSubmatch(filename, -1) {
		hasExplicitMarker = true
		season, err := strconv.Atoi(match[1])
		if err != nil || season != s.Season {
			continue
		}
		for _, episodeMatch := range stremioEpisodeOnlyPattern.FindAllStringSubmatch(match[2], -1) {
			episode, err := strconv.Atoi(episodeMatch[1])
			if err == nil && episode == s.Episode {
				return true
			}
		}
	}

	// Primary: NxNN (e.g. 1x05).
	for _, match := range stremioXEpisodePattern.FindAllStringSubmatch(filename, -1) {
		hasExplicitMarker = true
		season, seasonErr := strconv.Atoi(match[1])
		episode, episodeErr := strconv.Atoi(match[2])
		if seasonErr == nil && episodeErr == nil && season == s.Season && episode == s.Episode {
			return true
		}
	}

	// If the name carried an explicit combined marker, trust only that — never
	// fall back to looser matching (which could treat S02E05 as S01E05).
	if hasExplicitMarker {
		return false
	}

	// Fallback: the season is established by a separate token (often the parent
	// directory, e.g. "Season 01/Show E05.mkv") plus an episode-only token.
	seasonMatched := false
	for _, m := range stremioSeasonContextPattern.FindAllStringSubmatch(filename, -1) {
		if v, err := strconv.Atoi(m[1]); err == nil && v == s.Season {
			seasonMatched = true
			break
		}
	}
	if !seasonMatched {
		return false
	}
	for _, m := range stremioEpisodeContextPattern.FindAllStringSubmatch(filename, -1) {
		if v, err := strconv.Atoi(m[1]); err == nil && v == s.Episode {
			return true
		}
	}
	return false
}

// hasConflictingMarker checks if a filename contains an explicit season/episode tag
// that conflicts with the requested season or episode.
func (s *stremioEpisodeSelector) hasConflictingMarker(filename string) bool {
	if s == nil || s.Season <= 0 || s.Episode <= 0 {
		return false
	}
	for _, match := range stremioSeasonEpisodePattern.FindAllStringSubmatch(filename, -1) {
		season, err := strconv.Atoi(match[1])
		if err == nil && season != s.Season {
			return true
		}
		for _, episodeMatch := range stremioEpisodeOnlyPattern.FindAllStringSubmatch(match[2], -1) {
			episode, err := strconv.Atoi(episodeMatch[1])
			if err == nil && episode != s.Episode {
				return true
			}
		}
	}
	for _, match := range stremioXEpisodePattern.FindAllStringSubmatch(filename, -1) {
		season, seasonErr := strconv.Atoi(match[1])
		episode, episodeErr := strconv.Atoi(match[2])
		if seasonErr == nil && episodeErr == nil && (season != s.Season || episode != s.Episode) {
			return true
		}
	}
	return false
}
