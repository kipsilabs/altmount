package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/kipsilabs/altmount/internal/arrs"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/httpclient"
	"github.com/kipsilabs/altmount/internal/importer/utils"
	"github.com/kipsilabs/altmount/internal/importer/utils/nzbtrim"
	apputils "github.com/kipsilabs/altmount/internal/utils"
)

// getDefaultCategory returns the Default category from config or a fallback
func (s *Server) getDefaultCategory() config.SABnzbdCategory {
	if s.configManager != nil {
		cfg := s.configManager.GetConfig()
		for _, cat := range cfg.SABnzbd.Categories {
			if cat.Name == config.DefaultCategoryName {
				return cat
			}
		}
	}
	// Fallback if not found in config
	return config.SABnzbdCategory{
		Name:     config.DefaultCategoryName,
		Order:    0,
		Priority: 0,
		Dir:      config.DefaultCategoryDir,
	}
}

// qf returns a request parameter from the query string, falling back to the
// form body when absent. AltMount historically read auth/mode from the query
// string only; some SABnzbd clients (e.g. Sportarr) send these as multipart
// form fields, which is also valid per the SABnzbd API. Query takes precedence
// so existing query-string clients (Sonarr/Radarr) are unaffected.
func qf(c *fiber.Ctx, key string) string {
	if v := c.Query(key); v != "" {
		return v
	}
	return c.FormValue(key)
}

// handleSABnzbd is the main handler for SABnzbd API endpoints
func (s *Server) handleSABnzbd(c *fiber.Ctx) error {
	// Check if SABnzbd API is enabled
	if s.configManager != nil {
		config := s.configManager.GetConfig()
		if config.SABnzbd.Enabled == nil || !*config.SABnzbd.Enabled {
			return c.Status(404).SendString("Not Found")
		}
	}

	// Extract authentication parameters
	apiKey := qf(c, "apikey")
	maUsername := qf(c, "ma_username") // ARR URL
	maPassword := qf(c, "ma_password") // ARR API key

	// Determine authentication method
	authenticated := false

	// Method 1: Traditional API key authentication
	if apiKey != "" {
		if s.validateAPIKey(c, apiKey) {
			authenticated = true
			// Still try auto-registration if ARR credentials provided
			if maUsername != "" && maPassword != "" {
				if s.arrsService == nil {
					return s.writeSABnzbdErrorFiber(c, "Radarr/Sonarr Management is disabled")
				}
				s.tryAutoRegisterARR(c)
			}
		}
	}

	// Method 2: ARR credentials authentication
	if !authenticated && maUsername != "" && maPassword != "" {
		if s.validateARRCredentials(c, maUsername, maPassword) {
			authenticated = true
		}
	}

	// Check if authenticated by either method
	if !authenticated {
		if apiKey != "" {
			return s.writeSABnzbdErrorFiber(c, "Invalid API key")
		}

		if maUsername != "" && s.arrsService == nil {
			return s.writeSABnzbdErrorFiber(c, "Radarr/Sonarr Management is disabled")
		}

		return s.writeSABnzbdErrorFiber(c, "Authentication required: provide either apikey or ma_username+ma_password")
	}

	// Get mode parameter to determine which API method to call
	mode := qf(c, "mode")
	switch mode {
	case "addfile":
		return s.handleSABnzbdAddFile(c)
	case "addurl":
		return s.handleSABnzbdAddUrl(c)
	case "queue":
		return s.handleSABnzbdQueue(c)
	case "pause":
		return s.handleSABnzbdPause(c)
	case "resume":
		return s.handleSABnzbdResume(c)
	case "switch":
		return s.handleSABnzbdSwitch(c)
	case "history":
		return s.handleSABnzbdHistory(c)
	case "status", "fullstatus":
		return s.handleSABnzbdStatus(c)
	case "get_config":
		return s.handleSABnzbdGetConfig(c)
	case "get_cats":
		return s.handleSABnzbdGetCats(c)
	case "version":
		return s.handleSABnzbdVersion(c)
	default:
		return s.writeSABnzbdErrorFiber(c, fmt.Sprintf("Unknown mode: %s", mode))
	}
}

// handleSABnzbdPause handles global pause
func (s *Server) handleSABnzbdPause(c *fiber.Ctx) error {
	if s.importerService == nil {
		return s.writeSABnzbdErrorFiber(c, "Importer service not available")
	}
	s.importerService.Pause()
	return s.writeSABnzbdResponseFiber(c, SABnzbdResponse{Status: true})
}

// handleSABnzbdResume handles global resume
func (s *Server) handleSABnzbdResume(c *fiber.Ctx) error {
	if s.importerService == nil {
		return s.writeSABnzbdErrorFiber(c, "Importer service not available")
	}
	s.importerService.Resume()
	return s.writeSABnzbdResponseFiber(c, SABnzbdResponse{Status: true})
}

// handleSABnzbdSwitch handles queue position switching.
// value2 is a 0-based queue position (0 = top) per the SABnzbd API spec,
// but some clients send text-based priority tokens instead.
func (s *Server) handleSABnzbdSwitch(c *fiber.Ctx) error {
	value := c.Query("value")
	value2 := c.Query("value2")

	if value == "" || value2 == "" {
		return s.writeSABnzbdErrorFiber(c, "Missing parameters")
	}

	priority := s.parseSABnzbdPriority(value2)
	// Position 0 = "move to top of queue" — override to High regardless of token mapping.
	if value2 == "0" {
		priority = database.QueuePriorityHigh
	}

	// Attempt to parse as database ID first
	id, err := strconv.ParseInt(value, 10, 64)
	if err == nil {
		if err := s.queueRepo.UpdateQueueItemPriority(c.Context(), id, priority); err == nil {
			// When priority is updated by ID, notify web UI of queue change
			if s.progressBroadcaster != nil {
				s.progressBroadcaster.BroadcastQueueChanged()
			}
			return s.writeSABnzbdResponseFiber(c, SABnzbdResponse{Status: true})
		}
	}

	// Fallback to DownloadID lookup if parsing failed or ID not found in queue
	if s.queueRepo != nil {
		item, err := s.queueRepo.GetQueueItemByDownloadID(c.Context(), value)
		if err == nil && item != nil {
			if err := s.queueRepo.UpdateQueueItemPriority(c.Context(), item.ID, priority); err == nil {
				// When priority is updated by DownloadID, notify web UI of queue change
				if s.progressBroadcaster != nil {
					s.progressBroadcaster.BroadcastQueueChanged()
				}
				return s.writeSABnzbdResponseFiber(c, SABnzbdResponse{Status: true})
			}
		}
	}

	return s.writeSABnzbdErrorFiber(c, "Failed to update priority or item not found")
}

// handleSABnzbdQueuePause handles pausing/resuming a queue item
func (s *Server) handleSABnzbdQueuePause(c *fiber.Ctx, pause bool) error {
	value := c.Query("value")
	if value == "" {
		return s.writeSABnzbdErrorFiber(c, "Missing value parameter")
	}

	var item *database.ImportQueueItem
	var err error

	// 1. Try numeric ID
	id, parseErr := strconv.ParseInt(value, 10, 64)
	if parseErr == nil {
		item, err = s.queueRepo.GetQueueItem(c.Context(), id)
	}

	// 2. Fallback to DownloadID if not found or not numeric
	if item == nil && s.queueRepo != nil {
		item, err = s.queueRepo.GetQueueItemByDownloadID(c.Context(), value)
	}

	if err != nil {
		return s.writeSABnzbdErrorFiber(c, "Failed to get queue item")
	}
	if item == nil {
		return s.writeSABnzbdErrorFiber(c, "Queue item not found")
	}

	if pause {
		if item.Status == database.QueueStatusPending {
			if err := s.queueRepo.UpdateQueueItemStatus(c.Context(), item.ID, database.QueueStatusPaused, nil); err != nil {
				return s.writeSABnzbdErrorFiber(c, "Failed to pause item")
			}
		}
	} else {
		if item.Status == database.QueueStatusPaused {
			if err := s.queueRepo.UpdateQueueItemStatus(c.Context(), item.ID, database.QueueStatusPending, nil); err != nil {
				return s.writeSABnzbdErrorFiber(c, "Failed to resume item")
			}
		}
	}

	// When an item is paused or resumed, notify web UI of queue change
	if s.progressBroadcaster != nil {
		s.progressBroadcaster.BroadcastQueueChanged()
	}
	return s.writeSABnzbdResponseFiber(c, SABnzbdResponse{Status: true})
}

// tryAutoRegisterARR attempts to auto-register an ARR instance from SABnzbd request parameters
// It extracts ma_username (ARR URL) and ma_password (ARR API key) from the query parameters
// This method logs errors but does not fail the SABnzbd request if registration fails
func (s *Server) tryAutoRegisterARR(c *fiber.Ctx) {
	// Check if arrsService is available
	if s.arrsService == nil {
		return
	}

	// Extract ma_username (ARR URL) and ma_password (ARR API key)
	maUsername := c.Query("ma_username")
	maPassword := c.Query("ma_password")

	// Both parameters must be present
	if maUsername == "" || maPassword == "" {
		return
	}

	// URL decode the username parameter (contains ARR URL)
	arrURL, err := url.QueryUnescape(maUsername)
	if err != nil {
		slog.ErrorContext(c.Context(), "Failed to decode ma_username parameter", "error", err, "raw_value", maUsername)
	}

	arrAPIKey := maPassword

	slog.DebugContext(c.Context(), "Attempting ARR auto-registration from SABnzbd request",
		"arr_url", arrURL)

	// Attempt to register the instance (category is auto-assigned based on ARR type)
	if err := s.arrsService.RegisterInstance(c.Context(), arrURL, arrAPIKey); err != nil {
		slog.ErrorContext(c.Context(), "Failed to auto-register ARR instance",
			"arr_url", arrURL,
			"error", err)
		return
	}

	slog.InfoContext(c.Context(), "Successfully auto-registered ARR instance", "arr_url", arrURL)
}

// validateARRCredentials validates ARR credentials and auto-registers if needed
// Returns true if credentials are valid (either already registered or newly registered)
func (s *Server) validateARRCredentials(c *fiber.Ctx, maUsername, maPassword string) bool {
	if s.arrsService == nil {
		slog.ErrorContext(c.Context(), "ARR service not available for credential validation")
		return false
	}

	// URL decode the username parameter (contains ARR URL)
	arrURL, err := url.QueryUnescape(maUsername)
	if err != nil {
		slog.ErrorContext(c.Context(), "Failed to decode ma_username parameter", "error", err, "raw_value", maUsername)
		return false
	}

	arrAPIKey := maPassword

	// Step 1: Check if instance exists and credentials match
	if instance := s.findARRInstanceByURL(arrURL); instance != nil {
		// Instance exists, verify credentials match
		if instance.APIKey == arrAPIKey {
			return true
		}

		slog.ErrorContext(c.Context(), "ARR credentials do not match registered instance", "arr_url", arrURL)
		return false
	}

	// Step 2: Instance doesn't exist, try to register it
	slog.DebugContext(c.Context(), "ARR instance not found, attempting auto-registration", "arr_url", arrURL)

	if err := s.arrsService.RegisterInstance(c.Context(), arrURL, arrAPIKey); err != nil {
		slog.ErrorContext(c.Context(), "Failed to auto-register ARR instance",
			"arr_url", arrURL,
			"error", err)

		return false
	}

	slog.InfoContext(c.Context(), "Successfully auto-registered and validated ARR instance", "arr_url", arrURL)
	return true
}

// handleSABnzbdAddFile handles file upload for NZB files
func (s *Server) handleSABnzbdAddFile(c *fiber.Ctx) error {
	if c.Method() != "POST" {
		return s.writeSABnzbdErrorFiber(c, "Method not allowed")
	}

	// Get uploaded file
	file, err := c.FormFile("nzbfile")
	if err != nil {
		// Try alternative field name
		file, err = c.FormFile("name")
		if err != nil {
			return s.writeSABnzbdErrorFiber(c, "No NZB file provided")
		}
	}

	// Validate file extension
	if !nzbtrim.HasNzbExtension(file.Filename) {
		return s.writeSABnzbdErrorFiber(c, "Invalid file type, must be .nzb or .nzb.gz")
	}

	// Get and validate category from form or query parameters
	category := qf(c, "cat")
	if category == "" {
		category = qf(c, "category")
	}
	validatedCategory, err := s.validateSABnzbdCategory(category)
	if err != nil {
		return s.writeSABnzbdErrorFiber(c, err.Error())
	}

	// Ensure category directories exist in both temp and mount paths
	if err := s.ensureCategoryDirectories(validatedCategory); err != nil {
		return s.writeSABnzbdErrorFiber(c, fmt.Sprintf("Failed to create category directories: %v", err))
	}

	// Build category path and create temporary file with category subdirectory
	tempDir := os.TempDir()
	uploadDir := filepath.Join(tempDir, "altmount-uploads")
	if err := os.MkdirAll(uploadDir, 0755); err != nil {
		return s.writeSABnzbdErrorFiber(c, "Failed to create upload directory")
	}

	categoryPath := s.buildCategoryPath(validatedCategory)
	var tempFile string
	if categoryPath != "" {
		tempFile = filepath.Join(uploadDir, categoryPath, file.Filename)
		// Ensure category subfolder exists in temp
		if err := os.MkdirAll(filepath.Dir(tempFile), 0755); err != nil {
			return s.writeSABnzbdErrorFiber(c, "Failed to create category directory")
		}
	} else {
		tempFile = filepath.Join(uploadDir, file.Filename)
	}

	// Save the uploaded file to temporary location
	if err := c.SaveFile(file, tempFile); err != nil {
		return s.writeSABnzbdErrorFiber(c, "Failed to save file")
	}

	// Add to queue using importer service
	if s.importerService == nil {
		return s.writeSABnzbdErrorFiber(c, "Importer service not available")
	}

	// Capture additional metadata from form
	metadata := make(map[string]string)
	if series := c.FormValue("series"); series != "" {
		metadata["series_title"] = series
	}
	if movie := c.FormValue("movie"); movie != "" {
		metadata["movie_title"] = movie
	}

	var metadataJSON *string
	if len(metadata) > 0 {
		if b, err := json.Marshal(metadata); err == nil {
			s := string(b)
			metadataJSON = &s
		}
	}

	// Generate a stable download ID (GUID) for Sonarr/Radarr tracking
	// Some indexers provide a GUID in the 'nzbname' or 'name' parameter
	downloadID := c.FormValue("nzbname")
	if downloadID == "" {
		downloadID = uuid.New().String()
	}

	// Add the file to the processing queue using centralized method
	completeDir := s.configManager.GetConfig().SABnzbd.CompleteDir
	priority := s.parseSABnzbdPriority(c.FormValue("priority"))
	_, err = s.importerService.AddToQueue(c.Context(), tempFile, &completeDir, &validatedCategory, &priority, metadataJSON, &downloadID, nil)
	if err != nil {
		return s.writeSABnzbdErrorFiber(c, "Failed to add to queue")
	}

	// Return success response
	response := SABnzbdAddResponse{
		Status: true,
		NzoIds: []string{downloadID},
	}

	return s.writeSABnzbdResponseFiber(c, response)
}

// handleSABnzbdAddUrl handles adding NZB from URL
func (s *Server) handleSABnzbdAddUrl(c *fiber.Ctx) error {
	nzbUrl := c.Query("name")
	log.Printf("Received NZB download request for URL: %s", nzbUrl)

	if nzbUrl == "" {
		return s.writeSABnzbdErrorFiber(c, "URL parameter 'name' required")
	}

	// Download NZB file from URL using a proper HTTP client with a User-Agent header.
	// Some indexers (e.g. NZBHydra2) return 403 on redirect if User-Agent is missing.
	req, err := http.NewRequestWithContext(c.Context(), http.MethodGet, nzbUrl, nil)
	if err != nil {
		return s.writeSABnzbdErrorFiber(c, "Failed to build NZB download request")
	}
	cfg := s.configManager.GetConfig()
	req.Header.Set("User-Agent", cfg.GetUserAgent())
	resp, err := httpclient.NewForExternal(cfg.Network, httpclient.LongTimeout).Do(req)
	if err != nil {
		return s.writeSABnzbdErrorFiber(c, "Failed to download NZB from URL")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return s.writeSABnzbdErrorFiber(c, fmt.Sprintf("Failed to download NZB: HTTP %d", resp.StatusCode))
	}

	// Get and validate category from query or form parameters
	category := qf(c, "cat")
	if category == "" {
		category = qf(c, "category")
	}
	validatedCategory, err := s.validateSABnzbdCategory(category)
	if err != nil {
		return s.writeSABnzbdErrorFiber(c, err.Error())
	}

	// Ensure category directories exist in both temp and mount paths
	if err := s.ensureCategoryDirectories(validatedCategory); err != nil {
		return s.writeSABnzbdErrorFiber(c, fmt.Sprintf("Failed to create category directories: %v", err))
	}

	// Create temporary file with category path
	tempDir := os.TempDir()
	uploadDir := filepath.Join(tempDir, "altmount-uploads")
	if err := os.MkdirAll(uploadDir, 0755); err != nil {
		return s.writeSABnzbdErrorFiber(c, "Failed to create upload directory")
	}

	// Extract filename: prefer Content-Disposition header, then URL path, then default
	filename := ""

	// 1. Try Content-Disposition header
	if cd := resp.Header.Get("Content-Disposition"); cd != "" {
		if _, params, err := mime.ParseMediaType(cd); err == nil {
			if fn := params["filename"]; fn != "" {
				filename = filepath.Base(fn)
			}
		}
	}

	// 2. Fall back to URL path
	if filename == "" {
		if u, err := url.Parse(nzbUrl); err == nil && u.Path != "" {
			if base := filepath.Base(u.Path); base != "" && base != "." {
				filename = base
			}
		}
	}

	// 3. Final fallback
	if filename == "" {
		filename = "downloaded.nzb"
	}

	// Ensure .nzb or .nzb.gz extension
	if !nzbtrim.HasNzbExtension(filename) {
		filename += ".nzb"
	}

	// Build category path and create temporary file with category subdirectory
	categoryPath := s.buildCategoryPath(validatedCategory)
	var tempFile string
	if categoryPath != "" {
		tempFile = filepath.Join(uploadDir, categoryPath, filename)
		// Ensure category subfolder exists in temp
		if err := os.MkdirAll(filepath.Dir(tempFile), 0755); err != nil {
			return s.writeSABnzbdErrorFiber(c, "Failed to create category directory")
		}
	} else {
		tempFile = filepath.Join(uploadDir, filename)
	}

	outFile, err := os.Create(tempFile)
	if err != nil {
		return s.writeSABnzbdErrorFiber(c, "Failed to create temporary file")
	}
	defer outFile.Close()

	// Copy downloaded content to file
	_, err = io.Copy(outFile, resp.Body)
	if err != nil {
		return s.writeSABnzbdErrorFiber(c, "Failed to save downloaded file")
	}

	// Capture additional metadata from query parameters
	metadata := make(map[string]string)
	if series := c.Query("series"); series != "" {
		metadata["series_title"] = series
	}
	if movie := c.Query("movie"); movie != "" {
		metadata["movie_title"] = movie
	}

	var metadataJSON *string
	if len(metadata) > 0 {
		if b, err := json.Marshal(metadata); err == nil {
			s := string(b)
			metadataJSON = &s
		}
	}

	// Add to queue
	if s.importerService == nil {
		return s.writeSABnzbdErrorFiber(c, "Importer service not available")
	}

	// Add the file to the processing queue using centralized method
	completeDir := s.configManager.GetConfig().SABnzbd.CompleteDir
	priority := s.parseSABnzbdPriority(c.Query("priority"))

	// Generate or extract stable download ID for tracking
	// Some indexers provide a GUID in the 'nzbname' or 'name' parameter
	downloadID := c.Query("nzbname")
	if downloadID == "" {
		// Use filename (without extension) as a fallback ID if it looks like a GUID
		downloadID = strings.TrimSuffix(filename, filepath.Ext(filename))
		if len(downloadID) < 20 { // Simple heuristic: GUIDs are usually long
			downloadID = uuid.New().String()
		}
	}

	_, err = s.importerService.AddToQueue(c.Context(), tempFile, &completeDir, &validatedCategory, &priority, metadataJSON, &downloadID, nil)
	if err != nil {
		return s.writeSABnzbdErrorFiber(c, "Failed to add to queue")
	}

	// Return success response
	response := SABnzbdAddResponse{
		Status: true,
		NzoIds: []string{downloadID},
	}

	return s.writeSABnzbdResponseFiber(c, response)
}

// handleSABnzbdQueue handles queue operations
func (s *Server) handleSABnzbdQueue(c *fiber.Ctx) error {
	// Check for operations
	name := c.Query("name")
	switch name {
	case "delete":
		return s.handleSABnzbdQueueDelete(c)
	case "pause":
		return s.handleSABnzbdQueuePause(c, true)
	case "resume":
		return s.handleSABnzbdQueuePause(c, false)
	}

	// Get category filter from query parameter
	categoryFilter := s.normalizeCategoryFilter(c)

	// Get pagination parameters
	start := 0
	if s := c.Query("start"); s != "" {
		if val, err := strconv.Atoi(s); err == nil {
			start = val
		}
	}
	limit := 100
	if l := c.Query("limit"); l != "" {
		if val, err := strconv.Atoi(l); err == nil {
			if val > 0 {
				limit = val
			} else {
				limit = 100
			}
		}
	}

	// Get pending and processing items
	items, err := s.queueRepo.ListActiveQueueItems(c.Context(), "", categoryFilter, limit, start, "updated_at", "desc")
	if err != nil {
		return s.writeSABnzbdErrorFiber(c, "Failed to get queue")
	}

	// Get total count for pagination info

	// Let's just use a simple approach for now:
	totalCount, err := s.queueRepo.CountActiveQueueItems(c.Context(), "", categoryFilter)
	if err != nil {
		totalCount = len(items) // Fallback
	}

	// Convert to SABnzbd format
	slots := make([]SABnzbdQueueSlot, 0, len(items))
	var totalMb float64
	var totalMbLeft float64

	for i, item := range items {
		if item.Status == database.QueueStatusFallback || item.SkipArrNotification {
			continue
		}

		slot := ToSABnzbdQueueSlot(item, start+i, s.progressBroadcaster)
		slots = append(slots, slot)

		if mb, err := strconv.ParseFloat(slot.Mb, 64); err == nil {
			totalMb += mb
		}
		if mbLeft, err := strconv.ParseFloat(slot.Mbleft, 64); err == nil {
			totalMbLeft += mbLeft
		}
	}

	status := "Idle"
	if len(slots) > 0 {
		status = "Downloading"
	}
	if s.importerService.IsPaused() {
		status = "Paused"
	}

	// Get download speed from pool manager
	kbpersec := "0.00"
	speed := "0"
	if s.poolManager != nil {
		if metrics, err := s.poolManager.GetMetrics(); err == nil {
			kbpersec = fmt.Sprintf("%.2f", metrics.DownloadSpeedBytesPerSec/1024.0)
			speed = fmt.Sprintf("%.0f", metrics.DownloadSpeedBytesPerSec)
		}
	}

	response := SABnzbdQueueResponse{
		Status: true,
		Queue: SABnzbdQueueObject{
			Paused:    s.importerService.IsPaused(),
			Slots:     slots,
			Noofslots: totalCount,
			Status:    status,
			Mbleft:    fmt.Sprintf("%.2f", totalMbLeft),
			Mb:        fmt.Sprintf("%.2f", totalMb),
			Kbpersec:  kbpersec,
			Speed:     speed,
			Version:   "4.5.0",
		},
	}

	return s.writeSABnzbdResponseFiber(c, response)
}

// handleSABnzbdQueueDelete handles deleting items from queue
func (s *Server) handleSABnzbdQueueDelete(c *fiber.Ctx) error {
	nzoID := c.Query("value")

	if nzoID == "" {
		return s.writeSABnzbdErrorFiber(c, "Missing nzo_id parameter")
	}

	if s.importerService == nil {
		return s.writeSABnzbdErrorFiber(c, "Importer service not available")
	}

	// 1. Try numeric ID
	id, err := strconv.ParseInt(nzoID, 10, 64)
	if err == nil {
		// Fetch the item first so we can delete its NZB file after removal
		queueItem, _ := s.queueRepo.GetQueueItem(c.Context(), id)

		// Delete from queue
		err = s.queueRepo.RemoveFromQueue(c.Context(), id)
		if err == nil {
			// Also remove from history if it existed there (to prevent ghost items)
			_, _ = s.queueRepo.RemoveFromHistoryByNzbID(c.Context(), id)
			_, _ = s.queueRepo.RemoveFromHistory(c.Context(), id)

			if queueItem != nil {
				s.removeQueueNzbFiles(c, []string{queueItem.NzbPath})
			}

			// When a queue item is deleted by ID, notify web UI of queue change
			if s.progressBroadcaster != nil {
				s.progressBroadcaster.BroadcastQueueChanged()
			}
			return s.writeSABnzbdResponseFiber(c, SABnzbdDeleteResponse{Status: true})
		}
	}

	// 2. Fallback to DownloadID if not found or not numeric
	if s.queueRepo != nil {
		// Try to find the item first to get its ID and NZB path (for history cleanup)
		item, _ := s.queueRepo.GetQueueItemByDownloadID(c.Context(), nzoID)

		err = s.queueRepo.RemoveFromQueueByDownloadID(c.Context(), nzoID)
		if err == nil {
			// Also remove from history by DownloadID
			_, _ = s.queueRepo.RemoveFromHistoryByDownloadID(c.Context(), nzoID)

			if item != nil {
				_, _ = s.queueRepo.RemoveFromHistoryByNzbID(c.Context(), item.ID)
				s.removeQueueNzbFiles(c, []string{item.NzbPath})
			}

			// When a queue item is deleted by DownloadID, notify web UI of queue change
			if s.progressBroadcaster != nil {
				s.progressBroadcaster.BroadcastQueueChanged()
			}
			return s.writeSABnzbdResponseFiber(c, SABnzbdDeleteResponse{Status: true})
		}
	}

	return s.writeSABnzbdResponseFiber(c, SABnzbdDeleteResponse{Status: true}) // Always return true for delete consistency
}

// handleSABnzbdHistory handles history operations
func (s *Server) handleSABnzbdHistory(c *fiber.Ctx) error {
	// Check for delete operation
	if c.Query("name") == "delete" {
		return s.handleSABnzbdHistoryDelete(c)
	}

	// Get completed and failed items
	if s.importerService == nil {
		return s.writeSABnzbdErrorFiber(c, "Importer service not available")
	}

	ctx, cancel := context.WithTimeout(c.Context(), 10*time.Second)
	defer cancel()

	// Get category filter from query parameter
	categoryFilter := s.normalizeCategoryFilter(c)

	// Get specific job IDs if requested
	nzoIDs := make(map[string]bool)
	if ids := c.Query("nzo_ids"); ids != "" {
		for _, id := range strings.Split(ids, ",") {
			nzoIDs[strings.TrimSpace(id)] = true
		}
	}

	// Get pagination parameters
	start := 0
	if s := c.Query("start"); s != "" {
		if val, err := strconv.Atoi(s); err == nil {
			start = val
		}
	}
	limit := 100
	if l := c.Query("limit"); l != "" {
		if val, err := strconv.Atoi(l); err == nil && val > 0 {
			limit = val
		}
	}
	// Cap the page size so a client cannot force a huge materialization.
	const maxSABnzbdHistoryLimit = 1000
	if limit > maxSABnzbdHistoryLimit {
		limit = maxSABnzbdHistoryLimit
	}

	// When *arr asks for specific nzo_ids, look them up directly so the response
	// is independent of pagination. This is the path Sonarr/Radarr use to confirm
	// a known download by id, and it must succeed regardless of how old the entry
	// is — see issue #543.
	if len(nzoIDs) > 0 {
		return s.respondSABnzbdHistoryByIDs(c, ctx, nzoIDs, categoryFilter, start, limit)
	}

	// Serve the deduplicated, time-ordered union of completed-queue + persistent
	// history + failed-queue with real SQL LIMIT/OFFSET and an exact total. The
	// merge/dedup/pagination that used to run in memory over hardcoded caps now
	// happens in one DB query; failed items keep their existing import_queue
	// lifecycle (FailedItemRetentionHours).
	totalAvailableCount, err := s.queueRepo.CountSABnzbdHistory(ctx, categoryFilter)
	if err != nil {
		return s.writeSABnzbdErrorFiber(c, "Failed to count history")
	}

	historyRows, err := s.queueRepo.ListSABnzbdHistory(ctx, categoryFilter, limit, start)
	if err != nil {
		return s.writeSABnzbdErrorFiber(c, "Failed to get history")
	}

	// #498: Sonarr and Radarr only ever read this first page (start=0,
	// limit=DownloadClientHistoryLimit, default 60). A tracked download that is
	// in neither the queue nor that page is dropped without a downloadFailed
	// event, so no blocklist and no retry. During a burst of grabs, newer
	// completed rows can push a failed row off the page before the *arr polls.
	// Append every failed row still kept in import_queue that the page does not
	// already hold. The page itself, noofslots and later pages are unchanged; a
	// failed row can therefore appear both here and at its own position on a
	// later page.
	if start == 0 {
		failedRows, err := s.queueRepo.ListSABnzbdFailedHistory(ctx, categoryFilter, maxSABnzbdHistoryLimit)
		if err != nil {
			return s.writeSABnzbdErrorFiber(c, "Failed to get history")
		}
		onPage := make(map[int64]bool, len(historyRows))
		for _, row := range historyRows {
			if row.Source == "failed_queue" {
				onPage[row.ID] = true
			}
		}
		for _, row := range failedRows {
			if !onPage[row.ID] {
				historyRows = append(historyRows, row)
			}
		}
	}

	slots := make([]SABnzbdHistorySlot, 0, len(historyRows))
	var totalBytes int64
	itemBasePath := s.calculateItemBasePath()

	for i, row := range historyRows {
		item := sabnzbdHistoryRowToQueueItem(row)
		finalPath, exists := s.calculateHistoryStoragePath(item, itemBasePath)
		slot := ToSABnzbdHistorySlot(item, start+i, finalPath)
		// #596: only rows still present in the live completed queue may be
		// rewritten to Failed when their reported path is missing on disk.
		// Persistent-history and failed rows must never be retroactively failed.
		if !exists && row.Source == "completed_queue" {
			markHistorySlotMissing(&slot, finalPath)
		}
		slots = append(slots, slot)
		totalBytes += slot.Bytes
	}

	// Create the proper history response structure using the new struct
	response := SABnzbdCompleteHistoryResponse{
		History: SABnzbdHistoryObject{
			Slots:     slots,
			TotalSize: formatHumanSize(totalBytes),
			MonthSize: "0 B",
			WeekSize:  "0 B",
			Version:   "4.5.0",
			DaySize:   "0 B",
			Noofslots: totalAvailableCount,
		},
	}

	return s.writeSABnzbdResponseFiber(c, response)
}

// sabnzbdHistoryRowToQueueItem adapts a unified history row into the
// ImportQueueItem shape used by ToSABnzbdHistorySlot.
func sabnzbdHistoryRowToQueueItem(row *database.SABnzbdHistoryRow) *database.ImportQueueItem {
	completedAt := row.CompletedAt
	return &database.ImportQueueItem{
		ID:           row.ID,
		DownloadID:   row.DownloadID,
		NzbPath:      row.Name,
		Status:       row.Status,
		FileSize:     row.FileSize,
		CompletedAt:  completedAt,
		Category:     row.Category,
		StoragePath:  row.StoragePath,
		Metadata:     row.Metadata,
		ErrorMessage: row.ErrorMessage,
	}
}

// respondSABnzbdHistoryByIDs returns a SABnzbd history response containing only
// the rows that match the supplied nzo_ids. It looks each id up directly in the
// queue and persistent history tables, bypassing the bulk retention window so
// *arr clients can always reconcile a download they previously saw — even days
// or weeks later. See issue #543.
func (s *Server) respondSABnzbdHistoryByIDs(
	c *fiber.Ctx,
	ctx context.Context,
	nzoIDs map[string]bool,
	categoryFilter string,
	start, limit int,
) error {
	finalItems := make([]*database.ImportQueueItem, 0, len(nzoIDs))
	seen := make(map[int64]bool, len(nzoIDs))
	// See note in respondSABnzbdHistory: only items still in the live queue
	// with status=Completed should be rewritten to Failed when their reported
	// path is missing. Rows reconstructed from persistent history represent
	// past successful imports.
	liveCompleted := make(map[*database.ImportQueueItem]bool)

	categoryMatches := func(cat *string) bool {
		if categoryFilter == "" {
			return true
		}
		if cat == nil {
			return false
		}
		return strings.EqualFold(*cat, categoryFilter)
	}

	for raw := range nzoIDs {
		id := strings.TrimSpace(raw)
		if id == "" {
			continue
		}

		// 1. Numeric id → queue first, then history.nzb_id.
		if numericID, err := strconv.ParseInt(id, 10, 64); err == nil {
			if item, err := s.queueRepo.GetQueueItem(ctx, numericID); err == nil && item != nil {
				if !item.SkipArrNotification && categoryMatches(item.Category) && !seen[item.ID] {
					finalItems = append(finalItems, item)
					if item.Status == database.QueueStatusCompleted {
						liveCompleted[item] = true
					}
					seen[item.ID] = true
				}
				continue
			}
			if h, err := s.queueRepo.GetImportHistoryByNzbID(ctx, numericID); err == nil && h != nil {
				if categoryMatches(h.Category) && !seen[h.ID] {
					finalItems = append(finalItems, importHistoryToQueueItem(h))
					seen[h.ID] = true
				}
				continue
			}
		}

		// 2. String id → queue.download_id, then history.download_id.
		if item, err := s.queueRepo.GetQueueItemByDownloadID(ctx, id); err == nil && item != nil {
			if !item.SkipArrNotification && categoryMatches(item.Category) && !seen[item.ID] {
				finalItems = append(finalItems, item)
				if item.Status == database.QueueStatusCompleted {
					liveCompleted[item] = true
				}
				seen[item.ID] = true
			}
			continue
		}
		if h, err := s.queueRepo.GetImportHistoryByDownloadID(ctx, id); err == nil && h != nil {
			if categoryMatches(h.Category) && !seen[h.ID] {
				finalItems = append(finalItems, importHistoryToQueueItem(h))
				seen[h.ID] = true
			}
		}
	}

	totalAvailableCount := len(finalItems)

	if start < len(finalItems) {
		finalItems = finalItems[start:]
	} else {
		finalItems = []*database.ImportQueueItem{}
	}
	if limit > 0 && len(finalItems) > limit {
		finalItems = finalItems[:limit]
	}

	slots := make([]SABnzbdHistorySlot, 0, len(finalItems))
	var totalBytes int64
	itemBasePath := s.calculateItemBasePath()
	for i, item := range finalItems {
		finalPath, exists := s.calculateHistoryStoragePath(item, itemBasePath)
		slot := ToSABnzbdHistorySlot(item, start+i, finalPath)
		if !exists && liveCompleted[item] {
			markHistorySlotMissing(&slot, finalPath)
		}
		slots = append(slots, slot)
		totalBytes += slot.Bytes
	}

	response := SABnzbdCompleteHistoryResponse{
		History: SABnzbdHistoryObject{
			Slots:     slots,
			TotalSize: formatHumanSize(totalBytes),
			MonthSize: "0 B",
			WeekSize:  "0 B",
			Version:   "4.5.0",
			DaySize:   "0 B",
			Noofslots: totalAvailableCount,
		},
	}
	return s.writeSABnzbdResponseFiber(c, response)
}

// importHistoryToQueueItem adapts a persistent ImportHistory row into the
// ImportQueueItem shape used by ToSABnzbdHistorySlot.
func importHistoryToQueueItem(h *database.ImportHistory) *database.ImportQueueItem {
	id := h.ID
	if h.NzbID != nil {
		id = *h.NzbID
	}
	completedAt := h.CompletedAt
	fileSize := h.FileSize
	virtualPath := h.VirtualPath
	return &database.ImportQueueItem{
		ID:          id,
		DownloadID:  h.DownloadID,
		NzbPath:     h.NzbName,
		Status:      database.QueueStatusCompleted,
		FileSize:    &fileSize,
		CreatedAt:   completedAt,
		UpdatedAt:   completedAt,
		CompletedAt: &completedAt,
		Category:    h.Category,
		StoragePath: &virtualPath,
	}
}

// handleSABnzbdHistoryDelete handles deleting items from history
func (s *Server) handleSABnzbdHistoryDelete(c *fiber.Ctx) error {
	nzoID := c.Query("value")

	if nzoID == "" {
		return s.writeSABnzbdErrorFiber(c, "Missing nzo_id parameter")
	}

	if s.importerService == nil {
		return s.writeSABnzbdErrorFiber(c, "Importer service not available")
	}

	// 1. Try numeric ID
	id, err := strconv.ParseInt(nzoID, 10, 64)
	if err == nil {
		// Fetch the item first so we can delete its NZB file after removal
		queueItem, _ := s.queueRepo.GetQueueItem(c.Context(), id)

		// Delete from queue (history items are still queue items with completed/failed status)
		err = s.queueRepo.RemoveFromQueue(c.Context(), id)
		if err == nil {
			_, _ = s.queueRepo.RemoveFromHistoryByNzbID(c.Context(), id)
			_, _ = s.queueRepo.RemoveFromHistory(c.Context(), id)

			if queueItem != nil {
				s.removeQueueNzbFiles(c, []string{queueItem.NzbPath})
			}

			// When a history item is deleted by queue ID, notify web UI of queue change
			if s.progressBroadcaster != nil {
				s.progressBroadcaster.BroadcastQueueChanged()
			}
			return s.writeSABnzbdResponseFiber(c, SABnzbdDeleteResponse{Status: true})
		}

		// If not in active queue, it might be in persistent history
		// Try by original NzbID first
		affected, histErr := s.queueRepo.RemoveFromHistoryByNzbID(c.Context(), id)
		if histErr == nil && affected > 0 {
			// When a history item is deleted by NZB ID, notify web UI of queue change
			if s.progressBroadcaster != nil {
				s.progressBroadcaster.BroadcastQueueChanged()
			}
			return s.writeSABnzbdResponseFiber(c, SABnzbdDeleteResponse{Status: true})
		}

		affected, histErr = s.queueRepo.RemoveFromHistory(c.Context(), id)
		if histErr == nil && affected > 0 {
			// When a history item is deleted by history ID, notify web UI of queue change
			if s.progressBroadcaster != nil {
				s.progressBroadcaster.BroadcastQueueChanged()
			}
			return s.writeSABnzbdResponseFiber(c, SABnzbdDeleteResponse{Status: true})
		}
	}

	// 2. Fallback to DownloadID if not found or not numeric
	if s.queueRepo != nil {
		// Try to find the item first to get its ID
		item, _ := s.queueRepo.GetQueueItemByDownloadID(c.Context(), nzoID)

		// Remove from queue and history by DownloadID
		queueErr := s.queueRepo.RemoveFromQueueByDownloadID(c.Context(), nzoID)
		affected, err := s.queueRepo.RemoveFromHistoryByDownloadID(c.Context(), nzoID)

		if item != nil && queueErr == nil {
			s.removeQueueNzbFiles(c, []string{item.NzbPath})
		}

		if err == nil && affected > 0 {
			if item != nil {
				_, _ = s.queueRepo.RemoveFromHistoryByNzbID(c.Context(), item.ID)
			}
			// When a history item is deleted by DownloadID, notify web UI of queue change
			if s.progressBroadcaster != nil {
				s.progressBroadcaster.BroadcastQueueChanged()
			}
			return s.writeSABnzbdResponseFiber(c, SABnzbdDeleteResponse{Status: true})
		}

		// If item was found in queue but not in history, consider it handled
		if item != nil {
			// When a queue item is removed by DownloadID during history delete, notify web UI
			if s.progressBroadcaster != nil {
				s.progressBroadcaster.BroadcastQueueChanged()
			}
			return s.writeSABnzbdResponseFiber(c, SABnzbdDeleteResponse{Status: true})
		}
	}

	return s.writeSABnzbdResponseFiber(c, SABnzbdDeleteResponse{Status: true}) // Always return true for delete consistency
}

// handleSABnzbdStatus handles full status request
func (s *Server) handleSABnzbdStatus(c *fiber.Ctx) error {
	// Get queue information
	var slots []SABnzbdQueueSlot
	var totalMbLeft float64
	if s.queueRepo != nil {
		items, err := s.queueRepo.ListActiveQueueItems(c.Context(), "", "", 50, 0, "updated_at", "desc")
		if err == nil {
			for i, item := range items {
				slot := ToSABnzbdQueueSlot(item, i, s.progressBroadcaster)
				slots = append(slots, slot)

				// Parse mbleft from slot
				if mbLeft, err := strconv.ParseFloat(slot.Mbleft, 64); err == nil {
					totalMbLeft += mbLeft
				}
			}
		}
	}

	// Get actual disk space for storage directory
	cfg := s.configManager.GetConfig()
	targetPath := cfg.MountPath
	if targetPath == "" {
		targetPath = filepath.Join(os.TempDir(), "altmount-uploads")
	}
	diskFree, diskTotal := getDiskSpace(targetPath)

	response := SABnzbdStatusResponse{
		Status:          true,
		Version:         "4.5.0",
		Uptime:          time.Since(s.startTime).String(),
		Color:           "green",
		Darwin:          runtime.GOOS == "darwin",
		Nt:              runtime.GOOS == "windows",
		Pid:             os.Getpid(),
		NewRelURL:       "",
		ActiveDownload:  len(slots) > 0,
		Paused:          s.importerService != nil && s.importerService.IsPaused(),
		PauseInt:        0,
		Remaining:       fmt.Sprintf("%.1f MB", totalMbLeft),
		MbLeft:          totalMbLeft,
		Diskspace1:      formatHumanSize(int64(diskFree)),
		Diskspace2:      "0 B",
		DiskspaceTotal1: formatHumanSize(int64(diskTotal)),
		DiskspaceTotal2: "0 B",
		Loadavg:         "0.0",
		Cache: struct {
			Max  int `json:"max"`
			Left int `json:"left"`
			Art  int `json:"art"`
		}{
			Max:  100,
			Left: 100,
			Art:  0,
		},
		Folders: []string{},
		Slots:   slots,
	}

	return s.writeSABnzbdResponseFiber(c, response)
}

// handleSABnzbdGetConfig handles configuration request
func (s *Server) handleSABnzbdGetConfig(c *fiber.Ctx) error {
	var sabnzbdConfig SABnzbdConfig

	if s.configManager != nil {
		cfg := s.configManager.GetConfig()

		// Build misc configuration
		itemBasePath := s.calculateItemBasePath()
		sabnzbdConfig.Misc = SABnzbdMiscConfig{
			CompleteDir:            apputils.JoinAbsPath(itemBasePath, cfg.SABnzbd.CompleteDir),
			PreCheck:               0,
			HistoryRetention:       "",
			HistoryRetentionOption: "all",
			HistoryRetentionNumber: 1,
		}

		// Build categories from configuration
		if len(cfg.SABnzbd.Categories) > 0 {
			// Use configured categories
			for _, category := range cfg.SABnzbd.Categories {
				sabnzbdConfig.Categories = append(sabnzbdConfig.Categories, SABnzbdCategory{
					Name:     category.Name,
					Order:    category.Order,
					PP:       "3", // Default post-processing
					Script:   "None",
					Dir:      category.Dir,
					Newzbin:  "",
					Priority: category.Priority,
				})
			}
		} else {
			// Use default category when none configured
			sabnzbdConfig.Categories = []SABnzbdCategory{
				{
					Name:     "default",
					Order:    0,
					PP:       "3",
					Script:   "None",
					Dir:      "",
					Newzbin:  "",
					Priority: 0,
				},
			}
		}

		// Empty servers array (not exposing actual server configuration)
		sabnzbdConfig.Servers = []SABnzbdServer{}
	} else {
		// Fallback configuration when no config manager
		sabnzbdConfig = SABnzbdConfig{
			Misc: SABnzbdMiscConfig{
				CompleteDir:            "",
				PreCheck:               0,
				HistoryRetention:       "",
				HistoryRetentionOption: "all",
				HistoryRetentionNumber: 1,
			},
			Categories: []SABnzbdCategory{
				{
					Name:     "default",
					Order:    0,
					PP:       "3",
					Script:   "None",
					Dir:      "",
					Newzbin:  "",
					Priority: 0,
				},
			},
			Servers: []SABnzbdServer{},
		}
	}

	response := SABnzbdConfigResponse{
		Status:  true,
		Version: "4.5.0",
		Config:  sabnzbdConfig,
	}

	return s.writeSABnzbdResponseFiber(c, response)
}

// handleSABnzbdVersion handles version request
func (s *Server) handleSABnzbdVersion(c *fiber.Ctx) error {
	response := SABnzbdVersionResponse{
		Status:  true,
		Version: "4.5.0",
	}

	return s.writeSABnzbdResponseFiber(c, response)
}

// handleSABnzbdGetCats handles category list request (e.g. for Dropped Needle and other SABnzbd clients)
func (s *Server) handleSABnzbdGetCats(c *fiber.Ctx) error {
	categories := []string{"*"}
	seen := map[string]bool{"*": true}

	if s.configManager != nil {
		cfg := s.configManager.GetConfig()
		for _, cat := range cfg.SABnzbd.Categories {
			name := strings.TrimSpace(cat.Name)
			lower := strings.ToLower(name)
			if name != "" && !seen[lower] {
				seen[lower] = true
				categories = append(categories, name)
			}
		}
	}
	if !seen[strings.ToLower(config.DefaultCategoryName)] {
		categories = append(categories, config.DefaultCategoryName)
	}
	// Keep the implicit SABnzbd wildcard category separate from the configured
	// human-readable Default entry; clients expect both representations.

	return s.writeSABnzbdResponseFiber(c, SABnzbdCategoriesResponse{
		Categories: categories,
	})
}

// parseSABnzbdPriority converts SABnzbd priority string to AltMount priority.
// SABnzbd numeric values: 2=Force, 1=High, 0=Normal, -1=Low, -2=Paused.
func (s *Server) parseSABnzbdPriority(priority string) database.QueuePriority {
	switch strings.ToLower(priority) {
	case "force", "2", "high", "1":
		return database.QueuePriorityHigh
	case "low", "-1", "paused", "-2":
		return database.QueuePriorityLow
	default: // "normal", "0", or unrecognized
		return database.QueuePriorityNormal
	}
}

// buildCategoryPath builds the directory path for a category
func (s *Server) buildCategoryPath(category string) string {
	// Empty category uses Default category's Dir
	if category == "" {
		category = config.DefaultCategoryName
	}

	if s.configManager == nil {
		// No config manager, use category name as directory (Default uses its default dir)
		if category == config.DefaultCategoryName {
			return config.DefaultCategoryDir
		}
		return category
	}

	cfg := s.configManager.GetConfig()

	// If no categories are configured, use category name as directory
	if len(cfg.SABnzbd.Categories) == 0 {
		if category == config.DefaultCategoryName {
			return config.DefaultCategoryDir
		}
		return category
	}

	// Look for the category in configuration. Names are compared trimmed and
	// case-insensitively, matching the uniqueness rule config validation
	// applies, so a padded or differently-cased config entry still resolves.
	wanted := strings.ToLower(strings.TrimSpace(category))
	for _, configCategory := range cfg.SABnzbd.Categories {
		if strings.ToLower(strings.TrimSpace(configCategory.Name)) == wanted {
			// Use configured Dir if available, otherwise use category name
			if configCategory.Dir != "" {
				return configCategory.Dir
			}
			// For Default category with empty Dir, return default dir
			if category == config.DefaultCategoryName {
				return config.DefaultCategoryDir
			}
			return category
		}
	}

	// Category not found in configuration, use category name as directory
	return category
}

// validateSABnzbdCategory validates and returns the category, or error if invalid
func (s *Server) validateSABnzbdCategory(category string) (string, error) {
	defaultCategory := s.getDefaultCategory()
	trimmed := strings.TrimSpace(category)
	if trimmed == "" || trimmed == "*" || strings.EqualFold(trimmed, "default") {
		return defaultCategory.Name, nil
	}

	if s.configManager == nil {
		return trimmed, nil
	}

	cfg := s.configManager.GetConfig()

	// If no categories are configured, allow any category
	if len(cfg.SABnzbd.Categories) == 0 {
		return trimmed, nil
	}

	// Check if category exists in configuration (case-insensitive)
	for _, configCategory := range cfg.SABnzbd.Categories {
		if strings.EqualFold(configCategory.Name, trimmed) {
			return configCategory.Name, nil
		}
	}

	// Category not found in configuration
	return "", fmt.Errorf("invalid category '%s' - not found in configuration", trimmed)
}

// writeSABnzbdResponseFiber writes a successful SABnzbd-compatible response (Fiber version)
func (s *Server) writeSABnzbdResponseFiber(c *fiber.Ctx, data any) error {
	return c.Status(200).JSON(data)
}

// writeSABnzbdErrorFiber writes a SABnzbd-compatible error response (Fiber version)
func (s *Server) writeSABnzbdErrorFiber(c *fiber.Ctx, message string) error {
	response := SABnzbdResponse{
		Status: false,
		Error:  &message,
	}
	return c.Status(200).JSON(response) // SABnzbd returns 200 even for errors
}

// ensureCategoryDirectories creates directories for a category in both temp and mount paths
func (s *Server) ensureCategoryDirectories(category string) error {
	if s.configManager == nil {
		return fmt.Errorf("config manager not available")
	}

	categoryPath := s.buildCategoryPath(category)

	// Don't create directory for default category (empty path)
	if categoryPath == "" {
		return nil
	}

	// Create in temp path
	tempDir := filepath.Join(os.TempDir(), "altmount-uploads", categoryPath)
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}

	return nil
}

// normalizeCategoryFilter extracts and normalizes the category filter from query parameters
func (s *Server) normalizeCategoryFilter(c *fiber.Ctx) string {
	category := c.Query("category", "")
	if category == "" {
		category = c.Query("cat", "")
	}

	lower := strings.ToLower(category)
	if lower == "all" || lower == "*" || lower == "default" {
		return ""
	}

	return lower
}

// calculateItemBasePath calculates the base path for an item based on the import strategy configuration
func (s *Server) calculateItemBasePath() string {
	if s.configManager == nil {
		return ""
	}

	cfg := s.configManager.GetConfig()

	// Determine if we should use import directory or mount path
	var basePath string
	if cfg.Import.ImportStrategy != config.ImportStrategyNone &&
		cfg.Import.ImportDir != nil && *cfg.Import.ImportDir != "" {
		// Use import directory as base when import strategy is enabled
		basePath = *cfg.Import.ImportDir
	} else {
		// Fall back to mount path
		basePath = cfg.MountPath
	}

	// Return base path with category folder
	return basePath
}

// calculateHistoryStoragePath calculates the final storage path to report to SABnzbd history for Sonarr/Radarr.
//
// The second return value reports whether the path that altmount is about to
// hand back to the ARR client exists on disk. Callers should treat items
// where exists=false as failed downloads (see markHistorySlotMissing), to
// stop Sonarr/Radarr from looping on FileNotFoundException for items whose
// symlink under Import.ImportDir was never created (issue #596).
func (s *Server) calculateHistoryStoragePath(item *database.ImportQueueItem, basePath string) (string, bool) {
	if s.configManager == nil || item.StoragePath == nil || *item.StoragePath == "" {
		return basePath, true
	}

	cfg := s.configManager.GetConfig()
	storagePath := *item.StoragePath

	// Determine category folder
	category := config.DefaultCategoryName
	if item.Category != nil && *item.Category != "" {
		category = *item.Category
	}
	categoryPath := s.buildCategoryPath(category)

	// 1. Get the internal relative path (relative to FUSE mount)
	relPath := strings.TrimPrefix(storagePath, "/")

	// 2. Strip any existing /complete or /category prefix from the internal path to start clean
	if cfg.SABnzbd.CompleteDir != "" {
		completeDir := strings.Trim(filepath.ToSlash(cfg.SABnzbd.CompleteDir), "/")
		if after, ok := strings.CutPrefix(relPath, completeDir+"/"); ok {
			relPath = after
		} else if relPath == completeDir {
			relPath = ""
		}
	}
	if categoryPath != "" {
		if after, ok := strings.CutPrefix(relPath, categoryPath+"/"); ok {
			relPath = after
		} else if relPath == categoryPath {
			relPath = ""
		}
	}

	// 3. Determine the base path for reporting
	// For NONE, use MountPath. For others, use ImportDir.
	finalBasePath := cfg.MountPath
	if cfg.Import.ImportStrategy != config.ImportStrategyNone {
		if cfg.Import.ImportDir != nil && *cfg.Import.ImportDir != "" {
			finalBasePath = *cfg.Import.ImportDir
		}
	}

	// 4. Build the clean, isolated reporting path
	// Construct: Base + CompleteDir + CategoryPath + RelPath
	pathParts := []string{finalBasePath}
	if cfg.SABnzbd.CompleteDir != "" {
		pathParts = append(pathParts, strings.Trim(cfg.SABnzbd.CompleteDir, "/"))
	}
	if categoryPath != "" {
		pathParts = append(pathParts, categoryPath)
	}
	pathParts = append(pathParts, relPath)

	fullStoragePath := filepath.Join(pathParts...)
	fullStoragePath = filepath.ToSlash(filepath.Clean(fullStoragePath))

	exists := true
	if _, err := os.Stat(fullStoragePath); os.IsNotExist(err) {
		slog.DebugContext(context.Background(), "sabnzbd history: reported path does not exist on disk",
			"item_id", item.ID,
			"storage_path", *item.StoragePath,
			"reported_path", fullStoragePath,
		)
	}

	// Return the full file path for SYMLINK/STRM to help Arrs find it immediately.
	// Otherwise return directory.
	if cfg.Import.ImportStrategy == config.ImportStrategySYMLINK || cfg.Import.ImportStrategy == config.ImportStrategySTRM {
		return fullStoragePath, exists
	}

	if utils.HasPopularExtension(fullStoragePath) {
		return filepath.Dir(fullStoragePath), exists
	}

	return fullStoragePath, exists
}

// normalizeURL normalizes a URL for comparison by removing trailing slashes
func normalizeURL(rawURL string) string {
	return strings.TrimSuffix(rawURL, "/")
}

// findARRInstanceByURL finds an ARR instance by URL
func (s *Server) findARRInstanceByURL(checkURL string) *arrs.ConfigInstance {
	if s.arrsService == nil {
		return nil
	}

	normalizedCheck := normalizeURL(checkURL)
	instances := s.arrsService.GetAllInstances()

	for _, instance := range instances {
		normalizedInstance := normalizeURL(instance.URL)
		if normalizedInstance == normalizedCheck {
			return instance
		}
	}

	return nil
}

// getDiskSpace is defined in sabnzbd_disk_unix.go (non-Windows) and sabnzbd_disk_windows.go (Windows).
