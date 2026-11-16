package api

import (
	"fmt"
	"html"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/kipsilabs/altmount/internal/database"
	internalerrors "github.com/kipsilabs/altmount/internal/errors"
	"github.com/kipsilabs/altmount/internal/httpclient"
	"github.com/kipsilabs/altmount/internal/importer"
	"github.com/kipsilabs/altmount/internal/importer/utils/nzbtrim"
	"github.com/kipsilabs/altmount/internal/nzbfile"
	"github.com/kipsilabs/altmount/internal/nzblnk"
)

// removeQueueNzbFiles deletes the on-disk NZB files for every non-empty path.
// Missing files are ignored; other errors are logged so the caller can still
// report the DB deletion as successful.
func (s *Server) removeQueueNzbFiles(c *fiber.Ctx, paths []string) {
	for _, p := range paths {
		if p == "" {
			continue
		}
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			slog.WarnContext(c.Context(), "Failed to delete NZB file after queue removal",
				"path", p, "error", err)
		}
	}
}

// transformQueueError transforms specific errors to user-friendly messages
func transformQueueError(err *string) string {
	if err == nil {
		return ""
	}

	// Check if it's the article not found error (old raw NNTP message or new sentinel message)
	if strings.Contains(*err, "article is not found") || *err == internalerrors.ErrArticlesNotFound.Error() {
		return internalerrors.ErrArticlesNotFound.Error()
	}

	// Return the original error message for other errors
	return *err
}

// queueStatusFilters are the statuses GET /queue can filter by: every status
// the import_queue status CHECK constraint allows. waiting_repair (imports
// parked for a PAR2 repair) has its own Queue page tab, which this list
// previously rejected.
var queueStatusFilters = []database.QueueStatus{
	database.QueueStatusPending,
	database.QueueStatusProcessing,
	database.QueueStatusWaitingRepair,
	database.QueueStatusCompleted,
	database.QueueStatusFailed,
	database.QueueStatusFallback,
}

var queueStatusFilterValues = func() string {
	names := make([]string, len(queueStatusFilters))
	for i, st := range queueStatusFilters {
		names[i] = string(st)
	}
	return strings.Join(names, ", ")
}()

func validQueueStatusFilter(status database.QueueStatus) bool {
	return slices.Contains(queueStatusFilters, status)
}

// handleListQueue handles GET /api/queue
//
//	@Summary		List queue items
//	@Description	Returns a paginated list of NZB download queue items with optional filters.
//	@Tags			Queue
//	@Produce		json
//	@Param			status		query	string	false	"Filter by status"			Enums(pending,processing,waiting_repair,completed,failed,fallback)
//	@Param			search		query	string	false	"Search by filename"
//	@Param			sort_by		query	string	false	"Sort field"				Enums(created_at,updated_at,status,nzb_path)
//	@Param			sort_order	query	string	false	"Sort direction"			Enums(asc,desc)
//	@Param			since		query	string	false	"ISO8601 timestamp filter"
//	@Param			limit		query	int		false	"Page size (default 50)"
//	@Param			offset		query	int		false	"Page offset"
//	@Success		200			{object}	APIResponse{data=[]QueueItemResponse,meta=APIMeta}
//	@Failure		400			{object}	APIResponse
//	@Failure		401			{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/queue [get]
func (s *Server) handleListQueue(c *fiber.Ctx) error {
	// Parse pagination
	pagination := ParsePaginationFiber(c)

	// Parse status filter
	var statusFilter *database.QueueStatus
	if statusStr := c.Query("status"); statusStr != "" {
		status := database.QueueStatus(statusStr)
		if !validQueueStatusFilter(status) {
			return RespondValidationError(c, "Invalid status filter", "Valid values: "+queueStatusFilterValues)
		}
		statusFilter = &status
	}

	// Parse search parameter
	searchFilter := c.Query("search")

	// Parse sort parameters
	sortBy := c.Query("sort_by", "updated_at")
	sortOrder := c.Query("sort_order", "desc")

	// Validate sort_by
	validSortFields := map[string]bool{
		"created_at": true,
		"updated_at": true,
		"status":     true,
		"nzb_path":   true,
	}
	if !validSortFields[sortBy] {
		sortBy = "updated_at"
	}

	// Validate sort_order
	if sortOrder != "asc" && sortOrder != "desc" {
		sortOrder = "desc"
	}

	// Parse since filter
	var sinceFilter *time.Time
	if since, err := ParseTimeParamFiber(c, "since"); err != nil {
		return RespondValidationError(c, "Invalid since parameter", err.Error())
	} else if since != nil {
		sinceFilter = since
	}

	// Get total count for pagination
	totalCount, err := s.queueRepo.CountQueueItems(c.Context(), statusFilter, searchFilter, "")
	if err != nil {
		return RespondInternalError(c, "Failed to count queue items", err.Error())
	}

	// Get queue items from repository
	items, err := s.queueRepo.ListQueueItems(c.Context(), statusFilter, searchFilter, "", pagination.Limit, pagination.Offset, sortBy, sortOrder)
	if err != nil {
		return RespondInternalError(c, "Failed to retrieve queue items", err.Error())
	}

	// Filter by since if provided
	if sinceFilter != nil {
		filtered := make([]*database.ImportQueueItem, 0)
		for _, item := range items {
			if item.CreatedAt.After(*sinceFilter) || item.UpdatedAt.After(*sinceFilter) {
				filtered = append(filtered, item)
			}
		}
		items = filtered
	}

	// Convert to API response format
	response := make([]*QueueItemResponse, len(items))
	for i, item := range items {
		response[i] = ToQueueItemResponse(item)
	}
	s.annotateQueuePeerOrigin(c.Context(), items, response)

	// Create metadata
	meta := &APIMeta{
		Total:  totalCount,
		Count:  len(response),
		Limit:  pagination.Limit,
		Offset: pagination.Offset,
	}

	return RespondSuccessWithMeta(c, response, meta)
}

// handleGetQueue handles GET /api/queue/{id}
//
//	@Summary		Get queue item
//	@Description	Returns a single queue item by ID.
//	@Tags			Queue
//	@Produce		json
//	@Param			id	path		int	true	"Queue item ID"
//	@Success		200	{object}	APIResponse{data=QueueItemResponse}
//	@Failure		400	{object}	APIResponse
//	@Failure		404	{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/queue/{id} [get]
func (s *Server) handleGetQueue(c *fiber.Ctx) error {
	// Extract ID from path parameter
	idStr := c.Params("id")
	if idStr == "" {
		return RespondBadRequest(c, "Queue item ID is required", "")
	}

	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return RespondBadRequest(c, "Invalid queue item ID", "ID must be a valid integer")
	}

	// Get queue item from repository
	item, err := s.queueRepo.GetQueueItem(c.Context(), id)
	if err != nil {
		return RespondInternalError(c, "Failed to retrieve queue item", err.Error())
	}

	if item == nil {
		return RespondNotFound(c, "Queue item", "")
	}

	// Convert to API response format
	response := ToQueueItemResponse(item)
	s.annotateQueuePeerOrigin(c.Context(), []*database.ImportQueueItem{item}, []*QueueItemResponse{response})
	return RespondSuccess(c, response)
}

// handleDeleteQueue handles DELETE /api/queue/{id}
//
//	@Summary		Delete queue item
//	@Description	Deletes a queue item by ID. Cannot delete items currently being processed.
//	@Tags			Queue
//	@Produce		json
//	@Param			id	path	int	true	"Queue item ID"
//	@Success		204
//	@Failure		400	{object}	APIResponse
//	@Failure		404	{object}	APIResponse
//	@Failure		409	{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/queue/{id} [delete]
func (s *Server) handleDeleteQueue(c *fiber.Ctx) error {
	// Extract ID from path parameter
	idStr := c.Params("id")
	if idStr == "" {
		return RespondBadRequest(c, "Queue item ID is required", "")
	}

	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return RespondBadRequest(c, "Invalid queue item ID", "ID must be a valid integer")
	}

	// Check if item exists first
	item, err := s.queueRepo.GetQueueItem(c.Context(), id)
	if err != nil {
		return RespondInternalError(c, "Failed to check queue item", err.Error())
	}

	if item == nil {
		return RespondNotFound(c, "Queue item", "")
	}

	// Prevent deletion of items currently being processed
	if item.Status == database.QueueStatusProcessing {
		return RespondConflict(c, "Cannot delete item currently being processed", "Wait for processing to complete or fail")
	}

	// Remove from queue
	err = s.queueRepo.RemoveFromQueue(c.Context(), id)
	if err != nil {
		return RespondInternalError(c, "Failed to delete queue item", err.Error())
	}

	s.removeQueueNzbFiles(c, []string{item.NzbPath})

	if s.progressBroadcaster != nil {
		s.progressBroadcaster.BroadcastQueueChanged()
	}

	return RespondNoContent(c)
}

// handleRetryQueue handles POST /api/queue/{id}/retry
//
//	@Summary		Retry queue item
//	@Description	Resets a failed, pending, or completed queue item to pending so it is picked up by the worker pool.
//	@Tags			Queue
//	@Produce		json
//	@Param			id	path		int	true	"Queue item ID"
//	@Success		200	{object}	APIResponse{data=QueueItemResponse}
//	@Failure		400	{object}	APIResponse
//	@Failure		404	{object}	APIResponse
//	@Failure		409	{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/queue/{id}/retry [post]
func (s *Server) handleRetryQueue(c *fiber.Ctx) error {
	// Extract ID from path parameter
	idStr := c.Params("id")
	if idStr == "" {
		return RespondBadRequest(c, "Queue item ID is required", "")
	}

	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return RespondBadRequest(c, "Invalid queue item ID", "ID must be a valid integer")
	}

	// Check if item exists
	item, err := s.queueRepo.GetQueueItem(c.Context(), id)
	if err != nil {
		return RespondInternalError(c, "Failed to check queue item", err.Error())
	}

	if item == nil {
		return RespondNotFound(c, "Queue item", "")
	}

	if item.IsPeerImport() {
		return RespondConflict(c, "Cannot retry a peer import", importer.ErrPeerImportNoNZB.Error())
	}

	// Only allow retry of pending, failed or completed items
	if item.Status != database.QueueStatusPending && item.Status != database.QueueStatusFailed && item.Status != database.QueueStatusCompleted {
		return RespondConflict(c, "Can only retry pending, failed or completed items", "Current status: "+string(item.Status))
	}

	// Update status to pending for manual retry
	err = s.importerService.ExecuteItem(c.Context(), id)
	if err != nil {
		return RespondInternalError(c, "Failed to retry queue item", err.Error())
	}

	if s.progressBroadcaster != nil {
		s.progressBroadcaster.BroadcastQueueChanged()
	}

	// Get updated item
	updatedItem, err := s.queueRepo.GetQueueItem(c.Context(), id)
	if err != nil {
		return RespondInternalError(c, "Failed to retrieve updated queue item", err.Error())
	}

	response := ToQueueItemResponse(updatedItem)
	return RespondSuccess(c, response)
}

// handleCancelQueue handles POST /api/queue/{id}/cancel
//
//	@Summary		Cancel queue item
//	@Description	Requests cancellation of a queue item that is currently being processed.
//	@Tags			Queue
//	@Produce		json
//	@Param			id	path		int	true	"Queue item ID"
//	@Success		202	{object}	APIResponse
//	@Failure		400	{object}	APIResponse
//	@Failure		404	{object}	APIResponse
//	@Failure		409	{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/queue/{id}/cancel [post]
func (s *Server) handleCancelQueue(c *fiber.Ctx) error {
	// Extract ID from path parameter
	idStr := c.Params("id")
	if idStr == "" {
		return RespondBadRequest(c, "Queue item ID is required", "")
	}

	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return RespondBadRequest(c, "Invalid queue item ID", "ID must be a valid integer")
	}

	// Check if item exists
	item, err := s.queueRepo.GetQueueItem(c.Context(), id)
	if err != nil {
		return RespondInternalError(c, "Failed to check queue item", err.Error())
	}

	if item == nil {
		return RespondNotFound(c, "Queue item", "")
	}

	// Only allow cancellation of processing items
	if item.Status != database.QueueStatusProcessing {
		return RespondConflict(c, "Can only cancel items that are currently processing", "Current status: "+string(item.Status))
	}

	// Request cancellation
	if s.importerService == nil {
		return RespondInternalError(c, "Importer service not available", "")
	}

	err = s.importerService.CancelProcessing(id)
	if err != nil {
		return RespondNotFound(c, "Item is not currently processing", err.Error())
	}

	return c.Status(fiber.StatusAccepted).JSON(fiber.Map{
		"success": true,
		"data": fiber.Map{
			"message": "Cancellation requested",
			"id":      id,
		},
	})
}

// handleGetQueueStats handles GET /api/queue/stats
//
//	@Summary		Get queue statistics
//	@Description	Returns current queue statistics (counts by status, average processing time).
//	@Tags			Queue
//	@Produce		json
//	@Success		200	{object}	APIResponse{data=QueueStatsResponse}
//	@Failure		500	{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/queue/stats [get]
func (s *Server) handleGetQueueStats(c *fiber.Ctx) error {
	stats, err := s.queueRepo.GetQueueStats(c.Context())
	if err != nil {
		return RespondInternalError(c, "Failed to retrieve queue statistics", err.Error())
	}

	response := ToQueueStatsResponse(stats)
	return RespondSuccess(c, response)
}

// handleGetQueueHistoricalStats handles GET /api/queue/stats/history
//
//	@Summary		Get historical queue statistics
//	@Description	Returns historical import statistics over a rolling window (last 24h, 7d, 30d, 365d).
//	@Tags			Queue
//	@Produce		json
//	@Param			days	query		int	false	"Number of days of history (default 1, max 365)"
//	@Success		200		{object}	APIResponse{data=QueueHistoricalStatsResponse}
//	@Failure		500		{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/queue/stats/history [get]
func (s *Server) handleGetQueueHistoricalStats(c *fiber.Ctx) error {
	// Get optional days parameter, default to 1 (24h)
	days := 1
	if daysStr := c.Query("days"); daysStr != "" {
		if d, err := strconv.Atoi(daysStr); err == nil && d > 0 {
			days = d
		}
	}

	// Limit to max 365 days for performance
	if days > 365 {
		days = 365
	}

	dailyStats, err := s.queueRepo.GetImportDailyStats(c.Context(), days)
	if err != nil {
		return RespondInternalError(c, "Failed to retrieve queue historical statistics", err.Error())
	}

	// For 24h view, we want more granular hourly stats for strict rolling window
	var hourlyStats []*database.ImportHourlyStat
	if days == 1 {
		hourlyStats, _ = s.queueRepo.GetImportHourlyStats(c.Context(), 24)
	}

	response := ToQueueHistoricalStatsResponse(dailyStats, hourlyStats)
	return RespondSuccess(c, response)
}

// handleClearCompletedQueue handles DELETE /api/queue/completed
//
//	@Summary		Clear completed queue items
//	@Description	Removes all completed items from the queue.
//	@Tags			Queue
//	@Produce		json
//	@Success		200	{object}	APIResponse
//	@Failure		500	{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/queue/completed [delete]
func (s *Server) handleClearCompletedQueue(c *fiber.Ctx) error {
	paths, count, err := s.queueRepo.ClearCompletedQueueItems(c.Context())
	if err != nil {
		return RespondInternalError(c, "Failed to clear completed queue items", err.Error())
	}

	s.removeQueueNzbFiles(c, paths)

	if s.progressBroadcaster != nil {
		s.progressBroadcaster.BroadcastQueueChanged()
	}

	return RespondSuccess(c, fiber.Map{"removed_count": count})
}

// handleClearFailedQueue handles DELETE /api/queue/failed
//
//	@Summary		Clear failed queue items
//	@Description	Removes all failed items from the queue.
//	@Tags			Queue
//	@Produce		json
//	@Success		200	{object}	APIResponse
//	@Failure		500	{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/queue/failed [delete]
func (s *Server) handleClearFailedQueue(c *fiber.Ctx) error {
	paths, count, err := s.queueRepo.ClearFailedQueueItems(c.Context())
	if err != nil {
		return RespondInternalError(c, "Failed to clear failed queue items", err.Error())
	}

	s.removeQueueNzbFiles(c, paths)

	if s.progressBroadcaster != nil {
		s.progressBroadcaster.BroadcastQueueChanged()
	}

	return RespondSuccess(c, fiber.Map{"removed_count": count})
}

// handleClearPendingQueue handles DELETE /api/queue/pending
//
//	@Summary		Clear pending queue items
//	@Description	Removes all pending items from the queue.
//	@Tags			Queue
//	@Produce		json
//	@Success		200	{object}	APIResponse
//	@Failure		500	{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/queue/pending [delete]
func (s *Server) handleClearPendingQueue(c *fiber.Ctx) error {
	paths, count, err := s.queueRepo.ClearPendingQueueItems(c.Context())
	if err != nil {
		return RespondInternalError(c, "Failed to clear pending queue items", err.Error())
	}

	s.removeQueueNzbFiles(c, paths)

	if s.progressBroadcaster != nil {
		s.progressBroadcaster.BroadcastQueueChanged()
	}

	return RespondSuccess(c, fiber.Map{"removed_count": count})
}

// handleDeleteQueueBulk handles DELETE /api/queue/bulk
//
//	@Summary		Bulk delete queue items
//	@Description	Deletes multiple queue items by ID. Cannot delete items currently being processed.
//	@Tags			Queue
//	@Accept			json
//	@Produce		json
//	@Param			body	body		object{ids=[]int}	true	"List of queue item IDs"
//	@Success		200		{object}	APIResponse
//	@Failure		400		{object}	APIResponse
//	@Failure		409		{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/queue/bulk [delete]
func (s *Server) handleDeleteQueueBulk(c *fiber.Ctx) error {
	// Parse request body
	var request struct {
		IDs []int64 `json:"ids"`
	}

	if err := c.BodyParser(&request); err != nil {
		return RespondBadRequest(c, "Invalid request body", err.Error())
	}

	// Validate IDs
	if len(request.IDs) == 0 {
		return RespondBadRequest(c, "No IDs provided", "At least one ID is required")
	}

	// Remove from queue in bulk (this will check for processing items)
	result, err := s.queueRepo.RemoveFromQueueBulk(c.Context(), request.IDs)
	if err != nil {
		// Check if the error is about processing items
		if result != nil && result.ProcessingCount > 0 {
			return RespondConflict(c, "Cannot delete items currently being processed", fmt.Sprintf("%d items are currently being processed", result.ProcessingCount))
		}

		return RespondInternalError(c, "Failed to delete queue items", err.Error())
	}

	s.removeQueueNzbFiles(c, result.DeletedPaths)

	if s.progressBroadcaster != nil {
		s.progressBroadcaster.BroadcastQueueChanged()
	}

	return RespondSuccess(c, fiber.Map{
		"deleted_count": result.DeletedCount,
		"message":       fmt.Sprintf("Successfully deleted %d queue items", result.DeletedCount),
	})
}

// handleUploadToQueue handles POST /api/queue/upload
//
//	@Summary		Upload NZB file to queue
//	@Description	Uploads an NZB file and adds it to the download queue.
//	@Tags			Queue
//	@Accept			multipart/form-data
//	@Produce		json
//	@Param			file			formData	file	true	"NZB file to upload (max 100MB)"
//	@Param			category		formData	string	false	"Optional category"
//	@Param			relative_path	formData	string	false	"Optional relative path under CompleteDir"
//	@Param			priority		formData	int		false	"Priority: 1=high, 2=normal, 3=low"
//	@Success		201				{object}	APIResponse{data=QueueItemResponse}
//	@Failure		400				{object}	APIResponse
//	@Failure		503				{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/queue/upload [post]
func (s *Server) handleUploadToQueue(c *fiber.Ctx) error {
	// Get uploaded file
	file, err := c.FormFile("file")
	if err != nil {
		return RespondBadRequest(c, "No file provided", "A file must be uploaded")
	}

	// Validate file extension
	if !nzbtrim.HasNzbExtension(file.Filename) {
		return RespondValidationError(c, "Invalid file type", "Only .nzb or .nzb.gz files are allowed")
	}

	// Validate file size (100MB limit)
	if file.Size > 100*1024*1024 {
		return RespondValidationError(c, "File too large", "File size must be less than 100MB")
	}

	// Get optional category from form
	category := c.FormValue("category")

	// Get optional relative_path from form
	relativePath := c.FormValue("relative_path")

	// Get optional priority from form
	priorityStr := c.FormValue("priority")
	var priority database.QueuePriority
	if priorityStr != "" {
		if p, err := strconv.Atoi(priorityStr); err == nil {
			priority = database.QueuePriority(p)
		} else {
			priority = database.QueuePriorityNormal
		}
	} else {
		priority = database.QueuePriorityNormal
	}

	// Add to queue using importer service
	if s.importerService == nil {
		return RespondServiceUnavailable(c, "Importer service not available", "The import service is not configured or running")
	}

	var categoryPtr *string
	if category != "" {
		categoryPtr = &category
	}

	// De-dupe re-uploads: update the existing pending item instead of creating a duplicate.
	if existing, err := s.importerService.FindAndUpdatePendingUpload(c.Context(), file.Filename, categoryPtr, &priority); err != nil {
		return RespondInternalError(c, "Failed to update existing queue item", err.Error())
	} else if existing != nil {
		return RespondCreated(c, ToQueueItemResponse(existing))
	}

	// Create temporary directory for upload
	tempDir := os.TempDir()
	uploadDir := filepath.Join(tempDir, "altmount-uploads")
	if err := os.MkdirAll(uploadDir, 0755); err != nil {
		return RespondInternalError(c, "Failed to create upload directory", err.Error())
	}

	// Save the uploaded file to temporary location
	// Use filepath.Base to strip any path components from the filename
	safeFilename := filepath.Base(file.Filename)
	tempFile := filepath.Join(uploadDir, safeFilename)
	if err := c.SaveFile(file, tempFile); err != nil {
		return RespondInternalError(c, "Failed to save file", err.Error())
	}

	// Build base path from CompleteDir for manually uploaded files
	// The category will be appended to this by the processor
	var basePath *string
	if s.configManager != nil {
		completeDir := s.configManager.GetConfig().SABnzbd.CompleteDir
		if completeDir != "" {
			p := completeDir
			if relativePath != "" {
				p = filepath.Join(p, relativePath)
			}
			basePath = &p
		}
	}

	// For manually uploaded files, pass CompleteDir as the base path (not the temp upload directory)
	// The category will be appended to this by processNzbItem in the service
	item, err := s.importerService.AddToQueue(c.Context(), tempFile, basePath, categoryPtr, &priority, nil, nil, nil)
	if err != nil {
		// Clean up temp file on error
		os.Remove(tempFile)
		return RespondInternalError(c, "Failed to add file to queue", err.Error())
	}

	// Convert to API response format
	response := ToQueueItemResponse(item)
	return RespondCreated(c, response)
}

// handleUploadNZBLnk handles POST /api/queue/upload-nzblnk
//
//	@Summary		Add NZBLnk links to queue
//	@Description	Resolves one or more nzblnk:// links and adds the resulting NZBs to the queue.
//	@Tags			Queue
//	@Accept			json
//	@Produce		json
//	@Param			body	body		object{links=[]string,category=string,priority=int,relative_path=string}	true	"NZBLNK request (max 20 links)"
//	@Success		201		{object}	APIResponse
//	@Failure		400		{object}	APIResponse
//	@Failure		503		{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/queue/upload-nzblnk [post]
func (s *Server) handleUploadNZBLnk(c *fiber.Ctx) error {
	// Parse request body
	var req struct {
		Links        []string `json:"links"`
		Category     string   `json:"category"`
		Priority     int      `json:"priority"`
		RelativePath string   `json:"relative_path"`
	}

	if err := c.BodyParser(&req); err != nil {
		return RespondBadRequest(c, "Invalid request body", err.Error())
	}

	// Validate links
	if len(req.Links) == 0 {
		return RespondValidationError(c, "No links provided", "At least one NZBLNK is required")
	}

	if len(req.Links) > 20 {
		return RespondValidationError(c, "Too many links", "Maximum 20 links per request")
	}

	// Check if importer service is available
	if s.importerService == nil {
		return RespondServiceUnavailable(c, "Importer service not available", "The import service is not configured or running")
	}

	// Create resolver
	cfg := s.configManager.GetConfig()
	resolver := nzblnk.NewResolver(cfg.GetUserAgent(), httpclient.NewForExternal(cfg.Network, 30*time.Second))

	// Process each link
	type linkResult struct {
		Link         string `json:"link"`
		Success      bool   `json:"success"`
		QueueID      *int64 `json:"queue_id,omitempty"`
		Title        string `json:"title,omitempty"`
		ErrorMessage string `json:"error_message,omitempty"`
	}

	results := make([]linkResult, 0, len(req.Links))
	successCount := 0

	for _, link := range req.Links {
		result := linkResult{Link: link}

		// Parse the link first to get title for error messages
		params, err := nzblnk.ParseNZBLink(link)
		if err != nil {
			result.ErrorMessage = err.Error()
			results = append(results, result)
			continue
		}
		result.Title = params.Title

		// Resolve the link
		resolved, err := resolver.Resolve(c.Context(), link)
		if err != nil {
			result.ErrorMessage = err.Error()
			results = append(results, result)
			continue
		}

		// Sanitize filename from title
		safeTitle := sanitizeFilename(resolved.Title)

		var categoryPtr *string
		if req.Category != "" {
			categoryPtr = &req.Category
		}
		priority := database.QueuePriority(req.Priority)

		// De-dupe re-submissions: update the existing pending item instead of creating a duplicate.
		if existing, err := s.importerService.FindAndUpdatePendingUpload(c.Context(), safeTitle+".nzb", categoryPtr, &priority); err != nil {
			result.ErrorMessage = "Failed to update existing queue item: " + err.Error()
			results = append(results, result)
			continue
		} else if existing != nil {
			result.Success = true
			result.QueueID = &existing.ID
			successCount++
			results = append(results, result)
			continue
		}

		// Create temp file for the NZB
		tempDir := os.TempDir()
		uploadDir := filepath.Join(tempDir, "altmount-uploads")
		if err := os.MkdirAll(uploadDir, 0755); err != nil {
			result.ErrorMessage = "Failed to create upload directory"
			results = append(results, result)
			continue
		}
		tempFile := filepath.Join(uploadDir, safeTitle+".nzb")

		// Embed password in NZB if provided
		nzbContent := resolved.NZBContent
		if resolved.Password != "" {
			nzbContent = embedPasswordInNZB(nzbContent, resolved.Password)
			slog.DebugContext(c.Context(), "Embedded password in NZB",
				"title", resolved.Title)
		}

		// Write NZB content to file
		if err := os.WriteFile(tempFile, nzbContent, 0644); err != nil {
			result.ErrorMessage = "Failed to save NZB file"
			results = append(results, result)
			continue
		}

		var basePath *string
		if s.configManager != nil {
			completeDir := s.configManager.GetConfig().SABnzbd.CompleteDir
			if completeDir != "" {
				p := completeDir
				if req.RelativePath != "" {
					p = filepath.Join(p, req.RelativePath)
				}
				basePath = &p
			}
		}

		item, err := s.importerService.AddToQueue(c.Context(), tempFile, basePath, categoryPtr, &priority, queueMetadataForPassword(resolved.Password), nil, &resolved.Indexer)
		if err != nil {
			os.Remove(tempFile)
			result.ErrorMessage = "Failed to add to queue: " + err.Error()
			results = append(results, result)
			continue
		}

		result.Success = true
		result.QueueID = &item.ID
		successCount++
		results = append(results, result)
	}

	return RespondCreated(c, fiber.Map{
		"results":       results,
		"success_count": successCount,
		"failed_count":  len(req.Links) - successCount,
	})
}

// sanitizeFilename removes or replaces characters that are invalid in filenames
func sanitizeFilename(name string) string {
	// Replace invalid characters with underscore
	invalid := []string{"/", "\\", ":", "*", "?", "\"", "<", ">", "|"}
	result := name
	for _, char := range invalid {
		result = strings.ReplaceAll(result, char, "_")
	}
	// Trim spaces and dots from ends
	result = strings.TrimSpace(result)
	result = strings.Trim(result, ".")
	// Limit length
	if len(result) > 200 {
		result = result[:200]
	}
	if result == "" {
		result = "download"
	}
	return result
}

// queueMetadataForPassword is the queue metadata of an item whose NZB had a
// user-supplied password embedded by embedPasswordInNZB: it marks the password
// as injected (never copying it), so the importer does not take it for one the
// NZB came with. With no password there is no metadata.
func queueMetadataForPassword(password string) *string {
	if password == "" {
		return nil
	}
	return importer.MarkPasswordInjected(nil)
}

// embedPasswordInNZB injects a password into the NZB XML metadata
func embedPasswordInNZB(nzbContent []byte, password string) []byte {
	if password == "" {
		return nzbContent
	}

	content := string(nzbContent)
	passwordMeta := fmt.Sprintf(`<meta type="password">%s</meta>`, html.EscapeString(password))

	// If <head> exists, insert password meta inside it
	if strings.Contains(content, "<head>") {
		content = strings.Replace(content, "<head>", "<head>\n    "+passwordMeta, 1)
	} else {
		// If no <head>, add one after <nzb...> tag
		re := regexp.MustCompile(`(<nzb[^>]*>)`)
		content = re.ReplaceAllString(content, "$1\n  <head>\n    "+passwordMeta+"\n  </head>")
	}

	return []byte(content)
}

// handleSearchNZBByName handles POST /api/queue/upload-by-name
//
//	@Summary		Search NZB by name and add to queue
//	@Description	Searches for an NZB by name via the configured indexer and adds the result to the queue.
//	@Tags			Queue
//	@Accept			json
//	@Produce		json
//	@Param			body	body		object{name=string,password=string,category=string,priority=int,relative_path=string}	true	"Search request"
//	@Success		201		{object}	APIResponse
//	@Failure		400		{object}	APIResponse
//	@Failure		404		{object}	APIResponse
//	@Failure		503		{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/queue/upload-by-name [post]
func (s *Server) handleSearchNZBByName(c *fiber.Ctx) error {
	var req struct {
		Name         string `json:"name"`
		Password     string `json:"password"`
		Category     string `json:"category"`
		Priority     int    `json:"priority"`
		RelativePath string `json:"relative_path"`
	}
	if err := c.BodyParser(&req); err != nil {
		return RespondBadRequest(c, "Invalid request body", err.Error())
	}
	if strings.TrimSpace(req.Name) == "" {
		return RespondValidationError(c, "Name is required", "")
	}
	if s.importerService == nil {
		return RespondServiceUnavailable(c, "Importer service not available", "The import service is not configured or running")
	}

	// Build a synthetic nzblnk URL using name as both t= and h=
	syntheticLink := "nzblnk:?t=" + url.QueryEscape(req.Name) + "&h=" + url.QueryEscape(req.Name)
	if req.Password != "" {
		syntheticLink += "&p=" + url.QueryEscape(req.Password)
	}

	cfg := s.configManager.GetConfig()
	resolver := nzblnk.NewResolver(cfg.GetUserAgent(), httpclient.NewForExternal(cfg.Network, 30*time.Second))
	resolved, err := resolver.Resolve(c.Context(), syntheticLink)
	if err != nil {
		return RespondNotFound(c, "NZB", "Could not find NZB for name '"+req.Name+"': "+err.Error())
	}

	safeTitle := sanitizeFilename(resolved.Title)

	var categoryPtr *string
	if req.Category != "" {
		categoryPtr = &req.Category
	}
	priority := database.QueuePriority(req.Priority)

	// De-dupe re-submissions: update the existing pending item instead of creating a duplicate.
	if existing, err := s.importerService.FindAndUpdatePendingUpload(c.Context(), safeTitle+".nzb", categoryPtr, &priority); err != nil {
		return RespondInternalError(c, "Failed to update existing queue item", err.Error())
	} else if existing != nil {
		return RespondCreated(c, fiber.Map{
			"queue_id": existing.ID,
			"title":    resolved.Title,
			"indexer":  resolved.Indexer,
		})
	}

	uploadDir := filepath.Join(os.TempDir(), "altmount-uploads")
	if err := os.MkdirAll(uploadDir, 0755); err != nil {
		return RespondInternalError(c, "Failed to create upload directory", err.Error())
	}
	tempFile := filepath.Join(uploadDir, safeTitle+".nzb")

	nzbContent := resolved.NZBContent
	if resolved.Password != "" {
		nzbContent = embedPasswordInNZB(nzbContent, resolved.Password)
		slog.DebugContext(c.Context(), "Embedded password in NZB", "title", resolved.Title)
	}
	if err := os.WriteFile(tempFile, nzbContent, 0644); err != nil {
		return RespondInternalError(c, "Failed to save NZB file", err.Error())
	}

	var basePath *string
	if s.configManager != nil {
		if completeDir := s.configManager.GetConfig().SABnzbd.CompleteDir; completeDir != "" {
			p := completeDir
			if req.RelativePath != "" {
				p = filepath.Join(p, req.RelativePath)
			}
			basePath = &p
		}
	}

	item, err := s.importerService.AddToQueue(c.Context(), tempFile, basePath, categoryPtr, &priority, queueMetadataForPassword(resolved.Password), nil, &resolved.Indexer)
	if err != nil {
		os.Remove(tempFile)
		return RespondInternalError(c, "Failed to add to queue", err.Error())
	}

	return RespondCreated(c, fiber.Map{
		"queue_id": item.ID,
		"title":    resolved.Title,
		"indexer":  resolved.Indexer,
	})
}

// handleRestartQueueBulk handles POST /api/queue/bulk/restart
//
//	@Summary		Bulk restart queue items
//	@Description	Resets multiple queue items to pending status for reprocessing.
//	@Tags			Queue
//	@Accept			json
//	@Produce		json
//	@Param			body	body		object{ids=[]int}	true	"List of queue item IDs"
//	@Success		200		{object}	APIResponse
//	@Failure		400		{object}	APIResponse
//	@Failure		409		{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/queue/bulk/restart [post]
func (s *Server) handleRestartQueueBulk(c *fiber.Ctx) error {
	// Parse request body
	var request struct {
		IDs []int64 `json:"ids"`
	}

	if err := c.BodyParser(&request); err != nil {
		return RespondBadRequest(c, "Invalid request body", err.Error())
	}

	// Validate IDs
	if len(request.IDs) == 0 {
		return RespondBadRequest(c, "No IDs provided", "At least one ID is required")
	}

	// Check if any items are currently being processed
	processedCount := 0
	notFoundCount := 0
	for _, id := range request.IDs {
		item, err := s.queueRepo.GetQueueItem(c.Context(), id)
		if err != nil {
			return RespondInternalError(c, "Failed to check queue item", err.Error())
		}

		if item == nil {
			notFoundCount++
			continue
		}

		if item.Status == database.QueueStatusProcessing {
			processedCount++
		}
		if item.IsPeerImport() {
			return RespondConflict(c, "Cannot restart a peer import", importer.ErrPeerImportNoNZB.Error())
		}
	}

	if notFoundCount == len(request.IDs) {
		return RespondNotFound(c, "Queue items", "None of the provided IDs exist")
	}

	if processedCount > 0 {
		return RespondConflict(c, "Cannot restart items currently being processed", fmt.Sprintf("%d items are currently being processed", processedCount))
	}

	// Restart the queue items
	err := s.queueRepo.RestartQueueItemsBulk(c.Context(), request.IDs)
	if err != nil {
		return RespondInternalError(c, "Failed to restart queue items", err.Error())
	}

	if s.progressBroadcaster != nil {
		s.progressBroadcaster.BroadcastQueueChanged()
	}

	return RespondSuccess(c, fiber.Map{
		"restarted_count": len(request.IDs) - notFoundCount,
		"message":         fmt.Sprintf("Successfully restarted %d queue items", len(request.IDs)-notFoundCount),
	})
}

// handleCancelQueueBulk handles POST /api/queue/bulk/cancel
//
//	@Summary		Bulk cancel queue items
//	@Description	Requests cancellation of multiple currently-processing queue items.
//	@Tags			Queue
//	@Accept			json
//	@Produce		json
//	@Param			body	body		object{ids=[]int}	true	"List of queue item IDs"
//	@Success		202		{object}	APIResponse
//	@Failure		400		{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/queue/bulk/cancel [post]
func (s *Server) handleCancelQueueBulk(c *fiber.Ctx) error {
	// Parse request body
	var request struct {
		IDs []int64 `json:"ids"`
	}

	if err := c.BodyParser(&request); err != nil {
		return RespondBadRequest(c, "Invalid request body", err.Error())
	}

	// Validate IDs
	if len(request.IDs) == 0 {
		return RespondBadRequest(c, "No IDs provided", "At least one ID is required")
	}

	// Check importer service availability
	if s.importerService == nil {
		return RespondInternalError(c, "Importer service not available", "")
	}

	// Cancel each item and track results
	results := make(map[string]any)
	cancelledCount := 0
	notProcessingCount := 0
	notFoundCount := 0

	for _, id := range request.IDs {
		item, err := s.queueRepo.GetQueueItem(c.Context(), id)
		if err != nil {
			results[fmt.Sprintf("%d", id)] = "Error checking status"
			continue
		}

		if item == nil {
			notFoundCount++
			results[fmt.Sprintf("%d", id)] = "Not found"
			continue
		}

		if item.Status != database.QueueStatusProcessing {
			notProcessingCount++
			results[fmt.Sprintf("%d", id)] = fmt.Sprintf("Cannot cancel (status: %s)", item.Status)
			continue
		}

		err = s.importerService.CancelProcessing(id)
		if err != nil {
			results[fmt.Sprintf("%d", id)] = err.Error()
		} else {
			cancelledCount++
			results[fmt.Sprintf("%d", id)] = "Cancellation requested"
		}
	}

	return c.Status(fiber.StatusAccepted).JSON(fiber.Map{
		"success": true,
		"data": fiber.Map{
			"cancelled_count":      cancelledCount,
			"not_processing_count": notProcessingCount,
			"not_found_count":      notFoundCount,
			"results":              results,
			"message":              fmt.Sprintf("Cancellation requested for %d items", cancelledCount),
		},
	})
}

// handleAddTestQueueItem handles POST /api/queue/test
//
//	@Summary		Add test download to queue
//	@Description	Downloads a test NZB from sabnzbd.org and adds it to the queue for connectivity testing.
//	@Tags			Queue
//	@Accept			json
//	@Produce		json
//	@Param			body	body		object{size=string}	true	"Size: 100MB, 1GB, or 10GB"
//	@Success		201		{object}	APIResponse{data=QueueItemResponse}
//	@Failure		400		{object}	APIResponse
//	@Failure		502		{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/queue/test [post]
func (s *Server) handleAddTestQueueItem(c *fiber.Ctx) error {
	// Parse request body
	var req struct {
		Size string `json:"size"` // "100MB", "1GB", "10GB"
	}

	if err := c.BodyParser(&req); err != nil {
		return RespondBadRequest(c, "Invalid request body", err.Error())
	}

	// Validate size
	validSizes := map[string]bool{
		"100MB": true,
		"1GB":   true,
		"10GB":  true,
	}
	if !validSizes[req.Size] {
		return RespondValidationError(c, "Invalid size", "Allowed values: 100MB, 1GB, 10GB")
	}

	// Determine NZB URL
	nzbURL := fmt.Sprintf("https://sabnzbd.org/tests/test_download_%s.nzb", req.Size)

	// Create temporary file
	tempFile, err := os.CreateTemp("", fmt.Sprintf("test_download_%s_*.nzb", req.Size))
	if err != nil {
		return RespondInternalError(c, "Failed to create temp file", err.Error())
	}
	defer tempFile.Close()
	tempPath := tempFile.Name()

	// Download NZB
	resp, err := http.Get(nzbURL)
	if err != nil {
		os.Remove(tempPath)
		return RespondError(c, fiber.StatusBadGateway, "BAD_GATEWAY", "Failed to download test NZB from sabnzbd.org", err.Error())
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		os.Remove(tempPath)
		return RespondError(c, fiber.StatusBadGateway, "BAD_GATEWAY", "Failed to download test NZB", fmt.Sprintf("Remote server returned status: %s", resp.Status))
	}

	// Write content to file
	if _, err := io.Copy(tempFile, resp.Body); err != nil {
		os.Remove(tempPath)
		return RespondInternalError(c, "Failed to save test NZB", err.Error())
	}

	// Check if importer service is available
	if s.importerService == nil {
		os.Remove(tempPath)
		return RespondServiceUnavailable(c, "Importer service not available", "")
	}

	// Add to queue
	category := "test"
	priority := database.QueuePriorityHigh // Prioritize test files

	// Build base path from CompleteDir for manually uploaded files
	var basePath *string
	if s.configManager != nil {
		completeDir := s.configManager.GetConfig().SABnzbd.CompleteDir
		if completeDir != "" {
			basePath = &completeDir
		}
	}

	item, err := s.importerService.AddToQueue(c.Context(), tempPath, basePath, &category, &priority, nil, nil, nil)
	if err != nil {
		os.Remove(tempPath)
		return RespondInternalError(c, "Failed to add test file to queue", err.Error())
	}

	response := ToQueueItemResponse(item)
	return RespondCreated(c, response)
}

// handleUpdateQueueItemPriority handles PATCH /api/queue/{id}/priority
//
//	@Summary		Update queue item priority
//	@Description	Changes the priority of a pending or failed queue item.
//	@Tags			Queue
//	@Accept			json
//	@Produce		json
//	@Param			id		path		int					true	"Queue item ID"
//	@Param			body	body		object{priority=int}	true	"Priority: 1=high, 2=normal, 3=low"
//	@Success		200		{object}	APIResponse{data=QueueItemResponse}
//	@Failure		400		{object}	APIResponse
//	@Failure		404		{object}	APIResponse
//	@Failure		409		{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/queue/{id}/priority [patch]
func (s *Server) handleUpdateQueueItemPriority(c *fiber.Ctx) error {
	idStr := c.Params("id")
	if idStr == "" {
		return RespondBadRequest(c, "Queue item ID is required", "")
	}

	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return RespondBadRequest(c, "Invalid queue item ID", "ID must be a valid integer")
	}

	var req struct {
		Priority int `json:"priority"`
	}
	if err := c.BodyParser(&req); err != nil {
		return RespondBadRequest(c, "Invalid request body", err.Error())
	}

	// Validate priority: 1=high, 2=normal, 3=low
	if req.Priority < 1 || req.Priority > 3 {
		return RespondValidationError(c, "Invalid priority value", "Valid values: 1 (high), 2 (normal), 3 (low)")
	}

	item, err := s.queueRepo.GetQueueItem(c.Context(), id)
	if err != nil {
		return RespondInternalError(c, "Failed to check queue item", err.Error())
	}
	if item == nil {
		return RespondNotFound(c, "Queue item", "")
	}
	if item.Status == database.QueueStatusProcessing {
		return RespondConflict(c, "Cannot change priority of item currently being processed", "")
	}

	if err := s.queueRepo.UpdateQueueItemPriority(c.Context(), id, database.QueuePriority(req.Priority)); err != nil {
		return RespondInternalError(c, "Failed to update priority", err.Error())
	}

	if s.progressBroadcaster != nil {
		s.progressBroadcaster.BroadcastQueueChanged()
	}

	updated, err := s.queueRepo.GetQueueItem(c.Context(), id)
	if err != nil {
		return RespondInternalError(c, "Failed to retrieve updated queue item", err.Error())
	}

	return RespondSuccess(c, ToQueueItemResponse(updated))
}

// handleBulkUpdateQueuePriority handles PATCH /api/queue/bulk/priority
//
//	@Summary		Bulk update queue item priorities
//	@Description	Updates the priority for multiple queue items at once. Items currently being processed are skipped.
//	@Tags			Queue
//	@Accept			json
//	@Produce		json
//	@Param			body	body		object{ids=[]int64,priority=int}	true	"IDs and priority: 1=high, 2=normal, 3=low"
//	@Success		200		{object}	APIResponse{data=object{updated_count=int,skipped_count=int,message=string}}
//	@Failure		400		{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/queue/bulk/priority [patch]
func (s *Server) handleBulkUpdateQueuePriority(c *fiber.Ctx) error {
	var req struct {
		IDs      []int64 `json:"ids"`
		Priority int     `json:"priority"`
	}
	if err := c.BodyParser(&req); err != nil {
		return RespondBadRequest(c, "Invalid request body", err.Error())
	}

	if len(req.IDs) == 0 {
		return RespondBadRequest(c, "No queue item IDs provided", "")
	}

	if req.Priority < 1 || req.Priority > 3 {
		return RespondValidationError(c, "Invalid priority value", "Valid values: 1 (high), 2 (normal), 3 (low)")
	}

	updated, skipped, err := s.queueRepo.UpdateQueueItemsPriorityBulk(c.Context(), req.IDs, database.QueuePriority(req.Priority))
	if err != nil {
		return RespondInternalError(c, "Failed to update priorities", err.Error())
	}

	if s.progressBroadcaster != nil {
		s.progressBroadcaster.BroadcastQueueChanged()
	}

	priorityLabels := map[int]string{1: "high", 2: "normal", 3: "low"}
	return RespondSuccess(c, fiber.Map{
		"updated_count": updated,
		"skipped_count": skipped,
		"message":       fmt.Sprintf("Successfully set %d items to %s priority (%d skipped — currently processing)", updated, priorityLabels[req.Priority], skipped),
	})
}

// handleDownloadNZB handles GET /api/queue/{id}/download
//
//	@Summary		Download NZB file
//	@Description	Downloads the original NZB file associated with a queue item.
//	@Tags			Queue
//	@Produce		application/x-nzb
//	@Param			id	path	int	true	"Queue item ID"
//	@Success		200
//	@Failure		400	{object}	APIResponse
//	@Failure		404	{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/queue/{id}/download [get]
func (s *Server) handleDownloadNZB(c *fiber.Ctx) error {
	// Extract ID from path parameter
	idStr := c.Params("id")
	if idStr == "" {
		return RespondBadRequest(c, "Queue item ID is required", "")
	}

	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return RespondBadRequest(c, "Invalid queue item ID", "ID must be a valid integer")
	}

	// Get queue item from repository
	item, err := s.queueRepo.GetQueueItem(c.Context(), id)
	if err != nil {
		return RespondInternalError(c, "Failed to retrieve queue item", err.Error())
	}

	if item == nil {
		return RespondNotFound(c, "Queue item", "")
	}

	if item.IsPeerImport() {
		return RespondConflict(c, "No NZB for a peer import", importer.ErrPeerImportNoNZB.Error())
	}

	resolved, resolveErr := nzbfile.ResolveOnDisk(item.NzbPath)
	if resolveErr != nil {
		// Raw .nzb is gone (deleted after successful import).
		// Reconstruct the .nzbz path the same way processor.go writes it:
		//   configDir/.nzbs/{sanitizedCategory}/{queueID}-{nzbBasename}.nzbz
		if s.metadataService != nil && s.configManager != nil {
			cfg := s.configManager.GetConfigGetter()()
			configDir := filepath.Dir(cfg.Database.Path)
			if !filepath.IsAbs(configDir) {
				if abs, absErr := filepath.Abs(configDir); absErr == nil {
					configDir = abs
				}
			}
			var categoryStr string
			if item.Category != nil && *item.Category != "" {
				categoryStr = strings.ReplaceAll(*item.Category, `\`, "/")
				categoryStr = strings.Trim(categoryStr, "/")
				for _, part := range strings.Split(categoryStr, "/") {
					if part == ".." || part == "." {
						categoryStr = ""
						break
					}
				}
			}
			nzbBase := nzbtrim.TrimNzbExtension(filepath.Base(item.NzbPath))
			storeRef := filepath.Join(configDir, ".nzbs", categoryStr, fmt.Sprintf("%d-%s.nzbz", item.ID, nzbBase))
			nzbXML, err := s.metadataService.Store().RegenerateNZB(storeRef)
			if err != nil {
				return RespondInternalError(c, "Failed to regenerate NZB from store", err.Error())
			}
			if nzbXML != nil {
				filename := strings.TrimSuffix(filepath.Base(storeRef), ".nzbz") + ".nzb"
				c.Set("Content-Type", "application/x-nzb")
				c.Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))
				return c.Send(nzbXML)
			}
		}
		return RespondNotFound(c, "NZB file", "The NZB file no longer exists on disk and no store was found")
	}

	c.Set("Content-Type", "application/x-nzb")
	c.Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", nzbfile.PlainFilename(resolved)))

	if nzbfile.IsGzipped(resolved) {
		rc, err := nzbfile.Open(resolved)
		if err != nil {
			return RespondInternalError(c, "Failed to open NZB file", err.Error())
		}
		defer rc.Close()

		if _, err := io.Copy(c.Response().BodyWriter(), rc); err != nil {
			return RespondInternalError(c, "Failed to read NZB content", err.Error())
		}
		return nil
	}

	return c.SendFile(resolved)
}
