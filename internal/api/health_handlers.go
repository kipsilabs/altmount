package api

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/utils"
)

// handleListHealth handles GET /api/health
//
//	@Summary		List health records
//	@Description	Returns a paginated list of file health records with optional status/search filters.
//	@Tags			Health
//	@Produce		json
//	@Param			status		query		string	false	"Filter by status"	Enums(pending,checking,corrupted,repair_triggered,healthy)
//	@Param			search		query		string	false	"Search by file path"
//	@Param			sort_by		query		string	false	"Sort field"		Enums(file_path,created_at,status,priority,last_checked,scheduled_check_at)
//	@Param			sort_order	query		string	false	"Sort direction"	Enums(asc,desc)
//	@Param			since		query		string	false	"ISO8601 timestamp filter"
//	@Param			limit		query		int		false	"Page size (default 50)"
//	@Param			offset		query		int		false	"Page offset"
//	@Success		200			{object}	APIResponse{data=[]HealthItemResponse,meta=APIMeta}
//	@Failure		400			{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/health [get]
func (s *Server) handleListHealth(c *fiber.Ctx) error {
	// Parse pagination
	pagination := ParsePaginationFiber(c)

	// Parse search parameter
	search := c.Query("search")

	// Parse sort parameters
	sortBy := c.Query("sort_by", "created_at")
	sortOrder := c.Query("sort_order", "desc")

	// Validate sort parameters
	validSortFields := map[string]bool{
		"file_path":          true,
		"created_at":         true,
		"status":             true,
		"priority":           true,
		"last_checked":       true,
		"scheduled_check_at": true,
	}
	if !validSortFields[sortBy] {
		sortBy = "created_at"
	}

	if sortOrder != "asc" && sortOrder != "desc" {
		sortOrder = "desc"
	}

	// Parse status filter
	var statusFilter *database.HealthStatus
	if statusStr := c.Query("status"); statusStr != "" {
		statusStr = strings.TrimSpace(statusStr)
		status := database.HealthStatus(statusStr)
		// Validate status
		switch status {
		case database.HealthStatusPending, database.HealthStatusChecking, database.HealthStatusCorrupted, database.HealthStatusRepairTriggered, database.HealthStatusHealthy, database.HealthStatusDegraded:
			statusFilter = &status
		default:
			return RespondValidationError(c, fmt.Sprintf("Invalid status filter: '%s'", statusStr), "Valid values: pending, checking, corrupted, repair_triggered, healthy, degraded")
		}
	}

	// Parse since filter
	var sinceFilter *time.Time
	if since, err := ParseTimeParamFiber(c, "since"); err != nil {
		return RespondValidationError(c, "Invalid since parameter", err.Error())
	} else if since != nil {
		sinceFilter = since
	}

	// Get health items with search and sort support
	items, err := s.listHealthItems(c.Context(), statusFilter, pagination, sinceFilter, search, sortBy, sortOrder)
	if err != nil {
		return RespondInternalError(c, "Failed to retrieve health records", err.Error())
	}

	// Get total count for pagination
	totalCount, err := s.countHealthItems(c.Context(), statusFilter, sinceFilter, search)
	if err != nil {
		return RespondInternalError(c, "Failed to count health records", err.Error())
	}

	// Convert to API response format
	response := make([]*HealthItemResponse, len(items))
	for i, item := range items {
		response[i] = ToHealthItemResponse(item)
		if s.healthWorker != nil && item.Status == database.HealthStatusChecking {
			response[i].CheckProgress = s.healthWorker.GetCheckProgress(item.FilePath)
		}
	}

	// Create metadata
	meta := &APIMeta{
		Count:  len(response),
		Limit:  pagination.Limit,
		Offset: pagination.Offset,
		Total:  totalCount,
	}

	return RespondSuccessWithMeta(c, response, meta)
}

// listHealthItems is a helper method to list health items with filters
func (s *Server) listHealthItems(ctx context.Context, statusFilter *database.HealthStatus, pagination Pagination, sinceFilter *time.Time, search string, sortBy string, sortOrder string) ([]*database.FileHealth, error) {
	return s.healthRepo.ListHealthItems(ctx, statusFilter, pagination.Limit, pagination.Offset, sinceFilter, search, sortBy, sortOrder)
}

// countHealthItems is a helper method to count health items with filters
func (s *Server) countHealthItems(ctx context.Context, statusFilter *database.HealthStatus, sinceFilter *time.Time, search string) (int, error) {
	return s.healthRepo.CountHealthItems(ctx, statusFilter, sinceFilter, search)
}

// handleGetHealth handles GET /api/health/{id}
//
//	@Summary		Get health record
//	@Description	Returns a single health record by ID.
//	@Tags			Health
//	@Produce		json
//	@Param			id	path		int	true	"Health record ID"
//	@Success		200	{object}	APIResponse{data=HealthItemResponse}
//	@Failure		400	{object}	APIResponse
//	@Failure		404	{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/health/{id} [get]
func (s *Server) handleGetHealth(c *fiber.Ctx) error {
	// Extract ID from path parameter
	idStr := c.Params("id")
	if idStr == "" {
		return RespondBadRequest(c, "Health record identifier is required", "")
	}

	// Parse as numeric ID
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return RespondBadRequest(c, "Invalid health record ID", "ID must be a valid integer")
	}

	// Get by ID
	item, err := s.healthRepo.GetFileHealthByID(c.Context(), id)
	if err != nil {
		return RespondInternalError(c, "Failed to retrieve health record", err.Error())
	}

	if item == nil {
		return RespondNotFound(c, "Health record", "")
	}

	response := ToHealthItemResponse(item)
	if s.healthWorker != nil && item.Status == database.HealthStatusChecking {
		response.CheckProgress = s.healthWorker.GetCheckProgress(item.FilePath)
	}
	return RespondSuccess(c, response)
}

// handleDeleteHealth handles DELETE /api/health/{id}
//
//	@Summary		Delete health record
//	@Description	Deletes a health record and optionally the associated physical file.
//	@Tags			Health
//	@Produce		json
//	@Param			id				path	int		true	"Health record ID"
//	@Param			delete_file		query	bool	false	"Also delete the physical file"
//	@Success		204
//	@Failure		400				{object}	APIResponse
//	@Failure		404				{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/health/{id} [delete]
func (s *Server) handleDeleteHealth(c *fiber.Ctx) error {
	// Extract ID from path parameter
	idStr := c.Params("id")
	if idStr == "" {
		return RespondBadRequest(c, "Health record identifier is required", "")
	}

	// Parse as numeric ID
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return RespondBadRequest(c, "Invalid health record ID", "ID must be a valid integer")
	}

	// Check if the record exists
	item, err := s.healthRepo.GetFileHealthByID(c.Context(), id)
	if err != nil {
		return RespondInternalError(c, "Failed to check health record", err.Error())
	}

	if item == nil {
		return RespondNotFound(c, "Health record", "")
	}

	// If the item is currently being checked, cancel the check first
	if item.Status == database.HealthStatusChecking {
		// Check if health worker is available
		if s.healthWorker != nil {
			// Check if there's actually an active check to cancel
			if s.healthWorker.IsCheckActive(item.FilePath) {
				// Cancel the health check before deletion
				err = s.healthWorker.CancelHealthCheck(c.Context(), item.FilePath)
				if err != nil {
					return RespondInternalError(c, "Failed to cancel health check before deletion", err.Error())
				}
			}
		}
	}

	// Parse optional deletion flags
	deleteMeta := c.QueryBool("delete_meta", false)
	deleteSymlink := c.QueryBool("delete_symlink", false)

	// Delete the health record from database using ID
	err = s.healthRepo.DeleteHealthRecordByID(c.Context(), id)
	if err != nil {
		return RespondInternalError(c, "Failed to delete health record", err.Error())
	}

	metaDeleted := false
	symlinkDeleted := false

	if deleteMeta || deleteSymlink {
		cfg := s.configManager.GetConfig()

		// Delete metadata if requested
		if deleteMeta && s.metadataService != nil {
			if delErr := s.metadataService.DeleteFileMetadata(c.Context(), item.FilePath); delErr != nil {
				slog.ErrorContext(c.Context(), "Failed to delete metadata during health record deletion", "file_path", item.FilePath, "error", delErr)
			} else {
				metaDeleted = true
				if s.healthWorker != nil {
					s.healthWorker.NotifyRcloneVFS(item.FilePath)
				}
			}
		}

		// Delete library symlink if requested
		if deleteSymlink {
			if deleteLibraryFile(c.Context(), cfg, item) {
				symlinkDeleted = true
			}
		}
	}

	return RespondSuccess(c, fiber.Map{
		"message":         "Health record deleted successfully",
		"id":              id,
		"file_path":       item.FilePath,
		"deleted_at":      time.Now().Format(time.RFC3339),
		"meta_deleted":    metaDeleted,
		"symlink_deleted": symlinkDeleted,
	})
}

// handleDeleteHealthBulk handles POST /api/health/bulk/delete
//
//	@Summary		Bulk delete health records
//	@Description	Deletes multiple health records by ID.
//	@Tags			Health
//	@Accept			json
//	@Produce		json
//	@Param			body	body		object{ids=[]int}	true	"List of health record IDs"
//	@Success		200		{object}	APIResponse
//	@Failure		400		{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/health/bulk/delete [post]
func (s *Server) handleDeleteHealthBulk(c *fiber.Ctx) error {
	// Parse request body
	var req struct {
		FilePaths     []string `json:"file_paths"`
		DeleteMeta    bool     `json:"delete_meta"`
		DeleteSymlink bool     `json:"delete_symlink"`
	}

	if err := c.BodyParser(&req); err != nil {
		return RespondBadRequest(c, "Invalid request body", err.Error())
	}

	// Validate file paths
	if len(req.FilePaths) == 0 {
		return RespondValidationError(c, "At least one file path is required", "")
	}

	metaDeletedCount := 0
	symlinkDeletedCount := 0

	// Check for any items currently being checked and cancel if needed
	// Also perform meta/symlink deletions before removing DB records
	if s.healthWorker != nil || req.DeleteMeta || req.DeleteSymlink {
		cfg := s.configManager.GetConfig()

		for _, filePath := range req.FilePaths {
			// Get the record to check status and get library path
			item, err := s.healthRepo.GetFileHealth(c.Context(), filePath)
			if err != nil {
				continue
			}

			if item != nil && item.Status == database.HealthStatusChecking {
				if s.healthWorker != nil && s.healthWorker.IsCheckActive(filePath) {
					_ = s.healthWorker.CancelHealthCheck(c.Context(), filePath)
				}
			}

			if item == nil {
				continue
			}

			// Delete metadata if requested
			if req.DeleteMeta && s.metadataService != nil {
				if delErr := s.metadataService.DeleteFileMetadata(c.Context(), item.FilePath); delErr != nil {
					slog.ErrorContext(c.Context(), "Failed to delete metadata during bulk deletion", "file_path", item.FilePath, "error", delErr)
				} else {
					metaDeletedCount++
					if s.healthWorker != nil {
						s.healthWorker.NotifyRcloneVFS(item.FilePath)
					}
				}
			}

			// Delete library symlink if requested
			if req.DeleteSymlink {
				if deleteLibraryFile(c.Context(), cfg, item) {
					symlinkDeletedCount++
				}
			}
		}
	}

	// Delete health records in bulk
	deletedCount, err := s.healthRepo.DeleteHealthRecordsBulk(c.Context(), req.FilePaths)
	if err != nil {
		return RespondInternalError(c, "Failed to delete health records", err.Error())
	}

	return RespondSuccess(c, fiber.Map{
		"message":               "Health records deleted successfully",
		"deleted_count":         deletedCount,
		"file_paths":            req.FilePaths,
		"deleted_at":            time.Now().Format(time.RFC3339),
		"meta_deleted_count":    metaDeletedCount,
		"symlink_deleted_count": symlinkDeletedCount,
	})
}

// handleRepairHealth handles POST /api/health/{id}/repair
//
//	@Summary		Trigger health repair
//	@Description	Triggers a repair attempt for a corrupted file.
//	@Tags			Health
//	@Accept			json
//	@Produce		json
//	@Param			id		path		int					true	"Health record ID"
//	@Param			body	body		HealthRepairRequest	false	"Repair options"
//	@Success		200		{object}	APIResponse{data=HealthItemResponse}
//	@Failure		400		{object}	APIResponse
//	@Failure		404		{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/health/{id}/repair [post]
func (s *Server) handleRepairHealth(c *fiber.Ctx) error {
	// Extract ID from path parameter
	idStr := c.Params("id")
	if idStr == "" {
		return RespondBadRequest(c, "Health record identifier is required", "")
	}

	// Parse as numeric ID
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return RespondBadRequest(c, "Invalid health record ID", "ID must be a valid integer")
	}

	// Parse request body
	var req HealthRepairRequest
	if len(c.Body()) > 0 {
		if err := c.BodyParser(&req); err != nil {
			return RespondBadRequest(c, "Invalid request body", err.Error())
		}
	}

	// Check if item exists
	item, err := s.healthRepo.GetFileHealthByID(c.Context(), id)
	if err != nil {
		return RespondInternalError(c, "Failed to check health record", err.Error())
	}

	if item == nil {
		return RespondNotFound(c, "Health record", "")
	}

	// If already triggered, don't allow re-triggering to avoid redundancy
	if item.Status == database.HealthStatusRepairTriggered {
		return RespondConflict(c, "Repair already in progress", "This file is already in 'repair_triggered' status. Please wait for the ARR to complete the repair.")
	}

	// Determine the path to use for ARR rescan
	// Step 1: Try to use library_path from database if available
	// Step 2: If not in DB, search for library item using FindLibraryItem
	// Step 3: Determine final path (library path or mount path fallback)
	// Step 4: Trigger rescan with resolved path
	ctx := c.Context()
	cfg := s.configManager.GetConfig()

	var libraryPath string
	if item.LibraryPath != nil && *item.LibraryPath != "" {
		libraryPath = *item.LibraryPath
	}

	// Determine final path for ARR rescan
	pathForRescan := libraryPath
	if pathForRescan == "" && cfg.Health.LibraryDir != nil && *cfg.Health.LibraryDir != "" {
		pathForRescan = utils.JoinAbsPath(*cfg.Health.LibraryDir, item.FilePath)
		slog.InfoContext(ctx, "Using library dir for manual repair",
			"file_path", item.FilePath,
			"library_path", pathForRescan)
	}
	if pathForRescan == "" && cfg.Import.ImportStrategy == config.ImportStrategySYMLINK && cfg.Import.ImportDir != nil && *cfg.Import.ImportDir != "" {
		pathForRescan = utils.JoinAbsPath(*cfg.Import.ImportDir, item.FilePath)
		slog.InfoContext(ctx, "Using symlink import path for manual repair",
			"file_path", item.FilePath,
			"symlink_path", pathForRescan)
	}
	if pathForRescan == "" {
		// Fallback to mount path if no library path found
		pathForRescan = utils.JoinAbsPath(cfg.MountPath, item.FilePath)
		slog.InfoContext(ctx, "Using mount path fallback for manual repair",
			"file_path", item.FilePath,
			"mount_path", pathForRescan)
	}

	// Trigger rescan with the resolved path
	err = s.arrsService.TriggerFileRescan(ctx, pathForRescan, item.FilePath, item.Metadata)
	if err != nil {
		// Check if this is a "no ARR instance found" error
		if strings.Contains(err.Error(), "no ARR instance found") {
			return RespondNotFound(c, "File not managed by any ARR instance", "This file is not found in any of the configured Radarr or Sonarr instances. Please ensure the file is in your media library and the ARR instances are properly configured.")
		}
		// Handle other errors as internal server errors
		return RespondInternalError(c, "Failed to trigger repair in ARR instance, you might need to trigger a manual library sync", err.Error())
	}

	// Update status to repair_triggered instead of deleting
	if err := s.healthRepo.SetRepairTriggered(ctx, item.FilePath, item.LastError, item.ErrorDetails); err != nil {
		slog.ErrorContext(ctx, "Failed to set repair_triggered status after repair trigger",
			"error", err,
			"file_path", item.FilePath)
		// Don't fail the repair trigger if update fails
	} else {
		slog.InfoContext(ctx, "Set status to repair_triggered after successful repair trigger",
			"file_path", item.FilePath)
	}

	// Get updated item
	updatedItem, err := s.healthRepo.GetFileHealthByID(c.Context(), id)
	if err != nil {
		return RespondInternalError(c, "Failed to retrieve updated health record", err.Error())
	}

	response := ToHealthItemResponse(updatedItem)
	return RespondSuccess(c, response)
}

// handleRepairHealthBulk handles POST /api/health/bulk/repair
//
//	@Summary		Bulk trigger health repair
//	@Description	Triggers repair attempts for multiple corrupted files.
//	@Tags			Health
//	@Accept			json
//	@Produce		json
//	@Param			body	body		object{ids=[]int}	true	"List of health record IDs"
//	@Success		200		{object}	APIResponse
//	@Failure		400		{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/health/bulk/repair [post]
func (s *Server) handleRepairHealthBulk(c *fiber.Ctx) error {
	// Parse request body
	var req struct {
		FilePaths []string `json:"file_paths"`
	}

	if err := c.BodyParser(&req); err != nil {
		return RespondBadRequest(c, "Invalid request body", err.Error())
	}

	// Validate file paths
	if len(req.FilePaths) == 0 {
		return RespondValidationError(c, "At least one file path is required", "")
	}

	ctx := c.Context()
	cfg := s.configManager.GetConfig()
	successCount := 0
	failedCount := 0
	errors := make(map[string]string)

	for _, filePath := range req.FilePaths {
		// Check if item exists
		item, err := s.healthRepo.GetFileHealth(ctx, filePath)
		if err != nil {
			failedCount++
			errors[filePath] = fmt.Sprintf("Failed to check health record: %v", err)
			continue
		}

		if item == nil {
			failedCount++
			errors[filePath] = "Health record not found"
			continue
		}

		// Skip if already triggered
		if item.Status == database.HealthStatusRepairTriggered {
			continue // Silently skip items already being repaired
		}

		// Determine path for rescan
		var libraryPath string
		if item.LibraryPath != nil && *item.LibraryPath != "" {
			libraryPath = *item.LibraryPath
		}

		pathForRescan := libraryPath
		if pathForRescan == "" && cfg.Health.LibraryDir != nil && *cfg.Health.LibraryDir != "" {
			pathForRescan = utils.JoinAbsPath(*cfg.Health.LibraryDir, item.FilePath)
		}
		if pathForRescan == "" && cfg.Import.ImportStrategy == config.ImportStrategySYMLINK && cfg.Import.ImportDir != nil && *cfg.Import.ImportDir != "" {
			pathForRescan = utils.JoinAbsPath(*cfg.Import.ImportDir, item.FilePath)
		}
		if pathForRescan == "" {
			pathForRescan = utils.JoinAbsPath(cfg.MountPath, item.FilePath)
		}

		// Trigger rescan
		err = s.arrsService.TriggerFileRescan(ctx, pathForRescan, item.FilePath, item.Metadata)
		if err != nil {
			failedCount++
			errors[filePath] = fmt.Sprintf("Failed to trigger repair: %v", err)
			continue
		}

		// Update status to repair_triggered instead of deleting
		if err := s.healthRepo.SetRepairTriggered(ctx, item.FilePath, item.LastError, item.ErrorDetails); err != nil {
			slog.ErrorContext(ctx, "Failed to set repair_triggered status after repair trigger",
				"error", err,
				"file_path", item.FilePath)
			// Don't count as failure since repair was triggered
		}

		successCount++
	}

	return RespondSuccess(c, fiber.Map{
		"message":       "Bulk repair operation completed",
		"success_count": successCount,
		"failed_count":  failedCount,
		"errors":        errors,
	})
}

// handleListCorrupted handles GET /api/health/corrupted
//
//	@Summary		List corrupted files
//	@Description	Returns a paginated list of health records with corrupted status.
//	@Tags			Health
//	@Produce		json
//	@Param			limit	query		int	false	"Page size (default 50)"
//	@Param			offset	query		int	false	"Page offset"
//	@Success		200		{object}	APIResponse{data=[]HealthItemResponse,meta=APIMeta}
//	@Failure		500		{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/health/corrupted [get]
func (s *Server) handleListCorrupted(c *fiber.Ctx) error {
	// Parse pagination
	pagination := ParsePaginationFiber(c)

	// Get corrupted files using GetUnhealthyFiles
	cfg := s.configManager.GetConfig()
	strategy := string(cfg.Import.ImportStrategy)
	libraryDir := ""
	if cfg.Health.LibraryDir != nil {
		libraryDir = *cfg.Health.LibraryDir
	}
	items, err := s.healthRepo.GetUnhealthyFiles(c.Context(), pagination.Limit, strategy, libraryDir, cfg.GetMaxRetries())
	if err != nil {
		return RespondInternalError(c, "Failed to retrieve corrupted files", err.Error())
	}

	// Filter to only corrupted files (GetUnhealthyFiles returns all unhealthy)
	corruptedItems := make([]*database.FileHealth, 0)
	for _, item := range items {
		if item.Status == database.HealthStatusCorrupted {
			corruptedItems = append(corruptedItems, item)
		}
	}

	// Apply offset
	if pagination.Offset >= len(corruptedItems) {
		corruptedItems = []*database.FileHealth{}
	} else {
		end := min(pagination.Offset+pagination.Limit, len(corruptedItems))
		corruptedItems = corruptedItems[pagination.Offset:end]
	}

	// Convert to API response format
	response := make([]*HealthItemResponse, len(corruptedItems))
	for i, item := range corruptedItems {
		response[i] = ToHealthItemResponse(item)
		if s.healthWorker != nil && item.Status == database.HealthStatusChecking {
			response[i].CheckProgress = s.healthWorker.GetCheckProgress(item.FilePath)
		}
	}

	// Create metadata
	meta := &APIMeta{
		Count:  len(response),
		Limit:  pagination.Limit,
		Offset: pagination.Offset,
	}

	return RespondSuccessWithMeta(c, response, meta)
}

// handleGetHealthStats handles GET /api/health/stats
//
//	@Summary		Get health statistics
//	@Description	Returns counts of health records grouped by status.
//	@Tags			Health
//	@Produce		json
//	@Success		200	{object}	APIResponse{data=HealthStatsResponse}
//	@Failure		500	{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/health/stats [get]
func (s *Server) handleGetHealthStats(c *fiber.Ctx) error {
	stats, err := s.healthRepo.GetHealthStats(c.Context())
	if err != nil {
		return RespondInternalError(c, "Failed to retrieve health statistics", err.Error())
	}

	response := ToHealthStatsResponse(stats)
	return RespondSuccess(c, response)
}

// handleCleanupHealth handles DELETE /api/health/cleanup
//
//	@Summary		Cleanup health records
//	@Description	Removes old health records based on age, status, or both. Optionally deletes physical files.
//	@Tags			Health
//	@Accept			json
//	@Produce		json
//	@Param			body	body		HealthCleanupRequest	false	"Cleanup filter criteria"
//	@Success		200		{object}	APIResponse
//	@Failure		400		{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/health/cleanup [delete]
func (s *Server) handleCleanupHealth(c *fiber.Ctx) error {
	// Parse request body
	var req HealthCleanupRequest
	if len(c.Body()) > 0 {
		if err := c.BodyParser(&req); err != nil {
			return RespondBadRequest(c, "Invalid request body", err.Error())
		}
	}

	// Parse older_than parameter from query if not in body
	if req.OlderThan == nil {
		if olderThan, err := ParseTimeParamFiber(c, "older_than"); err != nil {
			return RespondValidationError(c, "Invalid older_than parameter", err.Error())
		} else if olderThan != nil {
			req.OlderThan = olderThan
		}
	}

	// Parse status parameter from query if not in body
	if req.Status == nil {
		if statusStr := c.Query("status"); statusStr != "" {
			statusStr = strings.TrimSpace(statusStr)
			status := database.HealthStatus(statusStr)
			switch status {
			case database.HealthStatusPending, database.HealthStatusChecking, database.HealthStatusCorrupted, database.HealthStatusRepairTriggered, database.HealthStatusHealthy, database.HealthStatusDegraded:
				req.Status = &status
			default:
				return RespondValidationError(c, fmt.Sprintf("Invalid status filter: '%s'", statusStr), "Valid values: pending, checking, corrupted, repair_triggered, healthy, degraded")
			}
		}
	}

	// Default to 7 days ago if not specified
	if req.OlderThan == nil {
		defaultTime := time.Now().Add(-7 * 24 * time.Hour)
		req.OlderThan = &defaultTime
	}

	// Perform cleanup with optional file deletion
	recordsDeleted, filesDeleted, deletionErrors, err := s.cleanupHealthRecords(c.Context(), *req.OlderThan, req.Status, req.DeleteFiles)
	if err != nil {
		return RespondInternalError(c, "Failed to cleanup health records", err.Error())
	}

	response := fiber.Map{
		"records_deleted": recordsDeleted,
		"older_than":      req.OlderThan.Format(time.RFC3339),
		"status_filter":   req.Status,
		"files_deleted":   filesDeleted,
	}

	// Include deletion errors if any occurred
	if len(deletionErrors) > 0 {
		response["file_deletion_errors"] = deletionErrors
		response["warning"] = fmt.Sprintf("%d file(s) could not be deleted", len(deletionErrors))
	}

	return RespondSuccess(c, response)
}

// cleanupHealthRecords is a helper method to cleanup health records
func (s *Server) cleanupHealthRecords(ctx context.Context, olderThan time.Time, statusFilter *database.HealthStatus, deleteFiles bool) (recordsDeleted int, filesDeleted int, deletionErrors []string, err error) {
	// If not deleting files, use direct SQL delete for efficiency (handles unlimited records)
	if !deleteFiles {
		count, deleteErr := s.healthRepo.DeleteHealthRecordsByDate(ctx, olderThan, statusFilter)
		if deleteErr != nil {
			return 0, 0, nil, fmt.Errorf("failed to delete health records: %w", deleteErr)
		}
		return count, 0, nil, nil
	}

	// If deleting files, need to fetch records in batches to get file paths
	const batchSize = 1000
	allFilePaths := make([]string, 0)
	deletedFileCount := 0
	fileErrors := make([]string, 0)
	offset := 0

	cfg := s.configManager.GetConfig()
	mountPath := cfg.MountPath

	// Process records in batches until no more records found
	for {
		// Fetch next batch of records
		items, queryErr := s.healthRepo.ListHealthItems(ctx, statusFilter, batchSize, offset, nil, "", "created_at", "asc")
		if queryErr != nil {
			return 0, 0, nil, fmt.Errorf("failed to query health records: %w", queryErr)
		}

		// No more records found
		if len(items) == 0 {
			break
		}

		// Filter items older than the specified date
		var oldItemsInBatch []*database.FileHealth
		for _, item := range items {
			if item.CreatedAt.Before(olderThan) {
				oldItemsInBatch = append(oldItemsInBatch, item)
			}
		}

		// If no items in this batch match the date criteria, we've processed all old records
		// (since results are sorted by created_at ascending)
		if len(oldItemsInBatch) == 0 {
			break
		}

		// Delete physical files and collect paths
		for _, item := range oldItemsInBatch {
			allFilePaths = append(allFilePaths, item.FilePath)

			// Determine path to delete
			var pathToDelete string
			if item.LibraryPath != nil && *item.LibraryPath != "" {
				pathToDelete = *item.LibraryPath
			} else {
				// Fallback to mount path
				pathToDelete = utils.JoinAbsPath(mountPath, item.FilePath)
			}

			// Attempt to delete the physical file using os.Remove
			if deleteErr := os.Remove(pathToDelete); deleteErr != nil {
				// Track error but continue with other files
				fileErrors = append(fileErrors, fmt.Sprintf("%s: %v", item.FilePath, deleteErr))
			} else {
				deletedFileCount++

				// Clean up empty parent directories
				var rootPath string
				if item.LibraryPath != nil && *item.LibraryPath != "" {
					// Use library directory as root if available
					if cfg.Health.LibraryDir != nil && *cfg.Health.LibraryDir != "" {
						rootPath = *cfg.Health.LibraryDir
					} else {
						// Fallback to the directory containing the file if root not known
						rootPath = filepath.Dir(filepath.Dir(pathToDelete))
					}
				} else {
					rootPath = mountPath
				}

				if rootPath != "" {
					utils.RemoveEmptyDirs(rootPath, filepath.Dir(pathToDelete))
				}
			}
		}

		// If we got fewer items than the batch size, we've reached the end
		if len(items) < batchSize {
			break
		}

		// If all items in batch were old, continue to next batch
		// If not all items were old, we're done (sorted by date)
		if len(oldItemsInBatch) < len(items) {
			break
		}

		offset += batchSize
	}

	// No records to cleanup
	if len(allFilePaths) == 0 {
		return 0, 0, nil, nil
	}

	// Delete database records (proceed even if some file deletions failed)
	deletedRecords, deleteErr := s.healthRepo.DeleteHealthRecordsBulk(ctx, allFilePaths)
	if deleteErr != nil {
		return 0, deletedFileCount, fileErrors, fmt.Errorf("failed to delete health records from database: %w", deleteErr)
	}

	return int(deletedRecords), deletedFileCount, fileErrors, nil
}

// handleAddHealthCheck handles POST /api/health/check
//
//	@Summary		Add file for health check
//	@Description	Adds a file to the health monitoring queue for checking.
//	@Tags			Health
//	@Accept			json
//	@Produce		json
//	@Param			body	body		HealthCheckRequest	true	"File to check"
//	@Success		201		{object}	APIResponse{data=HealthItemResponse}
//	@Failure		400		{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/health/check [post]
func (s *Server) handleAddHealthCheck(c *fiber.Ctx) error {
	// Parse request body
	var req HealthCheckRequest
	if err := c.BodyParser(&req); err != nil {
		return RespondBadRequest(c, "Invalid request body", err.Error())
	}

	// Validate required fields
	if req.FilePath == "" {
		return RespondValidationError(c, "file_path is required", "")
	}

	// Set default max retries if not specified
	cfg := s.configManager.GetConfigGetter()()
	maxRetries := cfg.GetMaxRetries()
	if req.MaxRetries != nil {
		if *req.MaxRetries < 0 {
			return RespondValidationError(c, "max_retries must be non-negative", "")
		}
		maxRetries = *req.MaxRetries
	}

	// Add file to health database
	err := s.healthRepo.AddFileToHealthCheck(c.Context(), req.FilePath, req.LibraryPath, maxRetries, cfg.GetMaxRepairRetries(), req.SourceNzb, req.Priority)
	if err != nil {
		return RespondInternalError(c, "Failed to add file for health check", err.Error())
	}

	// Return the health record
	item, err := s.healthRepo.GetFileHealth(c.Context(), req.FilePath)
	if err != nil {
		return RespondInternalError(c, "Failed to retrieve added health record", err.Error())
	}

	response := ToHealthItemResponse(item)
	return RespondSuccess(c, response)
}

// handleGetHealthWorkerStatus handles GET /api/health/worker/status
//
//	@Summary		Get health worker status
//	@Description	Returns the current status and statistics of the background health check worker.
//	@Tags			Health
//	@Produce		json
//	@Success		200	{object}	APIResponse{data=HealthWorkerStatusResponse}
//	@Security		BearerAuth
//	@Router			/health/worker/status [get]
func (s *Server) handleGetHealthWorkerStatus(c *fiber.Ctx) error {
	if s.healthWorker == nil {
		return RespondNotFound(c, "Health worker", "Health worker is not configured or not running")
	}

	stats := s.healthWorker.GetStats()
	response := HealthWorkerStatusResponse{
		Status:                 string(stats.Status),
		LastRunTime:            stats.LastRunTime,
		NextRunTime:            stats.NextRunTime,
		TotalRunsCompleted:     stats.TotalRunsCompleted,
		TotalFilesChecked:      stats.TotalFilesChecked,
		TotalFilesHealthy:      stats.TotalFilesHealthy,
		TotalFilesCorrupted:    stats.TotalFilesCorrupted,
		CurrentRunStartTime:    stats.CurrentRunStartTime,
		CurrentRunFilesChecked: stats.CurrentRunFilesChecked,
		LastError:              stats.LastError,
		ErrorCount:             stats.ErrorCount,
	}

	return RespondSuccess(c, response)
}

// handleDirectHealthCheck handles POST /api/health/{id}/check-now
//
//	@Summary		Trigger immediate health check
//	@Description	Triggers an immediate health check for a file, bypassing the queue.
//	@Tags			Health
//	@Produce		json
//	@Param			id	path		int	true	"Health record ID"
//	@Success		200	{object}	APIResponse{data=HealthItemResponse}
//	@Failure		400	{object}	APIResponse
//	@Failure		404	{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/health/{id}/check-now [post]
func (s *Server) handleDirectHealthCheck(c *fiber.Ctx) error {
	// Extract ID from path parameter
	idStr := c.Params("id")
	if idStr == "" {
		return RespondBadRequest(c, "Health record identifier is required", "")
	}

	// Parse as numeric ID
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return RespondBadRequest(c, "Invalid health record ID", "ID must be a valid integer")
	}

	// Check if health worker is available
	if s.healthWorker == nil {
		return RespondNotFound(c, "Health worker", "Health worker is not configured or not running")
	}

	// Check if item exists in health database
	item, err := s.healthRepo.GetFileHealthByID(c.Context(), id)
	if err != nil {
		return RespondInternalError(c, "Failed to check health record", err.Error())
	}

	if item == nil {
		return RespondNotFound(c, "Health record", "")
	}

	// Prevent starting multiple checks for the same file
	if item.Status == database.HealthStatusChecking {
		return RespondConflict(c, "Health check already in progress", "This file is currently being checked")
	}

	// Optional body: {"verify_content": true|false} forces content
	// verification on or off for this manual check, overriding the
	// configured default and the Pending-only automatic gate. Parsed and
	// validated BEFORE the row is transitioned to 'checking' below: rejecting
	// a malformed body after that transition would leave the record stuck.
	// An absent/empty body is a legitimate "no override" request and must
	// stay tolerated.
	var body struct {
		VerifyContent *bool `json:"verify_content"`
	}
	if len(c.Body()) > 0 {
		if err := c.BodyParser(&body); err != nil {
			return RespondBadRequest(c, "Invalid request body", err.Error())
		}
	}

	// preCheckStatus is the file's HealthStatus before this transition to
	// 'checking'; it must be threaded into PerformBackgroundCheck rather than
	// re-read afterward, since re-reading here would always observe 'checking'
	// and make the automatic Pending-only content-verification gate unreachable.
	preCheckStatus := item.Status

	// Immediately set status to 'checking' using ID
	err = s.healthRepo.SetFileCheckingByID(c.Context(), id)
	if err != nil {
		return RespondInternalError(c, "Failed to set checking status", err.Error())
	}

	// Start health check in background using worker (still needs file path)
	err = s.healthWorker.PerformBackgroundCheck(context.Background(), item.FilePath, preCheckStatus, body.VerifyContent)
	if err != nil {
		return RespondInternalError(c, "Failed to start background health check", err.Error())
	}

	// Verify that the file still exists
	f, err := s.metadataReader.GetFileMetadata(item.FilePath)
	if err != nil {
		return RespondInternalError(c, "Failed to retrieve file metadata", err.Error())
	}
	if f == nil {
		return RespondNotFound(c, "File metadata", "File metadata no longer exists on disk")
	}

	// Get the updated health record with 'checking' status
	updatedItem, err := s.healthRepo.GetFileHealthByID(c.Context(), id)
	if err != nil {
		return RespondInternalError(c, "Failed to retrieve updated health record", err.Error())
	}

	return RespondSuccess(c, fiber.Map{
		"message":     "Health check started",
		"id":          id,
		"file_path":   item.FilePath,
		"old_status":  string(item.Status),
		"new_status":  string(updatedItem.Status),
		"checked_at":  updatedItem.LastChecked,
		"health_data": ToHealthItemResponse(updatedItem),
	})
}

// handleRestartHealthChecksBulk handles POST /api/health/bulk/restart
//
//	@Summary		Bulk restart health checks
//	@Description	Resets multiple health records to pending status for re-checking.
//	@Tags			Health
//	@Accept			json
//	@Produce		json
//	@Param			body	body		object{ids=[]int}	true	"List of health record IDs"
//	@Success		200		{object}	APIResponse
//	@Failure		400		{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/health/bulk/restart [post]
func (s *Server) handleRestartHealthChecksBulk(c *fiber.Ctx) error {
	// Parse request body
	var req struct {
		FilePaths []string `json:"file_paths"`
	}

	if err := c.BodyParser(&req); err != nil {
		return RespondBadRequest(c, "Invalid request body", err.Error())
	}

	// Validate file paths
	if len(req.FilePaths) == 0 {
		return RespondValidationError(c, "At least one file path is required", "")
	}

	// Cancel any active checks for these files
	if s.healthWorker != nil {
		for _, filePath := range req.FilePaths {
			// Check if there's an active check to cancel
			if s.healthWorker.IsCheckActive(filePath) {
				// Cancel the health check
				_ = s.healthWorker.CancelHealthCheck(c.Context(), filePath) // Ignore error, proceed with restart
			}
		}
	}

	// Reset all items to pending status using bulk method
	restartedCount, err := s.healthRepo.ResetHealthChecksBulk(c.Context(), req.FilePaths)
	if err != nil {
		return RespondInternalError(c, "Failed to restart health checks", err.Error())
	}

	if restartedCount == 0 {
		return RespondNotFound(c, "Health records", "No health records found to restart")
	}

	response := map[string]any{
		"message":         "Health checks restarted successfully",
		"restarted_count": restartedCount,
		"file_paths":      req.FilePaths,
		"restarted_at":    time.Now().Format(time.RFC3339),
	}

	return RespondSuccess(c, response)
}

// handleCancelHealthCheck handles POST /api/health/{id}/cancel
//
//	@Summary		Cancel health check
//	@Description	Cancels an in-progress health check for a file.
//	@Tags			Health
//	@Produce		json
//	@Param			id	path		int	true	"Health record ID"
//	@Success		200	{object}	APIResponse
//	@Failure		400	{object}	APIResponse
//	@Failure		404	{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/health/{id}/cancel [post]
func (s *Server) handleCancelHealthCheck(c *fiber.Ctx) error {
	// Extract ID from path parameter
	idStr := c.Params("id")
	if idStr == "" {
		return RespondBadRequest(c, "Health record identifier is required", "")
	}

	// Parse as numeric ID
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return RespondBadRequest(c, "Invalid health record ID", "ID must be a valid integer")
	}

	// Check if health worker is available
	if s.healthWorker == nil {
		return RespondNotFound(c, "Health worker", "Health worker is not configured or not running")
	}

	// Check if item exists in health database
	item, err := s.healthRepo.GetFileHealthByID(c.Context(), id)
	if err != nil {
		return RespondInternalError(c, "Failed to check health record", err.Error())
	}

	if item == nil {
		return RespondNotFound(c, "Health record", "")
	}

	// Check if there's actually an active check to cancel (still needs file path)
	if !s.healthWorker.IsCheckActive(item.FilePath) {
		return RespondConflict(c, "No active health check found", "There is no active health check for this file")
	}

	// Cancel the health check (still needs file path)
	err = s.healthWorker.CancelHealthCheck(c.Context(), item.FilePath)
	if err != nil {
		return RespondInternalError(c, "Failed to cancel health check", err.Error())
	}

	// Get the updated health record
	updatedItem, err := s.healthRepo.GetFileHealthByID(c.Context(), id)
	if err != nil {
		return RespondInternalError(c, "Failed to retrieve updated health record", err.Error())
	}

	response := map[string]any{
		"message":      "Health check cancelled",
		"id":           id,
		"file_path":    item.FilePath,
		"old_status":   string(item.Status),
		"new_status":   string(updatedItem.Status),
		"cancelled_at": time.Now().Format(time.RFC3339),
		"health_data":  ToHealthItemResponse(updatedItem),
	}

	return RespondSuccess(c, response)
}

// handleUnmaskHealth handles POST /api/health/{id}/unmask
//
//	@Summary		Unmask health record
//	@Description	Clears the streaming-failure mask on a health record so it can be checked again.
//	@Tags			Health
//	@Produce		json
//	@Param			id	path		int	true	"Health record ID"
//	@Success		200	{object}	APIResponse{data=HealthItemResponse}
//	@Failure		400	{object}	APIResponse
//	@Failure		404	{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/health/{id}/unmask [post]
func (s *Server) handleUnmaskHealth(c *fiber.Ctx) error {
	// Extract ID from path parameter
	idStr := c.Params("id")
	if idStr == "" {
		return RespondBadRequest(c, "Health record identifier is required", "")
	}

	// Parse as numeric ID
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return RespondBadRequest(c, "Invalid health record ID", "ID must be a valid integer")
	}

	// Check if item exists in health database
	item, err := s.healthRepo.GetFileHealthByID(c.Context(), id)
	if err != nil {
		return RespondInternalError(c, "Failed to check health record", err.Error())
	}

	if item == nil {
		return RespondNotFound(c, "Health record", "")
	}

	// Unmask file
	err = s.healthRepo.UnmaskFile(c.Context(), item.FilePath)
	if err != nil {
		return RespondInternalError(c, "Failed to unmask file", err.Error())
	}

	// Get the updated health record
	updatedItem, err := s.healthRepo.GetFileHealthByID(c.Context(), id)
	if err != nil {
		return RespondInternalError(c, "Failed to retrieve updated health record", err.Error())
	}

	response := map[string]any{
		"message":     "File unmasked successfully",
		"id":          id,
		"file_path":   item.FilePath,
		"updated_at":  time.Now().Format(time.RFC3339),
		"health_data": ToHealthItemResponse(updatedItem),
	}

	return RespondSuccess(c, response)
}

// handleSetHealthPriority handles POST /api/health/{id}/priority
//
//	@Summary		Set health check priority
//	@Description	Sets the checking priority for a health record.
//	@Tags			Health
//	@Accept			json
//	@Produce		json
//	@Param			id		path		int							true	"Health record ID"
//	@Param			body	body		object{priority=string}		true	"Priority: normal or high"
//	@Success		200		{object}	APIResponse{data=HealthItemResponse}
//	@Failure		400		{object}	APIResponse
//	@Failure		404		{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/health/{id}/priority [post]
func (s *Server) handleSetHealthPriority(c *fiber.Ctx) error {
	// Extract ID from path parameter
	idStr := c.Params("id")
	if idStr == "" {
		return RespondBadRequest(c, "Health record identifier is required", "")
	}

	// Parse as numeric ID
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return RespondBadRequest(c, "Invalid health record ID", "ID must be a valid integer")
	}

	// Parse request body
	var req struct {
		Priority database.HealthPriority `json:"priority"`
	}

	if err := c.BodyParser(&req); err != nil {
		return RespondBadRequest(c, "Invalid request body", err.Error())
	}

	// Check if item exists in health database
	item, err := s.healthRepo.GetFileHealthByID(c.Context(), id)
	if err != nil {
		return RespondInternalError(c, "Failed to check health record", err.Error())
	}

	if item == nil {
		return RespondNotFound(c, "Health record", "")
	}

	// Set priority
	err = s.healthRepo.SetPriority(c.Context(), id, req.Priority)
	if err != nil {
		return RespondInternalError(c, "Failed to update priority", err.Error())
	}

	// Get the updated health record
	updatedItem, err := s.healthRepo.GetFileHealthByID(c.Context(), id)
	if err != nil {
		return RespondInternalError(c, "Failed to retrieve updated health record", err.Error())
	}

	response := map[string]any{
		"message":     "Health priority updated",
		"id":          id,
		"file_path":   item.FilePath,
		"priority":    updatedItem.Priority,
		"updated_at":  time.Now().Format(time.RFC3339),
		"health_data": ToHealthItemResponse(updatedItem),
	}

	return RespondSuccess(c, response)
}

// handleResetAllHealthChecks handles POST /api/health/reset-all
//
//	@Summary		Reset all health checks
//	@Description	Resets all health records to pending status for a full re-check cycle.
//	@Tags			Health
//	@Produce		json
//	@Success		200	{object}	APIResponse
//	@Failure		500	{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/health/reset-all [post]
func (s *Server) handleResetAllHealthChecks(c *fiber.Ctx) error {
	// Reset all items to pending status using repository method
	restartedCount, err := s.healthRepo.ResetAllHealthChecks(c.Context())
	if err != nil {
		return RespondInternalError(c, "Failed to reset all health checks", err.Error())
	}

	response := map[string]any{
		"message":         "All health checks reset successfully",
		"restarted_count": restartedCount,
		"restarted_at":    time.Now().Format(time.RFC3339),
	}

	return RespondSuccess(c, response)
}

// handleRegenerateLibraryFiles handles the POST /health/regenerate-symlinks request.
// It supports global, bulk (via file_paths), or single item regeneration.
func (s *Server) handleRegenerateLibraryFiles(c *fiber.Ctx) error {
	ctx := c.Context()
	cfg := s.configManager.GetConfig()

	// 1. Parse optional bulk request body
	var req struct {
		FilePaths     []string `json:"file_paths"`
		UseImportPath bool     `json:"use_import_path"`
	}
	_ = c.BodyParser(&req)

	var files []*database.FileHealth
	var err error

	// 2. Determine which files to process
	if len(req.FilePaths) > 0 {
		// Bulk/Single item mode
		files, err = s.healthRepo.GetFilesByPaths(ctx, req.FilePaths)
	} else {
		// Global mode: Get all records for verification
		files, err = s.healthRepo.GetFilesForLibrarySync(ctx)
	}

	if err != nil {
		return RespondInternalError(c, "Failed to retrieve health records", err.Error())
	}

	trigger := "health-page"
	if req.UseImportPath {
		trigger = "queue-or-file-explorer"
	}

	slog.InfoContext(ctx, "Regenerating library files",
		"trigger", trigger,
		"use_import_path", req.UseImportPath,
		"file_count", len(files),
		"strategy", string(cfg.Import.ImportStrategy),
	)

	if len(files) == 0 {
		return RespondSuccess(c, fiber.Map{
			"message":         "No library files found to process",
			"files_processed": 0,
			"success_count":   0,
			"errors":          []string{},
			"completed_at":    time.Now().Format(time.RFC3339),
		})
	}

	successCount := 0
	errorCount := 0
	errors := make([]string, 0)

	for _, file := range files {
		// Build the library file path in the import directory
		var libraryPath string

		// SMART LOGIC: If we already have a valid library path and caller did not
		// request the import path (queue / file-explorer callers set UseImportPath=true),
		// preserve the stored path exactly as is. This respects Sonarr's final renaming.
		preserveExistingPath := false
		if !req.UseImportPath && file.LibraryPath != nil && *file.LibraryPath != "" {
			preserveExistingPath = true
			libraryPath = *file.LibraryPath
			slog.InfoContext(ctx, "Regenerating at existing library path",
				"file_path", file.FilePath,
				"library_path", libraryPath,
			)
		}

		if !preserveExistingPath {
			// 1. Get the internal relative path (relative to FUSE mount)
			relPath := strings.TrimPrefix(file.FilePath, "/")

			// 2. Strip CompleteDir prefix
			if cfg.SABnzbd.CompleteDir != "" {
				completeDir := strings.Trim(filepath.ToSlash(cfg.SABnzbd.CompleteDir), "/")
				if after, ok := strings.CutPrefix(relPath, completeDir+"/"); ok {
					relPath = after
				} else if relPath == completeDir {
					relPath = ""
				}
			}

			// 3. Extract category dynamically as the first path component after CompleteDir.
			// This handles any category name (sonarr, radarr, tv, movies, etc.).
			category := config.DefaultCategoryName
			if idx := strings.Index(relPath, "/"); idx > 0 {
				category = relPath[:idx]
				relPath = relPath[idx+1:]
			}

			// 3. Build the clean, isolated library path
			var baseDir string
			useCompleteDir := true

			if cfg.Import.ImportStrategy == config.ImportStrategyNone {
				baseDir = cfg.MountPath
				useCompleteDir = false
			} else if cfg.Import.ImportDir != nil && *cfg.Import.ImportDir != "" {
				baseDir = *cfg.Import.ImportDir
			} else {
				baseDir = cfg.MountPath
			}

			// Construct path based on base directory
			pathParts := []string{baseDir}
			if useCompleteDir && cfg.SABnzbd.CompleteDir != "" {
				pathParts = append(pathParts, strings.Trim(cfg.SABnzbd.CompleteDir, "/"))
			}
			pathParts = append(pathParts, category)
			pathParts = append(pathParts, relPath)

			fullPathStr := filepath.Join(pathParts...)
			if cfg.Import.ImportStrategy == config.ImportStrategySYMLINK {
				libraryPath = filepath.ToSlash(filepath.Clean(fullPathStr))
			} else {
				// STRM files have the .strm extension
				libraryPath = filepath.ToSlash(filepath.Clean(fullPathStr + ".strm"))
			}

			slog.InfoContext(ctx, "Regenerating at computed import path",
				"file_path", file.FilePath,
				"library_path", libraryPath,
				"base_dir", baseDir,
				"category", category,
			)
		}

		// Build the actual file path in the mount
		actualPath := utils.JoinAbsPath(cfg.MountPath, file.FilePath)

		// Create directory if needed
		baseDir := filepath.Dir(libraryPath)
		if err := os.MkdirAll(baseDir, 0755); err != nil {
			errorCount++
			errors = append(errors, fmt.Sprintf("%s: failed to create directory: %v", file.FilePath, err))
			continue
		}

		// Remove existing file if present
		if _, err := os.Lstat(libraryPath); err == nil {
			_ = os.Remove(libraryPath)
		}

		// Create the file based on strategy
		var creationErr error
		if cfg.Import.ImportStrategy == config.ImportStrategySTRM {
			if s.importerService != nil && s.importerService.GetPostProcessor() != nil {
				creationErr = s.importerService.GetPostProcessor().CreateSingleStrmFile(ctx, libraryPath, file.FilePath, cfg.WebDAV.Port)
			} else {
				creationErr = fmt.Errorf("importer service or post-processor not available")
			}
		} else {
			creationErr = os.Symlink(actualPath, libraryPath)
		}

		if creationErr != nil {
			errorCount++
			errors = append(errors, fmt.Sprintf("%s: failed to recreate library file: %v", file.FilePath, creationErr))
			slog.ErrorContext(ctx, "Failed to recreate library file",
				"file_path", file.FilePath,
				"library_path", libraryPath,
				"error", creationErr,
			)
			continue
		}

		slog.InfoContext(ctx, "Library file recreated",
			"file_path", file.FilePath,
			"library_path", libraryPath,
			"actual_path", actualPath,
			"strategy", string(cfg.Import.ImportStrategy),
		)

		// Update the library path in the database
		if err := s.healthRepo.UpdateLibraryPath(ctx, file.FilePath, libraryPath); err != nil {
			slog.ErrorContext(ctx, "Failed to update library path in database",
				"file_path", file.FilePath,
				"library_path", libraryPath,
				"error", err)
		}

		successCount++
	}

	slog.InfoContext(ctx, "Library file regeneration complete",
		"trigger", trigger,
		"files_processed", len(files),
		"success_count", successCount,
		"error_count", errorCount,
	)

	response := fiber.Map{
		"message":         fmt.Sprintf("Successfully processed %d library files", successCount),
		"files_processed": len(files),
		"success_count":   successCount,
		"errors":          errors,
		"error_count":     errorCount,
		"completed_at":    time.Now().Format(time.RFC3339),
	}

	if errorCount > 0 {
		response["warning"] = fmt.Sprintf("%d file(s) failed to regenerate library files", errorCount)
	}

	return RespondSuccess(c, response)
}

// deleteLibraryFile removes the library file (symlink) for a health record and cleans up empty parent directories.
// Returns true if the file was successfully deleted.
func deleteLibraryFile(ctx context.Context, cfg *config.Config, item *database.FileHealth) bool {
	var pathToDelete string
	if item.LibraryPath != nil && *item.LibraryPath != "" {
		pathToDelete = *item.LibraryPath
	} else {
		pathToDelete = utils.JoinAbsPath(cfg.MountPath, item.FilePath)
	}

	if err := os.Remove(pathToDelete); err != nil {
		slog.ErrorContext(ctx, "Failed to delete library file", "path", pathToDelete, "error", err)
		return false
	}

	// Clean up empty parent directories
	var rootPath string
	if item.LibraryPath != nil && *item.LibraryPath != "" {
		if cfg.Health.LibraryDir != nil && *cfg.Health.LibraryDir != "" {
			rootPath = *cfg.Health.LibraryDir
		} else {
			rootPath = filepath.Dir(filepath.Dir(pathToDelete))
		}
	} else {
		rootPath = cfg.MountPath
	}
	if rootPath != "" {
		utils.RemoveEmptyDirs(rootPath, filepath.Dir(pathToDelete))
	}

	return true
}
