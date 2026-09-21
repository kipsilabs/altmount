package api

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/kipsilabs/altmount/internal/auth"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/slogutil"
	"github.com/javi11/nntppool/v5"
)

// ConfigManager interface defines methods for configuration management
type ConfigManager interface {
	GetConfig() *config.Config
	GetConfigGetter() config.ConfigGetter
	UpdateConfig(config *config.Config) error
	ValidateConfig(config *config.Config) error
	ValidateConfigUpdate(config *config.Config) error
	OnConfigChange(callback config.ChangeCallback)
	ReloadConfig() error
	SaveConfig() error
	NeedsLibrarySync() bool
	GetPreviousMountPath() string
	ClearLibrarySyncFlag()
}

// parseLogLevel converts string log level to slog.Level
func parseLogLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// getEffectiveLogLevel returns the effective log level, preferring new config over legacy
func getEffectiveLogLevel(newLevel, legacyLevel string) string {
	if newLevel != "" {
		return newLevel
	}
	if legacyLevel != "" {
		return legacyLevel
	}
	return "info"
}

// RegisterLogLevelHandler registers handler for log level configuration changes
func RegisterLogLevelHandler(ctx context.Context, configManager *config.Manager, debugMode *bool, dynamicLeveler *slogutil.DynamicLeveler) {
	configManager.OnConfigChange(func(oldConfig, newConfig *config.Config) {
		// Determine old and new log levels
		oldLevel := getEffectiveLogLevel(oldConfig.Log.Level, oldConfig.Log.Level)
		newLevel := getEffectiveLogLevel(newConfig.Log.Level, newConfig.Log.Level)

		// Apply log level change if it changed
		if oldLevel != newLevel {
			dynamicLeveler.SetLevel(parseLogLevel(newLevel))
			// Update Fiber logger debug mode
			*debugMode = newLevel == "debug"
			slog.InfoContext(ctx, "Log level updated dynamically",
				"old_level", oldLevel,
				"new_level", newLevel,
				"fiber_logging", *debugMode)
		}
	})
}

// handleGetConfig returns the current configuration
//
//	@Summary		Get configuration
//	@Description	Returns the current AltMount configuration with sensitive values masked.
//	@Tags			Config
//	@Produce		json
//	@Success		200	{object}	APIResponse{data=ConfigAPIResponse}
//	@Failure		503	{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/config [get]
func (s *Server) handleGetConfig(c *fiber.Ctx) error {
	if s.configManager == nil {
		return RespondServiceUnavailable(c, "Configuration management not available", "CONFIG_UNAVAILABLE")
	}

	config := s.configManager.GetConfig()
	if config == nil {
		return RespondInternalError(c, "Configuration not available", "CONFIG_NOT_FOUND")
	}

	// Get API key from authenticated user or first admin user
	apiKey := s.getAPIKeyForConfig(c)

	response := ToConfigAPIResponse(config, apiKey)
	return RespondSuccess(c, response)
}

// handleUpdateConfig updates the entire configuration
//
//	@Summary		Update configuration
//	@Description	Replaces the entire AltMount configuration. Triggers restart if required.
//	@Tags			Config
//	@Accept			json
//	@Produce		json
//	@Param			body	body		object{}	true	"Complete configuration object"
//	@Success		200		{object}	APIResponse
//	@Failure		400		{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/config [put]
func (s *Server) handleUpdateConfig(c *fiber.Ctx) error {
	if s.configManager == nil {
		return RespondServiceUnavailable(c, "Configuration management not available", "CONFIG_UNAVAILABLE")
	}

	// Get current config to use as base for defaults/missing fields
	currentConfig := s.configManager.GetConfig()
	newConfig := currentConfig.DeepCopy()

	// Decode directly into config structure
	if err := c.BodyParser(newConfig); err != nil {
		return RespondValidationError(c, "Invalid JSON in request body", err.Error())
	}

	// API responses intentionally mask provider credentials. Preserve the
	// stored value when a full-config update sends an empty masked field; a
	// separate credential-rotation operation can still replace it explicitly.
	preserveMaskedStremioCredentials(currentConfig, newConfig)

	slog.DebugContext(c.Context(), "Updating configuration")

	// Validate the new configuration with API restrictions
	if err := s.configManager.ValidateConfigUpdate(newConfig); err != nil {
		return RespondValidationError(c, "Configuration validation failed", err.Error())
	}

	// Update the configuration
	if err := s.configManager.UpdateConfig(newConfig); err != nil {
		return RespondInternalError(c, "Failed to update configuration", err.Error())
	}

	// Ensure SABnzbd category directories exist
	if err := s.ensureSABnzbdCategoryDirectories(newConfig); err != nil {
		// Log the error but don't fail the update
		slog.WarnContext(c.Context(), "Failed to create SABnzbd category directories", "error", err)
	}

	// Save to file
	if err := s.configManager.SaveConfig(); err != nil {
		return RespondInternalError(c, "Failed to save configuration", err.Error())
	}

	// Try to start RC server if RClone is enabled but RC is not running
	s.startRCServerIfNeeded(c.Context())

	// Get API key for response
	apiKey := s.getAPIKeyForConfig(c)

	response := ToConfigAPIResponse(newConfig, apiKey)
	return RespondSuccess(c, response)
}

func preserveMaskedStremioCredentials(current, updated *config.Config) {
	if current == nil || updated == nil {
		return
	}
	if updated.Stremio.Prowlarr.APIKey == "" {
		updated.Stremio.Prowlarr.APIKey = current.Stremio.Prowlarr.APIKey
	}
	if updated.Stremio.Indexers.Prowlarr.APIKey == "" {
		updated.Stremio.Indexers.Prowlarr.APIKey = current.Stremio.Indexers.Prowlarr.APIKey
	}
	for i := range updated.Stremio.Indexers.Newsnab {
		for _, existing := range current.Stremio.Indexers.Newsnab {
			if existing.ID == updated.Stremio.Indexers.Newsnab[i].ID && updated.Stremio.Indexers.Newsnab[i].APIKey == "" {
				updated.Stremio.Indexers.Newsnab[i].APIKey = existing.APIKey
			}
		}
	}
}

// handlePatchConfigSection updates a specific configuration section
//
//	@Summary		Patch configuration section
//	@Description	Updates a specific named section of the configuration (e.g. import, sabnzbd, rclone).
//	@Tags			Config
//	@Accept			json
//	@Produce		json
//	@Param			section	path		string		true	"Config section name"
//	@Param			body	body		object{}	true	"Section configuration"
//	@Success		200		{object}	APIResponse
//	@Failure		400		{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/config/{section} [patch]
func (s *Server) handlePatchConfigSection(c *fiber.Ctx) error {
	if s.configManager == nil {
		return RespondServiceUnavailable(c, "Configuration management not available", "CONFIG_UNAVAILABLE")
	}

	// Extract section from URL path parameter
	section := c.Params("section")
	if section == "" {
		return RespondValidationError(c, "Invalid configuration section path", "INVALID_PATH")
	}

	// Get current config to merge with updates
	currentConfig := s.configManager.GetConfig()
	if currentConfig == nil {
		return RespondInternalError(c, "Configuration not available", "CONFIG_NOT_FOUND")
	}

	// Create a copy to apply updates to
	newConfig := currentConfig.DeepCopy()

	// Decode into the specific section based on the URL parameter
	var err error
	switch section {
	case "providers":
		err = c.BodyParser(newConfig)
		if err == nil {
			// encoding/json updates slice elements in-place, preserving json:"-" fields
			// (Password) from their original positions. When the order changes this mixes
			// passwords across providers. Re-map each password to its correct provider by ID.
			oldPwdByID := make(map[string]string, len(currentConfig.Providers))
			for _, p := range currentConfig.Providers {
				oldPwdByID[p.ID] = p.Password
			}
			for i := range newConfig.Providers {
				newConfig.Providers[i].Password = oldPwdByID[newConfig.Providers[i].ID]
			}
		}
	case "webdav", "api", "auth", "database", "metadata", "streaming", "health", "rclone", "import", "log", "sabnzbd", "arrs", "fuse", "segment_cache", "system", "mount_path", "mount", "stremio", "network", "par2_repair":
		err = c.BodyParser(newConfig)
		// BodyParser will map fields like "profiler_enabled" from JSON to the root of newConfig
		// because Config struct has it with `json:"profiler_enabled"`.
		// Preserve existing rc_pass when the request omits or sends an empty value.
		// The frontend sends rc_pass: "" when the user hasn't entered a new password,
		// so an empty value means "keep the existing password", not "clear it".
		if err == nil && newConfig.RClone.RCPass == "" {
			newConfig.RClone.RCPass = currentConfig.RClone.RCPass
		}
		// Preserve existing WebDAV password when the request omits or sends an empty value.
		// The frontend sends password: "" when the user hasn't entered a new password.
		if err == nil && newConfig.WebDAV.Password == "" {
			newConfig.WebDAV.Password = currentConfig.WebDAV.Password
		}
		// Preserve existing Prowlarr and Newsnab credentials when patching the Stremio section.
		if err == nil && section == "stremio" {
			preserveMaskedStremioCredentials(currentConfig, newConfig)
		}
	default:
		return RespondValidationError(c, fmt.Sprintf("Unknown configuration section: %s", section), "INVALID_SECTION")
	}

	if err != nil {
		return RespondValidationError(c, "Invalid JSON in request body", err.Error())
	}

	slog.DebugContext(c.Context(), "Patching configuration section",
		"section", section)

	// Validate the new configuration with API restrictions
	if err := s.configManager.ValidateConfigUpdate(newConfig); err != nil {
		return RespondValidationError(c, "Configuration validation failed", err.Error())
	}

	// Update the configuration
	if err := s.configManager.UpdateConfig(newConfig); err != nil {
		return RespondInternalError(c, "Failed to update configuration", err.Error())
	}

	// Ensure SABnzbd category directories exist if SABnzbd section was updated
	if section == "sabnzbd" || section == "" {
		if err := s.ensureSABnzbdCategoryDirectories(newConfig); err != nil {
			// Log the error but don't fail the update
			slog.WarnContext(c.Context(), "Failed to create SABnzbd category directories", "error", err)
		}
	}

	// Save to file
	if err := s.configManager.SaveConfig(); err != nil {
		return RespondInternalError(c, "Failed to save configuration", err.Error())
	}

	// Try to start RC server if RClone/mount section was updated or full config update
	if section == "rclone" || section == "mount" || section == "" {
		s.startRCServerIfNeeded(c.Context())
	}

	// Get API key for response
	apiKey := s.getAPIKeyForConfig(c)

	response := ToConfigAPIResponse(newConfig, apiKey)
	return RespondSuccess(c, response)
}

// handleReloadConfig reloads configuration from file
//
//	@Summary		Reload configuration
//	@Description	Reloads the AltMount configuration from disk without restarting.
//	@Tags			Config
//	@Produce		json
//	@Success		200	{object}	APIResponse
//	@Failure		500	{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/config/reload [post]
func (s *Server) handleReloadConfig(c *fiber.Ctx) error {
	if s.configManager == nil {
		return RespondServiceUnavailable(c, "Configuration management not available", "CONFIG_UNAVAILABLE")
	}

	if err := s.configManager.ReloadConfig(); err != nil {
		return RespondInternalError(c, "Failed to reload configuration", err.Error())
	}

	config := s.configManager.GetConfig()

	// Get API key for response
	apiKey := s.getAPIKeyForConfig(c)

	response := ToConfigAPIResponse(config, apiKey)
	return RespondSuccess(c, response)
}

// handleValidateConfig validates configuration without applying changes
//
//	@Summary		Validate configuration
//	@Description	Validates a configuration object without saving it.
//	@Tags			Config
//	@Accept			json
//	@Produce		json
//	@Param			body	body		object{}	true	"Configuration to validate"
//	@Success		200		{object}	APIResponse
//	@Failure		400		{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/config/validate [post]
func (s *Server) handleValidateConfig(c *fiber.Ctx) error {
	if s.configManager == nil {
		return RespondServiceUnavailable(c, "Configuration management not available", "CONFIG_UNAVAILABLE")
	}

	// Decode directly into config structure
	var cfg config.Config
	if err := c.BodyParser(&cfg); err != nil {
		return RespondValidationError(c, "Invalid JSON in request body", err.Error())
	}

	// Validate the configuration
	validationErr := s.configManager.ValidateConfig(&cfg)

	response := struct {
		Valid  bool `json:"valid"`
		Errors []struct {
			Field   string `json:"field"`
			Message string `json:"message"`
		} `json:"errors,omitempty"`
	}{
		Valid: validationErr == nil,
	}

	if validationErr != nil {
		response.Errors = []struct {
			Field   string `json:"field"`
			Message string `json:"message"`
		}{
			{
				Field:   "config",
				Message: validationErr.Error(),
			},
		}
	}

	return RespondSuccess(c, response)
}

// Provider Management Handlers

// handleTestProvider tests NNTP provider connectivity
//
//	@Summary		Test NNTP provider
//	@Description	Tests NNTP provider connectivity with given credentials, returning RTT on success.
//	@Tags			Providers
//	@Accept			json
//	@Produce		json
//	@Param			body	body		ProviderTestRequest		true	"Provider credentials to test"
//	@Success		200		{object}	APIResponse{data=TestProviderResponse}
//	@Failure		400		{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/providers/test [post]
func (s *Server) handleTestProvider(c *fiber.Ctx) error {
	if s.configManager == nil {
		return RespondServiceUnavailable(c, "Configuration management not available", "CONFIG_UNAVAILABLE")
	}

	// Decode test request
	var testReq struct {
		ProviderID  string `json:"provider_id"`
		Host        string `json:"host"`
		Port        int    `json:"port"`
		Username    string `json:"username"`
		Password    string `json:"password"`
		TLS         bool   `json:"tls"`
		InsecureTLS bool   `json:"insecure_tls"`
		ProxyURL    string `json:"proxy_url"`
		SkipPing    bool   `json:"skip_ping"`
	}

	if err := c.BodyParser(&testReq); err != nil {
		return RespondValidationError(c, "Invalid JSON in request body", err.Error())
	}

	// Basic validation
	if testReq.Host == "" {
		return RespondValidationError(c, "Host is required", "MISSING_HOST")
	}
	if testReq.Port <= 0 || testReq.Port > 65535 {
		return RespondValidationError(c, "Valid port is required (1-65535)", "INVALID_PORT")
	}

	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
	defer cancel()

	host := fmt.Sprintf("%s:%d", testReq.Host, testReq.Port)
	providerName := testReq.ProviderID
	if providerName == "" {
		// Ad-hoc tests have no persisted provider ID. Keep the endpoint context
		// while preventing nntppool from deriving a name from Auth.Username.
		providerName = host
	}
	var tlsCfg *tls.Config
	if testReq.TLS {
		tlsCfg = &tls.Config{
			InsecureSkipVerify: testReq.InsecureTLS,
			ServerName:         testReq.Host,
		}
	}

	result := nntppool.TestProvider(ctx, nntppool.Provider{
		Host:      host,
		Name:      providerName,
		TLSConfig: tlsCfg,
		Auth:      nntppool.Auth{Username: testReq.Username, Password: testReq.Password},
		SkipPing:  testReq.SkipPing,
	})

	if result.Err != nil {
		return RespondSuccess(c, TestProviderResponse{
			Success:      false,
			ErrorMessage: result.Err.Error(),
		})
	}

	// If test is successful and we have a provider ID, update the config with RTT
	rtt := result.RTT.Milliseconds()
	if testReq.ProviderID != "" {
		currentConfig := s.configManager.GetConfig()
		newConfig := currentConfig.DeepCopy()
		updated := false
		for i, p := range newConfig.Providers {
			if p.ID == testReq.ProviderID {
				newConfig.Providers[i].LastRTTMs = rtt
				updated = true
				slog.DebugContext(c.Context(), "Updating provider latency",
					"provider_id", testReq.ProviderID,
					"rtt_ms", rtt)
				break
			}
		}

		if updated {
			if err := s.configManager.UpdateConfig(newConfig); err == nil {
				if err := s.configManager.SaveConfig(); err != nil {
					slog.WarnContext(c.Context(), "Failed to save config after updating RTT", "error", err)
				}
			}
		}
	}

	return RespondSuccess(c, TestProviderResponse{
		Success: true,
		RTTMs:   rtt,
	})
}

// handleCreateProvider creates a new NNTP provider
//
//	@Summary		Create NNTP provider
//	@Description	Adds a new NNTP provider to the configuration.
//	@Tags			Providers
//	@Accept			json
//	@Produce		json
//	@Param			body	body		ProviderCreateRequest	true	"Provider details"
//	@Success		201		{object}	APIResponse{data=ProviderAPIResponse}
//	@Failure		400		{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/providers [post]
// nextProviderID picks the lowest-numbered "provider_N" id not already used
// by an existing provider. Providers can be deleted, so the next free index
// is not simply len(existing)+1 — that can collide with a surviving provider.
func nextProviderID(existing []config.ProviderConfig) string {
	used := make(map[string]struct{}, len(existing))
	for _, p := range existing {
		used[p.ID] = struct{}{}
	}
	for i := len(existing) + 1; ; i++ {
		candidate := fmt.Sprintf("provider_%d", i)
		if _, exists := used[candidate]; !exists {
			return candidate
		}
	}
}

func (s *Server) handleCreateProvider(c *fiber.Ctx) error {
	if s.configManager == nil {
		return RespondServiceUnavailable(c, "Configuration management not available", "CONFIG_UNAVAILABLE")
	}

	// Get current config
	currentConfig := s.configManager.GetConfig()
	if currentConfig == nil {
		return RespondInternalError(c, "Configuration not available", "CONFIG_NOT_FOUND")
	}

	// Decode create request
	var createReq struct {
		Name                     string `json:"name"`
		Host                     string `json:"host"`
		Port                     int    `json:"port"`
		Username                 string `json:"username"`
		Password                 string `json:"password"`
		MaxConnections           int    `json:"max_connections"`
		MinConnectionsAlive      int    `json:"min_connections_alive"`
		InflightRequests         int    `json:"inflight_requests"`
		StatInflightRequests     int    `json:"stat_inflight_requests"`
		TLS                      bool   `json:"tls"`
		InsecureTLS              bool   `json:"insecure_tls"`
		ProxyURL                 string `json:"proxy_url"`
		Enabled                  bool   `json:"enabled"`
		IsBackupProvider         bool   `json:"is_backup_provider"`
		StorageGroup             string `json:"storage_group"`
		SkipPing                 bool   `json:"skip_ping"`
		KeepaliveIntervalSeconds int    `json:"keepalive_interval_seconds"`
		KeepaliveCommand         string `json:"keepalive_command"`
		UserAgent                string `json:"user_agent"`
		QuotaBytes               int64  `json:"quota_bytes"`
		QuotaPeriodHours         int    `json:"quota_period_hours"`
		AccountExpirationDate    string `json:"account_expiration_date"`
	}

	if err := c.BodyParser(&createReq); err != nil {
		return RespondValidationError(c, "Invalid JSON in request body", err.Error())
	}

	// Validation
	if createReq.Host == "" {
		return RespondValidationError(c, "Host is required", "MISSING_HOST")
	}
	if createReq.Port <= 0 || createReq.Port > 65535 {
		return RespondValidationError(c, "Valid port is required (1-65535)", "INVALID_PORT")
	}
	if createReq.Username == "" {
		return RespondValidationError(c, "Username is required", "MISSING_USERNAME")
	}
	if createReq.MaxConnections <= 0 {
		createReq.MaxConnections = 1 // Default
	}
	if createReq.MinConnectionsAlive < 0 || createReq.MinConnectionsAlive > createReq.MaxConnections {
		return RespondValidationError(c, "MinConnectionsAlive must be between 0 and MaxConnections", "INVALID_MIN_CONNECTIONS_ALIVE")
	}

	// Generate a new ID that doesn't collide with an existing one.
	newID := nextProviderID(currentConfig.Providers)

	// Create new provider
	newProvider := config.ProviderConfig{
		ID:                       newID,
		Name:                     createReq.Name,
		Host:                     createReq.Host,
		Port:                     createReq.Port,
		Username:                 createReq.Username,
		Password:                 createReq.Password,
		MaxConnections:           createReq.MaxConnections,
		MinConnectionsAlive:      createReq.MinConnectionsAlive,
		InflightRequests:         createReq.InflightRequests,
		StatInflightRequests:     createReq.StatInflightRequests,
		TLS:                      createReq.TLS,
		InsecureTLS:              createReq.InsecureTLS,
		ProxyURL:                 createReq.ProxyURL,
		Enabled:                  &createReq.Enabled,
		IsBackupProvider:         &createReq.IsBackupProvider,
		StorageGroup:             createReq.StorageGroup,
		SkipPing:                 createReq.SkipPing,
		KeepaliveIntervalSeconds: createReq.KeepaliveIntervalSeconds,
		KeepaliveCommand:         createReq.KeepaliveCommand,
		UserAgent:                createReq.UserAgent,
		QuotaBytes:               createReq.QuotaBytes,
		QuotaPeriodHours:         createReq.QuotaPeriodHours,
		AccountExpirationDate:    createReq.AccountExpirationDate,
	}

	// Add to config
	newConfig := currentConfig.DeepCopy()
	newConfig.Providers = append(newConfig.Providers, newProvider)

	// Validate and save
	if err := s.configManager.ValidateConfigUpdate(newConfig); err != nil {
		return RespondValidationError(c, "Configuration validation failed", err.Error())
	}

	// Refresh from normalized config (Validate may have applied defaults)
	newProvider = newConfig.Providers[len(newConfig.Providers)-1]

	if err := s.configManager.UpdateConfig(newConfig); err != nil {
		return RespondInternalError(c, "Failed to update configuration", err.Error())
	}

	if err := s.configManager.SaveConfig(); err != nil {
		return RespondInternalError(c, "Failed to save configuration", err.Error())
	}

	// Return sanitized provider
	response := ProviderAPIResponse{
		ID:                       newProvider.ID,
		Name:                     newProvider.Name,
		Host:                     newProvider.Host,
		Port:                     newProvider.Port,
		Username:                 newProvider.Username,
		MaxConnections:           newProvider.MaxConnections,
		MinConnectionsAlive:      newProvider.MinConnectionsAlive,
		TLS:                      newProvider.TLS,
		InsecureTLS:              newProvider.InsecureTLS,
		ProxyURL:                 newProvider.ProxyURL,
		PasswordSet:              newProvider.Password != "",
		Enabled:                  newProvider.Enabled != nil && *newProvider.Enabled,
		IsBackupProvider:         newProvider.IsBackupProvider != nil && *newProvider.IsBackupProvider,
		StorageGroup:             newProvider.StorageGroup,
		InflightRequests:         newProvider.InflightRequests,
		StatInflightRequests:     newProvider.StatInflightRequests,
		LastRTTMs:                newProvider.LastRTTMs,
		SkipPing:                 newProvider.SkipPing,
		KeepaliveIntervalSeconds: newProvider.KeepaliveIntervalSeconds,
		KeepaliveCommand:         newProvider.KeepaliveCommand,
		UserAgent:                newProvider.UserAgent,
		QuotaBytes:               newProvider.QuotaBytes,
		QuotaPeriodHours:         newProvider.QuotaPeriodHours,
		AccountExpirationDate:    newProvider.AccountExpirationDate,
	}

	return RespondSuccess(c, response)
}

// handleUpdateProvider updates an existing NNTP provider
//
//	@Summary		Update NNTP provider
//	@Description	Updates an existing NNTP provider by ID.
//	@Tags			Providers
//	@Accept			json
//	@Produce		json
//	@Param			id		path		string					true	"Provider ID"
//	@Param			body	body		ProviderUpdateRequest	true	"Fields to update"
//	@Success		200		{object}	APIResponse{data=ProviderAPIResponse}
//	@Failure		400		{object}	APIResponse
//	@Failure		404		{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/providers/{id} [put]
func (s *Server) handleUpdateProvider(c *fiber.Ctx) error {
	if s.configManager == nil {
		return RespondServiceUnavailable(c, "Configuration management not available", "CONFIG_UNAVAILABLE")
	}

	// Get provider ID from URL
	providerID := c.Params("id")
	if providerID == "" {
		return RespondValidationError(c, "Provider ID is required", "MISSING_PROVIDER_ID")
	}

	// Get current config
	currentConfig := s.configManager.GetConfig()
	if currentConfig == nil {
		return RespondInternalError(c, "Configuration not available", "CONFIG_NOT_FOUND")
	}

	// Find provider
	providerIndex := -1
	for i, p := range currentConfig.Providers {
		if p.ID == providerID {
			providerIndex = i
			break
		}
	}

	if providerIndex == -1 {
		return RespondNotFound(c, "Provider", "PROVIDER_NOT_FOUND")
	}

	// Decode update request (partial update)
	var updateReq struct {
		Name                     *string `json:"name,omitempty"`
		Host                     *string `json:"host,omitempty"`
		Port                     *int    `json:"port,omitempty"`
		Username                 *string `json:"username,omitempty"`
		Password                 *string `json:"password,omitempty"`
		MaxConnections           *int    `json:"max_connections,omitempty"`
		MinConnectionsAlive      *int    `json:"min_connections_alive,omitempty"`
		InflightRequests         *int    `json:"inflight_requests,omitempty"`
		StatInflightRequests     *int    `json:"stat_inflight_requests,omitempty"`
		TLS                      *bool   `json:"tls,omitempty"`
		InsecureTLS              *bool   `json:"insecure_tls,omitempty"`
		ProxyURL                 *string `json:"proxy_url,omitempty"`
		Enabled                  *bool   `json:"enabled,omitempty"`
		IsBackupProvider         *bool   `json:"is_backup_provider,omitempty"`
		StorageGroup             *string `json:"storage_group,omitempty"`
		SkipPing                 *bool   `json:"skip_ping,omitempty"`
		KeepaliveIntervalSeconds *int    `json:"keepalive_interval_seconds,omitempty"`
		KeepaliveCommand         *string `json:"keepalive_command,omitempty"`
		UserAgent                *string `json:"user_agent,omitempty"`
		QuotaBytes               *int64  `json:"quota_bytes,omitempty"`
		QuotaPeriodHours         *int    `json:"quota_period_hours,omitempty"`
		AccountExpirationDate    *string `json:"account_expiration_date,omitempty"`
	}

	if err := c.BodyParser(&updateReq); err != nil {
		return RespondValidationError(c, "Invalid JSON in request body", err.Error())
	}

	// Create updated config with proper deep copy
	newConfig := currentConfig.DeepCopy()

	// Get the provider to modify from the deep copy
	provider := newConfig.Providers[providerIndex]

	// Apply updates
	if updateReq.Host != nil {
		if *updateReq.Host == "" {
			return RespondValidationError(c, "Host cannot be empty", "INVALID_HOST")
		}
		provider.Host = *updateReq.Host
	}
	if updateReq.Port != nil {
		if *updateReq.Port <= 0 || *updateReq.Port > 65535 {
			return RespondValidationError(c, "Valid port is required (1-65535)", "INVALID_PORT")
		}
		provider.Port = *updateReq.Port
	}
	if updateReq.Username != nil {
		if *updateReq.Username == "" {
			return RespondValidationError(c, "Username cannot be empty", "INVALID_USERNAME")
		}
		provider.Username = *updateReq.Username
	}
	if updateReq.Password != nil {
		provider.Password = *updateReq.Password
	}
	if updateReq.MaxConnections != nil {
		if *updateReq.MaxConnections <= 0 {
			return RespondValidationError(c, "MaxConnections must be positive", "INVALID_MAX_CONNECTIONS")
		}
		provider.MaxConnections = *updateReq.MaxConnections
	}
	if updateReq.MinConnectionsAlive != nil {
		if *updateReq.MinConnectionsAlive < 0 {
			return RespondValidationError(c, "MinConnectionsAlive must not be negative", "INVALID_MIN_CONNECTIONS_ALIVE")
		}
		provider.MinConnectionsAlive = *updateReq.MinConnectionsAlive
	}
	if updateReq.InflightRequests != nil {
		provider.InflightRequests = *updateReq.InflightRequests
	}
	if updateReq.StatInflightRequests != nil {
		provider.StatInflightRequests = *updateReq.StatInflightRequests
	}
	if updateReq.TLS != nil {
		provider.TLS = *updateReq.TLS
	}
	if updateReq.InsecureTLS != nil {
		provider.InsecureTLS = *updateReq.InsecureTLS
	}
	if updateReq.ProxyURL != nil {
		provider.ProxyURL = *updateReq.ProxyURL
	}
	if updateReq.Enabled != nil {
		provider.Enabled = updateReq.Enabled
	}
	if updateReq.IsBackupProvider != nil {
		provider.IsBackupProvider = updateReq.IsBackupProvider
	}
	if updateReq.StorageGroup != nil {
		provider.StorageGroup = *updateReq.StorageGroup
	}
	if updateReq.SkipPing != nil {
		provider.SkipPing = *updateReq.SkipPing
	}
	if updateReq.KeepaliveIntervalSeconds != nil {
		provider.KeepaliveIntervalSeconds = *updateReq.KeepaliveIntervalSeconds
	}
	if updateReq.KeepaliveCommand != nil {
		provider.KeepaliveCommand = *updateReq.KeepaliveCommand
	}
	if updateReq.QuotaBytes != nil {
		provider.QuotaBytes = *updateReq.QuotaBytes
	}
	if updateReq.QuotaPeriodHours != nil {
		provider.QuotaPeriodHours = *updateReq.QuotaPeriodHours
	}
	if updateReq.UserAgent != nil {
		provider.UserAgent = *updateReq.UserAgent
	}
	if updateReq.AccountExpirationDate != nil {
		provider.AccountExpirationDate = *updateReq.AccountExpirationDate
	}
	if updateReq.Name != nil {
		provider.Name = *updateReq.Name
	}

	// Assign the updated provider back to the slice
	newConfig.Providers[providerIndex] = provider

	// Validate and save
	if err := s.configManager.ValidateConfigUpdate(newConfig); err != nil {
		return RespondValidationError(c, "Configuration validation failed", err.Error())
	}

	// Refresh from normalized config (Validate may have applied defaults)
	provider = newConfig.Providers[providerIndex]

	if err := s.configManager.UpdateConfig(newConfig); err != nil {
		return RespondInternalError(c, "Failed to update configuration", err.Error())
	}

	if err := s.configManager.SaveConfig(); err != nil {
		return RespondInternalError(c, "Failed to save configuration", err.Error())
	}

	// Return sanitized provider
	response := ProviderAPIResponse{
		ID:                       provider.ID,
		Name:                     provider.Name,
		Host:                     provider.Host,
		Port:                     provider.Port,
		Username:                 provider.Username,
		MaxConnections:           provider.MaxConnections,
		MinConnectionsAlive:      provider.MinConnectionsAlive,
		TLS:                      provider.TLS,
		InsecureTLS:              provider.InsecureTLS,
		ProxyURL:                 provider.ProxyURL,
		PasswordSet:              provider.Password != "",
		Enabled:                  provider.Enabled != nil && *provider.Enabled,
		IsBackupProvider:         provider.IsBackupProvider != nil && *provider.IsBackupProvider,
		StorageGroup:             provider.StorageGroup,
		InflightRequests:         provider.InflightRequests,
		StatInflightRequests:     provider.StatInflightRequests,
		LastRTTMs:                provider.LastRTTMs,
		SkipPing:                 provider.SkipPing,
		KeepaliveIntervalSeconds: provider.KeepaliveIntervalSeconds,
		KeepaliveCommand:         provider.KeepaliveCommand,
		UserAgent:                provider.UserAgent,
		QuotaBytes:               provider.QuotaBytes,
		QuotaPeriodHours:         provider.QuotaPeriodHours,
		AccountExpirationDate:    provider.AccountExpirationDate,
	}

	return RespondSuccess(c, response)
}

// handleResetProviderQuota resets the download quota counter for a provider.
//
//	@Summary		Reset provider download quota
//	@Description	Resets the download quota counter for the specified provider, allowing it to download again.
//	@Tags			Providers
//	@Produce		json
//	@Param			id	path	string	true	"Provider ID"
//	@Success		200	{object}	APIResponse
//	@Failure		404	{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/providers/{id}/reset-quota [post]
func (s *Server) handleResetProviderQuota(c *fiber.Ctx) error {
	if s.configManager == nil {
		return RespondServiceUnavailable(c, "Configuration management not available", "CONFIG_UNAVAILABLE")
	}
	if s.poolManager == nil {
		return RespondServiceUnavailable(c, "Pool manager not available", "POOL_UNAVAILABLE")
	}

	providerID := c.Params("id")
	if providerID == "" {
		return RespondValidationError(c, "Provider ID is required", "MISSING_PROVIDER_ID")
	}

	currentConfig := s.configManager.GetConfig()
	if currentConfig == nil {
		return RespondInternalError(c, "Configuration not available", "CONFIG_NOT_FOUND")
	}

	// Find the provider in config
	var provider *config.ProviderConfig
	for i := range currentConfig.Providers {
		if currentConfig.Providers[i].ID == providerID {
			provider = &currentConfig.Providers[i]
			break
		}
	}
	if provider == nil {
		return RespondNotFound(c, "Provider", "PROVIDER_NOT_FOUND")
	}

	if provider.QuotaBytes == 0 {
		return RespondBadRequest(c, "Provider has no quota configured", "NO_QUOTA")
	}

	poolName := provider.NNTPPoolName()

	if err := s.poolManager.ResetProviderQuota(c.Context(), poolName); err != nil {
		return RespondInternalError(c, "Failed to reset provider quota", err.Error())
	}

	return RespondMessage(c, "Provider quota reset successfully")
}

// handleDeleteProvider removes an NNTP provider
//
//	@Summary		Delete NNTP provider
//	@Description	Removes an NNTP provider by ID from the configuration.
//	@Tags			Providers
//	@Produce		json
//	@Param			id	path	string	true	"Provider ID"
//	@Success		204
//	@Failure		404	{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/providers/{id} [delete]
func (s *Server) handleDeleteProvider(c *fiber.Ctx) error {
	if s.configManager == nil {
		return RespondServiceUnavailable(c, "Configuration management not available", "CONFIG_UNAVAILABLE")
	}

	// Get provider ID from URL
	providerID := c.Params("id")
	if providerID == "" {
		return RespondValidationError(c, "Provider ID is required", "MISSING_PROVIDER_ID")
	}

	// Get current config
	currentConfig := s.configManager.GetConfig()
	if currentConfig == nil {
		return RespondInternalError(c, "Configuration not available", "CONFIG_NOT_FOUND")
	}

	// Find provider
	providerIndex := -1
	for i, p := range currentConfig.Providers {
		if p.ID == providerID {
			providerIndex = i
			break
		}
	}

	if providerIndex == -1 {
		return RespondNotFound(c, "Provider", "PROVIDER_NOT_FOUND")
	}

	// Create new config without the provider
	newConfig := currentConfig.DeepCopy()
	newConfig.Providers = append(newConfig.Providers[:providerIndex],
		newConfig.Providers[providerIndex+1:]...)

	// Validate and save
	if err := s.configManager.ValidateConfigUpdate(newConfig); err != nil {
		return RespondValidationError(c, "Configuration validation failed", err.Error())
	}

	if err := s.configManager.UpdateConfig(newConfig); err != nil {
		return RespondInternalError(c, "Failed to update configuration", err.Error())
	}

	if err := s.configManager.SaveConfig(); err != nil {
		return RespondInternalError(c, "Failed to save configuration", err.Error())
	}

	response := struct {
		Message string `json:"message"`
	}{
		Message: "Provider deleted successfully",
	}

	return RespondSuccess(c, response)
}

// handleReorderProviders reorders the provider list
//
//	@Summary		Reorder NNTP providers
//	@Description	Sets the priority order of NNTP providers.
//	@Tags			Providers
//	@Accept			json
//	@Produce		json
//	@Param			body	body		ProviderReorderRequest	true	"Ordered list of provider IDs"
//	@Success		200		{object}	APIResponse
//	@Failure		400		{object}	APIResponse
//	@Security		BearerAuth
//	@Router			/providers/reorder [put]
func (s *Server) handleReorderProviders(c *fiber.Ctx) error {
	if s.configManager == nil {
		return RespondServiceUnavailable(c, "Configuration management not available", "CONFIG_UNAVAILABLE")
	}

	// Decode reorder request
	var reorderReq struct {
		ProviderIDs []string `json:"provider_ids"`
	}

	if err := c.BodyParser(&reorderReq); err != nil {
		return RespondValidationError(c, "Invalid JSON in request body", err.Error())
	}

	if len(reorderReq.ProviderIDs) == 0 {
		return RespondValidationError(c, "Provider IDs array is required", "MISSING_PROVIDER_IDS")
	}

	// Get current config
	currentConfig := s.configManager.GetConfig()
	if currentConfig == nil {
		return RespondInternalError(c, "Configuration not available", "CONFIG_NOT_FOUND")
	}

	// Validate that all IDs exist and no duplicates
	providerMap := make(map[string]config.ProviderConfig)
	for _, p := range currentConfig.Providers {
		providerMap[p.ID] = p
	}

	if len(reorderReq.ProviderIDs) != len(currentConfig.Providers) {
		return RespondValidationError(c, "Provider IDs count mismatch", "INVALID_PROVIDER_COUNT")
	}

	// Build new ordered providers list
	newProviders := make([]config.ProviderConfig, 0, len(reorderReq.ProviderIDs))
	for _, id := range reorderReq.ProviderIDs {
		provider, exists := providerMap[id]
		if !exists {
			return RespondNotFound(c, fmt.Sprintf("Provider ID '%s'", id), "PROVIDER_NOT_FOUND")
		}
		newProviders = append(newProviders, provider)
	}

	// Create new config with reordered providers
	newConfig := currentConfig.DeepCopy()
	newConfig.Providers = newProviders

	// Validate and save
	if err := s.configManager.ValidateConfigUpdate(newConfig); err != nil {
		return RespondValidationError(c, "Configuration validation failed", err.Error())
	}

	if err := s.configManager.UpdateConfig(newConfig); err != nil {
		return RespondInternalError(c, "Failed to update configuration", err.Error())
	}

	if err := s.configManager.SaveConfig(); err != nil {
		return RespondInternalError(c, "Failed to save configuration", err.Error())
	}

	// Return sanitized providers in new order
	providers := make([]ProviderAPIResponse, len(newProviders))
	for i, p := range newProviders {
		providers[i] = ProviderAPIResponse{
			ID:                    p.ID,
			Name:                  p.Name,
			Host:                  p.Host,
			Port:                  p.Port,
			Username:              p.Username,
			MaxConnections:        p.MaxConnections,
			MinConnectionsAlive:   p.MinConnectionsAlive,
			TLS:                   p.TLS,
			InsecureTLS:           p.InsecureTLS,
			ProxyURL:              p.ProxyURL,
			PasswordSet:           p.Password != "",
			Enabled:               p.Enabled != nil && *p.Enabled,
			IsBackupProvider:      p.IsBackupProvider != nil && *p.IsBackupProvider,
			StorageGroup:          p.StorageGroup,
			InflightRequests:      p.InflightRequests,
			StatInflightRequests:  p.StatInflightRequests,
			LastRTTMs:             p.LastRTTMs,
			AccountExpirationDate: p.AccountExpirationDate,
		}
	}

	return RespondSuccess(c, providers)
}

// startRCServerIfNeeded starts the RC server if RClone is enabled and RC is not running
func (s *Server) startRCServerIfNeeded(ctx context.Context) {
	// Check if we have a mount service to work with
	if s.mountService == nil {
		slog.WarnContext(ctx, "Mount service not available, cannot start RC server")
		return
	}

	// Only start RC server for rclone-based mount types
	if s.configManager != nil {
		cfg := s.configManager.GetConfig()
		if cfg != nil && cfg.MountType != config.MountTypeRClone && cfg.MountType != config.MountTypeRCloneExternal {
			slog.DebugContext(ctx, "Skipping RC server start, mount_type is not rclone-based",
				"mount_type", string(cfg.MountType))
			return
		}
	}

	// Use the mount service to start the RC server (non-blocking for config save)
	go func() {
		if err := s.mountService.StartRCServer(ctx); err != nil {
			slog.ErrorContext(ctx, "Failed to start RClone RC server via mount service", "error", err)
			return
		}

		// Now that RC server is ready, initialize RClone client in importer service if available
		if s.importerService != nil {
			s.importerService.SetRcloneClient(s.mountService.GetManager())
			slog.InfoContext(ctx, "RClone client initialized in importer service")
		}
	}()
}

// ensureSABnzbdCategoryDirectories creates directories for all SABnzbd categories in the mount path
func (s *Server) ensureSABnzbdCategoryDirectories(cfg *config.Config) error {
	// Only process if SABnzbd is enabled
	if cfg.SABnzbd.Enabled == nil || !*cfg.SABnzbd.Enabled {
		return nil
	}

	// Create base SABnzbd complete directory
	baseDir := filepath.Join(cfg.Metadata.RootPath, cfg.SABnzbd.CompleteDir)
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return fmt.Errorf("failed to create SABnzbd base directory: %w", err)
	}

	// Create directories for each category
	for _, category := range cfg.SABnzbd.Categories {
		if category.Dir != "" {
			categoryDir := filepath.Join(baseDir, category.Dir)
			if err := os.MkdirAll(categoryDir, 0755); err != nil {
				return fmt.Errorf("failed to create category directory %s: %w", category.Name, err)
			}
		}
	}

	return nil
}

// getAPIKeyForConfig retrieves the API key for config responses
func (s *Server) getAPIKeyForConfig(c *fiber.Ctx) string {
	// Try to get user from context (if authenticated)
	user := auth.GetUserFromContext(c)
	if user != nil && user.APIKey != nil {
		return *user.APIKey
	}

	// Try to get from Arrs service which handles bootstrapping default admin if needed
	if s.arrsService != nil {
		if key := s.arrsService.GetFirstAdminAPIKey(c.Context()); key != "" {
			return key
		}
	}

	// If no authenticated user and arrs service didn't return one, try manual DB check
	if s.userRepo != nil {
		users, err := s.userRepo.GetAllUsers(c.Context())
		if err == nil && len(users) > 0 && users[0].APIKey != nil {
			return *users[0].APIKey
		}
	}

	return ""
}
