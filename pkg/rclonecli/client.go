package rclonecli

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"runtime"
	"time"
)

// Mount creates a mount using the rclone RC API with retry logic
func (m *Manager) Mount(ctx context.Context, provider, mountPath, webdavURL string) error {
	m.mountMu.Lock()
	defer m.mountMu.Unlock()
	return m.mountWithRetry(ctx, provider, mountPath, webdavURL, 3)
}

// mountWithRetry attempts to mount with retry logic.
//
// A failed mount/mount attempt leaves a leaked VFS instance inside the rcd
// subprocess: rclone creates the VFS before attempting the FUSE mount and does
// not shut it down on failure, and the failed mount is never registered with
// mount/unmount. Neither RC unmount nor fusermount can remove that in-process
// state, so the only reliable cleanup is restarting the rcd subprocess between
// attempts (and after the terminal failure) to guarantee a clean VFS registry.
func (m *Manager) mountWithRetry(ctx context.Context, provider, mountPath, webdavURL string, maxRetries int) error {
	if m.mountFn == nil {
		return fmt.Errorf("mount operation is not configured")
	}
	if m.retryDelay == nil {
		m.retryDelay = func(attempt int) time.Duration { return time.Duration(attempt*2) * time.Second }
	}
	if !m.IsReady() {
		if err := m.WaitForReady(30 * time.Second); err != nil {
			return fmt.Errorf("rclone RC server not ready: %w", err)
		}
	}

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			// Clear partial mount state and reset the rcd subprocess so the
			// retry starts from a clean VFS registry.
			if err := m.cleanupAfterFailedMount(ctx, provider, mountPath); err != nil {
				return fmt.Errorf("mount attempt %d failed for %s: %w; cleanup failed: %w", attempt, provider, lastErr, err)
			}

			// Wait before retry
			wait := m.retryDelay(attempt)
			m.logger.DebugContext(ctx, "Retrying mount operation", "attempt", attempt, "provider", provider)
			if !sleepWithContext(ctx, wait) {
				return ctx.Err()
			}
		}

		if err := m.mountFn(ctx, provider, mountPath, webdavURL); err != nil {
			lastErr = err
			m.logger.ErrorContext(ctx, "Mount attempt failed", "err", err, "provider", provider, "attempt", attempt+1)
			continue
		}

		return nil // Success
	}

	// The terminal attempt also leaked a VFS. Reset the rcd subprocess so a
	// clean state is left behind even when mounting is reported as failed.
	if err := m.cleanupAfterFailedMount(ctx, provider, mountPath); err != nil {
		return fmt.Errorf("mount failed for %s: %w; cleanup failed: %w", provider, lastErr, err)
	}

	if lastErr != nil {
		return fmt.Errorf("mount failed for %s: %w", provider, lastErr)
	}
	return fmt.Errorf("mount failed for %s", provider)
}

// cleanupAfterFailedMount removes partial mount state from a failed mount/mount
// attempt and restarts the rcd subprocess. Restarting is required because a
// failed FUSE mount leaves a leaked VFS instance inside the rcd process that
// mount/unmount and fusermount cannot remove.
func (m *Manager) cleanupAfterFailedMount(ctx context.Context, provider, mountPath string) error {
	var restore []*MountInfo
	m.mountsMutex.RLock()
	for name, info := range m.mounts {
		if name != provider && info.desired {
			copy := *info
			restore = append(restore, &copy)
		}
	}
	m.mountsMutex.RUnlock()

	// Force unmount using system commands
	if err := m.forceUnmount(mountPath); err != nil {
		m.logger.DebugContext(ctx, "Force unmount returned error (may be expected)", "err", err, "provider", provider)
	}

	// Clean up mount directory if empty
	_ = os.Remove(mountPath)

	if !m.IsReady() {
		// rcd is not running; there is no leaked VFS state to reset.
		return nil
	}

	m.logger.InfoContext(ctx, "Restarting rclone RC server to clear leaked VFS state", "provider", provider)
	cleanupCtx, cancel := context.WithTimeout(m.ctx, 45*time.Second)
	defer cancel()
	if err := m.restart(cleanupCtx); err != nil {
		return fmt.Errorf("failed to restart rclone RC server after failed mount: %w", err)
	}
	// Restarting rcd invalidates every FUSE mount in that process. Restore
	// mounts that were active before recycling after the current operation
	// releases mountMu; otherwise one failed provider permanently drops all
	// unrelated providers.
	for _, info := range restore {
		if err := m.mountWithRetry(cleanupCtx, info.Provider, info.LocalPath, info.WebDAVURL, 3); err != nil {
			return fmt.Errorf("failed to restore sibling mount %s after rcd restart: %w", info.Provider, err)
		}
	}
	return nil
}

// sleepWithContext sleeps for d unless ctx is cancelled first. It returns false
// when the sleep was interrupted by context cancellation.
func sleepWithContext(ctx context.Context, d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-ctx.Done():
		return false
	}
}

// performMount performs a single mount attempt
func (m *Manager) performMount(ctx context.Context, provider, mountPath, webdavURL string) error {
	cfg := m.cfg.GetConfig()

	// Create mount directory (skip on Windows: WinFSP requires the mount point to NOT exist)
	if runtime.GOOS != "windows" {
		m.logger.InfoContext(ctx, "Creating mount directory", "provider", provider, "path", mountPath)
		if err := os.MkdirAll(mountPath, 0755); err != nil {
			if !os.IsExist(err) {
				return fmt.Errorf("failed to create mount directory %s: %w", mountPath, err)
			}
		}
	}

	// Check if already mounted
	m.mountsMutex.RLock()
	existingMount, exists := m.mounts[provider]
	m.mountsMutex.RUnlock()

	if exists && existingMount.Mounted {
		m.logger.InfoContext(ctx, "Already mounted", "provider", provider, "path", mountPath)
		return nil
	}

	// Clean up any stale mount first
	if exists && !existingMount.Mounted {
		err := m.forceUnmountPath(mountPath)
		if err != nil {
			slog.InfoContext(ctx, "Nothing to unmount", "err", err, "provider", provider, "path", mountPath)
		}
	}

	// Create rclone config for this provider
	if err := m.createConfig(ctx, provider, webdavURL, cfg.WebDAV.User, cfg.WebDAV.Password); err != nil {
		return fmt.Errorf("failed to create rclone config: %w", err)
	}

	// Prepare mount arguments
	mountArgs := map[string]any{
		"fs":         fmt.Sprintf("%s:", provider),
		"mountPoint": mountPath,
	}
	mountOpt := map[string]any{
		"AllowNonEmpty": cfg.RClone.AllowNonEmpty,
		"AllowOther":    cfg.RClone.AllowOther,
		"DebugFUSE":     false,
		"DeviceName":    provider,
		"VolumeName":    provider,
	}

	configOpts := make(map[string]any)

	if cfg.RClone.BufferSize != "" {
		configOpts["BufferSize"] = cfg.RClone.BufferSize
	}

	if len(configOpts) > 0 {
		// Only add _config if there are options to set
		mountArgs["_config"] = configOpts
	}
	vfsOpt := map[string]any{
		"CacheMode": cfg.RClone.VFSCacheMode,
	}
	vfsOpt["PollInterval"] = 0 // Poll interval not supported for webdav, set to 0

	// DirCacheTime is applied regardless of vfs_cache_mode: it governs how long
	// directory listings are cached, not file content, so it still matters when
	// content caching is off. Cuts PROPFIND load against the WebDAV backend
	// during library scans.
	if cfg.RClone.DirCacheTime != "" {
		if dirCacheTime, e := time.ParseDuration(cfg.RClone.DirCacheTime); e == nil {
			vfsOpt["DirCacheTime"] = dirCacheTime.Nanoseconds()
		}
	}

	// Add VFS options if caching is enabled
	if cfg.RClone.VFSCacheMode != "off" {
		if cfg.RClone.VFSCacheMaxAge != "" {
			if attrTimeout, e := time.ParseDuration(cfg.RClone.VFSCacheMaxAge); e == nil {
				vfsOpt["CacheMaxAge"] = attrTimeout.Nanoseconds()
			}
		}
		if cfg.RClone.VFSCacheMaxSize != "" {
			vfsOpt["CacheMaxSize"] = cfg.RClone.VFSCacheMaxSize
			// Ensure the reported total disk space matches the configured cache size
			// so media servers see accurate available space
			vfsOpt["DiskSpaceTotalSize"] = cfg.RClone.VFSCacheMaxSize
		}
		if cfg.RClone.VFSCacheMinFreeSpace != "" {
			vfsOpt["CacheMinFreeSpace"] = cfg.RClone.VFSCacheMinFreeSpace
		}
		if cfg.RClone.VFSCachePollInterval != "" {
			vfsOpt["CachePollInterval"] = cfg.RClone.VFSCachePollInterval
		}
		if cfg.RClone.VFSReadChunkSize != "" {
			vfsOpt["ChunkSize"] = cfg.RClone.VFSReadChunkSize
		}
		if cfg.RClone.VFSReadChunkSizeLimit != "" {
			vfsOpt["ChunkSizeLimit"] = cfg.RClone.VFSReadChunkSizeLimit
		}
		if cfg.RClone.VFSReadAhead != "" {
			vfsOpt["ReadAhead"] = cfg.RClone.VFSReadAhead
		}
		if cfg.RClone.NoChecksum {
			vfsOpt["NoChecksum"] = cfg.RClone.NoChecksum
		}
		if cfg.RClone.NoModTime {
			vfsOpt["NoModTime"] = cfg.RClone.NoModTime
		}
	}

	// Add mount options based on configuration
	if cfg.RClone.UID != 0 {
		mountOpt["UID"] = cfg.RClone.UID
	}
	if cfg.RClone.GID != 0 {
		mountOpt["GID"] = cfg.RClone.GID
	}
	if cfg.RClone.AttrTimeout != "" {
		if attrTimeout, e := time.ParseDuration(cfg.RClone.AttrTimeout); e == nil {
			mountOpt["AttrTimeout"] = attrTimeout.Nanoseconds()
		}
	}

	// Merge custom mount options (can override any of the above)
	for k, v := range cfg.RClone.MountOptions {
		mountOpt[k] = v
	}

	if cfg.RClone.Links {
		vfsOpt["Links"] = true
	}
	mountArgs["vfsOpt"] = vfsOpt
	mountArgs["mountOpt"] = mountOpt
	// Make the mount request
	req := RCRequest{
		Command: "mount/mount",
		Args:    mountArgs,
	}

	_, err := m.makeRequestWithContext(ctx, req, true)
	if err != nil {
		// Clean up mount point on failure
		m.forceUnmountPath(mountPath)
		return fmt.Errorf("failed to create mount for %s: %w", provider, err)
	}

	// Store mount info
	mountInfo := &MountInfo{
		Provider:   provider,
		LocalPath:  mountPath,
		WebDAVURL:  webdavURL,
		Mounted:    true,
		MountedAt:  time.Now().Format(time.RFC3339),
		ConfigName: provider,
		desired:    true,
	}

	m.mountsMutex.Lock()
	m.mounts[provider] = mountInfo
	m.mountsMutex.Unlock()

	return nil
}

// Unmount unmounts a specific provider
func (m *Manager) Unmount(ctx context.Context, provider string) error {
	m.mountMu.Lock()
	defer m.mountMu.Unlock()
	return m.unmount(ctx, provider, true)
}

// unmount is the internal unmount function.
//
// When restartRCD is true the rcd subprocess is restarted after a successful
// unmount: rclone keeps the VFS alive in-process after mount/unmount, so
// without a restart a subsequent mount would create a second VFS instance
// under the same name. restartRCD should be false during full shutdown, where
// terminating the rcd subprocess already reclaims all VFS state.
func (m *Manager) unmount(ctx context.Context, provider string, restartRCD bool) error {
	m.mountsMutex.RLock()
	mountInfo, exists := m.mounts[provider]
	m.mountsMutex.RUnlock()

	if !exists || !mountInfo.Mounted {
		m.logger.InfoContext(ctx, "Mount not found or already unmounted", "provider", provider)
		return nil
	}

	m.logger.InfoContext(ctx, "Unmounting", "provider", provider, "path", mountInfo.LocalPath)

	// Try RC unmount first
	req := RCRequest{
		Command: "mount/unmount",
		Args: map[string]any{
			"mountPoint": mountInfo.LocalPath,
		},
	}

	var rcErr error
	if m.IsReady() {
		_, rcErr = m.makeRequestWithContext(ctx, req, true)
	}

	var forceErr error
	// If RC unmount fails or server is not ready, try force unmount.
	if rcErr != nil || !m.IsReady() {
		m.logger.WarnContext(ctx, "RC unmount failed, trying force unmount", "err", rcErr, "provider", provider)
		forceErr = m.forceUnmount(mountInfo.LocalPath)
		if forceErr != nil {
			m.logger.ErrorContext(ctx, "Force unmount failed", "err", forceErr, "provider", provider)
		}
	}

	// Update mount info
	m.mountsMutex.Lock()
	if info, exists := m.mounts[provider]; exists {
		info.Mounted = forceErr != nil
		if forceErr == nil {
			info.desired = false
		}
		info.Error = ""
		if rcErr != nil {
			info.Error = rcErr.Error()
		}
		if forceErr != nil {
			info.Error = forceErr.Error()
		}
	}
	m.mountsMutex.Unlock()

	m.logger.InfoContext(ctx, "Unmount completed", "provider", provider)
	if forceErr != nil {
		return fmt.Errorf("failed to unmount %s: %w", provider, forceErr)
	}

	return nil
}

// UnmountAll unmounts all mounts
func (m *Manager) UnmountAll(ctx context.Context) error {
	m.mountsMutex.RLock()
	providers := make([]string, 0, len(m.mounts))
	for provider, mount := range m.mounts {
		if mount.Mounted {
			providers = append(providers, provider)
		}
	}
	m.mountsMutex.RUnlock()

	var lastError error
	for _, provider := range providers {
		if err := m.unmount(ctx, provider, false); err != nil {
			lastError = err
			m.logger.DebugContext(ctx, "Failed to unmount", "err", err, "provider", provider)
		}
	}

	return lastError
}

// GetMountInfo returns information about a specific mount
func (m *Manager) GetMountInfo(provider string) (*MountInfo, bool) {
	m.mountsMutex.RLock()
	defer m.mountsMutex.RUnlock()

	info, exists := m.mounts[provider]
	if !exists {
		return nil, false
	}

	// Create a copy to avoid race conditions
	mountInfo := *info
	return &mountInfo, true
}

// GetAllMounts returns information about all mounts
func (m *Manager) GetAllMounts() map[string]*MountInfo {
	m.mountsMutex.RLock()
	defer m.mountsMutex.RUnlock()

	result := make(map[string]*MountInfo, len(m.mounts))
	for provider, info := range m.mounts {
		// Create a copy to avoid race conditions
		mountInfo := *info
		result[provider] = &mountInfo
	}

	return result
}

// IsMounted checks if a provider is mounted
func (m *Manager) IsMounted(provider string) bool {
	info, exists := m.GetMountInfo(provider)
	return exists && info.Mounted
}

// RefreshDir refreshes directories in the VFS cache
func (m *Manager) RefreshDir(ctx context.Context, provider string, dirs []string) error {
	if !m.IsReady() {
		return fmt.Errorf("rclone RC server not ready")
	}

	mountInfo, exists := m.GetMountInfo(provider)
	if !exists || !mountInfo.Mounted {
		return fmt.Errorf("provider %s not mounted", provider)
	}

	// If no specific directories provided, use root
	if len(dirs) == 0 {
		dirs = []string{"/"}
	}

	// rclone's VFS is forward-slash on every platform. Normalize here so a
	// caller that built a directory with filepath cannot silently no-op:
	// vfs/forget accepts any string and reports success either way.
	dirs = ToVFSPaths(dirs)

	errs := m.forgetDirs(ctx, provider, dirs)

	// Issue a vfs/refresh call for each directory individually
	for _, dir := range dirs {
		if dir == "" {
			continue
		}
		refreshArgs := map[string]any{
			"fs":        fmt.Sprintf("%s:", provider),
			"dir":       dir,
			"_async":    "false",
			"recursive": "false",
		}
		req := RCRequest{
			Command: "vfs/refresh",
			Args:    refreshArgs,
		}
		_, err := m.makeRequestWithContext(ctx, req, true)
		if err != nil {
			m.logger.ErrorContext(ctx, "Failed to refresh directory", "err", err, "provider", provider, "dir", dir)
			return fmt.Errorf("failed to refresh directory %s for provider %s: %w", dir, provider, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("VFS sync completed with %d errors; first error: %w", len(errs), errs[0])
	}

	return nil
}

// ForgetDir drops directories from the VFS cache without forcing an eager
// re-listing: the next client access re-lists lazily. Intended for deletions
// and moves, where an immediate re-enumeration of large directories would be
// wasted work.
func (m *Manager) ForgetDir(ctx context.Context, provider string, dirs []string) error {
	if !m.IsReady() {
		return fmt.Errorf("rclone RC server not ready")
	}

	mountInfo, exists := m.GetMountInfo(provider)
	if !exists || !mountInfo.Mounted {
		return fmt.Errorf("provider %s not mounted", provider)
	}

	// If no specific directories provided, use root
	if len(dirs) == 0 {
		dirs = []string{"/"}
	}

	errs := m.forgetDirs(ctx, provider, dirs)

	if len(errs) > 0 {
		return fmt.Errorf("VFS forget completed with %d errors; first error: %w", len(errs), errs[0])
	}

	return nil
}

// forgetDirs issues a vfs/forget call for each directory to ensure all parents/children are forgotten
func (m *Manager) forgetDirs(ctx context.Context, provider string, dirs []string) []error {
	var errs []error

	for _, dir := range dirs {
		if dir == "" {
			continue
		}
		forgetArgs := map[string]any{
			"fs":  fmt.Sprintf("%s:", provider),
			"dir": dir,
		}
		req := RCRequest{
			Command: "vfs/forget",
			Args:    forgetArgs,
		}
		_, err := m.makeRequestWithContext(ctx, req, true)
		if err != nil {
			m.logger.ErrorContext(ctx, "Failed to forget directory", "err", err, "provider", provider, "dir", dir)
			errs = append(errs, fmt.Errorf("failed to forget directory %s for provider %s: %w", dir, provider, err))
		}
	}

	return errs
}

// createConfig creates an rclone config entry for the provider
func (m *Manager) createConfig(ctx context.Context, configName, webdavURL string, user, pass string) error {
	req := RCRequest{
		Command: "config/create",
		Args: map[string]any{
			"name": configName,
			"type": "webdav",
			"parameters": map[string]any{
				"url":             webdavURL,
				"vendor":          "other",
				"pacer_min_sleep": "0",
				"user":            user,
				"pass":            pass,
			},
			// obscure tells rclone the password is plaintext. Without it,
			// config/create guesses, and guesses wrong for any password that is
			// itself revealable, storing it verbatim (#691).
			"opt": map[string]any{
				"obscure": true,
			},
		},
	}

	_, err := m.makeRequestWithContext(ctx, req, true)
	if err != nil {
		return fmt.Errorf("failed to create config %s: %w", configName, err)
	}
	return nil
}

// forceUnmountPath attempts to force unmount a path using system commands
func (m *Manager) forceUnmountPath(mountPath string) error {
	var methods [][]string

	if runtime.GOOS == "darwin" {
		// macOS-specific commands
		methods = [][]string{
			{"umount", "-f", mountPath},                 // macOS force unmount
			{"diskutil", "unmount", "force", mountPath}, // macOS diskutil
			{"umount", mountPath},                       // Standard unmount
		}
	} else {
		// Linux/Unix commands
		methods = [][]string{
			{"fusermount", "-uz", mountPath},
			{"umount", mountPath},
			{"umount", "-l", mountPath}, // lazy unmount
			{"fusermount3", "-uz", mountPath},
		}
	}

	for _, method := range methods {
		if err := m.tryUnmountCommand(method...); err == nil {
			m.logger.InfoContext(m.ctx, "Successfully unmounted using system command", "command", method, "path", mountPath)
			return nil
		}
	}

	return fmt.Errorf("all force unmount attempts failed for %s", mountPath)
}

// tryUnmountCommand tries to run an unmount command
func (m *Manager) tryUnmountCommand(args ...string) error {
	if len(args) == 0 {
		return fmt.Errorf("no command provided")
	}

	cmd := exec.CommandContext(m.ctx, args[0], args[1:]...)
	return cmd.Run()
}
