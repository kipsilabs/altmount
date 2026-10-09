package rclonecli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/httpclient"
)

// Manager handles the rclone RC server and provides mount operations
type Manager struct {
	cmd           *exec.Cmd
	rcPort        string
	rcloneDir     string
	mounts        map[string]*MountInfo
	mountsMutex   sync.RWMutex
	mountMu       sync.Mutex
	logger        *slog.Logger
	ctx           context.Context
	cancel        context.CancelFunc
	httpClient    *http.Client
	serverReady   chan struct{}
	processExited chan struct{}
	serverStarted bool
	stopping      bool
	restarting    bool
	generation    uint64
	lastStartErr  error
	mu            sync.RWMutex
	cfg           *config.Manager

	// readyAt is stamped when the RC server first becomes ready (serverReady
	// closed). Guarded by mu. Used to enforce a startup grace period before
	// the health check is allowed to kill+restart the rcd subprocess.
	readyAt time.Time

	// consecutiveProbeFailures counts back-to-back failed rcd liveness probes.
	// Mutated only by the single MonitorMounts goroutine (see monitorOnce), so
	// it needs no additional locking.
	consecutiveProbeFailures int

	// monitorOnce ensures MonitorMounts is launched at most once for the
	// lifetime of the Manager, so a restart (which calls Start again) does not
	// leak a second monitor goroutine.
	monitorOnce sync.Once

	// restartMu serializes rcd subprocess restarts so the health checker and
	// mount recovery paths cannot race each other through restartServer.
	restartMu sync.Mutex
	// recoveryMu prevents multiple health ticks from queueing stale recovery
	// operations for the same provider.
	recoveryMu sync.Mutex
	recovering map[string]struct{}

	// probe checks rcd liveness; defaults to pingServerWithTimeout. Injectable
	// for testing the health-check decision logic without a live subprocess.
	probe func(ctx context.Context, timeout time.Duration) bool

	// restart kills+respawns the rcd subprocess; defaults to restartServer.
	// Injectable so health-check tests can assert the restart decision without
	// touching a real process.
	restart func(ctx context.Context) error

	// mountFn performs a single mount attempt; defaults to performMount.
	// Injectable for testing the retry/cleanup logic without a live rcd.
	mountFn func(ctx context.Context, provider, mountPath, webdavURL string) error

	// forceUnmount force-unmounts a path via system commands; defaults to
	// forceUnmountPath. Injectable for testing.
	forceUnmount func(mountPath string) error

	// retryDelay returns the sleep between mount retry attempts; defaults to a
	// linear backoff (2s, 4s, 6s). Injectable for testing.
	retryDelay func(attempt int) time.Duration
}

type MountInfo struct {
	Provider   string `json:"provider"`
	LocalPath  string `json:"local_path"`
	WebDAVURL  string `json:"webdav_url"`
	Mounted    bool   `json:"mounted"`
	MountedAt  string `json:"mounted_at,omitempty"`
	ConfigName string `json:"config_name"`
	Error      string `json:"error,omitempty"`
	// consecutiveHealthFailures is guarded by Manager.mountsMutex. A new mount
	// instance starts with a fresh streak.
	consecutiveHealthFailures int
	desired                   bool
}

type RCRequest struct {
	Command string         `json:"command"`
	Args    map[string]any `json:"args,omitempty"`
}

type RCResponse struct {
	Result any    `json:"result,omitempty"`
	Error  string `json:"error,omitempty"`
}

// NewManager creates a new rclone RC manager
func NewManager(cfm *config.Manager) *Manager {
	cfg := cfm.GetConfig()
	logger := slog.Default().With("component", "rclone")

	rcPort := fmt.Sprintf("%d", cfg.RClone.RCPort)
	rcloneDir := filepath.Join(cfg.RClone.Path, "rclone")

	// Ensure config directory exists
	if err := os.MkdirAll(rcloneDir, 0755); err != nil {
		logger.ErrorContext(context.Background(), "Failed to create rclone config directory", "err", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	m := &Manager{
		cfg:           cfm,
		rcPort:        rcPort,
		rcloneDir:     rcloneDir,
		mounts:        make(map[string]*MountInfo),
		logger:        logger,
		ctx:           ctx,
		cancel:        cancel,
		httpClient:    httpclient.New(httpclient.WithTimeout(0)),
		serverReady:   make(chan struct{}),
		processExited: make(chan struct{}),
		recovering:    make(map[string]struct{}),
	}
	m.probe = m.pingServerWithTimeout
	m.restart = m.restartServer
	m.mountFn = m.performMount
	m.forceUnmount = m.forceUnmountPath
	m.retryDelay = func(attempt int) time.Duration {
		return time.Duration(attempt*2) * time.Second
	}
	return m
}

// Start starts the rclone RC server
func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.serverStarted {
		return nil
	}
	if m.stopping {
		return fmt.Errorf("rclone RC server is stopping")
	}
	// Every process generation owns fresh lifecycle channels. Reusing a closed
	// channel from a failed or previously stopped generation makes a retry look
	// ready before the new child has bound its port.
	m.serverReady = make(chan struct{})
	m.processExited = make(chan struct{})
	m.lastStartErr = nil
	m.readyAt = time.Time{}

	cfg := m.cfg.GetConfig()
	if !*cfg.RClone.RCEnabled {
		m.logger.InfoContext(ctx, "Rclone is disabled, skipping RC server startup")
		return nil
	}

	// Get log directory from config, fallback to current directory
	logDir := filepath.Dir(cfg.Log.File)
	if logDir == "." || logDir == "" {
		logDir = "."
	}
	logFile := filepath.Join(logDir, "rclone.log")

	// Delete old log file if it exists. If removal fails (e.g. another process
	// still holds the file open on Windows after an unclean shutdown), fall
	// back to a timestamped log file so startup is not blocked.
	if _, err := os.Stat(logFile); err == nil {
		if err := os.Remove(logFile); err != nil {
			fallback := filepath.Join(logDir, fmt.Sprintf("rclone-%d.log", time.Now().UnixNano()))
			m.logger.WarnContext(ctx, "Failed to remove old rclone log file, falling back to a new log file",
				"err", err, "original_log_file", logFile, "fallback_log_file", fallback)
			logFile = fallback
		}
	}

	args := []string{
		"rcd",
		"--rc-addr", ":" + m.rcPort,
		"--rc-no-auth", // We'll handle auth at the application level
		"--config", filepath.Join(m.rcloneDir, "rclone.conf"),
		"--log-file", logFile,
		"--use-cookies",
	}

	logLevel := cfg.RClone.LogLevel
	if logLevel != "" {
		if !slices.Contains([]string{"DEBUG", "INFO", "NOTICE", "ERROR"}, logLevel) {
			logLevel = "INFO"
		}
		args = append(args, "--log-level", logLevel)
	}

	if cfg.RClone.CacheDir != "" {
		if err := os.MkdirAll(cfg.RClone.CacheDir, 0755); err == nil {
			args = append(args, "--cache-dir", cfg.RClone.CacheDir)
		}
	}

	// Add custom RC options as flags
	for k, v := range cfg.RClone.RCOptions {
		flag := "--" + k
		if v == "true" {
			args = append(args, flag)
		} else if v != "false" {
			args = append(args, flag, v)
		}
	}

	m.logger.InfoContext(ctx, "Starting rclone RC server", "args", args)

	// Bind the subprocess to the Manager's own lifetime context rather than
	// the caller's request context: rcd must keep running even if the context
	// of the HTTP request that started (or restarted) it is cancelled.
	m.cmd = exec.CommandContext(m.ctx, "rclone", args...)
	m.lastStartErr = nil
	m.generation++
	generation := m.generation

	// Capture output for debugging
	var stdout, stderr bytes.Buffer
	m.cmd.Stdout = &stdout
	m.cmd.Stderr = &stderr

	if err := m.cmd.Start(); err != nil {
		m.logger.ErrorContext(ctx, "Failed to start rclone RC server", "stderr", stderr.String(), "stdout", stdout.String(), "err", err)
		return fmt.Errorf("failed to start rclone RC server: %w", err)
	}
	m.serverStarted = true

	// Capture references owned by this lifecycle so a future restart that
	// swaps these channels under m.mu cannot race with the goroutine below.
	cmd := m.cmd
	serverReady := m.serverReady
	processExited := m.processExited

	// Wait for server to be ready in a goroutine
	go func() {
		defer func() {
			if r := recover(); r != nil {
				m.logger.ErrorContext(m.ctx, "Panic in rclone RC server monitor", "panic", r)
			}
		}()

		readyErr := m.waitForServer()
		if readyErr != nil {
			m.logger.ErrorContext(m.ctx, "Rclone RC server not responding - mount operations will be disabled", "err", readyErr)
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
		} else {
			// Stamp readiness so the health check can enforce a startup grace
			// period before it is allowed to kill+restart the rcd subprocess.
			m.mu.Lock()
			if m.generation == generation {
				m.readyAt = time.Now()
				select {
				case <-serverReady:
				default:
					close(serverReady)
				}
			}
			m.mu.Unlock()

			if m.IsReady() {
				// Start mount monitoring once server is ready. monitorOnce guarantees a
				// single monitor for the Manager's lifetime, so a restart (which calls
				// Start again) cannot spawn a duplicate monitor goroutine.
				m.monitorOnce.Do(func() {
					go func() {
						defer func() {
							if r := recover(); r != nil {
								m.logger.ErrorContext(m.ctx, "Panic in mount monitor", "panic", r)
							}
						}()
						m.MonitorMounts(m.ctx)
					}()
				})
			}
		}

		// Wait for command to finish and log output
		err := cmd.Wait()
		if readyErr != nil {
			// Publish the startup error only after the failed subprocess has been
			// reaped, so a caller retry cannot race the old RC listener.
			m.mu.Lock()
			if m.generation == generation {
				m.lastStartErr = readyErr
				m.serverStarted = false
				select {
				case <-serverReady:
				default:
					close(serverReady)
				}
			}
			m.mu.Unlock()
		}
		// Signal that the subprocess has exited so restartServer can proceed.
		select {
		case <-processExited:
			// already closed (defensive)
		default:
			close(processExited)
		}
		switch {
		case err == nil:
			m.logger.InfoContext(m.ctx, "Rclone RC server exited normally")

		case errors.Is(err, context.Canceled):
			m.logger.InfoContext(m.ctx, "Rclone RC server terminated: context canceled")

		case WasHardTerminated(err): // SIGKILL on *nix; non-zero exit on Windows
			m.logger.InfoContext(m.ctx, "Rclone RC server hard-terminated")

		default:
			// Abnormal exit (e.g. failed to bind the RC port, immediate crash).
			// Log at WARN with captured output so a failed (re)start is visible
			// at the default INFO log level instead of being hidden behind a
			// generic "did not become ready" timeout.
			if code, ok := ExitCode(err); ok {
				m.logger.WarnContext(m.ctx, "Rclone RC server exited with error", "exit_code", code, "err", err, "stderr", stderr.String(), "stdout", stdout.String())
			} else {
				m.logger.WarnContext(m.ctx, "Rclone RC server exited with error (no exit code)", "err", err, "stderr", stderr.String(), "stdout", stdout.String())
			}
		}
	}()
	return nil
}

// Stop stops the rclone RC server and unmounts all mounts
func (m *Manager) Stop() error {
	m.mountMu.Lock()
	defer m.mountMu.Unlock()
	m.restartMu.Lock()
	defer m.restartMu.Unlock()

	m.mu.Lock()
	if !m.serverStarted {
		m.mu.Unlock()
		return nil
	}
	m.stopping = true
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.stopping = false
		m.mu.Unlock()
	}()

	m.logger.InfoContext(m.ctx, "Stopping rclone RC server")

	// Unmount all mounts first
	m.mountsMutex.RLock()
	mountList := make([]*MountInfo, 0, len(m.mounts))
	for _, mount := range m.mounts {
		if mount.Mounted {
			mountList = append(mountList, mount)
		}
	}
	m.mountsMutex.RUnlock()

	// Unmount in parallel
	var wg sync.WaitGroup
	for _, mount := range mountList {
		wg.Add(1)
		go func(mount *MountInfo) {
			defer wg.Done()
			// restartRCD is false: the rcd subprocess is terminated below,
			// which reclaims all in-process VFS state.
			if err := m.unmount(m.ctx, mount.Provider, false); err != nil {
				m.logger.ErrorContext(m.ctx, "Failed to unmount during shutdown", "err", err, "provider", mount.Provider)
			}
		}(mount)
	}

	// Wait for unmounts with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		m.logger.InfoContext(m.ctx, "All mounts unmounted successfully")
	case <-time.After(30 * time.Second):
		m.logger.WarnContext(m.ctx, "Timeout waiting for mounts to unmount, proceeding with shutdown")
	}

	m.mu.RLock()
	cmd := m.cmd
	exited := m.processExited
	m.mu.RUnlock()

	if cmd != nil && cmd.Process != nil {
		// Try graceful shutdown first
		if err := cmd.Process.Signal(os.Interrupt); err != nil {
			m.logger.WarnContext(m.ctx, "Failed to send interrupt signal, using kill", "err", err)
			if killErr := cmd.Process.Kill(); killErr != nil && !errors.Is(killErr, os.ErrProcessDone) {
				m.logger.ErrorContext(m.ctx, "Failed to kill rclone process", "err", killErr)
				return killErr
			}
		}

		// The Start monitor owns cmd.Wait and closes processExited after the
		// subprocess is reaped. Waiting on that channel avoids a double Wait.
		select {
		case <-exited:
		case <-time.After(10 * time.Second):
			m.logger.WarnContext(m.ctx, "Timeout waiting for rclone to exit, force killing")
			if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				m.logger.ErrorContext(m.ctx, "Failed to force kill rclone process", "err", err)
				return err
			}
			// Wait a bit more for the monitor goroutine to reap the process.
			select {
			case <-exited:
				m.logger.InfoContext(m.ctx, "Rclone process killed successfully")
			case <-time.After(5 * time.Second):
				m.logger.ErrorContext(m.ctx, "Process may still be running after kill")
			}
		}
	}
	// Cancel only after the graceful signal/kill path has completed. CommandContext
	// otherwise turns the intended graceful shutdown into an immediate kill.
	m.cancel()

	// Clean up any remaining mount directories
	cfg := m.cfg.GetConfig()
	if cfg.MountPath != "" {
		m.cleanupMountDirectories(cfg.MountPath)
	}

	m.mu.Lock()
	m.serverStarted = false
	m.mu.Unlock()
	m.logger.InfoContext(m.ctx, "Rclone RC server stopped")
	return nil
}

// cleanupMountDirectories removes empty mount directories
func (m *Manager) cleanupMountDirectories(_ string) {
	m.mountsMutex.RLock()
	defer m.mountsMutex.RUnlock()

	for _, mount := range m.mounts {
		if mount.LocalPath != "" {
			// Try to remove the directory if it's empty
			if err := os.Remove(mount.LocalPath); err == nil {
				m.logger.DebugContext(m.ctx, "Removed empty mount directory", "path", mount.LocalPath)
			}
			// Don't log errors here as the directory might not be empty, which is fine
		}
	}
}

// waitForServer waits for the RC server to become available. It returns an
// error when the server did not respond within the poll window so callers do
// not proceed against a dead or unresponsive rcd subprocess.
func (m *Manager) waitForServer() error {
	maxAttempts := 30
	for range maxAttempts {
		if err := m.ctx.Err(); err != nil {
			return err
		}

		if m.pingServer() {
			m.logger.InfoContext(m.ctx, "Rclone RC server is ready")
			return nil
		}

		time.Sleep(time.Second)
	}

	return fmt.Errorf("rclone RC server not responding after %d attempts", maxAttempts)
}

// pingServer checks if the RC server is responding
func (m *Manager) pingServer() bool {
	req := RCRequest{
		Command: "core/version",
		Args:    map[string]any{},
	}
	_, err := m.makeRequest(req, true)
	return err == nil
}

func (m *Manager) makeRequestWithContext(ctx context.Context, req RCRequest, close bool) (*http.Response, error) {
	reqBody, err := json.Marshal(req.Args)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	var reqCtx context.Context
	var cancel context.CancelFunc
	if close {
		reqCtx, cancel = context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
	} else {
		reqCtx = ctx
	}

	url := fmt.Sprintf("http://localhost:%s/%s", m.rcPort, req.Command)
	httpReq, err := http.NewRequestWithContext(reqCtx, "POST", url, bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := m.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		// Read the response body to get more details
		defer resp.Body.Close()
		var errorResp RCResponse
		if err := json.NewDecoder(resp.Body).Decode(&errorResp); err != nil {
			return nil, fmt.Errorf("request failed with status %s, but could not decode error response: %w", resp.Status, err)
		}
		if errorResp.Error != "" {
			return nil, fmt.Errorf("%s", errorResp.Error)
		} else {
			return nil, fmt.Errorf("request failed with status %s and no error message", resp.Status)
		}
	}

	if close {
		defer func() {
			if err := resp.Body.Close(); err != nil {
				m.logger.DebugContext(m.ctx, "Failed to close response body", "err", err)
			}
		}()
	}

	return resp, nil
}

func (m *Manager) makeRequest(req RCRequest, close bool) (*http.Response, error) {
	return m.makeRequestWithContext(m.ctx, req, close)
}

// IsReady returns true if the RC server is ready
func (m *Manager) IsReady() bool {
	m.mu.RLock()
	ready := m.serverReady
	restarting := m.restarting
	stopping := m.stopping
	started := m.serverStarted
	startErr := m.lastStartErr
	m.mu.RUnlock()

	if restarting || stopping || !started || startErr != nil {
		return false
	}

	select {
	case <-ready:
		return true
	default:
		return false
	}
}

// WaitForReady waits for the RC server to be ready
func (m *Manager) WaitForReady(timeout time.Duration) error {
	m.mu.RLock()
	ready := m.serverReady
	m.mu.RUnlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ready:
		m.mu.RLock()
		startErr := m.lastStartErr
		m.mu.RUnlock()
		if startErr != nil {
			return startErr
		}
		return nil
	case <-timer.C:
		return fmt.Errorf("timeout waiting for rclone RC server to be ready")
	case <-m.ctx.Done():
		return m.ctx.Err()
	}
}

func (m *Manager) GetLogger() *slog.Logger {
	return m.logger
}

// pingServerWithTimeout probes the rcd server with a bounded timeout so a
// wedged subprocess fails fast instead of hanging the recovery path.
// Returns true if rcd answers core/version within the timeout.
func (m *Manager) pingServerWithTimeout(ctx context.Context, timeout time.Duration) bool {
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	body, err := json.Marshal(map[string]any{})
	if err != nil {
		return false
	}

	url := fmt.Sprintf("http://localhost:%s/core/version", m.rcPort)
	req, err := http.NewRequestWithContext(probeCtx, http.MethodPost, url, bytes.NewBuffer(body))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return false
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			m.logger.DebugContext(probeCtx, "Failed to close ping response body", "err", cerr)
		}
	}()
	return resp.StatusCode == http.StatusOK
}

// waitForPortFree polls the RC port until a TCP connection can no longer be
// established (i.e. the old listener is gone) or the timeout elapses. It is a
// best-effort guard against a kill→respawn bind race; it never fails the
// restart, only logs if the port is still held when the window expires.
func (m *Manager) waitForPortFree(ctx context.Context, timeout time.Duration) {
	addr := net.JoinHostPort("localhost", m.rcPort)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return
		}
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err != nil {
			// Connection refused -> nothing listening -> port is free.
			return
		}
		_ = conn.Close()
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
	m.logger.WarnContext(ctx, "RC port still in use after waiting; restarting rcd anyway", "rc_port", m.rcPort)
}

// restartServer force-kills the rcd subprocess and starts a fresh one.
// Used by recovery paths when the rcd has wedged and is not responding to RPCs.
// The mount map is preserved; callers are expected to re-establish mounts
// against the fresh rcd via Mount(...).
func (m *Manager) restartServer(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	m.mu.RLock()
	requestedGeneration := m.generation
	m.mu.RUnlock()

	// Serialize restarts so the health checker and mount recovery cannot race
	// each other through restartServer.
	m.restartMu.Lock()
	defer m.restartMu.Unlock()

	m.mu.Lock()
	if m.generation != requestedGeneration {
		m.mu.Unlock()
		return nil
	}
	m.restarting = true
	defer func() {
		m.mu.Lock()
		m.restarting = false
		m.mu.Unlock()
	}()

	cmd := m.cmd
	exited := m.processExited
	wasStarted := m.serverStarted
	m.mu.Unlock()

	if !wasStarted || cmd == nil || cmd.Process == nil {
		return fmt.Errorf("rcd subprocess not running, cannot restart")
	}

	// The old process is about to be killed. Publish actual mount loss before
	// attempting respawn so a failed restart cannot leave callers believing the
	// FUSE mounts are still usable.
	m.mountsMutex.Lock()
	for _, mount := range m.mounts {
		mount.consecutiveHealthFailures = 0
		if mount.Mounted {
			mount.Mounted = false
			mount.Error = "rcd subprocess restarting"
		}
	}
	m.mountsMutex.Unlock()

	restartCtx, cancel := context.WithTimeout(m.ctx, 45*time.Second)
	defer cancel()

	m.logger.WarnContext(ctx, "Killing wedged rcd subprocess", "pid", cmd.Process.Pid)
	if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		m.logger.ErrorContext(ctx, "Failed to kill rcd process", "err", err)
		// Continue anyway — the Wait goroutine may still close processExited
		// if the process eventually dies on its own.
	}

	// Wait for the existing Start goroutine to observe the exit, with a hard
	// cap so a stuck Wait doesn't block recovery indefinitely.
	select {
	case <-exited:
	case <-time.After(10 * time.Second):
		m.logger.WarnContext(ctx, "Timed out waiting for rcd subprocess to exit; proceeding with restart")
	case <-restartCtx.Done():
		return restartCtx.Err()
	}

	// The killed process may not have released the RC listening socket the
	// instant Wait returned. Give it a bounded window to free the port so the
	// fresh rcd doesn't immediately fail to bind (the failure would otherwise
	// only surface as a 30s "did not become ready" timeout).
	m.waitForPortFree(restartCtx, 5*time.Second)

	// Reset lifecycle state so Start() will spawn a new subprocess.
	m.mu.Lock()
	m.serverStarted = false
	m.cmd = nil
	m.serverReady = make(chan struct{})
	m.processExited = make(chan struct{})
	m.lastStartErr = nil
	m.mu.Unlock()

	if err := m.Start(restartCtx); err != nil {
		return fmt.Errorf("failed to restart rcd: %w", err)
	}

	// Start's readiness goroutine closes serverReady before this wait returns.
	// WaitForReady must not reject that healthy generation merely because the
	// restart operation has not reached its final bookkeeping yet.
	if err := m.WaitForReady(30 * time.Second); err != nil {
		m.logger.ErrorContext(ctx, "Respawned rcd never answered core/version; see preceding rclone RC server exit logs for the cause (e.g. port still in use or stale mountpoint)",
			"rc_port", m.rcPort)
		return fmt.Errorf("rcd did not become ready after restart: %w", err)
	}

	m.logger.InfoContext(ctx, "rcd subprocess restarted successfully")
	return nil
}
