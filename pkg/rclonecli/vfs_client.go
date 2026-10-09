package rclonecli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/kipsilabs/altmount/internal/config"
)

type RcloneRcClient interface {
	RefreshDir(ctx context.Context, provider string, dirs []string) error
	// ForgetDir drops the directories from the VFS cache without forcing an
	// eager re-listing. Use it when files were deleted or moved away: the next
	// client access re-lists lazily, whereas RefreshDir would trigger an
	// immediate enumeration nothing asked for.
	ForgetDir(ctx context.Context, provider string, dirs []string) error
}

type rcloneRcClient struct {
	config     *config.Manager
	httpClient *http.Client
}

func NewRcloneRcClient(
	config *config.Manager,
	httpClient *http.Client,
) RcloneRcClient {
	return &rcloneRcClient{
		config:     config,
		httpClient: httpClient,
	}
}

func TestConnection(
	ctx context.Context,
	rcUrl string,
	rcUser string,
	rcPass string,
	vfsName string,
	httpClient *http.Client,
) error {
	if rcUrl == "" {
		return fmt.Errorf("RC URL is not configured")
	}

	baseUrl, err := buildRCUrl(rcUrl, rcUser, rcPass)
	if err != nil {
		return fmt.Errorf("invalid RC URL configuration: %w", err)
	}

	// 1. Test basic connection with rc/noop
	req, err := http.NewRequestWithContext(ctx, "POST", baseUrl+"/rc/noop", bytes.NewBuffer([]byte("{}")))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("RC server returned status %d", resp.StatusCode)
	}

	// 2. If vfsName is provided, verify it exists using vfs/forget
	if vfsName != "" {
		data := map[string]string{
			"fs":  fmt.Sprintf("%s:", vfsName),
			"dir": "/",
		}
		payload, _ := json.Marshal(data)

		req, err = http.NewRequestWithContext(ctx, "POST", baseUrl+"/vfs/forget", bytes.NewBuffer(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err = httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("VFS %q not found or not ready: %s", vfsName, string(body))
		}
	}

	return nil
}

func (c *rcloneRcClient) RefreshDir(ctx context.Context, provider string, dirs []string) error {
	baseUrl, err := c.prepareRCRequest(provider, &dirs)
	if err != nil {
		return err
	}
	if baseUrl == "" {
		return nil // RC notifications disabled
	}

	var errs []error

	errs = append(errs, c.forgetDirs(ctx, baseUrl, provider, dirs)...)

	// Issue a vfs/refresh call for each directory to ensure all parents/children are refreshed
	for _, dir := range dirs {
		if dir == "" {
			continue
		}
		refreshArgs := map[string]any{
			"dir":       dir,
			"_async":    "false", // Run synchronously to ensure cache is ready
			"recursive": "false", // Non-recursive by default
		}
		if provider != "" {
			refreshArgs["fs"] = fmt.Sprintf("%s:", provider)
		}

		refreshPayload, err := json.Marshal(refreshArgs)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		req, err := http.NewRequestWithContext(ctx, "POST", baseUrl+"/vfs/refresh", bytes.NewBuffer(refreshPayload))
		if err != nil {
			errs = append(errs, err)
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := c.httpClient.Do(req)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			errs = append(errs, fmt.Errorf("vfs/refresh failed for %s: status %d, error: %s", dir, resp.StatusCode, string(body)))
			continue
		}
		resp.Body.Close()
	}

	if len(errs) > 0 {
		return fmt.Errorf("VFS sync completed with %d errors; first error: %w", len(errs), errs[0])
	}

	return nil
}

// ForgetDir issues only the vfs/forget half of RefreshDir. It drops the
// directories from the VFS cache without forcing an eager re-listing: the next
// client access re-lists lazily. Intended for deletions and moves, where an
// immediate re-enumeration of large directories would be wasted work.
func (c *rcloneRcClient) ForgetDir(ctx context.Context, provider string, dirs []string) error {
	baseUrl, err := c.prepareRCRequest(provider, &dirs)
	if err != nil {
		return err
	}
	if baseUrl == "" {
		return nil // RC notifications disabled
	}

	errs := c.forgetDirs(ctx, baseUrl, provider, dirs)

	if len(errs) > 0 {
		return fmt.Errorf("VFS forget completed with %d errors; first error: %w", len(errs), errs[0])
	}

	return nil
}

// prepareRCRequest validates RC configuration, defaults empty dir lists to the
// root and builds the RC base URL. It returns an empty baseUrl when RC
// notifications are disabled, meaning the caller should skip silently.
func (c *rcloneRcClient) prepareRCRequest(provider string, dirs *[]string) (string, error) {
	cfg := c.config.GetConfig()

	// Check if RC notifications are enabled
	if cfg.RClone.RCEnabled == nil || !*cfg.RClone.RCEnabled {
		return "", nil // Silently skip if RC is not enabled
	}

	// Check if RC URL is configured
	if cfg.RClone.RCUrl == "" {
		return "", fmt.Errorf("RC URL is not configured")
	}

	// If no specific directories provided, use root
	if len(*dirs) == 0 {
		*dirs = []string{"/"}
	}

	baseUrl, err := buildRCUrl(cfg.RClone.RCUrl, cfg.RClone.RCUser, cfg.RClone.RCPass)
	if err != nil {
		return "", fmt.Errorf("invalid RC URL configuration: %w", err)
	}

	return baseUrl, nil
}

// forgetDirs issues a vfs/forget call for each directory to ensure all parents/children are forgotten
func (c *rcloneRcClient) forgetDirs(ctx context.Context, baseUrl string, provider string, dirs []string) []error {
	var errs []error

	for _, dir := range dirs {
		if dir == "" {
			continue
		}
		forgetArgs := map[string]any{
			"dir": dir,
		}
		if provider != "" {
			forgetArgs["fs"] = fmt.Sprintf("%s:", provider)
		}

		forgetPayload, err := json.Marshal(forgetArgs)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		req, err := http.NewRequestWithContext(ctx, "POST", baseUrl+"/vfs/forget", bytes.NewBuffer(forgetPayload))
		if err != nil {
			errs = append(errs, err)
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := c.httpClient.Do(req)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			errs = append(errs, fmt.Errorf("vfs/forget failed for %s: status %d, error: %s", dir, resp.StatusCode, string(body)))
			continue
		}
		resp.Body.Close()
	}

	return errs
}

// buildRCUrl constructs the RC URL with proper protocol and authentication handling
func buildRCUrl(
	rcUrl string,
	rcUser string,
	rcPass string,
) (string, error) {
	rawUrl := rcUrl
	if rawUrl == "" {
		return "", fmt.Errorf("RC URL is not configured")
	}

	// Parse the URL to handle all cases properly
	parsedUrl, err := url.Parse(rawUrl)
	if err != nil {
		// If parsing fails, return the error immediately
		return "", fmt.Errorf("failed to parse RC URL %q: %w", rcUrl, err)
	}

	// If no scheme is present, or if it looks like hostname:port was parsed as scheme:opaque
	// (which happens with URLs like "example.com:8080"), add http:// and re-parse
	needsScheme := parsedUrl.Scheme == "" ||
		(parsedUrl.Host == "" && parsedUrl.Opaque != "" &&
			parsedUrl.Scheme != "http" && parsedUrl.Scheme != "https")

	if needsScheme {
		rawUrl = "http://" + rcUrl
		parsedUrl, err = url.Parse(rawUrl)
		if err != nil {
			return "", fmt.Errorf("failed to parse RC URL %q after adding http prefix: %w", rcUrl, err)
		}
	}

	// Validate scheme
	if parsedUrl.Scheme != "http" && parsedUrl.Scheme != "https" {
		return "", fmt.Errorf("unsupported RC URL scheme %q, only http and https are supported", parsedUrl.Scheme)
	}

	// Handle authentication
	if rcUser != "" && rcPass != "" {
		// Set authentication, this will override any existing userinfo
		parsedUrl.User = url.UserPassword(rcUser, rcPass)
	}

	// Ensure host is present
	if parsedUrl.Host == "" {
		return "", fmt.Errorf("RC URL must contain a valid host")
	}

	return parsedUrl.String(), nil
}
