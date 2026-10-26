package clients

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"golift.io/starr"
	"golift.io/starr/sonarr"
)

const sonarrCapabilityTTL = 5 * time.Minute

type sonarrCacheEntry struct {
	client            SonarrClient
	url, key, version string
	expires           time.Time
}

// GetOrCreateSonarrClient accepts an optional explicit version for existing callers.
func (m *Manager) GetOrCreateSonarrClient(name, url, key string, versions ...string) (SonarrClient, error) {
	version := ""
	if len(versions) > 0 {
		version = versions[0]
	}
	return m.GetOrCreateSonarrClientContext(context.Background(), name, url, key, version)
}

// GetOrCreateSonarrClientContext probes only read endpoints outside the manager lock.
// Returning a fixed adapter means capability refresh never changes an in-flight operation.
func (m *Manager) GetOrCreateSonarrClientContext(ctx context.Context, name, url, key, version string) (SonarrClient, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	url = strings.TrimRight(url, "/")
	version = strings.ToLower(strings.TrimSpace(version))
	if version == "" {
		version = "auto"
	}
	flightKey := name + "\x00" + url + "\x00" + key + "\x00" + version
	result := m.sonarrProbes.DoChan(flightKey, func() (any, error) { return m.getOrCreateSonarrClientContext(ctx, name, url, key, version) })
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case outcome := <-result:
		if outcome.Err != nil {
			return nil, outcome.Err
		}
		return outcome.Val.(SonarrClient), nil
	}
}
func (m *Manager) getOrCreateSonarrClientContext(ctx context.Context, name, url, key, version string) (SonarrClient, error) {
	version = strings.ToLower(strings.TrimSpace(version))
	if version == "" {
		version = "auto"
	}
	if version != "auto" && version != "v3" && version != "v5" {
		return nil, fmt.Errorf("unsupported Sonarr API version %q", version)
	}
	url = strings.TrimRight(url, "/")
	m.mu.RLock()
	entry, ok := m.sonarrClients[name]
	m.mu.RUnlock()
	if ok && entry.url == url && entry.key == key && entry.version == version && time.Now().Before(entry.expires) {
		return entry.client, nil
	}
	client, err := selectSonarr(ctx, m.starrConfig(url, key), version)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// Concurrent probes may finish in either order; reuse the matching fresh winner.
	if current, exists := m.sonarrClients[name]; exists && current.url == url && current.key == key && current.version == version && time.Now().Before(current.expires) {
		return current.client, nil
	}
	// Invalidate future acquisitions after a confirmed unsupported collection read.
	// Numeric resource 404s are ordinary reconciliation, not capability evidence.
	invalidate := func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		if current, exists := m.sonarrClients[name]; exists && current.client == client {
			current.expires = time.Time{}
			m.sonarrClients[name] = current
		}
	}
	switch adapter := client.(type) {
	case *legacySonarr:
		adapter.APIer = &sonarrReadObserver{APIer: adapter.APIer, invalidate: invalidate}
	case *nativeSonarr:
		adapter.APIer = &sonarrReadObserver{APIer: adapter.APIer, invalidate: invalidate}
	}
	m.sonarrClients[name] = sonarrCacheEntry{client: client, url: url, key: key, version: version, expires: time.Now().Add(sonarrCapabilityTTL)}
	slog.DebugContext(ctx, "Selected Sonarr API", "instance", name, "api_version", client.APIVersion())
	return client, nil
}

func unsupportedSonarrRead(err error) bool {
	var req *starr.ReqError
	return errors.As(err, &req) && (req.Code == http.StatusNotFound || req.Code == http.StatusMethodNotAllowed)
}
func validateSonarrStatus(status *sonarr.SystemStatus, version string) error {
	if status == nil {
		return fmt.Errorf("sonarr system status is missing")
	}
	if status.AppName != "" && !strings.EqualFold(status.AppName, "Sonarr") {
		return fmt.Errorf("expected Sonarr, received %q", status.AppName)
	}
	if version == "v5" && !strings.HasPrefix(status.Version, "5.") {
		return fmt.Errorf("sonarr %s does not confirm native v5", status.Version)
	}
	return nil
}
func selectSonarr(ctx context.Context, cfg *starr.Config, version string) (SonarrClient, error) {
	version = strings.ToLower(strings.TrimSpace(version))
	if version == "" {
		version = "auto"
	}
	if version != "auto" && version != "v3" && version != "v5" {
		return nil, fmt.Errorf("unsupported Sonarr API version %q", version)
	}
	legacy := NewLegacySonarr(cfg)
	probe := func(version string) (SonarrClient, error) {
		client := legacy
		if version == "v5" {
			client = newNativeSonarr(cfg)
		}
		status, err := client.GetSystemStatusContext(ctx)
		if err != nil {
			return nil, sanitizeSonarrError(err)
		}
		if err = validateSonarrStatus(status, version); err != nil {
			return nil, err
		}
		return client, nil
	}
	if version != "auto" {
		return probe(version)
	}
	// Discovery can be absent or an HTML shell behind old reverse proxies. Only
	// unsupported/read-shape failures permit probing another read-only endpoint.
	resp, err := legacy.Get(ctx, starr.Request{URI: "/api"})
	current := ""
	if err == nil {
		defer resp.Body.Close()
		var discovery struct {
			Current   string   `json:"current"`
			Supported []string `json:"supported"`
		}
		data, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		if readErr != nil {
			return nil, readErr
		}
		if json.Unmarshal(data, &discovery) == nil {
			current = strings.ToLower(discovery.Current)
		}
	} else if !unsupportedSonarrRead(err) {
		return nil, fmt.Errorf("sonarr discovery: %w", sanitizeSonarrError(err))
	}
	if current == "v5" {
		return probe("v5")
	}
	if current == "v3" {
		return probe("v3")
	}
	status, err := legacy.GetSystemStatusContext(ctx)
	if err != nil {
		if unsupportedSonarrRead(err) {
			return probe("v5")
		}
		return nil, sanitizeSonarrError(err)
	}
	if err = validateSonarrStatus(status, "v3"); err != nil {
		return nil, err
	}
	if strings.HasPrefix(status.Version, "5.") {
		return probe("v5")
	}
	return legacy, nil
}

// sonarrReadObserver only observes reads; mutations are passed through once.
type sonarrReadObserver struct {
	starr.APIer
	invalidate func()
}

func (s *sonarrReadObserver) GetInto(ctx context.Context, r starr.Request, out any) error {
	err := s.APIer.GetInto(ctx, r, out)
	if unsupportedSonarrRead(err) {
		parts := strings.Split(strings.Trim(strings.ToLower(r.URI), "/"), "/")
		if len(parts) == 2 || (len(parts) == 3 && parts[1] == "system" && parts[2] == "status") {
			s.invalidate()
		}
	}
	return sanitizeSonarrError(err)
}

// Validation bodies can echo callback URLs containing a different application's
// API key. Preserve HTTP classification while excluding untrusted response text.
func sanitizeSonarrError(err error) error {
	var request *starr.ReqError
	if errors.As(err, &request) {
		return &starr.ReqError{Code: request.Code, Msg: "Sonarr request rejected"}
	}
	return err
}
func (s *sonarrReadObserver) PostInto(ctx context.Context, r starr.Request, out any) error {
	return sanitizeSonarrError(s.APIer.PostInto(ctx, r, out))
}
func (s *sonarrReadObserver) PutInto(ctx context.Context, r starr.Request, out any) error {
	return sanitizeSonarrError(s.APIer.PutInto(ctx, r, out))
}
func (s *sonarrReadObserver) DeleteAny(ctx context.Context, r starr.Request) error {
	return sanitizeSonarrError(s.APIer.DeleteAny(ctx, r))
}
