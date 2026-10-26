package clients

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/kipsilabs/altmount/internal/arrs/model"
	"golift.io/starr"
	"golift.io/starr/lidarr"
	"golift.io/starr/radarr"
	"golift.io/starr/readarr"
	"golift.io/starr/sonarr"
)

type Manager struct {
	mu              sync.RWMutex
	httpClient      *http.Client
	radarrClients   map[string]*radarr.Radarr   // key: instance name
	sonarrClients   map[string]*sonarr.Sonarr   // key: instance name
	lidarrClients   map[string]*lidarr.Lidarr   // key: instance name
	readarrClients  map[string]*readarr.Readarr // key: instance name
	whisparrClients map[string]*sonarr.Sonarr   // key: instance name
	sportarrClients map[string]*Sportarr        // key: instance name (native client)
}

// NewManager creates an arrs client manager. httpClient is shared with every
// starr.Config, so its Transport (incl. proxy) and Timeout apply to all
// Radarr/Sonarr/Lidarr/Readarr/Whisparr requests. When nil, a no-proxy 30s
// default client is used.
func NewManager(httpClient *http.Client) *Manager {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &Manager{
		httpClient:      httpClient,
		radarrClients:   make(map[string]*radarr.Radarr),
		sonarrClients:   make(map[string]*sonarr.Sonarr),
		lidarrClients:   make(map[string]*lidarr.Lidarr),
		readarrClients:  make(map[string]*readarr.Readarr),
		whisparrClients: make(map[string]*sonarr.Sonarr),
		sportarrClients: make(map[string]*Sportarr),
	}
}

// starrConfig builds a starr.Config that reuses the manager's shared
// *http.Client so proxy + timeout settings apply uniformly.
func (m *Manager) starrConfig(url, apiKey string) *starr.Config {
	return &starr.Config{URL: url, APIKey: apiKey, Client: m.httpClient}
}

// GetOrCreateRadarrClient gets or creates a Radarr client for an instance
func (m *Manager) GetOrCreateRadarrClient(instanceName, url, apiKey string) (*radarr.Radarr, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if client, exists := m.radarrClients[instanceName]; exists {
		return client, nil
	}

	client := radarr.New(m.starrConfig(url, apiKey))
	m.radarrClients[instanceName] = client
	return client, nil
}

// GetOrCreateSonarrClient gets or creates a Sonarr v4 or v5 client for an instance.
// Both versions expose the v3 API used by starr. Keep using that compatibility
// API: v5's native queue groups episodeIds, while v3 supplies per-episode records
// needed by repair and failure tracking.
func (m *Manager) GetOrCreateSonarrClient(instanceName, url, apiKey string) (*sonarr.Sonarr, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if client, exists := m.sonarrClients[instanceName]; exists {
		return client, nil
	}

	client := sonarr.New(m.starrConfig(url, apiKey))
	m.sonarrClients[instanceName] = client
	return client, nil
}

// GetOrCreateLidarrClient gets or creates a Lidarr client for an instance
func (m *Manager) GetOrCreateLidarrClient(instanceName, url, apiKey string) (*lidarr.Lidarr, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if client, exists := m.lidarrClients[instanceName]; exists {
		return client, nil
	}

	client := lidarr.New(m.starrConfig(url, apiKey))
	m.lidarrClients[instanceName] = client
	return client, nil
}

// GetOrCreateReadarrClient gets or creates a Readarr client for an instance
func (m *Manager) GetOrCreateReadarrClient(instanceName, url, apiKey string) (*readarr.Readarr, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if client, exists := m.readarrClients[instanceName]; exists {
		return client, nil
	}

	client := readarr.New(m.starrConfig(url, apiKey))
	m.readarrClients[instanceName] = client
	return client, nil
}

// GetOrCreateWhisparrClient gets or creates a Whisparr client for an instance (using Sonarr client)
func (m *Manager) GetOrCreateWhisparrClient(instanceName, url, apiKey string) (*sonarr.Sonarr, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if client, exists := m.whisparrClients[instanceName]; exists {
		return client, nil
	}

	client := sonarr.New(m.starrConfig(url, apiKey))
	m.whisparrClients[instanceName] = client
	return client, nil
}

// GetOrCreateSportarrClient gets or creates a native Sportarr client for an instance
func (m *Manager) GetOrCreateSportarrClient(instanceName, url, apiKey string) (*Sportarr, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if client, exists := m.sportarrClients[instanceName]; exists {
		return client, nil
	}

	client := NewSportarr(url, apiKey, m.httpClient)
	m.sportarrClients[instanceName] = client
	return client, nil
}

// GetOrCreateClient is a helper to get or create the appropriate client
func (m *Manager) GetOrCreateClient(instance *model.ConfigInstance) (any, error) {
	switch instance.Type {
	case "radarr":
		return m.GetOrCreateRadarrClient(instance.Name, instance.URL, instance.APIKey)
	case "sonarr":
		return m.GetOrCreateSonarrClient(instance.Name, instance.URL, instance.APIKey)
	case "lidarr":
		return m.GetOrCreateLidarrClient(instance.Name, instance.URL, instance.APIKey)
	case "readarr":
		return m.GetOrCreateReadarrClient(instance.Name, instance.URL, instance.APIKey)
	case "whisparr":
		return m.GetOrCreateWhisparrClient(instance.Name, instance.URL, instance.APIKey)
	case "sportarr":
		return m.GetOrCreateSportarrClient(instance.Name, instance.URL, instance.APIKey)
	default:
		return nil, fmt.Errorf("unsupported instance type: %s", instance.Type)
	}
}

// TestConnection tests the connection to an arrs instance
func (m *Manager) TestConnection(ctx context.Context, instanceType, url, apiKey string) error {
	switch instanceType {
	case "radarr":
		client := radarr.New(m.starrConfig(url, apiKey))
		_, err := client.GetSystemStatusContext(ctx)
		if err != nil {
			return fmt.Errorf("failed to connect to Radarr: %w", err)
		}
		return nil

	case "sonarr":
		client := sonarr.New(m.starrConfig(url, apiKey))
		_, err := client.GetSystemStatusContext(ctx)
		if err != nil {
			return fmt.Errorf("failed to connect to Sonarr: %w", err)
		}
		return nil

	case "lidarr":
		client := lidarr.New(m.starrConfig(url, apiKey))
		_, err := client.GetSystemStatusContext(ctx)
		if err != nil {
			return fmt.Errorf("failed to connect to Lidarr: %w", err)
		}
		return nil

	case "readarr":
		client := readarr.New(m.starrConfig(url, apiKey))
		_, err := client.GetSystemStatusContext(ctx)
		if err != nil {
			return fmt.Errorf("failed to connect to Readarr: %w", err)
		}
		return nil

	case "whisparr":
		client := sonarr.New(m.starrConfig(url, apiKey))
		_, err := client.GetSystemStatus()
		if err != nil {
			return fmt.Errorf("failed to connect to Whisparr: %w", err)
		}
		return nil

	case "sportarr":
		// Sportarr is not starr-compatible; use the native client's health check.
		return NewSportarr(url, apiKey, m.httpClient).Health(ctx)

	default:
		return fmt.Errorf("unsupported instance type: %s", instanceType)
	}
}
