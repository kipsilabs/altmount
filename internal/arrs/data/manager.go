package data

import (
	"context"
	"fmt"
	"github.com/kipsilabs/altmount/internal/arrs/clients"
	"log/slog"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
	"golift.io/starr/radarr"
	"golift.io/starr/sonarr"
)

const cacheTTL = 10 * time.Minute

type Manager struct {
	cacheMu           sync.RWMutex
	movieCache        map[string][]*radarr.Movie       // key: instance name
	seriesCache       map[string][]*sonarr.Series      // key: instance name
	episodeFilesCache map[string][]*sonarr.EpisodeFile // key: instance name + series id
	cacheExpiry       map[string]time.Time             // key: cache key
	requestGroup      singleflight.Group
}

func NewManager() *Manager {
	return &Manager{
		movieCache:        make(map[string][]*radarr.Movie),
		seriesCache:       make(map[string][]*sonarr.Series),
		episodeFilesCache: make(map[string][]*sonarr.EpisodeFile),
		cacheExpiry:       make(map[string]time.Time),
	}
}

// GetMovies retrieves all movies from Radarr, using a cache if available and valid
func (m *Manager) GetMovies(ctx context.Context, client *radarr.Radarr, instanceName string) ([]*radarr.Movie, error) {
	// 1. Check cache (read lock)
	m.cacheMu.RLock()
	movies, ok := m.movieCache[instanceName]
	expiry, valid := m.cacheExpiry[instanceName]
	m.cacheMu.RUnlock()

	if ok && valid && time.Now().Before(expiry) {
		slog.DebugContext(ctx, "Using cached movie list", "instance", instanceName, "count", len(movies))
		return movies, nil
	}

	// 2. Use singleflight to deduplicate requests
	key := "radarr_movies_" + instanceName
	v, err, _ := m.requestGroup.Do(key, func() (any, error) {
		// Double check cache
		m.cacheMu.RLock()
		movies, ok := m.movieCache[instanceName]
		expiry, valid := m.cacheExpiry[instanceName]
		m.cacheMu.RUnlock()
		if ok && valid && time.Now().Before(expiry) {
			return movies, nil
		}

		slog.DebugContext(ctx, "Fetching fresh movie list", "instance", instanceName)
		freshMovies, err := client.GetMovieContext(ctx, &radarr.GetMovie{})
		if err != nil {
			return nil, err
		}

		m.cacheMu.Lock()
		m.movieCache[instanceName] = freshMovies
		m.cacheExpiry[instanceName] = time.Now().Add(cacheTTL)
		m.cacheMu.Unlock()

		return freshMovies, nil
	})

	if err != nil {
		return nil, err
	}

	return v.([]*radarr.Movie), nil
}

// GetSeries retrieves all series from Sonarr, using a cache if available and valid
func (m *Manager) GetSeries(ctx context.Context, client clients.SonarrClient, instanceName string) ([]*sonarr.Series, error) {
	// 1. Check cache (read lock)
	m.cacheMu.RLock()
	series, ok := m.seriesCache[instanceName]
	expiry, valid := m.cacheExpiry[instanceName]
	m.cacheMu.RUnlock()

	if ok && valid && time.Now().Before(expiry) {
		slog.DebugContext(ctx, "Using cached series list", "instance", instanceName, "count", len(series))
		return series, nil
	}

	// 2. Use singleflight to deduplicate requests
	key := "sonarr_series_" + instanceName
	v, err, _ := m.requestGroup.Do(key, func() (any, error) {
		// Double check cache
		m.cacheMu.RLock()
		series, ok := m.seriesCache[instanceName]
		expiry, valid := m.cacheExpiry[instanceName]
		m.cacheMu.RUnlock()
		if ok && valid && time.Now().Before(expiry) {
			return series, nil
		}

		slog.DebugContext(ctx, "Fetching fresh series list", "instance", instanceName)
		freshSeries, err := client.GetAllSeriesContext(ctx)
		if err != nil {
			return nil, err
		}

		m.cacheMu.Lock()
		m.seriesCache[instanceName] = freshSeries
		m.cacheExpiry[instanceName] = time.Now().Add(cacheTTL)
		m.cacheMu.Unlock()

		return freshSeries, nil
	})

	if err != nil {
		return nil, err
	}

	return v.([]*sonarr.Series), nil
}

// GetEpisodeFiles retrieves all episode files for a series from Sonarr, using a cache if available and valid
func (m *Manager) GetEpisodeFiles(ctx context.Context, client clients.SonarrClient, instanceName string, seriesID int64) ([]*sonarr.EpisodeFile, error) {
	cacheKey := fmt.Sprintf("sonarr_episode_files_%s_%d", instanceName, seriesID)

	// 1. Check cache (read lock)
	m.cacheMu.RLock()
	files, ok := m.episodeFilesCache[cacheKey]
	expiry, valid := m.cacheExpiry[cacheKey]
	m.cacheMu.RUnlock()

	if ok && valid && time.Now().Before(expiry) {
		slog.DebugContext(ctx, "Using cached episode files list", "instance", instanceName, "series_id", seriesID, "count", len(files))
		return files, nil
	}

	// 2. Use singleflight to deduplicate requests
	v, err, _ := m.requestGroup.Do(cacheKey, func() (any, error) {
		// Double check cache
		m.cacheMu.RLock()
		files, ok := m.episodeFilesCache[cacheKey]
		expiry, valid := m.cacheExpiry[cacheKey]
		m.cacheMu.RUnlock()
		if ok && valid && time.Now().Before(expiry) {
			return files, nil
		}

		slog.DebugContext(ctx, "Fetching fresh episode files list", "instance", instanceName, "series_id", seriesID)
		freshFiles, err := client.GetSeriesEpisodeFilesContext(ctx, seriesID)
		if err != nil {
			return nil, err
		}

		m.cacheMu.Lock()
		m.episodeFilesCache[cacheKey] = freshFiles
		m.cacheExpiry[cacheKey] = time.Now().Add(cacheTTL)
		m.cacheMu.Unlock()

		return freshFiles, nil
	})

	if err != nil {
		return nil, err
	}

	return v.([]*sonarr.EpisodeFile), nil
}

// ClearMoviesCache clears the movies cache for a specific instance
func (m *Manager) ClearMoviesCache(instanceName string) {
	m.cacheMu.Lock()
	defer m.cacheMu.Unlock()
	delete(m.movieCache, instanceName)
	delete(m.cacheExpiry, instanceName)
}

// ClearSeriesCache clears the series and episode files cache for a specific instance
func (m *Manager) ClearSeriesCache(instanceName string) {
	m.cacheMu.Lock()
	defer m.cacheMu.Unlock()
	delete(m.seriesCache, instanceName)
	delete(m.cacheExpiry, instanceName)

	// Clear all episode file caches for this instance
	prefix := "sonarr_episode_files_" + instanceName + "_"
	for key := range m.episodeFilesCache {
		if strings.HasPrefix(key, prefix) {
			delete(m.episodeFilesCache, key)
			delete(m.cacheExpiry, key)
		}
	}
}
