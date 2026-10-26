package scanner

import (
	"context"
	"fmt"
	"github.com/kipsilabs/altmount/internal/arrs/clients"
	"log/slog"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/kipsilabs/altmount/internal/arrs/model"
	"golift.io/starr"
	"golift.io/starr/sonarr"
)

var (
	tvSeasonPattern = regexp.MustCompile(`(?i)S\d{1,4}E\d{1,4}`)
	tvDatePattern   = regexp.MustCompile(`(?i)\d{4}.\d{2}.\d{2}`)
)

// DiscoverFileMetadata attempts to find the rich metadata for a file by searching ARR instances.
// This implementation is 100% STRICT and performs zero fuzzy guessing.
func (m *Manager) DiscoverFileMetadata(ctx context.Context, filePath, relativePath, nzbName, libraryPath string) (*model.WebhookMetadata, error) {
	allInstances := m.instances.GetAllInstances()
	cleanNzbName := strings.TrimSuffix(nzbName, ".nzb")

	if cleanNzbName == "" {
		// Fallback to parent directory name if it looks like a release folder
		parentDir := filepath.Base(filepath.Dir(filePath))
		if parentDir != "." && parentDir != "tv" && parentDir != "movies" && parentDir != "/" && parentDir != "" {
			cleanNzbName = parentDir
		} else {
			// Fallback to filename without extension
			baseName := filepath.Base(filePath)
			ext := filepath.Ext(baseName)
			cleanNzbName = strings.TrimSuffix(baseName, ext)
		}
	}

	// Determine preferred type based on patterns (S01E01, etc.)
	isTV := tvSeasonPattern.MatchString(cleanNzbName) || tvSeasonPattern.MatchString(filePath) ||
		tvDatePattern.MatchString(cleanNzbName) || tvDatePattern.MatchString(filePath)

	preferredType := "radarr"
	if isTV {
		preferredType = "sonarr"
	}

	slog.DebugContext(ctx, "Strict Discovery: Starting", "nzb", cleanNzbName, "preferred_type", preferredType)

	// Strategy 1: Strict Global ID Lock (Primary Type First)
	if cleanNzbName != "" {
		// Pass 1: Preferred type
		for _, inst := range allInstances {
			if !inst.Enabled || inst.Type != preferredType {
				continue
			}

			if meta, err := m.runStrictDiscovery(ctx, inst, filePath, cleanNzbName, libraryPath); err == nil && meta != nil {
				slog.InfoContext(ctx, "Strict Discovery Success: Exact ID lock", "instance", inst.Name, "nzb", cleanNzbName, "type", inst.Type)
				return meta, nil
			}
		}

		// Pass 2: Fallback to other types (Only if preferred failed)
		for _, inst := range allInstances {
			if !inst.Enabled || inst.Type == preferredType {
				continue
			}

			if meta, err := m.runStrictDiscovery(ctx, inst, filePath, cleanNzbName, libraryPath); err == nil && meta != nil {
				slog.InfoContext(ctx, "Strict Discovery Success (Fallback Type): Exact ID lock", "instance", inst.Name, "nzb", cleanNzbName, "type", inst.Type)
				return meta, nil
			}
		}
	}

	return nil, fmt.Errorf("strict discovery failed: no exact match found for %s", filePath)
}

func (m *Manager) runStrictDiscovery(ctx context.Context, inst *model.ConfigInstance, filePath, cleanNzbName, libraryPath string) (*model.WebhookMetadata, error) {
	switch inst.Type {
	case "radarr":
		return m.discoverRadarrStrict(ctx, inst, filePath, cleanNzbName, libraryPath)
	case "sonarr", "whisparr":
		return m.discoverSonarrStrict(ctx, inst, filePath, cleanNzbName, libraryPath)
	case "lidarr":
		return m.discoverLidarrStrict(ctx, inst, filePath, cleanNzbName, libraryPath)
	case "readarr":
		return m.discoverReadarrStrict(ctx, inst, filePath, cleanNzbName, libraryPath)
	}
	return nil, fmt.Errorf("unsupported type")
}

func (m *Manager) normalizeTitle(title string) string {
	t := strings.ToLower(title)
	t = strings.ReplaceAll(t, ".", "")
	t = strings.ReplaceAll(t, " ", "")
	t = strings.ReplaceAll(t, "-", "")
	t = strings.ReplaceAll(t, "_", "")
	t = strings.ReplaceAll(t, "&", "")
	t = strings.ReplaceAll(t, "(", "")
	t = strings.ReplaceAll(t, ")", "")
	return t
}

func (m *Manager) discoverRadarrStrict(ctx context.Context, inst *model.ConfigInstance, filePath, cleanNzbName, libraryPath string) (*model.WebhookMetadata, error) {
	instanceName := inst.Name
	client, _ := m.clients.GetOrCreateRadarrClient(instanceName, inst.URL, inst.APIKey)

	// 1. Check Library (Active Files)
	movies, err := m.data.GetMovies(ctx, client, instanceName)
	if err == nil {
		normalizedNzb := m.normalizeTitle(cleanNzbName)
		normalizedPath := m.normalizeTitle(filePath)

		for _, movie := range movies {
			if movie.HasFile && movie.MovieFile != nil {
				// Optimization: only pull files for movies that might match
				normalizedTitle := m.normalizeTitle(movie.Title)
				if !strings.Contains(normalizedNzb, normalizedTitle) &&
					!strings.Contains(normalizedPath, normalizedTitle) &&
					(libraryPath == "" || !strings.Contains(libraryPath, movie.Path)) {
					continue
				}

				// Strict Match Conditions:
				// - Library Path matches
				// - OR Scene Name matches (NZB Name)
				if (libraryPath != "" && movie.MovieFile.Path == libraryPath) ||
					(cleanNzbName != "" && strings.EqualFold(movie.MovieFile.SceneName, cleanNzbName)) {
					metadata := &model.WebhookMetadata{
						EventType:    "StrictDiscovery",
						InstanceName: instanceName,
						Movie: &model.MovieMetadata{
							Id:     movie.ID,
							TmdbId: movie.TmdbID,
						},
						MovieFile: &model.MovieFileMetadata{
							Id:        movie.MovieFile.ID,
							SceneName: movie.MovieFile.SceneName,
						},
					}
					return metadata, nil
				}
			}
		}
	}

	// 2. Check History (Exact Release Name match)
	req := &starr.PageReq{PageSize: 100, SortKey: "date", SortDir: starr.SortDescend}
	history, err := client.GetHistoryPageContext(ctx, req)
	if err == nil {
		for _, record := range history.Records {
			if strings.EqualFold(record.SourceTitle, cleanNzbName) {
				var tmdbID int64
				if movies, err := m.data.GetMovies(ctx, client, instanceName); err == nil {
					for _, movie := range movies {
						if movie.ID == record.MovieID {
							tmdbID = movie.TmdbID
							break
						}
					}
				}
				metadata := &model.WebhookMetadata{
					EventType:    "StrictHistoryDiscovery",
					InstanceName: instanceName,
					Movie: &model.MovieMetadata{
						Id:     record.MovieID,
						TmdbId: tmdbID,
					},
				}
				return metadata, nil
			}
		}
	}

	return nil, fmt.Errorf("not found")
}

func (m *Manager) discoverSonarrStrict(ctx context.Context, inst *model.ConfigInstance, filePath, cleanNzbName, libraryPath string) (*model.WebhookMetadata, error) {
	// Use the already-resolved instance config (inst) directly. Both "sonarr" and
	// "whisparr" instances route here, so re-looking up by a hardcoded "sonarr"
	// type would miss whisparr instances and return a nil config, causing a
	// nil-pointer dereference. inst is guaranteed non-nil by runStrictDiscovery.
	instanceName := inst.Name
	var client clients.SonarrClient
	var clientErr error
	if inst.Type == "whisparr" {
		client, clientErr = m.clients.GetOrCreateWhisparrClient(instanceName, inst.URL, inst.APIKey)
	} else {
		client, clientErr = m.clients.GetOrCreateSonarrClientContext(ctx, instanceName, inst.URL, inst.APIKey, inst.APIVersion)
	}
	if clientErr != nil {
		return nil, clientErr
	}

	// 1. Check Library (Active Files)
	series, err := m.data.GetSeries(ctx, client, instanceName)
	if err == nil {
		normalizedNzb := m.normalizeTitle(cleanNzbName)
		normalizedPath := m.normalizeTitle(filePath)

		for _, show := range series {
			// Optimization: only pull files for shows that might contain this file
			// Use normalized titles to handle "Law.And.Order" vs "Law & Order" (laworder)
			normalizedTitle := m.normalizeTitle(show.Title)
			normalizedClean := strings.ToLower(show.CleanTitle)

			if !strings.Contains(normalizedNzb, normalizedTitle) &&
				!strings.Contains(normalizedNzb, normalizedClean) &&
				!strings.Contains(normalizedPath, normalizedTitle) &&
				!strings.Contains(normalizedPath, normalizedClean) &&
				!strings.Contains(filePath, show.Path) &&
				(libraryPath == "" || !strings.Contains(libraryPath, show.Path)) {
				continue
			}

			episodeFiles, err := m.data.GetEpisodeFiles(ctx, client, instanceName, show.ID)
			if err != nil {
				continue
			}
			for _, ef := range episodeFiles {
				if (libraryPath != "" && ef.Path == libraryPath) ||
					(cleanNzbName != "" && strings.EqualFold(ef.SceneName, cleanNzbName)) ||
					(ef.Path == filePath) {
					var episodes []model.EpisodeMetadata
					eps, err := client.GetSeriesEpisodesContext(ctx, &sonarr.GetEpisode{
						SeriesID: show.ID,
					})
					if err == nil {
						for _, ep := range eps {
							if ep.EpisodeFileID == ef.ID {
								episodes = append(episodes, model.EpisodeMetadata{
									Id: ep.ID,
								})
							}
						}
					}

					metadata := &model.WebhookMetadata{
						EventType:    "StrictDiscovery",
						InstanceName: instanceName,
						Series: &model.SeriesMetadata{
							Id:     show.ID,
							TvdbId: show.TvdbID,
						},
						EpisodeFile: &model.EpisodeFileMetadata{
							Id:        ef.ID,
							SceneName: ef.SceneName,
						},
						Episodes: episodes,
					}
					return metadata, nil
				}
			}
		}
	}

	// 2. Check History (Exact Release Name match)
	req := &starr.PageReq{PageSize: 100, SortKey: "date", SortDir: starr.SortDescend}
	history, err := client.GetHistoryPageContext(ctx, req)
	if err == nil {
		var matchedRecord *sonarr.HistoryRecord
		var episodeIDs []int64
		episodeIDMap := make(map[int64]bool)

		for _, record := range history.Records {
			if strings.EqualFold(record.SourceTitle, cleanNzbName) {
				if matchedRecord == nil {
					matchedRecord = record
				}
				if record.EpisodeID > 0 && !episodeIDMap[record.EpisodeID] {
					episodeIDMap[record.EpisodeID] = true
					episodeIDs = append(episodeIDs, record.EpisodeID)
				}
			}
		}

		if matchedRecord != nil {
			var episodes []model.EpisodeMetadata
			for _, epID := range episodeIDs {
				episodes = append(episodes, model.EpisodeMetadata{
					Id: epID,
				})
			}

			var tvdbID int64
			for _, show := range series {
				if show.ID == matchedRecord.SeriesID {
					tvdbID = show.TvdbID
					break
				}
			}

			metadata := &model.WebhookMetadata{
				EventType:    "StrictHistoryDiscovery",
				InstanceName: instanceName,
				Series: &model.SeriesMetadata{
					Id:     matchedRecord.SeriesID,
					TvdbId: tvdbID,
				},
				Episodes: episodes,
			}
			return metadata, nil
		}
	}

	return nil, fmt.Errorf("not found")
}

func (m *Manager) discoverLidarrStrict(_ context.Context, _ *model.ConfigInstance, _, _, _ string) (*model.WebhookMetadata, error) {
	return nil, fmt.Errorf("lidarr strict discovery not implemented")
}

func (m *Manager) discoverReadarrStrict(_ context.Context, _ *model.ConfigInstance, _, _, _ string) (*model.WebhookMetadata, error) {
	return nil, fmt.Errorf("readarr strict discovery not implemented")
}
