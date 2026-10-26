package scanner

import (
	"context"
	"github.com/kipsilabs/altmount/internal/arrs/clients"
	"log/slog"

	"github.com/kipsilabs/altmount/internal/arrs/failures"
	"golift.io/starr/radarr"
)

// splitEpisodesByBreaker bumps the failure breaker for every episode about to be
// re-searched and partitions them: episodes still under the configured threshold
// are searched, episodes at/over it are given up on (returned for unmonitoring).
// With the breaker disabled (threshold 0) all episodes are searched and nothing
// is counted.
func (m *Manager) splitEpisodesByBreaker(instanceName string, episodeIDs []int64, events ...string) (search, giveUp []int64) {
	maxFailures := m.configGetter().Arrs.QueueCleanupMaxFailures
	if maxFailures <= 0 || m.failures == nil {
		return episodeIDs, nil
	}
	event := ""
	if len(events) > 0 {
		event = events[0]
	}
	seen := map[int64]bool{}
	for _, id := range episodeIDs {
		if id <= 0 || seen[id] {
			continue
		}
		seen[id] = true
		if m.failures.BumpOnce(failures.EpisodeKey(instanceName, id), event) >= maxFailures {
			giveUp = append(giveUp, id)
		} else {
			search = append(search, id)
		}
	}
	return search, giveUp
}

// movieBreakerTripped bumps the failure breaker for a movie about to be
// re-searched and reports whether it has reached the give-up threshold.
func (m *Manager) movieBreakerTripped(instanceName string, movieID int64) bool {
	maxFailures := m.configGetter().Arrs.QueueCleanupMaxFailures
	if maxFailures <= 0 || m.failures == nil {
		return false
	}
	return m.failures.Bump(failures.MovieKey(instanceName, movieID)) >= maxFailures
}

// unmonitorSonarrEpisodes stops Sonarr/Whisparr from automatically re-searching
// the given episodes after the failure breaker tripped.
func unmonitorSonarrEpisodes(ctx context.Context, client clients.SonarrClient, instanceName string, episodeIDs []int64) error {
	if len(episodeIDs) == 0 {
		return nil
	}
	if _, err := client.MonitorEpisodeContext(ctx, episodeIDs, false); err != nil {
		slog.WarnContext(ctx, "Failed to unmonitor episodes after failure threshold",
			"instance", instanceName, "episode_ids", episodeIDs, "error", err)
		return err
	}
	slog.WarnContext(ctx, "Failure threshold reached, gave up on episodes (unmonitored, no re-search)",
		"instance", instanceName, "episode_ids", episodeIDs)
	return nil
}

// unmonitorRadarrMovie stops Radarr from automatically re-searching the movie
// after the failure breaker tripped.
func unmonitorRadarrMovie(ctx context.Context, client *radarr.Radarr, instanceName string, movieID int64) {
	movie, err := client.GetMovieByIDContext(ctx, movieID)
	if err != nil {
		slog.WarnContext(ctx, "Failed to unmonitor movie after failure threshold",
			"instance", instanceName, "movie_id", movieID, "error", err)
		return
	}
	if movie.Monitored {
		movie.Monitored = false
		if _, err := client.UpdateMovieContext(ctx, movieID, movie, false); err != nil {
			slog.WarnContext(ctx, "Failed to unmonitor movie after failure threshold",
				"instance", instanceName, "movie_id", movieID, "error", err)
			return
		}
	}
	slog.WarnContext(ctx, "Failure threshold reached, gave up on movie (unmonitored, no re-search)",
		"instance", instanceName, "movie_id", movieID)
}
