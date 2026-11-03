package api

import (
	"context"
	"log/slog"
	"net/url"
	"path/filepath"
	"time"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/prowlarr"
)

// findCachedStremioStreams resolves completed imports to direct stream URLs.
// Cache misses and lookup failures let the caller continue with indexer search.
func (s *Server) findCachedStremioStreams(ctx context.Context, cfg *config.Config, streamType, imdbID, baseURL, key string, season, episode int) []StremioStream {
	if s.queueRepo == nil {
		return nil
	}
	items, err := s.queueRepo.GetCachedStremioQueueItems(ctx, cfg.Stremio.EffectiveIncludeLibraryStreams(), cfg.Stremio.NzbTTLHours)
	if err != nil {
		slog.WarnContext(ctx, "Failed to load cached Stremio streams; continuing with search", "error", err)
		return nil
	}
	if len(items) == 0 {
		return nil
	}
	var title, year string
	var selector *stremioEpisodeSelector
	var matchCtx releaseMatchContext
	if streamType == "series" {
		_, title, _ = resolveSeriesMetadataFromIMDb(ctx, imdbID, cfg.GetUserAgent())
		selector = &stremioEpisodeSelector{Season: season, Episode: episode}
		meta := resolveSeriesEpisodeMeta(ctx, imdbID, cfg.GetUserAgent())
		matchCtx = releaseMatchContext{aliases: resolveSeriesTitleAliases(ctx, imdbID, cfg.GetUserAgent()), episodeMeta: meta, isAnime: meta.isAnimation}
	} else {
		_, title, year, _ = resolveMovieMetadataFromIMDb(ctx, imdbID, cfg.GetUserAgent())
	}
	// Unlike ID-based indexer results, queue entries have no trusted content ID.
	// Without a title we cannot safely identify a matching cached import.
	if title == "" {
		return nil
	}

	display := resolveStreamDisplay(cfg.Stremio)
	var result []StremioStream
	seen := make(map[string]bool)
	for _, item := range items {
		if item == nil || item.StoragePath == nil || *item.StoragePath == "" {
			continue
		}
		if ttl := cfg.Stremio.NzbTTLHours; ttl > 0 && (item.CompletedAt == nil || time.Since(*item.CompletedAt) >= time.Duration(ttl)*time.Hour) {
			continue
		}
		names := []string{filepath.Base(item.NzbPath), filepath.Base(*item.StoragePath)}
		if item.RelativePath != nil {
			names = append(names, filepath.Base(*item.RelativePath))
		}
		// Identify the show/movie before traversing its media files. Episode
		// selection belongs to buildStremioStreams, including season packs.
		matchesContent := false
		for _, name := range names {
			if releaseMatchesContent(streamType, name, title, year, 0, 0, matchCtx) {
				matchesContent = true
				break
			}
		}
		if !matchesContent {
			continue
		}
		streams, err := s.buildStremioStreams(ctx, item, baseURL, key, filepath.Base(item.NzbPath), selector)
		if err != nil {
			continue
		}
		for _, stream := range streams {
			u, err := url.Parse(stream.URL)
			if err != nil || seen[stream.URL] || isSampleFile(u.Query().Get("path")) {
				continue
			}
			streamNames := append([]string{stream.Title}, names...)
			matched := false
			for _, name := range streamNames {
				if releaseMatchesContent(streamType, name, title, year, season, episode, matchCtx) {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
			release := prowlarr.NZBResult{Title: stream.Title}
			if item.FileSize != nil {
				release.Size = *item.FileSize
			}
			stream.Name, stream.Title = formatStremioStream(display, release, true)
			result = append(result, stream)
			seen[stream.URL] = true
		}
	}
	return result
}
