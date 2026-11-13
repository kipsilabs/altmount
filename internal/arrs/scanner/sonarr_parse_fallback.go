package scanner

import (
	"context"
	"log/slog"
	"path/filepath"
	"strings"

	"github.com/kipsilabs/altmount/internal/arrs/clients"
	"github.com/kipsilabs/altmount/internal/config"
	"golift.io/starr/sonarr"
)

// Parse fallback for a Sonarr repair whose file path matches no series.
//
// Sonarr records the path of its own library entry (often a symlink into the
// mount), not the path under the mount that altmount repairs. When the series
// cannot be found by path, the repair used to give up and leave the broken
// file in place, so the episode kept pointing at a file that cannot be read
// and nobody searched for a new one. On one such library 11 of 26 files the
// repair gave up on were the file Sonarr was using.
//
// The fallback asks Sonarr to parse the release name. It acts in two cases
// only, and leaves everything else as it was:
//
//   - parseFallbackCurrentFile: the parsed episodes link to exactly one
//     episode file, and that file's sceneName is the release name (compared
//     without case). That file is the broken one, so the normal repair
//     (blocklist, delete the episode file, search) runs on it.
//   - parseFallbackMissing: none of the parsed episodes has a file. Nothing in
//     Sonarr points at the broken file, and the episodes are missing, so they
//     are searched. Nothing is blocklisted or deleted.
//
// When the episodes have a file with another sceneName (Sonarr has moved on to
// another release), or no sceneName at all, it is not safe to say which file
// is broken, and the file is left in place as before.

type parseFallbackKind int

const (
	parseFallbackNone parseFallbackKind = iota
	parseFallbackCurrentFile
	parseFallbackMissing
)

type parseFallback struct {
	kind          parseFallbackKind
	seriesID      int64
	episodeFileID int64
	sceneName     string
	episodeIDs    []int64
	reason        string
}

// releaseNameForPath returns the name the file was released under: the
// release folder for <category>/<release>/<file>, or the file name without its
// extension when the file sits directly in a category folder.
func releaseNameForPath(cfg *config.Config, filePath string) string {
	clean := filepath.ToSlash(filepath.Clean(filePath))
	dir := filepath.ToSlash(filepath.Dir(clean))
	base := filepath.Base(clean)
	stem := strings.TrimSuffix(base, filepath.Ext(base))
	if cfg != nil && cfg.IsCategoryRootDownloadPath(dir) {
		return stem
	}
	if cfg == nil {
		// Without config there is no way to tell a release folder from a
		// category folder, so only the file name is trusted.
		return stem
	}
	return filepath.Base(dir)
}

// decideSonarrParseFallback is the pure part of the fallback: given the
// release name, the episodes Sonarr parsed it to, and the series' episode
// files, it says what the repair may do.
func decideSonarrParseFallback(release string, episodes []*sonarr.Episode, files []*sonarr.EpisodeFile) parseFallback {
	if release == "" {
		return parseFallback{reason: "no release name"}
	}
	var eps []*sonarr.Episode
	for _, e := range episodes {
		if e != nil && e.ID > 0 {
			eps = append(eps, e)
		}
	}
	if len(eps) == 0 {
		return parseFallback{reason: "parse found no episodes"}
	}

	seriesID := eps[0].SeriesID
	fileIDs := map[int64]bool{}
	var episodeIDs []int64
	for _, e := range eps {
		if e.SeriesID != seriesID || seriesID <= 0 {
			return parseFallback{reason: "parse returned episodes of more than one series"}
		}
		episodeIDs = append(episodeIDs, e.ID)
		if e.EpisodeFileID > 0 {
			fileIDs[e.EpisodeFileID] = true
		} else if e.HasFile {
			// Sonarr says there is a file but gives no id: do not guess.
			return parseFallback{reason: "an episode has a file without a file id"}
		}
	}

	if len(fileIDs) == 0 {
		return parseFallback{kind: parseFallbackMissing, seriesID: seriesID, episodeIDs: episodeIDs,
			reason: "the parsed episodes have no file in Sonarr"}
	}
	if len(fileIDs) > 1 {
		return parseFallback{reason: "the parsed episodes link to more than one file"}
	}

	var fileID int64
	for id := range fileIDs {
		fileID = id
	}
	for _, f := range files {
		if f == nil || f.ID != fileID {
			continue
		}
		if f.SceneName == "" {
			return parseFallback{reason: "Sonarr's file for the episodes has no scene name"}
		}
		if !strings.EqualFold(f.SceneName, release) {
			return parseFallback{reason: "Sonarr's file for the episodes is another release"}
		}
		return parseFallback{kind: parseFallbackCurrentFile, seriesID: seriesID, episodeFileID: fileID,
			sceneName: f.SceneName, episodeIDs: episodeIDs,
			reason: "Sonarr's file for the episodes is this release"}
	}
	return parseFallback{reason: "Sonarr's file for the episodes was not found"}
}

// sonarrParseFallback runs the fallback against a Sonarr instance. Any error
// reading Sonarr gives parseFallbackNone, which keeps the old behaviour.
func (m *Manager) sonarrParseFallback(ctx context.Context, client clients.SonarrClient, instanceName, filePath string) parseFallback {
	var cfg *config.Config
	if m.configGetter != nil {
		cfg = m.configGetter()
	}
	release := releaseNameForPath(cfg, filePath)
	out, err := client.ParseContext(ctx, &sonarr.ParseInput{Title: release})
	if err != nil || out == nil {
		slog.WarnContext(ctx, "Sonarr parse fallback: parse failed, leaving file in place",
			"instance", instanceName, "release", release, "error", err)
		return parseFallback{reason: "parse failed"}
	}

	var files []*sonarr.EpisodeFile
	if len(out.Episodes) > 0 && out.Episodes[0] != nil && out.Episodes[0].SeriesID > 0 {
		files, err = m.data.GetEpisodeFiles(ctx, client, instanceName, out.Episodes[0].SeriesID)
		if err != nil {
			slog.WarnContext(ctx, "Sonarr parse fallback: could not read episode files, leaving file in place",
				"instance", instanceName, "release", release, "error", err)
			return parseFallback{reason: "episode files unreadable"}
		}
	}

	d := decideSonarrParseFallback(release, out.Episodes, files)
	slog.InfoContext(ctx, "Sonarr parse fallback",
		"instance", instanceName, "release", release, "file_path", filePath,
		"outcome", d.kind, "reason", d.reason, "series_id", d.seriesID,
		"episode_file_id", d.episodeFileID, "episode_ids", d.episodeIDs)
	return d
}

// searchSonarrMissingEpisodes searches episodes the parse fallback found
// without a file. It goes through the same failure breaker as every other
// repair search, so a release that keeps failing cannot start an endless
// search loop. Nothing is blocklisted or deleted: nothing in Sonarr points at
// the broken file.
func (m *Manager) searchSonarrMissingEpisodes(ctx context.Context, client clients.SonarrClient, instanceName string, fb parseFallback) error {
	searchIDs, giveUpIDs := m.splitEpisodesByBreaker(instanceName, fb.episodeIDs)
	unmonitorSonarrEpisodes(ctx, client, instanceName, giveUpIDs)
	if len(searchIDs) == 0 {
		return nil
	}
	response, err := m.sendSonarrSearch(ctx, client, instanceName, searchIDs, nil)
	if err != nil {
		return err
	}
	slog.InfoContext(ctx, "Sonarr parse fallback: searched episodes that have no file",
		"instance", instanceName, "series_id", fb.seriesID, "episode_ids", searchIDs,
		"command_id", response.ID)
	return nil
}
