package api

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/kipsilabs/altmount/internal/stremio"
)

// releaseJunkPattern matches releases that are not feature presentations.
var releaseJunkPattern = regexp.MustCompile(`(?i)(?:^|[^a-z0-9])(extras|sample|complete[ ._-]?blu[ ._-]?ray)(?:$|[^a-z0-9])`)

// Leading release-group and technical tags can hide the title from the parser.
// Strip only these prefixes, then apply the same title and episode checks.
var releasePrefixPattern = regexp.MustCompile(`(?i)^(?:(?:\[[^\]]+\]|\((?:bd|bluray|web-dl)\)|remux|2160p|1080p|1080i|720p|480p|576p|4k|uhd|bluray|blu-ray|web-dl|webrip|hdtv|x264|x265|hevc)[\s._-]+)+`)

func isJunkRelease(releaseName string) bool {
	return releaseJunkPattern.MatchString(releaseName)
}

// releaseMatchContext carries optional per-request metadata that widens the
// relevance gate: alternative series titles (localized names for anime) and
// the franchise-absolute episode number for shows whose releases use
// absolute numbering.
type releaseMatchContext struct {
	aliases     []string
	episodeMeta seriesEpisodeMeta
	isAnime     bool
}

// absoluteEpisode resolves the franchise-absolute number for the requested
// catalog season/episode (0 when unknown).
func (c releaseMatchContext) absoluteEpisode(season, episode int) int {
	return c.episodeMeta.absoluteFor(season, episode)
}

func releaseMatchesContent(streamType, releaseName, expectedTitle, expectedYear string, season, episode int, matchCtx ...releaseMatchContext) bool {
	var ctx releaseMatchContext
	if len(matchCtx) > 0 {
		ctx = matchCtx[0]
	}
	if strings.TrimSpace(expectedTitle) == "" {
		// Canonical metadata unavailable; fail open rather than returning
		// nothing during a metadata-provider outage.
		return true
	}
	if isJunkRelease(releaseName) {
		return false
	}

	expectedYearNum, _ := strconv.Atoi(strings.TrimSpace(expectedYear))

	titles := make([]string, 0, 1+len(ctx.aliases))
	if strings.TrimSpace(expectedTitle) != "" {
		titles = append(titles, expectedTitle)
	}
	for _, alias := range ctx.aliases {
		if strings.TrimSpace(alias) != "" {
			titles = append(titles, alias)
		}
	}

	// Try the original name as well: bracketed words can be part of a title.
	for _, name := range []string{releaseName, releasePrefixPattern.ReplaceAllString(strings.TrimSpace(releaseName), "")} {
		expectedEpisode := episode
		if parsed := stremio.ParseReleaseTitle(name); streamType == "series" && ctx.isAnime && parsed.Season == 0 && parsed.EpisodeStart > 0 {
			if absolute := ctx.absoluteEpisode(season, episode); absolute > 0 {
				expectedEpisode = absolute
			}
		}
		for _, candidate := range titles {
			if streamType == "series" {
				if stremio.MatchesSeries(name, candidate, season, expectedEpisode, expectedYearNum) {
					return true
				}
			} else if stremio.MatchesMovie(name, candidate, expectedYearNum) {
				return true
			}
		}
	}
	return false
}
