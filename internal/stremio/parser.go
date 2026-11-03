package stremio

import (
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

var (
	releaseExtensionRegex = regexp.MustCompile(`(?i)\.(?:nzb(?:\.gz)?|mkv|mp4|m4v|avi|mov|wmv|ts|m2ts|mpg|mpeg|webm)$`)
	// Matches season/episode patterns like S01E02, S1E2, 1x02, Season 1 Episode 2,
	// S01E01-E02, S01E01E02, 7x23-24. Seasons up to three digits are supported
	// (One Piece S004, S011-style padding).
	seasonEpRegex = regexp.MustCompile(`(?i)(?:^|[\. _\-\(\[]+)(?:s(\d{1,3})[\. _\-]?e(\d{1,3})(?:[\. _\-]*(?:e|[-~+])(\d{1,3}))?|(\d{1,3})x(\d{1,3})(?:[\. _\-]*(?:e|[-~+])(\d{1,3}))?|season[\. _\-]?(\d{1,3})[\. _\-]?episode[\. _\-]?(\d{1,3}))`)

	// Matches season packs like S01, Season 1 (without trailing episode)
	seasonPackRegex = regexp.MustCompile(`(?i)(?:^|[\. _\-\(\[]+)(?:s(\d{1,3})|season[\. _\-]?(\d{1,3}))(?:[\. _\-\)\]]+|$)`)

	seasonRangeTailRegex = regexp.MustCompile(`(?i)^[ ._]*(?:-|~|to)[ ._-]*(?:s|season[ ._-]*)?(\d{1,3})(?:[ ._()\[\]-]|$)`)

	// Matches 4-digit release year: (1990)-(2099)
	yearRegex = regexp.MustCompile(`\b(19\d{2}|20\d{2})\b`)

	// Matches common quality/source/codec tags to clean title
	qualityTagsRegex = regexp.MustCompile(`(?i)[\. _\-\(\[]+(?:2160p|1080p|1080i|720p|480p|576p|uhd|4k|bluray|blu-ray|remux|web-dl|webrip|web|hdtv|dvd|dvdr|x264|x265|h264|h265|hevc|avc|av1|hdr|hdr10|hdr10\+|dv|dolby[\. _\-]?vision|atmos|ddp5\.1|dd5\.1|aac|dts|dts-hd|truehd)`)

	// Matches a single continuation segment of an episode chain, e.g. the
	// "-03" in "S02E01-03.Shadows", the "E04" in "S01E01-E04", the "+E26" in
	// "S07E25+E26". Two forms:
	//   - attached operator: [-~+] must sit DIRECTLY after the previous number,
	//     so dated fragments like "3x05 - 2003.11.23" cannot be misread;
	//   - separator + 'e': optional punctuation then an explicit episode 'e'.
	// Anchored so segments must be contiguous with the previous one.
	episodeChainSegmentRegex = regexp.MustCompile(`(?i)^(?:[-~+]|[\. _\-]*e)[\. _\-]*(?:e[\. _\-]*)?(\d{1,3})`)

	// Leading fansub/release-group markers stripped before absolute-numbering
	// detection: "[SubsPlease] ", "(BD) ", etc.
	fansubPrefixRegex = regexp.MustCompile(`^[\[\(【][^\]\)】]*[\]\)】][\s._\-]*`)

	// Matches "Title - NN" boundaries used by absolute-numbered anime files:
	// the episode number is dash-delimited and terminated by another dash
	// segment, a technical bracket, or end of title.
	absoluteEpisodeRegex       = regexp.MustCompile(`(?:^|\s)-\s+(\d{1,4})(?:\s+-|\s*\[|\s*\(|$)`)
	absoluteEpisodePrefixRegex = regexp.MustCompile(`(?i)[ ._-]ep[ ._-]?(\d{1,4})(?:[ ._-]|$)`)
)

// ParsedRelease contains parsed metadata from a scene/P2P release title.
type ParsedRelease struct {
	SeriesTitle  string
	CleanTitle   string
	Season       int
	SeasonEnd    int
	EpisodeStart int
	EpisodeEnd   int
	Year         int
	IsSeasonPack bool
}

// ParseReleaseTitle parses a release title into its structured components.
func ParseReleaseTitle(title string) ParsedRelease {
	res := ParsedRelease{}
	clean := releaseExtensionRegex.ReplaceAllString(strings.TrimSpace(title), "")
	if clean == "" {
		return res
	}

	years := yearRegex.FindAllStringIndex(clean, -1)
	// A leading year-shaped token can be the title itself (1917, 1923).
	if len(years) > 0 && years[0][0] == 0 {
		years = years[1:]
	}

	// 1. Try matching Season & Episode
	if loc := seasonEpRegex.FindStringSubmatchIndex(clean); loc != nil {
		rawSeries := clean[:loc[0]]
		res.SeriesTitle = strings.TrimSpace(strings.Trim(rawSeries, ".-_ ()[]"))

		matches := seasonEpRegex.FindStringSubmatch(clean)
		switch {
		case len(matches) > 2 && matches[1] != "" && matches[2] != "":
			// SxxEyy form (with optional range tail)
			res.Season, _ = strconv.Atoi(matches[1])
			res.EpisodeStart, _ = strconv.Atoi(matches[2])
			res.EpisodeEnd = extendEpisodeChain(clean, loc[5], res.EpisodeStart)
		case len(matches) > 5 && matches[4] != "" && matches[5] != "":
			// NxNN form (with optional range tail)
			res.Season, _ = strconv.Atoi(matches[4])
			res.EpisodeStart, _ = strconv.Atoi(matches[5])
			res.EpisodeEnd = extendEpisodeChain(clean, loc[11], res.EpisodeStart)
		case len(matches) > 7 && matches[7] != "" && matches[8] != "":
			// "Season N Episode M" spelled-out form
			res.Season, _ = strconv.Atoi(matches[7])
			res.EpisodeStart, _ = strconv.Atoi(matches[8])
			res.EpisodeEnd = res.EpisodeStart
		default:
			res.EpisodeEnd = res.EpisodeStart
		}
	} else if loc := seasonPackRegex.FindStringSubmatchIndex(clean); loc != nil {
		// Season Pack
		rawSeries := clean[:loc[0]]
		res.SeriesTitle = strings.TrimSpace(strings.Trim(rawSeries, ".-_ ()[]"))
		res.IsSeasonPack = true

		matches := seasonPackRegex.FindStringSubmatch(clean)
		indices := seasonPackRegex.FindStringSubmatchIndex(clean)
		var seasonEnd int
		if len(matches) > 1 && matches[1] != "" {
			res.Season, _ = strconv.Atoi(matches[1])
			seasonEnd = indices[3]
		} else if len(matches) > 2 && matches[2] != "" {
			res.Season, _ = strconv.Atoi(matches[2])
			seasonEnd = indices[5]
		}
		if tail := seasonRangeTailRegex.FindStringSubmatch(clean[seasonEnd:]); len(tail) > 1 {
			end, _ := strconv.Atoi(tail[1])
			if end >= res.Season {
				res.SeasonEnd = end
			}
		}
	} else if episode, seriesPart, ok := detectAbsoluteEpisode(clean); ok {
		// Absolute-numbered anime release (e.g. "Naruto Shippuden - 107 - ...").
		// No season is inferred; MatchesSeries handles this case explicitly.
		res.SeriesTitle = seriesPart
		res.EpisodeStart = episode
		res.EpisodeEnd = episode
	} else if len(years) > 0 {
		// Movie: title is before the release year.
		rawSeries := clean[:years[0][0]]
		res.SeriesTitle = strings.TrimSpace(strings.Trim(rawSeries, ".-_ ()[]"))
	} else if loc := qualityTagsRegex.FindStringSubmatchIndex(clean); loc != nil {
		// Unstructured: title is before quality tags
		res.SeriesTitle = strings.TrimSpace(strings.Trim(clean[:loc[0]], ".-_ ()[]"))
	} else {
		res.SeriesTitle = clean
	}

	// 2. Extract Year if present
	if len(years) > 0 {
		res.Year, _ = strconv.Atoi(clean[years[0][0]:years[0][1]])
	}

	// Strip trailing year from series title if present (e.g., "The Ark (2023)" -> "The Ark")
	if res.SeriesTitle != "" {
		res.SeriesTitle = stripTitleYears(res.SeriesTitle)
		res.SeriesTitle = strings.TrimSpace(strings.Trim(res.SeriesTitle, ".-_ ()[]"))
	}

	res.CleanTitle = CleanSeriesTitle(res.SeriesTitle)
	return res
}

// looksLikeResolutionTail reports whether the digits ending at endIdx are
// immediately followed by a quality marker ('p' or 'i', as in 720p/1080i),
// optionally separated by one trailing digit of the full resolution number
// (the "0" left over when "1080" was captured as "108"). In that case the
// captured number belongs to a resolution token, not an episode range
// (e.g. the "-720"/"-108" in "S01E02-720p", "S01E01-1080p").
func looksLikeResolutionTail(title string, endIdx int) bool {
	rest := title[endIdx:]
	isMarker := func(b byte) bool { return b == 'p' || b == 'P' || b == 'i' || b == 'I' }
	switch {
	case len(rest) >= 1 && isMarker(rest[0]):
		return true
	case len(rest) >= 2 && rest[0] >= '0' && rest[0] <= '9' && isMarker(rest[1]):
		return true
	default:
		return false
	}
}

// extendEpisodeChain consumes contiguous episode continuation segments
// ("E01-E02-E03", "S07E25+E26", "S02E01-03") starting right after the primary
// episode number at from, and returns the highest valid episode number seen.
// Segments whose number would read as a resolution tail are rejected and stop
// the scan; inverted or implausibly wide (>200) spans collapse to start.
func extendEpisodeChain(title string, from, start int) int {
	end := start
	cur := from
	for i := 0; i < 50; i++ { // hard cap: chains are bounded by title length anyway
		loc := episodeChainSegmentRegex.FindStringSubmatchIndex(title[cur:])
		if loc == nil {
			break
		}
		n, err := strconv.Atoi(title[cur+loc[2] : cur+loc[3]])
		if err != nil {
			break
		}
		next := cur + loc[1]
		if looksLikeResolutionTail(title, next) {
			break
		}
		if n < start || n-start > 200 {
			// Implausible span or non-monotonic repeat: stop extending.
			break
		}
		if n > end {
			end = n
		}
		cur = next
	}
	if end < start {
		end = start
	}
	return end
}

// detectAbsoluteEpisode recognizes absolute-numbered anime releases such as
// "Naruto Shippuden - 107 - Strange Bedfellows.mkv" or
// "[SubsPlease] Digimon Adventure (2020) - 35 (720p) [...].mkv", where a bare
// episode number is dash-delimited after the show title. Leading fansub group
// markers ([Group], (Group)) are stripped from the extracted series title.
//
// It deliberately refuses:
//   - year-like numbers (1900-2099), which indicate dates rather than episodes
//   - titles containing no letters (pure numeric junk)
//   - glued forms without a dash separator (kept as a known divergence)
//
// Batch ranges ("01 ~ 12", "Cap.1905") are intentionally not handled here.
func detectAbsoluteEpisode(title string) (episode int, series string, ok bool) {
	rest := title
	for {
		m := fansubPrefixRegex.FindString(rest)
		if m == "" {
			break
		}
		rest = rest[len(m):]
	}

	loc := absoluteEpisodeRegex.FindStringSubmatchIndex(rest)
	if loc == nil {
		loc = absoluteEpisodePrefixRegex.FindStringSubmatchIndex(rest)
	}
	if loc == nil {
		return 0, "", false
	}
	n, err := strconv.Atoi(rest[loc[2]:loc[3]])
	if err != nil || (n >= 1900 && n <= 2099) {
		return 0, "", false
	}

	series = strings.TrimSpace(strings.Trim(rest[:loc[0]], ".-_ ()[]"))
	hasLetter := false
	for _, r := range series {
		if unicode.IsLetter(r) {
			hasLetter = true
			break
		}
	}
	if !hasLetter {
		return 0, "", false
	}
	return n, series, true
}

func stripTitleYears(title string) string {
	if loc := yearRegex.FindStringIndex(title); loc != nil && loc[0] == 0 {
		return title[:loc[1]] + yearRegex.ReplaceAllString(title[loc[1]:], " ")
	}
	return yearRegex.ReplaceAllString(title, " ")
}

// CleanSeriesTitle normalizes a title matching Sonarr/Prowlarr's CleanSeriesTitle:
// 1. Removes diacritics/accents
// 2. Converts to lowercase
// 3. Strips leading articles ("the", "a", "an")
// 4. Removes year if present
// 5. Removes all non-alphanumeric characters
func CleanSeriesTitle(title string) string {
	s := strings.TrimSpace(title)
	if s == "" {
		return ""
	}

	// Strip diacritics
	t := transform.Chain(norm.NFD, runes.Remove(runes.In(unicode.Mn)), norm.NFC)
	normStr, _, _ := transform.String(t, s)

	normStr = strings.ToLower(normStr)

	// Replace common separators with spaces
	for _, sep := range []string{".", "_", "-", "+", "/", "\\", ":", "(", ")", "[", "]"} {
		normStr = strings.ReplaceAll(normStr, sep, " ")
	}

	// Strip year
	normStr = stripTitleYears(strings.TrimSpace(normStr))

	// Strip leading articles
	trimmed := strings.TrimSpace(normStr)
	if strings.HasPrefix(trimmed, "the ") {
		trimmed = strings.TrimPrefix(trimmed, "the ")
	} else if strings.HasPrefix(trimmed, "a ") {
		trimmed = strings.TrimPrefix(trimmed, "a ")
	} else if strings.HasPrefix(trimmed, "an ") {
		trimmed = strings.TrimPrefix(trimmed, "an ")
	}

	// Keep only alphanumeric characters [a-z0-9]
	var b strings.Builder
	for _, r := range trimmed {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// MatchesSeries verifies if a release matches the requested TV series title and episode.
func MatchesSeries(releaseTitle, expectedTitle string, expectedSeason, expectedEpisode, expectedYear int) bool {
	if expectedTitle == "" {
		return true
	}

	parsed := ParseReleaseTitle(releaseTitle)

	// 1. Title verification (Prowlarr/Sonarr 1:1 clean title comparison)
	cleanExpected := CleanSeriesTitle(expectedTitle)
	if cleanExpected == "" || parsed.CleanTitle != cleanExpected {
		return false
	}

	// 2. Episode verification
	if expectedSeason > 0 && expectedEpisode > 0 {
		if parsed.Season > 0 {
			lastSeason := max(parsed.Season, parsed.SeasonEnd)
			if expectedSeason < parsed.Season || expectedSeason > lastSeason {
				return false
			}
			if !parsed.IsSeasonPack {
				if parsed.EpisodeStart > 0 {
					if expectedEpisode < parsed.EpisodeStart || expectedEpisode > parsed.EpisodeEnd {
						return false
					}
				}
			}
		}
	}

	// 2b. Absolute-numbered anime releases carry no season marker. Accept them
	// on exact episode-number agreement regardless of the requested season:
	// fansub numbering is relative to the franchise, not the season split used
	// by the metadata provider.
	if parsed.Season == 0 && parsed.EpisodeStart > 0 && expectedEpisode > 0 {
		return expectedEpisode >= parsed.EpisodeStart && expectedEpisode <= parsed.EpisodeEnd
	}

	// 3. Disambiguation Year verification (if both release and query specify year)
	if expectedYear > 0 && parsed.Year > 0 {
		// Allow +/- 1 year tolerance for international/theatrical broadcast dates
		if parsed.Year < expectedYear-1 || parsed.Year > expectedYear+1 {
			return false
		}
	}

	return true
}

// MatchesMovie verifies if a release matches the requested movie title and year.
func MatchesMovie(releaseTitle, expectedTitle string, expectedYear int) bool {
	if expectedTitle == "" {
		return true
	}

	parsed := ParseReleaseTitle(releaseTitle)
	cleanExpected := CleanSeriesTitle(expectedTitle)

	if cleanExpected == "" || parsed.CleanTitle != cleanExpected {
		return false
	}

	if expectedYear > 0 && parsed.Year > 0 {
		if parsed.Year < expectedYear-1 || parsed.Year > expectedYear+1 {
			return false
		}
	}

	return true
}
