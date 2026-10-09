package prowlarr

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/kipsilabs/altmount/internal/httpclient"
	"github.com/kipsilabs/altmount/internal/regexcache"
	parsetorrentname "github.com/middelink/go-parse-torrent-name"
	"golift.io/starr"
	starrprowlarr "golift.io/starr/prowlarr"
)

// Client is a Prowlarr API client backed by golift/starr.
type Client struct {
	prowlarr *starrprowlarr.Prowlarr
	host     string
	apiKey   string
	http     *http.Client
}

// NewClient creates a new Prowlarr client. The supplied httpClient is reused
// for both the starr API and direct NZB downloads, so its Transport (incl. any
// proxy configuration) and Timeout apply to every outbound call. When nil, a
// default 30s no-proxy client is used.
func NewClient(host, apiKey string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	timeout := httpClient.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	cfg := starr.New(apiKey, strings.TrimRight(host, "/"), timeout)
	cfg.Client = httpClient
	return &Client{
		prowlarr: starrprowlarr.New(cfg),
		host:     strings.TrimRight(host, "/"),
		apiKey:   apiKey,
		http:     httpClient,
	}
}

// NZBResult represents a single search result from Prowlarr.
type NZBResult struct {
	Title       string
	DownloadURL string
	Size        int64
	PublishDate time.Time
	Indexer     string
	IndexerID   int
	Source      string
	GUID        string
}

// Indexer describes a single Prowlarr indexer, used to let users pick which
// indexers the Stremio addon should search.
type Indexer struct {
	ID       int64  `json:"id"`
	Name     string `json:"name"`
	Enable   bool   `json:"enable"`
	Protocol string `json:"protocol"`
}

// GetIndexers returns the usenet indexers configured in Prowlarr, sorted by name.
// Torrent indexers are omitted because AltMount only queues usenet releases.
func (c *Client) GetIndexers(ctx context.Context) ([]Indexer, error) {
	outputs, err := c.prowlarr.GetIndexersContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("prowlarr: get indexers failed: %w", err)
	}

	indexers := make([]Indexer, 0, len(outputs))
	for _, o := range outputs {
		if o == nil || string(o.Protocol) != "usenet" {
			continue
		}
		indexers = append(indexers, Indexer{
			ID:       o.ID,
			Name:     o.Name,
			Enable:   o.Enable,
			Protocol: string(o.Protocol),
		})
	}

	sort.Slice(indexers, func(i, j int) bool {
		return strings.ToLower(indexers[i].Name) < strings.ToLower(indexers[j].Name)
	})

	return indexers, nil
}

var (
	// reExplicitRegex detects patterns a user clearly wrote as a regex.
	//
	// Bracket, paren and brace characters are deliberately NOT part of this set:
	// release names are full of them ("[SubsPlease]", "(2020)", "{Extended}"),
	// and treating such a keyword as a regex turns "[SubsPlease]" into a
	// character class that matches nearly every title — silently blacklisting a
	// user's whole result set. Only unambiguous regex constructs qualify:
	// escape classes, group directives, alternation, quantifiers and anchors.
	// Anything else is matched literally on token boundaries; users who want a
	// regex containing only brackets can use the explicit /pattern/ form.
	//
	// Kept in lockstep with REGEX_CONSTRUCTS in
	// frontend/src/components/config/stremio/scoringPresets.ts.
	reExplicitRegex = regexp.MustCompile(`\\b|\\[dwsDWS]|\(\?|[|*+?^$]`)
	reWhitespace    = regexp.MustCompile(`\s+`)
)

// getCompiledRegex returns the pattern from the shared bounded regex cache.
func getCompiledRegex(pattern string) (*regexp.Regexp, error) {
	return regexcache.Get(pattern)
}

// slashPatternExpr builds a case-insensitive (by default) regex expression
// from a slash-delimited pattern body and its trailing flags. Only the Go
// supported inline flags i, m, and s are honored; unknown flag letters are
// ignored, mirroring the JavaScript implementation's leniency.
func slashPatternExpr(raw, flags string) string {
	var b strings.Builder
	b.WriteString("(?i")
	for _, f := range flags {
		if f == 'm' || f == 's' {
			b.WriteRune(f)
		}
	}
	b.WriteString(")")
	return b.String() + raw
}

// isExplicitRegex reports whether the given pattern contains regex metacharacters or directives.
func isExplicitRegex(pattern string) bool {
	return reExplicitRegex.MatchString(pattern)
}

// BuildKeywordRegex constructs a regex pattern that matches a keyword phrase
// on token/word boundaries (separated by delimiters like ., _, -, spaces, brackets, or start/end of string).
func BuildKeywordRegex(keyword string) string {
	clean := strings.TrimSpace(keyword)
	clean = strings.Trim(clean, "._- \t")
	if clean == "" {
		return ""
	}

	parts := reWhitespace.Split(clean, -1)
	escapedParts := make([]string, len(parts))
	for i, p := range parts {
		escapedParts[i] = regexp.QuoteMeta(p)
	}
	body := strings.Join(escapedParts, `[ ._\-]+`)

	return `(?i)(?:^|[^a-zA-Z0-9])` + body + `(?:[^a-zA-Z0-9]|$)`
}

// MatchKeywordOrPattern matches a release title against either an explicit regex pattern or a token keyword.
func MatchKeywordOrPattern(title, pattern string) bool {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" || title == "" {
		return false
	}

	// 1. Explicit slash-delimited regex: /pattern/ or /pattern/flags.
	// A structurally valid slash pattern never falls through to keyword
	// matching; an invalid body simply fails to match (parity with the
	// JavaScript implementation in scoringPresets.ts).
	if strings.HasPrefix(pattern, "/") && len(pattern) >= 2 {
		lastSlash := strings.LastIndex(pattern, "/")
		if lastSlash > 0 {
			expr := slashPatternExpr(pattern[1:lastSlash], pattern[lastSlash+1:])
			if re, err := getCompiledRegex(expr); err == nil && re != nil {
				return re.MatchString(title)
			}
			return false
		}
	}

	// 2. Explicit regex pattern (e.g. \b(cam|ts)\b)
	if isExplicitRegex(pattern) {
		if re, err := getCompiledRegex(pattern); err == nil && re != nil {
			return re.MatchString(title)
		}
	}

	// 3. Plain keyword / phrase: match with boundary delimiters
	tokenPattern := BuildKeywordRegex(pattern)
	if tokenPattern == "" {
		return false
	}
	if re, err := getCompiledRegex(tokenPattern); err == nil && re != nil {
		return re.MatchString(title)
	}

	return false
}

// matchesAnyKeyword returns true when title matches at least one of the
// keywords or patterns (case-insensitive on token boundaries). Returns true when keywords is empty (no filter).
func matchesAnyKeyword(title string, keywords []string) bool {
	if len(keywords) == 0 {
		return true
	}
	for _, kw := range keywords {
		if MatchKeywordOrPattern(title, kw) {
			return true
		}
	}
	return false
}

// MatchesLanguage returns true when title contains at least one of the language
// keywords (case-insensitive). Returns true when keywords is empty (no filter).
func MatchesLanguage(title string, keywords []string) bool {
	return matchesAnyKeyword(title, keywords)
}

// MatchesQuality returns true when title contains at least one of the quality
// keywords (case-insensitive). Returns true when keywords is empty (no filter).
func MatchesQuality(title string, keywords []string) bool {
	return matchesAnyKeyword(title, keywords)
}

// MatchesExcludeKeywords reports whether title contains any excluded keyword or pattern.
func MatchesExcludeKeywords(title string, excludeKeywords []string) bool {
	if len(excludeKeywords) == 0 {
		return false
	}
	for _, kw := range excludeKeywords {
		if MatchKeywordOrPattern(title, kw) {
			return true
		}
	}
	return false
}

// InferLanguage detects the most likely language from a release title using common scene/group
// naming conventions. Returns a short label like "Spanish", "French", or "" when not detected.
// PTN's language support is limited for European releases so we use custom keyword matching here.
func InferLanguage(title string) string {
	lower := strings.ToLower(title)

	type langRule struct {
		label    string
		keywords []string
	}
	rules := []langRule{
		{"Multi", []string{"multi", "multilingual", "multi.lang"}},
		{"Dual Audio", []string{"dual.audio", "dual audio", "dual-audio"}},
		{"Dual", []string{".dual.", " dual ", "-dual-"}},
		{"Spanish", []string{"spanish", ".esp.", " esp ", "-esp-", ".spa.", " spa ", "castellano", "cast.", " cast ", "🇪🇸"}},
		{"French", []string{"french", ".fre.", " fre ", ".vf.", " vf ", "vostfr", ".fr.", "🇫🇷"}},
		{"German", []string{"german", ".ger.", " ger ", ".de.", "deutsch", "🇩🇪"}},
		{"Italian", []string{"italian", ".ita.", " ita ", "🇮🇹"}},
		{"Portuguese", []string{"portuguese", ".por.", " por ", ".pt.", "pt-br", ".ptbr.", "🇧🇷", "🇵🇹"}},
		{"Japanese", []string{"japanese", ".jpn.", " jpn ", ".ja.", "🇯🇵"}},
		{"Korean", []string{"korean", ".kor.", " kor ", ".ko.", "🇰🇷"}},
		{"Chinese", []string{"chinese", ".chi.", " chi ", ".zh.", "🇨🇳", "🇹🇼"}},
		{"Russian", []string{"russian", ".rus.", " rus ", ".ru.", "🇷🇺"}},
		{"English", []string{"english", ".eng.", " eng "}},
	}
	for _, r := range rules {
		for _, kw := range r.keywords {
			if strings.Contains(lower, kw) {
				return r.label
			}
		}
	}
	return ""
}

// ReleaseMeta holds inferred metadata from a release title.
type ReleaseMeta struct {
	Language     string // "Spanish", "French", etc.
	FlagEmoji    string // "🇪🇸", "🇫🇷", etc. (empty for English/unknown)
	LangCode     string // "Esp", "Fra", "Deu", etc. (empty if unknown)
	QualityLabel string // "4K", "FHD", "HD", "SD", or quality string (e.g. "WEB-DL")
	Resolution   string // from PTN: "720p", "1080p", "2160p"
	Quality      string // from PTN: "WEB-DL", "BluRay", etc.
	Codec        string // from PTN: "x264", "x265", "HEVC", etc.
	Audio        string // from PTN: "AAC", "DTS", etc.
	ParsedTitle  string // PTN-parsed title (e.g. "La película")
	Year         int    // PTN-parsed year
}

var langFlags = map[string]string{
	"Spanish":    "🇪🇸",
	"French":     "🇫🇷",
	"German":     "🇩🇪",
	"Italian":    "🇮🇹",
	"Portuguese": "🇵🇹",
	"Japanese":   "🇯🇵",
	"Korean":     "🇰🇷",
	"Chinese":    "🇨🇳",
	"Russian":    "🇷🇺",
	"English":    "🇬🇧",
	"Multi":      "🌍",
	"Dual Audio": "🌍",
	"Dual":       "🌍",
}

var langCodes = map[string]string{
	"Spanish":    "Esp",
	"French":     "Fra",
	"German":     "Deu",
	"Italian":    "Ita",
	"Portuguese": "Por",
	"Japanese":   "Jpn",
	"Korean":     "Kor",
	"Chinese":    "Chi",
	"Russian":    "Rus",
	"English":    "Eng",
	"Multi":      "Multi",
	"Dual Audio": "Dual",
	"Dual":       "Dual",
}

func resolutionLabel(res string) string {
	switch res {
	case "2160p":
		return "4K"
	case "1080p":
		return "FHD"
	case "720p":
		return "HD"
	case "480p", "576p":
		return "SD"
	default:
		return ""
	}
}

// InferReleaseMeta parses a release title and returns detected metadata.
// Uses PTN for quality/resolution/codec/audio and custom logic for language.
func InferReleaseMeta(title string) ReleaseMeta {
	info := parseTorrentName(title)
	meta := ReleaseMeta{
		Language: InferLanguage(title),
	}
	if info != nil {
		meta.Resolution = info.Resolution
		meta.Quality = info.Quality
		meta.Codec = info.Codec
		meta.Audio = info.Audio
		meta.ParsedTitle = info.Title
		meta.Year = info.Year
		if meta.Language == "" && info.Language != "" {
			meta.Language = info.Language
		}
	}
	meta.FlagEmoji = langFlags[meta.Language]
	meta.LangCode = langCodes[meta.Language]
	meta.QualityLabel = resolutionLabel(meta.Resolution)
	if meta.QualityLabel == "" {
		meta.QualityLabel = meta.Quality
	}
	return meta
}

// parseTorrentName isolates the third-party parser's unchecked title slicing.
// Overlapping matches (e.g. "[ABC 1080p]") can put its start after its end.
// Display metadata is optional: retain the release and independently inferred
// language when parsing fails; stream entries fall back to the original title.
func parseTorrentName(title string) (info *parsetorrentname.TorrentInfo) {
	defer func() {
		if recover() != nil {
			info = nil
		}
	}()
	info, _ = parsetorrentname.Parse(title)
	return info
}

// Search queries Prowlarr for NZB releases matching the given IMDB ID and categories.
// searchType should be "movie", "tvsearch", or "search".
// season and episode are optional (pass 0 to omit); used for tvsearch to scope results to a specific episode.
// indexers optionally restricts the search to specific indexer IDs (empty = all indexers).
// Results are returned sorted by publish date descending (newest first).
func (c *Client) Search(ctx context.Context, imdbID, searchType string, categories, indexers []int, season, episode int) ([]NZBResult, error) {
	return c.searchWithID(ctx, "ImdbId", imdbID, searchType, categories, indexers, season, episode)
}

// SearchByTVDB queries Prowlarr for NZB releases using TVDB ID and categories.
// This is primarily used by TV series lookups when indexers support TvdbId but not ImdbId.
// indexers optionally restricts the search to specific indexer IDs (empty = all indexers).
func (c *Client) SearchByTVDB(ctx context.Context, tvdbID, searchType string, categories, indexers []int, season, episode int) ([]NZBResult, error) {
	return c.searchWithID(ctx, "TvdbId", tvdbID, searchType, categories, indexers, season, episode)
}

// SearchByQuery queries Prowlarr for NZB releases using free-text query and categories.
func (c *Client) SearchByQuery(ctx context.Context, queryText, searchType string, categories, indexers []int, season, episode int) ([]NZBResult, error) {
	var query strings.Builder
	if queryText != "" {
		query.WriteString(queryText)
	}
	if season > 0 {
		query.WriteString(" {Season:" + strconv.Itoa(season) + "}")
	}
	if episode > 0 {
		query.WriteString(" {Episode:" + strconv.Itoa(episode) + "}")
	}

	cats := make([]int64, len(categories))
	for i, cat := range categories {
		cats[i] = int64(cat)
	}

	idxs := make([]int64, len(indexers))
	for i, idx := range indexers {
		idxs[i] = int64(idx)
	}

	input := starrprowlarr.SearchInput{
		Query:      strings.TrimSpace(query.String()),
		Type:       searchType,
		Categories: cats,
		IndexerIDs: idxs,
	}

	releases, err := c.prowlarr.SearchContext(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("prowlarr: search failed: %w", err)
	}

	results := make([]NZBResult, 0, len(releases))
	for _, r := range releases {
		if r.DownloadURL == "" || r.Protocol != "usenet" {
			continue
		}
		results = append(results, NZBResult{
			Title:       r.Title,
			DownloadURL: r.DownloadURL,
			Size:        r.Size,
			PublishDate: r.PublishDate,
			Indexer:     r.Indexer,
			IndexerID:   int(r.IndexerID),
			Source:      "prowlarr",
			GUID:        r.GUID,
		})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].PublishDate.After(results[j].PublishDate)
	})

	return results, nil
}

func (c *Client) searchWithID(ctx context.Context, idField, idValue, searchType string, categories, indexers []int, season, episode int) ([]NZBResult, error) {
	if strings.EqualFold(idField, "ImdbId") {
		idValue = strings.TrimPrefix(idValue, "tt")
	}

	var query strings.Builder
	if idValue != "" {
		query.WriteString("{" + idField + ":" + idValue + "}")
	}
	if season > 0 {
		query.WriteString("{Season:" + strconv.Itoa(season) + "}")
	}
	if episode > 0 {
		query.WriteString("{Episode:" + strconv.Itoa(episode) + "}")
	}

	cats := make([]int64, len(categories))
	for i, cat := range categories {
		cats[i] = int64(cat)
	}

	idxs := make([]int64, len(indexers))
	for i, idx := range indexers {
		idxs[i] = int64(idx)
	}

	input := starrprowlarr.SearchInput{
		Query:      query.String(),
		Type:       searchType,
		Categories: cats,
		IndexerIDs: idxs,
	}

	releases, err := c.prowlarr.SearchContext(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("prowlarr: search failed: %w", err)
	}

	results := make([]NZBResult, 0, len(releases))
	for _, r := range releases {
		if r.DownloadURL == "" || r.Protocol != "usenet" {
			continue
		}
		results = append(results, NZBResult{
			Title:       r.Title,
			DownloadURL: r.DownloadURL,
			Size:        r.Size,
			PublishDate: r.PublishDate,
			Indexer:     r.Indexer,
			IndexerID:   int(r.IndexerID),
			Source:      "prowlarr",
			GUID:        r.GUID,
		})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].PublishDate.After(results[j].PublishDate)
	})

	return results, nil
}

// DownloadNZB fetches the NZB file content from the given Prowlarr download URL or direct indexer URL.
//
// Security contract: direct indexer URLs are checked with
// httpclient.ValidateDownloadURL (literal-IP private/link-local blocking only;
// DNS hostnames NOT resolved — see ValidateDownloadURL). URLs targeting the
// configured Prowlarr host skip that initial check because the host is
// operator-configured and commonly a private/LAN address; treating it as
// untrusted indexer input would break legitimate deployments. Every redirect
// hop on BOTH paths is re-validated per hop via SafeDownloadCheckRedirect
// (same literal-IP-only rule, DNS hostnames NOT resolved), and the Prowlarr
// API key is sent only to the Prowlarr host — cross-host redirect handling
// governs credentials, not the destination allowlist.
func (c *Client) DownloadNZB(ctx context.Context, downloadURL string) ([]byte, error) {
	reqURL, err := url.Parse(downloadURL)
	if err != nil {
		return nil, fmt.Errorf("prowlarr: invalid download URL: %w", err)
	}

	prowlarrHostURL, _ := url.Parse(c.host)
	isProwlarrHost := prowlarrHostURL != nil && prowlarrHostURL.Host != "" &&
		strings.EqualFold(prowlarrHostURL.Scheme, reqURL.Scheme) &&
		strings.EqualFold(prowlarrHostURL.Host, reqURL.Host)

	if !isProwlarrHost {
		// Literal-IP-only check; DNS hostnames are NOT resolved (see
		// httpclient.ValidateDownloadURL).
		if err := httpclient.ValidateDownloadURL(downloadURL); err != nil {
			return nil, fmt.Errorf("prowlarr: refusing download: %w", err)
		}
	}
	// NOTE: the Prowlarr-host path intentionally skips the initial check above:
	// the host is operator-configured (often a LAN/private address), so the
	// literal-IP rule cannot apply to it. Redirect hops on this path are still
	// re-validated per hop via SafeDownloadCheckRedirect below.

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return nil, fmt.Errorf("prowlarr: create download request: %w", err)
	}
	if isProwlarrHost && c.apiKey != "" {
		req.Header.Set("X-Api-Key", c.apiKey)
	}

	client := *c.http
	client.CheckRedirect = httpclient.SafeDownloadCheckRedirect(10)

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("prowlarr: download request failed: %w", httpclient.RedactURLError(err))
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("prowlarr: download returned status %d: %s", resp.StatusCode, string(body))
	}

	return io.ReadAll(io.LimitReader(resp.Body, 50*1024*1024))
}
