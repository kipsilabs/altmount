package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/url"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/peernet/service"
	"github.com/kipsilabs/altmount/internal/prowlarr"
)

// PeerPlayRequest asks for a release to be imported from the peer network.
type PeerPlayRequest struct {
	ReleaseID  string
	Category   string
	Metadata   string
	DownloadID string
}

// PeerPlayResult is an imported release's files (virtual paths and sizes).
type PeerPlayResult struct {
	Paths []string
	Sizes []int64
}

// PeerPlayback is the peer network's part of the Stremio addon.
type PeerPlayback interface {
	Candidates(ctx context.Context, imdbID string, season, episode *int) []service.PeerCandidate
	HasOffer(ctx context.Context, releaseID string) bool
	Play(ctx context.Context, req PeerPlayRequest) (PeerPlayResult, error)
}

type peerPlaybackHolder struct{ p PeerPlayback }

// SetPeerPlayback installs (or, with nil, removes) the peer network's Stremio
// support. With none installed the stream list is exactly what it was without
// the peer network, the play route answers 404 and no lookup runs.
func (s *Server) SetPeerPlayback(p PeerPlayback) {
	if p == nil {
		s.peerPlayback.Store(nil)
		return
	}
	s.peerPlayback.Store(&peerPlaybackHolder{p: p})
}

const (
	// maxPeerStreams bounds the peer candidates of one stream list.
	maxPeerStreams = 10
	// peerStreamMarker is the fixed local marker of a peer candidate. Nothing a
	// peer sent is ever part of it.
	peerStreamMarker = "🌐 Peer network"
	// peerPlayTimeout bounds one peer import (fetch, probes, write), which
	// normally takes a few seconds.
	peerPlayTimeout = 2 * time.Minute
	// maxPeerTitleRunes caps a peer title before it is rendered (records cap it
	// too; this keeps the listing safe whatever the store holds).
	maxPeerTitleRunes = 200
)

var (
	peerReleaseIDRe = regexp.MustCompile(`^[0-9a-f]{64}$`)
	peerIMDbRe      = regexp.MustCompile(`^tt[0-9]{1,10}$`)
)

// peerText makes a peer-supplied title safe to show as plain text: control
// characters and line breaks are dropped and the length is capped. It is only
// ever placed inside a JSON string value, never in a URL or a path.
func peerText(s string) string {
	var b strings.Builder
	n := 0
	for _, r := range s {
		if unicode.IsControl(r) || r == '\u2028' || r == '\u2029' {
			continue
		}
		if n++; n > maxPeerTitleRunes {
			break
		}
		b.WriteRune(r)
	}
	return strings.TrimSpace(b.String())
}

// peerCandidateStreams lists the peer network's candidates for a media key as
// Stremio streams. It returns nothing with the feature off, with no season and
// episode for a series, or on any failure.
func (s *Server) peerCandidateStreams(ctx context.Context, cfg *config.Config, streamType, imdbID, baseURL, key string, season, episode int) []fiber.Map {
	h := s.peerPlayback.Load()
	if h == nil || !peerIMDbRe.MatchString(imdbID) {
		return nil
	}
	var sp, ep *int
	if streamType == "series" {
		if season <= 0 || episode <= 0 {
			return nil
		}
		sp, ep = &season, &episode
	}
	cands := h.p.Candidates(ctx, imdbID, sp, ep)
	display := resolveStreamDisplay(cfg.Stremio)
	var out []fiber.Map
	for _, c := range cands {
		if len(out) >= maxPeerStreams {
			break
		}
		title := peerText(c.Title)
		if !peerReleaseIDRe.MatchString(c.ReleaseID) || title == "" {
			continue
		}
		name, description := formatStremioStream(display, prowlarr.NZBResult{Title: title, Size: c.TotalSize, Indexer: peerStreamMarker}, false)
		description += "\n" + peerStreamMarker + " • verified on play"
		if c.Healthy {
			description += "\n" + stremioNetworkHealthyLabel
		}
		playURL := baseURL + "/stremio/" + key + "/peer/" + c.ReleaseID + "?type=" + url.QueryEscape(streamType) + "&id=" + url.QueryEscape(imdbID)
		if sp != nil {
			playURL += "&season=" + strconv.Itoa(season) + "&episode=" + strconv.Itoa(episode)
		}
		out = append(out, fiber.Map{
			"name": name, "title": description, "url": playURL,
			"behaviorHints": stremioStreamBehaviorHints{Service: "altmount", Cached: false, Filename: title, Indexer: peerStreamMarker},
		})
	}
	return out
}

const (
	// peerCandidateBudget bounds the whole peer lookup of one listing.
	peerCandidateBudget = 3 * time.Second
	// peerCandidateGrace is how long a listing waits for the peer lookup after
	// its indexer search finished: a slow or hung lookup never delays the
	// response by more than this.
	peerCandidateGrace = 250 * time.Millisecond
)

// startPeerCandidates starts the peer lookup of a listing in the background and
// returns the function that collects it. The lookup runs next to the indexer
// search; collecting waits at most peerCandidateGrace for a lookup that is still
// running and then gives up (the lookup is cancelled). It fails open: a panic or
// a slow lookup yields no peer streams, never an error or a delay. With the
// feature off nothing is started.
func (s *Server) startPeerCandidates(ctx context.Context, cfg *config.Config, streamType, imdbID, baseURL, key string, season, episode int) func() []fiber.Map {
	if s.peerPlayback.Load() == nil {
		return func() []fiber.Map { return nil }
	}
	lookupCtx, cancel := context.WithTimeout(ctx, peerCandidateBudget)
	done := make(chan []fiber.Map, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				slog.ErrorContext(ctx, "peer candidate lookup panicked", "panic", r)
				done <- nil
			}
		}()
		done <- s.peerCandidateStreams(lookupCtx, cfg, streamType, imdbID, baseURL, key, season, episode)
	}()
	return func() []fiber.Map {
		defer cancel()
		select {
		case out := <-done:
			return out
		default:
		}
		timer := time.NewTimer(peerCandidateGrace)
		defer timer.Stop()
		select {
		case out := <-done:
			return out
		case <-timer.C:
			return nil
		}
	}
}

// assembleStremioStreams merges the parts of a listing in their fixed order:
// library streams, peer streams, indexer entries. With no peer streams the
// result is exactly what a listing without the peer network contains.
func assembleStremioStreams(library, peer []fiber.Map, entries []stremioStreamEntry) []fiber.Map {
	streams := make([]fiber.Map, 0, len(library)+len(peer)+len(entries))
	streams = append(streams, library...)
	streams = append(streams, peer...)
	for _, e := range entries {
		streams = append(streams, fiber.Map{
			"name":          e.Name,
			"title":         e.Title,
			"url":           e.URL,
			"behaviorHints": e.BehaviorHints,
		})
	}
	return streams
}

// pickPeerStreamPath chooses the file to play from an imported release: the
// largest media file that is not a sample, and, for an episode, one that
// matches it.
func pickPeerStreamPath(res PeerPlayResult, selector *stremioEpisodeSelector) (string, bool) {
	best, bestSize := "", int64(-1)
	for i, p := range res.Paths {
		if !isMediaExtension(filepath.Ext(p)) || isSampleFile(p) {
			continue
		}
		if selector != nil && !selector.matches(p) {
			continue
		}
		var size int64
		if i < len(res.Sizes) {
			size = res.Sizes[i]
		}
		if size > bestSize {
			best, bestSize = p, size
		}
	}
	return best, best != ""
}

// handleStremioPeerPlay handles GET /stremio/:key/peer/:release: it imports a
// release from the peer network (fetch, verify, probes, write: a few seconds)
// and redirects to its stream like a freshly imported NZB. The route is key
// authenticated like the rest of the addon. The release must be 64 lower-case
// hex characters and one that a stored signed record offers a fetchable bundle
// for; the address dialled comes only from this node's verified peer table.
func (s *Server) handleStremioPeerPlay(c *fiber.Ctx) error {
	ctx := c.Context()
	if s.configManager == nil {
		return RespondServiceUnavailable(c, "Configuration not available", "")
	}
	cfg := s.configManager.GetConfig()
	h := s.peerPlayback.Load()
	if !isStremioEnabled(cfg) || h == nil {
		return RespondNotFound(c, "Peer network endpoint", "")
	}
	key := c.Params("key")
	if !s.validateDownloadKey(ctx, key) {
		return RespondUnauthorized(c, "Invalid key", "")
	}
	releaseID := c.Params("release")
	if !peerReleaseIDRe.MatchString(releaseID) {
		return RespondBadRequest(c, "Invalid release", "")
	}
	streamType := c.Query("type")
	imdbID := c.Query("id")
	if (streamType != "movie" && streamType != "series") || !peerIMDbRe.MatchString(imdbID) {
		return RespondBadRequest(c, "Invalid content", "")
	}
	selector := stremioEpisodeSelectorFromRequest(c)
	if streamType == "series" && selector == nil {
		return RespondBadRequest(c, "A season and an episode are required", "")
	}
	if retry, ok := s.peerLimiter().allow(key, time.Now()); !ok {
		c.Set("Retry-After", strconv.Itoa(retry))
		return RespondError(c, fiber.StatusTooManyRequests, "RATE_LIMITED", "Too many peer plays", "try again shortly")
	}
	if !h.p.HasOffer(ctx, releaseID) {
		return RespondNotFound(c, "Peer release", "")
	}

	category, season, episode := "Movies", 0, 0
	if streamType == "series" {
		category = "TV"
	}
	if selector != nil {
		season, episode = selector.Season, selector.Episode
	}
	// The media ids come from the request, not from anything verified: they
	// are kept for this node's own listing under distinct "unverified_" keys,
	// which the publisher's media-id reader does not parse, so a peer's claim
	// is never republished in this node's signed records.
	contentKey := stremioContentKey(streamType, imdbID, selector)
	data, err := json.Marshal(peerQueueMetadata{
		ContentKey: contentKey, ReleaseKey: "peernet:" + releaseID,
		UnverifiedIMDbID: imdbID, UnverifiedType: streamType, UnverifiedSeason: season, UnverifiedEpisode: episode,
	})
	if err != nil {
		return RespondInternalError(c, "Failed to prepare peer release", "")
	}
	meta := new(string)
	*meta = string(data)
	req := PeerPlayRequest{ReleaseID: releaseID, Category: category, Metadata: *meta, DownloadID: stremioDownloadIDPrefix + uuid.NewString()}
	// Concurrent plays of one release share one import; it is detached from the
	// caller so one client leaving does not abort the others' work.
	v, err, _ := s.peerPlayGroup.Do(releaseID+"|"+contentKey, func() (any, error) {
		workCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), peerPlayTimeout)
		defer cancel()
		return h.p.Play(workCtx, req)
	})
	if err != nil {
		slog.WarnContext(ctx, "Peer network play failed", "error", err)
		return RespondServiceUnavailable(c, "Failed to prepare peer release", "the release could not be verified here")
	}
	res, _ := v.(PeerPlayResult)
	path, ok := pickPeerStreamPath(res, selector)
	if !ok {
		return RespondServiceUnavailable(c, "Failed to prepare peer release", "no playable media file in the release")
	}
	baseURL := resolveBaseURL(c, cfg.Stremio.BaseURL)
	return c.Redirect(baseURL+"/api/files/stream?path="+url.QueryEscape(path)+"&download_key="+url.QueryEscape(key), fiber.StatusFound)
}

// peerPlayLimiter bounds peer plays, which trigger an outbound fetch and
// article probes: a token bucket per key and one for all keys.
type peerPlayLimiter struct {
	mu                    sync.Mutex
	keyRate, globalRate   float64 // tokens per second
	keyBurst, globalBurst float64
	keys                  map[string]*peerBucket
	global                peerBucket
}

type peerBucket struct {
	tokens float64
	at     time.Time
	init   bool
}

const maxPeerLimiterKeys = 1024

func newPeerPlayLimiter(perKeyPerMin, perKeyBurst, globalPerMin, globalBurst int) *peerPlayLimiter {
	return &peerPlayLimiter{
		keyRate: float64(perKeyPerMin) / 60, keyBurst: float64(perKeyBurst),
		globalRate: float64(globalPerMin) / 60, globalBurst: float64(globalBurst),
		keys: map[string]*peerBucket{},
	}
}

func (b *peerBucket) take(now time.Time, rate, burst float64) bool {
	if !b.init {
		b.tokens, b.at, b.init = burst, now, true
	}
	b.tokens = min(burst, b.tokens+now.Sub(b.at).Seconds()*rate)
	b.at = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// allow spends one token of the key's bucket and one of the global bucket. On
// refusal it returns the seconds to wait.
func (l *peerPlayLimiter) allow(key string, now time.Time) (retryAfter int, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.keys) >= maxPeerLimiterKeys {
		clear(l.keys)
	}
	kb := l.keys[key]
	if kb == nil {
		kb = &peerBucket{}
		l.keys[key] = kb
	}
	// Check both before spending either, so a refusal costs nothing.
	probeKey, probeGlobal := *kb, l.global
	if !probeKey.take(now, l.keyRate, l.keyBurst) {
		return max(1, int(1/max(l.keyRate, 1e-6))), false
	}
	if !probeGlobal.take(now, l.globalRate, l.globalBurst) {
		return max(1, int(1/max(l.globalRate, 1e-6))), false
	}
	*kb, l.global = probeKey, probeGlobal
	return 0, true
}

// peerLimiter returns the limiter, created with the defaults on first use: 6
// plays per minute per key (burst 3) and 20 per minute overall (burst 6).
func (s *Server) peerLimiter() *peerPlayLimiter {
	if l := s.peerPlayLimits.Load(); l != nil {
		return l
	}
	s.peerPlayLimits.CompareAndSwap(nil, newPeerPlayLimiter(6, 3, 20, 6))
	return s.peerPlayLimits.Load()
}

// peerQueueMetadata is the queue metadata of a peer play. Its media ids use
// "unverified_" keys on purpose: nothing that reads import metadata for media
// ids (the record publisher) recognises them.
type peerQueueMetadata struct {
	ContentKey        string `json:"content_key"`
	ReleaseKey        string `json:"release_key"`
	UnverifiedIMDbID  string `json:"unverified_imdb_id,omitempty"`
	UnverifiedType    string `json:"unverified_type,omitempty"`
	UnverifiedSeason  int    `json:"unverified_season,omitempty"`
	UnverifiedEpisode int    `json:"unverified_episode,omitempty"`
}
