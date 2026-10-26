// Package failures holds the in-memory per-target failure counts behind the
// arrs circuit breaker (arrs.queue_cleanup_max_failures). The tracker is shared
// by every producer of AltMount→arr re-acquire requests — the queue-cleanup
// worker, the health-repair re-trigger and the partial-pack reconcile sweep — so
// a target (a Radarr movie, a Sonarr/Whisparr episode, ...) that keeps failing
// accumulates one combined count no matter which path acted on it. Counts are
// in-memory only: they reset on restart and when a target imports healthy.
package failures

import (
	"fmt"
	"sync"
	"time"
)

// Tracker counts failures per stable target key.
type Tracker struct {
	mu          sync.Mutex
	counts      map[string]int
	events      map[string]map[string]struct{}
	actions     map[string]bool // false: in flight; true: completed
	completedAt map[string]time.Time
	locks       map[string]*actionLock
}

// NewTracker creates an empty failure tracker.
func NewTracker() *Tracker {
	return &Tracker{counts: make(map[string]int)}
}

// Bump records one more failure-driven action against a target and returns the
// new running count. A nil tracker or empty key is a no-op returning 0.
func (t *Tracker) Bump(key string) int {
	if t == nil || key == "" {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.counts[key]++
	return t.counts[key]
}

// Reset clears a target's failure count (e.g. after it imports healthy).
func (t *Tracker) Reset(key string) {
	if t == nil || key == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.counts, key)
	delete(t.events, key)
}

// Key builders — the single source of truth for breaker key formats, so the
// queue-cleanup worker and the scanner count against the same targets. The
// sonarr format is shared by Whisparr (same client/shape); the instance name
// disambiguates.

// EpisodeKey keys a Sonarr/Whisparr episode.
func EpisodeKey(instanceName string, episodeID int64) string {
	return fmt.Sprintf("sonarr|%s|ep:%d", instanceName, episodeID)
}

// MovieKey keys a Radarr movie.
func MovieKey(instanceName string, movieID int64) string {
	return fmt.Sprintf("radarr|%s|movie:%d", instanceName, movieID)
}

// AlbumKey keys a Lidarr album.
func AlbumKey(instanceName string, albumID int64) string {
	return fmt.Sprintf("lidarr|%s|album:%d", instanceName, albumID)
}

// BookKey keys a Readarr book.
func BookKey(instanceName string, bookID int64) string {
	return fmt.Sprintf("readarr|%s|book:%d", instanceName, bookID)
}
