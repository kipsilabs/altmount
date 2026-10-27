package nzbfilesystem

import (
	"os"
	"strings"
	"sync"
	"time"

	"github.com/kipsilabs/altmount/internal/config"
)

type deleteGuard struct {
	mu         sync.Mutex
	categories map[string]*categoryDeletes
}

type categoryDeletes struct {
	releases    map[string]*releaseDeletes
	lastRefusal time.Time
	lastWarning time.Time
}

type releaseDeletes struct {
	pending     int
	lastSuccess time.Time
}

type deleteReservation struct {
	category *categoryDeletes
	release  string
	entry    *releaseDeletes
}

// A recursive directory delete must not bypass per-release reservations by
// targeting an unconfigured category or an ancestor of the complete directory.
func isDeleteGuardRoot(name string, cfg *config.Config) bool {
	name = strings.Trim(normalizePath(name), "/")
	base := strings.Trim(normalizePath(cfg.SABnzbd.CompleteDir), "/")
	if base != "" {
		if name == base || strings.HasPrefix(base, name+"/") {
			return true
		}
		if !strings.HasPrefix(name, base+"/") {
			return false
		}
		name = strings.TrimPrefix(name, base+"/")
	}
	return name != "" && !strings.Contains(name, "/")
}

// In-flight deletes reserve a slot so parallel walkers cannot outrun the limit.
func (g *deleteGuard) reserve(category, release string, limit int, window time.Duration, now time.Time) (*deleteReservation, bool, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.categories == nil {
		g.categories = make(map[string]*categoryDeletes)
	}
	for name, c := range g.categories {
		for r, entry := range c.releases {
			if entry.pending == 0 && (entry.lastSuccess.IsZero() || now.Sub(entry.lastSuccess) >= window) {
				delete(c.releases, r)
			}
		}
		if len(c.releases) == 0 && (c.lastRefusal.IsZero() || now.Sub(c.lastRefusal) >= window) {
			delete(g.categories, name)
		}
	}
	c := g.categories[category]
	if c == nil {
		c = &categoryDeletes{releases: make(map[string]*releaseDeletes)}
		g.categories[category] = c
	}
	entry := c.releases[release]
	if (!c.lastRefusal.IsZero() && now.Sub(c.lastRefusal) < window) || (entry == nil && len(c.releases) >= limit) {
		c.lastRefusal = now
		warn := c.lastWarning.IsZero() || now.Sub(c.lastWarning) >= time.Minute
		if warn {
			c.lastWarning = now
		}
		return nil, warn, os.ErrPermission
	}
	if entry == nil {
		entry = &releaseDeletes{}
		c.releases[release] = entry
	}
	entry.pending++
	return &deleteReservation{category: c, release: release, entry: entry}, false, nil
}

func (g *deleteGuard) finish(r *deleteReservation, success bool, now time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	r.entry.pending--
	if success {
		r.entry.lastSuccess = now
	}
	if r.entry.pending == 0 && r.entry.lastSuccess.IsZero() {
		delete(r.category.releases, r.release)
	}
}

// Paths use the component below complete_dir as category, including categories
// no longer in the current config. A loose file occupies its own release slot.
func deleteGuardPath(name string, cfg *config.Config) (category, release string) {
	name = strings.Trim(normalizePath(name), "/")
	base := strings.Trim(normalizePath(cfg.SABnzbd.CompleteDir), "/")
	if base != "" {
		if !strings.HasPrefix(name, base+"/") {
			return "", ""
		}
		name = strings.TrimPrefix(name, base+"/")
	}
	category, rest, ok := strings.Cut(name, "/")
	if !ok || category == "" || category == "." || category == ".." {
		return "", ""
	}
	release, _, _ = strings.Cut(rest, "/")
	return category, release
}
