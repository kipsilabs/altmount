package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/httpclient"
	"golang.org/x/sync/singleflight"
)

// nzbGrabCache remembers which import an nzb_url led to, so a repeat request
// within stremio.nzb_grab_cache_hours reuses it instead of fetching the NZB from
// the indexer again. It holds only SHA-256 hashes of the URLs (indexer URLs
// carry API keys) and lives in memory: after a restart a release is fetched
// once more. The zero value is ready to use.
type nzbGrabCache struct {
	mu      sync.Mutex
	entries map[string]nzbGrab
	// fetches makes concurrent requests for the same URL share one download.
	fetches singleflight.Group
}

type nzbGrab struct {
	queueID int64
	at      time.Time
}

func nzbGrabKey(nzbURL string) string {
	sum := sha256.Sum256([]byte(nzbURL))
	return hex.EncodeToString(sum[:])
}

func (c *nzbGrabCache) get(key string, ttlHours int) (int64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	g, ok := c.entries[key]
	if !ok || time.Since(g.at) >= time.Duration(ttlHours)*time.Hour {
		return 0, false
	}
	return g.queueID, true
}

// put records the import for key and drops expired entries.
func (c *nzbGrabCache) put(key string, queueID int64, ttlHours int) {
	if ttlHours <= 0 {
		return
	}
	ttl := time.Duration(ttlHours) * time.Hour
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = make(map[string]nzbGrab)
	}
	for k, g := range c.entries {
		if time.Since(g.at) >= ttl {
			delete(c.entries, k)
		}
	}
	c.entries[key] = nzbGrab{queueID: queueID, at: time.Now()}
}

func (c *nzbGrabCache) drop(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, key)
}

// recentNzbGrab returns the import an earlier request for nzbURL led to within
// ttlHours, while it is still running or completed. Otherwise it returns nil and
// the NZB is fetched again.
func (s *Server) recentNzbGrab(ctx context.Context, ttlHours int, nzbURL string) *database.ImportQueueItem {
	if nzbURL == "" || ttlHours <= 0 || s.queueRepo == nil {
		return nil
	}
	key := nzbGrabKey(nzbURL)
	id, ok := s.nzbGrabs.get(key, ttlHours)
	if !ok {
		return nil
	}
	if item, err := s.queueRepo.GetQueueItem(ctx, id); err == nil && item != nil {
		switch item.Status {
		case database.QueueStatusPending, database.QueueStatusProcessing, database.QueueStatusPaused:
			return item
		case database.QueueStatusCompleted:
			if item.StoragePath != nil && *item.StoragePath != "" {
				return item
			}
		}
	}
	s.nzbGrabs.drop(key)
	return nil
}

// fetchNzbURL downloads an NZB from an indexer URL.
func fetchNzbURL(ctx context.Context, cfg *config.Config, nzbURL string) ([]byte, error) {
	const maxNzbFetchSize = 100 * 1024 * 1024 // 100 MB
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, nzbURL, nil)
	if err != nil {
		return nil, fmt.Errorf("invalid nzb_url: %w", err)
	}
	// Some indexers (e.g. NZBHydra2) reject requests without a browser-like User-Agent.
	req.Header.Set("User-Agent", cfg.GetUserAgent())
	resp, err := httpclient.NewForExternal(cfg.Network, httpclient.LongTimeout).Do(req) //nolint:gosec // URL is provided by an authenticated caller
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, maxNzbFetchSize))
}
