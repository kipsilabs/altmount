// Package providerbackbone fetches and caches the public dataset that maps
// usenet hostnames to the upstream backbone they run on. The backbone name is
// a suitable value for a provider's storage_group setting: providers sharing a
// backbone share article availability.
//
// The dataset has no CORS headers, so the browser cannot read it; the API
// proxies it through the shared Default cache, and the peer network reads the
// same cache to recognise backbone labels.
package providerbackbone

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

const (
	// URL is the public data source.
	URL = "https://usenet.rexum.space/api/providers"
	// TTL is how long a fetched list is served before it is fetched again.
	TTL = 24 * time.Hour
	// FetchTimeout bounds one outbound fetch made with the default client.
	FetchTimeout = 15 * time.Second
	// MaxBytes caps the response body.
	MaxBytes = 5 << 20
)

// Entry maps a provider hostname to its upstream backbone.
type Entry struct {
	Host     string `json:"host"`
	Backbone string `json:"backbone"`
	Provider string `json:"provider"`
}

// dataset is the subset of the dataset's /api/providers response in use.
type dataset struct {
	Providers []struct {
		Name   string `json:"name"`
		Server []struct {
			NNTP     string `json:"nntp"`
			Backbone string `json:"backbone"`
		} `json:"server"`
	} `json:"providers"`
}

// Parse transforms a raw payload into a slim, deduplicated host to backbone
// list. Entries missing a hostname or backbone are skipped; the first
// occurrence of a hostname wins. The error never contains payload content.
func Parse(raw []byte) ([]Entry, error) {
	var resp dataset
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse backbone data: %w", err)
	}
	entries := make([]Entry, 0, len(resp.Providers))
	seen := make(map[string]struct{})
	for _, p := range resp.Providers {
		for _, s := range p.Server {
			host := strings.ToLower(strings.TrimSpace(s.NNTP))
			backbone := strings.TrimSpace(s.Backbone)
			if host == "" || backbone == "" {
				continue
			}
			if _, ok := seen[host]; ok {
				continue
			}
			seen[host] = struct{}{}
			entries = append(entries, Entry{Host: host, Backbone: backbone, Provider: strings.TrimSpace(p.Name)})
		}
	}
	return entries, nil
}

// Backbones returns the distinct backbone names of entries, sorted, or nil.
func Backbones(entries []Entry) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, e := range entries {
		k := strings.ToLower(strings.TrimSpace(e.Backbone))
		if k == "" {
			continue
		}
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, strings.TrimSpace(e.Backbone))
	}
	sort.Strings(out)
	return out
}

// Cache holds the fetched list. The zero value is not usable; use NewCache or
// Default.
type Cache struct {
	url string
	ttl time.Duration
	now func() time.Time

	// snap is the stored list; readers load it without any lock.
	snap atomic.Pointer[snapshot]
	// fetching is a one-slot semaphore: whoever holds it is the single
	// in-flight fetch. Waiters give up with their context.
	fetching chan struct{}
}

type snapshot struct {
	entries   []Entry
	fetchedAt time.Time
}

// NewCache returns an empty cache over url.
func NewCache(url string, ttl time.Duration) *Cache {
	return &Cache{url: url, ttl: ttl, now: time.Now, fetching: make(chan struct{}, 1)}
}

// Default is the process-wide cache of the public dataset.
var Default = NewCache(URL, TTL)

// Cached returns the stored list without fetching; nil when nothing was
// fetched yet. The slice must not be modified.
func (c *Cache) Cached() []Entry {
	if s := c.snap.Load(); s != nil {
		return s.entries
	}
	return nil
}

// Get returns the list, fetching it when none is stored or the stored one is
// older than the TTL. A failed fetch serves the stored list when there is one.
// A nil client uses a plain one with FetchTimeout.
func (c *Cache) Get(ctx context.Context, client *http.Client) ([]Entry, error) {
	entries, _, err := c.get(ctx, client)
	return entries, err
}

// Refresh makes sure the list is fresh: it does nothing while the stored list
// is within the TTL, and otherwise fetches. Unlike Get it reports a failed
// fetch even when a stale list stays available.
func (c *Cache) Refresh(ctx context.Context, client *http.Client) error {
	_, fetchErr, err := c.get(ctx, client)
	if err != nil {
		return err
	}
	return fetchErr
}

// get returns the list, the error of a failed fetch that was papered over with
// a stale list, and the error when there is nothing to return.
func (c *Cache) get(ctx context.Context, client *http.Client) ([]Entry, error, error) {
	if s := c.snap.Load(); s != nil && s.entries != nil && c.now().Sub(s.fetchedAt) < c.ttl {
		return s.entries, nil, nil
	}
	// One fetch at a time; the others wait for it, or for their own context.
	select {
	case c.fetching <- struct{}{}:
		defer func() { <-c.fetching }()
	case <-ctx.Done():
		return c.stale(fmt.Errorf("failed to fetch backbone data: %w", ctx.Err()))
	}
	// Whoever held the slot may just have refreshed the list.
	s := c.snap.Load()
	if s != nil && s.entries != nil && c.now().Sub(s.fetchedAt) < c.ttl {
		return s.entries, nil, nil
	}
	entries, err := c.fetch(ctx, client)
	if err != nil {
		return c.stale(err)
	}
	c.snap.Store(&snapshot{entries: entries, fetchedAt: c.now()})
	return entries, nil, nil
}

// stale papers over a failed fetch with the stored list when there is one.
func (c *Cache) stale(fetchErr error) ([]Entry, error, error) {
	if s := c.snap.Load(); s != nil && s.entries != nil {
		return s.entries, fetchErr, nil
	}
	return nil, nil, fetchErr
}

func (c *Cache) fetch(ctx context.Context, client *http.Client) ([]Entry, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to build backbone request: %w", err)
	}
	if client == nil {
		client = &http.Client{Timeout: FetchTimeout}
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch backbone data: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("backbone data source returned status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, MaxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("failed to read backbone data: %w", err)
	}
	if len(body) > MaxBytes {
		return nil, errors.New("backbone data exceeds the size limit")
	}
	return Parse(body)
}

// Lookup finds the backbone of a provider host in entries. An exact
// (case-insensitive) host match wins. Otherwise the hosts sharing the
// registrable domain (its last two labels) are consulted, and only when every
// listed host of that domain maps to the same backbone, as with the regional
// hostnames of one provider; any disagreement means no match. It only reads
// the dataset: the host is never sent anywhere.
func Lookup(host string, entries []Entry) (string, bool) {
	h := strings.ToLower(strings.TrimSpace(host))
	if h == "" || len(entries) == 0 {
		return "", false
	}
	for _, e := range entries {
		if e.Host == h && strings.TrimSpace(e.Backbone) != "" {
			return strings.TrimSpace(e.Backbone), true
		}
	}
	domain := registrableDomain(h)
	if domain == "" {
		return "", false
	}
	found := ""
	for _, e := range entries {
		if registrableDomain(e.Host) != domain {
			continue
		}
		b := strings.TrimSpace(e.Backbone)
		if b == "" {
			continue
		}
		if found != "" && !strings.EqualFold(found, b) {
			return "", false
		}
		found = b
	}
	return found, found != ""
}

// registrableDomain is the last two labels of a host name, "" when it has
// fewer.
func registrableDomain(host string) string {
	parts := strings.FieldsFunc(host, func(r rune) bool { return r == '.' })
	if len(parts) < 2 {
		return ""
	}
	return strings.Join(parts[len(parts)-2:], ".")
}
