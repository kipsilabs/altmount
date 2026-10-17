package importer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/javi11/nzbparser"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/httpclient"
)

const maxRSSFeedBytes = 8 << 20
const maxRSSNZBBytes = 64 << 20

// RSSPoller imports unseen cart entries through the normal import queue.
// Run and Poll serialize so a slow feed cannot overlap with its next poll.
type RSSPoller struct {
	service      *Service
	configGetter config.ConfigGetter
	mu           sync.Mutex
}

func NewRSSPoller(service *Service, getter config.ConfigGetter) *RSSPoller {
	return &RSSPoller{service: service, configGetter: getter}
}

// Run checks for settings changes every second. Polls happen immediately on
// startup/enabling/changing the feed, then at the configured interval.
func (p *RSSPoller) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var lastURL string
	var lastInterval int
	var next time.Time
	for {
		cfg := p.configGetter()
		interval := cfg.Import.RSSPollIntervalSeconds
		if interval <= 0 {
			interval = 300
		}
		if cfg.Import.RSSFeedURL != lastURL || interval != lastInterval {
			lastURL, lastInterval, next = cfg.Import.RSSFeedURL, interval, time.Time{}
		}
		if lastURL != "" && !time.Now().Before(next) {
			if err := p.Poll(ctx); err != nil && ctx.Err() == nil {
				slog.WarnContext(ctx, "RSS cart poll failed", "error", err)
			}
			next = time.Now().Add(time.Duration(interval) * time.Second)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

type rssDocument struct {
	XMLName xml.Name   `xml:"rss"`
	Items   []rssEntry `xml:"channel>item"`
}

type rssEntry struct {
	Title      string `xml:"title"`
	GUID       string `xml:"guid"`
	Link       string `xml:"link"`
	Enclosures []struct {
		URL  string `xml:"url,attr"`
		Type string `xml:"type,attr"`
	} `xml:"enclosure"`
}

// Poll retrieves one feed snapshot. Failed entries remain eligible on the next
// poll; one bad entry does not prevent the remaining entries from importing.
func (p *RSSPoller) Poll(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	cfg := p.configGetter()
	if cfg.Import.RSSFeedURL == "" {
		return nil
	}
	base, err := url.Parse(cfg.Import.RSSFeedURL)
	if err != nil || !validRSSURL(base) {
		return errors.New("invalid RSS feed URL")
	}
	client := httpclient.NewForExternal(cfg.Network, 30*time.Second)
	defer client.CloseIdleConnections()
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return errors.New("too many RSS redirects")
		}
		if !sameRSSOrigin(base, req.URL) {
			if err := httpclient.ValidateDownloadURL(req.URL.String()); err != nil {
				return errors.New("unsafe RSS redirect destination")
			}
			req.Header.Del("Authorization")
			req.Header.Del("X-Api-Key")
		}
		return nil
	}
	data, resolvedBase, err := fetchRSS(ctx, client, cfg.GetUserAgent(), base, maxRSSFeedBytes)
	if err != nil {
		return fmt.Errorf("fetch RSS feed: %w", err)
	}
	var feed rssDocument
	if err := xml.Unmarshal(data, &feed); err != nil {
		return errors.New("invalid RSS XML")
	}
	var failures []error
	for _, entry := range feed.Items {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := p.importEntry(ctx, cfg, client, base, resolvedBase, entry); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func validRSSURL(u *url.URL) bool {
	return u != nil && (u.Scheme == "http" || u.Scheme == "https") && u.Hostname() != ""
}

func sameRSSOrigin(a, b *url.URL) bool {
	return strings.EqualFold(a.Scheme, b.Scheme) && strings.EqualFold(a.Host, b.Host)
}

func fetchRSS(ctx context.Context, client *http.Client, agent string, target *url.URL, limit int64) ([]byte, *url.URL, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return nil, nil, errors.New("invalid RSS request")
	}
	req.Header.Set("User-Agent", agent)
	resp, err := client.Do(req)
	// Never return net/http errors: feeds and indexer URLs may contain keys in
	// their query, userinfo, or path, including inside redirect error causes.
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		return nil, nil, errors.New("RSS HTTP request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("RSS HTTP status %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, nil, errors.New("failed to read RSS HTTP response")
	}
	if int64(len(data)) > limit {
		return nil, nil, errors.New("RSS HTTP response exceeds size limit")
	}
	return data, resp.Request.URL, nil
}

func (p *RSSPoller) importEntry(ctx context.Context, cfg *config.Config, client *http.Client, base, resolvedBase *url.URL, entry rssEntry) error {
	raw := strings.TrimSpace(entry.Link)
	for _, enclosure := range entry.Enclosures {
		if strings.TrimSpace(enclosure.URL) != "" && (enclosure.Type == "application/x-nzb" || enclosure.Type == "") {
			raw = strings.TrimSpace(enclosure.URL)
			break
		}
	}
	if raw == "" {
		return errors.New("RSS entry has no NZB download URL")
	}
	target, err := url.Parse(raw)
	if err != nil {
		return errors.New("invalid RSS entry URL")
	}
	target = resolvedBase.ResolveReference(target)
	if !validRSSURL(target) {
		return errors.New("invalid RSS entry URL")
	}
	// Configured feed servers can be local (e.g. Prowlarr), but remote feed
	// content cannot direct downloads or redirects to other private hosts.
	if !sameRSSOrigin(base, target) {
		if err := httpclient.ValidateDownloadURL(target.String()); err != nil {
			return errors.New("unsafe RSS entry destination")
		}
	}
	identity := strings.TrimSpace(entry.GUID)
	if identity == "" {
		identity = target.String()
	}
	sum := sha256.Sum256([]byte(base.String() + "\x00" + identity))
	id := "rss-" + hex.EncodeToString(sum[:])
	seen, err := p.service.database.HasRSSImport(ctx, id)
	if err != nil {
		return errors.New("failed to check RSS import receipt")
	}
	if seen {
		return p.service.database.RecordRSSImport(ctx, id)
	}
	// A row can exist before AddToQueue has moved its staging file. Only
	// promote a durable file/completed job to a receipt; replace an incomplete
	// pending/failed enqueue with a fresh download. The conditional delete
	// prevents removing a row that the importer has since claimed.
	existing, err := p.service.database.Repository.GetQueueItemByDownloadID(ctx, id)
	if err != nil {
		return errors.New("failed to check RSS queue recovery")
	}
	if existing != nil {
		durableRoot := filepath.Join(os.TempDir(), ".altmount-queue") + string(os.PathSeparator)
		absolute, _ := filepath.Abs(existing.NzbPath)
		info, statErr := os.Stat(existing.NzbPath)
		if existing.Status == database.QueueStatusCompleted || (statErr == nil && info.Mode().IsRegular() && strings.HasPrefix(absolute, durableRoot)) {
			return p.service.database.RecordRSSImport(ctx, id)
		}
		removed, err := p.service.database.RemoveIncompleteRSSImport(ctx, id)
		if err != nil || !removed {
			return errors.New("RSS enqueue recovery is waiting for the importer")
		}
	}
	data, _, err := fetchRSS(ctx, client, cfg.GetUserAgent(), target, maxRSSNZBBytes)
	if err != nil {
		return fmt.Errorf("download RSS NZB: %w", err)
	}
	nzb, err := nzbparser.Parse(bytes.NewReader(data))
	if err != nil || nzb == nil || len(nzb.Files) == 0 {
		return errors.New("RSS download is not a valid NZB")
	}
	dir, err := os.MkdirTemp("", "altmount-rss-")
	if err != nil {
		return errors.New("failed to create RSS download directory")
	}
	defer os.RemoveAll(dir)
	title := strings.TrimSpace(entry.Title)
	title = strings.NewReplacer("/", "_", "\\", "_", "\x00", "_").Replace(title)
	for len(title) > 180 {
		_, size := utf8.DecodeLastRuneInString(title)
		title = title[:len(title)-size]
	}
	if title == "" {
		title = "rss-download"
	}
	if !strings.HasSuffix(strings.ToLower(title), ".nzb") {
		title += ".nzb"
	}
	path := filepath.Join(dir, title)
	if err := os.WriteFile(path, data, 0600); err != nil {
		return errors.New("failed to save RSS NZB")
	}
	indexer := base.Hostname()
	if _, err := p.service.AddToQueue(ctx, path, nil, nil, nil, nil, &id, &indexer); err != nil {
		return errors.New("failed to enqueue RSS NZB")
	}
	if err := p.service.database.RecordRSSImport(ctx, id); err != nil {
		return errors.New("failed to record RSS import receipt")
	}
	return nil
}
