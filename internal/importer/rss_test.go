package importer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"
)

func TestRSSPollQueuesAndDeduplicatesAfterRestart(t *testing.T) {
	s, _ := newDedupTestService(t)
	downloads := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/feed" {
			fmt.Fprint(w, `<rss><channel><item><title>Movie</title><guid>cart-1</guid><enclosure url="/nzb" type="application/x-nzb"/></item><item><title>Movie</title><guid>cart-1</guid><link>/nzb</link></item></channel></rss>`)
			return
		}
		downloads++
		fmt.Fprint(w, dedupNzbContent)
	}))
	defer server.Close()
	cfg := s.configGetter().DeepCopy()
	cfg.Import.RSSFeedURL = server.URL + "/feed?apikey=secret"
	poller := NewRSSPoller(s, func() *config.Config { return cfg })
	require.NoError(t, poller.Poll(context.Background()))
	items, err := s.database.Repository.GetPendingQueueItemsByPathPrefix(context.Background(), "")
	require.NoError(t, err)
	require.Len(t, items, 1)
	contents, err := os.ReadFile(items[0].NzbPath)
	require.NoError(t, err)
	require.Equal(t, dedupNzbContent, string(contents))
	require.NoError(t, s.database.Repository.RemoveFromQueue(context.Background(), items[0].ID))
	path := cfg.Database.Path
	require.NoError(t, s.database.Close())
	db, err := database.NewDB(database.Config{DatabasePath: path})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	s.database = db
	restarted := NewRSSPoller(s, func() *config.Config { return cfg })
	require.NoError(t, restarted.Poll(context.Background()))
	require.Equal(t, 1, downloads)
	items, err = db.Repository.GetPendingQueueItemsByPathPrefix(context.Background(), "")
	require.NoError(t, err)
	require.Empty(t, items)
}

func TestRSSRetriesFailedDownloadsAndRejectsInvalidNZB(t *testing.T) {
	s, _ := newDedupTestService(t)
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/feed" {
			fmt.Fprint(w, `<rss><channel><item><guid>1</guid><link>/nzb</link></item></channel></rss>`)
			return
		}
		attempts++
		switch attempts {
		case 1:
			w.WriteHeader(http.StatusTooManyRequests)
		case 2:
			fmt.Fprint(w, `<html>Login required</html>`)
		default:
			fmt.Fprint(w, dedupNzbContent)
		}
	}))
	defer server.Close()
	cfg := s.configGetter().DeepCopy()
	cfg.Import.RSSFeedURL = server.URL + "/feed"
	poller := NewRSSPoller(s, func() *config.Config { return cfg })
	require.Error(t, poller.Poll(context.Background()))
	require.Error(t, poller.Poll(context.Background()))
	require.NoError(t, poller.Poll(context.Background()))
	require.NoError(t, poller.Poll(context.Background()))
	require.Equal(t, 3, attempts)
}

func TestRSSBadEntryDoesNotBlockOthersAndMasksErrors(t *testing.T) {
	s, _ := newDedupTestService(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/feed":
			fmt.Fprint(w, `<rss><channel><item><guid>bad</guid><link>file:///secret-api-key</link></item><item><guid>good</guid><title>../../Movie</title><link>/good?apikey=secret-api-key</link></item></channel></rss>`)
		case "/good":
			fmt.Fprint(w, dedupNzbContent)
		}
	}))
	defer server.Close()
	cfg := s.configGetter().DeepCopy()
	cfg.Import.RSSFeedURL = server.URL + "/feed?apikey=secret-api-key"
	err := NewRSSPoller(s, func() *config.Config { return cfg }).Poll(context.Background())
	require.Error(t, err)
	require.NotContains(t, err.Error(), "secret-api-key")
	items, err := s.database.Repository.GetPendingQueueItemsByPathPrefix(context.Background(), "")
	require.NoError(t, err)
	require.Len(t, items, 1)
}

func TestRSSMalformedFeed(t *testing.T) {
	for _, feed := range []string{`<rss><channel>`, `<html>login</html>`, `<feed xmlns="http://www.w3.org/2005/Atom"/>`} {
		t.Run(feed, func(t *testing.T) {
			s, _ := newDedupTestService(t)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, feed) }))
			defer server.Close()
			cfg := s.configGetter().DeepCopy()
			cfg.Import.RSSFeedURL = server.URL
			require.Error(t, NewRSSPoller(s, func() *config.Config { return cfg }).Poll(context.Background()))
		})
	}
}

func TestRSSProxyAndUserAgent(t *testing.T) {
	s, _ := newDedupTestService(t)
	requests := 0
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "rss-test-agent", r.UserAgent())
		require.Equal(t, "indexer.invalid", r.URL.Host)
		requests++
		if r.URL.Path == "/feed" {
			fmt.Fprint(w, `<rss><channel><item><link>http://indexer.invalid/nzb</link></item></channel></rss>`)
			return
		}
		fmt.Fprint(w, dedupNzbContent)
	}))
	defer proxy.Close()
	cfg := s.configGetter().DeepCopy()
	cfg.Import.RSSFeedURL = "http://indexer.invalid/feed?apikey=secret"
	cfg.UserAgent = "rss-test-agent"
	cfg.Network.HTTPProxy = proxy.URL
	require.NoError(t, NewRSSPoller(s, func() *config.Config { return cfg }).Poll(context.Background()))
	require.Equal(t, 2, requests)
}

func TestRSSRunCancelsInFlightRequest(t *testing.T) {
	s, _ := newDedupTestService(t)
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(started); <-r.Context().Done() }))
	defer server.Close()
	cfg := s.configGetter().DeepCopy()
	cfg.Import.RSSFeedURL = server.URL + "/secret-key/feed"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); NewRSSPoller(s, func() *config.Config { return cfg }).Run(ctx) }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("poll did not start immediately")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("poll did not stop after cancellation")
	}
}

func TestRSSDownloadBounds(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "12345") }))
	defer server.Close()
	target, err := url.Parse(server.URL)
	require.NoError(t, err)
	_, _, err = fetchRSS(context.Background(), server.Client(), "test", target, 4)
	require.ErrorContains(t, err, "size limit")
	data, _, err := fetchRSS(context.Background(), server.Client(), "test", target, 5)
	require.NoError(t, err)
	require.Equal(t, "12345", string(data))
}

func TestRSSEnclosureFallbackAndSafeFilename(t *testing.T) {
	s, _ := newDedupTestService(t)
	title := strings.Repeat("映", 100)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/feed" {
			fmt.Fprintf(w, `<rss><channel><item><title>%s</title><enclosure type="application/x-nzb"/><link>/nzb</link></item></channel></rss>`, title)
			return
		}
		fmt.Fprint(w, dedupNzbContent)
	}))
	defer server.Close()
	cfg := s.configGetter().DeepCopy()
	cfg.Import.RSSFeedURL = server.URL + "/feed"
	poller := NewRSSPoller(s, func() *config.Config { return cfg })
	require.NoError(t, poller.Poll(context.Background()))
	require.NoError(t, poller.Poll(context.Background()))
	items, err := s.database.Repository.GetPendingQueueItemsByPathPrefix(context.Background(), "")
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.True(t, utf8.ValidString(filepath.Base(items[0].NzbPath)))
	require.LessOrEqual(t, len(filepath.Base(items[0].NzbPath)), 255)
}

func TestRSSRejectsPrivateDownloadAndRedirect(t *testing.T) {
	for _, redirect := range []bool{false, true} {
		t.Run(fmt.Sprint(redirect), func(t *testing.T) {
			s, _ := newDedupTestService(t)
			reached := false
			private := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reached = true }))
			defer private.Close()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if redirect {
					http.Redirect(w, r, private.URL+"/secret-key", http.StatusFound)
					return
				}
				fmt.Fprintf(w, `<rss><channel><item><link>%s/secret-key</link></item></channel></rss>`, private.URL)
			}))
			defer server.Close()
			cfg := s.configGetter().DeepCopy()
			cfg.Import.RSSFeedURL = server.URL + "/feed?apikey=secret-key"
			err := NewRSSPoller(s, func() *config.Config { return cfg }).Poll(context.Background())
			require.Error(t, err)
			require.NotContains(t, err.Error(), "secret-key")
			require.False(t, reached)
		})
	}
}

func TestRSSRunPicksUpSettingsAndPollsAtInterval(t *testing.T) {
	s, _ := newDedupTestService(t)
	polls := make(chan struct{}, 10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		polls <- struct{}{}
		fmt.Fprint(w, `<rss><channel/></rss>`)
	}))
	defer server.Close()
	var current atomic.Pointer[config.Config]
	cfg := s.configGetter().DeepCopy()
	current.Store(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); NewRSSPoller(s, current.Load).Run(ctx) }()
	enabled := cfg.DeepCopy()
	enabled.Import.RSSFeedURL = server.URL
	enabled.Import.RSSPollIntervalSeconds = 1
	current.Store(enabled)
	for i := 0; i < 2; i++ {
		select {
		case <-polls:
		case <-time.After(5 * time.Second):
			t.Fatal("configured poll did not run")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop")
	}
}

func TestRSSRedirectRelativeDownload(t *testing.T) {
	s, _ := newDedupTestService(t)
	downloads := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cart":
			http.Redirect(w, r, "/cart/", http.StatusMovedPermanently)
		case "/cart/":
			fmt.Fprint(w, `<rss><channel><item><guid>redirect</guid><link>download.nzb</link></item></channel></rss>`)
		case "/cart/download.nzb":
			downloads++
			fmt.Fprint(w, dedupNzbContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	cfg := s.configGetter().DeepCopy()
	cfg.Import.RSSFeedURL = server.URL + "/cart"
	poller := NewRSSPoller(s, func() *config.Config { return cfg })
	require.NoError(t, poller.Poll(context.Background()))
	require.NoError(t, poller.Poll(context.Background()))
	require.Equal(t, 1, downloads)
}

func TestRSSRecoversIncompleteQueueRow(t *testing.T) {
	for _, stageExists := range []bool{false, true} {
		t.Run(fmt.Sprint(stageExists), func(t *testing.T) {
			s, _ := newDedupTestService(t)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/feed" {
					fmt.Fprint(w, `<rss><channel><item><guid>crashed</guid><link>/nzb</link></item></channel></rss>`)
					return
				}
				fmt.Fprint(w, dedupNzbContent)
			}))
			defer server.Close()
			cfg := s.configGetter().DeepCopy()
			cfg.Import.RSSFeedURL = server.URL + "/feed"
			sum := sha256.Sum256([]byte(cfg.Import.RSSFeedURL + "\x00crashed"))
			id := "rss-" + hex.EncodeToString(sum[:])
			stage := filepath.Join(t.TempDir(), "staging.nzb")
			if stageExists {
				require.NoError(t, os.WriteFile(stage, []byte(dedupNzbContent), 0600))
			}
			item := &database.ImportQueueItem{DownloadID: &id, NzbPath: stage, Status: database.QueueStatusPending, Priority: database.QueuePriorityNormal, MaxRetries: 3}
			require.NoError(t, s.database.Repository.AddToQueue(context.Background(), item))
			require.NoError(t, NewRSSPoller(s, func() *config.Config { return cfg }).Poll(context.Background()))
			items, err := s.database.Repository.GetPendingQueueItemsByPathPrefix(context.Background(), "")
			require.NoError(t, err)
			require.Len(t, items, 1)
			require.NotEqual(t, stage, items[0].NzbPath)
			_, err = os.Stat(items[0].NzbPath)
			require.NoError(t, err)
			seen, err := s.database.HasRSSImport(context.Background(), id)
			require.NoError(t, err)
			require.True(t, seen)
		})
	}
}
