package clients

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golift.io/starr"
	"golift.io/starr/sonarr"
)

func TestSonarrVersionSelection(t *testing.T) {
	for _, tc := range []struct {
		name, discovery, v3, v5, forced, want string
		code                                  int
	}{
		{name: "v5", discovery: `{"current":"v5","supported":["v3","v5"]}`, v5: "5.0.0", want: "v5"},
		{name: "v4", discovery: `{"current":"v3","supported":["v3"]}`, v3: "4.0.0", want: "v3"},
		{name: "missing discovery", v3: "4.0.0", want: "v3"},
		{name: "html discovery", discovery: `<html>Sonarr</html>`, v3: "4.0.0", want: "v3"},
		{name: "forced legacy", v3: "5.0.0", forced: "v3", want: "v3"},
		{name: "forced unsupported", v3: "4.0.0", forced: "v5"},
		{name: "auth no fallback", code: 401, v3: "4.0.0"},
		{name: "server no fallback", code: 500, v3: "4.0.0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "key", r.Header.Get("X-Api-Key"))
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/base/api" {
					if tc.code > 0 {
						w.WriteHeader(tc.code)
						return
					}
					if tc.discovery != "" {
						_, _ = w.Write([]byte(tc.discovery))
						return
					}
					http.NotFound(w, r)
					return
				}
				if tc.code > 0 {
					t.Error("must not fall back after discovery failure")
				}
				version := ""
				if strings.Contains(r.URL.Path, "/v5/") {
					version = tc.v5
				} else {
					version = tc.v3
				}
				if version == "" {
					http.NotFound(w, r)
					return
				}
				_, _ = fmt.Fprintf(w, `{"appName":"Sonarr","version":%q}`, version)
			}))
			defer srv.Close()
			c, err := NewManager(srv.Client()).GetOrCreateSonarrClientContext(context.Background(), tc.name, srv.URL+"/base", "key", tc.forced)
			if tc.want == "" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.want, c.APIVersion())
			}
		})
	}
}

func TestSonarrCacheInvalidationAndUpgrade(t *testing.T) {
	major := "4"
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api" {
			_, _ = fmt.Fprintf(w, `{"current":"v%s"}`, map[string]string{"4": "3", "5": "5"}[major])
			return
		}
		_, _ = fmt.Fprintf(w, `{"appName":"Sonarr","version":"%s.0.0"}`, major)
	}))
	defer srv.Close()
	m := NewManager(srv.Client())
	ctx := context.Background()
	first, err := m.GetOrCreateSonarrClientContext(ctx, "test", srv.URL, "key", "auto")
	require.NoError(t, err)
	require.Equal(t, "v3", first.APIVersion())
	originalCalls := calls
	cached, err := m.GetOrCreateSonarrClientContext(ctx, "test", srv.URL, "key", "auto")
	require.NoError(t, err)
	require.Same(t, first, cached)
	require.Equal(t, originalCalls, calls)
	changed, err := m.GetOrCreateSonarrClientContext(ctx, "test", srv.URL, "new-key", "auto")
	require.NoError(t, err)
	require.NotSame(t, first, changed)
	major = "5"
	m.mu.Lock()
	entry := m.sonarrClients["test"]
	entry.expires = entry.expires.Add(-2 * sonarrCapabilityTTL)
	m.sonarrClients["test"] = entry
	m.mu.Unlock()
	upgraded, err := m.GetOrCreateSonarrClientContext(ctx, "test", srv.URL, "new-key", "auto")
	require.NoError(t, err)
	require.Equal(t, "v5", upgraded.APIVersion())
	require.Equal(t, "v3", changed.APIVersion())
	forced, err := m.GetOrCreateSonarrClientContext(ctx, "test", srv.URL, "new-key", "v3")
	require.NoError(t, err)
	require.Equal(t, "v3", forced.APIVersion())
}

func TestSonarrSelectionCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := NewManager(nil).GetOrCreateSonarrClientContext(ctx, "test", "http://127.0.0.1:1", "key", "auto")
	require.ErrorIs(t, err, context.Canceled)
}

func TestUnsupportedReadExpiresCapability(t *testing.T) {
	major := "4"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api" {
			_, _ = fmt.Fprintf(w, `{"current":"v%s"}`, map[string]string{"4": "3", "5": "5"}[major])
			return
		}
		if strings.HasSuffix(r.URL.Path, "/rootFolder") {
			http.NotFound(w, r)
			return
		}
		_, _ = fmt.Fprintf(w, `{"appName":"Sonarr","version":"%s.0.0"}`, major)
	}))
	defer srv.Close()
	m := NewManager(srv.Client())
	ctx := context.Background()
	first, err := m.GetOrCreateSonarrClientContext(ctx, "test", srv.URL, "key", "auto")
	require.NoError(t, err)
	major = "5"
	_, err = first.GetRootFoldersContext(ctx)
	require.Error(t, err)
	next, err := m.GetOrCreateSonarrClientContext(ctx, "test", srv.URL, "key", "auto")
	require.NoError(t, err)
	require.Equal(t, "v5", next.APIVersion())
	require.Equal(t, "v3", first.APIVersion())
}
func TestConnectionAlwaysRevalidates(t *testing.T) {
	healthy := true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !healthy {
			w.WriteHeader(401)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api" {
			_, _ = w.Write([]byte(`{"current":"v3"}`))
			return
		}
		_, _ = w.Write([]byte(`{"appName":"Sonarr","version":"4.0.0"}`))
	}))
	defer srv.Close()
	m := NewManager(srv.Client())
	require.NoError(t, m.TestConnection(context.Background(), "sonarr", srv.URL, "key"))
	healthy = false
	require.Error(t, m.TestConnection(context.Background(), "sonarr", srv.URL, "key"))
}

func TestNativeUsesSharedProxyTransport(t *testing.T) {
	requests := []string{}
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "sonarr.example.invalid", r.URL.Host)
		require.Equal(t, "secret", r.Header.Get("X-Api-Key"))
		requests = append(requests, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/base/api":
			_, _ = w.Write([]byte(`{"current":"v5"}`))
		case "/base/api/v5/system/status":
			_, _ = w.Write([]byte(`{"appName":"Sonarr","version":"5.0.0"}`))
		case "/base/api/v5/series":
			_, _ = w.Write([]byte(`[]`))
		default:
			t.Errorf("unexpected proxy request %s", r.URL)
		}
	}))
	defer proxy.Close()
	parsed, err := url.Parse(proxy.URL)
	require.NoError(t, err)
	transport := &http.Transport{Proxy: http.ProxyURL(parsed)}
	defer transport.CloseIdleConnections()
	m := NewManager(&http.Client{Transport: transport, Timeout: time.Second})
	c, err := m.GetOrCreateSonarrClientContext(context.Background(), "proxy", "http://sonarr.example.invalid/base", "secret", "auto")
	require.NoError(t, err)
	_, err = c.GetAllSeriesContext(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"/base/api", "/base/api/v5/system/status", "/base/api/v5/series"}, requests)
}

func TestSonarrConcurrentAcquisitionCoalescesProbes(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api" {
			select {
			case <-started:
			default:
				close(started)
			}
			<-release
			_, _ = w.Write([]byte(`{"current":"v3"}`))
			return
		}
		_, _ = w.Write([]byte(`{"appName":"Sonarr","version":"4.0.0"}`))
	}))
	defer srv.Close()
	m := NewManager(srv.Client())
	results := make(chan error, 10)
	for i := 0; i < 10; i++ {
		go func() {
			_, e := m.GetOrCreateSonarrClientContext(context.Background(), "same", srv.URL, "key", "auto")
			results <- e
		}()
	}
	<-started
	// Give overlapping acquisitions an opportunity to enter their shared wait.
	time.Sleep(20 * time.Millisecond)
	close(release)
	for i := 0; i < 10; i++ {
		require.NoError(t, <-results)
	}
	require.Equal(t, int32(2), calls.Load())
}
func TestNativeErrorsExcludeServerEchoedSecrets(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(400)
		_, _ = w.Write([]byte(`{"message":"invalid callback http://altmount/?apikey=altmount-secret; sonarr-secret"}`))
	}))
	defer srv.Close()
	c := newNativeSonarr(&starr.Config{URL: srv.URL, APIKey: "sonarr-secret", Client: srv.Client()})
	_, err := c.AddNotificationContext(context.Background(), &sonarr.NotificationInput{Name: "AltMount"})
	require.Error(t, err)
	require.NotContains(t, err.Error(), "altmount-secret")
	require.NotContains(t, err.Error(), "sonarr-secret")
}

func TestNativeSelectionRequiresVersionEvidence(t *testing.T) {
	for _, body := range []string{`{}`, `null`, `{"appName":"Sonarr"}`, `{"appName":"Radarr","version":"5.0.0"}`} {
		t.Run(body, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/api" {
					_, _ = w.Write([]byte(`{"current":"v5"}`))
					return
				}
				_, _ = w.Write([]byte(body))
			}))
			defer srv.Close()
			_, err := NewManager(srv.Client()).GetOrCreateSonarrClientContext(context.Background(), "test", srv.URL, "key", "auto")
			require.Error(t, err)
		})
	}
}
func TestSonarrProbeWaitCancellationAndIndependentInstance(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api" {
			close(started)
			<-release
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"appName":"Sonarr","version":"4.0.0"}`))
	}))
	defer slow.Close()
	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"appName":"Sonarr","version":"4.0.0"}`))
	}))
	defer fast.Close()
	m := NewManager(nil)
	done := make(chan error, 1)
	go func() {
		_, e := m.GetOrCreateSonarrClientContext(context.Background(), "slow", slow.URL, "key", "auto")
		done <- e
	}()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := m.GetOrCreateSonarrClientContext(ctx, "slow", slow.URL, "key", "auto")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	independent, cancelFast := context.WithTimeout(context.Background(), time.Second)
	defer cancelFast()
	_, err = m.GetOrCreateSonarrClientContext(independent, "fast", fast.URL, "key", "auto")
	require.NoError(t, err)
	close(release)
	require.NoError(t, <-done)
}
