package providerbackbone

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const payload = `{"providers":[
 {"name":"Newshosting","server":[{"nntp":"news.newshosting.com","backbone":"Omicron"},{"nntp":"NEWS.NEWSHOSTING.COM","backbone":"Omicron"}]},
 {"name":"Eweka","server":[{"nntp":"news.eweka.nl","backbone":"Eweka Internet Services"}]},
 {"name":"NoBackbone","server":[{"nntp":"reader.nobackbone.com","backbone":""}]},
 {"name":"NoHost","server":[{"nntp":"","backbone":"Ghost"}]}]}`

func TestParse(t *testing.T) {
	entries, err := Parse([]byte(payload))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2: %+v", len(entries), entries)
	}
	byHost := map[string]Entry{}
	for _, e := range entries {
		byHost[e.Host] = e
	}
	if byHost["news.newshosting.com"].Backbone != "Omicron" || byHost["news.newshosting.com"].Provider != "Newshosting" {
		t.Errorf("newshosting: %+v", byHost["news.newshosting.com"])
	}
	if _, ok := byHost["news.eweka.nl"]; !ok {
		t.Errorf("missing eweka: %+v", entries)
	}
	if _, err := Parse([]byte("not json")); err == nil {
		t.Error("expected an error for invalid JSON")
	}
}

func TestBackbones(t *testing.T) {
	got := Backbones([]Entry{{Host: "a", Backbone: "Omicron"}, {Host: "b", Backbone: "omicron "}, {Host: "c", Backbone: "Eweka"}})
	if len(got) != 2 || got[0] != "Eweka" && got[1] != "Eweka" {
		t.Errorf("got %v, want the distinct names", got)
	}
	if Backbones(nil) != nil {
		t.Error("nil list yields nil")
	}
}

type fixture struct {
	srv   *httptest.Server
	calls atomic.Int32
	code  atomic.Int32
	body  atomic.Value
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{}
	f.code.Store(200)
	f.body.Store(payload)
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.calls.Add(1)
		w.WriteHeader(int(f.code.Load()))
		_, _ = w.Write([]byte(f.body.Load().(string)))
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func TestCacheFetchesOnceWithinTTLAndServesStaleOnFailure(t *testing.T) {
	f := newFixture(t)
	now := time.Unix(1_000_000, 0)
	c := NewCache(f.srv.URL, time.Hour)
	c.now = func() time.Time { return now }

	if c.Cached() != nil {
		t.Fatal("nothing is cached before the first fetch")
	}
	got, err := c.Get(context.Background(), nil)
	if err != nil || len(got) != 2 {
		t.Fatalf("got %v, %v", got, err)
	}
	if _, err := c.Get(context.Background(), nil); err != nil || f.calls.Load() != 1 {
		t.Fatalf("second call within the TTL must not fetch: calls=%d err=%v", f.calls.Load(), err)
	}
	if len(c.Cached()) != 2 {
		t.Fatal("Cached returns the stored list without fetching")
	}

	now = now.Add(2 * time.Hour)
	f.code.Store(500)
	got, err = c.Get(context.Background(), nil)
	if err != nil || len(got) != 2 || f.calls.Load() != 2 {
		t.Fatalf("a failed refresh serves the stale list: %v %v calls=%d", got, err, f.calls.Load())
	}
}

func TestCacheFailureWithoutStaleListIsAnError(t *testing.T) {
	f := newFixture(t)
	f.code.Store(503)
	c := NewCache(f.srv.URL, time.Hour)
	if _, err := c.Get(context.Background(), nil); err == nil {
		t.Fatal("expected an error")
	}
	if c.Cached() != nil {
		t.Fatal("a failed fetch caches nothing")
	}
}

func TestRefreshReportsFailureEvenWhenStaleServed(t *testing.T) {
	f := newFixture(t)
	now := time.Unix(1_000_000, 0)
	c := NewCache(f.srv.URL, time.Hour)
	c.now = func() time.Time { return now }
	if err := c.Refresh(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Refresh(context.Background(), nil); err != nil || f.calls.Load() != 1 {
		t.Fatalf("a fresh list is not fetched again: %v calls=%d", err, f.calls.Load())
	}
	now = now.Add(2 * time.Hour)
	f.code.Store(500)
	if err := c.Refresh(context.Background(), nil); err == nil {
		t.Fatal("Refresh must report that the list could not be renewed")
	}
	if len(c.Cached()) != 2 {
		t.Fatal("the stale list stays available")
	}
}

func TestFetchLimitsAndErrorsNeverEchoBody(t *testing.T) {
	f := newFixture(t)
	f.body.Store(strings.Repeat("x", MaxBytes+10))
	c := NewCache(f.srv.URL, time.Hour)
	_, err := c.Get(context.Background(), nil)
	if err == nil {
		t.Fatal("an oversized body must be refused")
	}
	f.body.Store("SECRET-CONTENT-not-json")
	_, err = c.Get(context.Background(), nil)
	if err == nil || strings.Contains(err.Error(), "SECRET-CONTENT") {
		t.Fatalf("error must not echo the body: %v", err)
	}
}

func TestFetchUsesTheGivenClient(t *testing.T) {
	f := newFixture(t)
	var used atomic.Bool
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		used.Store(true)
		return http.DefaultTransport.RoundTrip(r)
	})}
	c := NewCache(f.srv.URL, time.Hour)
	if _, err := c.Get(context.Background(), client); err != nil || !used.Load() {
		t.Fatalf("client not used: %v", err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// Readers (the health worker, the publisher, the API) must never wait for an
// outbound fetch.
func TestCachedDoesNotBlockWhileAFetchIsInFlight(t *testing.T) {
	release := make(chan struct{})
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		<-release
		_, _ = w.Write([]byte(payload))
	}))
	defer srv.Close()
	defer close(release)

	c := NewCache(srv.URL, time.Hour)
	for range 3 {
		go func() { _, _ = c.Get(context.Background(), nil) }()
	}
	deadline := time.Now().Add(3 * time.Second)
	for calls.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("fetch never started")
		}
		time.Sleep(time.Millisecond)
	}

	done := make(chan struct{})
	go func() { _ = c.Cached(); close(done) }()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Cached blocked behind an in-flight fetch")
	}
	time.Sleep(20 * time.Millisecond)
	if n := calls.Load(); n != 1 {
		t.Fatalf("concurrent callers must share one fetch, got %d", n)
	}
}

func TestGetWaitingForAFetchHonoursItsContext(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release
		_, _ = w.Write([]byte(payload))
	}))
	defer srv.Close()
	defer close(release)
	c := NewCache(srv.URL, time.Hour)
	go func() { _, _ = c.Get(context.Background(), nil) }()
	time.Sleep(30 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := c.Get(ctx, nil); err == nil || time.Since(start) > time.Second {
		t.Fatalf("a waiter must give up with its context: err=%v after %v", err, time.Since(start))
	}
}

func TestLookup(t *testing.T) {
	entries := []Entry{
		{Host: "news.alpha.example", Backbone: "Omicron", Provider: "Alpha"},
		{Host: "eu.alpha.example", Backbone: "Omicron", Provider: "Alpha"},
		{Host: "news.mixed.example", Backbone: "Omicron", Provider: "Mixed"},
		{Host: "news-us.mixed.example", Backbone: "Abavia", Provider: "Mixed"},
		{Host: "single.example", Backbone: "Abavia", Provider: "Single"},
	}
	for name, tc := range map[string]struct {
		host, want string
	}{
		"exact":                                  {"news.alpha.example", "Omicron"},
		"exact is case insensitive":              {" News.Alpha.Example ", "Omicron"},
		"exact beats an inconsistent domain":     {"news-us.mixed.example", "Abavia"},
		"regional host of a consistent domain":   {"asia.alpha.example", "Omicron"},
		"unknown host of an inconsistent domain": {"asia.mixed.example", ""},
		"unknown domain":                         {"news.unknown.example", ""},
		"bare label":                             {"localhost", ""},
		"empty":                                  {"", ""},
	} {
		t.Run(name, func(t *testing.T) {
			got, ok := Lookup(tc.host, entries)
			if got != tc.want || ok != (tc.want != "") {
				t.Fatalf("Lookup(%q) = %q, %v; want %q", tc.host, got, ok, tc.want)
			}
		})
	}
	if _, ok := Lookup("news.alpha.example", nil); ok {
		t.Fatal("no dataset: no match")
	}
}
