package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/kipsilabs/altmount/internal/providerbackbone"
)

// The handler serves the shared cache; the dataset parsing itself is tested in
// internal/providerbackbone.
func TestHandleProviderBackbones(t *testing.T) {
	status := http.StatusOK
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"providers":[{"name":"Newshosting","server":[{"nntp":"news.newshosting.com","backbone":"Omicron"}]}]}`))
	}))
	defer upstream.Close()

	orig := providerbackbone.Default
	providerbackbone.Default = providerbackbone.NewCache(upstream.URL, time.Hour)
	defer func() { providerbackbone.Default = orig }()

	app := fiber.New()
	s := &Server{}
	app.Get("/providers/backbones", s.handleProviderBackbones)

	status = http.StatusBadGateway
	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/providers/backbones", nil))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("an unavailable source with nothing cached is 503, got %d", resp.StatusCode)
	}

	status = http.StatusOK
	resp, err = app.Test(httptest.NewRequest(http.MethodGet, "/providers/backbones", nil))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	var out struct {
		Success bool            `json:"success"`
		Data    []BackboneEntry `json:"data"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("%v: %s", err, body)
	}
	if resp.StatusCode != http.StatusOK || !out.Success || len(out.Data) != 1 || out.Data[0].Backbone != "Omicron" {
		t.Fatalf("status %d body %s", resp.StatusCode, body)
	}

	// The peer network reads the same cache.
	if got := providerbackbone.Default.Cached(); len(got) != 1 {
		t.Fatalf("shared cache not filled: %v", got)
	}
}
