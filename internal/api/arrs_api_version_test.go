package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/kipsilabs/altmount/internal/arrs"
	"github.com/kipsilabs/altmount/internal/config"
)

func TestArrsConnectionPreservesStoredSonarrVersion(t *testing.T) {
	for _, tc := range []struct{ name, key, version, endpoint string }{
		{"masked key and omitted version", "", "", "v3"},
		{"supplied key and omitted version", "key", "", "v3"},
		{"explicit override", "", "v5", "v5"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("X-Api-Key") != "key" {
					http.Error(w, "bad key", http.StatusUnauthorized)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/api/"+tc.endpoint+"/system/status" {
					fmt.Fprint(w, `{"appName":"Sonarr","version":"5.0.0"}`)
					return
				}
				// Discovery must not override an explicit or stored choice.
				if r.URL.Path == "/api" {
					fmt.Fprint(w, `{"current":"v5","supported":["v3","v5"]}`)
					return
				}
				http.NotFound(w, r)
			}))
			defer srv.Close()
			cfg := config.DefaultConfig()
			cfg.Arrs.SonarrInstances = []config.ArrsInstanceConfig{{Name: "tv", URL: srv.URL, APIKey: "key", APIVersion: "v3"}}
			manager := &mockConfigManager{cfg: cfg}
			server := &Server{configManager: manager, arrsService: arrs.NewService(manager.GetConfig, manager, nil, nil)}
			app := fiber.New()
			app.Post("/test", server.handleTestArrsConnection)
			body, _ := json.Marshal(TestConnectionRequest{Name: "tv", Type: "sonarr", URL: srv.URL, APIKey: tc.key, APIVersion: tc.version})
			req := httptest.NewRequest("POST", "/test", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			resp, err := app.Test(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				var data any
				json.NewDecoder(resp.Body).Decode(&data)
				t.Fatalf("status %d: %v", resp.StatusCode, data)
			}
		})
	}
}

func TestArrsConnectionRejectsUnsupportedAPIVersion(t *testing.T) {
	for _, tc := range []struct{ kind, version string }{{"radarr", "v3"}, {"whisparr", "auto"}, {"sonarr", "v4"}} {
		t.Run(tc.kind+tc.version, func(t *testing.T) {
			server := &Server{arrsService: &arrs.Service{}}
			app := fiber.New()
			app.Post("/test", server.handleTestArrsConnection)
			body, _ := json.Marshal(TestConnectionRequest{Type: tc.kind, URL: "http://example.invalid", APIKey: "key", APIVersion: tc.version})
			req := httptest.NewRequest("POST", "/test", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			resp, err := app.Test(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != 400 {
				t.Fatalf("status = %d, want 400", resp.StatusCode)
			}
		})
	}
}
