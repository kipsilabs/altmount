package instances

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kipsilabs/altmount/internal/config"
)

func TestGetAllInstancesPreservesSonarrAPIVersion(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Arrs.SonarrInstances = []config.ArrsInstanceConfig{{Name: "native", APIVersion: "v5"}, {Name: "legacy", APIVersion: "v3"}, {Name: "automatic"}}
	manager := NewManager(func() *config.Config { return cfg }, nil)
	instances := manager.GetAllInstances()
	for i, want := range []string{"v5", "v3", ""} {
		if got := instances[i].APIVersion; got != want {
			t.Errorf("instance %d: version = %q, want %q", i, got, want)
		}
	}
}

func TestDetectNativeSonarrWithURLBase(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Api-Key") != "key" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/sonarr/api":
			fmt.Fprint(w, `{"current":"v5","supported":["v5"]}`)
		case "/sonarr/api/v5/system/status":
			fmt.Fprint(w, `{"appName":"Sonarr","version":"5.0.0"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	cfg := config.DefaultConfig()
	got, err := NewManager(func() *config.Config { return cfg }, nil).detectARRType(context.Background(), srv.URL+"/sonarr", "key")
	if err != nil || got != "sonarr" {
		t.Fatalf("detectARRType = %q, %v", got, err)
	}
}
