package rclonecli

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/kipsilabs/altmount/internal/config"
)

func TestRefreshDirChecksRcloneResult(t *testing.T) {
	const dir = "complete/tv/release"
	for _, tc := range []struct {
		name string
		dir  string
		body string
		want string
	}{
		{name: "success", body: `{"result":{"complete/tv/release":"OK"}}`},
		{name: "root", dir: "/", body: `{"result":{"":"OK"}}`},
		{name: "directory failure", body: `{"result":{"complete/tv/release":"file does not exist"}}`, want: "file does not exist"},
		{name: "missing result", body: `{"result":{}}`, want: "missing result"},
		{name: "invalid response", body: `invalid`, want: "decode"},
	} {
		for _, mode := range []string{"internal", "external"} {
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				requestDir := tc.dir
				if requestDir == "" {
					requestDir = dir
				}
				httpClient := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
					body := "{}"
					switch req.URL.Path {
					case "/vfs/refresh":
						var args map[string]any
						if err := json.NewDecoder(req.Body).Decode(&args); err != nil {
							t.Errorf("decode request: %v", err)
						}
						gotDir, hasDir := args["dir"].(string)
						switch {
						case requestDir == "/":
							if hasDir {
								t.Error("root refresh must omit the dir parameter")
							}
							body = tc.body
						case gotDir == requestDir:
							body = tc.body
						default:
							// The parent RefreshDir adds ahead of the target.
							body = `{"result":{"` + gotDir + `":"OK"}}`
						}
					default:
						t.Errorf("unexpected request: %s", req.URL.Path)
					}
					return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}, nil
				})}
				const provider = "altmount"
				client := newTestRefreshClient(mode, provider, httpClient)
				err := client.RefreshDir(context.Background(), provider, []string{requestDir})
				if tc.want == "" {
					if err != nil {
						t.Fatalf("refresh failed: %v", err)
					}
				} else if err == nil || !strings.Contains(err.Error(), dir) || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("expected error for %s containing %q, got %v", dir, tc.want, err)
				}
			})
		}
	}
}

// newTestRefreshClient builds the internal (Manager) or external RC client
// against httpClient.
func newTestRefreshClient(mode, provider string, httpClient *http.Client) RcloneRcClient {
	if mode == "internal" {
		ready := make(chan struct{})
		close(ready)
		return &Manager{
			logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
			serverReady:   ready,
			serverStarted: true,

			mounts:     map[string]*MountInfo{provider: {Mounted: true}},
			httpClient: httpClient,
			rcPort:     "5572",
		}
	}
	enabled := true
	cfg := &config.Config{}
	cfg.RClone.RCEnabled = &enabled
	cfg.RClone.RCUrl = "http://localhost:5572"
	return NewRcloneRcClient(config.NewManager(cfg, ""), httpClient)
}

// TestRefreshDirRefreshesParentsFirstWithoutForget pins the import-visibility
// fix: vfs/forget on an existing ancestor detaches the child Dir node the FUSE
// kernel holds, leaving new releases invisible through the mount for
// dir_cache_time. RefreshDir must therefore only refresh, shallowest first, so
// each new directory is reachable from its freshly re-read parent.
func TestRefreshDirRefreshesParentsFirstWithoutForget(t *testing.T) {
	// The order the import post-processor passes: release, parent, grandparent.
	dirs := []string{"complete/tvAR/Show.S01E02", "complete/tvAR", "complete", "complete/tvAR"}
	want := []string{"", "complete", "complete/tvAR", "complete/tvAR/Show.S01E02"}

	for _, mode := range []string{"internal", "external"} {
		t.Run(mode, func(t *testing.T) {
			var refreshed []string
			httpClient := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.URL.Path != "/vfs/refresh" {
					t.Errorf("RefreshDir must only call vfs/refresh, got %s", req.URL.Path)
					return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}"))}, nil
				}
				var args map[string]any
				if err := json.NewDecoder(req.Body).Decode(&args); err != nil {
					t.Fatalf("decode request: %v", err)
				}
				dir, _ := args["dir"].(string)
				refreshed = append(refreshed, dir)
				body := `{"result":{"` + dir + `":"OK"}}`
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}, nil
			})}

			client := newTestRefreshClient(mode, "altmount", httpClient)
			if err := client.RefreshDir(context.Background(), "altmount", dirs); err != nil {
				t.Fatalf("refresh failed: %v", err)
			}
			if strings.Join(refreshed, ",") != strings.Join(want, ",") {
				t.Fatalf("refresh order = %v, want %v", refreshed, want)
			}
		})
	}
}

func TestRefreshOrder(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   []string
		want []string
	}{
		{name: "empty", in: nil, want: []string{}},
		{name: "root has no parent", in: []string{"/"}, want: []string{"/"}},
		{name: "leaf gains its parent", in: []string{"tv/Show/S01"}, want: []string{"tv/Show", "tv/Show/S01"}},
		{name: "top level gains root", in: []string{"complete"}, want: []string{"/", "complete"}},
		{name: "importer dirs", in: []string{"c/tv/rel", "c/tv", "c"}, want: []string{"/", "c", "c/tv", "c/tv/rel"}},
		{name: "stable within depth", in: []string{"b/x", "a/y"}, want: []string{"b", "a", "b/x", "a/y"}},
		{name: "drops empty and duplicates", in: []string{"", "a/b", "a/b"}, want: []string{"a", "a/b"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := refreshOrder(tc.in)
			if strings.Join(got, ",") != strings.Join(tc.want, ",") || len(got) != len(tc.want) {
				t.Fatalf("refreshOrder(%v) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}
