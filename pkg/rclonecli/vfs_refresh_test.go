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
						body = tc.body
						if requestDir == "/" {
							var args map[string]any
							if err := json.NewDecoder(req.Body).Decode(&args); err != nil {
								t.Errorf("decode request: %v", err)
							}
							if _, ok := args["dir"]; ok {
								t.Error("root refresh must omit the dir parameter")
							}
						}
					case "/vfs/forget":
					default:
						t.Errorf("unexpected request: %s", req.URL.Path)
					}
					return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}, nil
				})}
				const provider = "altmount"
				var client RcloneRcClient
				if mode == "internal" {
					ready := make(chan struct{})
					close(ready)
					client = &Manager{
						logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
						serverReady:   ready,
						serverStarted: true,

						mounts:     map[string]*MountInfo{provider: {Mounted: true}},
						httpClient: httpClient,
						rcPort:     "5572",
					}
				} else {
					enabled := true
					cfg := &config.Config{}
					cfg.RClone.RCEnabled = &enabled
					cfg.RClone.RCUrl = "http://localhost:5572"
					client = NewRcloneRcClient(config.NewManager(cfg, ""), httpClient)
				}
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
