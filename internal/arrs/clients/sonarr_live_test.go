package clients

import (
	"context"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"golift.io/starr"
)

// TestSonarrLiveReadOnly is opt-in. Set ALTMOUNT_SONARR_V4_URL/KEY and/or
// ALTMOUNT_SONARR_V5_URL/KEY to explicitly authorize reads from those instances.
// No registration, deletion, monitoring updates or searches are submitted.
func TestSonarrLiveReadOnly(t *testing.T) {
	for _, tc := range []struct{ name, prefix, forced, want, major string }{
		{"v4_auto", "ALTMOUNT_SONARR_V4", "auto", "v3", "4."},
		{"v5_auto", "ALTMOUNT_SONARR_V5", "auto", "v5", "5."},
		{"v5_forced_v3", "ALTMOUNT_SONARR_V5", "v3", "v3", "5."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			endpoint, key := os.Getenv(tc.prefix+"_URL"), os.Getenv(tc.prefix+"_KEY")
			if endpoint == "" && key == "" {
				t.Skip("live URL/key not configured")
			}
			if endpoint == "" || key == "" {
				t.Fatal("both live URL and key must be configured")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			manager := NewManager(&http.Client{Timeout: 15 * time.Second})
			client, err := manager.GetOrCreateSonarrClientContext(ctx, tc.name, endpoint, key, tc.forced)
			// Do not print transport errors: they may embed server URLs or credentials.
			if err != nil {
				t.Fatalf("API selection failed (%T); inspect disposable instance logs", err)
			}
			if client.APIVersion() != tc.want {
				t.Fatalf("selected %s, want %s", client.APIVersion(), tc.want)
			}
			status, err := client.GetSystemStatusContext(ctx)
			if err != nil {
				t.Fatalf("status read failed (%T)", err)
			}
			if !strings.HasPrefix(status.Version, tc.major) {
				t.Fatalf("unexpected application version %q", status.Version)
			}
			t.Logf("Sonarr %s, API %s", status.Version, client.APIVersion())
			checks := []struct {
				name string
				read func() error
			}{
				{"library", func() error { _, err := client.GetAllSeriesContext(ctx); return err }},
				{"rootfolders", func() error { _, err := client.GetRootFoldersContext(ctx); return err }},
				{"queue", func() error { _, err := client.QueueGroups(ctx); return err }},
				{"history", func() error {
					_, err := client.GetHistoryPageContext(ctx, &starr.PageReq{PageSize: 10, SortKey: "date", SortDir: starr.SortDescend})
					return err
				}},
				{"notifications", func() error { _, err := client.GetNotificationsContext(ctx); return err }},
				{"downloadclients", func() error { _, err := client.GetDownloadClientsContext(ctx); return err }},
				{"commands", func() error { _, err := client.GetCommandsContext(ctx); return err }},
			}
			for _, check := range checks {
				t.Run(check.name, func(t *testing.T) {
					if err := check.read(); err != nil {
						t.Fatalf("read failed (%T); inspect disposable instance logs", err)
					}
				})
			}
		})
	}
}
