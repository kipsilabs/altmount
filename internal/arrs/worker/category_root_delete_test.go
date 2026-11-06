package worker

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kipsilabs/altmount/internal/arrs/model"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/stretchr/testify/require"
	"golift.io/starr"
)

// A queue item whose output is a category folder must never be removed with
// RemoveFromClient, or the arr deletes the whole category (#1029).
func TestDeleteStarrQueue_CategoryRootOutputKeepsClientData(t *testing.T) {
	cfg := &config.Config{
		MountPath: "/mnt/altmount",
		SABnzbd:   config.SABnzbdConfig{CompleteDir: "/complete", Categories: []config.SABnzbdCategory{{Name: "tv-uhd", Dir: "shows-UHD"}}},
	}
	w := NewWorker(func() *config.Config { return cfg }, nil, nil, nil, nil)
	instance := &model.ConfigInstance{Name: "sonarr-test"}

	got := map[int64]*starr.QueueDeleteOpts{}
	del := func(_ context.Context, id int64, opts *starr.QueueDeleteOpts) error {
		got[id] = opts
		return nil
	}

	actions := []stuckAction{
		{ID: 1, Action: config.StuckActionBlocklist, OutputPath: "/mnt/altmount/complete/shows-UHD"},
		{ID: 2, Action: config.StuckActionBlocklist, OutputPath: "/mnt/altmount/complete/shows-UHD/Show.S01E02"},
		{ID: 3, Action: config.StuckActionRemove, OutputPath: ""},
		{ID: 4, Action: config.StuckActionRemove, OutputPath: "/mnt/altmount/complete"},
	}
	if n := w.deleteStarrQueue(context.Background(), instance, actions, del); n != 4 {
		t.Fatalf("cleaned = %d, want 4 (the queue items are still removed)", n)
	}

	for id, want := range map[int64]bool{1: false, 2: true, 3: true, 4: false} {
		opts := got[id]
		if opts == nil || opts.RemoveFromClient == nil {
			t.Fatalf("item %d: no RemoveFromClient sent", id)
		}
		if *opts.RemoveFromClient != want {
			t.Errorf("item %d: RemoveFromClient = %v, want %v", id, *opts.RemoveFromClient, want)
		}
	}
	if !got[1].BlockList {
		t.Error("item 1: blocklist must still be sent")
	}
}

// Every Sonarr removal (stuck cleanup, import-failure give-up, scanner repair)
// goes through the pending-action removal, which always sent
// RemoveFromClient=true.
func TestSonarrPendingRemoval_CategoryRootOutputKeepsClientData(t *testing.T) {
	for _, tc := range []struct {
		name       string
		outputPath string
		wantRemove string
	}{
		{"category folder", "/mnt/altmount/complete/tv", "removeFromClient=false"},
		{"release folder", "/mnt/altmount/complete/tv/Test.Show.S02E03", "removeFromClient=true"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := queueRecord(101, 42, "pack", "AltMount")
			rec["outputPath"] = tc.outputPath
			fake := &fakeSonarrQueue{records: []map[string]any{rec}}
			srv := httptest.NewServer(fake.handler())
			defer srv.Close()
			w := newImportFailureTestWorker(srv.URL, 1)
			cfg := w.configGetter()
			cfg.MountPath = "/mnt/altmount"
			cfg.SABnzbd.CompleteDir = "/complete"

			w.HandleImportFailure(context.Background(), "pack", "tv")

			fake.mu.Lock()
			defer fake.mu.Unlock()
			require.Len(t, fake.deletes, 1, "the queue item is still removed")
			require.True(t, strings.Contains(fake.deletes[0], tc.wantRemove), "delete query %q, want %s", fake.deletes[0], tc.wantRemove)
			require.True(t, strings.Contains(fake.deletes[0], "blocklist=true"), "delete query %q must still blocklist", fake.deletes[0])
		})
	}
}
