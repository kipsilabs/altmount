package api

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/pool"
)

// rclone reading one film: many parallel GETs from one host, each on its own port.
func addRcloneReads(tr *StreamTracker, file string, n int) []string {
	ids := make([]string, 0, n)
	for i := range n {
		s := tr.AddStream(file, "WebDAV", "rclone", fmt.Sprintf("127.0.0.1:%d", 40000+i), "rclone/v1.75.2", 0)
		ids = append(ids, s.ID)
	}
	return ids
}

func TestStreamTrackerParallelReadsOfOneFileAreOnePlayback(t *testing.T) {
	tr := NewStreamTracker(nil)
	defer tr.Stop()
	ids := addRcloneReads(tr, "/webdav/complete/Movies/A/a.mkv", 16)
	if got := tr.ActiveStreams(); got != 1 {
		t.Fatalf("ActiveStreams = %d, want 1 playback for 16 parallel reads", got)
	}
	if got := tr.ActiveConnections(); got != 16 {
		t.Fatalf("ActiveConnections = %d, want 16", got)
	}
	for _, id := range ids[:15] {
		tr.Remove(id)
	}
	if got := tr.ActiveStreams(); got != 1 {
		t.Fatalf("ActiveStreams = %d with one read left, want 1", got)
	}
	tr.Remove(ids[15])
	tr.Remove(ids[15]) // removing twice must not go negative
	if got := tr.ActiveStreams(); got != 0 {
		t.Fatalf("ActiveStreams = %d after every read closed, want 0", got)
	}
}

func TestStreamTrackerDistinctFilesAndClientsAreSeparatePlaybacks(t *testing.T) {
	tr := NewStreamTracker(nil)
	defer tr.Stop()
	addRcloneReads(tr, "/webdav/complete/Movies/A/a.mkv", 16)
	addRcloneReads(tr, "/webdav/complete/Movies/B/b.mkv", 16)
	tr.AddStream("/webdav/complete/Movies/A/a.mkv", "WebDAV", "u", "192.0.2.10:56776", "ExoPlayer Android", 0)
	tr.AddStream("/webdav/complete/Movies/A/a.mkv", "WebDAV", "u", "192.0.2.10:56777", "ExoPlayer Android", 0)
	if got := tr.ActiveStreams(); got != 3 {
		t.Fatalf("ActiveStreams = %d, want 3 (two files read by rclone + one player)", got)
	}
}

// 75 provider connections, 18 reserved per stream: rclone reading one file
// with 16 parallel GETs used to count 16 times and leave imports a single
// connection; as one playback it leaves them 57.
func TestImportBudgetWhileRcloneReadsOneFile(t *testing.T) {
	tr := NewStreamTracker(nil)
	defer tr.Stop()
	addRcloneReads(tr, "/webdav/complete/Movies/A/a.mkv", 16)

	b := pool.NewImportBudget()
	b.SetCapacity(75)
	b.SetHeadroom(18)
	b.SetStreamSource(tr)

	var releases []func()
	defer func() {
		for _, r := range releases {
			r()
		}
	}()
	for i := range 57 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		release, err := b.Acquire(ctx)
		cancel()
		if err != nil {
			t.Fatalf("import token %d of 57 not granted: %v", i+1, err)
		}
		releases = append(releases, release)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if release, err := b.Acquire(ctx); err == nil {
		release()
		t.Fatal("a 58th import token was granted; the per-playback reserve of 18 should hold it back")
	}
}
