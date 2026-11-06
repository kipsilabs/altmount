package scanner

import (
	"context"
	"testing"

	"github.com/kipsilabs/altmount/internal/config"
)

func TestQueueOutputMatches(t *testing.T) {
	cfg := &config.Config{
		MountPath: "/mnt/altmount",
		SABnzbd:   config.SABnzbdConfig{CompleteDir: "/complete", Categories: []config.SABnzbdCategory{{Name: "tv-uhd", Dir: "shows-UHD"}}},
	}
	file := "/mnt/altmount/complete/shows-UHD/Show.S01E02/Show.S01E02.mkv"

	for _, tt := range []struct {
		name   string
		output string
		path   string
		want   bool
	}{
		{"same path", file, file, true},
		{"release folder holds the file", "/mnt/altmount/complete/shows-UHD/Show.S01E02", file, true},
		{"release folder with trailing slash", "/mnt/altmount/complete/shows-UHD/Show.S01E02/", file, true},
		{"same final name elsewhere", "/mnt/altmount/complete/shows-UHD/Show.S01E02.mkv", "/library/Show/Season 1/Show.S01E02.mkv", true},
		// #1029: a queue item reporting the category folder
		// must never be selected, or RemoveFromClient deletes the category.
		{"category folder as output", "/mnt/altmount/complete/shows-UHD", file, false},
		{"category folder as output, trailing slash", "/mnt/altmount/complete/shows-UHD/", file, false},
		{"complete dir as output", "/mnt/altmount/complete", file, false},
		{"mount root as output", "/mnt/altmount", file, false},
		{"empty output", "", file, false},
		// Prefix match only on a path-component boundary.
		{"sibling release sharing a prefix", "/mnt/altmount/complete/shows-UHD/Show.S01E0", file, false},
		{"unrelated release", "/mnt/altmount/complete/shows-UHD/Other.S01E01", file, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := queueOutputMatches(context.Background(), cfg, "sonarr", 1, tt.output, tt.path); got != tt.want {
				t.Fatalf("queueOutputMatches(%q, %q) = %v, want %v", tt.output, tt.path, got, tt.want)
			}
		})
	}
}

// Without a config the category check cannot run, but the boundary rule and
// the empty-output rule still hold.
func TestQueueOutputMatches_NilConfig(t *testing.T) {
	file := "/x/complete/tv/Rel/ep.mkv"
	if !queueOutputMatches(context.Background(), nil, "sonarr", 1, "/x/complete/tv/Rel", file) {
		t.Fatal("release folder should match")
	}
	if queueOutputMatches(context.Background(), nil, "sonarr", 1, "/x/complete/tv/Re", file) {
		t.Fatal("prefix without a component boundary must not match")
	}
	if queueOutputMatches(context.Background(), nil, "sonarr", 1, "", file) {
		t.Fatal("empty output must not match")
	}
}

// A Windows arr reports backslash paths; they match on the same boundaries,
// and a category folder is still refused.
func TestQueueOutputMatches_WindowsPaths(t *testing.T) {
	cfg := &config.Config{
		MountPath: "/mnt/altmount",
		SABnzbd:   config.SABnzbdConfig{CompleteDir: "/complete", Categories: []config.SABnzbdCategory{{Name: "tv-uhd", Dir: "shows-UHD"}}},
	}
	file := `D:\usenet\complete\shows-UHD\Show.S01E02\Show.S01E02.mkv`
	if !queueOutputMatches(context.Background(), cfg, "sonarr", 1, `D:\usenet\complete\shows-UHD\Show.S01E02`, file) {
		t.Fatal("release folder should match")
	}
	if queueOutputMatches(context.Background(), cfg, "sonarr", 1, `D:\usenet\complete\shows-UHD`, file) {
		t.Fatal("category folder must not match")
	}
}
