package postprocessor

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/metadata"
)

type recordingVFSClient struct {
	dirs []string
}

func (c *recordingVFSClient) RefreshDir(_ context.Context, _ string, dirs []string) error {
	c.dirs = dirs
	return nil
}

func (*recordingVFSClient) ForgetDir(context.Context, string, []string) error {
	return nil
}

func TestNotifyVFSRefreshesDirectories(t *testing.T) {
	for _, tc := range []struct {
		path   string
		isFile bool
		want   []string
	}{
		{path: "/complete/tv/release.mkv/episode.mkv", isFile: true, want: []string{"complete/tv/release.mkv", "complete/tv", "complete"}},
		{path: "/complete/tv/release.mkv", want: []string{"complete/tv/release.mkv", "complete/tv", "complete"}},
		{path: "/episode.mkv", isFile: true, want: []string{"/"}},
	} {
		t.Run(tc.path, func(t *testing.T) {
			service := metadata.NewMetadataService(t.TempDir())
			if tc.isFile {
				metaPath := service.GetMetadataFilePath(tc.path)
				if err := os.MkdirAll(filepath.Dir(metaPath), 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(metaPath, nil, 0644); err != nil {
					t.Fatal(err)
				}
			} else if err := os.MkdirAll(service.GetMetadataDirectoryPath(tc.path), 0755); err != nil {
				t.Fatal(err)
			}
			client := &recordingVFSClient{}
			coordinator := NewCoordinator(Config{
				ConfigGetter:    func() *config.Config { return &config.Config{MountType: config.MountTypeRClone} },
				MetadataService: service,
				RcloneClient:    client,
			})
			coordinator.NotifyVFS(context.Background(), tc.path, false)
			if !equalStrings(client.dirs, tc.want) {
				t.Fatalf("refreshed %q, want %q", client.dirs, tc.want)
			}
		})
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestRefreshDirsFor(t *testing.T) {
	// These expectations hold on every platform. Before the ancestry was walked
	// on the forward-slash form, Windows produced backslash-separated parents
	// and, because its root is "\" rather than "/", slipped a bare-root entry
	// past the guards into every batch.
	cases := []struct {
		name string
		in   string
		want []string
	}{
		{
			name: "nested release directory",
			in:   "/tv/Show.S01E01/ep.mkv",
			want: []string{"tv/Show.S01E01/ep.mkv", "tv/Show.S01E01", "tv"},
		},
		{
			name: "file one level under the mount root",
			in:   "/movies/film.mkv",
			want: []string{"movies/film.mkv", "movies"},
		},
		{
			name: "file at the mount root has no ancestors to add",
			in:   "/film.mkv",
			want: []string{"film.mkv"},
		},
		{
			name: "no leading slash",
			in:   "tv/Show.S01E01/ep.mkv",
			want: []string{"tv/Show.S01E01/ep.mkv", "tv/Show.S01E01", "tv"},
		},
		{
			name: "spaces are preserved",
			in:   "/movies/Fight Club (1999)/Fight.Club.1999.mkv",
			want: []string{
				"movies/Fight Club (1999)/Fight.Club.1999.mkv",
				"movies/Fight Club (1999)",
				"movies",
			},
		},
		{
			// Trimming one leading slash left the rest in place, so rclone saw
			// "/tv//Show//ep.mkv" with a slash still on the front.
			name: "duplicate slashes are collapsed",
			in:   "//tv//Show//ep.mkv",
			want: []string{"tv/Show/ep.mkv", "tv/Show", "tv"},
		},
		{
			name: "trailing slash does not survive",
			in:   "/tv/Show/",
			want: []string{"tv/Show", "tv"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := refreshDirsFor(tc.in)
			if !equalStrings(got, tc.want) {
				t.Errorf("refreshDirsFor(%q)\n got: %q\nwant: %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestRefreshDirsFor_NoWindowsSeparatorReachesRclone(t *testing.T) {
	// The property that broke in production: whatever separator the caller used,
	// nothing handed to rclone may carry one it does not understand. Only
	// meaningful where '\' is the separator, since on POSIX it is a legal
	// filename character and must be left alone.
	if runtime.GOOS != "windows" {
		t.Skip("backslash is a legal filename character on POSIX")
	}
	for _, in := range []string{`\tv\Show.S01E01\ep.mkv`, `\movies\film.mkv`, `\film.mkv`} {
		for _, dir := range refreshDirsFor(in) {
			if strings.Contains(dir, `\`) {
				t.Errorf("refreshDirsFor(%q) produced %q, which still carries a backslash", in, dir)
			}
			if dir == "" {
				t.Errorf("refreshDirsFor(%q) produced an empty directory", in)
			}
		}
	}
}

type countingVFSClient struct {
	calls  int
	onCall func(call int)
}

func (c *countingVFSClient) RefreshDir(context.Context, string, []string) error {
	c.calls++
	if c.onCall != nil {
		c.onCall(c.calls)
	}
	return nil
}

func (*countingVFSClient) ForgetDir(context.Context, string, []string) error { return nil }

func TestNotifyVFSReRefreshesUntilVisibleOnMount(t *testing.T) {
	oldAttempts, oldDelay := mountVisibilityAttempts, mountVisibilityRetryDelay
	mountVisibilityAttempts, mountVisibilityRetryDelay = 4, time.Millisecond
	t.Cleanup(func() { mountVisibilityAttempts, mountVisibilityRetryDelay = oldAttempts, oldDelay })

	const release = "/complete/tv/Show.S01E02"
	for _, tc := range []struct {
		name      string
		mountType config.MountType
		appearOn  int // refresh call after which the path appears; 0 = never
		wantCalls int
	}{
		{name: "visible after first refresh", mountType: config.MountTypeRClone, appearOn: 1, wantCalls: 1},
		{name: "visible after re-refresh", mountType: config.MountTypeRClone, appearOn: 3, wantCalls: 3},
		{name: "never visible gives up", mountType: config.MountTypeRClone, appearOn: 0, wantCalls: 4},
		{name: "external mount not checked", mountType: config.MountTypeRCloneExternal, appearOn: 0, wantCalls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mountPath := t.TempDir()
			client := &countingVFSClient{onCall: func(call int) {
				if call == tc.appearOn {
					if err := os.MkdirAll(filepath.Join(mountPath, filepath.FromSlash(release)), 0755); err != nil {
						t.Fatal(err)
					}
				}
			}}
			coordinator := NewCoordinator(Config{
				ConfigGetter: func() *config.Config {
					return &config.Config{MountType: tc.mountType, MountPath: mountPath}
				},
				RcloneClient: client,
			})
			coordinator.NotifyVFS(context.Background(), release, false)
			if client.calls != tc.wantCalls {
				t.Fatalf("RefreshDir calls = %d, want %d", client.calls, tc.wantCalls)
			}
		})
	}
}
