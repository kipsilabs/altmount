package config

import "testing"

func categoryRootsConfig() *Config {
	return &Config{
		MountPath: "/mnt/altmount",
		SABnzbd: SABnzbdConfig{
			CompleteDir: "/complete",
			Categories: []SABnzbdCategory{
				{Name: "tv"},
				{Name: "tv-uhd", Dir: "shows-UHD"},
				{Name: "anime", Dir: "complete/anime-dir"},
			},
		},
		Arrs: ArrsConfig{RadarrInstances: []ArrsInstanceConfig{{Name: "radarr", Category: "movies"}}},
	}
}

func TestIsCategoryRootVirtualPath(t *testing.T) {
	c := categoryRootsConfig()
	for _, tt := range []struct {
		path string
		want bool
	}{
		{"", true},
		{"/", true},
		{".", true},
		{"complete", true},
		{"/complete/", true},
		{"corrupted_metadata", true},
		{"complete/shows-UHD", true},
		{"complete/SHOWS-uhd/", true},
		{"complete/tv-uhd", true},
		{"complete/movies", true},    // an arr instance's category
		{"complete/complete", true},  // the Default category's dir
		{"complete/anime-dir", true}, // a dir configured with complete_dir in it
		{"shows-UHD", true},          // a category dir at the mount root
		{`complete\shows-UHD`, true},
		// A job without a category is stored directly under complete_dir.
		{"complete/Some.Release.2024", false},
		{"complete/Some.Release.2024/file.mkv", false},
		{"complete/shows-UHD/Release.S01", false},
		{"complete/tv/Release/ep.mkv", false},
		{"complete/tv/ep.mkv", false},
		{"other", false},
		{"other/Release", false},
	} {
		if got := c.IsCategoryRootVirtualPath(tt.path); got != tt.want {
			t.Errorf("IsCategoryRootVirtualPath(%q) = %v, want %v", tt.path, got, tt.want)
		}
	}
}

func TestIsCategoryRootDownloadPath(t *testing.T) {
	c := categoryRootsConfig()
	for _, tt := range []struct {
		path string
		want bool
	}{
		{"", true},
		{"  ", true},
		{"/", true},
		{"/mnt/altmount", true},
		{"/mnt/altmount/", true},
		{"/mnt/altmount/complete", true},
		{"/mnt/altmount/complete/shows-UHD", true},
		{"/mnt/altmount/complete/shows-UHD/", true},
		{"/mnt/altmount/complete/shows-UHD/Release.S01", false},
		{"/mnt/altmount/complete/tv/Release/ep.mkv", false},
		{"/mnt/altmount/complete/Uncategorised.Release", false},
		// An arr that sees the mount elsewhere: judged by folder names.
		{"/data/usenet/complete", true},
		{"/data/usenet/complete/tv", true},
		{"/data/usenet/corrupted_metadata", true},
		{"/data/usenet/complete/tv/Movies", false}, // a release named like a category
		{"/data/usenet/complete/anime-dir", true},  // dir configured as complete/anime-dir
		{"/data/usenet/complete/Uncategorised.Release", false},
		{"/data/usenet/complete/tv/Release", false},
		{"/data/usenet/other/Release", false},
		// A Windows arr: backslashes are separators.
		{`D:\usenet\complete\shows-UHD`, true},
		{`D:\usenet\complete\shows-UHD\`, true},
		{`D:\`, true},
		{`D:\usenet\complete\shows-UHD\Release.S01`, false},
		// Not a prefix match on the mount path.
		{"/mnt/altmount2/complete/tv/Release", false},
	} {
		if got := c.IsCategoryRootDownloadPath(tt.path); got != tt.want {
			t.Errorf("IsCategoryRootDownloadPath(%q) = %v, want %v", tt.path, got, tt.want)
		}
	}
}

// A category dir with a slash ("tv/uhd") is nested under complete_dir. It is a
// root whether the arr sees the mount at mount_path or somewhere else, and so
// is the folder above it, which holds that category too.
func TestIsCategoryRoot_NestedCategoryDir(t *testing.T) {
	cfg := &Config{
		MountPath: "/mnt/altmount",
		SABnzbd: SABnzbdConfig{
			CompleteDir: "/complete",
			Categories:  []SABnzbdCategory{{Name: "tv-uhd", Dir: "tv/uhd"}},
		},
	}
	for _, tt := range []struct {
		path string
		want bool
	}{
		// Arr sees the mount at mount_path.
		{"/mnt/altmount/complete/tv/uhd", true},
		{"/mnt/altmount/complete/tv/uhd/", true},
		{"/mnt/altmount/complete/tv", true},
		{"/mnt/altmount/complete/tv/uhd/Some.Show.S01", false},
		// Arr sees the mount somewhere else.
		{"/data/complete/tv/uhd", true},
		{"/data/complete/TV/UHD/", true},
		{"/data/complete/tv", true},
		{`D:\usenet\complete\tv\uhd`, true},
		{"/data/complete/tv/uhd/Some.Show.S01", false},
		{"/data/elsewhere/tv/uhd", false},
	} {
		if got := cfg.IsCategoryRootDownloadPath(tt.path); got != tt.want {
			t.Errorf("IsCategoryRootDownloadPath(%q) = %v, want %v", tt.path, got, tt.want)
		}
	}
	for _, tt := range []struct {
		path string
		want bool
	}{
		{"complete/tv/uhd", true},
		{"complete/tv", true},
		{"complete/tv/uhd/Some.Show.S01", false},
	} {
		if got := cfg.IsCategoryRootVirtualPath(tt.path); got != tt.want {
			t.Errorf("IsCategoryRootVirtualPath(%q) = %v, want %v", tt.path, got, tt.want)
		}
	}
}
