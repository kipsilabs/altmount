package rclonecli

import (
	"path"
	"sort"
	"strings"
)

// refreshOrder returns dirs plus each one's parent, deduplicated and sorted
// shallowest first, ready for vfs/refresh.
//
// RefreshDir issues vfs/refresh without a preceding vfs/forget. rclone's
// forget clears the target directory's cached children (Dir.ForgetAll), so
// forgetting an existing ancestor such as "complete" detaches the
// "complete/tvAR" Dir node and the next listing builds a replacement. Any
// holder of the old node (the FUSE kernel until it revalidates the entry)
// keeps reading a listing no later refresh can reach, which is a suspected
// cause of freshly imported releases staying invisible through the mount.
// vfs/refresh alone already forces a re-read, reuses existing Dir nodes and
// drops entries that disappeared, so forget adds nothing for a refresh.
//
// Without forget a refresh only re-reads the directory it targets, and rclone
// resolves that directory by walking its cached ancestors. Forget used to mark
// the parent stale as a side effect; refreshing the parent first keeps that:
// a new leaf becomes reachable and a deleted one is dropped from the listing.
func refreshOrder(dirs []string) []string {
	ordered := make([]string, 0, 2*len(dirs))
	seen := make(map[string]struct{}, 2*len(dirs))
	add := func(dir string) {
		if _, dup := seen[dir]; dup {
			return
		}
		seen[dir] = struct{}{}
		ordered = append(ordered, dir)
	}
	for _, dir := range dirs {
		if dir == "" {
			continue
		}
		if parent, ok := parentDir(dir); ok {
			add(parent)
		}
		add(dir)
	}
	sort.SliceStable(ordered, func(i, j int) bool {
		return dirDepth(ordered[i]) < dirDepth(ordered[j])
	})
	return ordered
}

// parentDir returns the mount-relative parent of a VFS directory ("/" for a
// top-level one) and false for the root itself.
func parentDir(dir string) (string, bool) {
	trimmed := strings.Trim(dir, "/")
	if trimmed == "" || trimmed == "." {
		return "", false
	}
	parent := path.Dir(trimmed)
	if parent == "." {
		return "/", true
	}
	return parent, true
}

// dirDepth counts the path segments of a mount-relative VFS directory; the
// root ("/", "." or "") has depth zero.
func dirDepth(dir string) int {
	trimmed := strings.Trim(dir, "/")
	if trimmed == "" || trimmed == "." {
		return 0
	}
	return strings.Count(trimmed, "/") + 1
}
