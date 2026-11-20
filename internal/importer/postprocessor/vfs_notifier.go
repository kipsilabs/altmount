package postprocessor

import (
	"context"
	"log/slog"
	"os"
	stdpath "path"
	"path/filepath"
	"strings"
	"time"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/pkg/rclonecli"
)

// NotifyVFS notifies rclone VFS about file changes
func (c *Coordinator) NotifyVFS(ctx context.Context, resultingPath string, async bool) {
	c.mu.RLock()
	rcloneClient := c.rcloneClient
	c.mu.RUnlock()

	c.notifyVFSWith(ctx, rcloneClient, resultingPath, async)
}

// notifyVFSWith notifies rclone VFS using the provided client (avoids re-locking)
func (c *Coordinator) notifyVFSWith(ctx context.Context, rcloneClient rclonecli.RcloneRcClient, resultingPath string, async bool) {
	if rcloneClient == nil {
		return
	}

	// Only notify for rclone-based mounts; FUSE and none don't use rclone VFS
	switch c.configGetter().MountType {
	case config.MountTypeRClone, config.MountTypeRCloneExternal:
		// continue
	default:
		return
	}

	refreshFunc := func(path string) {
		refreshCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()

		cfg := c.configGetter()
		vfsName := cfg.RClone.VFSName
		if vfsName == "" {
			vfsName = config.MountProvider
		}

		dir := path
		if c.metadataService != nil && c.metadataService.FileExists(path) && !c.metadataService.DirectoryExists(path) {
			dir = stdpath.Dir(rclonecli.ToVFSPath(path))
		}
		dirsToRefresh := refreshDirsFor(dir)

		slog.DebugContext(refreshCtx, "Notifying rclone VFS refresh", "dirs", dirsToRefresh, "vfs", vfsName)

		err := rcloneClient.RefreshDir(refreshCtx, vfsName, dirsToRefresh)
		if err != nil {
			slog.WarnContext(refreshCtx, "Failed to notify rclone VFS refresh",
				"dirs", dirsToRefresh,
				"error", err)
		} else {
			slog.InfoContext(refreshCtx, "Successfully notified rclone VFS refresh",
				"dirs", dirsToRefresh)
		}
	}

	if async {
		go refreshFunc(resultingPath)
	} else {
		refreshFunc(resultingPath)
	}
}

// RefreshMountPathIfNeeded refreshes the mount path cache if required
func (c *Coordinator) RefreshMountPathIfNeeded(ctx context.Context, resultingPath string, itemID int64) {
	c.mu.RLock()
	rcloneClient := c.rcloneClient
	c.mu.RUnlock()

	if rcloneClient == nil {
		return
	}

	// Only notify for rclone-based mounts; FUSE and none don't use rclone VFS
	switch c.configGetter().MountType {
	case config.MountTypeRClone, config.MountTypeRCloneExternal:
		// continue
	default:
		return
	}

	cfg := c.configGetter()
	mountPath := filepath.Join(cfg.MountPath, filepath.Dir(strings.TrimPrefix(resultingPath, "/")))

	if _, err := os.Stat(mountPath); err != nil {
		if os.IsNotExist(err) {
			vfsName := cfg.RClone.VFSName
			if vfsName == "" {
				vfsName = config.MountProvider
			}

			// Refresh the root path if the mount path is not found
			if err := rcloneClient.RefreshDir(ctx, vfsName, []string{"/"}); err != nil {
				c.log.ErrorContext(ctx, "Failed to refresh mount path",
					"queue_id", itemID,
					"path", mountPath,
					"error", err)
			}
		}
	}
}

// refreshDirsFor returns the release directory and up to two ancestors,
// since those directories may be new too.
//
// The ancestry is walked on the forward-slash form. rclone's VFS is
// forward-slash on every platform, and walking with filepath.Dir on Windows
// both yields backslash-separated entries (which match no node, while
// vfs/forget reports success regardless) and defeats the root guards below,
// since the Windows root is "\" rather than "/" - appending a useless bare-root
// entry to every batch.
//
// On POSIX the separator conversion is a no-op and path.Dir and filepath.Dir
// agree, so the only behaviour change there is Clean collapsing duplicate and
// trailing slashes, which previously leaked through to rclone.
func refreshDirsFor(p string) []string {
	// Use mount-relative directory paths and "/" for the root.
	normalize := func(s string) string {
		s = strings.TrimPrefix(s, "/")
		if s == "" || s == "." {
			return "/"
		}
		return s
	}

	// Clean once, up front, and walk the ancestry on the result. TrimPrefix
	// alone strips a single leading slash, so "//tv//Show//ep.mkv" previously
	// reached rclone still carrying one. Cleaning inside normalize instead
	// would leave the walk on the raw path, where Dir("/tv/Show/") is
	// "/tv/Show" and the first two entries come out identical.
	virtual := stdpath.Clean(rclonecli.ToVFSPath(p))
	dirs := []string{normalize(virtual)}

	parent := stdpath.Dir(virtual)
	if parent != "." && parent != "/" {
		dirs = append(dirs, normalize(parent))

		grandParent := stdpath.Dir(parent)
		if grandParent != "." && grandParent != "/" {
			dirs = append(dirs, normalize(grandParent))
		}
	}
	return dirs
}
