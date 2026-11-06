package config

import (
	"path"
	"strings"
)

// corruptedFolderName is the safety folder at the mount root that holds
// corrupted copies (nzbfilesystem's corruptedDirName).
const corruptedFolderName = "corrupted_metadata"

// IsCategoryRootVirtualPath reports whether virtualPath, a path relative to
// the mount as metadata stores it, is a folder that holds the output of many
// jobs rather than one job's own output: the mount root, complete_dir, the
// corrupted folder, or a category folder. A category folder is a known
// category's name or dir (see knownCategoryNames), directly under
// complete_dir, or at the mount root when complete_dir is empty.
//
// A release folder of a job without a category sits directly under
// complete_dir too; its name is not a known category, so it is not a root.
//
// altmount never deletes such a path itself, and never asks an arr to remove a
// queue item from the client when the item's output is such a path: the arr
// would then delete the whole folder (#1029).
func (c *Config) IsCategoryRootVirtualPath(virtualPath string) bool {
	p := trimVirtual(virtualPath)
	if p == "" {
		return true
	}
	if strings.EqualFold(p, corruptedFolderName) {
		return true
	}

	cd := trimVirtual(c.SABnzbd.CompleteDir)
	rest := p
	if cd != "" {
		if strings.EqualFold(p, cd) {
			return true
		}
		if len(p) <= len(cd) || !strings.EqualFold(p[:len(cd)], cd) || p[len(cd)] != '/' {
			// Outside complete_dir: only a category dir configured with
			// its own path (for example "complete/tv" or "media/tv").
			return c.isKnownCategory(p)
		}
		rest = p[len(cd)+1:]
	}
	return c.isKnownCategory(rest) || c.isKnownCategory(p)
}

// IsCategoryRootDownloadPath is IsCategoryRootVirtualPath for an absolute path
// as an arr reports it, for example a queue item's OutputPath. A path under
// mount_path is judged by its part relative to mount_path. For a path outside
// mount_path (an arr that sees the mount somewhere else, or a Windows arr) it
// falls back to the folder names: the folder is named like complete_dir or the
// corrupted folder, or it is a known category name directly under a folder
// named like complete_dir (anywhere, when complete_dir is empty). Backslashes
// count as separators. The empty path and the root are always category roots.
func (c *Config) IsCategoryRootDownloadPath(p string) bool {
	clean := cleanSlash(p)
	if clean == "" || clean == "/" || clean == "." || isDriveRoot(clean) {
		return true
	}

	if c.MountPath != "" {
		mount := strings.TrimSuffix(cleanSlash(c.MountPath), "/")
		if strings.EqualFold(clean, mount) {
			return true
		}
		if len(clean) > len(mount) && strings.EqualFold(clean[:len(mount)], mount) && clean[len(mount)] == '/' {
			return c.IsCategoryRootVirtualPath(clean[len(mount)+1:])
		}
	}

	base := path.Base(clean)
	if strings.EqualFold(base, corruptedFolderName) {
		return true
	}
	cd := trimVirtual(c.SABnzbd.CompleteDir)
	if cd == "" {
		return c.isKnownCategoryBase(base)
	}
	// complete_dir is always prepended to a job's storage path, so a category
	// folder sits directly under it; a folder elsewhere that merely shares a
	// category's name is a release.
	cdBase := path.Base(cd)
	if strings.EqualFold(base, cdBase) {
		return true
	}
	return strings.EqualFold(path.Base(path.Dir(clean)), cdBase) && c.isKnownCategoryBase(base)
}

// isKnownCategoryBase is isKnownCategory on the last path element only, for a
// path seen from outside mount_path: a category dir configured as
// "complete/anime" is known there by "anime".
func (c *Config) isKnownCategoryBase(base string) bool {
	base = trimVirtual(base)
	if base == "" {
		return false
	}
	for _, n := range c.knownCategoryNames() {
		if strings.EqualFold(base, path.Base(n)) {
			return true
		}
	}
	return false
}

// isKnownCategory reports whether p equals, ignoring case, a configured
// SABnzbd category name or dir, the Default category's dir, or the category
// an arr instance is configured to use.
func (c *Config) isKnownCategory(p string) bool {
	p = trimVirtual(p)
	if p == "" {
		return false
	}
	for _, n := range c.knownCategoryNames() {
		if strings.EqualFold(p, n) {
			return true
		}
	}
	return false
}

func (c *Config) knownCategoryNames() []string {
	names := []string{DefaultCategoryName, DefaultCategoryDir}
	for _, cat := range c.SABnzbd.Categories {
		names = append(names, cat.Name, cat.Dir)
	}
	for _, list := range [][]ArrsInstanceConfig{
		c.Arrs.RadarrInstances, c.Arrs.SonarrInstances, c.Arrs.LidarrInstances,
		c.Arrs.ReadarrInstances, c.Arrs.WhisparrInstances, c.Arrs.SportarrInstances,
	} {
		for _, inst := range list {
			names = append(names, inst.Category)
		}
	}
	out := names[:0]
	for _, n := range names {
		if n = trimVirtual(n); n != "" {
			out = append(out, n)
		}
	}
	return out
}

// cleanSlash cleans p with "/" as the only separator, after turning
// backslashes into slashes, so a Windows arr's path is judged on Linux too.
func cleanSlash(p string) string {
	p = strings.TrimSpace(strings.ReplaceAll(p, "\\", "/"))
	if p == "" {
		return ""
	}
	return path.Clean(p)
}

// isDriveRoot reports whether p is a Windows drive root such as "C:" or "C:/".
func isDriveRoot(p string) bool {
	p = strings.TrimSuffix(p, "/")
	return len(p) == 2 && p[1] == ':'
}

func trimVirtual(p string) string {
	p = cleanSlash(p)
	if p == "." {
		return ""
	}
	return strings.Trim(p, "/")
}
