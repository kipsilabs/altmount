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
	return c.isKnownCategory(rest) || c.isKnownCategory(p) || c.isCategoryAncestor(rest)
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
	parts := strings.Split(strings.Trim(clean, "/"), "/")
	if cd == "" {
		return c.matchesCategorySuffix(parts, "")
	}
	// complete_dir is always prepended to a job's storage path, so a category
	// folder sits under a folder named like complete_dir; a folder elsewhere
	// that merely shares a category's name is a release.
	cdBase := path.Base(cd)
	if strings.EqualFold(base, cdBase) {
		return true
	}
	return c.matchesCategorySuffix(parts, cdBase)
}

// matchesCategorySuffix reports whether the path made of parts ends in a
// known category folder, or in a folder above one, relative to complete_dir
// ("tv", "tv/uhd", or "tv" for a category dir "tv/uhd"). When cdBase is set,
// the folder just before that suffix must be named cdBase. This is how a path
// seen from outside mount_path is judged.
func (c *Config) matchesCategorySuffix(parts []string, cdBase string) bool {
	for _, rel := range c.categoryDirsUnderComplete() {
		relParts := strings.Split(rel, "/")
		for j := 1; j <= len(relParts); j++ {
			start := len(parts) - j
			if start < 0 || (cdBase != "" && start < 1) {
				continue
			}
			match := true
			for k := 0; k < j; k++ {
				if !strings.EqualFold(parts[start+k], relParts[k]) {
					match = false
					break
				}
			}
			if match && (cdBase == "" || strings.EqualFold(parts[start-1], cdBase)) {
				return true
			}
		}
	}
	return false
}

// isCategoryAncestor reports whether rest, a path relative to complete_dir, is
// a folder above a known category folder, such as "tv" for a category dir
// "tv/uhd". Such a folder holds that category's jobs too.
func (c *Config) isCategoryAncestor(rest string) bool {
	rest = trimVirtual(rest)
	if rest == "" {
		return false
	}
	prefix := strings.ToLower(rest) + "/"
	for _, rel := range c.categoryDirsUnderComplete() {
		if strings.HasPrefix(strings.ToLower(rel), prefix) {
			return true
		}
	}
	return false
}

// categoryDirsUnderComplete returns the known category names and dirs
// relative to complete_dir: a dir configured with complete_dir in it
// ("complete/anime") loses that prefix.
func (c *Config) categoryDirsUnderComplete() []string {
	cd := trimVirtual(c.SABnzbd.CompleteDir)
	names := c.knownCategoryNames()
	out := make([]string, 0, len(names))
	for _, n := range names {
		if cd != "" && len(n) > len(cd) && strings.EqualFold(n[:len(cd)], cd) && n[len(cd)] == '/' {
			n = n[len(cd)+1:]
		}
		out = append(out, n)
	}
	return out
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
