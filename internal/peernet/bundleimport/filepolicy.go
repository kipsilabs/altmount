package bundleimport

import (
	"fmt"
	"path"
	"slices"
	"strings"

	"github.com/kipsilabs/altmount/internal/importer/parser/fileinfo"
	"github.com/kipsilabs/altmount/internal/importer/utils"
	"github.com/kipsilabs/altmount/internal/peernet/bundle"
)

// ForbiddenExtensions are file types a media server or the operating system
// could act on (playlists, links, scripts, executables, queue files). A peer
// bundle containing one is refused whatever the allowed-extensions setting
// says.
var ForbiddenExtensions = []string{
	".strm", ".nzb", ".nzbz", ".meta", ".url", ".lnk", ".m3u", ".m3u8", ".pls", ".desktop",
	".exe", ".com", ".scr", ".msi", ".dll", ".bat", ".cmd", ".ps1", ".vbs", ".js", ".jse", ".wsf", ".hta",
	".sh", ".bash", ".zsh", ".command", ".app", ".jar", ".py", ".pl", ".rb", ".php", ".apk", ".dmg", ".pkg", ".deb", ".rpm",
	".cpl", ".reg", ".inf", ".webloc",
}

var subtitleExtensions = []string{".srt", ".ass", ".sub", ".idx", ".sup"}

// fileKind says what a bundle file is for the local policy.
type fileKind int

const (
	kindOther   fileKind = iota // not written
	kindMedia                   // verified media, written
	kindSidecar                 // subtitle or nfo, written under a size cap
)

func ext(name string) string { return strings.ToLower(path.Ext(name)) }

func classify(name string) fileKind {
	e := ext(name)
	switch {
	case fileinfo.IsVerifiableMediaFile(name):
		return kindMedia
	case e == ".nfo" || slices.Contains(subtitleExtensions, e):
		return kindSidecar
	}
	return kindOther
}

// selectFiles applies the local file policy to the bundle's files and returns
// the indices to write, in bundle order. Forbidden types reject the bundle; a
// file the importer's allowed-extensions list excludes, and any file that is
// neither verifiable media nor a small sidecar, is simply not written.
func (i *Importer) selectFiles(b *bundle.Bundle) ([]int, Reason, error) {
	var allowed []string
	if i.cfg.AllowedExtensions != nil {
		allowed = i.cfg.AllowedExtensions()
	}
	pol := i.cfg.Policy
	var keep []int
	media, sidecars := 0, 0
	for n := range b.Files {
		f := &b.Files[n]
		if slices.Contains(ForbiddenExtensions, ext(f.Name)) {
			return nil, ReasonForbiddenFile, fmt.Errorf("file type %q is never accepted from a peer", ext(f.Name))
		}
		if !utils.IsAllowedFile(f.Name, f.Size, allowed, false) {
			continue
		}
		switch classify(f.Name) {
		case kindMedia:
			if media++; media > pol.MediaFiles() {
				return nil, ReasonTooManyFiles, fmt.Errorf("more than %d media files", pol.MediaFiles())
			}
		case kindSidecar:
			limit := pol.SubtitleBytes()
			if ext(f.Name) == ".nfo" {
				limit = pol.NfoBytes()
			}
			if f.Size > limit {
				return nil, ReasonSidecarTooLarge, fmt.Errorf("%s is %d bytes", f.Name, f.Size)
			}
			if sidecars++; sidecars > pol.Sidecars() {
				return nil, ReasonTooManyFiles, fmt.Errorf("more than %d sidecar files", pol.Sidecars())
			}
		default:
			continue
		}
		keep = append(keep, n)
	}
	if media == 0 {
		return nil, ReasonNoMediaFile, fmt.Errorf("no media file to write")
	}
	return keep, "", nil
}

// MediaFiles, Sidecars, SubtitleBytes and NfoBytes return the caps with their
// defaults for unset values.
func (p Policy) MediaFiles() int { return defaultInt(p.MaxMediaFiles, DefaultPolicy().MaxMediaFiles) }

func (p Policy) Sidecars() int { return defaultInt(p.MaxSidecars, DefaultPolicy().MaxSidecars) }

func (p Policy) SubtitleBytes() int64 {
	if p.MaxSubtitleBytes <= 0 {
		return DefaultPolicy().MaxSubtitleBytes
	}
	return p.MaxSubtitleBytes
}

func (p Policy) NfoBytes() int64 {
	if p.MaxNfoBytes <= 0 {
		return DefaultPolicy().MaxNfoBytes
	}
	return p.MaxNfoBytes
}

func defaultInt(v, def int) int {
	if v <= 0 {
		return def
	}
	return v
}

// subset returns a copy of b and loc holding only the files at idx. Identity
// (Release ID, File IDs) was verified over the full bundle before this.
func subset(b *bundle.Bundle, loc *bundle.Local, idx []int) (*bundle.Bundle, *bundle.Local) {
	nb, nl := *b, *loc
	nb.Files = make([]bundle.File, len(idx))
	nl.Files = make([]bundle.LocalFile, len(idx))
	nb.TotalSize = 0
	for k, n := range idx {
		nb.Files[k], nl.Files[k] = b.Files[n], loc.Files[n]
		nb.TotalSize += b.Files[n].Size
	}
	return &nb, &nl
}
