// Package nzbname holds the pure string rules for NZB names: which spellings
// carry an archive password and how a stored NZB name becomes a plain release
// name. The importer extracts passwords with the same expressions the peer
// network uses to strip them, so the two cannot drift apart.
package nzbname

import (
	"regexp"
	"strings"
)

var (
	// BracketsRegex matches the "{{password}}" form.
	BracketsRegex = regexp.MustCompile(`(?i)\{\{\s*([^{}]+?)\s*\}\}`)
	// SlashRegex matches the "/password:xyz" and "/pwd=xyz" forms.
	SlashRegex = regexp.MustCompile(`(?i)/(?:password|pwd)[:=]\s*([^\s/]+)`)

	// looseRegex catches "password=xyz" style fragments the importer does not
	// read but that must still never leave the node.
	looseRegex = regexp.MustCompile(`(?i)\b(?:password|pwd|pass)\s*[:=]\s*\S+`)
	// strayBraces are leftovers of an unbalanced "{{" or "}}".
	strayBraces = regexp.MustCompile(`\{\{|\}\}`)
)

// Password returns the first password spelled in s ("{{pw}}" first, then the
// slash form), trimmed, or "" when there is none. It is what the importer
// reads from NZB file names and subjects.
func Password(s string) string {
	if m := BracketsRegex.FindStringSubmatch(s); len(m) > 1 {
		if t := strings.TrimSpace(m[1]); t != "" {
			return t
		}
	}
	if m := SlashRegex.FindStringSubmatch(s); len(m) > 1 {
		if t := strings.TrimSpace(m[1]); t != "" {
			return t
		}
	}
	return ""
}

// StripPassword removes every password-carrying spelling from s: the importer's
// two forms, loose "password=xyz" fragments and any stray "{{" or "}}".
func StripPassword(s string) string {
	s = BracketsRegex.ReplaceAllString(s, "")
	s = SlashRegex.ReplaceAllString(s, "")
	s = looseRegex.ReplaceAllString(s, "")
	s = strayBraces.ReplaceAllString(s, "")
	return strings.TrimSpace(s)
}

// ReleaseName turns an NZB name or path into the plain release name: password
// spellings removed, directories dropped, the NZB file suffix (.nzb, .nzbz,
// .nzb.gz) removed and, when queueID is set, the "<id>-" prefix the importer
// puts on queued files. The result never contains a path separator.
func ReleaseName(nameOrPath string, queueID bool) string {
	s := StripPassword(strings.ReplaceAll(nameOrPath, "\\", "/"))
	s = strings.TrimSpace(s[strings.LastIndexByte(s, '/')+1:])
	if queueID {
		if i := strings.IndexByte(s, '-'); i > 0 && allDigits(s[:i]) {
			s = s[i+1:]
		}
	}
	for _, suf := range []string{".nzb.gz", ".nzbz", ".nzb"} {
		if len(s) >= len(suf) && strings.EqualFold(s[len(s)-len(suf):], suf) {
			s = s[:len(s)-len(suf)]
			break
		}
	}
	return strings.TrimSpace(StripPassword(s))
}

func allDigits(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return s != ""
}

// mediaExtensions are the video and disc-image container extensions
// StripMediaExtension removes.
var mediaExtensions = map[string]struct{}{
	".mkv": {}, ".mp4": {}, ".m4v": {}, ".avi": {}, ".mov": {}, ".wmv": {}, ".flv": {}, ".webm": {},
	".mpg": {}, ".mpeg": {}, ".ts": {}, ".m2ts": {}, ".vob": {}, ".divx": {}, ".iso": {},
}

// StripMediaExtension removes one trailing media or container extension, for
// names that are a media file's name rather than a release name (an NZB named
// after its single file). Other names, and names that are only an extension,
// come back trimmed but unchanged.
func StripMediaExtension(name string) string {
	name = strings.TrimSpace(name)
	i := strings.LastIndexByte(name, '.')
	if i <= 0 {
		return name
	}
	if _, ok := mediaExtensions[strings.ToLower(name[i:])]; !ok {
		return name
	}
	return name[:i]
}
