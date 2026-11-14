package bundle

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/kipsilabs/altmount/internal/holes"
)

// unnamedFile replaces a file name that sanitises to nothing ("", ".", "..").
const unnamedFile = "unnamed"

// maxExtLen is the longest extension kept when a long name is shortened.
const maxExtLen = 16

// disallowedRune reports runes that must never appear in peer-supplied text:
// control characters (including NUL), path separators, bidirectional
// override and isolate characters, and line or paragraph separators. It is
// the same set the release records use.
func disallowedRune(r rune) bool {
	switch {
	case unicode.IsControl(r), r == '/', r == '\\':
		return true
	case r >= 0x202A && r <= 0x202E, r >= 0x2066 && r <= 0x2069:
		return true
	case r == 0x200E, r == 0x200F, r == 0x061C, r == 0x2028, r == 0x2029, r == 0xFEFF:
		return true
	}
	return false
}

// cutBytes shortens s to at most max bytes on a rune boundary.
func cutBytes(s string, max int) string {
	if len(s) <= max {
		return s
	}
	s = s[:max]
	for !utf8.ValidString(s) {
		s = s[:len(s)-1]
	}
	return s
}

// cleanText repairs local input: invalid UTF-8 is dropped, disallowed runes
// become spaces, whitespace is collapsed and the result is cut to max bytes.
// It is for the builder only; decoders reject instead.
func cleanText(s string, max int) string {
	s = strings.ToValidUTF8(s, "")
	s = strings.Map(func(r rune) rune {
		if disallowedRune(r) {
			return ' '
		}
		return r
	}, s)
	s = strings.Join(strings.Fields(s), " ")
	return strings.TrimSpace(cutBytes(s, max))
}

// SanitizeTitle repairs a release title (builder side).
func SanitizeTitle(s string) string { return cleanText(s, MaxTitleLen) }

func checkTitle(s string) error {
	switch {
	case s == "":
		return fmt.Errorf("%w: title is empty", ErrInvalidField)
	case len(s) > MaxTitleLen:
		return fmt.Errorf("%w: title too long", ErrInvalidField)
	case !utf8.ValidString(s):
		return fmt.Errorf("%w: title is not valid UTF-8", ErrInvalidField)
	}
	for _, r := range s {
		if disallowedRune(r) {
			return fmt.Errorf("%w: title contains a forbidden character", ErrInvalidField)
		}
	}
	return nil
}

// reservedDevice reports Windows device names, with or without extension.
func reservedDevice(name string) bool {
	stem := name
	if i := strings.IndexByte(stem, '.'); i >= 0 {
		stem = stem[:i]
	}
	stem = strings.ToUpper(strings.TrimRight(stem, " "))
	switch stem {
	case "CON", "PRN", "AUX", "NUL":
		return true
	}
	if len(stem) >= 4 && (strings.HasPrefix(stem, "COM") || strings.HasPrefix(stem, "LPT")) {
		switch stem[3:] {
		case "1", "2", "3", "4", "5", "6", "7", "8", "9", "¹", "²", "³":
			return true
		}
	}
	return false
}

// shortenName cuts name to MaxFilenameLen bytes, keeping a short extension.
func shortenName(name string) string {
	if len(name) <= MaxFilenameLen {
		return name
	}
	ext := ""
	if i := strings.LastIndexByte(name, '.'); i > 0 && len(name)-i <= maxExtLen {
		ext = name[i:]
	}
	stem := cutBytes(name[:len(name)-len(ext)], MaxFilenameLen-len(ext))
	return strings.TrimRight(stem, ". ") + ext
}

// SanitizeFilename turns an untrusted file name into a safe base name: only
// the part after the last path separator is kept; control, bidirectional and
// separator characters become spaces; trailing dots and spaces are removed;
// ".", ".." and empty names become "unnamed"; Windows device names get a "_"
// prefix; the result is at most MaxFilenameLen bytes. The function is
// idempotent, and checkFilename accepts exactly its fixed points.
func SanitizeFilename(s string) string {
	s = strings.ToValidUTF8(s, "")
	if i := strings.LastIndexAny(s, `/\`); i >= 0 {
		s = s[i+1:]
	}
	s = strings.Map(func(r rune) rune {
		if disallowedRune(r) {
			return ' '
		}
		return r
	}, s)
	s = strings.TrimRight(strings.Join(strings.Fields(s), " "), ". ")
	if s == "" {
		return unnamedFile
	}
	s = shortenName(s)
	if reservedDevice(s) {
		s = shortenName("_" + s)
	}
	return s
}

func checkFilename(s string) error {
	if s == "" || len(s) > MaxFilenameLen || !utf8.ValidString(s) || SanitizeFilename(s) != s {
		return fmt.Errorf("%w: %q", ErrBadFilename, truncateForError(s))
	}
	return nil
}

func truncateForError(s string) string { return cutBytes(strings.ToValidUTF8(s, "?"), 40) }

// CanonicalMessageID strips one pair of angle brackets and validates the
// result: 1 to MaxMessageIDLen printable ASCII characters without brackets,
// and not a gap placeholder id. Message-IDs reach NNTP commands, so anything
// that could end a command line is refused.
func CanonicalMessageID(s string) (string, error) {
	s = strings.TrimPrefix(s, "<")
	s = strings.TrimSuffix(s, ">")
	if s == "" || len(s) > MaxMessageIDLen {
		return "", fmt.Errorf("%w: length %d", ErrBadMessageID, len(s))
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; c <= 0x20 || c >= 0x7f || c == '<' || c == '>' {
			return "", fmt.Errorf("%w: forbidden character", ErrBadMessageID)
		}
	}
	if holes.IsPlaceholderID(s) {
		return "", fmt.Errorf("%w: reserved placeholder prefix", ErrBadMessageID)
	}
	return s, nil
}
