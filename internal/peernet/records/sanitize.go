package records

import (
	"fmt"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

// disallowedRune reports runes that must never appear in peer-supplied text:
// control characters (including NUL), path separators, bidirectional
// override/isolate characters, and line/paragraph separators.
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

// cleanText repairs local input: invalid UTF-8 is dropped, disallowed runes
// become spaces, whitespace is collapsed and the result is cut to max bytes on
// a rune boundary. It is for the builder only; decoders reject instead.
func cleanText(s string, max int) string {
	s = strings.ToValidUTF8(s, "")
	s = strings.Map(func(r rune) rune {
		if disallowedRune(r) {
			return ' '
		}
		return r
	}, s)
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > max {
		s = s[:max]
		for !utf8.ValidString(s) {
			s = s[:len(s)-1]
		}
		s = strings.TrimSpace(s)
	}
	return s
}

// checkText validates a free string.
func checkText(name, s string, max int) error {
	if s == "" {
		return fmt.Errorf("%w: %s is empty", ErrInvalidField, name)
	}
	if len(s) > max {
		return fmt.Errorf("%w: %s too long", ErrInvalidField, name)
	}
	if !utf8.ValidString(s) {
		return fmt.Errorf("%w: %s is not valid UTF-8", ErrInvalidField, name)
	}
	for _, r := range s {
		if disallowedRune(r) {
			return fmt.Errorf("%w: %s contains a forbidden character", ErrInvalidField, name)
		}
	}
	return nil
}

// validIMDb reports whether s matches ^tt[0-9]{1,10}$.
func validIMDb(s string) bool {
	if len(s) < 3 || len(s) > 2+maxIMDbDigits || s[:2] != "tt" {
		return false
	}
	for i := 2; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func validBackboneLabel(s string) bool {
	if s == "" || len(s) > MaxBackboneLen {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '.' && c != '_' && c != '-' {
			return false
		}
	}
	return true
}

// checkBackbones requires a non-empty, strictly sorted (hence unique) list of
// valid labels within the count cap.
func checkBackbones(labels []string) error {
	if len(labels) == 0 || len(labels) > MaxBackbones {
		return fmt.Errorf("%w: backbone count %d", ErrInvalidField, len(labels))
	}
	for i, l := range labels {
		if !validBackboneLabel(l) {
			return fmt.Errorf("%w: backbone label", ErrInvalidField)
		}
		if i > 0 && labels[i-1] >= l {
			return fmt.Errorf("%w: backbones not sorted and unique", ErrInvalidField)
		}
	}
	return nil
}

// normaliseBackbones trims, lower-cases, sorts and de-duplicates a copy.
func normaliseBackbones(in []string) []string {
	out := make([]string, 0, len(in))
	for _, l := range in {
		out = append(out, strings.ToLower(strings.TrimSpace(l)))
	}
	sort.Strings(out)
	n := 0
	for i, l := range out {
		if i == 0 || l != out[n-1] {
			out[n] = l
			n++
		}
	}
	return out[:n]
}
