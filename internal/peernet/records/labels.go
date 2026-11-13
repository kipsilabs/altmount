package records

import (
	"strings"

	"github.com/kipsilabs/altmount/internal/nzbname"
)

// NormaliseBackboneLabel turns a provider's free-text backbone name into the
// label form records carry: trimmed, lower-cased, runs of whitespace joined by
// a single '-'. ok is false when the result is not a valid label (empty, too
// long, or characters outside [a-z0-9._-]). Publisher and receiver must use
// this one function so subset checks compare like with like.
func NormaliseBackboneLabel(raw string) (string, bool) {
	l := strings.Join(strings.Fields(strings.ToLower(raw)), "-")
	if !validBackboneLabel(l) {
		return "", false
	}
	return l, true
}

// CleanTitle applies the builder's title cleaning (invalid UTF-8 dropped,
// control characters, path separators and bidi controls replaced by spaces,
// whitespace collapsed, cut to MaxTitleLen). Every spelling of an NZB password
// ("{{pw}}", "/password:pw", "password=pw") is removed first so a password
// embedded in a file name never reaches a record.
func CleanTitle(s string) string { return cleanText(nzbname.StripPassword(s), MaxTitleLen) }
