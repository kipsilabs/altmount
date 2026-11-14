package bundle

import (
	"errors"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestSanitizeFilename(t *testing.T) {
	long := strings.Repeat("a", 400) + ".mkv"
	tests := []struct {
		name, in, want string
	}{
		{"plain", "movie.mkv", "movie.mkv"},
		{"unix traversal", "../x", "x"},
		{"deep traversal", "../../etc/passwd", "passwd"},
		{"subdir", "a/b", "b"},
		{"backslash", `a\b`, "b"},
		{"mixed separators", `a/b\c.mkv`, "c.mkv"},
		{"windows path", `C:\Users\x\file.mkv`, "file.mkv"},
		{"nul inside", "a\x00b.mkv", "a b.mkv"},
		{"nul traversal", "..\x00/x", "x"},
		{"dot", ".", "unnamed"},
		{"dotdot", "..", "unnamed"},
		{"dotdot slash", "../", "unnamed"},
		{"empty", "", "unnamed"},
		{"only spaces", "   ", "unnamed"},
		{"trailing dots", "name...", "name"},
		{"trailing space and dot", "name. .", "name"},
		{"leading dotdot name stays a name", "..x", "..x"},
		{"bidi override", "a\u202Eevil.mkv", "a evil.mkv"},
		{"bidi isolate", "a\u2066b", "a b"},
		{"line separator", "a\u2028b", "a b"},
		{"bom", "\uFEFFa", "a"},
		{"control", "a\tb\nc", "a b c"},
		{"invalid utf8", "a\xffb", "ab"},
		{"collapse whitespace", "a   b", "a b"},
		{"reserved CON", "CON", "_CON"},
		{"reserved con ext", "con.txt", "_con.txt"},
		{"reserved COM1", "COM1.mkv", "_COM1.mkv"},
		{"reserved LPT9", "lpt9", "_lpt9"},
		{"reserved NUL", "NUL", "_NUL"},
		{"not reserved", "console.mkv", "console.mkv"},
		{"reserved superscript", "COM\u00B9", "_COM\u00B9"},
		{"long", long, strings.Repeat("a", 251) + ".mkv"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := SanitizeFilename(tc.in)
			if got != tc.want {
				t.Fatalf("SanitizeFilename(%q) = %q, want %q", tc.in, got, tc.want)
			}
			if again := SanitizeFilename(got); again != got {
				t.Fatalf("not idempotent: %q -> %q", got, again)
			}
			if err := checkFilename(got); err != nil {
				t.Fatalf("sanitised name rejected: %v", err)
			}
			if len(got) > MaxFilenameLen || !utf8.ValidString(got) {
				t.Fatalf("bad length or encoding: %q", got)
			}
		})
	}
}

func TestSanitizeFilenameLongMultibyteCutOnRune(t *testing.T) {
	in := strings.Repeat("é", 300)
	got := SanitizeFilename(in)
	if len(got) > MaxFilenameLen || !utf8.ValidString(got) || got == "" {
		t.Fatalf("got %d bytes valid=%v", len(got), utf8.ValidString(got))
	}
	if got != strings.Repeat("é", 127) {
		t.Fatalf("got %q", got)
	}
}

func TestCheckFilenameRejects(t *testing.T) {
	for _, in := range []string{"", ".", "..", "a/b", `a\b`, "a\x00b", "../x", " a", "a ", "a.", "CON", "con.txt",
		"a\u202Eb", "a  b", "a\xffb", strings.Repeat("a", 256)} {
		if err := checkFilename(in); !errors.Is(err, ErrBadFilename) {
			t.Errorf("checkFilename(%q) = %v, want ErrBadFilename", in, err)
		}
	}
	if err := checkFilename("ok name.mkv"); err != nil {
		t.Fatal(err)
	}
}

func TestSanitizeTitle(t *testing.T) {
	if got := SanitizeTitle("  A\u202E  b/c\\d\x00 "); got != "A b c d" {
		t.Fatalf("got %q", got)
	}
	if got := SanitizeTitle(strings.Repeat("x", 1000)); len(got) != MaxTitleLen {
		t.Fatalf("len %d", len(got))
	}
	if got := SanitizeTitle(""); got != "" {
		t.Fatalf("got %q", got)
	}
	if err := checkTitle("a\u202Eb"); !errors.Is(err, ErrInvalidField) {
		t.Fatalf("err %v", err)
	}
	if err := checkTitle(""); !errors.Is(err, ErrInvalidField) {
		t.Fatalf("empty title: %v", err)
	}
}

func TestCanonicalMessageID(t *testing.T) {
	ok := map[string]string{
		"abc@x.test":   "abc@x.test",
		"<abc@x.test>": "abc@x.test",
		"a$b%c@x.test": "a$b%c@x.test",
	}
	for in, want := range ok {
		got, err := CanonicalMessageID(in)
		if err != nil || got != want {
			t.Errorf("%q -> %q, %v", in, got, err)
		}
	}
	bad := []string{"", "<>", "a b@x", "a\r\nQUIT@x", "a\x00@x", "a\x7f@x", "é@x", "a<b@x", "a>b@x",
		"<<a@x>>", strings.Repeat("a", MaxMessageIDLen+1), "altmount-gap-1-abc", "<altmount-gap-2-x>"}
	for _, in := range bad {
		if _, err := CanonicalMessageID(in); !errors.Is(err, ErrBadMessageID) {
			t.Errorf("%q: %v, want ErrBadMessageID", in, err)
		}
	}
}
