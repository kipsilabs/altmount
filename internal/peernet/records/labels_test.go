package records

import (
	"strings"
	"testing"
)

func TestNormaliseBackboneLabel(t *testing.T) {
	cases := []struct {
		in   string
		want string
		ok   bool
	}{
		{"Omicron", "omicron", true},
		{"  Omicron  ", "omicron", true},
		{"Eweka Internet Services", "eweka-internet-services", true},
		{"a \t b", "a-b", true},
		{"usenet.farm_1", "usenet.farm_1", true},
		{"", "", false},
		{"   ", "", false},
		{"bad/label", "", false},
		{"bad\x00label", "", false},
		{"ünïcode", "", false},
		{strings.Repeat("a", MaxBackboneLen), strings.Repeat("a", MaxBackboneLen), true},
		{strings.Repeat("a", MaxBackboneLen+1), "", false},
	}
	for _, c := range cases {
		got, ok := NormaliseBackboneLabel(c.in)
		if got != c.want || ok != c.ok {
			t.Errorf("NormaliseBackboneLabel(%q) = %q,%v want %q,%v", c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestCleanTitle(t *testing.T) {
	if got := CleanTitle("a\x00b/c\\d\u202e e"); got != "a b c d e" {
		t.Fatalf("got %q", got)
	}
	long := CleanTitle(strings.Repeat("é", MaxTitleLen))
	if len(long) > MaxTitleLen {
		t.Fatalf("title not capped: %d", len(long))
	}
}
