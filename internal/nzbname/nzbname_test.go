package nzbname

import "testing"

func TestReleaseName(t *testing.T) {
	cases := []struct {
		in      string
		queueID bool
		want    string
	}{
		{"Movie.2020{{hunter2}}.nzb", false, "Movie.2020"},
		{"Movie.2020{{ hunter2 }}.nzb", false, "Movie.2020"},
		{"Movie.2020{{ spaced pw }}.NZB", false, "Movie.2020"},
		{"Movie.2020/password:hunter2.nzb", false, "Movie.2020"},
		{"Movie.2020/PWD=hunter2", false, "Movie.2020"},
		{"Movie.2020 password=hunter2.nzb", false, "Movie.2020"},
		{"Movie.2020{{.nzb", false, "Movie.2020"},
		{"Movie.2020}}.nzb", false, "Movie.2020"},
		{"/data/nzbs/42-Movie.2020{{pw}}.nzb", true, "Movie.2020"},
		{`C:\q\42-Movie.2020.nzbz`, true, "Movie.2020"},
		{"42-Movie.2020.nzb.gz", true, "Movie.2020"},
		{"2020-Movie.nzb", false, "2020-Movie"},
		{"{{pw}}.nzb", false, ""},
	}
	for _, c := range cases {
		if got := ReleaseName(c.in, c.queueID); got != c.want {
			t.Errorf("ReleaseName(%q,%v)=%q want %q", c.in, c.queueID, got, c.want)
		}
	}
}

func TestPassword(t *testing.T) {
	for in, want := range map[string]string{
		"a{{x}}.nzb": "x", "a{{ x y }}": "x y", "a/pwd=z": "z", "a/Password: q": "q", "plain": "", "a{{}}": "",
	} {
		if got := Password(in); got != want {
			t.Errorf("Password(%q)=%q want %q", in, got, want)
		}
	}
}

func TestStripMediaExtension(t *testing.T) {
	for in, want := range map[string]string{
		"Some.Movie.2020-GRP.mkv":  "Some.Movie.2020-GRP",
		"Some.Movie.2020-GRP.MP4":  "Some.Movie.2020-GRP",
		"Disc.Image.iso":           "Disc.Image",
		"Some.Movie.2020-GRP":      "Some.Movie.2020-GRP",
		"Some.Show.S01E01.x265":    "Some.Show.S01E01.x265",
		"Archive.part01.rar":       "Archive.part01.rar",
		"notes.txt":                "notes.txt",
		".mkv":                     ".mkv",
		"Name with dots v1.2.mkv ": "Name with dots v1.2",
	} {
		if got := StripMediaExtension(in); got != want {
			t.Errorf("StripMediaExtension(%q) = %q, want %q", in, got, want)
		}
	}
}
