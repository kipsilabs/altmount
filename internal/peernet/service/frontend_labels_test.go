package service

import (
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The status page shows these machine keys with plain-language labels. The
// frontend test lists every key the backend can emit; this test keeps those
// lists equal to the keys found in the Go sources, so a new backend key without
// a label (or a stale entry) fails here and not silently in the UI.

func goStrings(t *testing.T, file, pattern string) []string {
	t.Helper()
	src, err := os.ReadFile(file)
	require.NoError(t, err)
	var out []string
	for _, m := range regexp.MustCompile(pattern).FindAllStringSubmatch(string(src), -1) {
		if m[1] != "" {
			out = append(out, m[1])
		}
	}
	return out
}

func tsList(t *testing.T, name string) []string {
	t.Helper()
	src, err := os.ReadFile("../../../frontend/tests/peernetStats.test.ts")
	require.NoError(t, err)
	m := regexp.MustCompile(`(?s)const ` + name + ` = \[(.*?)\];`).FindStringSubmatch(string(src))
	require.NotNil(t, m, "list %s not found in the frontend test", name)
	var out []string
	for _, k := range regexp.MustCompile(`"([^"]+)"`).FindAllStringSubmatch(m[1], -1) {
		out = append(out, k[1])
	}
	return out
}

func sorted(in ...[]string) []string {
	seen := map[string]bool{}
	var out []string
	for _, l := range in {
		for _, k := range l {
			if !seen[k] {
				seen[k] = true
				out = append(out, k)
			}
		}
	}
	sort.Strings(out)
	return out
}

func TestFrontendLabelListsMatchTheBackendKeys(t *testing.T) {
	const peernet = "../"
	for name, backend := range map[string][]string{
		"BACKEND_REJECTION_KEYS": goStrings(t, peernet+"recsync/reasons.go", `Reason\w+:\s+"([a-z0-9_]+)",`),
		"BACKEND_SKIP_KEYS": sorted(
			goStrings(t, peernet+"publish/publish.go", `Skip\w+\s+SkipReason = "([a-z0-9_]*)"`),
			goStrings(t, "publishing.go", `\bskip\w+\s+= "([a-z0-9_]+)"`)),
		"BACKEND_NOT_SHAREABLE_KEYS": sorted(
			goStrings(t, peernet+"bundle/build.go", `Reason\w+\s+ShareReason = "([a-z0-9_]+)"`),
			goStrings(t, peernet+"bundlex/builder.go", `Reason\w+\s+= "([a-z0-9_]+)"`),
			[]string{"other"}),
		"BACKEND_SERVE_REFUSED_KEYS": sorted(
			goStrings(t, peernet+"bundlex/serve.go", `(?:deny\(w|refuse)\(?,?\s*"([a-z0-9_]+)"`),
			[]string{"aborted"}),
		"BACKEND_FETCH_FAILED_KEYS":  goStrings(t, peernet+"bundlex/fetch.go", `\bfail\w+\s+= "([a-z0-9_]+)"`),
		"BACKEND_IMPORT_REJECT_KEYS": goStrings(t, peernet+"bundleimport/types.go", `Reason\w+\s+Reason = "([a-z0-9_]+)"`),
	} {
		t.Run(strings.ToLower(name), func(t *testing.T) {
			require.NotEmpty(t, backend)
			assert.Equal(t, sorted(backend), sorted(tsList(t, name)))
		})
	}
}
