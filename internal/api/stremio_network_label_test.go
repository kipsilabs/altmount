package api

import (
	"context"
	"net/url"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeLabeler struct {
	healthy map[string]bool
	calls   [][]string
}

func (f *fakeLabeler) NetworkHealthyPaths(_ context.Context, paths []string) map[string]bool {
	f.calls = append(f.calls, paths)
	out := map[string]bool{}
	for _, p := range paths {
		if f.healthy[p] {
			out[p] = true
		}
	}
	return out
}

func libraryStream(path string) fiber.Map {
	stream := stremioStreamFromPath(path, "http://host", "key")
	return fiber.Map{"name": stream.Name, "title": stream.Title, "url": stream.URL}
}

func TestLabelNetworkHealthy_FeatureOffChangesNothing(t *testing.T) {
	s := &Server{}
	in := []fiber.Map{libraryStream("movies/a/a.mkv"), libraryStream("movies/b/b.mkv")}
	want := []fiber.Map{libraryStream("movies/a/a.mkv"), libraryStream("movies/b/b.mkv")}
	assert.Equal(t, want, s.labelNetworkHealthy(context.Background(), in))

	// Removing the labeler restores the default.
	l := &fakeLabeler{healthy: map[string]bool{"movies/a/a.mkv": true}}
	s.SetStremioNetworkLabeler(l)
	s.SetStremioNetworkLabeler(nil)
	assert.Equal(t, want, s.labelNetworkHealthy(context.Background(), in))
	assert.Empty(t, l.calls)
}

func TestLabelNetworkHealthy_LabelsOnlyExactHealthyMatches(t *testing.T) {
	s := &Server{}
	l := &fakeLabeler{healthy: map[string]bool{"movies/a/a.mkv": true}}
	s.SetStremioNetworkLabeler(l)
	in := []fiber.Map{libraryStream("movies/a/a.mkv"), libraryStream("movies/b/b.mkv")}
	got := s.labelNetworkHealthy(context.Background(), in)

	require.Len(t, got, 2)
	assert.Equal(t, "a.mkv\n"+stremioNetworkHealthyLabel, got[0]["title"])
	assert.Equal(t, "b.mkv", got[1]["title"], "no match, no label")
	assert.Equal(t, "a.mkv", got[0]["name"], "only the description line carries the label")
	require.Len(t, l.calls, 1, "one batch lookup per request")
	assert.ElementsMatch(t, []string{"movies/a/a.mkv", "movies/b/b.mkv"}, l.calls[0])
}

func TestLabelNetworkHealthy_NoLookupWhenThereAreNoLibraryPaths(t *testing.T) {
	s := &Server{}
	l := &fakeLabeler{}
	s.SetStremioNetworkLabeler(l)
	// Indexer results are not in the library: no release ID, nothing to ask.
	in := []fiber.Map{{"name": "x", "title": "y", "url": "http://host/stremio/key/play?title=" + url.QueryEscape("t")}}
	got := s.labelNetworkHealthy(context.Background(), in)
	assert.Equal(t, in, got)
	assert.Empty(t, l.calls)
	assert.Empty(t, s.labelNetworkHealthy(context.Background(), nil))
}

func TestLabelNetworkHealthy_LabelIsAFixedLocalString(t *testing.T) {
	// Peer-supplied text can never reach the label: it is a constant.
	assert.Equal(t, "🌐 Healthy on the peer network", stremioNetworkHealthyLabel)
}
