package importer

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"

	"github.com/kipsilabs/altmount/internal/testsupport/nzbbuild"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakePwRecorder struct {
	mu    sync.Mutex
	calls [][]string
}

func (f *fakePwRecorder) RecordNZBPasswordFiles(_ context.Context, paths []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, paths)
}

// pwNZB writes a one-file NZB named name with the given meta password and
// subject suffix.
func pwNZB(e *batteryEnv, name, metaPW, subjectSuffix string) string {
	content := make([]byte, 4096)
	for i := range content {
		content[i] = byte(i % 251)
	}
	segs := e.registerContent("pw", content, 1024, 1.0, nil)
	n := nzbbuild.Build(nzbbuild.File{Subject: "movie.mkv" + subjectSuffix, Segments: segs})
	if metaPW != "" {
		n.Meta = map[string]string{"password": metaPW}
	}
	return nzbbuild.WriteTemp(e.t, n, name)
}

func runWithRecorder(t *testing.T, name, metaPW, subjectSuffix string, metadata *string) *fakePwRecorder {
	t.Helper()
	e := newBatteryEnv(t)
	rec := &fakePwRecorder{}
	e.proc.SetNZBPasswordRecorder(rec)
	p := pwNZB(e, name, metaPW, subjectSuffix)
	_, _, err := e.proc.ProcessNzbFile(context.Background(), p, filepath.Dir(p), 7, nil, nil, nil, nil, metadata, nil)
	require.NoError(t, err)
	return rec
}

func str(s string) *string { return &s }

func TestNZBPasswordOrigin_MetaPasswordOfTheReceivedNZBIsRecorded(t *testing.T) {
	rec := runWithRecorder(t, "Some.Release", "pw-from-the-nzb", "", nil)
	require.Len(t, rec.calls, 1)
	assert.Equal(t, []string{"/Some.Release.mkv"}, rec.calls[0])
}

func TestNZBPasswordOrigin_OtherOriginsAreNotRecorded(t *testing.T) {
	injected := MarkPasswordInjected(nil)
	nested := str(`{"_altmount_nzb_chain":{"depth":1,"seen":[]}}`)
	for name, tc := range map[string]struct {
		file, metaPW, subject string
		metadata              *string
	}{
		"password spelled in the NZB name":            {file: "Some.Release{{pw-in-name}}"},
		"password spelled in a subject":               {file: "Some.Release", subject: " {{pw-in-subject}}"},
		"no password at all":                          {file: "Some.Release"},
		"injected by the API (the &p= or form field)": {file: "Some.Release", metaPW: "typed-by-the-user", metadata: injected},
		"NZB found inside another release's archive":  {file: "Some.Release", metaPW: "inner", metadata: nested},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Empty(t, runWithRecorder(t, tc.file, tc.metaPW, tc.subject, tc.metadata).calls)
		})
	}
}

func TestNZBPasswordOrigin_NoRecorderMeansNoChange(t *testing.T) {
	e := newBatteryEnv(t)
	p := pwNZB(e, "Some.Release", "pw", "")
	_, written, err := e.proc.ProcessNzbFile(context.Background(), p, filepath.Dir(p), 7, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"/Some.Release.mkv"}, written)
}

func TestMarkPasswordInjectedKeepsExistingMetadata(t *testing.T) {
	got := MarkPasswordInjected(str(`{"imdb_id":"tt1","season":2}`))
	var m map[string]any
	require.NoError(t, json.Unmarshal([]byte(*got), &m))
	assert.Equal(t, "tt1", m["imdb_id"])
	assert.EqualValues(t, 2, m["season"])
	assert.True(t, passwordInjected(got))
	assert.False(t, passwordInjected(str(`{"imdb_id":"tt1"}`)))
	assert.False(t, passwordInjected(nil))
	assert.False(t, passwordInjected(str("not json")))
	assert.True(t, passwordInjected(MarkPasswordInjected(str("not json"))), "unreadable metadata is replaced")
	assert.NotContains(t, *got, "secret", "the password itself is never in the mark")
}
