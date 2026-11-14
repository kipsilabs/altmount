package bundleimport

import (
	"context"
	"errors"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// exactLibrary behaves like a case- and normalisation-sensitive file system
// (the metadata service on the developer's machine may not): FileExists is an
// exact-name match against the real directory listing.
type exactLibrary struct{ Library }

func (l exactLibrary) FileExists(p string) bool {
	names, _ := l.ListDirectory(path.Dir(p))
	for _, n := range names {
		if n == path.Base(p) {
			return true
		}
	}
	return false
}

func TestAccept_CollisionIgnoresCaseAndNormalisation(t *testing.T) {
	for name, pair := range map[string][2]string{
		"case":          {"Movie.mkv", "movie.mkv"},
		"normalisation": {"café.mkv", "café.mkv"},
	} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			e.imp.cfg.Library = exactLibrary{e.lib}
			first := e.mkBundle("first", mkvSpec(pair[0]))
			_, err := e.imp.Accept(context.Background(), e.req(first))
			require.NoError(t, err)

			// Same slot, name differing only by case or normalisation form.
			second := e.mkBundle("second", fileSpec{name: pair[1], plain: mkvPlain(512), segSize: 128})
			_, err = e.imp.Accept(context.Background(), e.req(second))
			require.Error(t, err)
			assert.Equal(t, ReasonCollision, ReasonOf(err))
		})
	}
}

func TestAccept_SameFileInAnotherReleaseIsACollision(t *testing.T) {
	e := newEnv(t)
	b := e.mkBundle("rel-a", mkvSpec("movie.mkv"))
	_, err := e.imp.Accept(context.Background(), e.req(b))
	require.NoError(t, err)

	// Same file layout (same File ID) offered under a different Release ID.
	again := *b
	again.ReleaseID[0] ^= 0xFF
	req := e.req(&again)
	_, err = e.imp.checkTargets(&again, []string{"movies/Test.Release/movie.mkv"})
	require.Error(t, err, "the same File ID is only idempotent when the Release ID also matches")
	_ = req
	assert.True(t, strings.Contains(err.Error(), "release"), err.Error())
}

// orderLibrary records, at the moment each metadata file becomes visible,
// whether its pending state row already existed and whether the index already
// held it.
type orderLibrary struct {
	Library
	e      *env
	rowsAt map[string]bool
}

func (l *orderLibrary) WriteFileMetadata(p string, m *metapb.FileMetadata) error {
	_, ok, _ := l.e.repo.Get(context.Background(), p)
	l.rowsAt[p] = ok
	return l.Library.WriteFileMetadata(p, m)
}

func TestAccept_StateRowsExistBeforeAnyFileIsVisible(t *testing.T) {
	e := newEnv(t)
	lib := &orderLibrary{Library: e.lib, e: e, rowsAt: map[string]bool{}}
	e.imp.cfg.Library = lib
	b := e.mkBundle("ord", mkvSpec("a.mkv"), mkvSpec("b.mkv"))
	_, err := e.imp.Accept(context.Background(), e.req(b))
	require.NoError(t, err)
	require.Len(t, lib.rowsAt, 2)
	for p, had := range lib.rowsAt {
		assert.True(t, had, "%s was written before its pending row existed", p)
	}
}

type panicLibrary struct {
	Library
	n int
}

func (l *panicLibrary) WriteFileMetadata(p string, m *metapb.FileMetadata) error {
	if l.n++; l.n == 2 {
		panic("boom")
	}
	return l.Library.WriteFileMetadata(p, m)
}

func TestAccept_PanicWhileWritingLeavesNothingBehind(t *testing.T) {
	e := newEnv(t)
	e.imp.cfg.Library = &panicLibrary{Library: e.lib}
	e.imp.cfg.Index = database.NewPeernetContentRepository(e.db.Connection(), database.DialectSQLite)
	b := e.mkBundle("pan", mkvSpec("a.mkv"), mkvSpec("b.mkv"))
	assert.Panics(t, func() { _, _ = e.imp.Accept(context.Background(), e.req(b)) })
	e.assertNothingBehind()
}

type gateLibrary struct {
	Library
	mu      sync.Mutex
	writes  int
	entered chan struct{}
	release chan struct{}
}

func (l *gateLibrary) WriteFileMetadata(p string, m *metapb.FileMetadata) error {
	l.mu.Lock()
	l.writes++
	n := l.writes
	l.mu.Unlock()
	switch n {
	case 1:
		close(l.entered)
		<-l.release
	case 2:
		return errors.New("second writer fails")
	}
	return l.Library.WriteFileMetadata(p, m)
}

func TestAccept_ConcurrentAcceptsOfOneReleaseDoNotUndoEachOther(t *testing.T) {
	e := newEnv(t)
	idx := database.NewPeernetContentRepository(e.db.Connection(), database.DialectSQLite)
	e.imp.cfg.Index = idx
	lib := &gateLibrary{Library: e.lib, entered: make(chan struct{}), release: make(chan struct{})}
	e.imp.cfg.Library = lib
	b := e.mkBundle("conc", mkvSpec("a.mkv"))
	req := e.req(b)

	errA := make(chan error, 1)
	go func() { _, err := e.imp.Accept(context.Background(), req); errA <- err }()
	<-lib.entered
	errB := make(chan error, 1)
	go func() { _, err := e.imp.Accept(context.Background(), req); errB <- err }()
	time.Sleep(150 * time.Millisecond)
	close(lib.release)
	require.NoError(t, <-errA)
	<-errB // B may be "already imported" or a clean rejection; it must not break A

	p := "movies/Test.Release/a.mkv"
	assert.True(t, e.lib.FileExists(p), "A's file is still there")
	_, ok, err := idx.GetByPath(context.Background(), p)
	require.NoError(t, err)
	assert.True(t, ok, "A's index entry is still there")
	_, found, _ := e.repo.Get(context.Background(), p)
	assert.True(t, found, "A's state row is still there")
}
