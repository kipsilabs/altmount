package bundleimport

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAccept_RefusesFilesAMediaServerCouldAct(t *testing.T) {
	for _, name := range []string{"x.strm", "x.nzb", "x.NZBZ", "x.meta", "x.url", "x.lnk", "x.m3u", "x.m3u8", "x.pls", "x.desktop", "x.exe", "x.sh", "x.bat", "x.cmd", "x.ps1", "x.py", "x.jar"} {
		e := newEnv(t)
		b := e.mkBundle("fb", mkvSpec("movie.mkv"), fileSpec{name: name, plain: junkPlain(200), segSize: 128})
		_, err := e.imp.Accept(context.Background(), e.req(b))
		require.Error(t, err, name)
		assert.Equal(t, ReasonForbiddenFile, ReasonOf(err), name)
		e.assertNothingBehind()
	}
}

func TestAccept_EveryWrittenMediaFileIsVerifiedNotOnlyTheLargest(t *testing.T) {
	e := newEnv(t)
	specs := []fileSpec{}
	for i := 0; i < 5; i++ {
		specs = append(specs, mkvSpec(fmt.Sprintf("good%d.mkv", i)))
	}
	// Smaller than the others, so a "probe the four largest" policy skips it.
	specs = append(specs, fileSpec{name: "bad.mkv", plain: junkPlain(300), segSize: 128})
	b := e.mkBundle("pm", specs...)
	_, err := e.imp.Accept(context.Background(), e.req(b))
	require.Error(t, err)
	assert.Equal(t, ReasonMediaInvalid, ReasonOf(err))
	e.assertNothingBehind()
}

func TestAccept_ExtensionAllowListAndSidecarBounds(t *testing.T) {
	t.Run("allow list skips what the importer would skip", func(t *testing.T) {
		e := newEnv(t)
		e.imp.cfg.AllowedExtensions = func() []string { return []string{".mkv"} }
		b := e.mkBundle("al", mkvSpec("movie.mkv"), fileSpec{name: "movie.srt", plain: junkPlain(100), segSize: 128})
		res, err := e.imp.Accept(context.Background(), e.req(b))
		require.NoError(t, err)
		assert.Equal(t, []string{"movies/Test.Release/movie.mkv"}, res.Paths)
		assert.False(t, e.lib.FileExists("movies/Test.Release/movie.srt"))
	})
	t.Run("too many sidecars", func(t *testing.T) {
		e := newEnv(t)
		e.imp.cfg.Policy.MaxSidecars = 2
		specs := []fileSpec{mkvSpec("movie.mkv")}
		for i := 0; i < 3; i++ {
			specs = append(specs, fileSpec{name: fmt.Sprintf("s%d.srt", i), plain: junkPlain(100), segSize: 128})
		}
		_, err := e.imp.Accept(context.Background(), e.req(e.mkBundle("sc", specs...)))
		require.Error(t, err)
		assert.Equal(t, ReasonTooManyFiles, ReasonOf(err))
	})
	t.Run("an oversized nfo", func(t *testing.T) {
		e := newEnv(t)
		e.imp.cfg.Policy.MaxNfoBytes = 50
		_, err := e.imp.Accept(context.Background(), e.req(e.mkBundle("nf", mkvSpec("movie.mkv"), fileSpec{name: "movie.nfo", plain: junkPlain(100), segSize: 128})))
		require.Error(t, err)
		assert.Equal(t, ReasonSidecarTooLarge, ReasonOf(err))
	})
	t.Run("other files are not written", func(t *testing.T) {
		e := newEnv(t)
		res, err := e.imp.Accept(context.Background(), e.req(e.mkBundle("ot", mkvSpec("movie.mkv"), fileSpec{name: "readme.txt", plain: junkPlain(100), segSize: 128})))
		require.NoError(t, err)
		assert.Len(t, res.Paths, 1)
	})
}

func TestAccept_FilesThatWillNeverBeHealthCheckedAreNotLeftPending(t *testing.T) {
	e := newEnv(t)
	e.imp.cfg.WillBeChecked = func(p string) bool { return !strings.HasSuffix(p, ".srt") }
	b := e.mkBundle("nc", mkvSpec("movie.mkv"), fileSpec{name: "movie.srt", plain: junkPlain(100), segSize: 128})
	res, err := e.imp.Accept(context.Background(), e.req(b))
	require.NoError(t, err)
	var srt string
	for _, p := range res.Paths {
		if strings.HasSuffix(p, ".srt") {
			srt = p
		}
	}
	pending := e.imp.PendingFullCheck(context.Background(), res.Paths)
	assert.NotContains(t, pending, srt, "a sidecar no health record is ever created for must not stay pending")
	assert.Len(t, pending, 1, "the media file is still pending")
}
