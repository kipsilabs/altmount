package bundle

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// refs returns the pool indices of the given ids.
func poolIdx(t *testing.T, b *Bundle, id string) int64 {
	t.Helper()
	for i, p := range b.Pool {
		if p == id {
			return int64(i)
		}
	}
	t.Fatalf("%s not in pool", id)
	return 0
}

func TestNZBCoverageRules(t *testing.T) {
	t.Run("good bundle passes", func(t *testing.T) {
		require.NoError(t, Validate(goodNZBBundle(t)))
	})
	bad := map[string]func(t *testing.T, b *Bundle){
		"segment used twice in one file": func(t *testing.T, b *Bundle) {
			f := fileNamed(t, b, "movie.mkv")
			f.Segments[2].Ref = f.Segments[1].Ref
		},
		"segments out of NZB order": func(t *testing.T, b *Bundle) {
			f := fileNamed(t, b, "movie.mkv")
			f.Segments[0].Ref, f.Segments[1].Ref = f.Segments[1].Ref, f.Segments[0].Ref
		},
		"file covers only part of its NZB entry": func(t *testing.T, b *Bundle) {
			f := fileNamed(t, b, "movie.mkv")
			f.Segments = f.Segments[:2]
			f.Size = 2000
			b.TotalSize -= 500
		},
		"file spans two NZB entries": func(t *testing.T, b *Bundle) {
			f := fileNamed(t, b, "movie.mkv")
			f.Segments[2].Ref = poolIdx(t, b, "p1@x.test")
			f.Segments[2].Size, f.Segments[2].End = 500, 499
		},
		"one entry read by two inline files": func(t *testing.T, b *Bundle) {
			a, c := fileNamed(t, b, "movie.mkv"), fileNamed(t, b, "secret.bin")
			c.Segments = append([]Segment(nil), a.Segments...)
			c.AESKey, c.AESIV = nil, nil
			c.Size = a.Size
			b.TotalSize += a.Size - 100
		},
		"outer source is not a whole entry": func(t *testing.T, b *Bundle) {
			o := &fileNamed(t, b, "disc.m2ts").Outer[0]
			o.Segments = o.Segments[:2]
		},
	}
	for name, mutate := range bad {
		t.Run(name, func(t *testing.T) {
			b := clone(goodNZBBundle(t))
			mutate(t, b)
			err := Validate(b)
			require.Error(t, err)
			require.ErrorIs(t, err, ErrCoverage)
		})
	}
	t.Run("one par2 entry listed by two files is fine", func(t *testing.T) {
		b := clone(goodNZBBundle(t))
		fileNamed(t, b, "secret.bin").Par2 = append([]Par2File(nil), fileNamed(t, b, "movie.mkv").Par2...)
		require.NoError(t, Validate(b))
	})
}
