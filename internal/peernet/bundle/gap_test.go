package bundle

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kipsilabs/altmount/internal/holes"
)

// A shareable file has no known holes: a gap reference is refused wherever a
// segment table appears.
func TestValidateRejectsGapReferences(t *testing.T) {
	cases := map[string]func(b *Bundle){
		"inline": func(b *Bundle) { fileNamed(t, b, "movie.mkv").Segments[1].Ref = GapRef },
		"outer":  func(b *Bundle) { fileNamed(t, b, "disc.m2ts").Outer[0].Segments[1].Ref = GapRef },
		"par2":   func(b *Bundle) { fileNamed(t, b, "movie.mkv").Par2[0].Segments[0].Ref = GapRef },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			b := clone(goodBundle(t))
			mutate(b)
			err := Validate(b)
			require.Error(t, err, "a gap reference must be rejected")
			require.ErrorIs(t, err, ErrInvalidField)
		})
	}
}

func TestFromLocalRefusesPlaceholderSegments(t *testing.T) {
	in := releaseInput(testSpecs())
	in.Files[0].Meta.SegmentData[1].Id = holes.PlaceholderID(2, "salt")
	_, err := FromLocal(in)
	require.Error(t, err)
	if !errors.Is(err, ErrNotShareable) {
		t.Fatalf("want a not-shareable error, got %v", err)
	}
}
