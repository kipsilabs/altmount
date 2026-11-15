package bundleimport

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kipsilabs/altmount/internal/peernet/bundle"
)

// swapPartsOf swaps two parts (and their segments) of the movie.
func swapPartsOf(s *builtSet, a, b int) {
	m := s.movie()
	var chunks [][]bundle.Segment
	at := 0
	for _, n := range m.Parts {
		chunks = append(chunks, m.Segments[at:at+n])
		at += n
	}
	chunks[a], chunks[b] = chunks[b], chunks[a]
	m.Parts[a], m.Parts[b] = m.Parts[b], m.Parts[a]
	var flat []bundle.Segment
	for _, c := range chunks {
		flat = append(flat, c...)
	}
	m.Segments = flat
}

// R3: the middle volumes of a RAR 4 set carry identical headers, so only the
// volume names say which is which. A swap must not be accepted.
func TestParts_SwappedMiddleVolumesAreRejectedWhateverTheFormat(t *testing.T) {
	for _, kind := range []string{"rar4", "rar5"} {
		for _, scheme := range []bundle.Scheme{bundle.SchemeNZB, bundle.SchemeFiles} {
			t.Run(kind+"/"+scheme.String(), func(t *testing.T) {
				e := newEnv(t)
				payload := mkvPlain(1500)
				s := e.mkStoredSet(setSpec{tag: "sw", kind: kind, payload: payload, cuts: evenCuts(len(payload), 5), scheme: scheme, extraVol: -1})
				swapPartsOf(&s, 1, 2)
				require.NoError(t, e.tryReseal(s.b))
				_, err := e.imp.Accept(context.Background(), e.openReq(s.b))
				require.Error(t, err, "a swap of two middle volumes was accepted")
				if kind == "rar4" {
					assert.Equal(t, ReasonPartOrder, ReasonOf(err), "%v", err)
					assert.Empty(t, e.rep.got, "a name heuristic never blames the publisher")
				}
				e.assertNothingBehind()
			})
		}
	}
}

// R4: a volume whose posted bytes begin with junk (or are shifted) does not
// start like an archive, so its slice cannot be bound. That used to make the
// part "unverifiable", a state a publisher could pick at will; it is refused.
func TestParts_VolumeThatDoesNotStartLikeAnArchiveIsRefused(t *testing.T) {
	for _, kind := range []string{"rar4", "rar5"} {
		for _, scheme := range []bundle.Scheme{bundle.SchemeNZB, bundle.SchemeFiles} {
			t.Run(kind+"/"+scheme.String(), func(t *testing.T) {
				e := newEnv(t)
				payload := mkvPlain(1200)
				s := e.mkStoredSet(setSpec{tag: "jk", kind: kind, payload: payload, cuts: evenCuts(len(payload), 4), scheme: scheme, extraVol: -1, junk: 4, junkVol: 3})
				_, err := e.imp.Accept(context.Background(), e.openReq(s.b))
				require.Error(t, err)
				assert.Equal(t, ReasonExtentUnverifiable, ReasonOf(err), "%v", err)
				assert.Contains(t, err.Error(), "part 3 of 4")
				assert.Empty(t, e.rep.got, "an unreadable volume is not attributable")
				e.assertNothingBehind()
			})
		}
	}
}
