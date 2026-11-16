package bundleimport

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kipsilabs/altmount/internal/peernet/archivehdr/archivetest"
	"github.com/kipsilabs/altmount/internal/peernet/bundle"
)

// tsPayload is an MPEG-TS stream of n packets: a container with a sync grid and
// no size equation, so a dropped run of whole packets still looks valid.
func tsPayload(n int) []byte {
	out := make([]byte, 0, n*188)
	for k := 0; k < n; k++ {
		pkt := make([]byte, 188)
		pkt[0] = 0x47
		pkt[1], pkt[2], pkt[3] = 0x01, 0x00, 0x10|byte(k&0x0F)
		for j := 4; j < 188; j++ {
			pkt[j] = byte(k + j)
		}
		out = append(out, pkt...)
	}
	return out
}

// splitSevenZip builds a split 7z set holding a TS file (segments are whole
// packets, so dropping whole articles keeps the packet grid).
func (e *env) splitSevenZip(tag string, volumes int, encoded bool) builtSet {
	payload := tsPayload(220)
	total := len(archivetest.SevenZip(archivetest.SevenZipOpts{EncodedHeader: encoded}, archivetest.File{Name: "movie.mkv", Data: payload}).Data)
	const seg = 188
	each := (total / volumes / seg) * seg
	cuts := make([]int, volumes)
	for k := range cuts {
		cuts[k] = each
	}
	cuts[volumes-1] = total - (volumes-1)*each
	return e.mkStoredSet(setSpec{tag: tag, kind: "7z", payload: payload, cuts: cuts, segSize: seg, scheme: bundle.SchemeNZB, extraVol: -1, encodedHeader: encoded})
}

// dropArticles removes the last n articles of part k of the movie and reseals.
func dropArticles(e *env, s builtSet, k, n int) {
	m := s.movie()
	at := 0
	for j := 0; j < k; j++ {
		at += m.Parts[j]
	}
	end := at + m.Parts[k]
	m.Segments = append(m.Segments[:end-n], m.Segments[end:]...)
	m.Parts[k] -= n
	m.Size = 0
	for _, sg := range m.Segments {
		m.Size += sg.End - sg.Start + 1
	}
	s.b.TotalSize = m.Size
	e.sealAny(s.b)
}

// A split 7z archive with a plain end header is bound to that header: the
// stored stream starts where the first part starts and is as long as all parts.
func TestSplitSevenZip_PlainEndHeaderBindsTheParts(t *testing.T) {
	e := newEnv(t)
	s := e.splitSevenZip("zp", 4, false)
	res, err := e.imp.Accept(context.Background(), e.openReq(s.b))
	require.NoError(t, err, "%v", err)
	assert.Zero(t, res.Cost.PartsUnbound, "every part is bound through the 7z headers")
	assert.Equal(t, 4, res.Cost.PartsBound)
}

// Dropping whole articles from the parts of a split 7z set (the tail of a middle
// part, the tail of the last part) is refused: a middle part must be a whole
// posted file, and the end header must close the archive the parts make up.
func TestSplitSevenZip_DroppedArticlesAreRefused(t *testing.T) {
	cases := map[string]func(e *env, s builtSet){
		"tail of a middle part":  func(e *env, s builtSet) { dropArticles(e, s, 1, 2) },
		"tail of the first part": func(e *env, s builtSet) { dropArticles(e, s, 0, 1) },
		"tail of the last part":  func(e *env, s builtSet) { dropArticles(e, s, 3, 1) },
		"every middle part":      func(e *env, s builtSet) { dropArticles(e, s, 1, 1); dropArticles(e, s, 2, 1) },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			s := e.splitSevenZip("zd", 4, false)
			mutate(e, s)
			_, err := e.imp.Accept(context.Background(), e.openReq(s.b))
			require.Error(t, err, "a layout that drops articles of a split 7z set must be refused")
			// A dropped tail of the last part contradicts the 7z header itself.
			want := ReasonExtentUnverifiable
			if name == "tail of the last part" {
				want = ReasonExtentMismatch
			}
			assert.Equal(t, want, ReasonOf(err), "%v", err)
			e.assertNothingBehind()
		})
	}
}

// With an encoded end header nothing binds the stored stream, so the container
// itself must carry a size equation: a transport stream does not, a Matroska
// file with a known Segment size does.
func TestSplitSevenZip_EncodedEndHeaderNeedsASizeEquation(t *testing.T) {
	e := newEnv(t)
	s := e.splitSevenZip("ze", 4, true)
	_, err := e.imp.Accept(context.Background(), e.openReq(s.b))
	require.Error(t, err)
	assert.Equal(t, ReasonExtentUnverifiable, ReasonOf(err), "%v", err)

	e2 := newEnv(t)
	payload := mkvPlain(1500)
	total := len(archivetest.SevenZip(archivetest.SevenZipOpts{EncodedHeader: true}, archivetest.File{Name: "movie.mkv", Data: payload}).Data)
	s2 := e2.mkStoredSet(setSpec{tag: "zm", kind: "7z", payload: payload, cuts: evenCuts(total, 4), scheme: bundle.SchemeNZB, extraVol: -1, encodedHeader: true})
	_, err = e2.imp.Accept(context.Background(), e2.openReq(s2.b))
	require.NoError(t, err, "%v", err)
}
