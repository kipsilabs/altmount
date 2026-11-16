package bundleimport

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kipsilabs/altmount/internal/peernet/archivehdr/archivetest"
	"github.com/kipsilabs/altmount/internal/peernet/bundle"
)

// CBC garbles only the first block when the IV is wrong, so a peer that changes
// the IV of a keyed RAR 4 volume (which carries no IV to compare) leaves the
// whole data area readable and only breaks the archive signature, switching the
// header check off. A keyed volume that claims an inner archive must decrypt to
// one.
func TestExtents_KeyedOuterVolumeWithAWrongIVIsRejected(t *testing.T) {
	for _, kind := range []string{"rar4", "rar5"} {
		t.Run(kind, func(t *testing.T) {
			e := newEnv(t)
			mkv := mkvPlain(512)
			build := archivetest.RAR4
			if kind == "rar5" {
				build = archivetest.RAR5
			}
			a := build(archivetest.File{Name: "movie.mkv", Data: mkv})
			ext := []bundle.Extent{ext1(0, a.DataOffsets[0], int64(len(mkv)))}
			honest := e.mkNested("kv"+kind, "movie.mkv", int64(len(mkv)), []volSpec{{archive: a.Data, segSize: 128, key: testKey}}, ext)
			_, err := e.imp.Accept(context.Background(), e.openReq(honest))
			require.NoError(t, err, "the honest keyed layout is accepted: %v", err)

			e2 := newEnv(t)
			b := e2.mkNested("kw"+kind, "movie.mkv", int64(len(mkv)), []volSpec{{archive: a.Data, segSize: 128, key: testKey}}, ext)
			b.Files[0].Outer[0].AESIV = []byte("0000000000000000")
			e2.seal(b)
			_, err = e2.imp.Accept(context.Background(), e2.openReq(b))
			require.Error(t, err, "a changed IV switched the header check off")
			assert.Equal(t, ReasonExtentMismatch, ReasonOf(err), "%v", err)
			assert.Len(t, e2.rep.got, 1, "the layout contradicts the bytes: attributable")
			e2.assertNothingBehind()
		})
	}
}
