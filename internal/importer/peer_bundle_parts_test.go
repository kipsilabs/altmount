package importer

import (
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kipsilabs/altmount/internal/peernet/bundle"
	"github.com/kipsilabs/altmount/internal/testsupport/nzbbuild"
)

// A file the importer reads out of a multi-volume stored archive set is one flat
// segment list with partial ranges at every volume boundary. The bundle builder
// recovers the volumes from the NZB entries, and the receiving side converts the
// parts back to exactly that list.
func TestPeerBundle_ImporterMultiVolumeFileRoundTripsThroughParts(t *testing.T) {
	for _, tc := range []struct {
		name    string
		fixture string
		pattern string
		inner   string
		// parts is the number of parts expected: 0 for a set of one volume, which
		// keeps the original single-table layout.
		parts int
	}{
		{"rar two volumes", "rar_multi", "archive.part*.rar", "", 2},
		{"rar twelve volumes", "rar_widthmismatch", "archive.part*.rar", "", 0},
		{"7z two volumes", "7z_multi", "archive.7z.*", "", 2},
		{"rar one volume", "rar_single", "archive.rar", "", 0},
		{"7z one volume", "7z_single", "archive.7z", "", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newBatteryEnv(t)
			var files []nzbbuild.File
			var ids [][]string
			matches, err := filepath.Glob(filepath.Join("testdata", tc.fixture, tc.pattern))
			require.NoError(t, err)
			sort.Strings(matches)
			require.NotEmpty(t, matches)
			if tc.fixture == "rar_widthmismatch" {
				tc.parts = len(matches)
			}
			for i, m := range matches {
				v := filepath.Base(m)
				content := loadFixture(t, filepath.Join(tc.fixture, v))
				segs := env.registerContent("pp-"+strings.ReplaceAll(tc.name, " ", "-")+string(rune('a'+i)), content, archivePartSize, 1.0, nil)
				files = append(files, nzbbuild.File{Subject: v, Segments: segs})
				var list []string
				for _, s := range segs {
					list = append(list, s.ID)
				}
				ids = append(ids, list)
			}
			_, _, err = env.runImport(nzbbuild.Build(files...), "archive")
			require.NoError(t, err)
			names := env.listDir("/archive")
			require.Len(t, names, 1)
			tc.inner = names[0]
			meta := env.readMeta("/archive/" + tc.inner)
			require.NotEmpty(t, meta.SegmentData)

			in := bundle.ReleaseInput{Scheme: bundle.SchemeNZB, Title: "Archive Release", PostedAt: time.Unix(1_700_000_000, 0), NZBFiles: ids,
				Files: []bundle.FileInput{{Name: tc.inner, Meta: meta}}}
			bd, err := bundle.FromLocal(in)
			require.NoError(t, err)
			require.Len(t, bd.Files, 1)
			if tc.parts == 0 {
				assert.Empty(t, bd.Files[0].Parts, "a single volume keeps the original layout")
			} else {
				assert.Len(t, bd.Files[0].Parts, tc.parts, "one part per volume")
			}

			wire, id, err := bundle.Pack(bd)
			require.NoError(t, err)
			got, err := bundle.Verify(wire, id, bd.ReleaseID)
			require.NoError(t, err)
			loc, err := bundle.ToLocal(got, time.Unix(1_700_000_100, 0))
			require.NoError(t, err)
			have := loc.Files[0].Meta

			assert.Equal(t, meta.FileSize, have.FileSize)
			require.Len(t, have.SegmentData, len(meta.SegmentData))
			for i, want := range meta.SegmentData {
				assert.Equal(t, want.Id, have.SegmentData[i].Id, "segment %d", i)
				assert.Equal(t, want.SegmentSize, have.SegmentData[i].SegmentSize, "segment %d", i)
				assert.Equal(t, want.StartOffset, have.SegmentData[i].StartOffset, "segment %d", i)
				assert.Equal(t, want.EndOffset, have.SegmentData[i].EndOffset, "segment %d", i)
			}
		})
	}
}
