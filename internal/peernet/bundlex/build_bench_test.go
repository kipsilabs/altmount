package bundlex

import (
	"context"
	"fmt"
	"testing"

	"github.com/kipsilabs/altmount/internal/database"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/peernet/bundle"
)

// BenchmarkBuild75kSegments measures a rebuild of a realistic 50 GB release
// (75,000 segments, one file plus an unimported par2 set) from resolved metadata.
func BenchmarkBuild75kSegments(b *testing.B) {
	video := make([]string, 75000)
	for i := range video {
		video[i] = fmt.Sprintf("part%d-%05dof75000.%x@news.example.net", i, i, uint64(i)*2654435761)
	}
	lib := &fakeLib{scheme: database.PeernetSchemeNZB, paths: []string{"p"},
		files: map[string]*metapb.FileMetadata{"p": plainFile("big.mkv", video)}, nzb: [][]string{video}}
	rel := releaseHexOfNZB(lib.nzb)
	bl := NewBuilder(BuilderConfig{Source: lib, Library: lib, Patches: NoPatches{}})
	b.ResetTimer()
	for range b.N {
		bl.Invalidate(rel)
		got, err := bl.Build(context.Background(), rel, "Big.Release", posted)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(len(got.Wire))/1e6, "wireMB")
	}
	_ = bundle.MaxCompressedSize
}
