package bridge

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/javi11/nzbparser"
	"github.com/kipsilabs/altmount/internal/metadata"
	"github.com/kipsilabs/altmount/internal/peernet/contentid"
)

// syntheticNzb is an NZB of files*perFile segments.
func syntheticNzb(files, perFile int) *nzbparser.Nzb {
	n := &nzbparser.Nzb{}
	for f := 0; f < files; f++ {
		nf := nzbparser.NzbFile{Subject: fmt.Sprintf("release [%d/%d] \"part%03d.rar\" yEnc", f+1, files, f), Poster: "p@x", Groups: []string{"alt.binaries.test"}}
		for s := 1; s <= perFile; s++ {
			nf.Segments = append(nf.Segments, nzbparser.NzbSegment{Number: s, Bytes: 750_000, ID: fmt.Sprintf("f%03d-s%05d@example.invalid", f, s)})
		}
		n.Files = append(n.Files, nf)
	}
	return n
}

// What every import paid while the hook was installed and no peer offered
// anything: the NZB store plus the Release ID (before), against one cached
// answer (after). 100,000 segments.
func BenchmarkNoOfferImport_Before(b *testing.B) {
	n := syntheticNzb(100, 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store, _ := metadata.BuildStore(n)
		if _, err := contentid.ReleaseID(store); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNoOfferImport_After(b *testing.B) {
	var gen atomic.Uint64
	check := NewOfferCache(&fakeOfferSource{}, gen.Load)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if check(ctx) {
			b.Fatal("no offer expected")
		}
	}
}
