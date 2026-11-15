package bundleimport

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kipsilabs/altmount/internal/peernet/bundle"
)

// setBundle is a stored multi-volume set of n volumes of 100 MB, 768,000-byte
// articles (131 per volume, nzb scheme, one NZB entry per volume).
func setBundle(n int) *bundle.Bundle {
	const per = 131
	b := &bundle.Bundle{Scheme: bundle.SchemeNZB}
	f := bundle.File{Name: "movie.mkv"}
	for v := 0; v < n; v++ {
		for k := 0; k < per; k++ {
			ref := int64(len(b.Pool))
			b.Pool = append(b.Pool, fmt.Sprintf("v%d-%d@x", v, k))
			s := bundle.Segment{Ref: ref, Size: 768_000, End: 767_999}
			if k == 0 && v > 0 {
				s.Start = 1_000
			}
			f.Segments = append(f.Segments, s)
		}
		f.Parts = append(f.Parts, per)
		b.ReleaseFiles = append(b.ReleaseFiles, per)
	}
	b.Files = []bundle.File{f}
	return b
}

// planCostOf is what verifyYenc checks before reading anything.
func planCostOf(b *bundle.Bundle) (articles int, must, reserve int64) {
	tables := tablesOf(b, nil)
	markNeed0(tables)
	plan, bytes := planMust(tables)
	return len(plan), bytes, extentReserve(b, pickExtents(b)) + partReserve(b, pickParts(b))
}

// Every part costs its first article (yEnc header and archive header prefix) and
// its last (yEnc header) and nothing else mandatory; the header walk reuses the
// first article. The reservation for header reads must not count it twice.
func TestPlanCost_LargeHonestSetsFitTheDefaultCeiling(t *testing.T) {
	pol := DefaultPolicy()
	for _, n := range []int{1, 50, 200, 600, 1000} {
		articles, must, reserve := planCostOf(setBundle(n))
		t.Logf("%4d volumes: %5d mandatory articles, %8.1f MB mandatory + %6.1f MB reserved (ceiling %d articles, %.0f MB)",
			n, articles, float64(must)/1e6, float64(reserve)/1e6, pol.YencSamples(), float64(pol.YencBytes())/1e6)
		assert.LessOrEqual(t, articles, pol.YencSamples(), "%d volumes", n)
		assert.LessOrEqual(t, must+reserve, pol.YencBytes(), "%d volumes", n)
	}
}
