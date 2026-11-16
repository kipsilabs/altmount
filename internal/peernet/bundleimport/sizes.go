package bundleimport

import (
	"errors"
	"fmt"

	"github.com/kipsilabs/altmount/internal/peernet/bundle"
)

// Encoded versus decoded sizes. An NZB's <segment bytes> is the yEnc-ENCODED
// size: escaping and line breaks add roughly 2 to 3 percent, plus a header of a
// couple of hundred bytes per article, so a decoded size is at most the encoded one and
// not much below it. The band is deliberately loose (the importer's own
// estimate uses a 1 percent tolerance on top of this); it catches layouts that
// claim sizes the user's own NZB contradicts, not small lies.
const (
	minDecodedPermille = 900 // decoded >= 0.90 * encoded - sizeSlack
	sizeSlack          = 512 // header and short-segment slack, per table
)

func plausibleDecoded(decoded, encoded, parts int64) bool {
	return decoded <= encoded && decoded*1000 >= encoded*minDecodedPermille-sizeSlack*parts*1000
}

// checkSizes compares every segment table's declared decoded sizes with the
// encoded sizes of the user's NZB (nzb scheme only) and each table's sum
// likewise.
func checkSizes(b *bundle.Bundle, req Request) error {
	if req.ExpectedNZBSizes == nil || b.Scheme != bundle.SchemeNZB {
		return nil
	}
	if len(req.ExpectedNZBSizes) != len(req.ExpectedNZB) {
		return errors.New("NZB sizes do not match the NZB list")
	}
	enc := map[string]int64{}
	for i, ids := range req.ExpectedNZB {
		if len(ids) != len(req.ExpectedNZBSizes[i]) {
			return errors.New("NZB sizes do not match the NZB list")
		}
		for k, id := range ids {
			enc[id] = req.ExpectedNZBSizes[i][k]
		}
	}
	table := func(name string, segs []bundle.Segment) error {
		var dsum, esum int64
		for _, s := range segs {
			e, ok := enc[b.Pool[s.Ref]]
			if !ok {
				return fmt.Errorf("%s: a segment is not in the NZB", name)
			}
			if e <= 0 {
				return nil // an NZB without segment sizes cannot bound anything
			}
			if !plausibleDecoded(s.Size, e, 1) {
				return fmt.Errorf("%s: declared size %d against %d encoded bytes in your NZB", name, s.Size, e)
			}
			dsum += s.Size
			esum += e
		}
		if !plausibleDecoded(dsum, esum, int64(len(segs))) {
			return fmt.Errorf("%s: declared %d bytes against %d encoded bytes in your NZB", name, dsum, esum)
		}
		return nil
	}
	for i := range b.Files {
		f := &b.Files[i]
		if err := table(f.Name, f.Segments); err != nil {
			return err
		}
		for j := range f.Outer {
			if err := table(fmt.Sprintf("%s outer %d", f.Name, j), f.Outer[j].Segments); err != nil {
				return err
			}
		}
	}
	return nil
}
