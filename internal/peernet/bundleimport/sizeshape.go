package bundleimport

import (
	"fmt"

	"github.com/kipsilabs/altmount/internal/peernet/bundle"
)

// Article-size shape.
//
// A posted file is cut into articles of one size except the last, which holds
// the rest (the yEnc part size of every article but the last is the poster's
// block size; the importer's NZB fixtures show the encoded sizes spread by
// about 0.1 percent around one value, the escapes of yEnc, and no table with
// two block sizes). A bundle declares those decoded sizes, and the node pays
// for what it samples by the size of the articles it reads, so a layout that
// declares one-byte articles for a file whose articles are really 750 KB, or
// mixes sizes so that its few sampled articles are the small ones, is declined
// before anything is read. Every segment table must be one run of equal sizes
// followed by a last article that is not larger.
//
// The rule is enforced here and not in bundle.Validate because a bundle that
// breaks it is not malformed, only not worth verifying (an honest poster with
// variable articles would be declined locally, not blamed).

// checkRunShape applies the shape rule to one run of segments.
func checkRunShape(name string, segs []bundle.Segment) error {
	if len(segs) < 2 {
		return nil
	}
	block := segs[0].Size
	for i, s := range segs[:len(segs)-1] {
		if s.Size != block {
			return fmt.Errorf("%s: article %d declares %d bytes, the first %d (a posted file's articles are one size)", name, i, s.Size, block)
		}
	}
	if last := segs[len(segs)-1]; last.Size > block {
		return fmt.Errorf("%s: the last article declares %d bytes, more than the %d of the others", name, last.Size, block)
	}
	return nil
}

// checkSizeShape applies it to every table of the bundle: each whole inline
// file, each part of a multi-part file and each outer volume.
func checkSizeShape(b *bundle.Bundle) error {
	for i := range b.Files {
		f := &b.Files[i]
		if len(f.Parts) == 0 {
			if err := checkRunShape(f.Name, f.Segments); err != nil {
				return err
			}
		}
		for k, ps := range partsOfSliced(f) {
			if err := checkRunShape(fmt.Sprintf("%s (part %d of %d)", f.Name, k+1, len(f.Parts)), ps); err != nil {
				return err
			}
		}
		for j := range f.Outer {
			if err := checkRunShape(fmt.Sprintf("%s (volume %d)", f.Name, j+1), f.Outer[j].Segments); err != nil {
				return err
			}
		}
	}
	return nil
}
