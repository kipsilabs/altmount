package bundleimport

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/javi11/nntppool/v5"

	"github.com/kipsilabs/altmount/internal/peernet/archivehdr"
	"github.com/kipsilabs/altmount/internal/peernet/bundle"
)

// Split 7z sets.
//
// A split 7z archive is one archive cut raw into volumes (.7z.001, .002 ...): no
// volume has a header of its own, the start header is at the beginning of the
// first volume and the end header at the end of the last. The stored file is
// therefore a contiguous run inside the concatenation of the volumes, and
// nothing in a middle volume says where the run goes on. What a bundle may claim
// is fixed by that shape, and everything else is refused:
//
//   - every part begins at the first byte of its posted file (the first part may
//     begin after the 32-byte start header, at the stored stream's start);
//   - every part but the last is a whole posted file: all the articles of its
//     NZB entry, full ranges, and the posted file's yEnc total equals the part;
//   - the last part may stop before the end header.
//
// When the end header is plain, the archive is read through the concatenation of
// the posted volumes: the start header gives where the end header is, and the
// end header where the stored stream starts and how long it is. The stream must
// start where the first part starts, be as long as all the parts together, and
// the end header must close the concatenation exactly (a dropped tail puts it out
// of reach). When the end header is encoded nothing binds the stream, and the
// file is accepted only when its container carries a size equation of its own
// (a Matroska Segment of known size, an MP4 box chain that ends on the file's
// end) in addition to the structure check; a transport stream, an unknown-size
// Matroska or any other container is refused (extent_unverifiable).

// maxTrailingArticles is how many articles of the last posted file the layout may
// leave out after the last part (the end header's own).
const maxTrailingArticles = 4

// z7StartHeaderSize is the size of the 7z start header.
const z7StartHeaderSize = 32

// split7z is what the set check concluded for one file.
type split7z struct {
	// bound says the parts were bound through plain 7z headers.
	bound bool
	// sizeEquation says the stream is bound by nothing but the container's own
	// size equation (an encoded end header).
	sizeEquation bool
	why          string
}

// split7zInput is what verifySplit7z reads.
type split7zInput struct {
	b     *bundle.Bundle
	file  int
	table func(part int) int
	ents  *entryIndex
	rel   *relState
	st    Staging
	bud   *costBudget
}

// verifySplit7z checks the shape of a split 7z set and, when its end header is
// plain, binds the parts to it.
func (i *Importer) verifySplit7z(ctx context.Context, in split7zInput) (split7z, Reason, bool, error) {
	f := &in.b.Files[in.file]
	parts := partsOfSliced(f)
	fail := func(format string, a ...any) (split7z, Reason, bool, error) {
		return split7z{}, ReasonExtentUnverifiable, false, fmt.Errorf("%s: %s", f.Name, fmt.Sprintf(format, a...))
	}
	in.rel.mu.Lock()
	for k, ps := range parts {
		tbl := in.table(k)
		first, ok := in.rel.first[tbl]
		switch {
		case !ok:
			in.rel.mu.Unlock()
			return fail("the first article of part %d was not read", k+1)
		case first != 0:
			in.rel.mu.Unlock()
			return fail("part %d of a split 7z set does not begin at the first byte of its posted file", k+1)
		case k == 0 && ps[0].Start < z7StartHeaderSize:
			in.rel.mu.Unlock()
			return fail("the first part begins inside the 7z start header")
		case k > 0 && ps[0].Start != 0:
			in.rel.mu.Unlock()
			return fail("part %d begins inside its posted file: only the first part of a split 7z set may start after the start header", k+1)
		}
		if k == len(parts)-1 {
			continue
		}
		last := ps[len(ps)-1]
		whole := last.End == last.Size-1
		if len(in.ents.start) > 0 {
			whole = whole && in.ents.whole(ps)
		} else if base, ok := in.rel.base[tbl]; ok {
			var sum int64
			for _, s := range ps {
				sum += s.Size
			}
			whole = whole && base == 0 && in.rel.total[tbl] == sum
		}
		if !whole {
			in.rel.mu.Unlock()
			return fail("part %d of a split 7z set must be a whole posted file: only the last part may stop before the end header", k+1)
		}
	}
	prefix := in.rel.prefix[in.table(0)]
	in.rel.mu.Unlock()

	// Read the archive through the concatenation of the posted volumes. The last
	// part stops before the end header, so the articles of its posted file after
	// the last listed one (the end header's own) are added, sized by their yEnc
	// headers. An honest layout leaves out at most a few of them.
	joined := &bundle.OuterSource{}
	for _, ps := range parts {
		joined.Segments = append(joined.Segments, rawView(ps).Segments...)
	}
	trailing := 0
	if len(in.ents.start) > 0 {
		lastRef := parts[len(parts)-1][len(parts[len(parts)-1])-1].Ref
		k := sort.Search(len(in.ents.start), func(k int) bool { return in.ents.start[k]+int64(in.ents.n[k]) > lastRef })
		if k < len(in.ents.start) {
			end := in.ents.start[k] + int64(in.ents.n[k])
			if end-lastRef-1 > maxTrailingArticles {
				return fail("the last part leaves %d articles of its posted file out; the end header is not that long", end-lastRef-1)
			}
			for ref := lastRef + 1; ref < end; ref++ {
				h, err := i.cfg.Headers.SegmentHeader(ctx, in.b.Pool[ref])
				switch {
				case err != nil && errors.Is(err, nntppool.ErrArticleNotFound):
					return split7z{}, ReasonProbeMissing, false, fmt.Errorf("%s: a trailing article is missing", f.Name)
				case err != nil:
					return split7z{}, ReasonProbeInconclusive, false, fmt.Errorf("%s: %w", f.Name, err)
				case h.PartSize < 1:
					return fail("a trailing article has no size")
				case in.bud.chargeActual(h.Bytes) != nil:
					return fail("the verification budget is spent")
				}
				joined.Segments = append(joined.Segments, bundle.Segment{Ref: ref, Size: h.PartSize, Start: 0, End: h.PartSize - 1})
				trailing++
			}
		}
	}
	const name = "split-7z.bin"
	if err := in.st.WriteFileMetadata(name, outerMeta(in.b, joined)); err != nil {
		return split7z{}, ReasonStaging, false, err
	}
	v, err := scanVolume(ctx, in.st, &volumeWork{name: name, outer: joined, prefix: prefix}, in.bud)
	switch {
	case errors.Is(err, errOverrun):
		return split7z{}, ReasonYencMismatch, false, fmt.Errorf("%s: %w", f.Name, err)
	case err != nil && errors.Is(err, archivehdr.ErrCompressedHeader):
		return split7z{sizeEquation: true, why: "the 7z end header is encoded, so nothing binds the stored stream"}, "", false, nil
	case err != nil:
		if reason, hard := readFailure(err); hard {
			return split7z{}, reason, false, fmt.Errorf("%s: %w", f.Name, err)
		}
		if len(in.ents.start) == 0 && errors.Is(err, archivehdr.ErrMalformed) {
			// A layout without the NZB's article list does not carry the articles
			// that hold the end header: it cannot be reached.
			return split7z{sizeEquation: true, why: "the 7z end header is not among the layout's articles"}, "", false, nil
		}
		return fail("the parts do not make up a 7z archive with a readable end header: %v", err)
	case v == nil || v.Format != archivehdr.Format7z:
		return fail("the parts do not make up a 7z archive")
	case v.EndHeaderEnd != v.Size:
		return fail("the 7z end header ends at %d but the parts end at %d: the layout drops or adds articles", v.EndHeaderEnd, v.Size)
	}
	res := archivehdr.Check(v, nil, archivehdr.Extent{Offset: parts[0][0].Start, Length: bundle.Usable(joinParts(parts)), Index: 0, Count: 1, FileSize: f.Size, Name: f.Name})
	switch res.Outcome {
	case archivehdr.Bound:
		return split7z{bound: true}, "", false, nil
	case archivehdr.Mismatch:
		return split7z{}, ReasonExtentMismatch, true, evidenceError{fmt.Errorf("%s: %s", f.Name, res.Detail), partEvidence(f.FileID.String(), -2, bundle.TableKey{})}
	}
	return fail("the stored stream could not be bound to the 7z headers: %s", res.Detail)
}

func joinParts(parts [][]bundle.Segment) []bundle.Segment {
	var out []bundle.Segment
	for _, ps := range parts {
		out = append(out, ps...)
	}
	return out
}
