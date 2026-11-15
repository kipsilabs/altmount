package bundleimport

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/kipsilabs/altmount/internal/importer/parser/fileinfo"
	"github.com/kipsilabs/altmount/internal/peernet/archivehdr"
	"github.com/kipsilabs/altmount/internal/peernet/bundle"
)

// Multi-part inline layouts.
//
// A file stored in an archive set (a video in part01.rar ... partNN.rar) is the
// concatenation of one slice per volume: from the byte after the volume's own
// headers to the byte before its trailer. The bundle declares the slices
// (File.Parts); this file is what the receiver does with them.
//
// Yenc. A part that is a whole NZB entry (nzb scheme), or whose ranges are full
// (files scheme), claims to be a whole posted file and is checked as one: its
// first article begins at 0 and the posted file's total is the sum of the part.
// Any other part is a slice of a larger posted file: every sampled article must
// agree on where the part begins in it and on its total (relState). The last
// article of every part is always sampled, so a size lie anywhere in a part
// shows as a disagreement between the end and the other samples.
//
// Archive headers. A part that is a slice is bound to the archive's own file
// header: the data area of a stored file entry starts at the byte where the part
// starts, is as long as the part, and its continuation flags fit the part's place
// in the file; for RAR 5 the volume numbers must run in the order of the parts.
// A part that claims to be a whole media file is checked not to be an archive
// volume. A part that cannot be judged (a slice that does not begin at its posted
// file's first article, an unknown or encoded archive, a spent budget) is
// unverifiable, and the file is then accepted only when its media structure check
// passes and its container has a real structural check.

// maxPartChecks bounds how many parts of a bundle get an archive header check.
const maxPartChecks = 64

// entryIndex answers which NZB entry a pool position belongs to (nzb scheme).
type entryIndex struct {
	start []int64
	n     []int
}

func newEntryIndex(b *bundle.Bundle) *entryIndex {
	if b.Scheme != bundle.SchemeNZB {
		return &entryIndex{}
	}
	e := &entryIndex{start: make([]int64, len(b.ReleaseFiles)), n: b.ReleaseFiles}
	var at int64
	for i, n := range b.ReleaseFiles {
		e.start[i] = at
		at += int64(n)
	}
	return e
}

// whole reports whether segs are exactly the articles of one NZB entry.
func (e *entryIndex) whole(segs []bundle.Segment) bool {
	if len(segs) == 0 || len(e.start) == 0 {
		return false
	}
	k := sort.Search(len(e.start), func(i int) bool { return e.start[i]+int64(e.n[i]) > segs[0].Ref })
	for k < len(e.start) && e.n[k] == 0 {
		k++
	}
	return k < len(e.start) && segs[0].Ref == e.start[k] && len(segs) == e.n[k]
}

func fullRange(segs []bundle.Segment) bool {
	return segs[0].Start == 0 && segs[len(segs)-1].End == segs[len(segs)-1].Size-1
}

// wholeYenc says the part is checked as a whole posted file: it is a whole NZB
// entry, or (files scheme, no entries) its ends are full.
func (e *entryIndex) wholeYenc(segs []bundle.Segment) bool {
	if len(e.start) > 0 {
		return e.whole(segs)
	}
	return fullRange(segs)
}

// partsOfSliced returns the parts of a multi-part file (nil for any other).
func partsOfSliced(f *bundle.File) [][]bundle.Segment {
	if len(f.Parts) == 0 {
		return nil
	}
	return bundle.PartsOf(f)
}

// partKind is what the header check does for one part.
type partKind uint8

const (
	// partSlice: bind the part to a stored file entry of its volume.
	partSlice partKind = iota
	// partRaw: the part claims to be a whole posted file of media data; it must
	// not start like an archive volume.
	partRaw
)

type partRef struct {
	file, part int
	kind       partKind
}

// classifyPart says which header check a part needs, and whether it needs one.
func classifyPart(ents *entryIndex, f *bundle.File, ps []bundle.Segment) (partKind, bool) {
	whole := ents.wholeYenc(ps)
	switch {
	case whole && fullRange(ps):
		return partRaw, fileinfo.IsVerifiableMediaFile(f.Name)
	default:
		return partSlice, true
	}
}

// pickParts chooses the parts to check: all of them up to maxPartChecks, then
// the first and last part of every media file, then a uniform random choice
// (crypto/rand) of the rest. Slices come before raw parts.
func pickParts(b *bundle.Bundle) []partRef {
	ents := newEntryIndex(b)
	var all []partRef
	for fi := range b.Files {
		f := &b.Files[fi]
		for k, ps := range partsOfSliced(f) {
			if kind, ok := classifyPart(ents, f, ps); ok {
				all = append(all, partRef{fi, k, kind})
			}
		}
	}
	if len(all) <= maxPartChecks {
		return all
	}
	var picks, rest []partRef
	for _, r := range all {
		f := &b.Files[r.file]
		if fileinfo.IsVerifiableMediaFile(f.Name) && (r.part == 0 || r.part == len(f.Parts)-1) && len(picks) < maxPartChecks/2 {
			picks = append(picks, r)
		} else {
			rest = append(rest, r)
		}
	}
	shuffle(rest)
	return append(picks, rest[:maxPartChecks-len(picks)]...)
}

// partReserve is the cost the header checks of the picked parts are expected to
// have: a few articles each.
func partReserve(b *bundle.Bundle, picks []partRef) int64 {
	var n int64
	for _, r := range picks {
		first := partsOfSliced(&b.Files[r.file])[r.part][0]
		n += extentReserveArticles * first.Size
	}
	return n
}

// markNeed0 makes the first article of every picked part a mandatory sample.
func markNeed0(tables []sampleTable, picks []partRef) {
	at := map[[2]int]int{}
	for i := range tables {
		if tables[i].part {
			at[[2]int{tables[i].file, tables[i].index}] = i
		}
	}
	for _, r := range picks {
		if i, ok := at[[2]int{r.file, r.part}]; ok {
			tables[i].need0 = true
		}
	}
}

type partWork struct {
	ref  partRef
	segs []bundle.Segment
	// raw is the posted volume as far as the part reaches, read from its first
	// byte; offset is where the part starts in it.
	raw    *bundle.OuterSource
	offset int64
	name   string
	upTo   int64
	vol    *archivehdr.Volume
	err    error
	skip   string
	table  int
}

// rawView is the part's articles read whole from the first byte of the first
// article, as the posted volume begins.
func rawView(ps []bundle.Segment) *bundle.OuterSource {
	raw := &bundle.OuterSource{Segments: make([]bundle.Segment, len(ps))}
	for i, s := range ps {
		raw.Segments[i] = bundle.Segment{Ref: s.Ref, Size: s.Size, Start: 0, End: s.Size - 1}
	}
	return raw
}

// verifyParts runs the header checks of the picked parts.
func (i *Importer) verifyParts(ctx context.Context, b *bundle.Bundle, tables []sampleTable, st Staging, picks []partRef, rel *relState, bud *costBudget, rep *extentReport) (Reason, bool, error) {
	if len(picks) == 0 {
		return "", false, nil
	}
	byTable := map[[2]int]int{}
	for k := range tables {
		if tables[k].part {
			byTable[[2]int{tables[k].file, tables[k].index}] = k
		}
	}
	var work []*partWork
	ents := newEntryIndex(b)
	for n, r := range picks {
		f := &b.Files[r.file]
		ps := partsOfSliced(f)[r.part]
		tbl, ok := byTable[[2]int{r.file, r.part}]
		if !ok {
			return ReasonInvalid, false, fmt.Errorf("part %d of file %d has no sample table", r.part, r.file)
		}
		w := &partWork{ref: r, segs: ps, raw: rawView(ps), name: fmt.Sprintf("part-%d.bin", n), upTo: ps[0].Start, offset: ps[0].Start, table: tbl}
		if r.kind == partRaw {
			w.upTo = 0
		} else {
			i.placePart(ctx, b, ents, w, rel, bud)
		}
		if w.skip == "" {
			if err := st.WriteFileMetadata(w.name, outerMeta(b, w.raw)); err != nil {
				return ReasonStaging, false, err
			}
		}
		work = append(work, w)
	}
	ctx, cancel := context.WithTimeout(ctx, i.cfg.Policy.YencTimeoutOrDefault())
	defer cancel()
	var wg sync.WaitGroup
	sem := make(chan struct{}, 4)
	for _, w := range work {
		if w.skip != "" {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			w.vol, w.err = scanVolume(ctx, st, &volumeWork{name: w.name, outer: w.raw, upTo: w.upTo}, bud)
		}()
	}
	wg.Wait()
	volumes := map[int][]*partWork{}
	for _, w := range work {
		f := &b.Files[w.ref.file]
		if w.err != nil {
			if reason, hard := readFailure(w.err); hard {
				return reason, false, fmt.Errorf("%s (part %d): %w", f.Name, w.ref.part+1, w.err)
			}
		}
		res := checkPart(w, f, len(f.Parts))
		switch res.Outcome {
		case archivehdr.Bound:
			rep.partsBound++
			if w.vol != nil {
				volumes[w.ref.file] = append(volumes[w.ref.file], w)
			}
		case archivehdr.Mismatch:
			return ReasonExtentMismatch, true, fmt.Errorf("%s: part %d of %d: %s", f.Name, w.ref.part+1, len(f.Parts), res.Detail)
		default:
			rep.partsUnbound++
			if _, ok := rep.unbound[w.ref.file]; !ok {
				rep.unbound[w.ref.file] = res.Detail
			}
		}
	}
	for fi, ws := range volumes {
		if d := volumeOrder(ws); d != "" {
			return ReasonExtentMismatch, true, fmt.Errorf("%s: %s", b.Files[fi].Name, d)
		}
	}
	return "", false, nil
}

// placePart decides what the header walk of a slice reads. A part that begins at
// the first article of its posted file is read from there. In the nzb scheme a
// part that begins further into its volume (a second file stored after the
// first) is read through a view that starts at the volume's first article: the
// articles before the part are taken to be as long as the first article
// (a poster's articles are, but the last), which the part's own offset has to
// confirm (it must equal the number of articles before it times that length).
// Anything else cannot be judged and is left unverifiable.
func (i *Importer) placePart(ctx context.Context, b *bundle.Bundle, ents *entryIndex, w *partWork, rel *relState, bud *costBudget) {
	first, ok := rel.first[w.table]
	switch {
	case !ok:
		w.skip = "the first article of the part was not read"
		return
	case first == 0:
		return
	case len(ents.start) == 0:
		w.skip = "the part does not begin at the first article of its posted file"
		return
	}
	k := sort.Search(len(ents.start), func(k int) bool { return ents.start[k]+int64(ents.n[k]) > w.segs[0].Ref })
	if k >= len(ents.start) {
		w.skip = "the part is not inside an NZB entry"
		return
	}
	before := w.segs[0].Ref - ents.start[k]
	h, err := i.cfg.Headers.SegmentHeader(ctx, b.Pool[ents.start[k]])
	switch {
	case err != nil:
		w.skip = "the first article of the posted file could not be read: " + err.Error()
		return
	case h.PartBegin != 0 || h.PartSize < 1 || before*h.PartSize != first:
		w.skip = "the articles before the part are not as long as the first one"
		return
	case bud.charge(h.PartSize) != nil:
		w.skip = "the verification budget is spent"
		return
	}
	view := &bundle.OuterSource{}
	for j := int64(0); j < before; j++ {
		view.Segments = append(view.Segments, bundle.Segment{Ref: ents.start[k] + j, Size: h.PartSize, Start: 0, End: h.PartSize - 1})
	}
	view.Segments = append(view.Segments, w.raw.Segments...)
	w.raw, w.offset, w.upTo = view, first+w.segs[0].Start, first+w.segs[0].Start
}

// checkPart judges one scanned part.
func checkPart(w *partWork, f *bundle.File, count int) archivehdr.Result {
	if w.skip != "" {
		return archivehdr.Result{Outcome: archivehdr.Unverifiable, Detail: w.skip}
	}
	if w.ref.kind == partRaw {
		if w.vol != nil && w.vol.Format != archivehdr.FormatUnknown {
			return archivehdr.Result{Outcome: archivehdr.Mismatch,
				Detail: fmt.Sprintf("the part claims to be whole media data but begins like a %s archive volume", w.vol.Format)}
		}
		return archivehdr.Result{Outcome: archivehdr.Bound}
	}
	e := archivehdr.Extent{Offset: w.offset, Length: bundle.Usable(w.segs), Index: w.ref.part, Count: count, FileSize: f.Size, Keyed: len(f.AESKey) > 0}
	if e.Keyed && w.ref.part == 0 {
		e.IV = f.AESIV
	}
	return archivehdr.Check(w.vol, w.err, e)
}

// volumeOrder requires the RAR 5 volume numbers of the checked parts of one file
// to run in the order of the parts (consecutive volumes). A volume that declares
// no number (RAR 4, 7z) says nothing.
func volumeOrder(ws []*partWork) string {
	type seen struct {
		part int
		vol  int64
	}
	var known []seen
	for _, w := range ws {
		if w.vol != nil && w.vol.VolumeNumber >= 0 {
			known = append(known, seen{w.ref.part, w.vol.VolumeNumber})
		}
	}
	for k := 1; k < len(known); k++ {
		if d := known[k].vol - known[0].vol; d != int64(known[k].part-known[0].part) {
			return fmt.Sprintf("part %d is volume %d and part %d is volume %d: the volumes are not in the order of the parts",
				known[0].part+1, known[0].vol+1, known[k].part+1, known[k].vol+1)
		}
	}
	return ""
}
