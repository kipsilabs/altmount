package bundleimport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/javi11/nntppool/v5"

	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/peernet/archivehdr"
	"github.com/kipsilabs/altmount/internal/peernet/bundle"
	"github.com/kipsilabs/altmount/internal/utils"
)

// Archive-extent spot checks.
//
// A file that is read through outer archive volumes is described by extents:
// (outer volume, InnerOffset, InnerLength). No identifier binds them (the IDs
// hash message-IDs only, yEnc sampling binds the OUTER volumes' article sizes
// and offsets), so a publisher could point an extent at any bytes of a volume.
// The archive's own file header is the one thing that says where a stored
// file's data area starts and how long it is in a volume, so each extent is
// compared with the header that precedes its data, read from the user's
// providers through the staged reader (so an encrypted outer volume is read
// decrypted, with the bundle's key, exactly as it will be served).
//
// What is checked per extent: the data area of a STORED file entry (not a
// service block, not compressed, not itself encrypted) starts exactly at
// InnerOffset, is InnerLength long in this volume, the entry's unpacked size is
// the file's size, and its continuation flags fit the extent's place in the file
// (first, middle, last). A 7z volume is judged by its plain end header.
//
// An extent that cannot be judged (unknown format such as a disc image,
// encoded 7z end header, encrypted archive headers, budget spent, damaged
// header chain) is unverifiable: the file is then accepted only when its media
// structure check passes and its container has a real structural check; a
// generic container is rejected (ReasonExtentUnverifiable).

const (
	// maxExtentChecks bounds how many extents of a bundle are checked.
	maxExtentChecks = 64
	// maxVolumeArticles bounds the distinct articles one volume's header walk may
	// touch.
	maxVolumeArticles = 24
	// extentReserveArticles is what each checked volume is expected to cost, to
	// size the shared budget.
	extentReserveArticles = 3
)

type extentRef struct{ file, ext int }

// pickExtents chooses the extents to check: all of them up to maxExtentChecks;
// beyond that the first and last extent of the main media file plus a uniform
// random choice (crypto/rand) of the rest.
func pickExtents(b *bundle.Bundle) []extentRef {
	var all []extentRef
	for fi := range b.Files {
		for ei := range b.Files[fi].Extents {
			all = append(all, extentRef{fi, ei})
		}
	}
	if len(all) <= maxExtentChecks {
		return all
	}
	var picks, rest []extentRef
	mainFile := mainMediaFile(b)
	for _, r := range all {
		last := len(b.Files[r.file].Extents) - 1
		if r.file == mainFile && (r.ext == 0 || r.ext == last) {
			picks = append(picks, r)
		} else {
			rest = append(rest, r)
		}
	}
	shuffle(rest)
	return append(picks, rest[:maxExtentChecks-len(picks)]...)
}

// volumeKey identifies one outer volume by content (table, key, volume size).
type volumeKey struct {
	table bundle.TableKey
	key   string
	iv    string
	size  int64
}

type volumeWork struct {
	key   volumeKey
	outer *bundle.OuterSource
	name  string
	upTo  int64
	refs  []extentRef
	vol   *archivehdr.Volume
	err   error
	// wrap is the verdict on the archive that wraps the volume (the header that
	// precedes the table's first byte), when the table starts inside its first
	// article.
	wrap archivehdr.Result
}

func keyOf(o *bundle.OuterSource) volumeKey {
	return volumeKey{table: bundle.KeyOf(o.Segments), key: string(o.AESKey), iv: string(o.AESIV), size: o.VolumeSize}
}

// groupVolumes lists the distinct outer volumes of the picked extents, with the
// largest offset asked of each.
func groupVolumes(b *bundle.Bundle, picks []extentRef) []*volumeWork {
	var out []*volumeWork
	index := map[volumeKey]*volumeWork{}
	for _, r := range picks {
		f := &b.Files[r.file]
		e := f.Extents[r.ext]
		o := &f.Outer[e.Outer]
		k := keyOf(o)
		w := index[k]
		if w == nil {
			w = &volumeWork{key: k, outer: o, name: fmt.Sprintf("outer-%d.bin", len(out))}
			index[k] = w
			out = append(out, w)
		}
		w.upTo = max(w.upTo, e.InnerOffset)
		w.refs = append(w.refs, r)
	}
	return out
}

// extentReserve is the cost the header walks are expected to have: a few
// articles per checked volume.
func extentReserve(b *bundle.Bundle, picks []extentRef) int64 {
	var n int64
	for _, w := range groupVolumes(b, picks) {
		first := w.outer.Segments[0].Size
		n += extentReserveArticles * first
	}
	return n
}

// outerMeta is the staged metadata of one outer volume: the volume as a plain
// (or AES) file of its own, read through the same stack as everything else.
func outerMeta(b *bundle.Bundle, o *bundle.OuterSource) *metapb.FileMetadata {
	m := &metapb.FileMetadata{Status: metapb.FileStatus_FILE_STATUS_HEALTHY}
	var covered int64
	for _, s := range o.Segments {
		m.SegmentData = append(m.SegmentData, &metapb.SegmentData{Id: b.Pool[s.Ref], SegmentSize: s.Size, StartOffset: s.Start, EndOffset: s.End})
		covered += s.End - s.Start + 1
	}
	m.FileSize = covered
	if len(o.AESKey) > 0 {
		m.FileSize = o.VolumeSize
		m.Encryption = metapb.Encryption_AES
		m.AesKey = append([]byte(nil), o.AESKey...)
		m.AesIv = append([]byte(nil), o.AESIV...)
	}
	return m
}

// chargedReader charges the shared budget for every distinct article a read
// touches (by the table's own geometry) before it reads, and refuses a volume
// more than maxVolumeArticles articles.
type chargedReader struct {
	r      io.ReaderAt
	starts []int64 // starts[k] = decoded offset where usable article k begins
	sizes  []int64
	seen   map[int]struct{}
	bud    *costBudget
}

func newChargedReader(r io.ReaderAt, o *bundle.OuterSource, bud *costBudget) *chargedReader {
	c := &chargedReader{r: r, seen: map[int]struct{}{}, bud: bud}
	var at int64
	for _, s := range o.Segments {
		c.starts = append(c.starts, at)
		c.sizes = append(c.sizes, s.Size)
		at += s.End - s.Start + 1
	}
	c.starts = append(c.starts, at)
	return c
}

func (c *chargedReader) ReadAt(p []byte, off int64) (int, error) {
	end := off + int64(len(p))
	for k := range c.sizes {
		if c.starts[k+1] <= off || c.starts[k] >= end {
			continue
		}
		if _, ok := c.seen[k]; ok {
			continue
		}
		if len(c.seen) >= maxVolumeArticles {
			return 0, fmt.Errorf("%w: volume walk touched %d articles", errCost, maxVolumeArticles)
		}
		if err := c.bud.charge(c.sizes[k]); err != nil {
			return 0, err
		}
		c.seen[k] = struct{}{}
	}
	return c.r.ReadAt(p, off)
}

// extentReport says which files of the bundle have extents that were checked
// and could not be bound.
type extentReport struct {
	// unbound maps a file index to why one of its checked extents is
	// unverifiable.
	unbound map[int]string
	bound   int
	checked int
}

// verifyExtents runs the header check for the picked extents. A mismatch is
// attributable to the publisher.
func (i *Importer) verifyExtents(ctx context.Context, b *bundle.Bundle, st Staging, picks []extentRef, bud *costBudget) (extentReport, Reason, bool, error) {
	rep := extentReport{unbound: map[int]string{}}
	if len(picks) == 0 {
		return rep, "", false, nil
	}
	volumes := groupVolumes(b, picks)
	for _, w := range volumes {
		if err := st.WriteFileMetadata(w.name, outerMeta(b, w.outer)); err != nil {
			return rep, ReasonStaging, false, err
		}
		if w.outer.Segments[0].Start > 0 {
			if err := st.WriteFileMetadata(w.name+wrapSuffix, rawFromArticleStart(b, w.outer)); err != nil {
				return rep, ReasonStaging, false, err
			}
		}
	}
	ctx, cancel := context.WithTimeout(ctx, i.cfg.Policy.YencTimeoutOrDefault())
	defer cancel()
	var wg sync.WaitGroup
	sem := make(chan struct{}, 4)
	for _, w := range volumes {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			w.vol, w.err = scanVolume(ctx, st, w, bud)
			w.wrap = checkWrapper(ctx, st, w, bud)
		}()
	}
	wg.Wait()
	for _, w := range volumes {
		if w.wrap.Outcome == archivehdr.Mismatch {
			return rep, ReasonExtentMismatch, true, fmt.Errorf("%s (outer volume): %s", b.Files[w.refs[0].file].Name, w.wrap.Detail)
		}
		for _, r := range w.refs {
			f := &b.Files[r.file]
			e := f.Extents[r.ext]
			if w.err != nil {
				if reason, hard := readFailure(w.err); hard {
					return rep, reason, false, fmt.Errorf("%s (volume %d): %w", f.Name, e.Outer+1, w.err)
				}
			}
			res := archivehdr.Check(w.vol, w.err, archivehdr.Extent{Offset: e.InnerOffset, Length: e.InnerLength,
				Index: r.ext, Count: len(f.Extents), FileSize: f.Size})
			rep.checked++
			switch res.Outcome {
			case archivehdr.Bound:
				rep.bound++
			case archivehdr.Mismatch:
				return rep, ReasonExtentMismatch, true, fmt.Errorf("%s: extent %d of %d: %s", f.Name, r.ext+1, len(f.Extents), res.Detail)
			default:
				if _, ok := rep.unbound[r.file]; !ok {
					rep.unbound[r.file] = res.Detail
				}
			}
		}
	}
	return rep, "", false, nil
}

// readFailure classifies an error of the underlying read that is not about the
// archive: a missing article, a timeout. Budget errors are not hard (the extent
// is then unverifiable).
func readFailure(err error) (Reason, bool) {
	var re *archivehdr.ReadError
	switch {
	case !errors.As(err, &re), errors.Is(err, errCost):
		return "", false
	case errors.Is(err, nntppool.ErrArticleNotFound):
		return ReasonProbeMissing, true
	}
	return ReasonProbeInconclusive, true
}

func scanVolume(ctx context.Context, st Staging, w *volumeWork, bud *costBudget) (*archivehdr.Volume, error) {
	vctx := context.WithValue(ctx, utils.MaxPrefetchKey, 1)
	vctx = context.WithValue(vctx, utils.SuppressStreamTrackingKey, true)
	f, err := st.Opener().OpenFile(vctx, w.name, os.O_RDONLY, 0)
	if err != nil {
		return nil, &archivehdr.ReadError{Err: err}
	}
	defer func() { _ = f.Close() }()
	size := volumeBytes(w.outer)
	return archivehdr.Scan(newChargedReader(f, w.outer, bud), size, w.upTo, archivehdr.DefaultLimits())
}

// volumeBytes is the readable size of the staged volume.
func volumeBytes(o *bundle.OuterSource) int64 {
	if len(o.AESKey) > 0 {
		return o.VolumeSize
	}
	var n int64
	for _, s := range o.Segments {
		n += s.End - s.Start + 1
	}
	return n
}

// needsStructure lists the files whose media must pass a real structural check
// because an extent of theirs could not be bound to an archive header.
func needsStructure(rep extentReport, b *bundle.Bundle) map[string]string {
	out := map[string]string{}
	for fi, why := range rep.unbound {
		out[b.Files[fi].Name] = why
	}
	return out
}

// wrapSuffix names the staged view of a volume's raw bytes from the start of
// its first article.
const wrapSuffix = ".raw"

// rawFromArticleStart is the volume's table read raw (no key) from the start of
// its first article, so the archive header that precedes the table's first byte
// is readable.
func rawFromArticleStart(b *bundle.Bundle, o *bundle.OuterSource) *metapb.FileMetadata {
	raw := &bundle.OuterSource{Segments: append([]bundle.Segment(nil), o.Segments...)}
	raw.Segments[0].Start = 0
	m := outerMeta(b, raw)
	return m
}

// checkWrapper binds the table's first byte to the archive that wraps it. The
// outer volume is itself a stored file of a wrapping archive (usually an
// encrypted one: the bundle's key and IV are that file's); when the table's
// first article starts that archive, the file header ending exactly at the
// table's first byte must be a stored entry whose packed size is the table's
// size and, for a RAR 5 file with a key, whose header IV equals the bundle's.
// A table that does not start inside an archive's first article cannot be
// judged here (the decrypted view still has to be a valid volume).
func checkWrapper(ctx context.Context, st Staging, w *volumeWork, bud *costBudget) archivehdr.Result {
	first := w.outer.Segments[0]
	if first.Start == 0 {
		return archivehdr.Result{Outcome: archivehdr.Unverifiable, Detail: "the volume is a posted file of its own"}
	}
	raw := &bundle.OuterSource{Segments: append([]bundle.Segment(nil), w.outer.Segments...)}
	raw.Segments[0].Start = 0
	vctx := context.WithValue(ctx, utils.MaxPrefetchKey, 1)
	vctx = context.WithValue(vctx, utils.SuppressStreamTrackingKey, true)
	f, err := st.Opener().OpenFile(vctx, w.name+wrapSuffix, os.O_RDONLY, 0)
	if err != nil {
		return archivehdr.Result{Outcome: archivehdr.Unverifiable, Detail: err.Error()}
	}
	defer func() { _ = f.Close() }()
	v, err := archivehdr.Scan(newChargedReader(f, raw, bud), volumeBytes(raw), first.Start, archivehdr.DefaultLimits())
	if v == nil || v.Format == archivehdr.Format7z {
		return archivehdr.Result{Outcome: archivehdr.Unverifiable, Detail: errString(err)}
	}
	for k := range v.Blocks {
		blk := &v.Blocks[k]
		if blk.DataOffset != first.Start {
			continue
		}
		var covered int64
		for _, s := range w.outer.Segments {
			covered += s.End - s.Start + 1
		}
		bad := func(format string, a ...any) archivehdr.Result {
			return archivehdr.Result{Outcome: archivehdr.Mismatch, Detail: fmt.Sprintf(format, a...)}
		}
		switch {
		case !blk.File || blk.Dir || !blk.Stored:
			return bad("the entry before the volume's first byte is not a stored file")
		case blk.Encrypted != (len(w.outer.AESKey) > 0):
			return bad("the entry before the volume's first byte is encrypted = %v, the layout carries a key = %v", blk.Encrypted, len(w.outer.AESKey) > 0)
		case blk.IV != nil && !bytes.Equal(blk.IV, w.outer.AESIV):
			return bad("the IV of the archive header differs from the layout's")
		case blk.DataSize != covered:
			return bad("the entry before the volume's first byte holds %d bytes, the volume's articles %d", blk.DataSize, covered)
		}
		return archivehdr.Result{Outcome: archivehdr.Bound}
	}
	if err == nil && v.Done {
		return archivehdr.Result{Outcome: archivehdr.Mismatch, Detail: fmt.Sprintf("no stored file of the wrapping archive starts at byte %d of the first article", first.Start)}
	}
	return archivehdr.Result{Outcome: archivehdr.Unverifiable, Detail: errString(err)}
}

func errString(err error) string {
	if err == nil {
		return "no answer"
	}
	return err.Error()
}
