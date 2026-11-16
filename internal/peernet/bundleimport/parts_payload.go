package bundleimport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kipsilabs/altmount/internal/peernet/bundle"
	"github.com/kipsilabs/altmount/internal/utils"
)

// Part order from the payload.
//
// The order of the parts of a multi-part file is bound by, in turn: the RAR 5
// volume numbers of the parts, the volume names the articles carry (when they
// parse as one numbered set), and, for a Matroska file, the payload itself. A
// multi-part file whose order none of these binds is DECLINED
// (part_order_unverifiable): two swapped middle volumes would otherwise pass
// every other check.
//
// A Matroska file says where in the stream any piece of it sits: its Clusters
// carry increasing timecodes. The first Cluster that starts after EVERY part
// boundary is read through the staged reader of the assembled file (the bytes
// right after a boundary are in the part's first article, which the accept has
// already fetched and keeps in its article cache, so most boundaries cost
// nothing; the window grows only when no Cluster starts in it, up to
// orderScanWindow), and the timecodes must not decrease along the parts in
// order. A swap of two parts puts a later cluster before an earlier one. A
// boundary with no Cluster in its window, a spent allowance or a container that
// is not Matroska leaves the order unverified. The reads are charged as really
// downloaded, against an allowance of orderArticlesPerBoundary articles per
// boundary on top of the accept's budget (bounded by the node-wide ceiling).
//
// Not verifiable this way, and so declined when no volume number or name binds
// the order: MP4 and MOV (the box chain does not change when the data inside
// the media box is reordered), MPEG-TS and M2TS (a PCR or a continuity counter
// wraps and is no safe order), AVI and every other container, and a Matroska
// file whose clusters are longer than the window.
//
// Cluster timecodes are in units of one TimecodeScale per file, so comparing
// them needs no scaling.
const (
	orderScanWindow = 6 << 20
	// orderArticlesPerBoundary is the allowance, in articles of the part's first
	// article size, of the window search of one boundary.
	orderArticlesPerBoundary = 8
	// orderWorkers is how many boundaries are read at once.
	orderWorkers = 4
	idCluster    = 0x1F43B675
	idTimecode   = 0xE7
)

// clusterTimecodeAt returns the timecode of the first Cluster that starts in
// win: a Cluster ID, its size, and a Timecode element as its first child.
func clusterTimecodeAt(win []byte) (uint64, bool) {
	needle := []byte{0x1F, 0x43, 0xB6, 0x75}
	for from := 0; from < len(win); {
		j := bytes.Index(win[from:], needle)
		if j < 0 {
			return 0, false
		}
		at := from + j
		from = at + 1
		_, sn, _, ok := ebmlVint(win[at+4:], false)
		if !ok {
			continue
		}
		child := at + 4 + sn
		if child+2 > len(win) || win[child] != idTimecode {
			continue
		}
		size, n, unknown, ok := ebmlVint(win[child+1:], false)
		if !ok || unknown || size < 1 || size > 8 {
			continue
		}
		valAt := child + 1 + n
		if valAt+int(size) > len(win) {
			continue
		}
		var v uint64
		for _, c := range win[valAt : valAt+int(size)] {
			v = v<<8 | uint64(c)
		}
		return v, true
	}
	return 0, false
}

// timecodesNondecreasing reports whether the timecodes never decrease.
func timecodesNondecreasing(tcs []uint64) (int, bool) {
	for k := 1; k < len(tcs); k++ {
		if tcs[k] < tcs[k-1] {
			return k, false
		}
	}
	return 0, true
}

// boundaryWindows lists the window sizes tried at one boundary: the part's first
// article, four times that, then the cap.
func boundaryWindows(first, room int64) []int64 {
	first = min(max(first, 64<<10), orderScanWindow)
	var out []int64
	for _, w := range []int64{first, 4 * first, orderScanWindow} {
		w = min(w, room, orderScanWindow)
		if w > 0 && (len(out) == 0 || w > out[len(out)-1]) {
			out = append(out, w)
		}
	}
	return out
}

// verifyPayloadOrder binds the order of the parts of one staged media file to
// its Matroska clusters. It is called for the files whose order no volume number
// or name binds (required). It returns ReasonPartOrder when two boundaries'
// first clusters run backwards, ReasonPartOrderUnverifiable when the order could
// not be confirmed at every boundary, and "" when it is confirmed (or the file
// has nothing to order).
func (i *Importer) verifyPayloadOrder(ctx context.Context, st Staging, b *bundle.Bundle, name string, size int64, bud *costBudget, required bool) (Reason, error) {
	var f *bundle.File
	for k := range b.Files {
		if b.Files[k].Name == name {
			f = &b.Files[k]
		}
	}
	if f == nil || bud == nil || !required {
		return "", nil
	}
	parts := partsOfSliced(f)
	if len(parts) < 2 {
		return "", nil
	}
	unverified := func(format string, a ...any) (Reason, error) {
		return ReasonPartOrderUnverifiable, fmt.Errorf("%s: the order of its %d parts is bound by no volume number or name and %s", name, len(parts), fmt.Sprintf(format, a...))
	}
	ctx, cancel := context.WithTimeout(ctx, structReadTimeout+time.Duration(len(parts))*500*time.Millisecond)
	defer cancel()
	ctx = context.WithValue(ctx, utils.MaxPrefetchKey, 1)
	ctx = context.WithValue(ctx, utils.SuppressStreamTrackingKey, true)
	open := func() (io.ReaderAt, func(), error) {
		fh, err := st.Opener().OpenFile(ctx, name, os.O_RDONLY, 0)
		if err != nil {
			return nil, nil, err
		}
		return fh, func() { _ = fh.Close() }, nil
	}
	fh, closeFh, err := open()
	if err != nil {
		return unverified("the file could not be opened (%v)", err)
	}
	var magic [4]byte
	n, _ := fh.ReadAt(magic[:], 0)
	closeFh()
	if n < 4 || magic != [4]byte{0x1A, 0x45, 0xDF, 0xA3} {
		return unverified("its container (not Matroska) has no check that orders the parts")
	}
	starts := make([]int64, len(parts))
	for k := 1; k < len(parts); k++ {
		starts[k] = starts[k-1] + bundle.Usable(parts[k-1])
	}
	var biggest int64
	for _, ps := range parts[1:] {
		biggest = max(biggest, ps[0].Size)
	}
	bud.grow(int64(len(parts)-1)*orderArticlesPerBoundary*max(biggest, 64<<10), i.cfg.Policy.YencBytes())
	down := stagedDownloads(st)

	type found struct {
		tc uint64
		ok bool
	}
	results := make([]found, len(parts))
	var wg sync.WaitGroup
	sem := make(chan struct{}, orderWorkers)
	var spent atomic.Bool
	for k := 1; k < len(parts); k++ {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if spent.Load() || ctx.Err() != nil {
				return
			}
			h, closeH, err := open()
			if err != nil {
				return
			}
			defer closeH()
			for _, w := range boundaryWindows(parts[k][0].Size, size-starts[k]) {
				win := make([]byte, w)
				got, err := h.ReadAt(win, starts[k])
				if down != nil {
					if oerr := bud.noteStaged(down()); oerr != nil {
						spent.Store(true)
						return
					}
				}
				if err != nil && !errors.Is(err, io.EOF) && got < int(w) {
					return
				}
				if tc, ok := clusterTimecodeAt(win[:got]); ok {
					results[k] = found{tc: tc, ok: true}
					return
				}
			}
		}()
	}
	wg.Wait()
	if spent.Load() {
		return unverified("the allowance for reading the part boundaries was used up")
	}
	tcs := make([]uint64, 0, len(parts)-1)
	for k := 1; k < len(parts); k++ {
		if !results[k].ok {
			return unverified("no Matroska cluster could be read after the boundary before part %d", k+1)
		}
		tcs = append(tcs, results[k].tc)
	}
	if at, ok := timecodesNondecreasing(tcs); !ok {
		return ReasonPartOrder, fmt.Errorf("%s: the first cluster after part %d starts at timecode %d, the one after part %d at %d: the parts are not in stream order",
			name, at, tcs[at-1], at+1, tcs[at])
	}
	return "", nil
}
