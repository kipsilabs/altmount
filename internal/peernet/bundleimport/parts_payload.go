package bundleimport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"

	"github.com/kipsilabs/altmount/internal/peernet/bundle"
	"github.com/kipsilabs/altmount/internal/utils"
)

// Part order from the payload.
//
// When the volume names do not parse (an obfuscated release) and the archive
// format declares no volume number (RAR 4, split 7z), nothing in the archives
// says which part follows which, so two swapped middle volumes are accepted by
// every other check. A Matroska file does say where in the stream any piece of
// it sits: its Clusters carry increasing timecodes. The first Cluster after
// each part boundary is read through the staged reader of the assembled file;
// the timecodes must not decrease along the parts in order. A swap of two
// middle volumes puts a later cluster before an earlier one.
//
// Bounded: at most orderMaxBoundaries boundaries (the first and the last
// always, the rest chosen with crypto/rand), each read as one window of
// orderScanWindow bytes charged to the accept's budget; a boundary whose window
// holds no cluster start (a cluster longer than the window) or that does not
// fit the budget is skipped. The check needs two boundaries that were read.
//
// Cluster timecodes are in units of one TimecodeScale per file, so comparing
// them needs no scaling.
//
// Not verifiable this way (the order of their parts is bound only by names and
// by RAR 5 volume numbers, when they exist): MP4 and MOV, MPEG-TS and M2TS (a
// PCR or a continuity counter wraps and is no safe order), AVI and every other
// container, a Matroska file with a single Cluster per part or clusters longer
// than the window, and a file of fewer than three parts.
const (
	orderMaxBoundaries = 8
	orderScanWindow    = 6 << 20
	idCluster          = 0x1F43B675
	idTimecode         = 0xE7
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

// pickBoundaries chooses up to max of the n-1 part boundaries (numbered 1..n-1):
// the first and the last, then random ones, returned in order.
func pickBoundaries(n, max int) []int {
	if n < 2 {
		return nil
	}
	all := make([]int, 0, n-1)
	for k := 1; k < n; k++ {
		all = append(all, k)
	}
	if len(all) <= max {
		return all
	}
	rest := append([]int(nil), all[1:len(all)-1]...)
	shuffle(rest)
	picks := append([]int{all[0], all[len(all)-1]}, rest[:max-2]...)
	slices.Sort(picks)
	return picks
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

// verifyPayloadOrder binds the order of the parts of one staged media file to
// its Matroska clusters. It returns ReasonPartOrder when two boundaries' first
// clusters run backwards, and "" when the order is confirmed or cannot be judged.
func (i *Importer) verifyPayloadOrder(ctx context.Context, st Staging, b *bundle.Bundle, name string, size int64, bud *costBudget) (Reason, error) {
	var f *bundle.File
	for k := range b.Files {
		if b.Files[k].Name == name {
			f = &b.Files[k]
		}
	}
	parts := partsOfSliced(f)
	if f == nil || len(parts) < 3 || bud == nil {
		return "", nil
	}
	ctx, cancel := context.WithTimeout(ctx, structReadTimeout)
	defer cancel()
	ctx = context.WithValue(ctx, utils.MaxPrefetchKey, 1)
	ctx = context.WithValue(ctx, utils.SuppressStreamTrackingKey, true)
	fh, err := st.Opener().OpenFile(ctx, name, os.O_RDONLY, 0)
	if err != nil {
		return "", nil
	}
	defer func() { _ = fh.Close() }()
	var magic [4]byte
	if n, _ := fh.ReadAt(magic[:], 0); n < 4 || magic != [4]byte{0x1A, 0x45, 0xDF, 0xA3} {
		return "", nil // not Matroska: no cheap property orders its parts
	}
	starts := make([]int64, len(parts))
	for k := 1; k < len(parts); k++ {
		starts[k] = starts[k-1] + bundle.Usable(parts[k-1])
	}
	var tcs []uint64
	var from []int
	for _, k := range pickBoundaries(len(parts), orderMaxBoundaries) {
		n := int(min(orderScanWindow, size-starts[k]))
		if n <= 0 || bud.chargeStaged(int64(n)) != nil {
			continue
		}
		win := make([]byte, n)
		got, err := fh.ReadAt(win, starts[k])
		i.verifyDebit(int64(n))
		if err != nil && !errors.Is(err, io.EOF) && got < n {
			if ctx.Err() != nil {
				return "", nil
			}
			continue
		}
		if tc, ok := clusterTimecodeAt(win[:got]); ok {
			tcs, from = append(tcs, tc), append(from, k)
		}
	}
	if at, ok := timecodesNondecreasing(tcs); !ok {
		return ReasonPartOrder, fmt.Errorf("%s: the first cluster after part %d starts at timecode %d, the one after part %d at %d: the parts are not in stream order",
			name, from[at-1], tcs[at-1], from[at], tcs[at])
	}
	return "", nil
}
