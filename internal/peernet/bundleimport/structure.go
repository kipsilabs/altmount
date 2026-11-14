package bundleimport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/javi11/nntppool/v5"

	"github.com/kipsilabs/altmount/internal/contentverify"
	"github.com/kipsilabs/altmount/internal/utils"
)

// Structural media verification. The head signature check (contentverify)
// only proves the first bytes decode. This reads the container's own index at
// several positions, including the end of the file, through the staged layout,
// with a small bounded number of small reads:
//
//	Matroska / WebM  EBML header, Segment size against the file size, and the
//	                 SeekHead: every element it points at (Info, Tracks, Cues,
//	                 ...) must start with the ID the SeekHead promises, which
//	                 reaches the Cues at the tail. A file muxed for streaming
//	                 has no SeekHead (and may have an unknown-size Segment): it
//	                 needs a level-1 element first, a Segment that ends at the
//	                 end of the file when its size is known, and readable
//	                 quartiles and tail.
//	MP4 / MOV        the whole top-level box chain must land exactly on the
//	                 end of the file (a zero-filled tail of up to 64 KiB, which
//	                 some writers pad with, is allowed) and contain moov and
//	                 mdat. A fragmented file (moof boxes) has thousands of
//	                 boxes: its first boxes are walked, and a box of the
//	                 fragment family must end exactly at the end of the file.
//	MPEG-TS / M2TS   the sync byte on the packet grid (188, 192 with the
//	                 4-byte timestamp, or 204 bytes) at the start, quartiles and
//	                 the last packet.
//	anything else    full-length reads at the quartiles and the last bytes.
//
// There is no deeper media parser in the repository (no MediaStructure probe),
// so this is the strongest check available without adding one.

const (
	structReadTimeout = 20 * time.Second
	maxBoxes          = 4096
	maxSeekEntries    = 16
	tailWindow        = 4096
)

var errStructure = errors.New("media structure is not valid")

type readerAt struct {
	ctx  context.Context
	f    io.ReaderAt
	size int64
	n    int // reads made, bounded
}

const maxStructReads = 64

func (r *readerAt) read(off int64, n int) ([]byte, error) {
	if r.n++; r.n > maxStructReads {
		return nil, fmt.Errorf("%w: too many reads", errStructure)
	}
	if off < 0 || off >= r.size {
		return nil, fmt.Errorf("%w: offset %d outside the file", errStructure, off)
	}
	n = int(min(int64(n), r.size-off))
	buf := make([]byte, n)
	got, err := r.f.ReadAt(buf, off)
	if got == n {
		return buf, nil
	}
	if err == nil || errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%w: short read at %d", errStructure, off)
	}
	return nil, err
}

// verifyStructure runs the container check for the staged file name.
func verifyStructure(ctx context.Context, op contentverify.Opener, name string, size int64) error {
	ctx, cancel := context.WithTimeout(ctx, structReadTimeout)
	defer cancel()
	ctx = context.WithValue(ctx, utils.MaxPrefetchKey, 1)
	ctx = context.WithValue(ctx, utils.SuppressStreamTrackingKey, true)
	f, err := op.OpenFile(ctx, name, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	r := &readerAt{ctx: ctx, f: f, size: size}
	head, err := r.read(0, 16)
	if err != nil {
		return err
	}
	switch {
	case bytes.HasPrefix(head, []byte{0x1A, 0x45, 0xDF, 0xA3}):
		return verifyMatroska(r)
	case len(head) >= 8 && string(head[4:8]) == "ftyp":
		return verifyMP4(r)
	case tsStride(r) != 0:
		return verifyTS(r, tsStride(r))
	}
	return verifyWindows(r)
}

// ---- generic ----

func verifyWindows(r *readerAt) error {
	for _, q := range []int64{0, 1, 2, 3} {
		off := r.size / 4 * q
		if _, err := r.read(off, 512); err != nil {
			return err
		}
	}
	_, err := r.read(max(r.size-tailWindow, 0), tailWindow)
	return err
}

// ---- MP4 ----

// maxFragHeadBoxes is how many top-level boxes of a fragmented file are walked
// from the start before the end is checked on its own.
const maxFragHeadBoxes = 8

// fragTailWindows are the sizes of the tail windows searched for the last box of
// a fragmented file; the second covers fragments of several megabytes.
var fragTailWindows = []int64{4 << 20, 16 << 20}

// maxZeroTail is the zero padding some writers leave after the last box.
const maxZeroTail = 64 << 10

// fragmentBoxTypes are the box types a fragmented file can end with.
var fragmentBoxTypes = map[string]bool{
	"moof": true, "mdat": true, "mfra": true, "free": true, "skip": true,
	"styp": true, "sidx": true, "emsg": true, "prft": true,
}

func verifyMP4(r *readerAt) error {
	var off int64
	seen := map[string]bool{}
	walked := 0
	for boxes := 0; off < r.size; boxes++ {
		if boxes > maxBoxes {
			return fmt.Errorf("%w: too many boxes", errStructure)
		}
		hdr, err := r.read(off, 16)
		if err != nil {
			return err
		}
		if len(hdr) < 8 {
			return fmt.Errorf("%w: truncated box header", errStructure)
		}
		if zeroTail, err := r.isZeroTail(off, hdr); err != nil {
			return err
		} else if zeroTail {
			break
		}
		size := int64(uint32(hdr[0])<<24 | uint32(hdr[1])<<16 | uint32(hdr[2])<<8 | uint32(hdr[3]))
		typ := string(hdr[4:8])
		if !printableBoxType(typ) {
			return fmt.Errorf("%w: bad box type at %d", errStructure, off)
		}
		switch size {
		case 0:
			size = r.size - off // runs to the end of the file
		case 1:
			if len(hdr) < 16 {
				return fmt.Errorf("%w: truncated large box", errStructure)
			}
			size = 0
			for _, b := range hdr[8:16] {
				size = size<<8 | int64(b)
			}
		}
		if size < 8 || off+size > r.size || off+size < off {
			return fmt.Errorf("%w: box %q at %d runs past the file", errStructure, typ, off)
		}
		seen[typ] = true
		off += size
		walked++
		// A fragmented file: thousands of moof/mdat pairs cannot all be walked
		// with a bounded number of reads. Walk the head, then check the end.
		if seen["moof"] && walked >= maxFragHeadBoxes && off < r.size {
			if !seen["moov"] || !seen["mdat"] {
				return fmt.Errorf("%w: fragmented file without moov and mdat", errStructure)
			}
			return verifyFragmentedTail(r)
		}
	}
	if !seen["moov"] || !seen["mdat"] || (off != r.size && !r.tailIsZero(off)) {
		return fmt.Errorf("%w: box chain incomplete", errStructure)
	}
	return nil
}

// isZeroTail reports whether everything from off to the end of the file is
// zero and short (padding after the last box). hdr is the 16 bytes at off.
func (r *readerAt) isZeroTail(off int64, hdr []byte) (bool, error) {
	for _, c := range hdr {
		if c != 0 {
			return false, nil
		}
	}
	return r.tailIsZero(off), nil
}

func (r *readerAt) tailIsZero(off int64) bool {
	if r.size-off > maxZeroTail || off >= r.size {
		return off >= r.size
	}
	buf, err := r.read(off, int(r.size-off))
	if err != nil {
		return false
	}
	for _, c := range buf {
		if c != 0 {
			return false
		}
	}
	return true
}

// verifyFragmentedTail requires a fragment-family box that ends exactly at the
// end of the file, found in a tail window. Random bytes at the end do not
// satisfy it: a header with a known type whose size lands on the last byte.
func verifyFragmentedTail(r *readerAt) error {
	for _, w := range fragTailWindows {
		n := min(w, r.size)
		base := r.size - n
		buf, err := r.read(base, int(n))
		if err != nil {
			return err
		}
		for o := 0; o+8 <= len(buf); o++ {
			size := int64(uint32(buf[o])<<24 | uint32(buf[o+1])<<16 | uint32(buf[o+2])<<8 | uint32(buf[o+3]))
			if size < 8 || int64(o)+size != int64(len(buf)) {
				continue
			}
			if fragmentBoxTypes[string(buf[o+4:o+8])] {
				return nil
			}
		}
		if n == r.size {
			break
		}
	}
	return fmt.Errorf("%w: no box ends at the end of the fragmented file", errStructure)
}

func printableBoxType(t string) bool {
	for _, c := range []byte(t) {
		letter := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
		if !letter && c != ' ' && c != '_' && c != 0xA9 {
			return false
		}
	}
	return true
}

// ---- MPEG-TS ----

func tsStride(r *readerAt) int64 {
	buf, err := r.read(0, 4*204)
	if err != nil {
		return 0
	}
	for _, c := range []struct{ stride, sync int }{{188, 0}, {192, 4}, {204, 0}} {
		ok := len(buf) > c.sync+3*c.stride
		for k := 0; ok && k < 4; k++ {
			ok = buf[c.sync+k*c.stride] == 0x47
		}
		if ok {
			return int64(c.stride)
		}
	}
	return 0
}

func verifyTS(r *readerAt, stride int64) error {
	sync := int64(0)
	if stride == 192 {
		sync = 4
	}
	packets := r.size / stride
	if packets < 8 {
		return fmt.Errorf("%w: too short for a transport stream", errStructure)
	}
	for _, q := range []int64{1, 2, 3} {
		if err := tsSync(r, packets/4*q*stride+sync); err != nil {
			return err
		}
	}
	return tsSync(r, (packets-1)*stride+sync)
}

func tsSync(r *readerAt, off int64) error {
	b, err := r.read(off, 1)
	if err != nil {
		return err
	}
	if b[0] != 0x47 {
		return fmt.Errorf("%w: lost transport stream sync at %d", errStructure, off)
	}
	return nil
}

// ---- Matroska / WebM ----

// ebmlVint parses an EBML variable-length integer. keepMarker keeps the length
// marker bit (element IDs); unknown reports the all-ones "unknown size" value.
func ebmlVint(b []byte, keepMarker bool) (v uint64, n int, unknown bool, ok bool) {
	if len(b) == 0 || b[0] == 0 {
		return 0, 0, false, false
	}
	n = 1
	for mask := byte(0x80); b[0]&mask == 0; mask >>= 1 {
		n++
	}
	if n > 8 || len(b) < n {
		return 0, 0, false, false
	}
	v = uint64(b[0])
	if !keepMarker {
		v &= (1 << (8 - uint(n))) - 1
	}
	allOnes := (b[0] | byte(0xFF<<(8-uint(n)))) == 0xFF
	for _, c := range b[1:n] {
		v = v<<8 | uint64(c)
		allOnes = allOnes && c == 0xFF
	}
	return v, n, !keepMarker && allOnes, true
}

const (
	idEBML     = 0x1A45DFA3
	idSegment  = 0x18538067
	idSeekHead = 0x114D9B74
	idSeek     = 0x4DBB
	idSeekID   = 0x53AB
	idSeekPos  = 0x53AC
	idVoid     = 0xEC
)

func verifyMatroska(r *readerAt) error {
	hdr, err := r.read(0, 64)
	if err != nil {
		return err
	}
	_, idLen, _, ok := ebmlVint(hdr, true)
	if !ok {
		return fmt.Errorf("%w: bad EBML header", errStructure)
	}
	hsize, sLen, _, ok := ebmlVint(hdr[idLen:], false)
	if !ok || hsize == 0 || hsize > 1024 {
		return fmt.Errorf("%w: bad EBML header size", errStructure)
	}
	segOff := int64(idLen+sLen) + int64(hsize)
	seg, err := r.read(segOff, 16)
	if err != nil {
		return err
	}
	id, idn, _, ok := ebmlVint(seg, true)
	if !ok || id != idSegment {
		return fmt.Errorf("%w: no Segment after the EBML header", errStructure)
	}
	ssize, sn, unknown, ok := ebmlVint(seg[idn:], false)
	if !ok {
		return fmt.Errorf("%w: bad Segment size", errStructure)
	}
	start := segOff + int64(idn+sn)
	if !unknown {
		end := start + int64(ssize)
		if end < start || end > r.size || r.size-end > 1<<16 {
			return fmt.Errorf("%w: Segment ends at %d, file is %d bytes", errStructure, end, r.size)
		}
	}
	return verifySeekHead(r, start)
}

// level1IDs are the elements that can start a Segment's children.
var level1IDs = map[uint64]bool{
	idSeekHead: true, 0x1549A966: true /* Info */, 0x1654AE6B: true /* Tracks */, 0x1F43B675: true, /* Cluster */
	0x1C53BB6B: true /* Cues */, 0x1254C367: true /* Tags */, 0x1043A770: true /* Chapters */, 0x1941A469: true, /* Attachments */
	idVoid: true, 0xBF: true, /* CRC-32 */
}

func verifySeekHead(r *readerAt, segStart int64) error {
	buf, err := r.read(segStart, 1024)
	if err != nil {
		return err
	}
	// The SeekHead is the first meaningful child (a Void may precede it). A
	// file muxed for streaming has none.
	for len(buf) > 0 {
		id, idn, _, ok := ebmlVint(buf, true)
		if !ok || !level1IDs[id] {
			return fmt.Errorf("%w: unreadable element after the Segment header", errStructure)
		}
		sz, sn, _, ok := ebmlVint(buf[idn:], false)
		if !ok || uint64(len(buf)-idn-sn) < sz && id == idSeekHead {
			return fmt.Errorf("%w: bad SeekHead", errStructure)
		}
		body := idn + sn
		switch id {
		case idVoid:
			if uint64(len(buf)-body) < sz {
				return verifyNoSeekHead(r)
			}
			buf = buf[body+int(sz):]
		case idSeekHead:
			return checkSeeks(r, segStart, buf[body:body+int(sz)])
		default:
			return verifyNoSeekHead(r)
		}
	}
	return verifyNoSeekHead(r)
}

// verifyNoSeekHead checks a file that has no SeekHead (a live or streaming
// mux): a level-1 element opens the Segment (checked by the caller), the
// Segment size already had to land on the end of the file when it is known, and
// the quartiles and the tail must be readable through the layout.
func verifyNoSeekHead(r *readerAt) error { return verifyWindows(r) }

func checkSeeks(r *readerAt, segStart int64, head []byte) error {
	entries := 0
	for len(head) > 0 && entries < maxSeekEntries {
		id, idn, _, ok := ebmlVint(head, true)
		sz, sn, _, ok2 := ebmlVint(head[min(idn, len(head)):], false)
		if !ok || !ok2 || id != idSeek || uint64(len(head)-idn-sn) < sz {
			return fmt.Errorf("%w: bad Seek entry", errStructure)
		}
		entry := head[idn+sn : idn+sn+int(sz)]
		head = head[idn+sn+int(sz):]
		wantID, pos, err := parseSeek(entry)
		if err != nil {
			return err
		}
		got, err := r.read(segStart+pos, len(wantID))
		if err != nil {
			return err
		}
		if !bytes.Equal(got, wantID) {
			return fmt.Errorf("%w: the SeekHead promises %x at %d, the file has %x", errStructure, wantID, segStart+pos, got)
		}
		entries++
	}
	if entries == 0 {
		return fmt.Errorf("%w: empty SeekHead", errStructure)
	}
	return nil
}

func parseSeek(b []byte) (id []byte, pos int64, err error) {
	havePos := false
	for len(b) > 0 {
		eid, idn, _, ok := ebmlVint(b, true)
		sz, sn, _, ok2 := ebmlVint(b[min(idn, len(b)):], false)
		if !ok || !ok2 || uint64(len(b)-idn-sn) < sz {
			return nil, 0, fmt.Errorf("%w: bad Seek entry", errStructure)
		}
		body := b[idn+sn : idn+sn+int(sz)]
		b = b[idn+sn+int(sz):]
		switch eid {
		case idSeekID:
			if len(body) < 1 || len(body) > 4 {
				return nil, 0, fmt.Errorf("%w: bad SeekID", errStructure)
			}
			id = append([]byte(nil), body...)
		case idSeekPos:
			if len(body) < 1 || len(body) > 8 {
				return nil, 0, fmt.Errorf("%w: bad SeekPosition", errStructure)
			}
			var v uint64
			for _, c := range body {
				v = v<<8 | uint64(c)
			}
			if v > 1<<50 {
				return nil, 0, fmt.Errorf("%w: SeekPosition out of range", errStructure)
			}
			pos, havePos = int64(v), true
		}
	}
	if id == nil || !havePos {
		return nil, 0, fmt.Errorf("%w: incomplete Seek entry", errStructure)
	}
	return id, pos, nil
}

// structureReason maps a structure error to the rejection reason.
func structureReason(err error) Reason {
	switch {
	case errors.Is(err, errStructure):
		return ReasonStructureInvalid
	case errors.Is(err, nntppool.ErrArticleNotFound):
		return ReasonMediaMissing
	}
	return ReasonMediaInconclusive
}
