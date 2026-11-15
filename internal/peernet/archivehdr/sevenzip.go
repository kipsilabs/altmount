package archivehdr

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// 7z property ids used here.
const (
	z7End          = 0x00
	z7Header       = 0x01
	z7ArchiveProps = 0x02
	z7AddStreams   = 0x03
	z7MainStreams  = 0x04
	z7FilesInfo    = 0x05
	z7PackInfo     = 0x06
	z7UnpackInfo   = 0x07
	z7SubStreams   = 0x08
	z7Size         = 0x09
	z7CRC          = 0x0A
	z7FolderID     = 0x0B
	z7CodersUnpack = 0x0C
	z7NumUnpack    = 0x0D
	z7Encoded      = 0x17

	z7StartHeader = 32
	// z7MaxCount bounds every declared count (folders, streams, coders).
	z7MaxCount = 1 << 14
)

// z7AES is the method id of the 7z AES coder.
var z7AES = []byte{0x06, 0xF1, 0x07, 0x01}

// z7Buf reads 7z numbers and bytes from the end header, never past its end.
type z7Buf struct {
	b   []byte
	bad bool
}

func (z *z7Buf) fail() { z.bad = true; z.b = nil }

func (z *z7Buf) byte() byte {
	if len(z.b) == 0 {
		z.fail()
		return 0
	}
	c := z.b[0]
	z.b = z.b[1:]
	return c
}

func (z *z7Buf) take(n uint64) []byte {
	if n > uint64(len(z.b)) {
		z.fail()
		return nil
	}
	out := z.b[:n]
	z.b = z.b[n:]
	return out
}

// number reads a 7z variable length number: the leading one-bits of the first
// byte say how many further bytes (little endian) follow.
func (z *z7Buf) number() uint64 {
	first := z.byte()
	mask := byte(0x80)
	var value uint64
	for i := 0; i < 8; i++ {
		if first&mask == 0 {
			high := uint64(first & (mask - 1))
			return value | high<<(8*uint(i))
		}
		value |= uint64(z.byte()) << (8 * uint(i))
		mask >>= 1
	}
	return value
}

// count reads a number that is a declared element count: bounded, and not more
// than the bytes left could describe.
func (z *z7Buf) count() int {
	n := z.number()
	if n > z7MaxCount || n > uint64(len(z.b)) {
		z.fail()
		return 0
	}
	return int(n)
}

// digests skips a digest list of n entries and returns how many are defined.
func (z *z7Buf) digests(n int) (defined []bool) {
	defined = make([]bool, n)
	if z.byte() != 0 {
		for i := range defined {
			defined[i] = true
		}
	} else {
		bits := z.take(uint64(n+7) / 8)
		for i := range defined {
			if i/8 < len(bits) {
				defined[i] = bits[i/8]&(0x80>>(uint(i)%8)) != 0
			}
		}
	}
	for _, d := range defined {
		if d {
			z.take(4)
		}
	}
	return defined
}

type z7Folder struct {
	copyOnly  bool
	encrypted bool
	// keyedCopy is an AES coder over a Copy coder and nothing else: stored data
	// that is encrypted, which a keyed layout describes.
	keyedCopy bool
	numPacked int
	unpack    uint64
	hasCRC    bool
}

func scan7z(d *reader, v *Volume, lim Limits) error {
	head, err := d.at(0, z7StartHeader)
	if err != nil {
		return err
	}
	if len(head) < z7StartHeader || head[6] != 0 {
		return fmt.Errorf("%w: 7z start header", ErrMalformed)
	}
	if checkCRC && binary.LittleEndian.Uint32(head[8:12]) != crc32.ChecksumIEEE(head[12:32]) {
		return fmt.Errorf("%w: 7z start header checksum", ErrMalformed)
	}
	off, size := binary.LittleEndian.Uint64(head[12:20]), binary.LittleEndian.Uint64(head[20:28])
	if size == 0 {
		v.Done = true
		return nil
	}
	if size > uint64(lim.Max7zHeader) || off > uint64(d.size) || size > uint64(d.size) {
		return fmt.Errorf("%w: 7z end header outside the volume", ErrMalformed)
	}
	pos, ok := add(z7StartHeader, int64(off))
	if !ok || pos > d.size-int64(size) {
		return fmt.Errorf("%w: 7z end header outside the volume", ErrMalformed)
	}
	body, err := d.at(pos, int(size))
	if err != nil {
		return err
	}
	if checkCRC && binary.LittleEndian.Uint32(head[28:32]) != crc32.ChecksumIEEE(body) {
		return fmt.Errorf("%w: 7z end header checksum", ErrMalformed)
	}
	z := &z7Buf{b: body}
	switch z.byte() {
	case z7Header:
	case z7Encoded:
		return ErrCompressedHeader
	default:
		return fmt.Errorf("%w: 7z end header type", ErrMalformed)
	}
	blocks, err := parse7zHeader(z)
	if err != nil {
		return err
	}
	v.EndHeaderEnd = pos + int64(size)
	v.Blocks = blocks
	v.Done = true
	return nil
}

// parse7zHeader walks the plain end header up to the main streams info, which
// is all that is needed: where each folder's packed streams are and how its
// unpacked streams are cut.
func parse7zHeader(z *z7Buf) ([]Block, error) {
	for {
		switch id := z.byte(); {
		case z.bad:
			return nil, fmt.Errorf("%w: 7z header", ErrMalformed)
		case id == z7End || id == z7FilesInfo:
			return nil, nil // no streams: no data areas
		case id == z7ArchiveProps:
			for {
				t := z.byte()
				if z.bad || t == z7End {
					break
				}
				z.take(z.number())
			}
		case id == z7MainStreams:
			return parse7zStreams(z)
		default:
			return nil, fmt.Errorf("%w: unsupported 7z header property %#x", ErrMalformed, id)
		}
	}
}

func parse7zStreams(z *z7Buf) ([]Block, error) {
	var (
		packPos   uint64
		packSizes []uint64
		folders   []z7Folder
		subs      []int // unpack streams per folder
		subSizes  [][]uint64
	)
	for {
		id := z.byte()
		if z.bad {
			return nil, fmt.Errorf("%w: 7z streams info", ErrMalformed)
		}
		switch id {
		case z7End:
			if subs == nil { // no SubStreamsInfo: one stream per folder
				subs = make([]int, len(folders))
				subSizes = make([][]uint64, len(folders))
				for i, f := range folders {
					subs[i], subSizes[i] = 1, []uint64{f.unpack}
				}
			}
			return layout7z(packPos, packSizes, folders, subs, subSizes)
		case z7PackInfo:
			packPos = z.number()
			n := z.count()
			for {
				t := z.byte()
				if z.bad || t == z7End {
					break
				}
				switch t {
				case z7Size:
					for i := 0; i < n; i++ {
						packSizes = append(packSizes, z.number())
					}
				case z7CRC:
					z.digests(n)
				default:
					z.fail()
				}
			}
		case z7UnpackInfo:
			var err error
			if folders, err = parse7zFolders(z); err != nil {
				return nil, err
			}
		case z7SubStreams:
			var err error
			if subs, subSizes, err = parse7zSubStreams(z, folders); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("%w: unsupported 7z streams property %#x", ErrMalformed, id)
		}
	}
}

func parse7zFolders(z *z7Buf) ([]z7Folder, error) {
	bad := func() ([]z7Folder, error) { return nil, fmt.Errorf("%w: 7z folders", ErrMalformed) }
	if z.byte() != z7FolderID {
		return bad()
	}
	n := z.count()
	if z.byte() != 0 { // external
		return bad()
	}
	folders := make([]z7Folder, 0, n)
	outs := make([]int, 0, n)
	for i := 0; i < n && !z.bad; i++ {
		coders := z.count()
		if coders < 1 || coders > 8 {
			return bad()
		}
		var f z7Folder
		var totalIn, totalOut int
		var copies int
		for c := 0; c < coders && !z.bad; c++ {
			flags := z.byte()
			if flags&0x80 != 0 {
				return bad()
			}
			id := z.take(uint64(flags & 0x0F))
			in, out := 1, 1
			if flags&0x10 != 0 {
				in, out = z.count(), z.count()
			}
			if flags&0x20 != 0 {
				z.take(z.number())
			}
			if string(id) == string(z7AES) {
				f.encrypted = true
			}
			if len(id) == 1 && id[0] == 0x00 && in == 1 && out == 1 {
				copies++
			}
			f.copyOnly = coders == 1 && copies == 1
			totalIn += in
			totalOut += out
		}
		f.keyedCopy = coders == 2 && copies == 1 && f.encrypted
		pairs := totalOut - 1
		if pairs < 0 || totalIn-pairs < 1 || z.bad {
			return bad()
		}
		for p := 0; p < pairs*2; p++ {
			z.number()
		}
		f.numPacked = totalIn - pairs
		if f.numPacked > 1 {
			for p := 0; p < f.numPacked; p++ {
				z.number()
			}
		}
		folders = append(folders, f)
		outs = append(outs, totalOut)
	}
	if z.byte() != z7CodersUnpack {
		return bad()
	}
	for i := range folders {
		var last uint64
		for o := 0; o < outs[i]; o++ {
			last = z.number()
		}
		folders[i].unpack = last // the last out stream is the folder's result for the layouts that matter
	}
	for {
		t := z.byte()
		if z.bad {
			return bad()
		}
		switch t {
		case z7End:
			return folders, nil
		case z7CRC:
			for i, def := range z.digests(len(folders)) {
				folders[i].hasCRC = def
			}
		default:
			return bad()
		}
	}
}

func parse7zSubStreams(z *z7Buf, folders []z7Folder) ([]int, [][]uint64, error) {
	bad := func() ([]int, [][]uint64, error) { return nil, nil, fmt.Errorf("%w: 7z substreams", ErrMalformed) }
	subs := make([]int, len(folders))
	for i := range subs {
		subs[i] = 1
	}
	var sizes [][]uint64
	total := 0
	for {
		t := z.byte()
		if z.bad {
			return bad()
		}
		switch t {
		case z7End:
			if sizes == nil {
				sizes = make([][]uint64, len(folders))
				for i, f := range folders {
					if subs[i] == 1 {
						sizes[i] = []uint64{f.unpack}
					}
				}
			}
			return subs, sizes, nil
		case z7NumUnpack:
			total = 0
			for i := range subs {
				subs[i] = z.count()
				if total += subs[i]; total > z7MaxCount {
					return bad()
				}
			}
		case z7Size:
			sizes = make([][]uint64, len(folders))
			for i, f := range folders {
				if subs[i] == 0 {
					continue
				}
				var sum uint64
				for k := 0; k < subs[i]-1; k++ {
					s := z.number()
					if s > f.unpack || sum > f.unpack-s {
						return bad()
					}
					sum += s
					sizes[i] = append(sizes[i], s)
				}
				sizes[i] = append(sizes[i], f.unpack-sum)
			}
		case z7CRC:
			unknown := 0
			for i, f := range folders {
				if subs[i] != 1 || !f.hasCRC {
					unknown += subs[i]
				}
			}
			z.digests(unknown)
		default:
			return bad()
		}
	}
}

// layout7z turns the stream tables into data blocks: for each folder, where its
// packed stream starts in the volume and how its unpacked streams are cut.
func layout7z(packPos uint64, packSizes []uint64, folders []z7Folder, subs []int, subSizes [][]uint64) ([]Block, error) {
	bad := func() ([]Block, error) { return nil, fmt.Errorf("%w: 7z layout", ErrMalformed) }
	if packPos > 1<<62 {
		return bad()
	}
	pos, ok := add(z7StartHeader, int64(packPos))
	if !ok {
		return bad()
	}
	if len(subs) != len(folders) || len(subSizes) != len(folders) {
		return bad()
	}
	var out []Block
	next := 0
	for i, f := range folders {
		if next+f.numPacked > len(packSizes) {
			return bad()
		}
		start := pos
		var packed uint64
		for k := 0; k < f.numPacked; k++ {
			ps := packSizes[next+k]
			if ps > 1<<62 {
				return bad()
			}
			packed += ps
			if pos, ok = add(pos, int64(ps)); !ok {
				return bad()
			}
		}
		next += f.numPacked
		stored := f.copyOnly && f.numPacked == 1 && packed == f.unpack && !f.encrypted
		if subs[i] == 0 {
			continue
		}
		if f.keyedCopy && f.numPacked == 1 && subs[i] == 1 {
			// stored data under an AES coder: the data area is the packed stream (the
			// encrypted bytes), whose plain size is not recorded exactly (padding).
			out = append(out, Block{HeaderOffset: start, DataOffset: start, DataSize: int64(packed), UnpackedSize: -1,
				File: true, Stored: true, Encrypted: true})
			continue
		}
		if !stored {
			out = append(out, Block{HeaderOffset: start, DataOffset: start, DataSize: int64(packed), UnpackedSize: int64(min(f.unpack, 1<<62)),
				File: true, Encrypted: f.encrypted})
			continue
		}
		at := start
		for _, s := range subSizes[i] {
			if s > 1<<62 {
				return bad()
			}
			out = append(out, Block{HeaderOffset: start, DataOffset: at, DataSize: int64(s), UnpackedSize: int64(s), File: true, Stored: true})
			if at, ok = add(at, int64(s)); !ok {
				return bad()
			}
		}
	}
	return out, nil
}
