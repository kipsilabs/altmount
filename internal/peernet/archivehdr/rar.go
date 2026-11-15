package archivehdr

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// vint decodes a RAR 5 variable length integer (7 bits per byte, little endian,
// high bit = more) of at most ten bytes.
func vint(b []byte) (v uint64, n int, ok bool) {
	for i := 0; i < len(b) && i < 10; i++ {
		if i == 9 && b[i] > 1 {
			return 0, 0, false
		}
		v |= uint64(b[i]&0x7f) << (7 * uint(i))
		if b[i]&0x80 == 0 {
			return v, i + 1, true
		}
	}
	return 0, 0, false
}

// cursor reads vints and fixed fields from one header, never past its end.
type cursor struct {
	b   []byte
	err bool
}

func (c *cursor) vint() uint64 {
	v, n, ok := vint(c.b)
	if !ok {
		c.err = true
		return 0
	}
	c.b = c.b[n:]
	return v
}

func (c *cursor) skip(n uint64) {
	if n > uint64(len(c.b)) {
		c.err = true
		c.b = nil
		return
	}
	c.b = c.b[n:]
}

func (c *cursor) take(n int) []byte {
	if n < 0 || n > len(c.b) {
		c.err = true
		c.b = nil
		return nil
	}
	out := c.b[:n]
	c.b = c.b[n:]
	return out
}

// RAR 5 block header types and flags.
const (
	r5Main    = 1
	r5File    = 2
	r5Service = 3
	r5Encrypt = 4
	r5End     = 5

	r5FlagExtra     = 0x1
	r5FlagData      = 0x2
	r5FlagSplitPrev = 0x8
	r5FlagSplitNext = 0x10

	r5FileDir      = 0x1
	r5FileMTime    = 0x2
	r5FileCRC      = 0x4
	r5FileUnkSize  = 0x8
	r5ExtraEncrypt = 0x1
	maxExtraRecs   = 32
)

func scanRAR5(d *reader, v *Volume, upTo int64, lim Limits) error {
	off := int64(len(rar5Sig))
	for blocks := 0; ; blocks++ {
		if off >= d.size || off > upTo {
			v.Done = true
			return nil
		}
		if blocks >= lim.MaxBlocks {
			return ErrBudget
		}
		hdr, err := d.at(off, min(1024, lim.MaxHeader))
		if err != nil {
			return err
		}
		if len(hdr) < 6 {
			return fmt.Errorf("%w: truncated block header", ErrMalformed)
		}
		sz, n, ok := vint(hdr[4:])
		if !ok || sz == 0 || sz > uint64(lim.MaxHeader) {
			return fmt.Errorf("%w: bad header size", ErrMalformed)
		}
		total := 4 + n + int(sz)
		if int64(total) > d.size-off {
			return fmt.Errorf("%w: header runs past the volume", ErrMalformed)
		}
		if total > len(hdr) {
			if hdr, err = d.at(off, total); err != nil {
				return err
			}
		}
		hdr = hdr[:total]
		if checkCRC && binary.LittleEndian.Uint32(hdr[:4]) != crc32.ChecksumIEEE(hdr[4:]) {
			return fmt.Errorf("%w: header checksum", ErrMalformed)
		}
		blk, kind, err := parseRAR5(hdr[4+n:], off, int64(total))
		if err != nil {
			return err
		}
		switch kind {
		case r5Main:
			v.VolumeNumber = rar5VolumeNumber(hdr[4+n:])
		case r5Encrypt:
			return ErrEncryptedHeaders
		case r5End:
			v.Done = true
			return nil
		case r5File, r5Service:
			v.Blocks = append(v.Blocks, blk)
		}
		next, ok := add(blk.DataOffset, blk.DataSize)
		if !ok {
			return fmt.Errorf("%w: data area overflows", ErrMalformed)
		}
		off = next
	}
}

// parseRAR5 decodes the part of a block header after its size field.
func parseRAR5(body []byte, off, total int64) (Block, uint64, error) {
	bad := func(what string) (Block, uint64, error) {
		return Block{}, 0, fmt.Errorf("%w: %s", ErrMalformed, what)
	}
	c := &cursor{b: body}
	typ, flags := c.vint(), c.vint()
	var extra, dataSize uint64
	if flags&r5FlagExtra != 0 {
		extra = c.vint()
	}
	if flags&r5FlagData != 0 {
		dataSize = c.vint()
	}
	if c.err || extra > uint64(len(c.b)) || dataSize > 1<<62 {
		return bad("block header fields")
	}
	blk := Block{HeaderOffset: off, DataOffset: off + total, DataSize: int64(dataSize), UnpackedSize: -1,
		SplitBefore: flags&r5FlagSplitPrev != 0, SplitAfter: flags&r5FlagSplitNext != 0}
	if typ != r5File && typ != r5Service {
		return blk, typ, nil
	}
	blk.File = typ == r5File
	// File and service blocks: the extra area is the tail of the header.
	extraArea := c.b[uint64(len(c.b))-extra:]
	c.b = c.b[:uint64(len(c.b))-extra]
	fileFlags := c.vint()
	unpacked := c.vint()
	c.vint() // attributes
	if fileFlags&r5FileMTime != 0 {
		c.skip(4)
	}
	if fileFlags&r5FileCRC != 0 {
		c.skip(4)
	}
	comp := c.vint()
	c.vint() // host OS
	nameLen := c.vint()
	if c.err {
		return bad("file header fields")
	}
	if nameLen <= maxNameLen && nameLen <= uint64(len(c.b)) {
		blk.Name = cleanName(c.b[:nameLen])
	}
	blk.Dir = fileFlags&r5FileDir != 0
	blk.Stored = (comp>>7)&7 == 0
	if fileFlags&r5FileUnkSize == 0 && unpacked <= 1<<62 {
		blk.UnpackedSize = int64(unpacked)
	}
	if err := parseRAR5Extra(extraArea, &blk); err != nil {
		return Block{}, 0, err
	}
	return blk, typ, nil
}

// rar5VolumeNumber reads the volume number of a RAR 5 main header (body after
// the size field): archive flags bit 0x1 says the archive is a volume, bit 0x2
// that a volume number follows (the first volume has none and is 0). It returns
// -1 for an archive that is not a volume or a header it cannot read.
func rar5VolumeNumber(body []byte) int64 {
	c := &cursor{b: body}
	c.vint() // type
	flags := c.vint()
	if flags&r5FlagExtra != 0 {
		c.vint()
	}
	if flags&r5FlagData != 0 {
		c.vint()
	}
	arch := c.vint()
	if c.err || arch&0x1 == 0 {
		return -1
	}
	if arch&0x2 == 0 {
		return 0
	}
	n := c.vint()
	if c.err || n > 1<<40 {
		return -1
	}
	return int64(n)
}

// parseRAR5Extra reads the extra records of a file header for the file
// encryption record (type 1): version, flags, KDF count, 16-byte salt, 16-byte
// IV.
func parseRAR5Extra(area []byte, blk *Block) error {
	c := &cursor{b: area}
	for rec := 0; len(c.b) > 0; rec++ {
		if rec >= maxExtraRecs {
			return fmt.Errorf("%w: too many extra records", ErrMalformed)
		}
		size := c.vint()
		if c.err || size > uint64(len(c.b)) {
			return fmt.Errorf("%w: extra record size", ErrMalformed)
		}
		r := &cursor{b: c.take(int(size))}
		if r.vint() != r5ExtraEncrypt || r.err {
			continue
		}
		r.vint() // version
		r.vint() // flags
		r.take(1)
		r.take(16) // salt
		iv := r.take(16)
		if r.err {
			return fmt.Errorf("%w: encryption record", ErrMalformed)
		}
		blk.Encrypted = true
		blk.IV = append([]byte(nil), iv...)
	}
	return nil
}

// RAR 4 block types and flags.
const (
	r4Main    = 0x73
	r4File    = 0x74
	r4Service = 0x7A
	r4End     = 0x7B

	r4LongBlock  = 0x8000
	r4MainPass   = 0x80
	r4SplitPrev  = 0x01
	r4SplitNext  = 0x02
	r4Password   = 0x04
	r4DirMask    = 0xE0
	r4Large      = 0x100
	r4StoreBytes = 0x30
)

func scanRAR4(d *reader, v *Volume, upTo int64, lim Limits) error {
	off := int64(len(rar4Sig))
	for blocks := 0; ; blocks++ {
		if off >= d.size || off > upTo {
			v.Done = true
			return nil
		}
		if blocks >= lim.MaxBlocks {
			return ErrBudget
		}
		base, err := d.at(off, 11)
		if err != nil {
			return err
		}
		if len(base) < 7 {
			return fmt.Errorf("%w: truncated block header", ErrMalformed)
		}
		typ, flags, hsz := base[2], binary.LittleEndian.Uint16(base[3:5]), int64(binary.LittleEndian.Uint16(base[5:7]))
		if hsz < 7 || hsz > int64(lim.MaxHeader) || hsz > d.size-off {
			return fmt.Errorf("%w: bad header size", ErrMalformed)
		}
		hdr := base
		if int64(len(hdr)) < hsz {
			if hdr, err = d.at(off, int(hsz)); err != nil {
				return err
			}
		}
		hdr = hdr[:hsz]
		if checkCRC && uint16(crc32.ChecksumIEEE(hdr[2:])) != binary.LittleEndian.Uint16(hdr[:2]) {
			return fmt.Errorf("%w: header checksum", ErrMalformed)
		}
		blk := Block{HeaderOffset: off, DataOffset: off + hsz, UnpackedSize: -1}
		switch typ {
		case r4End:
			v.Done = true
			return nil
		case r4Main:
			if flags&r4MainPass != 0 {
				return ErrEncryptedHeaders
			}
		case r4File, r4Service:
			if err := parseRAR4File(hdr, flags, &blk, typ == r4File); err != nil {
				return err
			}
			v.Blocks = append(v.Blocks, blk)
		default:
			if flags&r4LongBlock != 0 {
				if hsz < 11 {
					return fmt.Errorf("%w: short block", ErrMalformed)
				}
				blk.DataSize = int64(binary.LittleEndian.Uint32(hdr[7:11]))
			}
		}
		next, ok := add(blk.DataOffset, blk.DataSize)
		if !ok {
			return fmt.Errorf("%w: data area overflows", ErrMalformed)
		}
		off = next
	}
}

// parseRAR4File decodes a file (0x74) or service (0x7A) header.
func parseRAR4File(hdr []byte, flags uint16, blk *Block, file bool) error {
	if len(hdr) < 32 {
		return fmt.Errorf("%w: short file header", ErrMalformed)
	}
	pack := uint64(binary.LittleEndian.Uint32(hdr[7:11]))
	unp := uint64(binary.LittleEndian.Uint32(hdr[11:15]))
	if flags&r4Large != 0 {
		if len(hdr) < 40 {
			return fmt.Errorf("%w: short large file header", ErrMalformed)
		}
		pack |= uint64(binary.LittleEndian.Uint32(hdr[32:36])) << 32
		unp |= uint64(binary.LittleEndian.Uint32(hdr[36:40])) << 32
	}
	if pack > 1<<62 {
		return fmt.Errorf("%w: packed size", ErrMalformed)
	}
	blk.DataSize = int64(pack)
	if unp <= 1<<62 {
		blk.UnpackedSize = int64(unp)
	}
	nameAt := 32
	if flags&r4Large != 0 {
		nameAt = 40
	}
	if n := int(binary.LittleEndian.Uint16(hdr[26:28])); n <= maxNameLen && nameAt+n <= len(hdr) {
		raw := hdr[nameAt : nameAt+n]
		if i := bytes.IndexByte(raw, 0); i >= 0 { // unicode names: the plain part comes first
			raw = raw[:i]
		}
		blk.Name = cleanName(raw)
	}
	blk.File = file
	blk.Dir = flags&r4DirMask == r4DirMask
	blk.Stored = hdr[25] == r4StoreBytes
	blk.Encrypted = flags&r4Password != 0
	blk.SplitBefore = flags&r4SplitPrev != 0
	blk.SplitAfter = flags&r4SplitNext != 0
	return nil
}

// cleanName turns a header name into the form compared with a layout: "/"
// separators, no control characters, bounded.
func cleanName(raw []byte) string {
	out := make([]byte, 0, len(raw))
	for _, c := range raw {
		switch {
		case c == '\\':
			out = append(out, '/')
		case c < 0x20 || c == 0x7f:
			return ""
		default:
			out = append(out, c)
		}
	}
	return string(out)
}
