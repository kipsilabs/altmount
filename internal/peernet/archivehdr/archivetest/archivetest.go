// Package archivetest builds small stored RAR 4, RAR 5 and 7z archives for
// tests. They are byte-exact enough for the header readers (checksums
// included) and carry the file data verbatim, so offsets can be asserted.
package archivetest

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
)

// File is one entry of a built archive.
type File struct {
	Name string
	Data []byte
	// Compressed marks the entry as compressed (the data is still written
	// verbatim; only the header says otherwise).
	Compressed bool
	// SplitBefore and SplitAfter set the continuation flags (RAR).
	SplitBefore, SplitAfter bool
	// UnpackedSize overrides the declared unpacked size (0: len(Data)).
	UnpackedSize int64
	// IV, when set, marks the entry encrypted with this IV (RAR 5) or just
	// encrypted (RAR 4).
	IV []byte
	// Service writes the entry as a service block (comment, recovery record).
	Service bool
}

func (f File) unpacked() int64 {
	if f.UnpackedSize != 0 {
		return f.UnpackedSize
	}
	return int64(len(f.Data))
}

// Built is an archive and where each entry's data starts.
type Built struct {
	Data        []byte
	DataOffsets []int64
}

func vint(v uint64) []byte {
	var out []byte
	for v >= 0x80 {
		out = append(out, byte(v)|0x80)
		v >>= 7
	}
	return append(out, byte(v))
}

// RAR5 builds a RAR 5 archive: signature, main header, one block per file, end
// of archive.
func RAR5(files ...File) Built {
	var buf bytes.Buffer
	buf.Write([]byte{'R', 'a', 'r', '!', 0x1A, 0x07, 0x01, 0x00})
	block := func(content []byte) {
		size := vint(uint64(len(content)))
		sum := crc32.ChecksumIEEE(append(append([]byte{}, size...), content...))
		var crc [4]byte
		binary.LittleEndian.PutUint32(crc[:], sum)
		buf.Write(crc[:])
		buf.Write(size)
		buf.Write(content)
	}
	block(append(vint(1), append(vint(0), vint(0)...)...)) // main: type 1, flags 0, archive flags 0
	var offsets []int64
	for _, f := range files {
		typ, flags := uint64(2), uint64(0x2)
		if f.Service {
			typ = 3
		}
		if f.SplitBefore {
			flags |= 0x8
		}
		if f.SplitAfter {
			flags |= 0x10
		}
		var extra []byte
		if f.IV != nil {
			rec := append(vint(1), vint(0)...)
			rec = append(rec, vint(0)...)
			rec = append(rec, 15)
			rec = append(rec, make([]byte, 16)...)
			rec = append(rec, f.IV...)
			extra = append(vint(uint64(len(rec))), rec...)
			flags |= 0x1
		}
		c := vint(typ)
		c = append(c, vint(flags)...)
		if extra != nil {
			c = append(c, vint(uint64(len(extra)))...)
		}
		c = append(c, vint(uint64(len(f.Data)))...)
		c = append(c, vint(0)...) // file flags
		c = append(c, vint(uint64(f.unpacked()))...)
		c = append(c, vint(0x20)...) // attributes
		comp := uint64(0)
		if f.Compressed {
			comp = 3 << 7
		}
		c = append(c, vint(comp)...)
		c = append(c, vint(0)...) // host OS
		c = append(c, vint(uint64(len(f.Name)))...)
		c = append(c, f.Name...)
		c = append(c, extra...)
		block(c)
		offsets = append(offsets, int64(buf.Len()))
		buf.Write(f.Data)
	}
	block(append(vint(5), append(vint(0), vint(0)...)...)) // end of archive
	return Built{Data: buf.Bytes(), DataOffsets: offsets}
}

// RAR4 builds a RAR 4 archive.
func RAR4(files ...File) Built {
	var buf bytes.Buffer
	buf.Write([]byte{'R', 'a', 'r', '!', 0x1A, 0x07, 0x00})
	header := func(kind byte, flags uint16, body []byte) {
		data := make([]byte, 7+len(body))
		data[2] = kind
		binary.LittleEndian.PutUint16(data[3:5], flags)
		binary.LittleEndian.PutUint16(data[5:7], uint16(len(data)))
		copy(data[7:], body)
		binary.LittleEndian.PutUint16(data[:2], uint16(crc32.ChecksumIEEE(data[2:])))
		buf.Write(data)
	}
	header(0x73, 0, make([]byte, 6))
	var offsets []int64
	for _, f := range files {
		body := make([]byte, 25)
		binary.LittleEndian.PutUint32(body[:4], uint32(len(f.Data)))
		binary.LittleEndian.PutUint32(body[4:8], uint32(f.unpacked()))
		body[8] = 3
		binary.LittleEndian.PutUint32(body[9:13], crc32.ChecksumIEEE(f.Data))
		body[17], body[18] = 20, 0x30
		if f.Compressed {
			body[18] = 0x33
		}
		binary.LittleEndian.PutUint16(body[19:21], uint16(len(f.Name)))
		binary.LittleEndian.PutUint32(body[21:25], 0o100644)
		flags := uint16(0x8000)
		if f.SplitBefore {
			flags |= 0x1
		}
		if f.SplitAfter {
			flags |= 0x2
		}
		if f.IV != nil {
			flags |= 0x4
		}
		kind := byte(0x74)
		if f.Service {
			kind = 0x7A
		}
		header(kind, flags, append(body, f.Name...))
		offsets = append(offsets, int64(buf.Len()))
		buf.Write(f.Data)
	}
	header(0x7B, 0, nil)
	return Built{Data: buf.Bytes(), DataOffsets: offsets}
}

func z7num(v uint64) []byte {
	if v < 0x80 {
		return []byte{byte(v)}
	}
	for n := 1; n < 8; n++ {
		if v < 1<<(7*uint(n+1)) {
			first := byte(0xFF<<(8-uint(n))) | byte(v>>(8*uint(n)))
			out := []byte{first}
			for i := 0; i < n; i++ {
				out = append(out, byte(v>>(8*uint(i))))
			}
			return out
		}
	}
	out := []byte{0xFF}
	for i := 0; i < 8; i++ {
		out = append(out, byte(v>>(8*uint(i))))
	}
	return out
}

// SevenZipOpts shape a built 7z archive.
type SevenZipOpts struct {
	// Solid puts all files in one folder with one stream each.
	Solid bool
	// EncodedHeader writes the end header as an encoded (0x17) header.
	EncodedHeader bool
}

// SevenZip builds a 7z archive of stored (copy) folders: the packed streams
// first, then the end header.
func SevenZip(o SevenZipOpts, files ...File) Built {
	var packed bytes.Buffer
	var offsets []int64
	for _, f := range files {
		offsets = append(offsets, 32+int64(packed.Len()))
		packed.Write(f.Data)
	}
	var h bytes.Buffer
	h.WriteByte(0x01) // Header
	if len(files) > 0 {
		h.WriteByte(0x04) // MainStreamsInfo
		h.WriteByte(0x06) // PackInfo
		h.Write(z7num(0))
		nPacks := len(files)
		if o.Solid {
			nPacks = 1
		}
		h.Write(z7num(uint64(nPacks)))
		h.WriteByte(0x09)
		if o.Solid {
			h.Write(z7num(uint64(packed.Len())))
		} else {
			for _, f := range files {
				h.Write(z7num(uint64(len(f.Data))))
			}
		}
		h.WriteByte(0x00)
		h.WriteByte(0x07) // UnpackInfo
		h.WriteByte(0x0B)
		nFolders := nPacks
		h.Write(z7num(uint64(nFolders)))
		h.WriteByte(0x00)
		for i := 0; i < nFolders; i++ {
			compressed := !o.Solid && files[i].Compressed
			if compressed {
				h.Write([]byte{0x01, 0x23, 0x03, 0x01, 0x01, 0x05, 0x5D, 0, 0, 0x10, 0}) // one LZMA coder with props
			} else {
				h.Write([]byte{0x01, 0x01, 0x00}) // one coder, id size 1, Copy
			}
		}
		h.WriteByte(0x0C)
		if o.Solid {
			h.Write(z7num(uint64(packed.Len())))
		} else {
			for _, f := range files {
				h.Write(z7num(uint64(len(f.Data))))
			}
		}
		h.WriteByte(0x00)
		if o.Solid {
			h.WriteByte(0x08) // SubStreamsInfo
			h.WriteByte(0x0D)
			h.Write(z7num(uint64(len(files))))
			h.WriteByte(0x09)
			for _, f := range files[:len(files)-1] {
				h.Write(z7num(uint64(len(f.Data))))
			}
			h.WriteByte(0x00)
		}
		h.WriteByte(0x00) // end of streams info
	}
	h.WriteByte(0x05) // FilesInfo
	h.Write(z7num(uint64(len(files))))
	h.WriteByte(0x00)
	h.WriteByte(0x00)
	end := h.Bytes()
	if o.EncodedHeader {
		end = append([]byte{0x17, 0x06, 0x00, 0x01, 0x09, 0x20, 0x00, 0x00}, make([]byte, 24)...)
	}
	start := make([]byte, 32)
	copy(start, []byte{'7', 'z', 0xBC, 0xAF, 0x27, 0x1C, 0, 4})
	binary.LittleEndian.PutUint64(start[12:20], uint64(packed.Len()))
	binary.LittleEndian.PutUint64(start[20:28], uint64(len(end)))
	binary.LittleEndian.PutUint32(start[28:32], crc32.ChecksumIEEE(end))
	binary.LittleEndian.PutUint32(start[8:12], crc32.ChecksumIEEE(start[12:32]))
	out := append(append(start, packed.Bytes()...), end...)
	return Built{Data: out, DataOffsets: offsets}
}
