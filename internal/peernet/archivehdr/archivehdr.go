// Package archivehdr reads the clear headers of an archive volume (RAR 4,
// RAR 5, 7z) far enough to say where the data area of each stored file starts,
// how long it is and whether it continues across volumes. It exists to bind a
// bundle's archive extents (InnerOffset, InnerLength) to the archive's own file
// headers.
//
// Every byte it reads is hostile: the archive was posted by anyone, and a
// bundle only claims to describe it. The parsers therefore
//
//   - read through an io.ReaderAt with a hard cap on the number of reads,
//   - never allocate from a declared size (a header is read into a buffer of at
//     most Limits.MaxHeader bytes, a table of declared length is only decoded
//     when the bytes it needs are inside the header already read),
//   - use checked arithmetic for every offset and length,
//   - walk a fixed number of blocks, iteratively (no recursion), and
//   - never trust a CRC they have not verified.
//
// RAR volumes are walked from their start, hopping over data areas by their
// declared size. 7z keeps its headers at the end, so only a plain (not
// compressed, not encrypted) end header can be read; an encoded one is reported
// as ErrCompressedHeader and the caller treats the extent as unverifiable.
package archivehdr

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"path"
	"strings"
)

// Format is the archive format of a volume.
type Format uint8

// Formats.
const (
	FormatUnknown Format = iota
	FormatRAR4
	FormatRAR5
	Format7z
)

func (f Format) String() string {
	switch f {
	case FormatRAR4:
		return "rar4"
	case FormatRAR5:
		return "rar5"
	case Format7z:
		return "7z"
	}
	return "unknown"
}

// Errors that say a volume could not be judged (as opposed to Mismatch, which
// is a positive contradiction).
var (
	// ErrUnknownFormat: the volume does not start with a known archive signature.
	ErrUnknownFormat = errors.New("archivehdr: not a known archive")
	// ErrEncryptedHeaders: the headers are encrypted, so they cannot be read.
	ErrEncryptedHeaders = errors.New("archivehdr: encrypted headers")
	// ErrCompressedHeader: a 7z end header is encoded (compressed or encrypted).
	ErrCompressedHeader = errors.New("archivehdr: encoded 7z header")
	// ErrBudget: the read or block budget ran out before the answer was found.
	ErrBudget = errors.New("archivehdr: header budget exhausted")
	// ErrMalformed: the header chain is damaged or contradicts itself.
	ErrMalformed = errors.New("archivehdr: malformed headers")
)

// Limits bound one scan.
type Limits struct {
	// MaxBlocks is how many header blocks are walked.
	MaxBlocks int
	// MaxReads is how many reads are made.
	MaxReads int
	// MaxHeader is the largest header (RAR) that is read.
	MaxHeader int
	// Max7zHeader is the largest 7z end header that is read.
	Max7zHeader int
}

// DefaultLimits are the limits used by the importer: 64 blocks, 160 reads, and
// 64 KiB per header.
func DefaultLimits() Limits {
	return Limits{MaxBlocks: 64, MaxReads: 160, MaxHeader: 64 << 10, Max7zHeader: 64 << 10}
}

func (l Limits) norm() Limits {
	d := DefaultLimits()
	if l.MaxBlocks <= 0 {
		l.MaxBlocks = d.MaxBlocks
	}
	if l.MaxReads <= 0 {
		l.MaxReads = d.MaxReads
	}
	if l.MaxHeader <= 0 {
		l.MaxHeader = d.MaxHeader
	}
	if l.Max7zHeader <= 0 {
		l.Max7zHeader = d.Max7zHeader
	}
	return l
}

// Block is a header that is followed by a data area.
type Block struct {
	// HeaderOffset is where the header starts, DataOffset where its data area
	// starts and DataSize how long that area is in this volume.
	HeaderOffset, DataOffset, DataSize int64
	// UnpackedSize is the size of the whole file once unpacked (-1: unknown).
	UnpackedSize int64
	// File is a file entry (not a service block such as a comment or recovery
	// record); Dir marks a directory.
	File, Dir bool
	// Stored says the data is not compressed.
	Stored bool
	// Name is the entry's name as the header gives it ("" when the header does
	// not carry one this package can read), with path separators as "/", at most
	// maxNameLen bytes.
	Name string
	// Encrypted says the data area is AES encrypted; IV is the per-file IV the
	// header carries (RAR 5 only).
	Encrypted bool
	IV        []byte
	// SplitBefore and SplitAfter say the data continues from the previous and
	// into the next volume (RAR only).
	SplitBefore, SplitAfter bool
}

// Volume is what a scan found.
type Volume struct {
	Format Format
	// Size is the number of bytes of the volume.
	Size int64
	// VolumeNumber is the 0-based volume number a RAR 5 main header declares
	// (the first volume of a set carries none and is 0); -1 when the format
	// does not say (RAR 4, 7z, or a RAR 5 archive that is not a volume).
	VolumeNumber int64
	// Blocks are the data blocks found in order, up to the offset asked for.
	Blocks []Block
	// Reads is the number of reads made.
	Reads int
	// EndHeaderEnd is, for a 7z archive whose plain end header was read, the
	// offset just past that header: a complete archive ends there, so a caller
	// that knows the archive's real length can tell a dropped tail.
	EndHeaderEnd int64
	// Done says the walk reached the end of the archive or got past the offset
	// asked for without error: a block that is not in Blocks does not exist.
	Done bool
}

// reader counts and bounds the reads of one scan.
type reader struct {
	r        io.ReaderAt
	size     int64
	reads    int
	maxReads int
}

// at reads up to n bytes at off (fewer at the end of the volume).
func (d *reader) at(off int64, n int) ([]byte, error) {
	if off < 0 || off > d.size || n < 0 {
		return nil, fmt.Errorf("%w: read outside the volume", ErrMalformed)
	}
	n = int(min(int64(n), d.size-off))
	if n == 0 {
		return nil, fmt.Errorf("%w: read at the end of the volume", ErrMalformed)
	}
	if d.reads++; d.reads > d.maxReads {
		return nil, ErrBudget
	}
	buf := make([]byte, n)
	got, err := d.r.ReadAt(buf, off)
	if got == n {
		return buf, nil
	}
	if err == nil || errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%w: short read at %d", ErrMalformed, off)
	}
	return nil, &ReadError{Err: err}
}

// ReadError wraps an error of the underlying reader (a missing article, a
// timeout): it says nothing about the archive.
type ReadError struct{ Err error }

func (e *ReadError) Error() string { return "archivehdr: read: " + e.Err.Error() }

// Unwrap returns the reader's error.
func (e *ReadError) Unwrap() error { return e.Err }

var (
	rar5Sig = []byte{'R', 'a', 'r', '!', 0x1A, 0x07, 0x01, 0x00}
	rar4Sig = []byte{'R', 'a', 'r', '!', 0x1A, 0x07, 0x00}
	sevenSg = []byte{'7', 'z', 0xBC, 0xAF, 0x27, 0x1C}
)

// checkCRC is true outside tests: the fuzz target that explores the parsers
// below the checksums turns it off (a mutated header would otherwise always
// stop at its checksum).
var checkCRC = true

// Scan identifies the volume and returns its data blocks. upTo is the largest
// data offset the caller is interested in: the walk stops at the first block
// whose header starts beyond it (RAR) and returns everything found so far.
func Scan(r io.ReaderAt, size int64, upTo int64, lim Limits) (*Volume, error) {
	lim = lim.norm()
	if size <= 0 {
		return nil, ErrUnknownFormat
	}
	d := &reader{r: r, size: size, maxReads: lim.MaxReads}
	head, err := d.at(0, 8)
	if err != nil {
		if errors.Is(err, ErrMalformed) {
			return nil, ErrUnknownFormat
		}
		return nil, err
	}
	v := &Volume{Size: size, VolumeNumber: -1}
	switch {
	case len(head) >= 8 && string(head[:8]) == string(rar5Sig):
		v.Format = FormatRAR5
		err = scanRAR5(d, v, upTo, lim)
	case len(head) >= 7 && string(head[:7]) == string(rar4Sig):
		v.Format = FormatRAR4
		err = scanRAR4(d, v, upTo, lim)
	case len(head) >= 6 && string(head[:6]) == string(sevenSg):
		v.Format = Format7z
		err = scan7z(d, v, lim)
	default:
		return nil, ErrUnknownFormat
	}
	v.Reads = d.reads
	return v, err
}

// add returns a+b, or false when it overflows or is negative.
func add(a, b int64) (int64, bool) {
	if a < 0 || b < 0 || a > math.MaxInt64-b {
		return 0, false
	}
	return a + b, true
}

// Outcome is the verdict of Check.
type Outcome uint8

// Outcomes.
const (
	// Bound: a stored file's data area starts exactly at the extent, has the
	// extent's length here, and is positioned in the file as the extent is.
	Bound Outcome = iota
	// Unverifiable: the volume could not be judged (unknown format, encoded
	// header, budget, damaged chain before the extent).
	Unverifiable
	// Mismatch: the headers positively contradict the extent.
	Mismatch
)

// Result is the verdict for one extent with the reason.
type Result struct {
	Outcome Outcome
	Detail  string
}

// Extent is what a bundle claims about one slice of a volume.
type Extent struct {
	// Offset and Length are the claimed data area in the volume.
	Offset, Length int64
	// Index and Count place the extent in its file (0-based, of Count extents).
	Index, Count int
	// FileSize is the size of the whole file the extents make up.
	FileSize int64
	// Keyed says the layout carries an AES key for the file: its entry must be
	// encrypted, and when both IVs are known they must be equal.
	Keyed bool
	IV    []byte
	// Name is the base name the layout gives the file ("" skips the name check).
	Name string
}

// Check judges an extent against a scan of its volume. A scan that stopped on
// an error is passed as scanErr: the extent is then unverifiable unless the
// blocks found before the error already contradict it.
func Check(v *Volume, scanErr error, e Extent) Result {
	if v == nil {
		return Result{Unverifiable, errText(scanErr)}
	}
	var hit *Block
	for k := range v.Blocks {
		b := &v.Blocks[k]
		if b.DataOffset == e.Offset {
			hit = b
			break
		}
		if b.DataOffset < e.Offset && e.Offset < b.DataOffset+b.DataSize {
			return Result{Mismatch, fmt.Sprintf("offset %d lies inside the data area of another entry (%d..%d)", e.Offset, b.DataOffset, b.DataOffset+b.DataSize)}
		}
	}
	if hit == nil {
		// The walk found no entry starting here. It proves a contradiction only
		// when it got past the offset without error.
		if scanErr == nil && v.Done {
			return Result{Mismatch, fmt.Sprintf("no stored file data starts at offset %d", e.Offset)}
		}
		return Result{Unverifiable, errText(scanErr)}
	}
	return checkBlock(v, hit, e)
}

func errText(err error) string {
	if err == nil {
		return "the walk ended before the extent"
	}
	return err.Error()
}

func checkBlock(v *Volume, b *Block, e Extent) Result {
	bad := func(format string, a ...any) Result { return Result{Mismatch, fmt.Sprintf(format, a...)} }
	switch {
	case !b.File || b.Dir:
		return bad("the data area at %d belongs to a service block or a directory, not a file", e.Offset)
	case !b.Stored:
		return bad("the file at %d is compressed, not stored", e.Offset)
	case b.Encrypted && !e.Keyed:
		return bad("the file at %d is itself encrypted but the layout carries no key for it", e.Offset)
	case e.Keyed && !b.Encrypted:
		return bad("the layout carries a key for the file at %d but the entry is not encrypted", e.Offset)
	case e.Keyed && b.IV != nil && e.IV != nil && !bytes.Equal(b.IV, e.IV):
		return bad("the IV of the entry at %d differs from the layout's", e.Offset)
	case b.DataSize != e.Length:
		return bad("the header says %d bytes in this volume, the layout %d", b.DataSize, e.Length)
	case b.UnpackedSize >= 0 && b.UnpackedSize != e.FileSize:
		return bad("the header says the file is %d bytes, the layout %d", b.UnpackedSize, e.FileSize)
	}
	if r, bad2 := checkName(v, b, e.Name); bad2 {
		return r
	}
	end, ok := add(b.DataOffset, b.DataSize)
	if !ok || end > v.Size {
		return bad("the data area ends beyond the volume (%d of %d bytes)", end, v.Size)
	}
	if v.Format == Format7z {
		if e.Count != 1 {
			return Result{Unverifiable, "a 7z file is not split across extents"}
		}
		return Result{Bound, ""}
	}
	if first := e.Index == 0; first == b.SplitBefore {
		return bad("extent %d of %d: continuation flag before = %v", e.Index+1, e.Count, b.SplitBefore)
	}
	if last := e.Index == e.Count-1; last == b.SplitAfter {
		return bad("extent %d of %d: continuation flag after = %v", e.Index+1, e.Count, b.SplitAfter)
	}
	return Result{Bound, ""}
}

// Verify scans a volume and checks one extent against it. It is the function
// the importer calls per extent and the fuzz target exercises.
func Verify(r io.ReaderAt, size int64, e Extent, lim Limits) Result {
	if e.Offset < 0 || e.Length < 1 || e.Count < 1 || e.Index < 0 || e.Index >= e.Count {
		return Result{Mismatch, "invalid extent"}
	}
	v, err := Scan(r, size, e.Offset, lim)
	if v == nil {
		return Result{Unverifiable, errText(err)}
	}
	return Check(v, err, e)
}

// maxNameLen bounds the entry names that are read.
const maxNameLen = 1024

// entryBase is the last component of an entry name.
func entryBase(name string) string { return path.Base(strings.ReplaceAll(name, `\`, "/")) }

// checkName binds the extent to the entry that carries its file's name. The
// library may present a file under another name than the archive entry (a
// deobfuscated payload keeps its extension), so a different name is accepted
// only with the same extension, and only when no other file entry of the volume
// has exactly the layout's name. The second return is true when the check
// failed.
func checkName(v *Volume, b *Block, name string) (Result, bool) {
	if name == "" || b.Name == "" {
		return Result{}, false
	}
	have := entryBase(b.Name)
	if strings.EqualFold(have, name) {
		return Result{}, false
	}
	if !strings.EqualFold(path.Ext(have), path.Ext(name)) {
		return Result{Mismatch, fmt.Sprintf("the entry at this offset is named %q, the layout's file %q (not the same kind of file)", have, name)}, true
	}
	for k := range v.Blocks {
		o := &v.Blocks[k]
		if o != b && o.File && !o.Dir && o.Name != "" && strings.EqualFold(entryBase(o.Name), name) {
			return Result{Mismatch, fmt.Sprintf("the entry at this offset is named %q but another entry of the volume is named %q like the layout's file", have, name)}, true
		}
	}
	return Result{}, false
}
