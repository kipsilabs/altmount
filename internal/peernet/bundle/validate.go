package bundle

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Reader assumptions and the checks that uphold them.
//
// Everything a peer sends is hostile, so a bundle must never be able to break
// an assumption the readers of the on-disk metadata make. Line numbers refer
// to the readers as of this writing.
//
//	reader assumption                                         reader code                                   check here
//	--------------------------------------------------------  --------------------------------------------  -------------------------------
//	usable bytes of a segment are End-Start+1 and positive;   nzbfilesystem/metadata_remote_file.go:1176,    checkTable: 0 <= Start <= End
//	a segment with usable <= 0 is silently dropped, shifting  usenet/segment.go:112-114                     <= Size-1
//	every later offset
//	a gap or patch is zero-filled with End+1 bytes, so End    usenet/usenet_reader.go:947-949,1057-1076     checkTable: Size <= MaxSegmentSize
//	bounds an allocation                                                                                    (8 MiB), End < Size
//	a placeholder message-id prefix means "zero-fill, do not  usenet/usenet_reader.go:1057,                 CanonicalMessageID rejects the
//	fetch"; a shareable file has no gaps at all                importer/validation/fast_fail.go:282          prefix; Ref == GapRef is refused
//	a message-id is sent in NNTP commands; a space or CRLF    usenet/usenet_reader.go (BODY/STAT by id)     CanonicalMessageID: printable
//	would split or inject a command                                                                         ASCII, no brackets, <= 248 bytes
//	FileSize >= 1 (a zero size is an invalid range, never a   nzbfilesystem/metadata_remote_file.go:2128    validateFile: 1 <= Size <= cap
//	readable file)
//	unencrypted file: segment coverage equals FileSize        metadata_remote_file.go:2236-2246 (reads are  validateFile: coverage == Size
//	(coverage past EOF is never requested; a shortfall is a   bounded by coverage, EOF by FileSize),
//	truncated tail)                                           1140-1155 (truncatedTailError)
//	AES file: coverage holds the padded final block, i.e.     metadata_remote_file.go:2231-2234,            validateFile: padded(Size) <=
//	between the block-aligned size and FileSize+15            encryption/aes/aes_cipher.go:74,              coverage <= Size+15
//	                                                          aes_decrypt_reader.go:82-90
//	AES key is 16/24/32 bytes, IV 16 bytes, both present      encryption/aes/aes_decrypt_reader.go:42-48,   checkKeyIV
//	together                                                  metadata_remote_file.go:1751-1756
//	rclone and header encryption need local secrets the       metadata_remote_file.go:1721-1745             not representable; the builder
//	bundle never carries                                                                                    refuses such files (not shareable)
//	nested reads take priority over inline segments, so a     metadata_remote_file.go:1670-1676             validateFile: nested files have
//	nested file with both is ambiguous                                                                      no inline segments, no file key
//	extents are concatenated by InnerLength, so they must     metadata_remote_file.go:2321-2357             checkExtents: sum == Size, each
//	add up to FileSize; a zero or negative length is skipped                                                length >= 1
//	an extent reads outer bytes [InnerOffset, +InnerLength)   metadata_remote_file.go:2369-2393             checkExtents: inside the encrypted
//	of the (decrypted) outer volume                                                                         volume size, or inside the plain
//	                                                                                                        outer coverage
//	an encrypted outer volume is read as ciphertext from 0    metadata_remote_file.go:2371-2393,            checkOuter: padded(VolumeSize) <=
//	to padded(InnerVolumeSize)-1 and needs InnerVolumeSize    aes_decrypt_reader.go:82-90                   coverage <= VolumeSize+15
//	a shared outer source index must exist                    metadata/expand.go:29-37,                     Extent.Outer in range
//	                                                          metadata/store.go:280-300 (resolveRefs)
//	every shared outer source is used (else it is dead data)  metadata/service.go (v3 write dissolves them)  checkExtents: no unused outer
//	clip lengths are positive 192-byte multiples and cover    nzbfilesystem/tsremux_reader.go:21-38,        checkClips
//	the whole file: the remux aligns reads on the packet grid metadata_remote_file.go:1690-1710,
//	and clips past FileSize are never produced                importer/archive/iso_expansion.go:206-228
//	remux deltas are small enough to add to 90 kHz PTS/DTS    nzbfilesystem/tsremux.go                      checkClips: |delta| <= MaxClipDelta
//	without wrapping
//	metadata file names are single path components            metadata/service.go:196-228 (metaFilePath)    checkFilename (sanitiser fixed
//	                                                                                                        point), unique ignoring case
//	hole-fill and hedging are only enabled for plain files    nzbfilesystem/holes.go:35-37                  nothing to check: derived locally
//	arithmetic on sizes never overflows int64                 (all readers use int64 sums)                  every size <= 2^50, counts
//	                                                                                                        <= 500k * 8 MiB; sums checked
//
// The nesting depth is exactly one by construction: outer sources are plain
// segment tables, so a cycle or deeper nesting cannot be expressed.

// validator carries the running totals of one Validate call.
type validator struct {
	b       *Bundle
	segs    int
	extents int
	// owner maps a pool entry to the segment table that references it (-1 =
	// none). A pool entry belongs to exactly one table; two usages of one table
	// (a shared outer volume, a PAR2 file listed by several files) are the same
	// table only when every reference, size and range is identical.
	owner  []int32
	tables map[TableKey]int32
	names  map[string]struct{}
	// nzb scheme: the pool range of every NZB file entry, and which entries
	// are read inline (once, by one file) or as shared outer/PAR2 sources.
	entStart []int
	entLen   []int
	inline   map[int]struct{}
	sharedE  map[int]struct{}
	total    int64
	lastFile *File
}

// Validate checks every structural rule of the bundle schema: limits, ranges,
// sums, encryption sizes, names and the message-ID pool. It does not touch the
// network or recompute identifiers (Verify does that). It never panics and
// never allocates in proportion to a size the bundle merely claims.
func Validate(b *Bundle) error {
	if b == nil {
		return fmt.Errorf("%w: nil bundle", ErrInvalidField)
	}
	if b.Scheme != SchemeNZB && b.Scheme != SchemeFiles {
		return fmt.Errorf("%w: %d", ErrUnknownScheme, b.Scheme)
	}
	if b.ReleaseID.IsZero() {
		return fmt.Errorf("%w: zero release id", ErrInvalidField)
	}
	if err := checkTitle(b.Title); err != nil {
		return err
	}
	if b.PostedUnix < 1 || b.PostedUnix > maxUnixSeconds {
		return fmt.Errorf("%w: post date out of range", ErrInvalidField)
	}
	if err := checkPassword(b.NZBPassword); err != nil {
		return err
	}
	v := &validator{b: b, names: map[string]struct{}{}, tables: map[TableKey]int32{}}
	if err := v.checkPool(); err != nil {
		return err
	}
	switch {
	case len(b.Files) == 0:
		return fmt.Errorf("%w: no files", ErrInvalidField)
	case len(b.Files) > MaxFiles:
		return fmt.Errorf("%w: %d files", ErrLimit, len(b.Files))
	}
	for i := range b.Files {
		if err := v.checkFile(&b.Files[i]); err != nil {
			return fmt.Errorf("file %d: %w", i, err)
		}
	}
	if b.Scheme == SchemeFiles {
		for i, o := range v.owner {
			if o < 0 {
				return fmt.Errorf("%w: pool entry %d is not referenced", ErrBadPool, i)
			}
		}
	}
	if v.total != b.TotalSize {
		return fmt.Errorf("%w: total size %d, files add up to %d", ErrSizeMismatch, b.TotalSize, v.total)
	}
	return nil
}

func checkPassword(p string) error {
	if len(p) > MaxPasswordLen || !utf8.ValidString(p) {
		return fmt.Errorf("%w: password", ErrInvalidField)
	}
	for _, r := range p {
		if unicode.IsControl(r) {
			return fmt.Errorf("%w: password contains a control character", ErrInvalidField)
		}
	}
	return nil
}

func (v *validator) checkPool() error {
	b := v.b
	if len(b.Pool) > MaxSegments {
		return fmt.Errorf("%w: %d pool entries", ErrLimit, len(b.Pool))
	}
	for i, id := range b.Pool {
		if c, err := CanonicalMessageID(id); err != nil || c != id {
			return fmt.Errorf("%w: pool entry %d", ErrBadMessageID, i)
		}
	}
	if b.Scheme == SchemeNZB {
		if len(b.ReleaseFiles) > MaxReleaseFiles {
			return fmt.Errorf("%w: %d release files", ErrLimit, len(b.ReleaseFiles))
		}
		sum := 0
		for _, n := range b.ReleaseFiles {
			if n < 0 || n > MaxSegments {
				return fmt.Errorf("%w: release file segment count", ErrBadPool)
			}
			sum += n
		}
		if len(b.ReleaseFiles) == 0 || sum != len(b.Pool) {
			return fmt.Errorf("%w: release list covers %d of %d pool entries", ErrBadPool, sum, len(b.Pool))
		}
		seen := make(map[string]struct{}, len(b.Pool))
		for _, id := range b.Pool {
			if _, dup := seen[id]; dup {
				return fmt.Errorf("%w: duplicate message-id in the release list", ErrBadPool)
			}
			seen[id] = struct{}{}
		}
		pos := 0
		v.entStart, v.entLen = make([]int, len(b.ReleaseFiles)), b.ReleaseFiles
		for i, n := range b.ReleaseFiles {
			v.entStart[i] = pos
			pos += n
		}
		v.inline, v.sharedE = map[int]struct{}{}, map[int]struct{}{}
		v.initOwner()
		return nil
	}
	if len(b.ReleaseFiles) != 0 {
		return fmt.Errorf("%w: release list on a files-scheme bundle", ErrInvalidField)
	}
	seen := make(map[string]struct{}, len(b.Pool))
	for _, id := range b.Pool {
		if _, dup := seen[id]; dup {
			return fmt.Errorf("%w: duplicate message-id", ErrBadPool)
		}
		seen[id] = struct{}{}
	}
	v.initOwner()
	return nil
}

func (v *validator) initOwner() {
	v.owner = make([]int32, len(v.b.Pool))
	for i := range v.owner {
		v.owner[i] = -1
	}
}

func validDate(d int64) bool { return d == 0 || (d >= 1 && d <= maxUnixSeconds) }

func (v *validator) checkFile(f *File) error {
	if err := checkFilename(f.Name); err != nil {
		return err
	}
	key := strings.ToLower(f.Name)
	if _, dup := v.names[key]; dup {
		return fmt.Errorf("%w: %q", ErrDuplicateName, f.Name)
	}
	v.names[key] = struct{}{}

	if err := v.checkFileOrder(f); err != nil {
		return err
	}
	if len(f.Outer) > MaxOuterSources {
		return fmt.Errorf("%w: %d outer sources", ErrLimit, len(f.Outer))
	}
	if v.extents += len(f.Extents); v.extents > MaxExtents {
		return fmt.Errorf("%w: more than %d extents", ErrLimit, MaxExtents)
	}
	if len(f.Clips) > MaxClips {
		return fmt.Errorf("%w: %d clips", ErrLimit, len(f.Clips))
	}
	if len(f.Par2) > MaxPar2Files {
		return fmt.Errorf("%w: %d par2 files", ErrLimit, len(f.Par2))
	}
	if f.Size < 1 || f.Size > MaxFileSize {
		return fmt.Errorf("%w: file size %d", ErrInvalidField, f.Size)
	}
	if !validDate(f.ReleaseDate) {
		return fmt.Errorf("%w: release date", ErrInvalidField)
	}
	if err := checkKeyIV(f.AESKey, f.AESIV); err != nil {
		return err
	}

	nested := len(f.Extents) > 0 || len(f.Outer) > 0
	switch {
	case nested && len(f.Segments) > 0:
		return fmt.Errorf("%w: nested file lists inline segments", ErrInvalidField)
	case nested && len(f.AESKey) > 0:
		return fmt.Errorf("%w: nested file has a file-level key", ErrBadEncryption)
	case nested:
		if err := v.checkNested(f); err != nil {
			return err
		}
	default:
		if err := v.checkInline(f); err != nil {
			return err
		}
	}
	if len(f.Clips) > 0 {
		if err := checkClips(f); err != nil {
			return err
		}
	}
	if err := v.checkPar2(f); err != nil {
		return err
	}
	if err := v.checkCoverage(f); err != nil {
		return err
	}
	if v.total > MaxFileSize-f.Size {
		return fmt.Errorf("%w: bundle total above the cap", ErrInvalidField)
	}
	v.total += f.Size
	return nil
}

// checkFileOrder requires strictly ascending, non-zero File IDs.
func (v *validator) checkFileOrder(f *File) error {
	switch {
	case f.FileID.IsZero():
		return fmt.Errorf("%w: zero file id", ErrInvalidField)
	case v.lastFile != nil && v.lastFile.FileID == f.FileID:
		return fmt.Errorf("%w: %s", ErrDuplicateFile, f.FileID)
	case v.lastFile != nil && string(v.lastFile.FileID[:]) > string(f.FileID[:]):
		return fmt.Errorf("%w: files are not sorted by file id", ErrInvalidField)
	}
	v.lastFile = f
	return nil
}

func checkKeyIV(key, iv []byte) error {
	if len(key) == 0 && len(iv) == 0 {
		return nil
	}
	if len(key) != 16 && len(key) != 24 && len(key) != 32 {
		return fmt.Errorf("%w: key length %d", ErrBadEncryption, len(key))
	}
	if len(iv) != aesBlockSize {
		return fmt.Errorf("%w: iv length %d", ErrBadEncryption, len(iv))
	}
	return nil
}

// padded is the AES-CBC encrypted size of n plaintext bytes.
func padded(n int64) int64 {
	if r := n % aesBlockSize; r != 0 {
		return n + aesBlockSize - r
	}
	return n
}

// checkInline validates a file read straight from its own segments.
func (v *validator) checkInline(f *File) error {
	if len(f.Segments) == 0 {
		return fmt.Errorf("%w: file has no data", ErrInvalidField)
	}
	covered, err := v.checkTable(f.Segments)
	if err != nil {
		return err
	}
	if f.Size > int64(len(f.Segments))*MaxSegmentSize {
		return fmt.Errorf("%w: %d bytes in %d segments", ErrImplausible, f.Size, len(f.Segments))
	}
	if len(f.AESKey) > 0 {
		if covered < padded(f.Size) || covered > f.Size+aesBlockSize-1 {
			return fmt.Errorf("%w: encrypted file of %d bytes covered by %d", ErrBadEncryption, f.Size, covered)
		}
		return nil
	}
	if covered != f.Size {
		return fmt.Errorf("%w: size %d, segments cover %d", ErrSizeMismatch, f.Size, covered)
	}
	return nil
}

// TableKey identifies a segment table by its whole content: every reference,
// size and usable range, in order.
type TableKey [32]byte

// KeyOf hashes a segment table (references, sizes and ranges, in order).
func KeyOf(segs []Segment) TableKey {
	h := sha256.New()
	var buf [32]byte
	for _, s := range segs {
		binary.BigEndian.PutUint64(buf[0:], uint64(s.Ref))
		binary.BigEndian.PutUint64(buf[8:], uint64(s.Size))
		binary.BigEndian.PutUint64(buf[16:], uint64(s.Start))
		binary.BigEndian.PutUint64(buf[24:], uint64(s.End))
		h.Write(buf[:])
	}
	var k TableKey
	h.Sum(k[:0])
	return k
}

// checkTable validates one segment table and returns the bytes it covers.
//
// A table reads one posted file front to back. Only its first segment may
// start inside the article and only its last may end inside it (a single
// segment may do both); every other segment is used whole, so no usable range
// can hide bytes in the middle. A pool entry is referenced at most once within
// a table, and by one table only unless the other usage is identical.
func (v *validator) checkTable(segs []Segment) (int64, error) {
	if v.segs += len(segs); v.segs > MaxSegments {
		return 0, fmt.Errorf("%w: more than %d segments", ErrLimit, MaxSegments)
	}
	poolLen := int64(len(v.b.Pool))
	ceiling := int64(MaxSegmentSize)
	if v.b.Scheme == SchemeFiles {
		ceiling = MaxFilesSchemeSegment
	}
	seen := make(map[int64]struct{}, len(segs))
	var covered int64
	for i, s := range segs {
		switch {
		case s.Ref == GapRef:
			// A shareable file has no known holes: a gap is never accepted.
			return 0, fmt.Errorf("%w: segment %d is a gap", ErrInvalidField, i)
		case s.Ref < 0 || s.Ref >= poolLen:
			return 0, fmt.Errorf("%w: segment %d references pool entry %d of %d", ErrOutOfRange, i, s.Ref, poolLen)
		case s.Size > ceiling:
			return 0, fmt.Errorf("%w: segment %d is %d bytes", ErrImplausible, i, s.Size)
		case s.Size < 1:
			return 0, fmt.Errorf("%w: segment %d size %d", ErrOutOfRange, i, s.Size)
		case s.Start < 0 || s.End < s.Start || s.End > s.Size-1:
			return 0, fmt.Errorf("%w: segment %d range [%d,%d] of %d", ErrOutOfRange, i, s.Start, s.End, s.Size)
		case i > 0 && s.Start != 0, i < len(segs)-1 && s.End != s.Size-1:
			return 0, fmt.Errorf("%w: segment %d range [%d,%d] of %d", ErrPartialRange, i, s.Start, s.End, s.Size)
		}
		if _, dup := seen[s.Ref]; dup {
			return 0, fmt.Errorf("%w: segment %d repeats pool entry %d within one table", ErrCoverage, i, s.Ref)
		}
		seen[s.Ref] = struct{}{}
		covered += s.End - s.Start + 1
	}
	key := KeyOf(segs)
	if _, same := v.tables[key]; same {
		return covered, nil
	}
	id := int32(len(v.tables))
	v.tables[key] = id
	for i, s := range segs {
		if v.owner[s.Ref] >= 0 {
			return 0, fmt.Errorf("%w: segment %d shares pool entry %d with a different table", ErrCoverage, i, s.Ref)
		}
		v.owner[s.Ref] = id
	}
	return covered, nil
}

func (v *validator) checkNested(f *File) error {
	if len(f.Extents) == 0 {
		return fmt.Errorf("%w: outer sources without extents", ErrInvalidField)
	}
	limits := make([]int64, len(f.Outer))
	for i := range f.Outer {
		lim, err := v.checkOuter(&f.Outer[i])
		if err != nil {
			return fmt.Errorf("outer source %d: %w", i, err)
		}
		limits[i] = lim
	}
	return checkExtents(f, limits)
}

// checkOuter validates one outer volume and returns the number of bytes an
// extent may address in it.
func (v *validator) checkOuter(o *OuterSource) (int64, error) {
	covered, err := v.checkTable(o.Segments)
	if err != nil {
		return 0, err
	}
	if len(o.Segments) == 0 {
		return 0, fmt.Errorf("%w: outer source without segments", ErrInvalidField)
	}
	if err := checkKeyIV(o.AESKey, o.AESIV); err != nil {
		return 0, err
	}
	if len(o.AESKey) == 0 {
		if o.VolumeSize != 0 {
			return 0, fmt.Errorf("%w: volume size on an unencrypted outer source", ErrBadEncryption)
		}
		return covered, nil
	}
	if o.VolumeSize < 1 || o.VolumeSize > MaxFileSize {
		return 0, fmt.Errorf("%w: volume size %d", ErrBadEncryption, o.VolumeSize)
	}
	if covered < padded(o.VolumeSize) || covered > o.VolumeSize+aesBlockSize-1 {
		return 0, fmt.Errorf("%w: encrypted volume of %d bytes covered by %d", ErrBadEncryption, o.VolumeSize, covered)
	}
	return o.VolumeSize, nil
}

func checkExtents(f *File, limits []int64) error {
	used := make([]bool, len(f.Outer))
	var sum int64
	for i, e := range f.Extents {
		switch {
		case e.Outer < 0 || e.Outer >= len(f.Outer):
			return fmt.Errorf("%w: extent %d outer index %d", ErrOutOfRange, i, e.Outer)
		case e.InnerLength < 1 || e.InnerLength > MaxFileSize,
			e.InnerOffset < 0 || e.InnerOffset > MaxFileSize:
			return fmt.Errorf("%w: extent %d [%d,+%d)", ErrOutOfRange, i, e.InnerOffset, e.InnerLength)
		case e.InnerOffset+e.InnerLength > limits[e.Outer]:
			return fmt.Errorf("%w: extent %d ends at %d, outer source holds %d", ErrOutOfRange, i, e.InnerOffset+e.InnerLength, limits[e.Outer])
		case sum > MaxFileSize-e.InnerLength:
			return fmt.Errorf("%w: extents add up past the file cap", ErrSizeMismatch)
		}
		used[e.Outer] = true
		sum += e.InnerLength
	}
	for i, u := range used {
		if !u {
			return fmt.Errorf("%w: outer source %d is not used by any extent", ErrInvalidField, i)
		}
	}
	if err := checkOverlap(f.Extents); err != nil {
		return err
	}
	if sum != f.Size {
		return fmt.Errorf("%w: size %d, extents add up to %d", ErrSizeMismatch, f.Size, sum)
	}
	return nil
}

// checkOverlap rejects two extents of one outer source that share bytes.
func checkOverlap(extents []Extent) error {
	order := make([]int, len(extents))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool {
		x, y := extents[order[a]], extents[order[b]]
		if x.Outer != y.Outer {
			return x.Outer < y.Outer
		}
		return x.InnerOffset < y.InnerOffset
	})
	for k := 1; k < len(order); k++ {
		p, c := extents[order[k-1]], extents[order[k]]
		if p.Outer == c.Outer && p.InnerOffset+p.InnerLength > c.InnerOffset {
			return fmt.Errorf("%w: extents %d and %d", ErrOverlap, order[k-1], order[k])
		}
	}
	return nil
}

func checkClips(f *File) error {
	var sum int64
	for i, c := range f.Clips {
		switch {
		case c.ByteLen < 1 || c.ByteLen > f.Size || c.ByteLen%ClipPacketSize != 0:
			return fmt.Errorf("%w: clip %d length %d", ErrBadClip, i, c.ByteLen)
		case c.Delta90k < -MaxClipDelta || c.Delta90k > MaxClipDelta:
			return fmt.Errorf("%w: clip %d delta", ErrBadClip, i)
		}
		sum += c.ByteLen // each is <= Size <= 2^50 and there are <= 100k
	}
	if sum != f.Size {
		return fmt.Errorf("%w: clips add up to %d of %d", ErrBadClip, sum, f.Size)
	}
	return nil
}

func (v *validator) checkPar2(f *File) error {
	names := make(map[string]struct{}, len(f.Par2))
	for i := range f.Par2 {
		p := &f.Par2[i]
		if err := checkFilename(p.Name); err != nil {
			return fmt.Errorf("par2 %d: %w", i, err)
		}
		key := strings.ToLower(p.Name)
		if _, dup := names[key]; dup {
			return fmt.Errorf("%w: par2 %q", ErrDuplicateName, p.Name)
		}
		names[key] = struct{}{}
		if p.Size < 1 || p.Size > MaxFileSize || len(p.Segments) == 0 {
			return fmt.Errorf("%w: par2 %d size or segments", ErrInvalidField, i)
		}
		covered, err := v.checkTable(p.Segments)
		if err != nil {
			return fmt.Errorf("par2 %d: %w", i, err)
		}
		if p.Size > int64(len(p.Segments))*MaxSegmentSize {
			return fmt.Errorf("%w: par2 %d", ErrImplausible, i)
		}
		if covered != p.Size {
			return fmt.Errorf("%w: par2 %d size %d, segments cover %d", ErrSizeMismatch, i, p.Size, covered)
		}
	}
	return nil
}

// entryOf returns the NZB file entry that holds pool position pos.
func (v *validator) entryOf(pos int64) int {
	i := sort.Search(len(v.entStart), func(k int) bool { return int64(v.entStart[k]+v.entLen[k]) > pos })
	for i < len(v.entStart) && v.entLen[i] == 0 {
		i++
	}
	return i
}

// wholeEntry returns the NZB file entry a segment table reads, requiring the
// table to be exactly that entry's segments in NZB order.
func (v *validator) wholeEntry(segs []Segment) (int, error) {
	if len(segs) == 0 {
		return 0, fmt.Errorf("%w: empty table", ErrCoverage)
	}
	e := v.entryOf(segs[0].Ref)
	if e >= len(v.entStart) || segs[0].Ref != int64(v.entStart[e]) || len(segs) != v.entLen[e] {
		return 0, fmt.Errorf("%w: table does not start at, or does not span, one NZB file entry", ErrCoverage)
	}
	for i, s := range segs {
		if s.Ref != int64(v.entStart[e]+i) {
			return 0, fmt.Errorf("%w: segment %d is out of order or from another entry", ErrCoverage, i)
		}
	}
	return e, nil
}

// checkCoverage (nzb scheme) requires every segment table to be exactly one
// NZB file entry. An entry read inline belongs to one file and is read once;
// outer volumes and PAR2 files may be shared by several files, but only once
// within a file and never also inline.
func (v *validator) checkCoverage(f *File) error {
	if v.b.Scheme != SchemeNZB {
		return nil
	}
	own := map[int]struct{}{}
	use := func(segs []Segment, inline bool) error {
		e, err := v.wholeEntry(segs)
		if err != nil {
			return err
		}
		if _, dup := own[e]; dup {
			return fmt.Errorf("%w: NZB entry %d is read twice by one file", ErrCoverage, e)
		}
		own[e] = struct{}{}
		_, wasInline := v.inline[e]
		if wasInline || (inline && v.shared(e)) {
			return fmt.Errorf("%w: NZB entry %d is used inline and elsewhere", ErrCoverage, e)
		}
		if inline {
			v.inline[e] = struct{}{}
		} else {
			v.markShared(e)
		}
		return nil
	}
	if len(f.Segments) > 0 {
		if err := use(f.Segments, true); err != nil {
			return err
		}
	}
	for i := range f.Outer {
		if err := use(f.Outer[i].Segments, false); err != nil {
			return fmt.Errorf("outer source %d: %w", i, err)
		}
	}
	for i := range f.Par2 {
		if err := use(f.Par2[i].Segments, false); err != nil {
			return fmt.Errorf("par2 %d: %w", i, err)
		}
	}
	return nil
}

func (v *validator) shared(e int) bool { _, ok := v.sharedE[e]; return ok }

func (v *validator) markShared(e int) { v.sharedE[e] = struct{}{} }
