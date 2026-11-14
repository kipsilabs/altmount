package bundle

import (
	"fmt"
	"math"

	"google.golang.org/protobuf/encoding/protowire"
)

// ---- decoding ----

type field struct {
	num protowire.Number
	typ protowire.Type
	v   uint64
	b   []byte
}

// fieldReader walks one message: ascending field numbers, and only the
// numbers accepted by repeated may recur.
type fieldReader struct {
	b        []byte
	prev     protowire.Number
	repeated func(protowire.Number) bool
}

func (r *fieldReader) next() (field, bool, error) {
	if len(r.b) == 0 {
		return field{}, false, nil
	}
	num, typ, n := protowire.ConsumeTag(r.b)
	if n < 0 {
		return field{}, false, fmt.Errorf("%w: bad tag", ErrMalformed)
	}
	r.b = r.b[n:]
	if num < r.prev || (num == r.prev && (r.repeated == nil || !r.repeated(num))) {
		return field{}, false, fmt.Errorf("%w: field %d out of order or duplicated", ErrMalformed, num)
	}
	r.prev = num
	f := field{num: num, typ: typ}
	switch typ {
	case protowire.VarintType:
		f.v, n = protowire.ConsumeVarint(r.b)
	case protowire.BytesType:
		f.b, n = protowire.ConsumeBytes(r.b)
	default:
		return field{}, false, fmt.Errorf("%w: unsupported wire type", ErrMalformed)
	}
	if n < 0 {
		return field{}, false, fmt.Errorf("%w: truncated field %d", ErrMalformed, num)
	}
	r.b = r.b[n:]
	return f, true, nil
}

func bytesField(f field, name string) ([]byte, error) {
	if f.typ != protowire.BytesType {
		return nil, fmt.Errorf("%w: %s wire type", ErrMalformed, name)
	}
	return f.b, nil
}

func varintField(f field, max uint64, name string) (uint64, error) {
	if f.typ != protowire.VarintType {
		return 0, fmt.Errorf("%w: %s wire type", ErrMalformed, name)
	}
	if f.v > max {
		return 0, fmt.Errorf("%w: %s out of range", ErrInvalidField, name)
	}
	return f.v, nil
}

func int64Field(f field, name string) (int64, error) {
	v, err := varintField(f, math.MaxInt64, name)
	return int64(v), err
}

func fixedField(f field, dst []byte, name string) error {
	b, err := bytesField(f, name)
	if err != nil {
		return err
	}
	if len(b) != len(dst) {
		return fmt.Errorf("%w: %s length", ErrInvalidField, name)
	}
	copy(dst, b)
	return nil
}

func stringField(f field, name string) (string, error) {
	b, err := bytesField(f, name)
	return string(b), err
}

// varReader reads consecutive varints from a packed field.
type varReader struct{ b []byte }

func (r *varReader) more() bool { return len(r.b) > 0 }

func (r *varReader) next() (uint64, error) {
	v, n := protowire.ConsumeVarint(r.b)
	if n < 0 {
		return 0, fmt.Errorf("%w: bad packed varint", ErrMalformed)
	}
	r.b = r.b[n:]
	return v, nil
}

func (r *varReader) nextInt64() (int64, error) {
	v, err := r.next()
	if err != nil {
		return 0, err
	}
	if v > math.MaxInt64 {
		return 0, fmt.Errorf("%w: packed value out of range", ErrInvalidField)
	}
	return int64(v), nil
}

// decoder carries the running budgets shared by every table of one bundle.
type decoder struct {
	segBudget    int
	extentBudget int
	clipBudget   int
	outerBudget  int
	par2Budget   int
	poolLen      int64
}

// take deducts n from a bundle-wide budget, failing before anything
// proportional to n is allocated.
func take(budget *int, n int, what string, limit int) error {
	if n > *budget {
		return fmt.Errorf("%w: more than %d %s in the bundle", ErrLimit, limit, what)
	}
	*budget -= n
	return nil
}

// countVarints counts the varints of a packed field without decoding them.
func countVarints(b []byte) int {
	n := 0
	for _, c := range b {
		if c < 0x80 {
			n++
		}
	}
	return n
}

func (d *decoder) takeSegments(n uint64) error {
	if n > uint64(d.segBudget) {
		return fmt.Errorf("%w: more than %d segments", ErrLimit, MaxSegments)
	}
	d.segBudget -= int(n)
	return nil
}

// decodeTable expands a segment table. Every claim is checked against the
// budget before anything proportional to it is allocated.
func (d *decoder) decodeTable(b []byte) ([]Segment, error) {
	r := fieldReader{b: b}
	var count uint64
	var refs, sizes, partials []byte
	seen := map[protowire.Number]bool{}
	for {
		f, ok, err := r.next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		seen[f.num] = true
		switch f.num {
		case fTabCount:
			if count, err = varintField(f, math.MaxUint64, "table count"); err != nil {
				return nil, err
			}
			if err = d.takeSegments(count); err != nil {
				return nil, err
			}
		case fTabRefs:
			refs, err = bytesField(f, "table refs")
		case fTabSizes:
			sizes, err = bytesField(f, "table sizes")
		case fTabPartials:
			partials, err = bytesField(f, "table partials")
		default:
			err = fmt.Errorf("%w: unknown table field %d", ErrMalformed, f.num)
		}
		if err != nil {
			return nil, err
		}
	}
	if !seen[fTabCount] || count == 0 || !seen[fTabRefs] || !seen[fTabSizes] {
		return nil, fmt.Errorf("%w: incomplete segment table", ErrMalformed)
	}
	segs := make([]Segment, count)

	pos, rr := uint64(0), varReader{b: refs}
	for rr.more() {
		base, err := rr.next()
		if err != nil {
			return nil, err
		}
		n, err := rr.next()
		if err != nil {
			return nil, err
		}
		if n == 0 || n > count-pos {
			return nil, fmt.Errorf("%w: reference run overruns the table", ErrMalformed)
		}
		if base > uint64(d.poolLen) || (base != 0 && base-1+n > uint64(d.poolLen)) {
			return nil, fmt.Errorf("%w: reference run past the pool", ErrOutOfRange)
		}
		for k := uint64(0); k < n; k++ {
			if base == 0 {
				segs[pos+k].Ref = GapRef
			} else {
				segs[pos+k].Ref = int64(base - 1 + k)
			}
		}
		pos += n
	}
	if pos != count {
		return nil, fmt.Errorf("%w: reference runs cover %d of %d positions", ErrMalformed, pos, count)
	}

	pos, rr = 0, varReader{b: sizes}
	for rr.more() {
		size, err := rr.nextInt64()
		if err != nil {
			return nil, err
		}
		n, err := rr.next()
		if err != nil {
			return nil, err
		}
		if n == 0 || n > count-pos {
			return nil, fmt.Errorf("%w: size run overruns the table", ErrMalformed)
		}
		for k := uint64(0); k < n; k++ {
			segs[pos+k].Size = size
			segs[pos+k].End = size - 1
		}
		pos += n
	}
	if pos != count {
		return nil, fmt.Errorf("%w: size runs cover %d of %d positions", ErrMalformed, pos, count)
	}

	idx, rr := int64(-1), varReader{b: partials}
	for rr.more() {
		delta, err := rr.next()
		if err != nil {
			return nil, err
		}
		if delta >= count {
			return nil, fmt.Errorf("%w: partial index past the table", ErrMalformed)
		}
		idx += int64(delta) + 1
		if uint64(idx) >= count {
			return nil, fmt.Errorf("%w: partial index past the table", ErrMalformed)
		}
		if segs[idx].Start, err = rr.nextInt64(); err != nil {
			return nil, err
		}
		if segs[idx].End, err = rr.nextInt64(); err != nil {
			return nil, err
		}
	}
	return segs, nil
}

func (d *decoder) decodeOuter(b []byte) (OuterSource, error) {
	var o OuterSource
	r := fieldReader{b: b}
	hasSegs := false
	for {
		f, ok, err := r.next()
		if err != nil {
			return o, err
		}
		if !ok {
			break
		}
		switch f.num {
		case fOuterSegs:
			var tb []byte
			if tb, err = bytesField(f, "outer segments"); err == nil {
				o.Segments, err = d.decodeTable(tb)
				hasSegs = true
			}
		case fOuterKey:
			o.AESKey, err = copyBytes(f, "outer key")
		case fOuterIV:
			o.AESIV, err = copyBytes(f, "outer iv")
		case fOuterVol:
			o.VolumeSize, err = int64Field(f, "outer volume size")
		default:
			err = fmt.Errorf("%w: unknown outer source field %d", ErrMalformed, f.num)
		}
		if err != nil {
			return o, err
		}
	}
	if !hasSegs {
		return o, fmt.Errorf("%w: outer source without segments", ErrMalformed)
	}
	return o, nil
}

func (d *decoder) decodePar2(b []byte) (Par2File, error) {
	var p Par2File
	r := fieldReader{b: b}
	hasSegs := false
	for {
		f, ok, err := r.next()
		if err != nil {
			return p, err
		}
		if !ok {
			break
		}
		switch f.num {
		case fPar2Name:
			p.Name, err = stringField(f, "par2 name")
		case fPar2Size:
			p.Size, err = int64Field(f, "par2 size")
		case fPar2Segs:
			var tb []byte
			if tb, err = bytesField(f, "par2 segments"); err == nil {
				p.Segments, err = d.decodeTable(tb)
				hasSegs = true
			}
		default:
			err = fmt.Errorf("%w: unknown par2 field %d", ErrMalformed, f.num)
		}
		if err != nil {
			return p, err
		}
	}
	if !hasSegs {
		return p, fmt.Errorf("%w: par2 file without segments", ErrMalformed)
	}
	return p, nil
}

func copyBytes(f field, name string) ([]byte, error) {
	b, err := bytesField(f, name)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), b...), nil
}

func isRepeatedFileField(n protowire.Number) bool { return n == fOuter || n == fPar2 }

func (d *decoder) decodeFile(b []byte) (File, error) {
	var f File
	r := fieldReader{b: b, repeated: isRepeatedFileField}
	var haveName, haveSize, haveID bool
	for {
		fl, ok, err := r.next()
		if err != nil {
			return f, err
		}
		if !ok {
			break
		}
		switch fl.num {
		case fName:
			f.Name, err = stringField(fl, "file name")
			haveName = true
		case fSize:
			f.Size, err = int64Field(fl, "file size")
			haveSize = true
		case fDate:
			f.ReleaseDate, err = int64Field(fl, "release date")
		case fSegs:
			var tb []byte
			if tb, err = bytesField(fl, "file segments"); err == nil {
				f.Segments, err = d.decodeTable(tb)
			}
		case fKey:
			f.AESKey, err = copyBytes(fl, "file key")
		case fIV:
			f.AESIV, err = copyBytes(fl, "file iv")
		case fOuter:
			if len(f.Outer) >= MaxOuterSources {
				return f, fmt.Errorf("%w: outer sources", ErrLimit)
			}
			if err = take(&d.outerBudget, 1, "outer sources", MaxTotalOuterSources); err != nil {
				return f, err
			}
			var ob []byte
			if ob, err = bytesField(fl, "outer source"); err == nil {
				var o OuterSource
				if o, err = d.decodeOuter(ob); err == nil {
					f.Outer = append(f.Outer, o)
				}
			}
		case fExtents:
			f.Extents, err = d.decodeExtents(fl)
		case fClips:
			f.Clips, err = d.decodeClips(fl)
		case fPar2:
			if len(f.Par2) >= MaxPar2Files {
				return f, fmt.Errorf("%w: par2 files", ErrLimit)
			}
			if err = take(&d.par2Budget, 1, "par2 files", MaxTotalPar2Files); err != nil {
				return f, err
			}
			var pb []byte
			if pb, err = bytesField(fl, "par2 file"); err == nil {
				var p Par2File
				if p, err = d.decodePar2(pb); err == nil {
					f.Par2 = append(f.Par2, p)
				}
			}
		case fFileID:
			err = fixedField(fl, f.FileID[:], "file id")
			haveID = true
		default:
			err = fmt.Errorf("%w: unknown file field %d", ErrMalformed, fl.num)
		}
		if err != nil {
			return f, err
		}
	}
	if !haveName || !haveSize || !haveID {
		return f, fmt.Errorf("%w: incomplete file entry", ErrMalformed)
	}
	return f, nil
}

func (d *decoder) decodeExtents(f field) ([]Extent, error) {
	b, err := bytesField(f, "extents")
	if err != nil {
		return nil, err
	}
	var out []Extent
	r := varReader{b: b}
	for r.more() {
		if d.extentBudget == 0 {
			return nil, fmt.Errorf("%w: more than %d extents", ErrLimit, MaxExtents)
		}
		d.extentBudget--
		outer, err := r.next()
		if err != nil {
			return nil, err
		}
		if outer > MaxOuterSources {
			return nil, fmt.Errorf("%w: extent outer index", ErrOutOfRange)
		}
		var e Extent
		e.Outer = int(outer)
		if e.InnerOffset, err = r.nextInt64(); err != nil {
			return nil, err
		}
		if e.InnerLength, err = r.nextInt64(); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}

func (d *decoder) decodeClips(f field) ([]Clip, error) {
	b, err := bytesField(f, "clips")
	if err != nil {
		return nil, err
	}
	// Count the packed varints first so the allocation is bounded by the
	// budgets, not by what the input claims.
	nv := countVarints(b)
	if nv%2 != 0 {
		return nil, fmt.Errorf("%w: odd clip varint count", ErrMalformed)
	}
	if nv/2 > MaxClips {
		return nil, fmt.Errorf("%w: clips", ErrLimit)
	}
	if err := take(&d.clipBudget, nv/2, "clips", MaxTotalClips); err != nil {
		return nil, err
	}
	out := make([]Clip, 0, nv/2)
	r := varReader{b: b}
	for r.more() {
		var c Clip
		if c.ByteLen, err = r.nextInt64(); err != nil {
			return nil, err
		}
		z, err := r.next()
		if err != nil {
			return nil, err
		}
		c.Delta90k = unzigzag(z)
		out = append(out, c)
	}
	return out, nil
}

func decodeReleaseFiles(f field) ([]int, error) {
	b, err := bytesField(f, "release files")
	if err != nil {
		return nil, err
	}
	var out []int
	r := varReader{b: b}
	for r.more() {
		if len(out) >= MaxReleaseFiles {
			return nil, fmt.Errorf("%w: release files", ErrLimit)
		}
		v, err := r.next()
		if err != nil {
			return nil, err
		}
		if v > MaxSegments {
			return nil, fmt.Errorf("%w: release file segment count", ErrLimit)
		}
		out = append(out, int(v))
	}
	return out, nil
}

const (
	haveVersion = 1 << iota
	haveScheme
	haveRelease
	haveTitle
	havePosted
	haveTotal
	haveFiles
	haveAll = haveVersion | haveScheme | haveRelease | haveTitle | havePosted | haveTotal | haveFiles
)

func isRepeatedBundleField(n protowire.Number) bool { return n == fPool || n == fFiles }

// decodeRaw parses the wire layout strictly (order, duplicates, resource
// budgets) without the structural checks of Validate.
func decodeRaw(raw []byte) (*Bundle, error) {
	if len(raw) > MaxDecompressedSize {
		return nil, fmt.Errorf("%w: %d bytes", ErrTooLarge, len(raw))
	}
	if len(raw) == 0 {
		return nil, fmt.Errorf("%w: empty input", ErrMalformed)
	}
	b := &Bundle{}
	d := &decoder{segBudget: MaxSegments, extentBudget: MaxExtents, clipBudget: MaxTotalClips,
		outerBudget: MaxTotalOuterSources, par2Budget: MaxTotalPar2Files}
	var val *validator
	r := fieldReader{b: raw, repeated: isRepeatedBundleField}
	have := 0
	for {
		f, ok, err := r.next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		switch f.num {
		case fVersion:
			var v uint64
			if v, err = varintField(f, math.MaxUint64, "version"); err == nil && v != SchemaVersion {
				err = fmt.Errorf("%w: %d", ErrUnsupportedVersion, v)
			}
			have |= haveVersion
		case fScheme:
			var v uint64
			if v, err = varintField(f, math.MaxUint64, "scheme"); err == nil {
				if b.Scheme = Scheme(v); v != uint64(SchemeNZB) && v != uint64(SchemeFiles) {
					err = fmt.Errorf("%w: %d", ErrUnknownScheme, v)
				}
			}
			have |= haveScheme
		case fReleaseID:
			err = fixedField(f, b.ReleaseID[:], "release id")
			have |= haveRelease
		case fTitle:
			b.Title, err = stringField(f, "title")
			have |= haveTitle
		case fPosted:
			b.PostedUnix, err = int64Field(f, "posted")
			have |= havePosted
		case fTotal:
			b.TotalSize, err = int64Field(f, "total size")
			have |= haveTotal
		case fPassword:
			b.NZBPassword, err = stringField(f, "password")
		case fPool:
			if len(b.Pool) >= MaxSegments {
				return nil, fmt.Errorf("%w: pool", ErrLimit)
			}
			var s string
			if s, err = stringField(f, "pool entry"); err == nil {
				b.Pool = append(b.Pool, s)
			}
		case fReleaseFiles:
			b.ReleaseFiles, err = decodeReleaseFiles(f)
		case fFiles:
			if len(b.Files) >= MaxFiles {
				return nil, fmt.Errorf("%w: files", ErrLimit)
			}
			d.poolLen = int64(len(b.Pool))
			var fb []byte
			if fb, err = bytesField(f, "file"); err == nil {
				var file File
				if file, err = d.decodeFile(fb); err == nil {
					// Validate each file as soon as it is decoded so a
					// hostile bundle fails before the next one is expanded.
					if val == nil {
						val = &validator{b: b, names: map[string]struct{}{}, tables: map[TableKey]int32{}}
						err = val.checkPool()
					}
					if err == nil {
						err = val.checkFile(&file)
					}
					if err == nil {
						b.Files = append(b.Files, file)
					}
				}
			}
			have |= haveFiles
		default:
			err = fmt.Errorf("%w: unknown field %d", ErrMalformed, f.num)
		}
		if err != nil {
			return nil, err
		}
	}
	if have != haveAll {
		return nil, fmt.Errorf("%w: missing required field", ErrMalformed)
	}
	return b, nil
}

// Decode strictly decodes canonical uncompressed bundle bytes and validates
// the result. Anything that is not exactly what Encode would produce for the
// decoded bundle is rejected, so a bundle has one byte representation and one
// Bundle ID.
func Decode(raw []byte) (*Bundle, error) {
	b, err := decodeRaw(raw)
	if err != nil {
		return nil, err
	}
	if err := Validate(b); err != nil {
		return nil, err
	}
	if !matchesCanonical(b, raw) {
		return nil, fmt.Errorf("%w: not the canonical encoding", ErrMalformed)
	}
	return b, nil
}
