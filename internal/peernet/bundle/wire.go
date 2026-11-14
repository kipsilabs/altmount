package bundle

import (
	"crypto/sha256"
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
)

// Field numbers. The layout is canonical: fields appear in ascending number,
// repeated fields are consecutive, optional fields are omitted when empty.
const (
	// Bundle.
	fVersion      protowire.Number = 1
	fScheme       protowire.Number = 2
	fReleaseID    protowire.Number = 3
	fTitle        protowire.Number = 4
	fPosted       protowire.Number = 5
	fTotal        protowire.Number = 6
	fPassword     protowire.Number = 7
	fPool         protowire.Number = 8
	fReleaseFiles protowire.Number = 9
	fFiles        protowire.Number = 10

	// File.
	fName    protowire.Number = 1
	fSize    protowire.Number = 2
	fDate    protowire.Number = 3
	fSegs    protowire.Number = 4
	fKey     protowire.Number = 5
	fIV      protowire.Number = 6
	fOuter   protowire.Number = 7
	fExtents protowire.Number = 8
	fClips   protowire.Number = 9
	fPar2    protowire.Number = 10
	fFileID  protowire.Number = 11

	// OuterSource.
	fOuterSegs protowire.Number = 1
	fOuterKey  protowire.Number = 2
	fOuterIV   protowire.Number = 3
	fOuterVol  protowire.Number = 4

	// Par2File.
	fPar2Name protowire.Number = 1
	fPar2Size protowire.Number = 2
	fPar2Segs protowire.Number = 3

	// Segment table.
	fTabCount    protowire.Number = 1
	fTabRefs     protowire.Number = 2
	fTabSizes    protowire.Number = 3
	fTabPartials protowire.Number = 4
)

// ComputeID returns the Bundle ID: the SHA-256 of the canonical bytes.
func ComputeID(canonical []byte) BundleID { return sha256.Sum256(canonical) }

// ---- encoding ----

func appendBytes(b []byte, num protowire.Number, v []byte) []byte {
	return protowire.AppendBytes(protowire.AppendTag(b, num, protowire.BytesType), v)
}

func appendString(b []byte, num protowire.Number, v string) []byte {
	return protowire.AppendString(protowire.AppendTag(b, num, protowire.BytesType), v)
}

func appendVarint(b []byte, num protowire.Number, v uint64) []byte {
	return protowire.AppendVarint(protowire.AppendTag(b, num, protowire.VarintType), v)
}

func zigzag(v int64) uint64   { return uint64(v<<1) ^ uint64(v>>63) }
func unzigzag(v uint64) int64 { return int64(v>>1) ^ -int64(v&1) }

func isFull(s Segment) bool { return s.Start == 0 && s.End == s.Size-1 }

// appendTable encodes a segment table: position count, reference runs, size
// runs and the partial-use exceptions.
func appendTable(segs []Segment) []byte {
	var out, refs, sizes, partials []byte
	out = appendVarint(out, fTabCount, uint64(len(segs)))

	for i := 0; i < len(segs); {
		j := i + 1
		for j < len(segs) && sameRun(segs[j-1], segs[j]) {
			j++
		}
		base := uint64(0)
		if segs[i].Ref != GapRef {
			base = uint64(segs[i].Ref) + 1
		}
		refs = protowire.AppendVarint(refs, base)
		refs = protowire.AppendVarint(refs, uint64(j-i))
		i = j
	}
	out = appendBytes(out, fTabRefs, refs)

	for i := 0; i < len(segs); {
		j := i + 1
		for j < len(segs) && segs[j].Size == segs[i].Size {
			j++
		}
		sizes = protowire.AppendVarint(sizes, uint64(segs[i].Size))
		sizes = protowire.AppendVarint(sizes, uint64(j-i))
		i = j
	}
	out = appendBytes(out, fTabSizes, sizes)

	prev := -1
	for i, s := range segs {
		if isFull(s) {
			continue
		}
		partials = protowire.AppendVarint(partials, uint64(i-prev-1))
		partials = protowire.AppendVarint(partials, uint64(s.Start))
		partials = protowire.AppendVarint(partials, uint64(s.End))
		prev = i
	}
	if len(partials) > 0 {
		out = appendBytes(out, fTabPartials, partials)
	}
	return out
}

// sameRun reports whether b continues the reference run that a is part of.
func sameRun(a, b Segment) bool {
	if a.Ref == GapRef {
		return b.Ref == GapRef
	}
	return b.Ref == a.Ref+1
}

func appendFile(f *File) []byte {
	var b []byte
	b = appendString(b, fName, f.Name)
	b = appendVarint(b, fSize, uint64(f.Size))
	if f.ReleaseDate != 0 {
		b = appendVarint(b, fDate, uint64(f.ReleaseDate))
	}
	if len(f.Segments) > 0 {
		b = appendBytes(b, fSegs, appendTable(f.Segments))
	}
	if len(f.AESKey) > 0 {
		b = appendBytes(b, fKey, f.AESKey)
		b = appendBytes(b, fIV, f.AESIV)
	}
	for i := range f.Outer {
		o := &f.Outer[i]
		var ob []byte
		ob = appendBytes(ob, fOuterSegs, appendTable(o.Segments))
		if len(o.AESKey) > 0 {
			ob = appendBytes(ob, fOuterKey, o.AESKey)
			ob = appendBytes(ob, fOuterIV, o.AESIV)
			ob = appendVarint(ob, fOuterVol, uint64(o.VolumeSize))
		}
		b = appendBytes(b, fOuter, ob)
	}
	if len(f.Extents) > 0 {
		var eb []byte
		for _, e := range f.Extents {
			eb = protowire.AppendVarint(eb, uint64(e.Outer))
			eb = protowire.AppendVarint(eb, uint64(e.InnerOffset))
			eb = protowire.AppendVarint(eb, uint64(e.InnerLength))
		}
		b = appendBytes(b, fExtents, eb)
	}
	if len(f.Clips) > 0 {
		var cb []byte
		for _, c := range f.Clips {
			cb = protowire.AppendVarint(cb, uint64(c.ByteLen))
			cb = protowire.AppendVarint(cb, zigzag(c.Delta90k))
		}
		b = appendBytes(b, fClips, cb)
	}
	for i := range f.Par2 {
		p := &f.Par2[i]
		var pb []byte
		pb = appendString(pb, fPar2Name, p.Name)
		pb = appendVarint(pb, fPar2Size, uint64(p.Size))
		pb = appendBytes(pb, fPar2Segs, appendTable(p.Segments))
		b = appendBytes(b, fPar2, pb)
	}
	return appendBytes(b, fFileID, f.FileID[:])
}

// encodeUnchecked writes the canonical bytes without validating b. Callers
// validate first; the decoder uses it only for the canonical re-encode check.
func encodeUnchecked(b *Bundle) []byte {
	var out []byte
	out = appendVarint(out, fVersion, SchemaVersion)
	out = appendVarint(out, fScheme, uint64(b.Scheme))
	out = appendBytes(out, fReleaseID, b.ReleaseID[:])
	out = appendString(out, fTitle, b.Title)
	out = appendVarint(out, fPosted, uint64(b.PostedUnix))
	out = appendVarint(out, fTotal, uint64(b.TotalSize))
	if b.NZBPassword != "" {
		out = appendString(out, fPassword, b.NZBPassword)
	}
	for _, id := range b.Pool {
		out = appendString(out, fPool, id)
	}
	if b.Scheme == SchemeNZB {
		var rb []byte
		for _, n := range b.ReleaseFiles {
			rb = protowire.AppendVarint(rb, uint64(n))
		}
		out = appendBytes(out, fReleaseFiles, rb)
	}
	for i := range b.Files {
		out = appendBytes(out, fFiles, appendFile(&b.Files[i]))
	}
	return out
}

// Encode validates b and returns its canonical uncompressed bytes.
func Encode(b *Bundle) ([]byte, error) {
	if b == nil {
		return nil, fmt.Errorf("%w: nil bundle", ErrInvalidField)
	}
	if err := Validate(b); err != nil {
		return nil, err
	}
	return encodeUnchecked(b), nil
}
