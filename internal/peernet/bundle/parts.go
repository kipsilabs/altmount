package bundle

// PartsOf returns the slices of f's inline table: one per part of a multi-part
// file, or the whole table for a file read as one posted file. The slices alias
// f.Segments. A file with no inline table has none. The caller has validated f
// (the part lengths add up to the table).
func PartsOf(f *File) [][]Segment {
	if len(f.Segments) == 0 {
		return nil
	}
	if len(f.Parts) == 0 {
		return [][]Segment{f.Segments}
	}
	out := make([][]Segment, 0, len(f.Parts))
	at := 0
	for _, n := range f.Parts {
		if n < 0 || n > len(f.Segments)-at {
			return [][]Segment{f.Segments}
		}
		out = append(out, f.Segments[at:at+n])
		at += n
	}
	return out
}

// Usable is the number of bytes the usable ranges of segs add up to.
func Usable(segs []Segment) int64 {
	var n int64
	for _, s := range segs {
		n += s.End - s.Start + 1
	}
	return n
}
