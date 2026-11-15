// Package mediagen builds small, structurally valid media files for tests.
package mediagen

// MKV returns a structurally valid Matroska file of exactly size bytes
// (size >= 200): EBML header, a Segment ending at the file end, a SeekHead
// pointing at Info and at Cues (the last element), and a Cluster of
// SimpleBlocks between them.
func MKV(size int) []byte {
	u32 := func(v int) []byte { return []byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)} }
	seek := func(id []byte, pos int) []byte {
		body := append(append([]byte{0x53, 0xAB, 0x84}, id...), append([]byte{0x53, 0xAC, 0x84}, u32(pos)...)...)
		return append([]byte{0x4D, 0xBB, byte(0x80 | len(body))}, body...)
	}
	info := []byte{0x15, 0x49, 0xA9, 0x66}
	cues := []byte{0x1C, 0x53, 0xBB, 0x6B}
	ebml := append([]byte{0x1A, 0x45, 0xDF, 0xA3, 0x8B, 0x42, 0x82, 0x88}, "matroska"...)
	segStart := len(ebml) + 12
	seekHead := append([]byte{0x11, 0x4D, 0x9B, 0x74, byte(0x80 | 34)}, append(seek(info, 39), seek(cues, size-segStart-9)...)...)
	out := append([]byte{}, ebml...)
	segSize := size - segStart
	out = append(out, 0x18, 0x53, 0x80, 0x67, 0x01, 0, byte(segSize>>40), byte(segSize>>32), byte(segSize>>24), byte(segSize>>16), byte(segSize>>8), byte(segSize))
	out = append(out, seekHead...)
	out = append(out, append(append([]byte{}, info...), 0x84, 0, 0, 0, 0)...)
	fill := size - len(out) - 12 - 9
	out = append(out, 0x1F, 0x43, 0xB6, 0x75, 0x01, 0, byte(fill>>40), byte(fill>>32), byte(fill>>24), byte(fill>>16), byte(fill>>8), byte(fill))
	out = append(out, SimpleBlocks(fill)...)
	return append(out, append(append([]byte{}, cues...), 0x84, 0, 0, 0, 0)...)
}

// MKVClusters is MKV with the body cut into Clusters of about clusterBytes,
// each starting with a Timecode element (5000 ticks apart, increasing), so a
// reader can tell where in the stream any part of the file sits.
func MKVClusters(size, clusterBytes int) []byte {
	head := MKV(size)
	// The fixed part: everything before the Cluster and the 9-byte Cues.
	const clusterHdr = 12
	bodyAt := len(head) - 9
	for i := 0; i+4 <= len(head); i++ {
		if head[i] == 0x1F && head[i+1] == 0x43 && head[i+2] == 0xB6 && head[i+3] == 0x75 {
			bodyAt = i
			break
		}
	}
	cues := head[len(head)-9:]
	out := append([]byte{}, head[:bodyAt]...)
	remaining := len(head) - 9 - bodyAt
	for k := 0; remaining > 0; k++ {
		n := min(clusterBytes, remaining)
		if remaining-n < clusterHdr+6+7 { // never leave a tail too small for a cluster
			n = remaining
		}
		inner := n - clusterHdr
		out = append(out, 0x1F, 0x43, 0xB6, 0x75, 0x01, 0, byte(inner>>40), byte(inner>>32), byte(inner>>24), byte(inner>>16), byte(inner>>8), byte(inner))
		tc := (k + 1) * 5000
		out = append(out, 0xE7, 0x84, byte(tc>>24), byte(tc>>16), byte(tc>>8), byte(tc))
		out = append(out, SimpleBlocks(inner-6)...)
		remaining -= n
	}
	return append(out, cues...)
}

// AVI returns a RIFF/AVI container of size bytes: the container signature and
// header list followed by filler.
func AVI(size int) []byte {
	out := append([]byte("RIFF\x00\x00\x00\x00AVI LIST\x00\x00\x00\x00hdrlavih"), SimpleBlocks(size-28)...)
	n := uint32(len(out) - 8)
	out[4], out[5], out[6], out[7] = byte(n), byte(n>>8), byte(n>>16), byte(n>>24)
	return out
}

// SimpleBlocks returns exactly n bytes (n >= 7) of consecutive Matroska
// SimpleBlock elements, what the body of a real Cluster looks like.
func SimpleBlocks(n int) []byte {
	out := make([]byte, 0, n)
	for k := 0; len(out) < n; k++ {
		remaining := n - len(out)
		want := min(remaining, 90+(k*37)%300)
		if remaining-want < 7 {
			want = remaining
		}
		body := want - 3
		out = append(out, 0xA3, 0x40|byte(body>>8), byte(body), 0x81, byte(k>>8), byte(k), 0x80)
		for j := 0; j < body-4; j++ {
			out = append(out, byte(1+(len(out)*7+k)%250))
		}
	}
	return out
}
