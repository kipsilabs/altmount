package bundle

import (
	"bytes"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/kipsilabs/altmount/internal/peernet/contentid"
)

func TestValidateAcceptsGoodBundles(t *testing.T) {
	for name, b := range map[string]*Bundle{"files": goodBundle(t), "nzb": goodNZBBundle(t)} {
		if err := Validate(b); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	if err := Validate(nil); !errors.Is(err, ErrInvalidField) {
		t.Errorf("nil bundle: %v", err)
	}
}

func TestValidateRejects(t *testing.T) {
	tests := []struct {
		name   string
		nzb    bool
		mutate func(b *Bundle)
		want   error
	}{
		// ---- segment ranges and sums
		{"file size one too big", false, func(b *Bundle) { fileNamed(t, b, "movie.mkv").Size++ }, ErrSizeMismatch},
		{"end offset one past the segment", false, func(b *Bundle) { fileNamed(t, b, "movie.mkv").Segments[0].End = 1000 }, ErrOutOfRange},
		{"start after end", false, func(b *Bundle) { s := &fileNamed(t, b, "movie.mkv").Segments[0]; s.Start, s.End = 500, 100 }, ErrOutOfRange},
		{"negative start", false, func(b *Bundle) { fileNamed(t, b, "movie.mkv").Segments[0].Start = -1 }, ErrOutOfRange},
		{"ref past the pool", false, func(b *Bundle) { fileNamed(t, b, "movie.mkv").Segments[0].Ref = int64(len(b.Pool)) }, ErrOutOfRange},
		{"ref below gap", false, func(b *Bundle) { fileNamed(t, b, "movie.mkv").Segments[0].Ref = -2 }, ErrOutOfRange},
		{"segment size zero", false, func(b *Bundle) { s := &fileNamed(t, b, "movie.mkv").Segments[0]; s.Size, s.Start, s.End = 0, 0, -1 }, ErrOutOfRange},
		{"segment larger than any article", false, func(b *Bundle) {
			s := &fileNamed(t, b, "movie.mkv").Segments[0]
			s.Size, s.End = MaxSegmentSize+1, MaxSegmentSize
		}, ErrImplausible},
		{"segment size near MaxInt64", false, func(b *Bundle) {
			s := &fileNamed(t, b, "movie.mkv").Segments[0]
			s.Size, s.End = math.MaxInt64, math.MaxInt64-1
		}, ErrImplausible},
		{"one byte gap inside the extents", false, func(b *Bundle) { fileNamed(t, b, "movie.mkv").Segments[1].Start = 1 }, ErrSizeMismatch},
		{"declared size far beyond the segments", false, func(b *Bundle) {
			f := fileNamed(t, b, "movie.mkv")
			b.TotalSize += (1 << 40) - f.Size
			f.Size = 1 << 40
		}, ErrImplausible},
		{"file size zero", false, func(b *Bundle) { f := fileNamed(t, b, "movie.mkv"); b.TotalSize -= f.Size; f.Size = 0 }, ErrInvalidField},
		{"file size above cap", false, func(b *Bundle) { fileNamed(t, b, "movie.mkv").Size = MaxFileSize + 1 }, ErrInvalidField},
		{"file without data", false, func(b *Bundle) { fileNamed(t, b, "movie.mkv").Segments = nil }, ErrInvalidField},
		{"total size mismatch", false, func(b *Bundle) { b.TotalSize++ }, ErrSizeMismatch},
		{"release date negative", false, func(b *Bundle) { fileNamed(t, b, "movie.mkv").ReleaseDate = -1 }, ErrInvalidField},
		{"release date too far", false, func(b *Bundle) { fileNamed(t, b, "movie.mkv").ReleaseDate = maxUnixSeconds + 1 }, ErrInvalidField},

		// ---- encryption
		{"aes key 15 bytes", false, func(b *Bundle) { f := fileNamed(t, b, "secret.bin"); f.AESKey = f.AESKey[:15] }, ErrBadEncryption},
		{"aes key 33 bytes", false, func(b *Bundle) { f := fileNamed(t, b, "secret.bin"); f.AESKey = append(f.AESKey, 1) }, ErrBadEncryption},
		{"aes iv 15 bytes", false, func(b *Bundle) { f := fileNamed(t, b, "secret.bin"); f.AESIV = f.AESIV[:15] }, ErrBadEncryption},
		{"aes key without iv", false, func(b *Bundle) { fileNamed(t, b, "secret.bin").AESIV = nil }, ErrBadEncryption},
		{"aes iv without key", false, func(b *Bundle) { f := fileNamed(t, b, "movie.mkv"); f.AESIV = bytes.Repeat([]byte{1}, 16) }, ErrBadEncryption},
		{"encrypted usable range shorter than padded size", false, func(b *Bundle) { fileNamed(t, b, "secret.bin").Segments[0].End-- }, ErrBadEncryption},
		{"encrypted usable range longer than padded size", false, func(b *Bundle) { fileNamed(t, b, "secret.bin").Segments[0].End += 4 }, ErrBadEncryption},
		{"aes key on a nested file", false, func(b *Bundle) {
			f := fileNamed(t, b, "nested.mkv")
			f.AESKey, f.AESIV = key32(), iv16()
		}, ErrBadEncryption},
		{"outer key length", false, func(b *Bundle) { f := fileNamed(t, b, "nested.mkv"); f.Outer[0].AESKey = f.Outer[0].AESKey[:20] }, ErrBadEncryption},
		{"outer iv missing", false, func(b *Bundle) { fileNamed(t, b, "nested.mkv").Outer[0].AESIV = nil }, ErrBadEncryption},
		{"outer volume size zero", false, func(b *Bundle) { fileNamed(t, b, "nested.mkv").Outer[0].VolumeSize = 0 }, ErrBadEncryption},
		{"outer volume size on unencrypted outer", false, func(b *Bundle) { fileNamed(t, b, "disc.m2ts").Outer[0].VolumeSize = 5 }, ErrBadEncryption},
		{"outer coverage short of the padded volume", false, func(b *Bundle) { fileNamed(t, b, "nested.mkv").Outer[0].VolumeSize = 1100 }, ErrBadEncryption},

		// ---- nested sources
		{"extent outer index out of range", false, func(b *Bundle) { fileNamed(t, b, "nested.mkv").Extents[0].Outer = 1 }, ErrOutOfRange},
		{"extent outer index negative", false, func(b *Bundle) { fileNamed(t, b, "nested.mkv").Extents[0].Outer = -1 }, ErrOutOfRange},
		{"extent beyond the encrypted volume", false, func(b *Bundle) { fileNamed(t, b, "nested.mkv").Extents[1].InnerOffset = 801 }, ErrOutOfRange},
		{"extent beyond the plain outer coverage", false, func(b *Bundle) { fileNamed(t, b, "disc.m2ts").Extents[1].InnerOffset = 1100 }, ErrOutOfRange},
		{"extent length zero", false, func(b *Bundle) { fileNamed(t, b, "nested.mkv").Extents[0].InnerLength = 0 }, ErrOutOfRange},
		{"extent offset negative", false, func(b *Bundle) { fileNamed(t, b, "nested.mkv").Extents[0].InnerOffset = -1 }, ErrOutOfRange},
		{"extents do not sum to the file size", false, func(b *Bundle) { fileNamed(t, b, "nested.mkv").Extents[0].InnerLength-- }, ErrSizeMismatch},
		{"extents overlap", false, func(b *Bundle) {
			f := fileNamed(t, b, "nested.mkv")
			f.Extents[1].InnerOffset = 300
		}, ErrOverlap},
		{"sum of extents overflows int64", false, func(b *Bundle) {
			f := fileNamed(t, b, "nested.mkv")
			f.Extents[0].InnerLength = math.MaxInt64 - 5
			f.Extents[1].InnerLength = 10
		}, ErrOutOfRange},
		{"extent offset plus length overflows", false, func(b *Bundle) {
			f := fileNamed(t, b, "nested.mkv")
			f.Extents[0].InnerOffset = math.MaxInt64 - 5
		}, ErrOutOfRange},
		{"unreferenced outer source", false, func(b *Bundle) {
			f := fileNamed(t, b, "nested.mkv")
			f.Outer = append(f.Outer, f.Outer[0])
		}, ErrInvalidField},
		{"nested file also lists segments", false, func(b *Bundle) {
			fileNamed(t, b, "nested.mkv").Segments = []Segment{{Ref: 0, Size: 10, Start: 0, End: 9}}
		}, ErrInvalidField},
		{"extents without outer sources", false, func(b *Bundle) { fileNamed(t, b, "nested.mkv").Outer = nil }, ErrOutOfRange},
		{"outer sources without extents", false, func(b *Bundle) { fileNamed(t, b, "nested.mkv").Extents = nil }, ErrInvalidField},
		{"too many outer sources", false, func(b *Bundle) {
			f := fileNamed(t, b, "nested.mkv")
			for i := 0; i <= MaxOuterSources; i++ {
				f.Outer = append(f.Outer, OuterSource{})
			}
		}, ErrLimit},
		{"too many extents", false, func(b *Bundle) {
			f := fileNamed(t, b, "disc.m2ts")
			f.Extents = make([]Extent, MaxExtents+1)
		}, ErrLimit},

		// ---- clips
		{"clips not summing to the file size", false, func(b *Bundle) { fileNamed(t, b, "disc.m2ts").Clips[1].ByteLen += ClipPacketSize }, ErrBadClip},
		{"clip not a packet multiple", false, func(b *Bundle) {
			c := fileNamed(t, b, "disc.m2ts").Clips
			c[0].ByteLen++
			c[1].ByteLen--
		}, ErrBadClip},
		{"clip length zero", false, func(b *Bundle) {
			c := fileNamed(t, b, "disc.m2ts").Clips
			c[0].ByteLen = 0
			c[1].ByteLen += 192
		}, ErrBadClip},
		{"clip delta out of range", false, func(b *Bundle) { fileNamed(t, b, "disc.m2ts").Clips[0].Delta90k = MaxClipDelta + 1 }, ErrBadClip},
		{"clip delta min int64", false, func(b *Bundle) { fileNamed(t, b, "disc.m2ts").Clips[0].Delta90k = math.MinInt64 }, ErrBadClip},
		{"too many clips", false, func(b *Bundle) { fileNamed(t, b, "disc.m2ts").Clips = make([]Clip, MaxClips+1) }, ErrLimit},

		// ---- PAR2
		{"par2 gap", false, func(b *Bundle) { fileNamed(t, b, "movie.mkv").Par2[0].Segments[0].Ref = GapRef }, ErrInvalidField},
		{"par2 size mismatch", false, func(b *Bundle) { fileNamed(t, b, "movie.mkv").Par2[0].Size++ }, ErrSizeMismatch},
		{"par2 bad name", false, func(b *Bundle) { fileNamed(t, b, "movie.mkv").Par2[0].Name = "../x.par2" }, ErrBadFilename},
		{"par2 duplicate name", false, func(b *Bundle) {
			f := fileNamed(t, b, "movie.mkv")
			f.Par2 = append(f.Par2, f.Par2[0])
		}, ErrDuplicateName},
		{"too many par2 files", false, func(b *Bundle) {
			f := fileNamed(t, b, "movie.mkv")
			f.Par2 = make([]Par2File, MaxPar2Files+1)
		}, ErrLimit},

		// ---- names and text
		{"path traversal name", false, func(b *Bundle) { fileNamed(t, b, "movie.mkv").Name = "../x" }, ErrBadFilename},
		{"slash in name", false, func(b *Bundle) { fileNamed(t, b, "movie.mkv").Name = "a/b" }, ErrBadFilename},
		{"backslash in name", false, func(b *Bundle) { fileNamed(t, b, "movie.mkv").Name = `a\b` }, ErrBadFilename},
		{"NUL in name", false, func(b *Bundle) { fileNamed(t, b, "movie.mkv").Name = "a\x00b" }, ErrBadFilename},
		{"dot-dot name", false, func(b *Bundle) { fileNamed(t, b, "movie.mkv").Name = ".." }, ErrBadFilename},
		{"windows reserved name", false, func(b *Bundle) { fileNamed(t, b, "movie.mkv").Name = "CON.mkv" }, ErrBadFilename},
		{"bidi in name", false, func(b *Bundle) { fileNamed(t, b, "movie.mkv").Name = "a\u202Eb" }, ErrBadFilename},
		{"long name", false, func(b *Bundle) { fileNamed(t, b, "movie.mkv").Name = strings.Repeat("a", 256) }, ErrBadFilename},
		{"duplicate names", false, func(b *Bundle) { fileNamed(t, b, "secret.bin").Name = "movie.mkv" }, ErrDuplicateName},
		{"duplicate names ignoring case", false, func(b *Bundle) { fileNamed(t, b, "secret.bin").Name = "MOVIE.MKV" }, ErrDuplicateName},
		{"bad title", false, func(b *Bundle) { b.Title = "a\u202Eb" }, ErrInvalidField},
		{"empty title", false, func(b *Bundle) { b.Title = "" }, ErrInvalidField},
		{"posted zero", false, func(b *Bundle) { b.PostedUnix = 0 }, ErrInvalidField},
		{"posted too far", false, func(b *Bundle) { b.PostedUnix = maxUnixSeconds + 1 }, ErrInvalidField},
		{"password control character", false, func(b *Bundle) { b.NZBPassword = "a\x00b" }, ErrInvalidField},
		{"password too long", false, func(b *Bundle) { b.NZBPassword = strings.Repeat("p", MaxPasswordLen+1) }, ErrInvalidField},

		// ---- ids, files, pool
		{"zero release id", false, func(b *Bundle) { b.ReleaseID = contentid.ID{} }, ErrInvalidField},
		{"zero file id", false, func(b *Bundle) { b.Files[0].FileID = contentid.ID{} }, ErrInvalidField},
		{"unknown scheme", false, func(b *Bundle) { b.Scheme = 9 }, ErrUnknownScheme},
		{"no files", false, func(b *Bundle) { b.Files = nil; b.TotalSize = 0 }, ErrInvalidField},
		{"files unsorted", false, func(b *Bundle) { b.Files[0], b.Files[1] = b.Files[1], b.Files[0] }, ErrInvalidField},
		{"duplicate file id", false, func(b *Bundle) { b.Files[1].FileID = b.Files[0].FileID }, ErrDuplicateFile},
		{"pool entry with a space", false, func(b *Bundle) { b.Pool[0] = "a b@x.test" }, ErrBadMessageID},
		{"pool entry with CRLF", false, func(b *Bundle) { b.Pool[0] = "a\r\nQUIT@x" }, ErrBadMessageID},
		{"pool entry that looks like a placeholder", false, func(b *Bundle) { b.Pool[0] = "altmount-gap-3-x" }, ErrBadMessageID},
		{"pool entry too long", false, func(b *Bundle) { b.Pool[0] = strings.Repeat("a", MaxMessageIDLen+1) }, ErrBadMessageID},
		{"pool entry in angle brackets", false, func(b *Bundle) { b.Pool[0] = "<" + b.Pool[0] + ">" }, ErrBadMessageID},
		{"files scheme duplicate pool entry", false, func(b *Bundle) { b.Pool[1] = b.Pool[0] }, ErrBadPool},
		{"files scheme unreferenced pool entry", false, func(b *Bundle) { b.Pool = append(b.Pool, "extra@x.test") }, ErrBadPool},
		{"files scheme with a release list", false, func(b *Bundle) { b.ReleaseFiles = []int{1} }, ErrInvalidField},
		{"nzb scheme counts do not match the pool", true, func(b *Bundle) { b.ReleaseFiles[0]++ }, ErrBadPool},
		{"nzb scheme without a release list", true, func(b *Bundle) { b.ReleaseFiles = nil }, ErrBadPool},
		{"nzb scheme negative count", true, func(b *Bundle) { b.ReleaseFiles[0] = -1; b.ReleaseFiles[1]++ }, ErrBadPool},
		{"nzb scheme too many release files", true, func(b *Bundle) { b.ReleaseFiles = make([]int, MaxReleaseFiles+1) }, ErrLimit},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := goodBundle(t)
			if tc.nzb {
				b = goodNZBBundle(t)
			}
			tc.mutate(b)
			err := Validate(b)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Validate = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestValidateSegmentCountCaps(t *testing.T) {
	const n = MaxSegments + 1
	pool := make([]string, n)
	segs := make([]Segment, n)
	for i := range pool {
		pool[i] = "id" + strings.Repeat("x", 5) + string(rune('a'+i%26)) + string(rune('a'+(i/26)%26)) + string(rune('a'+(i/676)%26)) + string(rune('a'+(i/17576)%26)) + string(rune('a'+(i/456976)%26))
		segs[i] = Segment{Ref: int64(i), Size: 1, Start: 0, End: 0}
	}
	b := &Bundle{Scheme: SchemeFiles, ReleaseID: contentid.ID{1}, Title: "t", PostedUnix: 1, TotalSize: n, Pool: pool,
		Files: []File{{Name: "a", Size: n, FileID: contentid.ID{2}, Segments: segs}}}
	if err := Validate(b); !errors.Is(err, ErrLimit) {
		t.Fatalf("500,001 segments: %v", err)
	}
	// Exactly at the cap the count check passes (other checks may still fail
	// on this synthetic data, but never with ErrLimit).
	b.Pool, b.Files[0].Segments, b.Files[0].Size, b.TotalSize = pool[:MaxSegments], segs[:MaxSegments], MaxSegments, MaxSegments
	if err := Validate(b); errors.Is(err, ErrLimit) {
		t.Fatalf("500,000 segments rejected as too many: %v", err)
	}
}

func TestValidateFileCountCap(t *testing.T) {
	b := goodBundle(t)
	for len(b.Files) <= MaxFiles {
		b.Files = append(b.Files, b.Files[0])
	}
	if err := Validate(b); !errors.Is(err, ErrLimit) {
		t.Fatalf("10,001 files: %v", err)
	}
}

func TestValidateDoesNotMutate(t *testing.T) {
	b := goodBundle(t)
	c := clone(b)
	if err := Validate(b); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encodeUnchecked(b), encodeUnchecked(c)) {
		t.Fatal("Validate changed the bundle")
	}
}
