package fileinfo

import (
	"crypto/md5"
	"testing"

	"github.com/kipsilabs/altmount/internal/importer/parser/par2"
	"github.com/javi11/nntppool/v5"
	"github.com/javi11/nzbparser"
)

func TestIsProbablyObfuscated(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		want     bool
	}{
		// Certainly obfuscated patterns
		{
			name:     "32 hex digits (MD5 hash)",
			filename: "b082fa0beaa644d3aa01045d5b8d0b36.mkv",
			want:     true,
		},
		{
			name:     "40+ hex digits and dots",
			filename: "0675e29e9abfd2.f7d069dab0b853283cc1b069a25f82.6547.mkv",
			want:     true,
		},
		{
			name:     "30+ hex with square brackets",
			filename: "[BlaBla] something 5937bc5e32146e.bef89a622e4a23f07b0d3757ad5e8a.a02b264e [More].mkv",
			want:     true,
		},
		{
			name:     "abc.xyz prefix",
			filename: "abc.xyz.a4c567edbcbf27.BLA.mkv",
			want:     true,
		},

		// Not obfuscated patterns
		{
			name:     "Great Distro - has uppercase, lowercase, space",
			filename: "Great Distro.mkv",
			want:     false,
		},
		{
			name:     "this is a download - multiple spaces",
			filename: "this is a download.mkv",
			want:     false,
		},
		{
			name:     "Beast 2020 - letters, digits, space",
			filename: "Beast 2020.mkv",
			want:     false,
		},
		{
			name:     "Catullus - starts with capital, mostly lowercase",
			filename: "Catullus.mkv",
			want:     false,
		},
		{
			name:     "Movie.Name.2023.1080p - typical release name",
			filename: "Movie.Name.2023.1080p.mkv",
			want:     false,
		},
		{
			name:     "The.Big.Movie.S01E01 - typical TV show",
			filename: "The.Big.Movie.S01E01.mkv",
			want:     false,
		},

		// Multi-volume archive extensions are never obfuscated
		{
			name:     "partNN.rar volume - lowercase base",
			filename: "archive.part01.rar",
			want:     false,
		},
		{
			name:     "partNNN.rar volume - lowercase base",
			filename: "archive.part001.rar",
			want:     false,
		},
		{
			name:     "r00 old-style volume",
			filename: "archive.r00",
			want:     false,
		},
		{
			name:     "r01 old-style volume",
			filename: "archive.r01",
			want:     false,
		},
		{
			name:     "numeric split archive .001",
			filename: "archive.001",
			want:     false,
		},

		// Edge cases
		{
			name:     "Empty filename",
			filename: "",
			want:     false,
		},
		{
			name:     "Just extension",
			filename: ".mkv",
			want:     true, // No base filename, defaults to obfuscated
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isProbablyObfuscated(tt.filename)
			if got != tt.want {
				t.Errorf("isProbablyObfuscated(%q) = %v, want %v", tt.filename, got, tt.want)
			}
		})
	}
}

func TestSelectBestFilename(t *testing.T) {
	tests := []struct {
		name            string
		par2Filename    string
		subjectFilename string
		headerFilename  string
		subjectHeader   string
		want            string
	}{
		{
			name:            "PAR2 wins over obfuscated subject",
			par2Filename:    "Movie.Name.2023.mkv",
			subjectFilename: "b082fa0beaa644d3aa01045d5b8d0b36.mkv",
			headerFilename:  "xyz123.mkv",
			want:            "Movie.Name.2023.mkv",
		},
		{
			name:            "Subject wins when PAR2 is obfuscated",
			par2Filename:    "abc.xyz.random123.mkv",
			subjectFilename: "Good.Movie.Name.mkv",
			headerFilename:  "header.mkv",
			want:            "Good.Movie.Name.mkv",
		},
		{
			name:            "Header wins when all others empty",
			par2Filename:    "",
			subjectFilename: "",
			headerFilename:  "Final.Name.mkv",
			want:            "Final.Name.mkv",
		},
		{
			name:            "Prefer important file type",
			par2Filename:    "",
			subjectFilename: "Movie.txt",
			headerFilename:  "Movie.mkv",
			want:            "Movie.mkv",
		},
		{
			name:            "Subject header used when all others obfuscated",
			par2Filename:    "",
			subjectFilename: "b082fa0beaa644d3aa01045d5b8d0b36.mkv",
			headerFilename:  "",
			subjectHeader:   "Movie.Name.2023.1080p.BluRay",
			want:            "Movie.Name.2023.1080p.BluRay",
		},
		{
			name:            "Clear subject wins over subject header",
			par2Filename:    "",
			subjectFilename: "Good.Movie.Name.mkv",
			headerFilename:  "",
			subjectHeader:   "Movie.Name.2023.1080p.BluRay",
			want:            "Good.Movie.Name.mkv",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := selectBestFilename(tt.par2Filename, tt.subjectFilename, tt.headerFilename, tt.subjectHeader)
			if got != tt.want {
				t.Errorf("selectBestFilename() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCorrectExtensionFromMagicBytes(t *testing.T) {
	rar4Header := append([]byte(nil), Rar4Magic...)
	sevenZipHeader := append([]byte(nil), SevenZipMagic...)

	tests := []struct {
		name     string
		filename string
		data     []byte
		want     string
	}{
		{
			name:     "obfuscated .bin with RAR magic → .rar",
			filename: "b082fa0beaa644d3aa01045d5b8d0b36.bin",
			data:     rar4Header,
			want:     "b082fa0beaa644d3aa01045d5b8d0b36.rar",
		},
		{
			name:     "obfuscated .dat with 7z magic → .7z",
			filename: "0675e29e9abfd2.f7d069dab0b853283cc1b069a25f82.dat",
			data:     sevenZipHeader,
			want:     "0675e29e9abfd2.f7d069dab0b853283cc1b069a25f82.7z",
		},
		{
			name:     "clear filename not changed even with RAR magic",
			filename: "Movie.Name.2023.mkv",
			data:     rar4Header,
			want:     "Movie.Name.2023.mkv",
		},
		{
			name:     "no magic bytes — no change",
			filename: "b082fa0beaa644d3aa01045d5b8d0b36.bin",
			data:     []byte{0x00, 0x01, 0x02},
			want:     "b082fa0beaa644d3aa01045d5b8d0b36.bin",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := correctExtensionFromMagicBytes(tt.filename, tt.data)
			if got != tt.want {
				t.Errorf("correctExtensionFromMagicBytes(%q) = %q, want %q", tt.filename, got, tt.want)
			}
		})
	}
}

// TestGetFileInfo_Par2DetectionViaSubject verifies that a file whose yEnc header
// strips the .par2 extension is still correctly identified as a PAR2 archive when
// the subject filename retains the full .par2 extension.
func TestGetFileInfo_Par2DetectionViaSubject(t *testing.T) {
	tests := []struct {
		name            string
		subjectFilename string // NzbFile.Filename (from subject line)
		headerFilename  string // yEnc header name=
		wantIsPar2      bool
	}{
		{
			name:            "subject .par2 wins even when yEnc header shows .mkv",
			subjectFilename: "Movie.Name.2023.mkv.vol07+8.par2",
			headerFilename:  "Movie.Name.2023.mkv",
			wantIsPar2:      true,
		},
		{
			name:            "both subject and header are .par2",
			subjectFilename: "Movie.Name.2023.mkv.vol07+8.par2",
			headerFilename:  "Movie.Name.2023.mkv.vol07+8.par2",
			wantIsPar2:      true,
		},
		{
			name:            "neither is .par2 — not a PAR2 file",
			subjectFilename: "Movie.Name.2023.mkv",
			headerFilename:  "Movie.Name.2023.mkv",
			wantIsPar2:      false,
		},
		{
			name:            "header is .par2 and subject is not",
			subjectFilename: "Movie.Name.2023.mkv",
			headerFilename:  "Movie.Name.2023.mkv.vol07+8.par2",
			wantIsPar2:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file := &NzbFileWithFirstSegment{
				NzbFile: &nzbparser.NzbFile{
					Filename: tt.subjectFilename,
				},
				Headers: &nntppool.YEncMeta{
					FileName: tt.headerFilename,
				},
				First16KB: make([]byte, 16),
			}
			info := getFileInfo(file, nil, "nzb-stem")
			if info.IsPar2Archive != tt.wantIsPar2 {
				t.Errorf("getFileInfo IsPar2Archive = %v, want %v (subject=%q header=%q selected=%q)",
					info.IsPar2Archive, tt.wantIsPar2, tt.subjectFilename, tt.headerFilename, info.Filename)
			}
		})
	}
}

// par2DescFor builds a descriptor map keyed the same way getFileInfo computes it:
// md5 of the first 16KB zero-padded to 16384 bytes.
func par2DescFor(first16KB []byte, name string) map[[16]byte]*par2.FileDescriptor {
	padded := make([]byte, 16384)
	copy(padded, first16KB)
	key := md5.Sum(padded)
	return map[[16]byte]*par2.FileDescriptor{key: {Name: name, Hash16k: key, Length: 100}}
}

// TestGetFileInfo_Par2NameSurvivesGap5 guards the RAR-set fragmentation bug: PAR2
// recovers a real first-volume name "yay.rar", but its short lowercase base trips
// isProbablyObfuscated, so Gap 5 used to overwrite it with the NZB stem. That split
// the volume from its yay.rNN siblings and broke the whole set. PAR2 is
// authoritative, so its name must survive.
func TestGetFileInfo_Par2NameSurvivesGap5(t *testing.T) {
	content := []byte("rar volume payload")
	file := &NzbFileWithFirstSegment{
		NzbFile:   &nzbparser.NzbFile{Filename: "3fefe1fb9d2632bed19c026e37af5f92"},
		First16KB: content,
	}

	info := getFileInfo(file, par2DescFor(content, "yay.rar"), "Example.Show.S01E01.1080p.WEB-DL-GRP")
	if info.Filename != "yay.rar" {
		t.Errorf("Filename = %q, want %q (PAR2 name was clobbered by Gap 5)", info.Filename, "yay.rar")
	}
}

// TestGetFileInfo_Gap5StillFiresWithoutPar2 ensures the fix does not disable Gap 5:
// with no PAR2 match and an obfuscated subject, the NZB stem is still used.
func TestGetFileInfo_Gap5StillFiresWithoutPar2(t *testing.T) {
	file := &NzbFileWithFirstSegment{
		NzbFile:   &nzbparser.NzbFile{Filename: "b082fa0beaa644d3aa01045d5b8d0b36"},
		First16KB: make([]byte, 16),
	}

	info := getFileInfo(file, nil, "My.Show.S01E01")
	if info.Filename != "My.Show.S01E01" {
		t.Errorf("Filename = %q, want %q (Gap 5 should still substitute the NZB stem)", info.Filename, "My.Show.S01E01")
	}
}

// An NZB named after the file it carries ("The.Movie.2024.mkv.nzb") yields a stem
// that already ends in the extension; Gap 5 must not produce "The.Movie.2024.mkv.mkv".
func TestGetFileInfo_Gap5DoesNotDoubleExtension(t *testing.T) {
	file := &NzbFileWithFirstSegment{
		NzbFile:   &nzbparser.NzbFile{Filename: "b082fa0beaa644d3aa01045d5b8d0b36.mkv"},
		First16KB: make([]byte, 16),
	}

	info := getFileInfo(file, nil, "The.Movie.2024.mkv")
	if info.Filename != "The.Movie.2024.mkv" {
		t.Errorf("Filename = %q, want %q", info.Filename, "The.Movie.2024.mkv")
	}
}
