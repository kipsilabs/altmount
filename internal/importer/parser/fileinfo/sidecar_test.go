package fileinfo

import (
	"crypto/md5"
	"testing"

	"github.com/javi11/nntppool/v5"
	"github.com/javi11/nzbparser"
	"github.com/kipsilabs/altmount/internal/importer/parser/par2"
)

func TestGetFileInfoSubjectSidecar(t *testing.T) {
	const mediaName = "Release.Movie.2024.mkv"
	for _, ext := range []string{".nfo", ".sfv", ".txt", ".srt", ".sub", ".jpg", ".jpeg", ".png", ".nzb", ".md5", ".NFO"} {
		t.Run(ext, func(t *testing.T) {
			name := mediaName + ext
			info := getFileInfo(&NzbFileWithFirstSegment{
				NzbFile:   &nzbparser.NzbFile{Filename: name},
				Headers:   &nntppool.YEncMeta{FileName: mediaName, FileSize: 32 << 20},
				First16KB: []byte("release information"),
			}, nil, "Release.Movie.2024")
			if info.Filename != name {
				t.Errorf("Filename = %q, want %q", info.Filename, name)
			}
		})
	}
}

func TestGetFileInfoSidecarPreservesProtectedContent(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		size int64
	}{
		{"matroska", mkvFixture(), 1024},
		{"mp4", append([]byte{0, 0, 0, 0x18, 'f', 't', 'y', 'p'}, make([]byte, 16)...), 1024},
		{"mpeg-ts", mpegTSFixture(), 1024},
		{"rar4", Rar4Magic, 1024},
		{"rar5", Rar5Magic, 1024},
		{"7z", SevenZipMagic, 1024},
		{"par2", []byte("PAR2\x00PKT"), 1024},
		{"no bytes", nil, 1024},
		{"too large", []byte("unknown content"), 32<<20 + 1},
		{"unknown size", []byte("unknown content"), 0},
		{"negative size", []byte("unknown content"), -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file := &NzbFileWithFirstSegment{NzbFile: &nzbparser.NzbFile{Filename: "Release.Movie.2024.mkv.nfo"}, First16KB: tt.data}
			file.Headers = &nntppool.YEncMeta{FileName: "Release.Movie.2024.mkv", FileSize: tt.size}
			info := getFileInfo(file, nil, "Release.Movie.2024")
			if info.Filename != "Release.Movie.2024.mkv" {
				t.Errorf("Filename = %q, want media name preserved", info.Filename)
			}
		})
	}
}

func TestGetFileInfoPar2SidecarAuthoritative(t *testing.T) {
	const hashName = "a3f9c1d2e4b5a6f7c8d9e0f1a2b3c4d5.nfo"
	data := make([]byte, 16384)
	copy(data, "release information")
	for _, name := range []string{"Release.Movie.2024.mkv.nfo", hashName, "yay.nfo"} {
		t.Run(name, func(t *testing.T) {
			info := getFileInfo(&NzbFileWithFirstSegment{
				NzbFile:   &nzbparser.NzbFile{Filename: "Release.Movie.2024.mkv"},
				Headers:   &nntppool.YEncMeta{FileName: "Release.Movie.2024.mkv", FileSize: 1024},
				First16KB: data,
			}, map[[16]byte]*par2.FileDescriptor{md5.Sum(data): {Name: name, Length: 1024}}, "Release.Movie.2024")
			if info.Filename != name {
				t.Errorf("Filename = %q, want authoritative PAR2 name %q", info.Filename, name)
			}
		})
	}
}

func TestGetFileInfoSubjectSidecarNameSurvivesFallback(t *testing.T) {
	for _, name := range []string{"yay.nfo", "a3f9c1d2e4b5a6f7c8d9e0f1a2b3c4d5.nfo"} {
		t.Run(name, func(t *testing.T) {
			info := getFileInfo(&NzbFileWithFirstSegment{
				NzbFile:   &nzbparser.NzbFile{Filename: name},
				Headers:   &nntppool.YEncMeta{FileName: "Release.Movie.2024.mkv", FileSize: 1024},
				First16KB: []byte("release information"),
			}, nil, "Release.Movie.2024")
			if info.Filename != name {
				t.Errorf("Filename = %q, want original sidecar name %q", info.Filename, name)
			}
		})
	}
}

func TestGetFileInfoSubjectSidecarDoesNotOverridePar2Media(t *testing.T) {
	data := make([]byte, 16384)
	copy(data, "unrecognized media head")
	const mediaName = "Release.Movie.2024.mkv"
	info := getFileInfo(&NzbFileWithFirstSegment{
		NzbFile:   &nzbparser.NzbFile{Filename: mediaName + ".nfo"},
		Headers:   &nntppool.YEncMeta{FileName: mediaName, FileSize: 1024},
		First16KB: data,
	}, map[[16]byte]*par2.FileDescriptor{md5.Sum(data): {Name: mediaName, Length: 1024}}, "Release.Movie.2024")
	if info.Filename != mediaName {
		t.Errorf("Filename = %q, want PAR2 media name %q", info.Filename, mediaName)
	}
}
