package parser

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/pool"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/kipsilabs/altmount/internal/testsupport/segments"
	"github.com/javi11/nntppool/v5"
	"github.com/javi11/nzbparser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func testConfigGetter() config.ConfigGetter {
	cfg := config.DefaultConfig()
	return func() *config.Config { return cfg }
}

type mockPoolManager struct {
	mock.Mock
	pool.Manager
}

func (m *mockPoolManager) HasPool() bool {
	args := m.Called()
	return args.Bool(0)
}

func TestParseFile_EmptySegments(t *testing.T) {
	m := &mockPoolManager{}
	p := NewParser(m, testConfigGetter())

	nzbXML := `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE nzb PUBLIC "-//newzBin//DTD NZB 1.1//EN" "http://www.newzbin.com/DTD/nzb-1.1.dtd">
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
 <file poster="poster" date="123456789" subject="test file">
  <groups>
   <group>alt.binaries.test</group>
  </groups>
  <segments>
  </segments>
 </file>
</nzb>`

	r := strings.NewReader(nzbXML)

	// We expect fetchAllFirstSegments to be called, which will return MissingFirstSegment for files with no segments.
	// Then it will fall back to fallbackGetFileInfos.
	m.On("HasPool").Return(false)

	parsed, err := p.ParseFile(context.Background(), r, "test.nzb", nil)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "NZB file contains no valid files")
	assert.Nil(t, parsed)
}

func TestParseFile_MixedSegments(t *testing.T) {
	m := &mockPoolManager{}
	p := NewParser(m, testConfigGetter())

	nzbXML := `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE nzb PUBLIC "-//newzBin//DTD NZB 1.1//EN" "http://www.newzbin.com/DTD/nzb-1.1.dtd">
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
 <file poster="poster" date="123456789" subject="file with segments">
  <groups>
   <group>alt.binaries.test</group>
  </groups>
  <segments>
   <segment bytes="100" number="1">seg1</segment>
  </segments>
 </file>
 <file poster="poster" date="123456789" subject="file without segments">
  <groups>
   <group>alt.binaries.test</group>
  </groups>
  <segments>
  </segments>
 </file>
</nzb>`

	r := strings.NewReader(nzbXML)

	// HasPool returns false to trigger fallback to fallbackGetFileInfos
	m.On("HasPool").Return(false)

	parsed, err := p.ParseFile(context.Background(), r, "test.nzb", nil)

	assert.NoError(t, err)
	assert.NotNil(t, parsed)
	assert.Len(t, parsed.Files, 1)
	assert.Equal(t, "file with segments", parsed.Files[0].Filename)
}

func TestFallbackGetFileInfos_EmptySegments(t *testing.T) {
	p := NewParser(nil, testConfigGetter())

	files := []nzbparser.NzbFile{
		{
			Filename: "file1.txt",
			Segments: []nzbparser.NzbSegment{},
		},
		{
			Filename: "file2.txt",
			Segments: []nzbparser.NzbSegment{
				{ID: "seg1", Bytes: 100},
			},
		},
	}

	infos := p.fallbackGetFileInfos(files)

	assert.Len(t, infos, 1)
	assert.Equal(t, "file2.txt", infos[0].Filename)
}

// TestDetermineNzbType_ExcludesPar2Files verifies that PAR2 recovery files
// are excluded when determining NZB type, so 1 media + N PAR2 = SingleFile.
func TestDetermineNzbType_ExcludesPar2Files(t *testing.T) {
	p := NewParser(nil, testConfigGetter())

	tests := []struct {
		name     string
		files    []ParsedFile
		wantType NzbType
	}{
		{
			name: "single media file + par2 files → SingleFile",
			files: []ParsedFile{
				{Filename: "Movie.Name.2023.mkv", IsPar2Archive: false},
				{Filename: "Movie.Name.2023.mkv.vol00+1.par2", IsPar2Archive: true},
				{Filename: "Movie.Name.2023.mkv.vol01+2.par2", IsPar2Archive: true},
				{Filename: "Movie.Name.2023.mkv.vol03+4.par2", IsPar2Archive: true},
			},
			wantType: NzbTypeSingleFile,
		},
		{
			name: "multiple media files → MultiFile",
			files: []ParsedFile{
				{Filename: "Movie.Part1.mkv", IsPar2Archive: false},
				{Filename: "Movie.Part2.mkv", IsPar2Archive: false},
				{Filename: "Movie.Part1.mkv.vol00+1.par2", IsPar2Archive: true},
			},
			wantType: NzbTypeMultiFile,
		},
		{
			name: "par2 IsPar2Archive=false but filename ends in .par2",
			files: []ParsedFile{
				{Filename: "Movie.Name.2023.mkv", IsPar2Archive: false},
				{Filename: "Movie.Name.2023.mkv.vol07+8.par2", IsPar2Archive: false},
			},
			wantType: NzbTypeSingleFile,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := p.determineNzbType(tt.files)
			assert.Equal(t, tt.wantType, got)
		})
	}
}

// TestFetchAllFirstSegments_MissingSegmentEmitsDebugLog verifies that a
// "missing segment" debug log is emitted when Body() returns ErrArticleNotFound
// while fetching the first segment of a file.
// NOT parallel: we inject p.log directly.
func TestFetchAllFirstSegments_MissingSegmentEmitsDebugLog(t *testing.T) {
	segID := segments.MessageID(0)

	var mu sync.Mutex
	type logRecord struct{ msg, segID string }
	var captured []logRecord

	handler := &parserCaptureLogHandler{
		onHandle: func(r slog.Record) {
			var sid string
			r.Attrs(func(a slog.Attr) bool {
				if a.Key == "segment_id" {
					sid = a.Value.String()
				}
				return true
			})
			mu.Lock()
			captured = append(captured, logRecord{msg: r.Message, segID: sid})
			mu.Unlock()
		},
	}

	fp := fakepool.New()
	fp.SetBehavior(segID, fakepool.SegmentBehavior{
		Err: nntppool.ErrArticleNotFound,
	})

	mgr := newFakeFullPoolManager(fp)
	p := NewParser(mgr, testConfigGetter())
	p.log = slog.New(handler)

	nzbXML := `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE nzb PUBLIC "-//newzBin//DTD NZB 1.1//EN" "http://www.newzbin.com/DTD/nzb-1.1.dtd">
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
 <file poster="poster" date="123456789" subject="` + segID + `">
  <groups><group>alt.binaries.test</group></groups>
  <segments>
   <segment bytes="100" number="1">` + segID + `</segment>
  </segments>
 </file>
</nzb>`

	_, _ = p.ParseFile(context.Background(), strings.NewReader(nzbXML), "test.nzb", nil)

	mu.Lock()
	defer mu.Unlock()
	found := false
	for _, r := range captured {
		if r.msg == "missing segment" && r.segID == segID {
			found = true
		}
	}
	assert.True(t, found, "expected 'missing segment' debug log for segment_id=%q, got: %+v", segID, captured)
}

// parserCaptureLogHandler is a minimal slog.Handler for parser tests.
type parserCaptureLogHandler struct {
	onHandle func(slog.Record)
	preAttrs []slog.Attr
}

func (h *parserCaptureLogHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }
func (h *parserCaptureLogHandler) Handle(_ context.Context, r slog.Record) error {
	full := r.Clone()
	for _, a := range h.preAttrs {
		full.AddAttrs(a)
	}
	h.onHandle(full)
	return nil
}
func (h *parserCaptureLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	c := *h
	c.preAttrs = append(append([]slog.Attr{}, h.preAttrs...), attrs...)
	return &c
}
func (h *parserCaptureLogHandler) WithGroup(_ string) slog.Handler { return h }

// TestPropagateArchiveType_SkipsTxtSidecar is the regression test for
// Fresh.Off.the.Boat.S01E12 where a .txt sidecar was incorrectly marked
// IsRarArchive=true by the archive-type propagation loop.
//
// Post-PAR2 state modelled here: all RAR volumes already have real names and
// IsRarArchive=true; the .txt sidecar has IsRarArchive=false and must not be
// touched by propagation.
func TestPropagateArchiveType_SkipsTxtSidecar(t *testing.T) {
	release := "Fresh.Off.the.Boat.S01E12.Dribbling.Tiger.Bounce.Pass.Dragon.1080p.DSNP.WEB-DL.DD5.1.H.264-playWEB"
	parsed := &ParsedNzb{
		Type: NzbTypeRarArchive,
		Files: []ParsedFile{
			{Filename: release + ".part01.rar", IsRarArchive: true},
			{Filename: release + ".part02.rar", IsRarArchive: true},
			{Filename: release + ".part03.rar", IsRarArchive: true},
			{Filename: "5a3ae665828fe76b0bb904e41d4d2429.txt", IsRarArchive: false},
			{Filename: "5a3ae665828fe76b0bb904e41d4d2429.par2", IsPar2Archive: true},
		},
	}

	p := &Parser{}
	p.propagateArchiveType(parsed)

	for _, f := range parsed.Files {
		switch {
		case strings.HasSuffix(f.Filename, ".rar"):
			assert.True(t, f.IsRarArchive, "%s must stay IsRarArchive=true", f.Filename)
		case strings.HasSuffix(f.Filename, ".txt"):
			assert.False(t, f.IsRarArchive, ".txt sidecar must NOT be marked IsRarArchive=true")
		case strings.HasSuffix(f.Filename, ".par2"):
			assert.False(t, f.IsRarArchive, "PAR2 file must NOT be marked IsRarArchive=true")
		}
	}
}

func TestExtractNzbPassword(t *testing.T) {
	// Case 1: n.Meta with password
	n1 := &nzbparser.Nzb{
		Meta: map[string]string{
			"password": "secret_password_1",
		},
	}
	assert.Equal(t, "secret_password_1", extractNzbPassword(n1, "movie.nzb"))

	// Case 2: n.Meta with uppercase Password
	n2 := &nzbparser.Nzb{
		Meta: map[string]string{
			"Password": "secret_password_2",
		},
	}
	assert.Equal(t, "secret_password_2", extractNzbPassword(n2, "movie.nzb"))

	// Case 3: Filename {{password}}
	n3 := &nzbparser.Nzb{}
	assert.Equal(t, "secret_in_name", extractNzbPassword(n3, "Movie.Title.2024.1080p{{secret_in_name}}.nzb"))

	// Case 4: Subject {{password}}
	n4 := &nzbparser.Nzb{
		Files: []nzbparser.NzbFile{
			{Subject: "[1234/5678] - \"movie.part01.rar\" yEnc (1/100) {{subject_pass}}"},
		},
	}
	assert.Equal(t, "subject_pass", extractNzbPassword(n4, "movie.nzb"))

	// Case 5: No password
	n5 := &nzbparser.Nzb{
		Files: []nzbparser.NzbFile{
			{Subject: "[1234/5678] - \"movie.part01.rar\" yEnc (1/100)"},
		},
	}
	assert.Equal(t, "", extractNzbPassword(n5, "movie.nzb"))
}

func TestPropagateArchiveType_NumericExtensions(t *testing.T) {
	parsed := &ParsedNzb{
		Type: NzbTypeRarArchive,
		Files: []ParsedFile{
			{Filename: "b082fa0beaa644d3aa01045d5b8d0b36.1", IsRarArchive: true},
			{Filename: "b082fa0beaa644d3aa01045d5b8d0b36.2", IsRarArchive: false},
			{Filename: "b082fa0beaa644d3aa01045d5b8d0b36.193", IsRarArchive: false},
			{Filename: "b082fa0beaa644d3aa01045d5b8d0b36.nfo", IsRarArchive: false},
			{Filename: "b082fa0beaa644d3aa01045d5b8d0b36.par2", IsPar2Archive: true},
		},
	}

	p := &Parser{}
	p.propagateArchiveType(parsed)

	assert.True(t, parsed.Files[0].IsRarArchive, ".1 must be IsRarArchive=true")
	assert.True(t, parsed.Files[1].IsRarArchive, ".2 must be propagated to IsRarArchive=true")
	assert.True(t, parsed.Files[2].IsRarArchive, ".193 must be propagated to IsRarArchive=true")
	assert.False(t, parsed.Files[3].IsRarArchive, ".nfo must NOT be IsRarArchive")
	assert.False(t, parsed.Files[4].IsRarArchive, ".par2 must NOT be IsRarArchive")
}
