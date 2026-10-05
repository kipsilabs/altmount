package importer

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/javi11/nntppool/v5"
	"github.com/javi11/nzbparser"
	"github.com/javi11/rardecode/v2"
	"github.com/kipsilabs/altmount/internal/importer/parser"
	"github.com/kipsilabs/altmount/internal/importer/rarname"
	"github.com/kipsilabs/altmount/internal/nzbfile"
	"github.com/kipsilabs/altmount/internal/testsupport/nzbbuild"
)

func nestedZip(t *testing.T, entries map[string][]byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	for name, data := range entries {
		f, err := w.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestImportNestedNzbDepth(t *testing.T) {
	for _, depth := range []int{2, maxNestedNzbDepth, maxNestedNzbDepth + 1} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			e := newBatteryEnv(t)
			n := nzbbuild.Build(nzbbuild.File{Subject: "Movie.mkv", Segments: e.registerContent("depth-media", bytes.Repeat([]byte("M"), 1000), 1000, 1, nil)})
			for level := range depth {
				data, err := nzbparser.Write(n)
				if err != nil {
					t.Fatal(err)
				}
				payload := nestedZip(t, map[string][]byte{"release.nzb": data})
				name := fmt.Sprintf("level%d.zip", level)
				n = nzbbuild.Build(nzbbuild.File{Subject: name, Segments: e.registerContent(name, payload, 100, 1.1, &nntppool.YEncMeta{FileName: name, FileSize: int64(len(payload))})})
			}
			_, written, err := e.runImport(n, "Nested.Release")
			if depth > maxNestedNzbDepth {
				if err == nil || !strings.Contains(err.Error(), "nesting limit") || !IsNonRetryable(err) {
					t.Fatalf("error = %v, want nesting limit", err)
				}
				if len(written) != 0 {
					t.Fatalf("unexpected writes: %v", written)
				}
			} else if err != nil || len(filePaths(written)) != 1 {
				t.Fatalf("import failed: %v; writes: %v", err, written)
			}
		})
	}
}

func TestImportNestedNzbCycle(t *testing.T) {
	e := newBatteryEnv(t)
	outer := nzbbuild.Build(nzbbuild.File{Subject: "loop.zip", Segments: []nzbbuild.Segment{{ID: "loop-p001@battery", Bytes: 1000}}})
	data, err := nzbparser.Write(outer)
	if err != nil {
		t.Fatal(err)
	}
	payload := nestedZip(t, map[string][]byte{"loop.nzb": data})
	e.registerContent("loop", payload, len(payload), 1, &nntppool.YEncMeta{FileName: "loop.zip", FileSize: int64(len(payload))})
	_, written, err := e.runImport(outer, "Loop")
	if err == nil || !strings.Contains(err.Error(), "cycle detected") || !IsNonRetryable(err) {
		t.Fatalf("error = %v, want cycle detected", err)
	}
	if len(written) != 0 {
		t.Fatalf("unexpected writes: %v", written)
	}
}

func TestNestedNzbProbePreservesObfuscatedMediaRar(t *testing.T) {
	e := newBatteryEnv(t)
	var files []nzbbuild.File
	parts, err := filepath.Glob("testdata/rar_widthmismatch/*.rar")
	if err != nil || len(parts) < 3 {
		t.Fatalf("multipart fixture missing: %v", err)
	}
	sort.Slice(parts, func(i, j int) bool {
		_, a, _ := rarname.VolumeNumber(filepath.Base(parts[i]))
		_, b, _ := rarname.VolumeNumber(filepath.Base(parts[j]))
		return a < b
	})
	for i, part := range parts {
		filename := fmt.Sprintf("%032x.part%02d.rar", i+1, i+1)
		payload, err := os.ReadFile(part)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, nzbbuild.File{Subject: filename, Segments: e.registerContent(filename, payload, archivePartSize, 1.1, &nntppool.YEncMeta{FileName: filename, FileSize: int64(len(payload))})})
	}
	_, written, err := e.runImport(nzbbuild.Build(files...), "Obfuscated.Release")
	if err != nil {
		t.Fatalf("ordinary RAR import regressed: %v", err)
	}
	if len(filePaths(written)) != 1 {
		t.Fatalf("written = %v", written)
	}
}

func TestReadNestedNzbZip(t *testing.T) {
	inner, err := nzbparser.Write(nzbbuild.Build(nzbbuild.File{Subject: "Movie.mkv", Segments: []nzbbuild.Segment{{ID: "media", Bytes: 1000}}}))
	if err != nil {
		t.Fatal(err)
	}
	var compressed bytes.Buffer
	gz := gzip.NewWriter(&compressed)
	if _, err := gz.Write(inner); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name      string
		entries   map[string][]byte
		wantError string
		wantNzb   bool
	}{
		{"gzip", map[string][]byte{"release.nzb.gz": compressed.Bytes()}, "", true},
		{"invalid", map[string][]byte{"release.nzb": []byte("not XML")}, "invalid embedded NZB", false},
		{"empty", map[string][]byte{"release.nzb": []byte("<nzb></nzb>")}, "contains no files", false},
		{"oversize", map[string][]byte{"release.nzb": bytes.Repeat([]byte("x"), maxNestedNzbSize+1)}, "size limit", false},
		{"ordinary", map[string][]byte{"Movie.mkv": []byte("media")}, "", false},
		{"mixed", map[string][]byte{"Movie.mkv": []byte("media"), "release.nzb": inner}, "", false},
		// The archive filename is read as data; it is never used as an output path.
		{"path", map[string][]byte{"../../release.nzb": inner}, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			payload := nestedZip(t, tc.entries)
			if err := os.WriteFile(filepath.Join(dir, "wrapper.zip"), payload, 0600); err != nil {
				t.Fatal(err)
			}
			data, err := readNestedNzb(context.Background(), os.DirFS(dir), []parser.ParsedFile{{Filename: "wrapper.zip", Size: int64(len(payload))}}, "")
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) || !IsNonRetryable(err) {
					t.Fatalf("error = %v, want %s", err, tc.wantError)
				}
			} else if err != nil || (data != nil) != tc.wantNzb {
				t.Fatalf("data present = %v, error = %v", data != nil, err)
			}
			if tc.wantNzb && !bytes.Equal(data, inner) {
				t.Fatal("resolved NZB changed")
			}
		})
	}
}

func TestPersistNestedNzbGzip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "release.nzb.gz")
	data := []byte("<nzb></nzb>")
	if err := persistNestedNzb(path, data); err != nil {
		t.Fatal(err)
	}
	f, err := nzbfile.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	got, err := io.ReadAll(f)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("persisted = %q, error = %v", got, err)
	}
}

type nestedTailFS struct{ dir string }

func (s nestedTailFS) Open(name string) (fs.File, error) {
	f, err := os.Open(filepath.Join(s.dir, name))
	if err != nil {
		return nil, err
	}
	if !strings.Contains(name, "part02") {
		return f, nil
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &nestedTailFile{File: f, limit: st.Size() - 8}, nil
}

type nestedTailFile struct {
	*os.File
	limit int64
}

func (f *nestedTailFile) ReadAt(p []byte, off int64) (int, error) {
	if off+int64(len(p)) > f.limit {
		return 0, nntppool.ErrArticleNotFound
	}
	return f.File.ReadAt(p, off)
}

func (f *nestedTailFile) Read(p []byte) (int, error) {
	pos, err := f.File.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	if pos+int64(len(p)) > f.limit {
		return 0, nntppool.ErrArticleNotFound
	}
	return f.File.Read(p)
}

func TestNestedNzbProbeToleratesMissingMediaRarTail(t *testing.T) {
	source := nestedTailFS{dir: "testdata/rar_multi"}
	var archives []parser.ParsedFile
	for _, name := range []string{"archive.part01.rar", "archive.part02.rar"} {
		st, err := os.Stat(filepath.Join(source.dir, name))
		if err != nil {
			t.Fatal(err)
		}
		archives = append(archives, parser.ParsedFile{Filename: name, Size: st.Size(), IsRarArchive: true})
	}
	// Ensure this fixture actually fails strict header listing.
	if _, err := rardecode.ListArchiveInfo(archives[0].Filename, rardecode.FileSystem(source)); err == nil {
		t.Fatal("fixture did not exercise missing tail")
	}
	data, err := readNestedNzb(context.Background(), source, archives, "")
	if err != nil || data != nil {
		t.Fatalf("ordinary media probe: data present = %v, error = %v", data != nil, err)
	}
}

func TestImportNestedNzb(t *testing.T) {
	for _, format := range []string{"zip", "zip-gzip", "rar", "encrypted-rar", "solid-rar", "multipart-rar"} {
		t.Run(format, func(t *testing.T) {
			e := newBatteryEnv(t)
			media := bytes.Repeat([]byte("M"), 1000)
			segments := e.registerContent("nested-media", media, len(media), 1, nil)
			inner, err := nzbparser.Write(nzbbuild.Build(nzbbuild.File{Subject: "Movie.mkv", Segments: segments}))
			if err != nil {
				t.Fatal(err)
			}
			name := "wrapper.zip"
			payload := nestedZip(t, map[string][]byte{"folder/release.NZB": inner, "release.nfo": []byte("info")})
			if strings.Contains(format, "rar") {
				name = "wrapper.rar"
				fixture := "compressed.rar"
				if format == "encrypted-rar" {
					fixture = "encrypted.rar"
				}
				if format == "solid-rar" {
					fixture = "solid.rar"
				}
				payload = loadFixture(t, "nested_nzb/"+fixture)
			}
			outer := nzbbuild.Build(nzbbuild.File{Subject: name, Segments: e.registerContent("wrapper", payload, 100, 1.1, &nntppool.YEncMeta{FileName: name, FileSize: int64(len(payload))})})
			if format == "multipart-rar" {
				var files []nzbbuild.File
				for part := 1; part <= 3; part++ {
					name := fmt.Sprintf("multi.part%d.rar", part)
					payload := loadFixture(t, "nested_nzb/"+name)
					files = append(files, nzbbuild.File{Subject: name, Segments: e.registerContent(name, payload, 100, 1.1, &nntppool.YEncMeta{FileName: name, FileSize: int64(len(payload))})})
				}
				outer = nzbbuild.Build(files...)
			}
			if format == "encrypted-rar" {
				outer.Meta = map[string]string{"password": "wrapper-secret"}
			}
			nzbPath := nzbbuild.WriteTemp(t, outer, "Original.Release")
			if format == "zip-gzip" {
				data, err := os.ReadFile(nzbPath)
				if err != nil {
					t.Fatal(err)
				}
				nzbPath += ".gz"
				if err := persistNestedNzb(nzbPath, data); err != nil {
					t.Fatal(err)
				}
			}
			category, downloadID, virtualDir := "movies", "original-download-id", "/movies/Original.Release"
			_, written, err := e.proc.ProcessNzbFile(context.Background(), nzbPath, filepath.Dir(nzbPath), 7, nil, &virtualDir, nil, &category, nil, &downloadID)
			if err != nil {
				t.Fatalf("import failed: %v", err)
			}
			paths := filePaths(written)
			if len(paths) != 1 || !strings.HasSuffix(paths[0], "/Movie.mkv") {
				t.Fatalf("written = %v", paths)
			}
			m := e.readMeta(paths[0])
			if m.FileSize != int64(len(media)) || !strings.HasSuffix(m.SourceNzbPath, "/.nzbs/movies/7-Original.Release.nzbz") || len(m.SegmentData) != 1 || m.SegmentData[0].Id != "nested-media-p001@battery" {
				t.Fatalf("incorrect metadata: %v", m)
			}
			// Retries and fallback downloads must use the release NZB, not its wrapper.
			f, err := nzbfile.Open(nzbPath)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			data, err := io.ReadAll(f)
			if err != nil {
				t.Fatal(err)
			}
			n, err := nzbparser.Parse(bytes.NewReader(data))
			if err != nil || len(n.Files) != 1 || !strings.Contains(n.Files[0].Subject, "Movie.mkv") {
				t.Fatalf("persisted NZB is not the release: %v", err)
			}
		})
	}
}

func TestImportNestedNzbRejectsAmbiguousZip(t *testing.T) {
	e := newBatteryEnv(t)
	inner, err := nzbparser.Write(nzbbuild.Build(nzbbuild.File{Subject: "Movie.mkv", Segments: []nzbbuild.Segment{{ID: "media", Bytes: 1000}}}))
	if err != nil {
		t.Fatal(err)
	}
	payload := nestedZip(t, map[string][]byte{"one.nzb": inner, "two.nzb": inner})
	outer := nzbbuild.Build(nzbbuild.File{Subject: "wrapper.zip", Segments: e.registerContent("ambiguous", payload, 100, 1, &nntppool.YEncMeta{FileName: "wrapper.zip", FileSize: int64(len(payload))})})
	_, written, err := e.runImport(outer, "Ambiguous")
	if err == nil || !strings.Contains(err.Error(), "multiple NZB") {
		t.Fatalf("error = %v, want multiple NZBs", err)
	}
	if len(written) != 0 {
		t.Fatalf("unexpected writes: %v", written)
	}
}
