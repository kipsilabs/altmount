package importer

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/metadata"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/kipsilabs/altmount/internal/testsupport/nzbbuild"
	"github.com/javi11/nntppool/v5"
	"github.com/javi11/nzbparser"
)

// batteryEnv holds the full test environment for an import battery test.
type batteryEnv struct {
	t         *testing.T
	client    *fakepool.Client
	svc       *metadata.MetadataService
	cfg       *config.Config
	proc      *Processor
	metaRoot  string
	configDir string // temp dir used as the config directory (Database.Path = configDir/altmount.db)
}

// newBatteryEnv creates a fresh test environment backed by an in-memory fakepool.
// SegmentSamplePercentage is set to 100 so fast-fail checks every segment.
// ".bin" is added to AllowedFileExtensions so archive fixture inner files pass the filter.
func newBatteryEnv(t *testing.T) *batteryEnv {
	t.Helper()
	client := fakepool.New()
	metaRoot := t.TempDir()
	configDir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.Database.Path = filepath.Join(configDir, "altmount.db")
	cfg.Import.SegmentSamplePercentage = 100
	cfg.Import.AllowedFileExtensions = append(cfg.Import.AllowedFileExtensions, ".bin")
	svc := metadata.NewMetadataService(metaRoot)
	proc := NewProcessor(svc, processorTestPoolManager{client: client}, nil, func() *config.Config { return cfg }, nil)
	return &batteryEnv{t: t, client: client, svc: svc, cfg: cfg, proc: proc, metaRoot: metaRoot, configDir: configDir}
}

// rawMetaPath returns the on-disk path to the .meta file for virtualPath.
// Mirrors the logic in MetadataService.WriteFileMetadata: base filename + ".meta".
func (e *batteryEnv) rawMetaPath(virtualPath string) string {
	return filepath.Join(e.metaRoot, filepath.Dir(virtualPath), filepath.Base(virtualPath)+".meta")
}

// registerContent slices content into chunks of at most partSize bytes, registers
// each chunk as a fakepool behavior keyed by a deterministic message-ID derived from
// idPrefix, and returns the nzbbuild.Segment slice for building the NZB.
//
// declaredOverhead > 1.0 inflates the NZB-declared segment size to simulate yEnc
// encoding overhead. yEncTemplate, if non-nil, provides the base YEncMeta; per-segment
// fields (Part, PartBegin, PartSize) are filled in automatically.
func (e *batteryEnv) registerContent(
	idPrefix string,
	content []byte,
	partSize int,
	declaredOverhead float64,
	yEncTemplate *nntppool.YEncMeta,
) []nzbbuild.Segment {
	e.t.Helper()
	total := len(content)
	var segs []nzbbuild.Segment
	for idx, off := 0, 0; off < total; idx, off = idx+1, off+partSize {
		end := off + partSize
		if end > total {
			end = total
		}
		chunk := content[off:end]
		msgID := fmt.Sprintf("%s-p%03d@battery", idPrefix, idx+1)

		yenc := nntppool.YEncMeta{}
		if yEncTemplate != nil {
			yenc = *yEncTemplate
			yenc.Part = int64(idx + 1)
			yenc.PartBegin = int64(off + 1) // yEnc uses 1-based offsets
			yenc.PartSize = int64(len(chunk))
		}
		e.client.SetBehavior(msgID, fakepool.SegmentBehavior{Bytes: chunk, YEnc: yenc})

		declared := len(chunk)
		if declaredOverhead > 1.0 {
			declared = int(float64(declared) * declaredOverhead)
		}
		segs = append(segs, nzbbuild.Segment{ID: msgID, Bytes: declared})
	}
	return segs
}

// runImport writes nzb to a temp .nzb file named "<name>.nzb" and runs ProcessNzbFile.
// The relativePath is set to the temp dir so the virtual directory resolves to "/".
func (e *batteryEnv) runImport(nzb *nzbparser.Nzb, name string) (string, []string, error) {
	e.t.Helper()
	nzbPath := nzbbuild.WriteTemp(e.t, nzb, name)
	return e.proc.ProcessNzbFile(
		context.Background(),
		nzbPath, filepath.Dir(nzbPath),
		1, nil, nil, nil, nil, nil, nil,
	)
}

// runImportWithCategory is like runImport but passes queueID and category.
func (e *batteryEnv) runImportWithCategory(nzb *nzbparser.Nzb, name string, queueID int, category string) (string, []string, error) {
	e.t.Helper()
	nzbPath := nzbbuild.WriteTemp(e.t, nzb, name)
	return e.proc.ProcessNzbFile(
		context.Background(),
		nzbPath, filepath.Dir(nzbPath),
		queueID, nil, nil, nil, &category, nil, nil,
	)
}

// readMeta reads and returns FileMetadata for a virtual path, fatal on error.
func (e *batteryEnv) readMeta(virtualPath string) *metapb.FileMetadata {
	e.t.Helper()
	m, err := e.svc.ReadFileMetadata(virtualPath)
	if err != nil {
		e.t.Fatalf("readMeta(%q): %v", virtualPath, err)
	}
	return m
}

// listDir returns the virtual filenames inside a virtual directory, fatal on error.
func (e *batteryEnv) listDir(virtualPath string) []string {
	e.t.Helper()
	files, err := e.svc.ListDirectory(virtualPath)
	if err != nil {
		e.t.Fatalf("listDir(%q): %v", virtualPath, err)
	}
	return files
}

// filePaths returns writtenPaths with "DIR:" cleanup markers removed.
func filePaths(writtenPaths []string) []string {
	var out []string
	for _, p := range writtenPaths {
		if !strings.HasPrefix(p, "DIR:") {
			out = append(out, p)
		}
	}
	return out
}

// fixtureEntry is one entry from testdata/manifest.json.
type fixtureEntry struct {
	Name string `json:"name"`
	Size int    `json:"size"`
}

// loadManifest reads the manifest for the given fixture key.
// Calls t.Skip when the key is absent (e.g., rar_oldstyle not generated on this host).
func loadManifest(t *testing.T, key string) []fixtureEntry {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "manifest.json"))
	if err != nil {
		t.Fatalf("loadManifest: %v", err)
	}
	var m map[string][]fixtureEntry
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("loadManifest: unmarshal: %v", err)
	}
	entries, ok := m[key]
	if !ok || len(entries) == 0 {
		t.Skipf("fixture %q absent from manifest — run testdata/gen_fixtures.sh to generate it", key)
	}
	return entries
}

// loadFixture reads a file from testdata/.
func loadFixture(t *testing.T, rel string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", rel))
	if err != nil {
		t.Fatalf("loadFixture(%q): %v", rel, err)
	}
	return data
}
