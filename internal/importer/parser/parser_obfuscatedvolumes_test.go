package parser

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/kipsilabs/altmount/internal/testsupport/par2gen"
	"github.com/javi11/nntppool/v5"
	"github.com/javi11/nzbparser"
)

// TestParseNzbDeobfuscatesDivergentVolumeSet is the end-to-end regression for the
// Supergirl-style failure: a multi-volume .partNN.rar set whose volumes each carry a
// distinct random base name. The per-file obfuscation heuristic treats those names as
// clean, so without the divergent-base gate (hasObfuscatedVolumeSet) the PAR2 index is
// never downloaded and the real names are never recovered — leaving every volume under a
// different base, which splits the set during grouping.
//
// With the gate, PAR2 matching runs, each volume's first-16KB hash resolves to the real
// FileDesc name, and all volumes converge on one shared base so grouping can reassemble
// the set.
func TestParseNzbDeobfuscatesDivergentVolumeSet(t *testing.T) {
	const (
		numVols  = 4
		realBase = "Real.Show.S01E01.1080p.WEB-DL"
		volSize  = 20_000 // > 16 KB so Hash16k is taken from real bytes, not zero-padding
	)
	obfBases := []string{
		"US8yidqpbbD0tHBa-Y5l_Phs8V5qb",
		"BtEPCuoFuvkpQLMHo1rs_Qp4fOtj6",
		"bY2YttkbyosUtsy",
		"DasjtQyqvxamxMrKTsW-Nw6t9",
	}

	fp := fakepool.New()
	var nzbFiles nzbparser.NzbFiles
	par2Entries := make([]par2gen.FileEntry, numVols)

	for i := range numVols {
		// Unique payload per volume → unique Hash16k → unique FileDesc match.
		content := bytes.Repeat([]byte{byte('A' + i)}, volSize)
		segID := fmt.Sprintf("vol-%d-seg0", i)
		fp.SetBehavior(segID, fakepool.SegmentBehavior{
			Bytes: content,
			YEnc:  nntppool.YEncMeta{PartSize: volSize, FileSize: volSize},
		})

		nzbFiles = append(nzbFiles, nzbparser.NzbFile{
			Filename: fmt.Sprintf("%s.part%02d.rar", obfBases[i], i+1),
			Segments: nzbparser.NzbSegments{{Bytes: volSize, Number: 1, ID: segID}},
		})

		par2Entries[i] = par2gen.FileEntry{
			Name:    fmt.Sprintf("%s.part%02d.rar", realBase, i+1),
			Content: content,
		}
	}

	// PAR2 index naming every volume with the real, shared base.
	par2Bytes := par2gen.Build(par2Entries...)
	fp.SetBehavior("par2-seg0", fakepool.SegmentBehavior{Bytes: par2Bytes})
	nzbFiles = append(nzbFiles, nzbparser.NzbFile{
		Filename: "deadbeefcafe.par2",
		Segments: nzbparser.NzbSegments{{Bytes: len(par2Bytes), Number: 1, ID: "par2-seg0"}},
	})

	pm := newFakeFullPoolManager(fp)
	p := NewParser(pm, stormConfigGetter(4))

	parsed, err := p.ParseNzb(context.Background(), &nzbparser.Nzb{Files: nzbFiles}, "test.nzb", nil, ParseOptions{})
	if err != nil {
		t.Fatalf("ParseNzb error = %v", err)
	}

	// Every RAR volume must have been renamed to the real, shared base recovered from PAR2.
	recovered := 0
	for _, f := range parsed.Files {
		if strings.HasSuffix(strings.ToLower(f.Filename), ".par2") {
			continue
		}
		if !strings.HasPrefix(f.Filename, realBase+".part") {
			t.Errorf("volume %q was not deobfuscated to the real base %q", f.Filename, realBase)
			continue
		}
		recovered++
	}
	if recovered != numVols {
		t.Fatalf("recovered %d/%d volume names from PAR2; want all volumes renamed to the shared base", recovered, numVols)
	}
}
