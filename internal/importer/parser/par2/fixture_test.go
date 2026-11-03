package par2_test

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/kipsilabs/altmount/internal/importer/parser/par2"
	"github.com/kipsilabs/altmount/internal/par2repair"
)

// The realtool fixture was produced by par2cmdline (par2 create -s1024 -c4
// -n1) over two deterministic input files. It pins our parser and solver to a
// real, external PAR2 implementation rather than our own generator.
const realToolDir = "testdata/realtool"

// loadRealToolSet reads the fixture .par2 files (index first, volumes after,
// sorted by filename) and returns the parsed index plus the raw bytes of each
// stream in the exact order they were handed to ParseIndex, so that
// RecoverySliceRef.FileIndex can be used to index into them.
func loadRealToolSet(t *testing.T) (*par2.Index, [][]byte) {
	t.Helper()

	entries, err := filepath.Glob(filepath.Join(realToolDir, "*.par2"))
	if err != nil {
		t.Fatalf("glob fixture: %v", err)
	}
	if len(entries) < 2 {
		t.Fatalf("expected at least an index and one volume file, got %v", entries)
	}
	sort.Strings(entries) // "realtool.par2" sorts before "realtool.vol0+4.par2"

	raw := make([][]byte, len(entries))
	streams := make([]io.Reader, len(entries))
	for i, name := range entries {
		b, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		raw[i] = b
		streams[i] = bytes.NewReader(b)
	}

	idx, err := par2.ParseIndex(streams)
	if err != nil {
		t.Fatalf("ParseIndex: %v", err)
	}
	return idx, raw
}

// loadInput reads one of the fixture's original input files.
func loadInput(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(realToolDir, name))
	if err != nil {
		t.Fatalf("read input %s: %v", name, err)
	}
	return b
}

func TestParseRealToolFixture(t *testing.T) {
	idx, _ := loadRealToolSet(t)

	if idx.SliceSize != 1024 {
		t.Errorf("SliceSize = %d, want 1024", idx.SliceSize)
	}
	if len(idx.RecoveryIDs) != 2 {
		t.Fatalf("recovery-set members = %d, want 2", len(idx.RecoveryIDs))
	}

	wantFiles := map[string]uint64{
		"fileA.bin": 8000,
		"fileB.bin": 5600,
	}
	for _, id := range idx.RecoveryIDs {
		fd, ok := idx.Files[id]
		if !ok {
			t.Fatalf("recovery-set member %x has no FileDesc", id)
		}
		wantLen, ok := wantFiles[fd.Name]
		if !ok {
			t.Fatalf("unexpected recovery-set file %q", fd.Name)
		}
		if fd.Length != wantLen {
			t.Errorf("file %q length = %d, want %d", fd.Name, fd.Length, wantLen)
		}
		delete(wantFiles, fd.Name)

		wantSlices := int((fd.Length + idx.SliceSize - 1) / idx.SliceSize)
		if got := len(idx.SliceChecks[id]); got != wantSlices {
			t.Errorf("file %q IFSC slice count = %d, want %d", fd.Name, got, wantSlices)
		}
	}
	if len(wantFiles) != 0 {
		t.Errorf("recovery set missing files: %v", wantFiles)
	}

	if len(idx.Recovery) != 4 {
		t.Fatalf("recovery slices = %d, want 4", len(idx.Recovery))
	}
	seen := make(map[uint32]bool)
	for _, ref := range idx.Recovery {
		if seen[ref.Exponent] {
			t.Errorf("duplicate recovery exponent %d", ref.Exponent)
		}
		seen[ref.Exponent] = true
	}
}

// TestSolveAgainstRealToolRecovery proves our GF(2^16) math matches
// par2cmdline: it reconstructs a "missing" input slice using only a recovery
// payload emitted by the real tool plus the remaining present slices, and
// byte-compares the result against the true slice content.
func TestSolveAgainstRealToolRecovery(t *testing.T) {
	idx, raw := loadRealToolSet(t)
	sliceSize := int(idx.SliceSize)

	// Build the global slice array in recovery-set order (RecoveryIDs keeps
	// the Main packet's order, which is the PAR2 global slice numbering).
	var slices [][]byte
	for _, id := range idx.RecoveryIDs {
		fd := idx.Files[id]
		content := loadInput(t, fd.Name)
		if uint64(len(content)) != fd.Length {
			t.Fatalf("input %q is %d bytes, FileDesc says %d", fd.Name, len(content), fd.Length)
		}
		for off := 0; off < len(content); off += sliceSize {
			s := make([]byte, sliceSize) // zero-padded
			copy(s, content[off:])
			slices = append(slices, s)
		}
	}

	const missingIdx = 5 // arbitrary slice to reconstruct

	// Extract one recovery payload straight from the real tool's file bytes.
	ref := idx.Recovery[0]
	stream := raw[ref.FileIndex]
	if int64(len(stream)) < ref.BodyOffset+int64(sliceSize) {
		t.Fatalf("recovery payload out of bounds: stream %d has %d bytes, need offset %d + %d",
			ref.FileIndex, len(stream), ref.BodyOffset, sliceSize)
	}
	payload := stream[ref.BodyOffset : ref.BodyOffset+int64(sliceSize)]

	solver, err := par2repair.NewSolver([]int{missingIdx}, []uint32{ref.Exponent}, sliceSize)
	if err != nil {
		t.Fatalf("NewSolver: %v", err)
	}
	solver.AddRecovery(0, payload)
	for i, s := range slices {
		if i == missingIdx {
			continue
		}
		solver.FoldPresent(i, s)
	}

	out, err := solver.Solve()
	if err != nil {
		t.Fatalf("Solve: %v", err)
	}
	if !bytes.Equal(out[0], slices[missingIdx]) {
		t.Fatalf("recovered slice %d does not match real content:\n got %x...\nwant %x...",
			missingIdx, out[0][:32], slices[missingIdx][:32])
	}
}
