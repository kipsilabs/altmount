package usenet

import (
	"bytes"
	"context"
	"io"
	"sync/atomic"
	"testing"

	"github.com/kipsilabs/altmount/internal/holes"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/kipsilabs/altmount/internal/testsupport/segments"
	"github.com/javi11/nntppool/v5"
)

// A confirmed-missing segment with a patch serves the patched bytes and never
// consults OnHole (already-repaired damage is not new damage).
func TestReaderServesPatchOnConfirmedMissing(t *testing.T) {
	ctx := context.Background()
	const n, segSize = 8, 4
	fp := fillFakePool(n, segSize)
	fp.SetBehavior(segments.MessageID(3), fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})

	patch := bytes.Repeat([]byte{0xEE}, segSize)
	var onHoleCalls int32
	hooks := &HoleHooks{
		OnHole: func(segIndex int, segID string) holes.Decision {
			atomic.AddInt32(&onHoleCalls, 1)
			return holes.DecisionPad
		},
		PatchLookup: func(segID string) []byte {
			if segID == segments.MessageID(3) {
				return patch
			}
			return nil
		},
	}

	rg := buildEagerRange(ctx, t, n, segSize)
	ur := newReaderWithHooks(t, ctx, fp, rg, 60, hooks)

	got, err := io.ReadAll(ur)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got[3*segSize:4*segSize], patch) {
		t.Fatalf("segment 3 = %v, want patched bytes", got[3*segSize:4*segSize])
	}
	if calls := atomic.LoadInt32(&onHoleCalls); calls != 0 {
		t.Fatalf("OnHole called %d times for a patched segment, want 0", calls)
	}
	// Neighbors intact.
	if got[2*segSize] != 3 || got[4*segSize] != 5 {
		t.Fatal("neighbor segments corrupted")
	}
}

// A patched segment serves its patch even when the article still fetches:
// wire bytes can be corrupt-but-present (that damage is why the patch was
// built), while the patch is IFSC-verified. Repaired data takes precedence.
func TestReaderPatchTakesPrecedenceOverFetchedBytes(t *testing.T) {
	ctx := context.Background()
	const n, segSize = 8, 4
	fp := fillFakePool(n, segSize)
	// No behavior override: segment 3 fetches fine, serving (possibly corrupt)
	// wire bytes — the patch must win anyway.

	patch := bytes.Repeat([]byte{0xEE}, segSize)
	hooks := &HoleHooks{
		PatchLookup: func(segID string) []byte {
			if segID == segments.MessageID(3) {
				return patch
			}
			return nil
		},
	}

	rg := buildEagerRange(ctx, t, n, segSize)
	ur := newReaderWithHooks(t, ctx, fp, rg, 60, hooks)

	got, err := io.ReadAll(ur)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got[3*segSize:4*segSize], patch) {
		t.Fatalf("segment 3 = %v, want the patch to take precedence over fetched bytes", got[3*segSize:4*segSize])
	}
	// Neighbors intact.
	if got[2*segSize] != 3 || got[4*segSize] != 5 {
		t.Fatal("neighbor segments corrupted")
	}
}

// A known hole with a patch serves the patched bytes instead of zeros,
// without any fetch.
func TestReaderServesPatchOnKnownHole(t *testing.T) {
	ctx := context.Background()
	const n, segSize = 8, 4
	fp := fillFakePool(n, segSize)
	// No behavior override for segment 3: a fetch attempt would serve real
	// bytes, but KnownHoles must short-circuit before any fetch.

	patch := bytes.Repeat([]byte{0xAA}, segSize)
	hooks := &HoleHooks{
		KnownHoles: func(segIndex int) bool { return segIndex == 3 },
		PatchLookup: func(segID string) []byte {
			if segID == segments.MessageID(3) {
				return patch
			}
			return nil
		},
	}

	rg := buildEagerRange(ctx, t, n, segSize)
	ur := newReaderWithHooks(t, ctx, fp, rg, 60, hooks)

	got, err := io.ReadAll(ur)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got[3*segSize:4*segSize], patch) {
		t.Fatalf("segment 3 = %v, want patched bytes", got[3*segSize:4*segSize])
	}
}

// A known hole with NO patch still zero-fills (unchanged behavior).
func TestReaderKnownHoleWithoutPatchZeroFills(t *testing.T) {
	ctx := context.Background()
	const n, segSize = 8, 4
	fp := fillFakePool(n, segSize)

	hooks := &HoleHooks{
		KnownHoles:  func(segIndex int) bool { return segIndex == 3 },
		PatchLookup: func(segID string) []byte { return nil },
	}

	rg := buildEagerRange(ctx, t, n, segSize)
	ur := newReaderWithHooks(t, ctx, fp, rg, 60, hooks)

	got, err := io.ReadAll(ur)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	for j := 3 * segSize; j < 4*segSize; j++ {
		if got[j] != 0 {
			t.Fatalf("byte %d = %d, want 0", j, got[j])
		}
	}
}

// A patch whose size doesn't match the segment is ignored (falls back to
// zero-fill) rather than corrupting stream offsets.
func TestReaderIgnoresSizeMismatchedPatch(t *testing.T) {
	ctx := context.Background()
	const n, segSize = 8, 4
	fp := fillFakePool(n, segSize)

	hooks := &HoleHooks{
		KnownHoles:  func(segIndex int) bool { return segIndex == 3 },
		PatchLookup: func(segID string) []byte { return []byte{0xEE} }, // wrong size
	}

	rg := buildEagerRange(ctx, t, n, segSize)
	ur := newReaderWithHooks(t, ctx, fp, rg, 60, hooks)

	got, err := io.ReadAll(ur)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(got) != n*segSize {
		t.Fatalf("read %d bytes, want %d", len(got), n*segSize)
	}
	for j := 3 * segSize; j < 4*segSize; j++ {
		if got[j] != 0 {
			t.Fatalf("byte %d = %d, want 0 (mismatched patch must be ignored)", j, got[j])
		}
	}
}
