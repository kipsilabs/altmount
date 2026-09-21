package usenet

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/kipsilabs/altmount/internal/testsupport/segments"
	"github.com/javi11/nntppool/v5"
)

// Readers built without explicit hole hooks (the import path) still serve
// repaired payloads via the process-wide default, so a repaired release can be
// imported even though the bytes exist only locally.
func TestReaderUsesDefaultPatchLookup(t *testing.T) {
	ctx := context.Background()
	const n, segSize = 6, 4
	fp := fillFakePool(n, segSize)
	fp.SetBehavior(segments.MessageID(2), fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})

	patch := bytes.Repeat([]byte{0x7F}, segSize)
	SetDefaultPatchLookup(func(segID string) []byte {
		if segID == segments.MessageID(2) {
			return patch
		}
		return nil
	})
	t.Cleanup(func() { SetDefaultPatchLookup(nil) })

	rg := buildEagerRange(ctx, t, n, segSize)
	// No WithHoleHooks: exactly how the import path builds its readers.
	ur := newReaderWithHooks(t, ctx, fp, rg, 60, nil)

	got, err := io.ReadAll(ur)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got[2*segSize:3*segSize], patch) {
		t.Fatalf("segment 2 = %v, want the repaired payload", got[2*segSize:3*segSize])
	}
}
