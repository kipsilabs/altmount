package usenet

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/javi11/nntppool/v5"
	"github.com/kipsilabs/altmount/internal/pool"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/kipsilabs/altmount/internal/testsupport/segments"
)

// transient_miss_test.go pins the contract for a mid-stream 430 that is not
// actually a missing article (issue #749): providers desync and answer "no
// such article" for a segment that is still there seconds later.

// TestTransientMiss_ReadRecovers is the issue itself: one segment 430s once,
// the article is present on a re-check, and playback must survive.
func TestTransientMiss_ReadRecovers(t *testing.T) {
	t.Parallel()
	const (
		segCount    = 4
		segSize     = 16
		maxPrefetch = 4
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fp := fakepool.New()
	for i := range segCount {
		b := fakepool.SegmentBehavior{Bytes: segments.Payload(i, segSize)}
		if i == 1 {
			// One transient 430, then the article serves normally.
			b.FailFirst = 1
			b.FailErr = nntppool.ErrArticleNotFound
		}
		fp.SetBehavior(segments.MessageID(i), b)
	}

	rg := buildEagerRange(ctx, t, segCount, segSize)
	ur := newReaderForTest(t, ctx, fp, rg, maxPrefetch)
	ur.Start()

	got, err := io.ReadAll(ur)
	if err != nil {
		t.Fatalf("read failed on a transient miss: %v", err)
	}

	var want []byte
	for i := range segCount {
		want = append(want, segments.Payload(i, segSize)...)
	}
	if string(got) != string(want) {
		t.Fatalf("payload mismatch after recovery: got %d bytes, want %d", len(got), len(want))
	}
	if n := fp.StatPriorityCalls(); n != 1 {
		t.Errorf("re-check issued %d priority STATs, want exactly 1", n)
	}
}

// TestTransientMiss_ConfirmedGoneNamesSegment pins what the health pipeline
// needs from a genuine miss: the segment and the verdict. Without SegmentID
// the downstream re-check in nzbfilesystem cannot run at all.
func TestTransientMiss_ConfirmedGoneNamesSegment(t *testing.T) {
	t.Parallel()
	const (
		segCount    = 3
		segSize     = 16
		maxPrefetch = 3
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fp := fakepool.New()
	fp.SetBehavior(segments.MessageID(0), fakepool.SegmentBehavior{Bytes: segments.Payload(0, segSize)})
	// Permanently gone: both the body fetch and the re-check say 430.
	fp.SetBehavior(segments.MessageID(1), fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	fp.SetBehavior(segments.MessageID(2), fakepool.SegmentBehavior{Bytes: segments.Payload(2, segSize)})

	rg := buildEagerRange(ctx, t, segCount, segSize)
	ur := newReaderForTest(t, ctx, fp, rg, maxPrefetch)
	ur.Start()

	_, err := io.ReadAll(ur)
	var dce *DataCorruptionError
	if !errors.As(err, &dce) {
		t.Fatalf("want DataCorruptionError, got %v", err)
	}
	if dce.SegmentID != segments.MessageID(1) {
		t.Errorf("SegmentID = %q, want %q", dce.SegmentID, segments.MessageID(1))
	}
	if dce.MissVerdict != MissConfirmed {
		t.Errorf("MissVerdict = %v, want MissConfirmed", dce.MissVerdict)
	}
	// One body attempt (never retried) plus exactly one confirming STAT.
	if n := fp.PerMessageBodyCalls(segments.MessageID(1)); n != 1 {
		t.Errorf("segment 1 issued %d body calls, want 1", n)
	}
	if n := fp.StatPriorityCalls(); n != 1 {
		t.Errorf("issued %d priority STATs, want 1", n)
	}
}

// TestTransientMiss_RecheckIsBudgeted keeps a dead release from paying a
// ~1.2s existence check per hole: one round trip per reader, not per miss.
func TestTransientMiss_RecheckIsBudgeted(t *testing.T) {
	t.Parallel()
	const (
		segCount    = 8
		segSize     = 16
		maxPrefetch = 8
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fp := fakepool.New()
	fp.SetDefaultBehavior(fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})

	rg := buildEagerRange(ctx, t, segCount, segSize)
	ur := newReaderForTest(t, ctx, fp, rg, maxPrefetch)
	ur.Start()
	_, _ = io.ReadAll(ur)
	time.Sleep(150 * time.Millisecond)

	if n := fp.StatPriorityCalls(); n > 1 {
		t.Errorf("issued %d priority STATs across %d missing segments, want at most 1", n, segCount)
	}
}

// TestTransientMiss_ImportProfileDoesNotRecheck keeps the import path on its
// own validation: it sweeps availability up front, so a re-check is pure cost.
func TestTransientMiss_ImportProfileDoesNotRecheck(t *testing.T) {
	t.Parallel()
	const (
		segCount    = 3
		segSize     = 16
		maxPrefetch = 3
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fp := fakepool.New()
	fp.SetDefaultBehavior(fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})

	rg := buildEagerRange(ctx, t, segCount, segSize)
	getter := func() (pool.NntpClient, error) { return fp, nil }
	ur, err := NewUsenetReader(ctx, getter, rg, maxPrefetch, noopMetrics{}, "import-stream", nil,
		withFlightMap(newFlightMap()), WithImportProfile(nil))
	if err != nil {
		t.Fatalf("NewUsenetReader: %v", err)
	}
	t.Cleanup(func() { _ = ur.Close() })
	ur.Start()
	_, _ = io.ReadAll(ur)
	time.Sleep(150 * time.Millisecond)

	if n := fp.StatCalls(); n != 0 {
		t.Errorf("import profile issued %d existence checks, want 0", n)
	}
}
