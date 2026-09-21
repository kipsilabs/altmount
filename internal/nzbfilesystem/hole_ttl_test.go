package nzbfilesystem

import (
	"bytes"
	"context"
	"testing"
	"time"

	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/kipsilabs/altmount/internal/testsupport/segments"
	"github.com/javi11/nntppool/v5"
)

// holeFile builds a plain video handle whose segment 2 is gone on the
// provider and recorded as a hole with the given stamp and fingerprint.
func holeFile(t *testing.T, fp *fakepool.Client, stamp time.Time, storedFP string) *MetadataVirtualFile {
	t.Helper()
	const n, segSize = 100, 4096
	configurePoolForFile(fp, n, segSize, fakepool.SegmentBehavior{})
	fp.SetBehavior(segments.MessageID(2), fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	mvf := newTestMVF(t, context.Background(), fp, n, segSize, 4)
	mvf.name = "movies/hole.mkv"
	mvf.meta.KnownHoles = []*metapb.HoleRun{{StartSegment: 2, Count: 1, RecordedAt: stamp.Unix()}}
	mvf.meta.HoleProviderFingerprint = storedFP
	mvf.padRecorder = newPadRecorder(&fakePadMetadataStore{holesRecorded: make(chan struct{}, 8), statusRecorded: make(chan struct{}, 8)}, nil, nil)
	t.Cleanup(mvf.padRecorder.Close)
	return mvf
}

func readWhole(t *testing.T, mvf *MetadataVirtualFile) []byte {
	t.Helper()
	buf := make([]byte, mvf.meta.FileSize)
	if _, err := mvf.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	return buf
}

// The test handle has no config, so its fingerprint is "": records stored
// under "" are the current set, anything else is foreign.
func TestFreshHoleIsPaddedWithoutAFetch(t *testing.T) {
	fp := fakepool.New()
	mvf := holeFile(t, fp, time.Now().Add(-time.Hour), "")
	got := readWhole(t, mvf)
	if !bytes.Equal(got[2*4096:3*4096], make([]byte, 4096)) {
		t.Fatal("known hole must be zero-filled")
	}
	if fp.PerMessageCalls(segments.MessageID(2)) != 0 {
		t.Fatal("a fresh hole must not be fetched")
	}
}

func TestExpiredHoleIsProbedAgain(t *testing.T) {
	fp := fakepool.New()
	mvf := holeFile(t, fp, time.Now().Add(-25*time.Hour), "")
	got := readWhole(t, mvf)
	if !bytes.Equal(got[2*4096:3*4096], make([]byte, 4096)) {
		t.Fatal("still-missing article must be padded again")
	}
	if fp.PerMessageCalls(segments.MessageID(2)) == 0 {
		t.Fatal("an expired hole must be asked for again")
	}
}

func TestHoleFromAnotherProviderSetIsProbedAgain(t *testing.T) {
	fp := fakepool.New()
	mvf := holeFile(t, fp, time.Now().Add(-time.Hour), "some-other-set")
	readWhole(t, mvf)
	if fp.PerMessageCalls(segments.MessageID(2)) == 0 {
		t.Fatal("a hole confirmed against other providers must be asked for again")
	}
}
