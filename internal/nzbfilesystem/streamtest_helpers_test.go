package nzbfilesystem

import (
	"context"
	"testing"

	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/pool"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/kipsilabs/altmount/internal/testsupport/segments"
	"github.com/javi11/nntppool/v5"
)

// streamtest_helpers_test.go provides the construction kit shared by the
// MetadataVirtualFile-level storm tests in this package. The harness is
// deliberately minimal: only the fields actually touched by the read /
// ReadAt / Seek / Close paths are populated. The full struct has many
// optional collaborators (health repo, ARRs, rclone client, repair
// coalescer, ciphers) that the streaming-storm tests do not exercise,
// and leaving them nil exercises the same nil-guards that production
// code relies on.

// fakePoolManager is a pool.Manager that always returns the supplied
// *fakepool.Client. Constructed via newFakePoolManager so tests pass the
// fake explicitly and any future changes to pool.Manager surface compile
// errors here rather than in random test files.
type fakePoolManager struct {
	client pool.NntpClient
}

var _ pool.Manager = (*fakePoolManager)(nil)

func newFakePoolManager(c pool.NntpClient) *fakePoolManager {
	return &fakePoolManager{client: c}
}

func (m *fakePoolManager) GetPool() (pool.NntpClient, error)        { return m.client, nil }
func (m *fakePoolManager) SetProviders(_ []nntppool.Provider) error { return nil }
func (m *fakePoolManager) ClearPool() error                         { return nil }
func (m *fakePoolManager) HasPool() bool                            { return true }
func (m *fakePoolManager) GetMetrics() (pool.MetricsSnapshot, error) {
	return pool.MetricsSnapshot{}, nil
}
func (m *fakePoolManager) ResetMetrics(_ context.Context, _, _ bool) error { return nil }
func (m *fakePoolManager) ResetProviderErrors(_ context.Context) error     { return nil }
func (m *fakePoolManager) IncArticlesDownloaded()                          {}
func (m *fakePoolManager) UpdateDownloadProgress(_ string, _ int64)        {}
func (m *fakePoolManager) IncArticlesPosted()                              {}
func (m *fakePoolManager) AddProvider(_ nntppool.Provider) error           { return nil }
func (m *fakePoolManager) RemoveProvider(_ string) error                   { return nil }
func (m *fakePoolManager) ResetProviderQuota(_ context.Context, _ string) error {
	return nil
}
func (m *fakePoolManager) SetProviderIDs(_ map[string]string) {}
func (m *fakePoolManager) AcquireImportSlot(_ context.Context) (func(), error) {
	return func() {}, nil
}
func (m *fakePoolManager) SetAdmissionCap(_ int) {}
func (m *fakePoolManager) AcquireImportConnection(_ context.Context) (func(), error) {
	return func() {}, nil
}
func (m *fakePoolManager) SetImportConnCapacity(_ int)                 {}
func (m *fakePoolManager) ImportConnCapacity() int                     { return 0 }
func (m *fakePoolManager) SetStreamSource(_ pool.StreamActivitySource) {}
func (m *fakePoolManager) NotifyStreamChange()                         {}
func (m *fakePoolManager) StatSweepConcurrency(conservative int) int   { return conservative }

// noopStreamTracker is a zero-state StreamTracker. The streaming-storm
// tests don't care about stream metrics; they only need a non-nil
// implementation so UsenetReader doesn't nil-deref on
// IncArticlesDownloaded / UpdateDownloadProgress.
type noopStreamTracker struct{}

func (noopStreamTracker) Add(_, _, _, _, _ string, _ int64) string { return "test-stream" }
func (noopStreamTracker) UpdateProgress(_ string, _ int64)         {}
func (noopStreamTracker) UpdateDownloadProgress(_ string, _ int64) {}
func (noopStreamTracker) UpdateCurrentOffset(_ string, _ int64)    {}
func (noopStreamTracker) UpdateBufferedOffset(_ string, _ int64)   {}
func (noopStreamTracker) Remove(_ string)                          {}
func (noopStreamTracker) IncArticlesDownloaded()                   {}
func (noopStreamTracker) IncArticlesPosted()                       {}

// buildSegmentData generates N segments of size segSize, each with the
// canonical fake message-ID and offsets that match what production code
// would see for a contiguous file of size N*segSize. The result can be
// assigned directly to fileHandleMeta.SegmentData.
func buildSegmentData(t testing.TB, n, segSize int) []*metapb.SegmentData {
	t.Helper()
	out := make([]*metapb.SegmentData, n)
	for i := 0; i < n; i++ {
		out[i] = &metapb.SegmentData{
			Id:          segments.MessageID(i),
			SegmentSize: int64(segSize),
			StartOffset: 0,
			EndOffset:   int64(segSize - 1),
		}
	}
	return out
}

func (m *fakePoolManager) SetStreamHeadroom(int)                      {}
func (m *fakePoolManager) SpeculativeBudget() *pool.SpeculativeBudget { return nil }

// configurePoolForFile teaches the fakepool how to satisfy every segment
// of a synthetic file built via buildSegmentData(n, segSize). Each
// message-ID gets the deterministic payload and any latency the caller
// supplied via the SegmentBehavior template (Bytes is overwritten).
func configurePoolForFile(fp *fakepool.Client, n, segSize int, behavior fakepool.SegmentBehavior) {
	for i := 0; i < n; i++ {
		b := behavior
		b.Bytes = segments.Payload(i, segSize)
		fp.SetBehavior(segments.MessageID(i), b)
	}
}

// newTestMVF builds a MetadataVirtualFile suitable for the streaming-
// storm tests. The file is plain (no encryption, no nested sources), of
// total size n*segSize, wired to the supplied fake pool. The maxPrefetch
// parameter mirrors production's per-reader concurrency cap.
//
// The file's metadataService, healthRepository, arrsService, ciphers, and
// rcloneClient are left nil; the read paths exercised here never touch
// them on the happy path, and leaving them nil verifies the nil-guards
// stay in place.
func newTestMVF(
	t testing.TB,
	ctx context.Context,
	fp *fakepool.Client,
	n, segSize, maxPrefetch int,
) *MetadataVirtualFile {
	t.Helper()
	segData := buildSegmentData(t, n, segSize)
	mvf := &MetadataVirtualFile{
		name: "test-virtual-file",
		meta: &fileHandleMeta{
			FileSize:    int64(n * segSize),
			SegmentData: segData,
		},
		poolManager:      newFakePoolManager(fp),
		ctx:              ctx,
		maxPrefetch:      maxPrefetch,
		originalRangeEnd: -1, // unbounded — equivalent to "no Range header"
		streamTracker:    noopStreamTracker{},
		streamID:         "test-stream",
	}
	t.Cleanup(func() { _ = mvf.Close() })
	return mvf
}
