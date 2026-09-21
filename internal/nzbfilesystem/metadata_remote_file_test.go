package nzbfilesystem

import (
	"context"
	"io"
	"sync"
	"testing"

	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/pool"
	"github.com/javi11/nntppool/v5"
)

// createTestVirtualFile creates a MetadataVirtualFile with default configuration for testing
func createTestVirtualFile(fileSize int64) *MetadataVirtualFile {
	return &MetadataVirtualFile{
		meta: &fileHandleMeta{
			FileSize: fileSize,
		},
	}
}

func TestCreateTestVirtualFile(t *testing.T) {
	fileSize := int64(100 * 1024 * 1024) // 100MB
	mvf := createTestVirtualFile(fileSize)

	if mvf.meta.FileSize != fileSize {
		t.Errorf("createTestVirtualFile() fileSize = %d, want %d", mvf.meta.FileSize, fileSize)
	}

	if mvf.meta == nil {
		t.Error("createTestVirtualFile() meta should not be nil")
	}
}

func TestBasicRangeCalculation(t *testing.T) {
	fileSize := int64(100 * 1024 * 1024) // 100MB

	tests := []struct {
		name       string
		start      int64
		end        int64
		expectErr  bool
		shouldPass bool
	}{
		{
			name:       "valid range within file",
			start:      0,
			end:        1024,
			expectErr:  false,
			shouldPass: true,
		},
		{
			name:       "range at file end",
			start:      fileSize - 1024,
			end:        fileSize - 1,
			expectErr:  false,
			shouldPass: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test basic range validation
			if tt.start < 0 || tt.end >= fileSize || tt.start > tt.end {
				if tt.shouldPass {
					t.Errorf("Test %s: invalid range [%d, %d] for file size %d", tt.name, tt.start, tt.end, fileSize)
				}
			} else {
				if !tt.shouldPass {
					t.Errorf("Test %s: expected invalid range but got valid [%d, %d]", tt.name, tt.start, tt.end)
				}
			}
		})
	}
}

// TestBuildSegmentIndex tests the segment offset index building
func TestBuildSegmentIndex(t *testing.T) {
	tests := []struct {
		name     string
		segments []*metapb.SegmentData
		wantNil  bool
	}{
		{
			name:     "nil segments",
			segments: nil,
			wantNil:  true,
		},
		{
			name:     "empty segments",
			segments: []*metapb.SegmentData{},
			wantNil:  true,
		},
		{
			name: "single segment",
			segments: []*metapb.SegmentData{
				{StartOffset: 0, EndOffset: 999, SegmentSize: 1000},
			},
			wantNil: false,
		},
		{
			name: "multiple segments",
			segments: []*metapb.SegmentData{
				{StartOffset: 0, EndOffset: 999, SegmentSize: 1000},
				{StartOffset: 0, EndOffset: 999, SegmentSize: 1000},
				{StartOffset: 0, EndOffset: 499, SegmentSize: 500},
			},
			wantNil: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := buildSegmentIndex(tt.segments)
			if tt.wantNil {
				if idx != nil {
					t.Errorf("buildSegmentIndex() = %v, want nil", idx)
				}
			} else {
				if idx == nil {
					t.Errorf("buildSegmentIndex() = nil, want non-nil")
				} else if len(idx.offsets) != len(tt.segments) {
					t.Errorf("buildSegmentIndex() offsets len = %d, want %d", len(idx.offsets), len(tt.segments))
				}
			}
		})
	}
}

// TestSegmentOffsetIndexFindSegment tests O(1) segment lookup
func TestSegmentOffsetIndexFindSegment(t *testing.T) {
	// Create an index with 3 segments: [0-999], [1000-1999], [2000-2499]
	segments := []*metapb.SegmentData{
		{StartOffset: 0, EndOffset: 999, SegmentSize: 1000}, // usable: 1000 bytes
		{StartOffset: 0, EndOffset: 999, SegmentSize: 1000}, // usable: 1000 bytes
		{StartOffset: 0, EndOffset: 499, SegmentSize: 500},  // usable: 500 bytes
	}
	idx := buildSegmentIndex(segments)

	tests := []struct {
		name   string
		offset int64
		want   int
	}{
		{"start of first segment", 0, 0},
		{"middle of first segment", 500, 0},
		{"end of first segment", 999, 0},
		{"start of second segment", 1000, 1},
		{"middle of second segment", 1500, 1},
		{"end of second segment", 1999, 1},
		{"start of third segment", 2000, 2},
		{"end of third segment", 2499, 2},
		{"negative offset", -1, -1},
		{"beyond end", 2500, -1},
		{"way beyond end", 10000, -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := idx.findSegmentForOffset(tt.offset)
			if got != tt.want {
				t.Errorf("findSegmentForOffset(%d) = %d, want %d", tt.offset, got, tt.want)
			}
		})
	}
}

// TestSegmentOffsetIndexNil tests nil index safety
func TestSegmentOffsetIndexNil(t *testing.T) {
	var idx *segmentOffsetIndex

	got := idx.findSegmentForOffset(100)
	if got != -1 {
		t.Errorf("nil index findSegmentForOffset() = %d, want -1", got)
	}

	// Also test getOffsetForSegment on nil index
	offset := idx.getOffsetForSegment(0)
	if offset != 0 {
		t.Errorf("nil index getOffsetForSegment() = %d, want 0", offset)
	}
}

// TestGetOffsetForSegment tests the getOffsetForSegment method
func TestGetOffsetForSegment(t *testing.T) {
	// Create an index with 3 segments: [0-999], [1000-1999], [2000-2499]
	segments := []*metapb.SegmentData{
		{StartOffset: 0, EndOffset: 999, SegmentSize: 1000}, // usable: 1000 bytes, offset: 0
		{StartOffset: 0, EndOffset: 999, SegmentSize: 1000}, // usable: 1000 bytes, offset: 1000
		{StartOffset: 0, EndOffset: 499, SegmentSize: 500},  // usable: 500 bytes, offset: 2000
	}
	idx := buildSegmentIndex(segments)

	tests := []struct {
		name         string
		segmentIndex int
		wantOffset   int64
	}{
		{"first segment", 0, 0},
		{"second segment", 1, 1000},
		{"third segment", 2, 2000},
		{"negative index", -1, 0},
		{"out of bounds", 3, 0},
		{"way out of bounds", 100, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := idx.getOffsetForSegment(tt.segmentIndex)
			if got != tt.wantOffset {
				t.Errorf("getOffsetForSegment(%d) = %d, want %d", tt.segmentIndex, got, tt.wantOffset)
			}
		})
	}
}

// TestSegmentIndexIntegration tests that findSegmentForOffset and getOffsetForSegment work together
func TestSegmentIndexIntegration(t *testing.T) {
	// Create a realistic segment index with varying segment sizes
	segments := []*metapb.SegmentData{
		{StartOffset: 0, EndOffset: 749999, SegmentSize: 750000}, // 750KB
		{StartOffset: 0, EndOffset: 749999, SegmentSize: 750000}, // 750KB
		{StartOffset: 0, EndOffset: 749999, SegmentSize: 750000}, // 750KB
		{StartOffset: 0, EndOffset: 749999, SegmentSize: 750000}, // 750KB
		{StartOffset: 0, EndOffset: 249999, SegmentSize: 250000}, // 250KB (final partial)
	}
	idx := buildSegmentIndex(segments)

	// Test that findSegmentForOffset and getOffsetForSegment are consistent
	testOffsets := []int64{0, 375000, 750000, 1500000, 2250000, 2750000, 3000000}

	for _, offset := range testOffsets {
		segIdx := idx.findSegmentForOffset(offset)
		if segIdx < 0 {
			continue // Skip offsets beyond the file
		}

		segOffset := idx.getOffsetForSegment(segIdx)

		// The segment's start offset should be <= the query offset
		if segOffset > offset {
			t.Errorf("offset %d: segment %d starts at %d which is after the query offset",
				offset, segIdx, segOffset)
		}

		// The segment should contain this offset (check upper bound)
		if segIdx < len(segments) {
			usableLen := segments[segIdx].EndOffset - segments[segIdx].StartOffset + 1
			segEnd := segOffset + usableLen - 1
			if offset > segEnd {
				t.Errorf("offset %d: segment %d ends at %d which is before the query offset",
					offset, segIdx, segEnd)
			}
		}
	}
}

// TestReadAtBoundsValidation tests ReadAt boundary validation
func TestReadAtBoundsValidation(t *testing.T) {
	mvf := &MetadataVirtualFile{
		meta: &fileHandleMeta{
			FileSize: 1000,
		},
	}

	tests := []struct {
		name    string
		offset  int64
		bufSize int
		wantErr error
	}{
		{"negative offset", -1, 100, ErrNegativeOffset},
		{"at file size", 1000, 100, io.EOF},
		{"beyond file size", 1500, 100, io.EOF},
		{"empty buffer", 0, 0, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := make([]byte, tt.bufSize)
			_, err := mvf.ReadAt(buf, tt.offset)

			if tt.wantErr == nil {
				if err != nil {
					t.Errorf("ReadAt() error = %v, want nil", err)
				}
			} else {
				if err != tt.wantErr {
					t.Errorf("ReadAt() error = %v, want %v", err, tt.wantErr)
				}
			}
		})
	}
}

// TestReadAtContextAfterClose verifies that ReadAtContext returns ErrFileClosed
// instead of panicking once Close() has released mvf.meta. Regression test for
// a nil-pointer dereference where mvf.meta was read unlocked before mvf.mu was
// acquired, racing with Close() nil-ing it out under the lock.
func TestReadAtContextAfterClose(t *testing.T) {
	mvf := &MetadataVirtualFile{
		meta: &fileHandleMeta{
			FileSize: 1000,
		},
	}

	if err := mvf.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	buf := make([]byte, 100)
	n, err := mvf.ReadAtContext(context.Background(), buf, 0)
	if err != ErrFileClosed {
		t.Errorf("ReadAtContext() after Close() error = %v, want ErrFileClosed", err)
	}
	if n != 0 {
		t.Errorf("ReadAtContext() after Close() n = %d, want 0", n)
	}
}

// TestReadAtContextConcurrentClose races ReadAtContext against Close() to
// reproduce the interleaving that produced the original nil-pointer panic:
// ReadAtContext must never dereference mvf.meta after it observes nil, and
// Close() must never race unsynchronized with a held mvf.meta read. Run with
// -race to catch any regression.
func TestReadAtContextConcurrentClose(t *testing.T) {
	for range 200 {
		mvf := &MetadataVirtualFile{
			meta: &fileHandleMeta{
				FileSize: 1000,
			},
		}

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			buf := make([]byte, 100)
			for j := range 20 {
				_, _ = mvf.ReadAtContext(context.Background(), buf, int64(j%10)*100)
			}
		}()

		go func() {
			defer wg.Done()
			_ = mvf.Close()
		}()

		wg.Wait()
	}
}

// TestReadAtNoPoolManager tests ReadAt when pool manager is nil
func TestReadAtNoPoolManager(t *testing.T) {
	segments := []*metapb.SegmentData{
		{StartOffset: 0, EndOffset: 999, SegmentSize: 1000},
	}

	mvf := &MetadataVirtualFile{
		meta: &fileHandleMeta{
			FileSize:    1000,
			SegmentData: segments,
		},
		segmentIndex: buildSegmentIndex(segments),
		poolManager:  nil, // No pool manager
	}

	buf := make([]byte, 100)
	_, err := mvf.ReadAt(buf, 0)

	if err != ErrNoUsenetPool {
		t.Errorf("ReadAt() error = %v, want ErrNoUsenetPool", err)
	}
}

// TestReadAtNoSegments tests ReadAt when there are no segments
func TestReadAtNoSegments(t *testing.T) {
	mvf := &MetadataVirtualFile{
		meta: &fileHandleMeta{
			FileSize:    1000,
			SegmentData: nil, // No segments
		},
		poolManager: &mockPoolManager{},
		ctx:         context.Background(),
	}

	buf := make([]byte, 100)
	_, err := mvf.ReadAt(buf, 0)

	if err != ErrMissmatchedSegments {
		t.Errorf("ReadAt() error = %v, want ErrMissmatchedSegments", err)
	}
}

// Compile-time check that mockPoolManager implements pool.Manager
var _ pool.Manager = (*mockPoolManager)(nil)

// mockPoolManager implements pool.Manager for testing
type mockPoolManager struct{}

func (m *mockPoolManager) GetPool() (pool.NntpClient, error) {
	return nil, nil
}

func (m *mockPoolManager) SetProviders(_ []nntppool.Provider) error {
	return nil
}

func (m *mockPoolManager) ClearPool() error {
	return nil
}

func (m *mockPoolManager) HasPool() bool {
	return true
}

func (m *mockPoolManager) GetMetrics() (pool.MetricsSnapshot, error) {
	return pool.MetricsSnapshot{}, nil
}

func (m *mockPoolManager) ResetMetrics(_ context.Context, _, _ bool) error {
	return nil
}

func (m *mockPoolManager) ResetProviderErrors(_ context.Context) error {
	return nil
}

func (m *mockPoolManager) IncArticlesDownloaded() {}

func (m *mockPoolManager) UpdateDownloadProgress(_ string, _ int64) {}

func (m *mockPoolManager) IncArticlesPosted() {}

func (m *mockPoolManager) AddProvider(_ nntppool.Provider) error {
	return nil
}

func (m *mockPoolManager) RemoveProvider(_ string) error {
	return nil
}

func (m *mockPoolManager) ResetProviderQuota(_ context.Context, _ string) error {
	return nil
}

func (m *mockPoolManager) SetProviderIDs(_ map[string]string) {}

func (m *mockPoolManager) AcquireImportSlot(_ context.Context) (func(), error) {
	return func() {}, nil
}

func (m *mockPoolManager) SetAdmissionCap(_ int) {}
func (m *mockPoolManager) AcquireImportConnection(_ context.Context) (func(), error) {
	return func() {}, nil
}
func (m *mockPoolManager) SetImportConnCapacity(_ int) {}
func (m *mockPoolManager) ImportConnCapacity() int     { return 0 }

func (m *mockPoolManager) SetStreamSource(_ pool.StreamActivitySource) {}

func (m *mockPoolManager) NotifyStreamChange()                       {}
func (m *mockPoolManager) StatSweepConcurrency(conservative int) int { return conservative }

// TestSeekResetsOriginalRangeEnd tests that Seek properly resets originalRangeEnd
// This is critical for video playback - without this fix, seeking causes stale range
// information to be reused, breaking subsequent reads
func TestSeekResetsOriginalRangeEnd(t *testing.T) {
	fileSize := int64(100 * 1024 * 1024) // 100MB
	mvf := &MetadataVirtualFile{
		meta: &fileHandleMeta{
			FileSize: fileSize,
		},
		position:          0,
		originalRangeEnd:  -1, // Simulate unbounded range from initial HTTP request
		readerInitialized: false,
	}

	// Seek to a new position - this should reset originalRangeEnd
	newPos, err := mvf.Seek(1024*1024, io.SeekStart) // Seek to 1MB
	if err != nil {
		t.Fatalf("Seek() error = %v", err)
	}
	if newPos != 1024*1024 {
		t.Errorf("Seek() returned position = %d, want %d", newPos, 1024*1024)
	}

	// originalRangeEnd should be reset to 0 (not -1) to force fresh range calculation
	if mvf.originalRangeEnd != 0 {
		t.Errorf("After Seek(), originalRangeEnd = %d, want 0 (reset)", mvf.originalRangeEnd)
	}
}

func (m *mockPoolManager) SetStreamHeadroom(int)                      {}
func (m *mockPoolManager) SpeculativeBudget() *pool.SpeculativeBudget { return nil }

// TestSeekSamePositionDoesNotResetRange tests that seeking to the same position
// does not reset originalRangeEnd (optimization)
func TestSeekSamePositionDoesNotResetRange(t *testing.T) {
	fileSize := int64(100 * 1024 * 1024) // 100MB
	mvf := &MetadataVirtualFile{
		meta: &fileHandleMeta{
			FileSize: fileSize,
		},
		position:          1024,
		originalRangeEnd:  -1, // Unbounded range
		readerInitialized: false,
	}

	// Seek to the SAME position
	_, err := mvf.Seek(1024, io.SeekStart)
	if err != nil {
		t.Fatalf("Seek() error = %v", err)
	}

	// originalRangeEnd should NOT be reset since position didn't change
	if mvf.originalRangeEnd != -1 {
		t.Errorf("After Seek() to same position, originalRangeEnd = %d, want -1 (unchanged)", mvf.originalRangeEnd)
	}
}

// TestMultipleConsecutiveSeeks tests that multiple seeks all reset originalRangeEnd correctly
func TestMultipleConsecutiveSeeks(t *testing.T) {
	fileSize := int64(100 * 1024 * 1024) // 100MB
	mvf := &MetadataVirtualFile{
		meta: &fileHandleMeta{
			FileSize: fileSize,
		},
		position:          0,
		originalRangeEnd:  5000, // Bounded range
		readerInitialized: false,
	}

	positions := []int64{1000, 5000, 10000, 50000, 1000} // Back and forth

	for i, targetPos := range positions {
		mvf.originalRangeEnd = int64(i * 1000) // Set a non-zero value before each seek

		_, err := mvf.Seek(targetPos, io.SeekStart)
		if err != nil {
			t.Fatalf("Seek(%d) error = %v", targetPos, err)
		}

		if mvf.position != targetPos {
			t.Errorf("After Seek(%d), position = %d", targetPos, mvf.position)
		}

		// originalRangeEnd should be reset after each seek (except when position unchanged)
		if mvf.originalRangeEnd != 0 {
			t.Errorf("After Seek(%d), originalRangeEnd = %d, want 0", targetPos, mvf.originalRangeEnd)
		}
	}
}

// TestSeekWithWhenceModes tests all three seek whence modes reset originalRangeEnd
func TestSeekWithWhenceModes(t *testing.T) {
	fileSize := int64(100 * 1024 * 1024) // 100MB

	tests := []struct {
		name        string
		initialPos  int64
		offset      int64
		whence      int
		expectedPos int64
	}{
		{
			name:        "SeekStart to middle",
			initialPos:  0,
			offset:      50 * 1024 * 1024,
			whence:      io.SeekStart,
			expectedPos: 50 * 1024 * 1024,
		},
		{
			name:        "SeekCurrent forward",
			initialPos:  1024,
			offset:      1024,
			whence:      io.SeekCurrent,
			expectedPos: 2048,
		},
		{
			name:        "SeekEnd backwards",
			initialPos:  0,
			offset:      -1024,
			whence:      io.SeekEnd,
			expectedPos: fileSize - 1024,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mvf := &MetadataVirtualFile{
				meta: &fileHandleMeta{
					FileSize: fileSize,
				},
				position:          tt.initialPos,
				originalRangeEnd:  -1, // Unbounded
				readerInitialized: false,
			}

			newPos, err := mvf.Seek(tt.offset, tt.whence)
			if err != nil {
				t.Fatalf("Seek() error = %v", err)
			}

			if newPos != tt.expectedPos {
				t.Errorf("Seek() position = %d, want %d", newPos, tt.expectedPos)
			}

			// originalRangeEnd should be reset
			if mvf.originalRangeEnd != 0 {
				t.Errorf("originalRangeEnd = %d, want 0", mvf.originalRangeEnd)
			}
		})
	}
}

// TestSeekErrorCases tests that invalid seeks don't modify state
func TestSeekErrorCases(t *testing.T) {
	fileSize := int64(100 * 1024 * 1024) // 100MB

	tests := []struct {
		name        string
		initialPos  int64
		offset      int64
		whence      int
		expectedErr error
	}{
		{
			name:        "negative position via SeekStart",
			initialPos:  0,
			offset:      -100,
			whence:      io.SeekStart,
			expectedErr: ErrSeekNegative,
		},
		{
			name:        "beyond file size",
			initialPos:  0,
			offset:      fileSize + 100,
			whence:      io.SeekStart,
			expectedErr: ErrSeekTooFar,
		},
		{
			name:        "invalid whence",
			initialPos:  0,
			offset:      0,
			whence:      99, // Invalid whence
			expectedErr: ErrInvalidWhence,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mvf := &MetadataVirtualFile{
				meta: &fileHandleMeta{
					FileSize: fileSize,
				},
				position:          tt.initialPos,
				originalRangeEnd:  -1,
				readerInitialized: false,
			}

			_, err := mvf.Seek(tt.offset, tt.whence)
			if err != tt.expectedErr {
				t.Errorf("Seek() error = %v, want %v", err, tt.expectedErr)
			}

			// Position should not change on error
			if mvf.position != tt.initialPos {
				t.Errorf("Position changed on error: got %d, want %d", mvf.position, tt.initialPos)
			}

			// originalRangeEnd should not be reset on error
			if mvf.originalRangeEnd != -1 {
				t.Errorf("originalRangeEnd changed on error: got %d, want -1", mvf.originalRangeEnd)
			}
		})
	}
}

// TestConcurrentSegmentIndexAccess tests thread safety of segment index
func TestConcurrentSegmentIndexAccess(t *testing.T) {
	segments := []*metapb.SegmentData{
		{StartOffset: 0, EndOffset: 999, SegmentSize: 1000},
		{StartOffset: 0, EndOffset: 999, SegmentSize: 1000},
		{StartOffset: 0, EndOffset: 999, SegmentSize: 1000},
	}
	idx := buildSegmentIndex(segments)

	// Run concurrent lookups
	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func(offset int64) {
			defer wg.Done()
			for range 100 {
				_ = idx.findSegmentForOffset(offset % 3000)
			}
		}(int64(i * 30))
	}
	wg.Wait()
}
