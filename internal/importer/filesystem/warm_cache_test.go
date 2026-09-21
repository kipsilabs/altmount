package filesystem

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/importer/parser"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/pool"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/javi11/nntppool/v5"
)

// fsFakePoolManager satisfies the full pool.Manager surface so UsenetFile can
// call GetPool / HasPool / metric helpers without nil-deref. GetPool returns the
// embedded fakepool client so wire calls are counted.
type fsFakePoolManager struct {
	client pool.NntpClient
}

var _ pool.Manager = (*fsFakePoolManager)(nil)

func (m *fsFakePoolManager) GetPool() (pool.NntpClient, error)        { return m.client, nil }
func (m *fsFakePoolManager) SetProviders(_ []nntppool.Provider) error { return nil }
func (m *fsFakePoolManager) ClearPool() error                         { return nil }
func (m *fsFakePoolManager) HasPool() bool                            { return true }
func (m *fsFakePoolManager) GetMetrics() (pool.MetricsSnapshot, error) {
	return pool.MetricsSnapshot{}, nil
}
func (m *fsFakePoolManager) ResetMetrics(_ context.Context, _, _ bool) error { return nil }
func (m *fsFakePoolManager) ResetProviderErrors(_ context.Context) error     { return nil }
func (m *fsFakePoolManager) IncArticlesDownloaded()                          {}
func (m *fsFakePoolManager) UpdateDownloadProgress(_ string, _ int64)        {}
func (m *fsFakePoolManager) IncArticlesPosted()                              {}
func (m *fsFakePoolManager) AddProvider(_ nntppool.Provider) error           { return nil }
func (m *fsFakePoolManager) RemoveProvider(_ string) error                   { return nil }
func (m *fsFakePoolManager) ResetProviderQuota(_ context.Context, _ string) error {
	return nil
}
func (m *fsFakePoolManager) SetProviderIDs(_ map[string]string) {}
func (m *fsFakePoolManager) AcquireImportSlot(_ context.Context) (func(), error) {
	return func() {}, nil
}
func (m *fsFakePoolManager) SetAdmissionCap(_ int) {}
func (m *fsFakePoolManager) AcquireImportConnection(_ context.Context) (func(), error) {
	return func() {}, nil
}
func (m *fsFakePoolManager) SetImportConnCapacity(_ int)                 {}
func (m *fsFakePoolManager) ImportConnCapacity() int                     { return 0 }
func (m *fsFakePoolManager) SetStreamSource(_ pool.StreamActivitySource) {}
func (m *fsFakePoolManager) NotifyStreamChange()                         {}
func (m *fsFakePoolManager) StatSweepConcurrency(conservative int) int   { return conservative }

// TestWarmCacheServesPrefixWithoutNetwork verifies that a read confined to a
// file's warm first-segment bytes is served from memory, issuing zero wire calls.
func TestWarmCacheServesPrefixWithoutNetwork(t *testing.T) {
	const size = 50
	want := make([]byte, size)
	for i := range want {
		want[i] = byte('A' + (i % 26))
	}

	fpc := fakepool.New()
	mgr := &fsFakePoolManager{client: fpc}

	file := parser.ParsedFile{
		Filename: "movie.part01.rar",
		Size:     size,
		Segments: []*metapb.SegmentData{
			{Id: "seg0", StartOffset: 0, EndOffset: size - 1, SegmentSize: size},
		},
		FirstSegmentBytes: want,
	}

	ufs := NewUsenetFileSystem(context.Background(), mgr, []parser.ParsedFile{file}, 1, nil, time.Minute, nil)
	f, err := ufs.Open("movie.part01.rar")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	ra, ok := f.(io.ReaderAt)
	if !ok {
		t.Fatalf("file does not implement io.ReaderAt")
	}

	buf := make([]byte, 20)
	n, err := ra.ReadAt(buf, 0)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadAt: %v", err)
	}
	if n != len(buf) {
		t.Fatalf("ReadAt returned n=%d, want %d", n, len(buf))
	}
	if string(buf) != string(want[:20]) {
		t.Fatalf("ReadAt bytes mismatch:\n got %q\nwant %q", buf, want[:20])
	}

	if got := fpc.BodyPriorityCalls(); got != 0 {
		t.Errorf("warm-cache read issued %d BodyPriority calls, want 0", got)
	}
	if got := fpc.BodyCalls(); got != 0 {
		t.Errorf("warm-cache read issued %d Body calls, want 0", got)
	}
}

func (m *fsFakePoolManager) SetStreamHeadroom(int)                      {}
func (m *fsFakePoolManager) SpeculativeBudget() *pool.SpeculativeBudget { return nil }

// stubReadCloser is an in-memory ReadCloser for warmPrefixReader unit tests.
type stubReadCloser struct {
	data   []byte
	pos    int
	closed bool
}

func (s *stubReadCloser) Read(p []byte) (int, error) {
	if s.pos >= len(s.data) {
		return 0, io.EOF
	}
	n := copy(p, s.data[s.pos:])
	s.pos += n
	return n, nil
}

func (s *stubReadCloser) Close() error {
	s.closed = true
	return nil
}

// TestWarmPrefixReaderFallsThrough verifies the reader serves the prefix
// from memory and only invokes the network factory when a read crosses past it.
func TestWarmPrefixReaderFallsThrough(t *testing.T) {
	prefix := []byte("HEADERBYTES") // 11 bytes
	rest := []byte("TAILDATA")      // 8 bytes

	madeRest := 0
	stub := &stubReadCloser{data: rest}
	wr := &warmPrefixReader{
		prefix: prefix,
		makeRest: func() (io.ReadCloser, error) {
			madeRest++
			return stub, nil
		},
	}

	// Read the full concatenation in small chunks.
	got, err := io.ReadAll(wr)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != string(prefix)+string(rest) {
		t.Fatalf("concat mismatch:\n got %q\nwant %q", got, string(prefix)+string(rest))
	}
	if madeRest != 1 {
		t.Errorf("makeRest called %d times, want 1", madeRest)
	}
	if err := wr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !stub.closed {
		t.Errorf("underlying rest reader was not closed")
	}
}

// TestWarmPrefixReaderPrefixOnly verifies that a read fully contained in the
// prefix never constructs the network reader.
func TestWarmPrefixReaderPrefixOnly(t *testing.T) {
	prefix := []byte("HEADER")
	madeRest := 0
	wr := &warmPrefixReader{
		prefix: prefix,
		// makeRest must never be called for a prefix-only read.
		makeRest: func() (io.ReadCloser, error) {
			madeRest++
			return nil, nil
		},
	}

	buf := make([]byte, len(prefix))
	n, err := io.ReadFull(wr, buf)
	if err != nil {
		t.Fatalf("ReadFull: %v", err)
	}
	if n != len(prefix) || string(buf) != string(prefix) {
		t.Fatalf("prefix read mismatch: n=%d buf=%q", n, buf)
	}
	if madeRest != 0 {
		t.Errorf("makeRest called %d times for prefix-only read, want 0", madeRest)
	}
}
