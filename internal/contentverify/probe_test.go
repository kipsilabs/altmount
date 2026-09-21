package contentverify

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/utils"
	"github.com/javi11/nntppool/v5"
	"github.com/spf13/afero"
)

// fakeFile implements afero.File with only Read/Close exercised by Probe.
type fakeFile struct {
	afero.File // embed to satisfy the interface; only Read/Close are called
	data       []byte
	readErr    error
	pos        int
}

func (f *fakeFile) Read(p []byte) (int, error) {
	if f.readErr != nil {
		return 0, f.readErr
	}
	if f.pos >= len(f.data) {
		return 0, io.EOF
	}
	n := copy(p, f.data[f.pos:])
	f.pos += n
	return n, nil
}

func (f *fakeFile) Close() error { return nil }

type fakeOpener struct {
	file        *fakeFile
	err         error
	capturedCtx context.Context
	calls       int
}

func (o *fakeOpener) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (afero.File, error) {
	o.capturedCtx = ctx
	o.calls++
	if o.err != nil {
		return nil, o.err
	}
	// Each attempt gets its own reader: a retried probe must not resume from
	// the previous attempt's read offset.
	return &fakeFile{data: o.file.data, readErr: o.file.readErr}, nil
}

func TestProbe_ValidSignature(t *testing.T) {
	data := append([]byte{0x1A, 0x45, 0xDF, 0xA3, 0x42, 0x82, 0x88}, []byte("matroska")...)
	data = append(data, make([]byte, 512-len(data))...)
	res := Probe(context.Background(), &fakeOpener{file: &fakeFile{data: data}}, "movie.mkv", time.Second)
	if res.Result != ContentValid {
		t.Errorf("got %s, want %s", res.Result, ContentValid)
	}
}

func TestProbe_InvalidSignature(t *testing.T) {
	data := make([]byte, 512) // all zero bytes, no known signature
	res := Probe(context.Background(), &fakeOpener{file: &fakeFile{data: data}}, "movie.mkv", time.Second)
	if res.Result != ContentInvalid {
		t.Errorf("got %s, want %s", res.Result, ContentInvalid)
	}
}

func TestProbe_ShortFileTolerated(t *testing.T) {
	// Shorter than 512 bytes but a valid, complete signature.
	data := []byte("\x66\x4C\x61\x43\x00\x00\x00\x22")
	res := Probe(context.Background(), &fakeOpener{file: &fakeFile{data: data}}, "song.flac", time.Second)
	if res.Result != ContentValid {
		t.Errorf("got %s, want %s for short-but-valid file", res.Result, ContentValid)
	}
}

func TestProbe_MissingSegment(t *testing.T) {
	opener := &fakeOpener{file: &fakeFile{readErr: nntppool.ErrArticleNotFound}}
	res := Probe(context.Background(), opener, "movie.mkv", time.Second)
	if res.Result != ContentSegmentMissing {
		t.Errorf("got %s, want %s", res.Result, ContentSegmentMissing)
	}
	if !errors.Is(res.Err, nntppool.ErrArticleNotFound) {
		t.Errorf("expected wrapped ErrArticleNotFound, got %v", res.Err)
	}
}

func TestProbe_TransientError(t *testing.T) {
	opener := &fakeOpener{file: &fakeFile{readErr: errors.New("connection reset by peer")}}
	res := Probe(context.Background(), opener, "movie.mkv", time.Second)
	if res.Result != ContentProbeError {
		t.Errorf("got %s, want %s", res.Result, ContentProbeError)
	}
}

func TestProbe_OpenFileError(t *testing.T) {
	opener := &fakeOpener{err: errors.New("boom")}
	res := Probe(context.Background(), opener, "movie.mkv", time.Second)
	if res.Result != ContentProbeError {
		t.Errorf("got %s, want %s", res.Result, ContentProbeError)
	}
}

func TestProbe_TransientErrorIsRetriedOnce(t *testing.T) {
	opener := &fakeOpener{file: &fakeFile{readErr: errors.New("connection reset by peer")}}
	res := Probe(context.Background(), opener, "movie.mkv", time.Second)
	if res.Result != ContentProbeError {
		t.Fatalf("got %s, want %s", res.Result, ContentProbeError)
	}
	if opener.calls != 2 {
		t.Errorf("got %d probe attempts, want 2 (one retry on a transient error)", opener.calls)
	}
}

func TestProbe_MissingSegmentIsNotRetried(t *testing.T) {
	opener := &fakeOpener{file: &fakeFile{readErr: nntppool.ErrArticleNotFound}}
	res := Probe(context.Background(), opener, "movie.mkv", time.Second)
	if res.Result != ContentSegmentMissing {
		t.Fatalf("got %s, want %s", res.Result, ContentSegmentMissing)
	}
	if opener.calls != 1 {
		t.Errorf("got %d probe attempts, want 1 (a definitive result must not be retried)", opener.calls)
	}
}

func TestProbe_InvalidSignatureIsNotRetried(t *testing.T) {
	opener := &fakeOpener{file: &fakeFile{data: make([]byte, 512)}}
	res := Probe(context.Background(), opener, "movie.mkv", time.Second)
	if res.Result != ContentInvalid {
		t.Fatalf("got %s, want %s", res.Result, ContentInvalid)
	}
	if opener.calls != 1 {
		t.Errorf("got %d probe attempts, want 1 (a definitive result must not be retried)", opener.calls)
	}
}

func TestProbe_Timeout(t *testing.T) {
	opener := &fakeOpener{file: &fakeFile{readErr: context.DeadlineExceeded}}
	res := Probe(context.Background(), opener, "movie.mkv", time.Millisecond)
	if res.Result != ContentProbeError {
		t.Errorf("got %s, want %s", res.Result, ContentProbeError)
	}
}

func TestProbe_ContextCarriesPrefetchAndSuppressFlags(t *testing.T) {
	data := make([]byte, 512)
	opener := &fakeOpener{file: &fakeFile{data: data}}
	Probe(context.Background(), opener, "movie.mkv", time.Second)

	if opener.capturedCtx == nil {
		t.Fatal("OpenFile was not called with a context")
	}
	prefetch, ok := opener.capturedCtx.Value(utils.MaxPrefetchKey).(int)
	if !ok || prefetch != 1 {
		t.Errorf("got MaxPrefetchKey=%v (ok=%v), want 1", prefetch, ok)
	}
	suppress, ok := opener.capturedCtx.Value(utils.SuppressStreamTrackingKey).(bool)
	if !ok || suppress != true {
		t.Errorf("got SuppressStreamTrackingKey=%v (ok=%v), want true", suppress, ok)
	}
}

func TestResult_String(t *testing.T) {
	cases := map[Result]string{
		ContentValid:          "ContentValid",
		ContentInvalid:        "ContentInvalid",
		ContentSegmentMissing: "ContentSegmentMissing",
		ContentProbeError:     "ContentProbeError",
		Result(99):            "Result(99)",
	}
	for result, want := range cases {
		if got := result.String(); got != want {
			t.Errorf("Result(%d).String() = %q, want %q", int(result), got, want)
		}
	}
}
