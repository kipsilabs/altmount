package health

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/metadata"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/kipsilabs/altmount/internal/usenet"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

// fakeContentFile implements afero.File with only Read/Close exercised.
type fakeContentFile struct {
	afero.File
	data    []byte
	readErr error
	pos     int
}

func (f *fakeContentFile) Read(p []byte) (int, error) {
	if f.readErr != nil {
		return 0, f.readErr
	}
	if f.pos >= len(f.data) {
		return 0, os.ErrClosed
	}
	n := copy(p, f.data[f.pos:])
	f.pos += n
	return n, nil
}

func (f *fakeContentFile) Close() error { return nil }

type fakeContentOpener struct {
	data []byte
	err  error
}

func (o *fakeContentOpener) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (afero.File, error) {
	if o.err != nil {
		return nil, o.err
	}
	return &fakeContentFile{data: o.data}, nil
}

func newTestHealthCheckerWithVerifyContentEnabled(t *testing.T, opener *fakeContentOpener) *HealthChecker {
	enabled := true
	cfg := &config.Config{Health: config.HealthConfig{VerifyContent: &enabled}}
	return &HealthChecker{
		configGetter:    func() *config.Config { return cfg },
		contentVerifyFS: opener,
		metadataService: newContentTestMetadata(t),
	}
}

// newContentTestMetadata holds metadata for the files these tests judge: a
// clean verdict re-reads it to look for known holes.
func newContentTestMetadata(t *testing.T) *metadata.MetadataService {
	t.Helper()
	ms := metadata.NewMetadataService(t.TempDir())
	for _, p := range []string{"/movie.mkv", "/release.nfo"} {
		if err := ms.WriteFileMetadata(p, validSegmentMeta(ms, 1024)); err != nil {
			t.Fatal(err)
		}
	}
	return ms
}

func allPresentResult() usenet.ValidationResult {
	return usenet.ValidationResult{TotalChecked: 1, MissingCount: 0}
}

func TestJudgeValidation_ContentInvalid(t *testing.T) {
	hc := newTestHealthCheckerWithVerifyContentEnabled(t, &fakeContentOpener{data: make([]byte, 512)}) // no recognized signature
	prep := preparedCheck{filePath: "/movie.mkv", currentStatus: database.HealthStatusPending}

	event := hc.judgeValidation(context.Background(), prep, allPresentResult(), nil)

	if event.Status != database.HealthStatusCorrupted {
		t.Errorf("got status %v, want Corrupted", event.Status)
	}
	if event.Details == nil || !strings.Contains(*event.Details, "content_invalid") {
		t.Errorf("expected content_invalid in details, got %v", event.Details)
	}
}

func TestJudgeValidation_ContentProbeErrorLeavesStatusUnchanged(t *testing.T) {
	hc := newTestHealthCheckerWithVerifyContentEnabled(t, &fakeContentOpener{err: errors.New("connection reset")})
	prep := preparedCheck{filePath: "/movie.mkv", currentStatus: database.HealthStatusPending}

	event := hc.judgeValidation(context.Background(), prep, allPresentResult(), nil)

	if event.Type != EventTypeFileHealthy {
		t.Errorf("got event type %v, want EventTypeFileHealthy (transient probe error must not corrupt)", event.Type)
	}
}

func TestJudgeContentVerification_ProbeErrorReturnsNoEvent(t *testing.T) {
	hc := newTestHealthCheckerWithVerifyContentEnabled(t, &fakeContentOpener{err: errors.New("connection reset")})
	prep := preparedCheck{filePath: "/movie.mkv", currentStatus: database.HealthStatusPending}

	event, healthyDetails := hc.judgeContentVerification(context.Background(), prep)
	if event != nil {
		t.Errorf("a transient probe error must produce no event, got %+v", *event)
	}
	if healthyDetails == nil {
		t.Error("a transient probe error must still be recorded on the healthy result")
	}
}

func TestJudgeValidation_SkipsProbeWhenNotPending(t *testing.T) {
	hc := newTestHealthCheckerWithVerifyContentEnabled(t, &fakeContentOpener{data: make([]byte, 512)}) // would fail if probed
	prep := preparedCheck{filePath: "/movie.mkv", currentStatus: database.HealthStatusDegraded}

	event := hc.judgeValidation(context.Background(), prep, allPresentResult(), nil)

	if event.Type != EventTypeFileHealthy {
		t.Errorf("expected a Degraded-status recheck to skip content probing entirely, got %v", event.Type)
	}
}

func TestJudgeValidation_ValidSignaturePasses(t *testing.T) {
	data := append([]byte{0x1A, 0x45, 0xDF, 0xA3, 0x42, 0x82, 0x88}, []byte("matroska")...)
	data = append(data, make([]byte, 512-len(data))...)
	hc := newTestHealthCheckerWithVerifyContentEnabled(t, &fakeContentOpener{data: data})
	prep := preparedCheck{filePath: "/movie.mkv", currentStatus: database.HealthStatusPending}

	event := hc.judgeValidation(context.Background(), prep, allPresentResult(), nil)

	if event.Type != EventTypeFileHealthy {
		t.Errorf("expected a valid signature to pass, got %v (%v)", event.Type, event.Error)
	}
}

func TestJudgeValidation_OverrideForcesProbeOnNonPending(t *testing.T) {
	force := true
	hc := newTestHealthCheckerWithVerifyContentEnabled(t, &fakeContentOpener{data: make([]byte, 512)})
	prep := preparedCheck{filePath: "/movie.mkv", currentStatus: database.HealthStatusDegraded, verifyContentOverride: &force}

	event := hc.judgeValidation(context.Background(), prep, allPresentResult(), nil)

	if event.Status != database.HealthStatusCorrupted {
		t.Errorf("expected override to force the probe and mark corrupted, got %v", event.Type)
	}
}

func TestJudgeValidation_SkipsNonMediaFiles(t *testing.T) {
	hc := newTestHealthCheckerWithVerifyContentEnabled(t, &fakeContentOpener{data: make([]byte, 512)}) // would fail if probed
	prep := preparedCheck{filePath: "/release.nfo", currentStatus: database.HealthStatusPending}

	event := hc.judgeValidation(context.Background(), prep, allPresentResult(), nil)

	if event.Type != EventTypeFileHealthy {
		t.Errorf("expected non-media files to skip content verification, got %v", event.Type)
	}
}

// blockingContentOpener tracks the maximum number of OpenFile calls observed
// in flight simultaneously, holding each call open for delay before
// returning. Used to prove the batch judge loop actually runs content probes
// concurrently rather than one-at-a-time.
type blockingContentOpener struct {
	delay       time.Duration
	data        []byte
	current     int64
	maxInFlight int64
}

func (o *blockingContentOpener) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (afero.File, error) {
	n := atomic.AddInt64(&o.current, 1)
	for {
		max := atomic.LoadInt64(&o.maxInFlight)
		if n <= max || atomic.CompareAndSwapInt64(&o.maxInFlight, max, n) {
			break
		}
	}
	time.Sleep(o.delay)
	atomic.AddInt64(&o.current, -1)
	return &fakeContentFile{data: o.data}, nil
}

func (o *blockingContentOpener) maxObserved() int64 {
	return atomic.LoadInt64(&o.maxInFlight)
}

// TestCheckFilesBatch_JudgesContentVerificationConcurrently is the regression
// test for issue 1: judgeValidation's content-verification probe does a real
// network read per file bounded by a multi-second timeout, so a sequential
// judge loop would serialize the whole batch behind file_count * timeout of
// wall-clock time. This proves the judge phase runs with bounded parallelism.
func TestCheckFilesBatch_JudgesContentVerificationConcurrently(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks not supported on Windows")
	}

	client := fakepool.New()
	env := newBatchTestEnv(t, t.TempDir(), client)

	enabled := true
	cfg := env.hw.configGetter()
	cfg.Health.VerifyContent = &enabled

	const files = 6
	const probeDelay = 200 * time.Millisecond
	opener := &blockingContentOpener{delay: probeDelay, data: make([]byte, 512)} // no recognized signature; result is irrelevant to this test
	env.healthChecker.contentVerifyFS = opener

	paths := make([]string, files)
	statuses := make([]database.HealthStatus, files)
	for i := range files {
		paths[i] = fmt.Sprintf("complete/movie-%02d.mkv", i)
		writeHealthyFile(t, env, paths[i])
		statuses[i] = database.HealthStatusPending
	}

	start := time.Now()
	events := env.healthChecker.CheckFilesBatch(context.Background(), paths, statuses)
	elapsed := time.Since(start)

	require.Len(t, events, files)
	require.Greater(t, opener.maxObserved(), int64(1),
		"content probes across the batch must overlap, not run one at a time")
	require.Less(t, elapsed, files*probeDelay,
		"a concurrent judge loop must complete well under file_count * per-file probe delay")
}
