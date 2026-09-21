package validation

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/holes"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/pool"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/kipsilabs/altmount/internal/usenet"
	"github.com/javi11/nntppool/v5"
)

type fastFailPoolManager struct {
	client pool.NntpClient
}

func (m fastFailPoolManager) GetPool() (pool.NntpClient, error) { return m.client, nil }
func (m fastFailPoolManager) HasPool() bool                     { return m.client != nil }
func (m fastFailPoolManager) IncArticlesDownloaded()            {}
func (m fastFailPoolManager) IncArticlesPosted()                {}
func (m fastFailPoolManager) UpdateDownloadProgress(string, int64) {
}
func (m fastFailPoolManager) GetMetrics() (pool.MetricsSnapshot, error) {
	return pool.MetricsSnapshot{}, nil
}
func (m fastFailPoolManager) ResetMetrics(context.Context, bool, bool) error { return nil }
func (m fastFailPoolManager) ResetProviderErrors(context.Context) error      { return nil }
func (m fastFailPoolManager) SetProviders([]nntppool.Provider) error         { return nil }
func (m fastFailPoolManager) ClearPool() error                               { return nil }
func (m fastFailPoolManager) AddProvider(nntppool.Provider) error            { return nil }
func (m fastFailPoolManager) RemoveProvider(string) error                    { return nil }
func (m fastFailPoolManager) ResetProviderQuota(context.Context, string) error {
	return nil
}
func (m fastFailPoolManager) SetProviderIDs(map[string]string) {}
func (m fastFailPoolManager) AcquireImportSlot(context.Context) (func(), error) {
	return func() {}, nil
}
func (m fastFailPoolManager) SetAdmissionCap(int) {}
func (m fastFailPoolManager) AcquireImportConnection(context.Context) (func(), error) {
	return func() {}, nil
}
func (m fastFailPoolManager) SetImportConnCapacity(int)                 {}
func (m fastFailPoolManager) ImportConnCapacity() int                   { return 0 }
func (m fastFailPoolManager) SetStreamSource(pool.StreamActivitySource) {}
func (m fastFailPoolManager) NotifyStreamChange()                       {}
func (m fastFailPoolManager) StatSweepConcurrency(conservative int) int { return conservative }

// scriptedStatClient returns a configured sequence of STAT errors per
// message ID. Once a sequence is exhausted, its final outcome repeats.
type scriptedStatClient struct {
	pool.NntpClient
	mu       sync.Mutex
	outcomes map[string][]error
	calls    map[string]int
}

func (m fastFailPoolManager) SetStreamHeadroom(int)                      {}
func (m fastFailPoolManager) SpeculativeBudget() *pool.SpeculativeBudget { return nil }

type uncancelableStatClient struct {
	pool.NntpClient
	result nntppool.ExistsResult
}

func (c uncancelableStatClient) StatMany(context.Context, []string, nntppool.ManyOptions) <-chan nntppool.ExistsResult {
	out := make(chan nntppool.ExistsResult, 1)
	out <- c.result
	close(out)
	return out
}

func newScriptedStatClient(outcomes map[string][]error) *scriptedStatClient {
	return &scriptedStatClient{
		NntpClient: fakepool.New(),
		outcomes:   outcomes,
		calls:      make(map[string]int),
	}
}

func (c *scriptedStatClient) ExistsMany(ctx context.Context, ids []string, _ nntppool.ManyOptions) <-chan nntppool.ExistsResult {
	out := make(chan nntppool.ExistsResult, len(ids))
	go func() {
		defer close(out)
		for _, id := range ids {
			c.mu.Lock()
			attempt := c.calls[id]
			c.calls[id]++
			sequence := c.outcomes[id]
			var err error
			if len(sequence) > 0 {
				idx := min(attempt, len(sequence)-1)
				err = sequence[idx]
			}
			c.mu.Unlock()

			result := nntppool.ExistsResult{MessageID: id, Err: err}
			if err == nil {
				result.Result = &nntppool.StatResult{MessageID: id}
			}
			select {
			case out <- result:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

func (c *scriptedStatClient) callCount(id string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls[id]
}

func TestFastFailReleaseProbeRetriesTransientFailure(t *testing.T) {
	client := newScriptedStatClient(map[string][]error{
		"flaky-0": {nntppool.ErrConnectionDied, nil},
	})

	missing, err := FastFailReleaseProbe(
		context.Background(),
		[]FastFailFile{{Filename: "movie.mkv", Segments: makeTestSegments("flaky", 1)}},
		fastFailPoolManager{client: client},
		100,
		1,
		100*time.Millisecond,
		nil,
		time.Time{},
	)
	if err != nil {
		t.Fatalf("FastFailReleaseProbe error = %v, want nil after transient recovery", err)
	}
	if missing {
		t.Fatal("missing = true, want false after transient recovery")
	}
	if got := client.callCount("flaky-0"); got != 2 {
		t.Fatalf("STAT attempts = %d, want 2", got)
	}
}

func TestFastFailReleaseProbeDoesNotRetryDefinitiveMiss(t *testing.T) {
	client := newScriptedStatClient(map[string][]error{
		"gone-0": {nntppool.ErrArticleNotFound},
	})

	missing, err := FastFailReleaseProbe(
		context.Background(),
		[]FastFailFile{{Filename: "movie.mkv", Segments: makeTestSegments("gone", 1)}},
		fastFailPoolManager{client: client},
		100,
		1,
		100*time.Millisecond,
		nil,
		time.Time{},
	)
	if err != nil {
		t.Fatalf("FastFailReleaseProbe error = %v, want nil for definitive miss", err)
	}
	if !missing {
		t.Fatal("missing = false, want true for definitive miss")
	}
	if got := client.callCount("gone-0"); got != 1 {
		t.Fatalf("STAT attempts = %d, want 1 for definitive miss", got)
	}
}

func TestFastFailReleaseProbeCancellationWinsOverDefinitiveMiss(t *testing.T) {
	client := uncancelableStatClient{
		NntpClient: fakepool.New(),
		result:     nntppool.ExistsResult{MessageID: "gone-0", Err: nntppool.ErrArticleNotFound},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	missing, err := FastFailReleaseProbe(
		ctx,
		[]FastFailFile{{Filename: "movie.mkv", Segments: makeTestSegments("gone", 1)}},
		fastFailPoolManager{client: client},
		100,
		1,
		100*time.Millisecond,
		nil,
		time.Time{},
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("FastFailReleaseProbe error = %v, want context.Canceled", err)
	}
	if missing {
		t.Fatal("missing = true, want cancellation to take precedence")
	}
}

func TestFastFailCheckFilesRetriesOnlyTransientIDs(t *testing.T) {
	client := newScriptedStatClient(map[string][]error{
		"flaky-0": {nntppool.ErrServiceUnavailable, nil},
	})
	files := []FastFailFile{
		{Filename: "good.mkv", Segments: makeTestSegments("good", 1)},
		{Filename: "flaky.mkv", Segments: makeTestSegments("flaky", 1)},
	}

	results, err := FastFailCheckFiles(
		context.Background(), files, fastFailPoolManager{client: client},
		100, 2, 100*time.Millisecond, nil,
		nil,
		false,
		time.Time{},
	)
	if err != nil {
		t.Fatalf("FastFailCheckFiles error = %v, want nil after transient recovery", err)
	}
	if results[0].Broken || results[1].Broken {
		t.Fatalf("results = %+v, want both files healthy after transient recovery", results)
	}
	if got := client.callCount("good-0"); got != 1 {
		t.Fatalf("good STAT attempts = %d, want 1", got)
	}
	if got := client.callCount("flaky-0"); got != 2 {
		t.Fatalf("flaky STAT attempts = %d, want 2", got)
	}
}

func TestFastFailCheckFilesTransientExhaustionIsInconclusive(t *testing.T) {
	client := newScriptedStatClient(map[string][]error{
		"down-0": {nntppool.ErrConnectionDied},
	})

	results, err := FastFailCheckFiles(
		context.Background(),
		[]FastFailFile{{Filename: "movie.mkv", Segments: makeTestSegments("down", 1)}},
		fastFailPoolManager{client: client},
		100, 1, 100*time.Millisecond, nil,
		nil,
		false,
		time.Time{},
	)
	if err == nil {
		t.Fatal("FastFailCheckFiles error = nil, want inconclusive error after retries are exhausted")
	}
	if !errors.Is(err, ErrFastFailInconclusive) {
		t.Fatalf("FastFailCheckFiles error = %v, want ErrFastFailInconclusive", err)
	}
	if !errors.Is(err, nntppool.ErrConnectionDied) {
		t.Fatalf("FastFailCheckFiles error = %v, want wrapped connection error", err)
	}
	if results != nil {
		t.Fatalf("results = %+v, want nil when validation is inconclusive", results)
	}
	if got := client.callCount("down-0"); got != 3 {
		t.Fatalf("STAT attempts = %d, want bounded maximum of 3", got)
	}
}

func TestFastFailReleaseProbeUsesSegmentSamplePercentage(t *testing.T) {
	client := fakepool.New()
	files := []FastFailFile{
		{
			Filename: "movie.mkv",
			Segments: makeTestSegments("video", 100),
		},
	}

	missing, err := FastFailReleaseProbe(
		context.Background(),
		files,
		fastFailPoolManager{client: client},
		10,
		1,
		100*time.Millisecond,
		nil,
		time.Time{},
	)
	if err != nil {
		t.Fatalf("FastFailReleaseProbe returned error: %v", err)
	}
	if missing {
		t.Fatal("missing = true, want false (all segments reachable)")
	}

	if got := client.StatCalls(); got != 10 {
		t.Fatalf("StatCalls = %d, want 10 (10%% of 100 segments)", got)
	}
}

func TestFastFailReleaseProbeReportsMissingOnUnreachableSegment(t *testing.T) {
	client := fakepool.New()
	client.SetBehavior("rar-2", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})

	files := []FastFailFile{
		{
			Filename: "release.part01.rar",
			Segments: []*metapb.SegmentData{
				{Id: "rar-0"},
				{Id: "rar-1"},
				{Id: "rar-2"},
			},
		},
	}

	missing, err := FastFailReleaseProbe(
		context.Background(),
		files,
		fastFailPoolManager{client: client},
		100,
		1,
		100*time.Millisecond,
		nil,
		time.Time{},
	)
	if err != nil {
		t.Fatalf("FastFailReleaseProbe error = %v, want nil (a missing segment is not an error)", err)
	}
	if !missing {
		t.Fatal("missing = false, want true (rar-2 is unreachable)")
	}
}

func TestFastFailReleaseProbePoolUnavailableReturnsError(t *testing.T) {
	missing, err := FastFailReleaseProbe(
		context.Background(),
		[]FastFailFile{{Filename: "movie.mkv", Segments: makeTestSegments("v", 5)}},
		fastFailPoolManager{client: nil},
		100,
		1,
		100*time.Millisecond,
		nil,
		time.Time{},
	)
	if err == nil {
		t.Fatal("FastFailReleaseProbe returned nil error, want error for nil pool")
	}
	if missing {
		t.Error("missing = true, want false when the pool is unavailable (infra error path)")
	}
}

func TestFastFailReleaseProbeNoSegmentsIsHealthy(t *testing.T) {
	client := fakepool.New()
	missing, err := FastFailReleaseProbe(
		context.Background(),
		[]FastFailFile{{Filename: "release.par2"}}, // PAR2 slot: no segments
		fastFailPoolManager{client: client},
		100,
		1,
		100*time.Millisecond,
		nil,
		time.Time{},
	)
	if err != nil {
		t.Fatalf("FastFailReleaseProbe error = %v, want nil", err)
	}
	if missing {
		t.Error("missing = true, want false when there are no segments to probe")
	}
	if got := client.StatCalls(); got != 0 {
		t.Errorf("StatCalls = %d, want 0 (nothing to probe)", got)
	}
}

func TestSelectFastFailSegments(t *testing.T) {
	idOf := func(segs []*metapb.SegmentData) map[string]struct{} {
		m := make(map[string]struct{}, len(segs))
		for _, s := range segs {
			m[s.Id] = struct{}{}
		}
		return m
	}

	// len <= 2 returns all segments unchanged.
	two := makeTestSegments("s", 2)
	if got := selectFastFailSegments(two, 100); len(got) != 2 {
		t.Fatalf("len<=2: got %d segments, want 2", len(got))
	}

	segs := makeTestSegments("s", 100)

	// pct=0 → exactly first + last.
	got := selectFastFailSegments(segs, 0)
	if len(got) != 2 {
		t.Fatalf("pct=0: got %d, want 2 (first+last)", len(got))
	}
	if got[0].Id != segs[0].Id || got[1].Id != segs[len(segs)-1].Id {
		t.Fatalf("pct=0: want first=%s last=%s, got %s,%s", segs[0].Id, segs[99].Id, got[0].Id, got[1].Id)
	}

	// pct=10 of 100 → 10 middle + first + last = 12, with first/last present and no dups.
	got = selectFastFailSegments(segs, 10)
	if len(got) != 12 {
		t.Fatalf("pct=10: got %d, want 12", len(got))
	}
	ids := idOf(got)
	if _, ok := ids[segs[0].Id]; !ok {
		t.Error("pct=10: first segment missing")
	}
	if _, ok := ids[segs[99].Id]; !ok {
		t.Error("pct=10: last segment missing")
	}
	if len(ids) != len(got) {
		t.Errorf("pct=10: duplicates present (%d unique of %d)", len(ids), len(got))
	}

	// Regression for #812: large files honor the configured percentage instead
	// of collapsing onto a fixed sample cap, and stay duplicate-free.
	big := selectFastFailSegments(makeTestSegments("b", 10000), 100)
	if len(big) != 10000 {
		t.Fatalf("pct=100: got %d, want 10000", len(big))
	}
	if len(idOf(big)) != len(big) {
		t.Error("pct=100: duplicates present")
	}

	// 10% of 10000 middle segments + first + last.
	sampled := selectFastFailSegments(makeTestSegments("b", 10000), 10)
	if len(sampled) != 1002 {
		t.Fatalf("pct=10: got %d, want 1002", len(sampled))
	}
	if len(idOf(sampled)) != len(sampled) {
		t.Error("pct=10: duplicates present")
	}
}

func makeTestSegments(prefix string, count int) []*metapb.SegmentData {
	segments := make([]*metapb.SegmentData, count)
	for i := range count {
		segments[i] = &metapb.SegmentData{Id: fmt.Sprintf("%s-%d", prefix, i)}
	}
	return segments
}

// FastFailCheckFiles tests

func TestFastFailCheckFilesAllReachable(t *testing.T) {
	client := fakepool.New()
	files := []FastFailFile{
		{Filename: "movie.mkv", Segments: makeTestSegments("video", 5)},
		{Filename: "extras.mkv", Segments: makeTestSegments("extras", 5)},
	}

	results, err := FastFailCheckFiles(
		context.Background(), files,
		fastFailPoolManager{client: client},
		100, 2, 100*time.Millisecond,
		nil,
		nil,
		false,
		time.Time{},
	)
	if err != nil {
		t.Fatalf("FastFailCheckFiles error = %v, want nil", err)
	}
	if len(results) != 2 {
		t.Fatalf("len(results) = %d, want 2", len(results))
	}
	for i, r := range results {
		if r.Broken {
			t.Errorf("results[%d].Broken = true, want false", i)
		}
	}
}

func TestFastFailCheckFilesOneFileBroken(t *testing.T) {
	client := fakepool.New()
	client.SetBehavior("bad-0", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	client.SetBehavior("bad-1", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	client.SetBehavior("bad-2", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})

	files := []FastFailFile{
		{Filename: "good.mkv", Segments: makeTestSegments("good", 3)},
		{Filename: "broken.mkv", Segments: makeTestSegments("bad", 3)},
	}

	results, err := FastFailCheckFiles(
		context.Background(), files,
		fastFailPoolManager{client: client},
		100, 2, 100*time.Millisecond,
		nil,
		nil,
		false,
		time.Time{},
	)
	if err != nil {
		t.Fatalf("FastFailCheckFiles error = %v, want nil", err)
	}
	if len(results) != 2 {
		t.Fatalf("len(results) = %d, want 2", len(results))
	}
	if results[0].Broken {
		t.Errorf("results[0].Broken = true, want false (good file)")
	}
	if !results[1].Broken {
		t.Errorf("results[1].Broken = false, want true (broken file)")
	}
	if len(results[1].MissingSegmentIDs) == 0 {
		t.Errorf("results[1].MissingSegmentIDs is empty, want populated")
	}
}

func TestFastFailCheckFilesBrokenSidecarsAreReported(t *testing.T) {
	client := fakepool.New()
	client.SetDefaultBehavior(fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})

	files := []FastFailFile{
		{Filename: "readme.nfo", Segments: makeTestSegments("nfo", 3)},
		{Filename: "checksum.sfv", Segments: makeTestSegments("sfv", 3)},
		{Filename: "cover.jpg", Segments: makeTestSegments("jpg", 3)},
	}

	results, err := FastFailCheckFiles(
		context.Background(), files,
		fastFailPoolManager{client: client},
		100, 2, 100*time.Millisecond,
		nil,
		nil,
		false,
		time.Time{},
	)
	if err != nil {
		t.Fatalf("FastFailCheckFiles error = %v, want nil", err)
	}
	for i, r := range results {
		if !r.Broken {
			t.Errorf("results[%d].Broken = false for sidecar %q, want true (missing segments)", i, files[i].Filename)
		}
	}
	if client.StatCalls() == 0 {
		t.Errorf("StatCalls = 0, want >0 (all files must be checked)")
	}
}

// TestFastFailCheckFilesBrokenSidecarIsReported verifies that a sidecar file
// (.nfo, .sfv, etc.) with missing segments IS reported broken — all files are
// now checked regardless of extension, so broken sidecars are excluded from
// parsing just like broken media files.
func TestFastFailCheckFilesBrokenSidecarIsReported(t *testing.T) {
	client := fakepool.New()
	client.SetDefaultBehavior(fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})

	files := []FastFailFile{
		{Filename: "readme.nfo", Segments: makeTestSegments("nfo", 3)},
	}

	results, err := FastFailCheckFiles(
		context.Background(), files,
		fastFailPoolManager{client: client},
		100, 1, 100*time.Millisecond,
		nil,
		nil,
		false,
		time.Time{},
	)
	if err != nil {
		t.Fatalf("FastFailCheckFiles error = %v, want nil", err)
	}
	if len(results) != 1 {
		t.Fatalf("len(results) = %d, want 1", len(results))
	}
	if !results[0].Broken {
		t.Error("results[0].Broken = false, want true: sidecar with missing segments must be reported broken")
	}
	if client.StatCalls() == 0 {
		t.Error("StatCalls = 0, want >0: sidecar must be Stat-checked")
	}
}

func TestFastFailCheckFilesPoolUnavailableReturnsError(t *testing.T) {
	_, err := FastFailCheckFiles(
		context.Background(),
		[]FastFailFile{{Filename: "movie.mkv", Segments: makeTestSegments("v", 1)}},
		fastFailPoolManager{client: nil},
		100, 1, 100*time.Millisecond,
		nil,
		nil,
		false,
		time.Time{},
	)
	if err == nil {
		t.Fatal("FastFailCheckFiles returned nil error, want error for nil pool")
	}
}

// TestFastFailCheckFilesFirstSegmentAlwaysChecked verifies that Segments[0] is
// always Stat-checked even when sampling would otherwise omit it.
// We pass samplePercentage=0, which forces SelectSegmentsForValidation to its
// minimum of 5 segments. For a 1-segment file the minimum is 1, so this already
// exercises the guarantee. For a large file we confirm segment-0 is checked by
// making only segment-0 unreachable — the file must be reported broken.
func TestFastFailCheckFilesFirstSegmentAlwaysChecked(t *testing.T) {
	client := fakepool.New()
	// Only segment-0 is missing. All others are reachable.
	client.SetBehavior("only-0", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})

	// Build a file where "only-0" is index 0 and the rest are reachable.
	segs := []*metapb.SegmentData{{Id: "only-0"}}
	for i := 1; i <= 4; i++ {
		segs = append(segs, &metapb.SegmentData{Id: fmt.Sprintf("only-%d", i)})
	}

	files := []FastFailFile{
		{Filename: "movie.mkv", Segments: segs},
	}

	results, err := FastFailCheckFiles(
		context.Background(), files,
		fastFailPoolManager{client: client},
		0, 1, 100*time.Millisecond,
		nil,
		nil,
		false,
		time.Time{},
	)
	if err != nil {
		t.Fatalf("FastFailCheckFiles error = %v, want nil", err)
	}
	if len(results) != 1 {
		t.Fatalf("len(results) = %d, want 1", len(results))
	}
	if !results[0].Broken {
		t.Error("results[0].Broken = false, want true: first segment is missing but was not checked")
	}
}

// TestFastFailCheckFilesGroupPropagation verifies that when one part of a RAR
// set is unreachable, every member of that set is marked Broken, while an
// ungrouped sibling file stays healthy.
func TestFastFailCheckFilesGroupPropagation(t *testing.T) {
	client := fakepool.New()
	client.SetBehavior("setA-1-0", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})

	files := []FastFailFile{
		{Filename: "setA.part01.rar", Segments: makeTestSegments("setA-0", 3), GroupKey: "seta"},
		{Filename: "setA.part02.rar", Segments: makeTestSegments("setA-1", 3), GroupKey: "seta"},
		{Filename: "setA.part03.rar", Segments: makeTestSegments("setA-2", 3), GroupKey: "seta"},
		{Filename: "standalone.mkv", Segments: makeTestSegments("solo", 3)},
	}

	results, err := FastFailCheckFiles(
		context.Background(), files,
		fastFailPoolManager{client: client},
		100, 4, 100*time.Millisecond,
		nil,
		nil,
		false,
		time.Time{},
	)
	if err != nil {
		t.Fatalf("FastFailCheckFiles error = %v, want nil", err)
	}
	for i := range 3 {
		if !results[i].Broken {
			t.Errorf("results[%d].Broken = false, want true (whole set is doomed)", i)
		}
	}
	if results[3].Broken {
		t.Error("results[3].Broken = true, want false (standalone file is healthy)")
	}
	// Only the observed-missing segment is reported; siblings carry none.
	if len(results[0].MissingSegmentIDs) != 0 {
		t.Errorf("results[0].MissingSegmentIDs = %v, want empty (no observed miss)", results[0].MissingSegmentIDs)
	}
}

// TestFastFailCheckFilesGroupShortCircuitSkipsStats verifies that once a group
// is known broken, the remaining Stats for that group are skipped — fewer Stats
// run than the total selected sample count.
func TestFastFailCheckFilesGroupShortCircuitSkipsStats(t *testing.T) {
	client := fakepool.New()
	// Every segment of the broken set is unreachable, so any Stat that runs fails;
	// the point is that most are skipped after the first miss.
	client.SetDefaultBehavior(fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})

	const parts = 20
	const segsPerPart = 3
	files := make([]FastFailFile, parts)
	for i := range files {
		files[i] = FastFailFile{
			Filename: fmt.Sprintf("doomed.part%02d.rar", i+1),
			Segments: makeTestSegments(fmt.Sprintf("p%d", i), segsPerPart),
			GroupKey: "doomed",
		}
	}

	results, err := FastFailCheckFiles(
		context.Background(), files,
		fastFailPoolManager{client: client},
		100, 1, 100*time.Millisecond, // single connection → deterministic ordering
		nil,
		nil,
		false,
		time.Time{},
	)
	if err != nil {
		t.Fatalf("FastFailCheckFiles error = %v, want nil", err)
	}
	for i, r := range results {
		if !r.Broken {
			t.Errorf("results[%d].Broken = false, want true", i)
		}
	}
	// Round-robin means the first round Stats one segment per part (20 Stats),
	// the very first of which marks the group broken; all later Stats are skipped.
	// So total Stats must be well below the full sample of parts*segsPerPart.
	if got, full := client.StatCalls(), int64(parts*segsPerPart); got >= full {
		t.Errorf("StatCalls = %d, want < %d (group short-circuit must skip Stats)", got, full)
	}
}

// TestFastFailCheckFilesEmptyGroupKeyNoPropagation guards against propagation
// leaking across ungrouped files: two standalone files, one broken, must not
// taint the other.
func TestFastFailCheckFilesEmptyGroupKeyNoPropagation(t *testing.T) {
	client := fakepool.New()
	client.SetBehavior("a-0", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})

	files := []FastFailFile{
		{Filename: "a.mkv", Segments: makeTestSegments("a", 3)},
		{Filename: "b.mkv", Segments: makeTestSegments("b", 3)},
	}

	results, err := FastFailCheckFiles(
		context.Background(), files,
		fastFailPoolManager{client: client},
		100, 2, 100*time.Millisecond,
		nil,
		nil,
		false,
		time.Time{},
	)
	if err != nil {
		t.Fatalf("FastFailCheckFiles error = %v, want nil", err)
	}
	if !results[0].Broken {
		t.Error("results[0].Broken = false, want true")
	}
	if results[1].Broken {
		t.Error("results[1].Broken = true, want false (empty GroupKey must not propagate)")
	}
}

func TestFastFailCheckFilesIndexAligned(t *testing.T) {
	client := fakepool.New()
	// Only the middle file's segments fail.
	client.SetBehavior("mid-0", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})

	files := []FastFailFile{
		{Filename: "first.mkv", Segments: makeTestSegments("first", 3)},
		{Filename: "second.mkv", Segments: []*metapb.SegmentData{{Id: "mid-0"}}},
		{Filename: "third.mkv", Segments: makeTestSegments("third", 3)},
	}

	results, err := FastFailCheckFiles(
		context.Background(), files,
		fastFailPoolManager{client: client},
		100, 2, 100*time.Millisecond,
		nil,
		nil,
		false,
		time.Time{},
	)
	if err != nil {
		t.Fatalf("FastFailCheckFiles error = %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("len(results) = %d, want 3", len(results))
	}
	if results[0].Broken {
		t.Error("results[0].Broken = true, want false")
	}
	if !results[1].Broken {
		t.Error("results[1].Broken = false, want true")
	}
	if results[2].Broken {
		t.Error("results[2].Broken = true, want false")
	}
}

// TestFastFailReleaseProbeTimeoutIsInconclusive verifies a probe timeout is
// retried but never converted into a definitive missing-segment verdict.
func TestFastFailReleaseProbeTimeoutIsInconclusive(t *testing.T) {
	client := fakepool.New()
	// Every Stat outlives the probe's own deadline.
	client.SetDefaultBehavior(fakepool.SegmentBehavior{Latency: 2 * time.Second})

	missing, err := FastFailReleaseProbe(
		context.Background(),
		[]FastFailFile{{Filename: "movie.mkv", Segments: makeTestSegments("slow", 5)}},
		fastFailPoolManager{client: client},
		100,
		1,
		10*time.Millisecond,
		nil,
		time.Time{},
	)
	if err == nil {
		t.Fatal("FastFailReleaseProbe error = nil, want inconclusive error after retries")
	}
	if !errors.Is(err, ErrFastFailInconclusive) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("FastFailReleaseProbe error = %v, want inconclusive deadline error", err)
	}
	if missing {
		t.Fatal("missing = true, want false when reachability is inconclusive")
	}
	if got := client.StatCalls(); got != fastFailStatMaxAttempts {
		t.Fatalf("StatCalls = %d, want bounded maximum of %d", got, fastFailStatMaxAttempts)
	}
}

// TestFastFailReleaseProbeCallerCancellationReturnsError verifies the opposite
// case: when the caller's own context is cancelled the probe reports an error
// rather than claiming the release is missing.
func TestFastFailReleaseProbeCallerCancellationReturnsError(t *testing.T) {
	client := fakepool.New()
	client.SetDefaultBehavior(fakepool.SegmentBehavior{Latency: 2 * time.Second})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	missing, err := FastFailReleaseProbe(
		ctx,
		[]FastFailFile{{Filename: "movie.mkv", Segments: makeTestSegments("slow", 5)}},
		fastFailPoolManager{client: client},
		100,
		1,
		time.Minute,
		nil,
		time.Time{},
	)
	if err == nil {
		t.Fatal("FastFailReleaseProbe error = nil, want error when the caller cancelled")
	}
	if missing {
		t.Error("missing = true, want false when the caller cancelled (reachability unknown)")
	}
}

// TestFastFailCheckFilesTimeoutIsInconclusive verifies timeout exhaustion
// aborts validation without marking the owning file definitively broken.
func TestFastFailCheckFilesTimeoutIsInconclusive(t *testing.T) {
	client := fakepool.New()
	client.SetDefaultBehavior(fakepool.SegmentBehavior{Latency: 2 * time.Second})

	files := []FastFailFile{
		{Filename: "movie.mkv", Segments: makeTestSegments("slow", 3)},
	}

	results, err := FastFailCheckFiles(
		context.Background(),
		files,
		fastFailPoolManager{client: client},
		100,
		1,
		10*time.Millisecond,
		nil,
		nil,
		false,
		time.Time{},
	)
	if err == nil {
		t.Fatal("FastFailCheckFiles error = nil, want inconclusive error after retries")
	}
	if !errors.Is(err, ErrFastFailInconclusive) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("FastFailCheckFiles error = %v, want inconclusive deadline error", err)
	}
	if results != nil {
		t.Fatalf("results = %+v, want nil when validation is inconclusive", results)
	}
	if got := client.StatCalls(); got != fastFailStatMaxAttempts {
		t.Fatalf("StatCalls = %d, want bounded maximum of %d", got, fastFailStatMaxAttempts)
	}
}

// A sweep whose definitive answers are dominated by 430s is a dead release:
// the unverified remainder cannot change the verdict, so the sweep condemns
// the rest instead of surfacing an inconclusive error after full retries.
func TestFastFailCheckFilesDeadReleaseSettlesWithoutWaitingForUnverified(t *testing.T) {
	outcomes := make(map[string][]error)
	var files []FastFailFile
	for f := 0; f < 10; f++ {
		segs := makeTestSegments(fmt.Sprintf("f%d", f), 1)
		files = append(files, FastFailFile{Filename: fmt.Sprintf("part%02d.rar", f), Segments: segs, GroupKey: "set"})
		if f < 8 {
			outcomes[segs[0].Id] = []error{nntppool.ErrArticleNotFound}
		} else {
			outcomes[segs[0].Id] = []error{nntppool.ErrConnectionDied}
		}
	}
	// A second, unrelated file that never gets STAT-ed once the release is
	// condemned: it must come back Broken all the same.
	tail := makeTestSegments("tail", 1)
	files = append(files, FastFailFile{Filename: "tail.mkv", Segments: tail})
	outcomes[tail[0].Id] = []error{nntppool.ErrConnectionDied}

	client := newScriptedStatClient(outcomes)
	results, err := FastFailCheckFiles(
		context.Background(),
		files,
		fastFailPoolManager{client: client},
		100, 10, 100*time.Millisecond, nil,
		nil,
		false,
		time.Time{},
	)
	if err != nil {
		t.Fatalf("FastFailCheckFiles error = %v, want nil for a dead release", err)
	}
	if len(results) != len(files) {
		t.Fatalf("results = %d, want %d", len(results), len(files))
	}
	for i, r := range results {
		if !r.Broken {
			t.Fatalf("results[%d] (%s) not Broken", i, files[i].Filename)
		}
	}
	if got := len(results[8].MissingSegmentIDs); got != 0 {
		t.Fatalf("unverified file reported %d missing ids, want none (only observed misses are reported)", got)
	}
	if got := client.callCount(tail[0].Id); got != 0 {
		t.Fatalf("tail STAT calls = %d, want 0 once the release is condemned", got)
	}
	if got := client.callCount("f8-0"); got != 1 {
		t.Fatalf("unverified STAT attempts = %d, want 1 (no bounded retries once the release is dead)", got)
	}
}

func TestReleaseLooksDead(t *testing.T) {
	cases := []struct {
		missing, reported int
		want              bool
	}{
		{0, 0, false},
		{7, 7, false},  // below the minimum miss count
		{8, 8, true},   // all definitive answers are misses
		{8, 16, true},  // exactly half
		{8, 17, false}, // under half
		{40, 100, false},
	}
	for _, c := range cases {
		if got := releaseLooksDead(c.missing, c.reported); got != c.want {
			t.Errorf("releaseLooksDead(%d, %d) = %v, want %v", c.missing, c.reported, got, c.want)
		}
	}
}

// Gap placeholders (articles the NZB never listed) are reported as observed
// misses without a STAT, and do not condemn their set: the caller judges an
// exactly-known gap against the hole caps.
func TestFastFailCheckFilesPlaceholdersAreKnownMissesWithoutStat(t *testing.T) {
	client := fakepool.New()
	segs := makeTestSegments("vol2", 4)
	segs[1] = &metapb.SegmentData{Id: holes.PlaceholderID(2, "vol2-0")}
	files := []FastFailFile{
		{Filename: "set.part01.rar", Segments: makeTestSegments("vol1", 3), GroupKey: "set"},
		{Filename: "set.part02.rar", Segments: segs, GroupKey: "set"},
	}

	results, err := FastFailCheckFiles(
		context.Background(), files,
		fastFailPoolManager{client: client},
		100, 8, 100*time.Millisecond, nil, nil,
		false,
		time.Time{},
	)
	if err != nil {
		t.Fatalf("FastFailCheckFiles error = %v", err)
	}
	if results[0].Broken {
		t.Fatal("sibling volume condemned by a declared gap; the caller decides that")
	}
	if !results[1].Broken || results[1].KnownGapCount != 1 || len(results[1].MissingSegmentIDs) != 1 {
		t.Fatalf("results[1] = %+v, want Broken with one known gap", results[1])
	}
	if !holes.IsPlaceholderID(results[1].MissingSegmentIDs[0]) {
		t.Fatalf("missing id %q is not the placeholder", results[1].MissingSegmentIDs[0])
	}
	if results[1].SampledCount != 3 {
		t.Fatalf("SampledCount = %d, want 3 (placeholder excluded from the sample)", results[1].SampledCount)
	}
	if got := client.PerMessageCalls(segs[1].Id); got != 0 {
		t.Fatalf("placeholder STAT-ed %d times, want 0", got)
	}
}

func TestCapReleaseProbeSampleKeepsEdgesAndBounds(t *testing.T) {
	segs := makeTestSegments("big", 400)
	selected := usenet.SelectSegmentsForValidation(segs, 100)
	capped := capReleaseProbeSample(selected)
	if len(capped) != maxReleaseProbeSamples {
		t.Fatalf("capped sample = %d, want %d", len(capped), maxReleaseProbeSamples)
	}
	for i := 0; i < 5; i++ {
		if capped[i] != selected[i] {
			t.Fatalf("edge pick %d changed: %v vs %v", i, capped[i].Id, selected[i].Id)
		}
	}
	seen := make(map[string]struct{})
	for _, s := range capped {
		if _, dup := seen[s.Id]; dup {
			t.Fatalf("duplicate id %s in capped sample", s.Id)
		}
		seen[s.Id] = struct{}{}
	}
	small := makeTestSegments("small", 20)
	if got := capReleaseProbeSample(small); len(got) != 20 {
		t.Fatalf("small sample must pass through untouched, got %d", len(got))
	}
}

type recordingTracker struct {
	mu      sync.Mutex
	current int
	total   int
}

func (r *recordingTracker) Update(current, total int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.current, r.total = current, total
}

func (r *recordingTracker) UpdateAbsolute(int) {}

func (r *recordingTracker) last() (int, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.current, r.total
}

// A standalone file (no GroupKey) is condemned by its first definitive miss:
// the rest of its sample is never STAT-ed.
func TestFastFailCheckFilesStopFileOnFirstMissSkipsRestOfFile(t *testing.T) {
	client := fakepool.New()
	client.SetBehavior("a-0", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})

	files := []FastFailFile{
		{Filename: "a.mkv", Segments: makeTestSegments("a", 3)},
		{Filename: "b.mkv", Segments: makeTestSegments("b", 3)},
	}

	results, err := FastFailCheckFiles(
		context.Background(), files,
		fastFailPoolManager{client: client},
		100, 1, 100*time.Millisecond, // single connection → deterministic ordering
		nil,
		nil,
		true,
		time.Time{},
	)
	if err != nil {
		t.Fatalf("FastFailCheckFiles error = %v, want nil", err)
	}
	if !results[0].Broken {
		t.Error("results[0].Broken = false, want true")
	}
	if results[1].Broken {
		t.Error("results[1].Broken = true, want false")
	}
	if got := client.PerMessageCalls("a-0"); got != 1 {
		t.Errorf("a-0 STAT calls = %d, want 1", got)
	}
	for _, id := range []string{"a-1", "a-2"} {
		if got := client.PerMessageCalls(id); got != 0 {
			t.Errorf("%s STAT calls = %d, want 0 once the file is condemned", id, got)
		}
	}
	if got := client.StatCalls(); got != 4 {
		t.Errorf("StatCalls = %d, want 4 (1 for the condemned file, 3 for the healthy one)", got)
	}
}

// Once every eligible file is condemned the sweep returns without STAT-ing the
// remaining jobs, and progress still reports the full job count.
func TestFastFailCheckFilesStopsWhenNoEligibleFilesRemain(t *testing.T) {
	client := fakepool.New()
	client.SetBehavior("a-0", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	client.SetBehavior("b-0", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})

	files := []FastFailFile{
		{Filename: "a.mkv", Segments: makeTestSegments("a", 3)},
		{Filename: "b.mkv", Segments: makeTestSegments("b", 3)},
	}

	tracker := &recordingTracker{}
	results, err := FastFailCheckFiles(
		context.Background(), files,
		fastFailPoolManager{client: client},
		100, 1, 100*time.Millisecond,
		tracker,
		nil,
		true,
		time.Time{},
	)
	if err != nil {
		t.Fatalf("FastFailCheckFiles error = %v, want nil", err)
	}
	for i, r := range results {
		if !r.Broken {
			t.Errorf("results[%d].Broken = false, want true", i)
		}
	}
	if got := client.StatCalls(); got != 2 {
		t.Errorf("StatCalls = %d, want 2 (one per file, then the sweep ends)", got)
	}
	if current, total := tracker.last(); current != total || total != 6 {
		t.Errorf("progress = %d/%d, want 6/6", current, total)
	}
}

// Regression guard: with stopFileOnFirstMiss disabled the sweep still checks
// every selected segment of a broken file.
func TestFastFailCheckFilesWithoutStopFileSweepsWholeSample(t *testing.T) {
	client := fakepool.New()
	client.SetBehavior("a-0", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})

	files := []FastFailFile{
		{Filename: "a.mkv", Segments: makeTestSegments("a", 3)},
		{Filename: "b.mkv", Segments: makeTestSegments("b", 3)},
	}

	results, err := FastFailCheckFiles(
		context.Background(), files,
		fastFailPoolManager{client: client},
		100, 1, 100*time.Millisecond,
		nil,
		nil,
		false,
		time.Time{},
	)
	if err != nil {
		t.Fatalf("FastFailCheckFiles error = %v, want nil", err)
	}
	if !results[0].Broken || results[1].Broken {
		t.Fatalf("results = %+v, want only the first file broken", results)
	}
	if got := client.StatCalls(); got != 6 {
		t.Errorf("StatCalls = %d, want 6 (full sample for both files)", got)
	}
	for _, id := range []string{"a-1", "a-2"} {
		if got := client.PerMessageCalls(id); got != 1 {
			t.Errorf("%s STAT calls = %d, want 1", id, got)
		}
	}
}

// A miss in one volume condemns every member of its set, so the sweep ends
// without STAT-ing any sibling.
func TestFastFailCheckFilesStopFileOnFirstMissCondemnsGroup(t *testing.T) {
	client := fakepool.New()
	client.SetBehavior("p0-0", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})

	files := make([]FastFailFile, 3)
	for i := range files {
		files[i] = FastFailFile{
			Filename: fmt.Sprintf("set.part%02d.rar", i+1),
			Segments: makeTestSegments(fmt.Sprintf("p%d", i), 3),
			GroupKey: "set",
		}
	}

	results, err := FastFailCheckFiles(
		context.Background(), files,
		fastFailPoolManager{client: client},
		100, 1, 100*time.Millisecond,
		nil,
		nil,
		true,
		time.Time{},
	)
	if err != nil {
		t.Fatalf("FastFailCheckFiles error = %v, want nil", err)
	}
	for i, r := range results {
		if !r.Broken {
			t.Errorf("results[%d].Broken = false, want true", i)
		}
	}
	if got := client.StatCalls(); got != 1 {
		t.Errorf("StatCalls = %d, want 1 (the whole set is condemned by the first miss)", got)
	}
}

// TestFastFailReleaseProbeIgnoresPlaceholders pins that a gap the NZB itself
// declares is not a reason to skip the probe: the placeholder is never STATed
// and the probe still answers for the provider's copy of the real segments.
func TestFastFailReleaseProbeIgnoresPlaceholders(t *testing.T) {
	client := fakepool.New()
	placeholder := holes.PlaceholderID(2, "salt")
	files := []FastFailFile{{
		Filename: "release.part01.rar",
		GroupKey: "release",
		Segments: []*metapb.SegmentData{
			{Id: "rar-1"},
			{Id: placeholder},
			{Id: "rar-3"},
		},
	}}

	missing, err := FastFailReleaseProbe(context.Background(), files, fastFailPoolManager{client: client}, 100, 1, 100*time.Millisecond, nil, time.Time{})
	if err != nil {
		t.Fatalf("FastFailReleaseProbe error = %v", err)
	}
	if missing {
		t.Fatal("missing = true, want false: every real segment is reachable and a declared gap is not a provider miss")
	}
	if got := client.PerMessageCalls(placeholder); got != 0 {
		t.Errorf("placeholder STATed %d times, want 0", got)
	}
	if got := client.StatCalls(); got == 0 {
		t.Error("StatCalls = 0, want the real segments probed")
	}
}

// TestPlaceholderResultsMapsDeclaredGapsWithoutStats pins the STAT-free result
// shape for a release whose only damage is declared in the NZB.
func TestPlaceholderResultsMapsDeclaredGapsWithoutStats(t *testing.T) {
	p1, p2 := holes.PlaceholderID(2, "s"), holes.PlaceholderID(3, "s")
	files := []FastFailFile{
		{Filename: "release.part01.rar", GroupKey: "release", Segments: []*metapb.SegmentData{{Id: "a-1"}, {Id: "a-2"}}},
		{Filename: "release.part02.rar", GroupKey: "release", Segments: []*metapb.SegmentData{{Id: "b-1"}, {Id: p1}, {Id: p2}}},
		{Filename: "release.par2"},
	}

	results := PlaceholderResults(files)

	if len(results) != len(files) {
		t.Fatalf("len(results) = %d, want %d (index-aligned)", len(results), len(files))
	}
	if results[0].Broken || results[0].KnownGapCount != 0 || len(results[0].MissingSegmentIDs) != 0 || results[0].SampledCount != 0 {
		t.Errorf("gap-free file result = %+v, want zero value", results[0])
	}
	got := results[1]
	if !got.Broken || got.KnownGapCount != 2 || got.SampledCount != 0 {
		t.Errorf("gapped file result = %+v, want Broken with KnownGapCount 2 and no sample", got)
	}
	if len(got.MissingSegmentIDs) != 2 || got.MissingSegmentIDs[0] != p1 || got.MissingSegmentIDs[1] != p2 {
		t.Errorf("MissingSegmentIDs = %v, want [%s %s]", got.MissingSegmentIDs, p1, p2)
	}
	if results[2].Broken {
		t.Errorf("segment-less sidecar result = %+v, want zero value", results[2])
	}
}
