package parser

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/pool"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/kipsilabs/altmount/internal/testsupport/segments"
	"github.com/javi11/nntppool/v5"
	"github.com/javi11/nzbparser"
)

// parser_storm_test.go pins the connection-budget invariants on the parser
// side. Since the removal of the per-import max_import_connections cap, the
// parser's fan-out is bounded GLOBALLY (across all concurrent imports) by the
// pool manager's import connection budget.

// fakeFullPoolManager satisfies the full pool.Manager surface so the
// parser can call GetPool, HasPool, and the metric inc helpers without
// nil-deref. When budget is set, AcquireImportConnection delegates to it,
// mirroring the real manager.
type fakeFullPoolManager struct {
	client pool.NntpClient
	budget *pool.ImportBudget
}

func newFakeFullPoolManager(client pool.NntpClient) *fakeFullPoolManager {
	return &fakeFullPoolManager{client: client}
}

var _ pool.Manager = (*fakeFullPoolManager)(nil)

func (m *fakeFullPoolManager) GetPool() (pool.NntpClient, error)        { return m.client, nil }
func (m *fakeFullPoolManager) SetProviders(_ []nntppool.Provider) error { return nil }
func (m *fakeFullPoolManager) ClearPool() error                         { return nil }
func (m *fakeFullPoolManager) HasPool() bool                            { return true }
func (m *fakeFullPoolManager) GetMetrics() (pool.MetricsSnapshot, error) {
	return pool.MetricsSnapshot{}, nil
}
func (m *fakeFullPoolManager) ResetMetrics(_ context.Context, _, _ bool) error { return nil }
func (m *fakeFullPoolManager) ResetProviderErrors(_ context.Context) error     { return nil }
func (m *fakeFullPoolManager) IncArticlesDownloaded()                          {}
func (m *fakeFullPoolManager) UpdateDownloadProgress(_ string, _ int64)        {}
func (m *fakeFullPoolManager) IncArticlesPosted()                              {}
func (m *fakeFullPoolManager) AddProvider(_ nntppool.Provider) error           { return nil }
func (m *fakeFullPoolManager) RemoveProvider(_ string) error                   { return nil }
func (m *fakeFullPoolManager) ResetProviderQuota(_ context.Context, _ string) error {
	return nil
}
func (m *fakeFullPoolManager) SetProviderIDs(_ map[string]string) {}
func (m *fakeFullPoolManager) AcquireImportSlot(_ context.Context) (func(), error) {
	return func() {}, nil
}
func (m *fakeFullPoolManager) SetAdmissionCap(_ int) {}
func (m *fakeFullPoolManager) AcquireImportConnection(ctx context.Context) (func(), error) {
	if m.budget != nil {
		return m.budget.Acquire(ctx)
	}
	return func() {}, nil
}
func (m *fakeFullPoolManager) SetImportConnCapacity(total int) {
	if m.budget != nil {
		m.budget.SetCapacity(total)
	}
}

func (m *fakeFullPoolManager) SetStreamHeadroom(int)                      {}
func (m *fakeFullPoolManager) SpeculativeBudget() *pool.SpeculativeBudget { return nil }
func (m *fakeFullPoolManager) ImportConnCapacity() int {
	if m.budget != nil {
		return m.budget.Capacity()
	}
	return 0
}
func (m *fakeFullPoolManager) SetStreamSource(_ pool.StreamActivitySource) {}
func (m *fakeFullPoolManager) NotifyStreamChange()                         {}
func (m *fakeFullPoolManager) StatSweepConcurrency(conservative int) int   { return conservative }

// stormConfigGetter returns a ConfigGetter whose provider capacity is exactly
// totalConnections. The parser sizes its fetch goroutine pool from
// TotalProviderConnections; the wire-call bound comes from the budget.
func stormConfigGetter(totalConnections int) config.ConfigGetter {
	cfg := config.DefaultConfig()
	enabled := true
	cfg.Providers = []config.ProviderConfig{
		{MaxConnections: totalConnections, Enabled: &enabled},
	}
	return func() *config.Config { return cfg }
}

// buildSyntheticNzbFiles returns numFiles nzbparser.NzbFile entries each
// pointing at a single fakepool-known message-ID. The parser's
// fetchAllFirstSegments path will issue one Body call per file — these
// are the calls we count.
func buildSyntheticNzbFiles(numFiles int) []nzbparser.NzbFile {
	files := make([]nzbparser.NzbFile, numFiles)
	for i := range files {
		files[i] = nzbparser.NzbFile{
			Filename: segments.MessageID(i) + ".bin",
			Segments: nzbparser.NzbSegments{
				{Bytes: 1024, Number: 1, ID: segments.MessageID(i)},
			},
		}
	}
	return files
}

// TestStorm_ImporterFanOutRespectsConnectionBudget pins the budget invariant:
// the parser's first-segment fan-out MUST stay bounded by the pool manager's
// import connection budget, and a single import MUST be able to fan out past
// the old fixed per-import cap of 5 when capacity allows.
func TestStorm_ImporterFanOutRespectsConnectionBudget(t *testing.T) {
	t.Parallel()
	const (
		filesPerImport = 20
		budgetCapacity = 8
		bodyLatency    = 100 * time.Millisecond
	)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fp := fakepool.New()
	for i := 0; i < filesPerImport; i++ {
		fp.SetBehavior(segments.MessageID(i), fakepool.SegmentBehavior{
			Latency: bodyLatency,
			Bytes:   make([]byte, 64),
		})
	}

	mgr := newFakeFullPoolManager(fp)
	mgr.budget = pool.NewImportBudget()
	mgr.budget.SetCapacity(budgetCapacity)
	parser := NewParser(mgr, stormConfigGetter(budgetCapacity))

	files := buildSyntheticNzbFiles(filesPerImport)
	_, _, _ = parser.fetchAllFirstSegments(ctx, files, nil, ParseOptions{})

	mif := fp.MaxInFlight()
	t.Logf("single import × %d files (budget capacity=%d) "+
		"produced MaxInFlight=%d Body calls (invariant: MaxInFlight <= capacity)",
		filesPerImport, budgetCapacity, mif)

	if mif > int32(budgetCapacity) {
		t.Errorf("INVARIANT regression: MaxInFlight=%d, want <= %d (budget capacity). "+
			"Every parser Body call must hold an AcquireImportConnection token — "+
			"if this fails, imports can saturate the pool past the adaptive budget.",
			mif, budgetCapacity)
	}
	// With 20 slow files and capacity 8, fan-out must exceed the old fixed
	// per-import cap of 5 — the whole point of removing max_import_connections.
	if mif <= 5 {
		t.Errorf("regression: MaxInFlight=%d, expected > 5. A single import should "+
			"expand to the pool's capacity, not the removed fixed cap.", mif)
	}
}

// TestStorm_ParallelImportsShareGlobalBudget asserts the cross-job bound the
// old design lacked: N concurrent imports share ONE budget, so global
// MaxInFlight stays <= capacity instead of scaling as N × per-import cap.
func TestStorm_ParallelImportsShareGlobalBudget(t *testing.T) {
	t.Parallel()
	const (
		concurrentImports = 4
		filesPerImport    = 6
		budgetCapacity    = 5
		bodyLatency       = 60 * time.Millisecond
	)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fp := fakepool.New()
	for i := 0; i < filesPerImport; i++ {
		fp.SetBehavior(segments.MessageID(i), fakepool.SegmentBehavior{
			Latency: bodyLatency,
			Bytes:   make([]byte, 64),
		})
	}

	mgr := newFakeFullPoolManager(fp)
	mgr.budget = pool.NewImportBudget()
	mgr.budget.SetCapacity(budgetCapacity)
	parser := NewParser(mgr, stormConfigGetter(budgetCapacity))

	var wg sync.WaitGroup
	for i := 0; i < concurrentImports; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			files := buildSyntheticNzbFiles(filesPerImport)
			_, _, _ = parser.fetchAllFirstSegments(ctx, files, nil, ParseOptions{})
		}()
	}
	wg.Wait()

	mif := fp.MaxInFlight()
	t.Logf("%d concurrent imports × %d files (budget capacity=%d) "+
		"produced global MaxInFlight=%d (invariant: MaxInFlight <= capacity)",
		concurrentImports, filesPerImport, budgetCapacity, mif)

	if mif > int32(budgetCapacity) {
		t.Errorf("INVARIANT regression: global MaxInFlight=%d across %d concurrent "+
			"imports, want <= %d. The import connection budget must bound total "+
			"in-flight fetches pool-wide, not per job.",
			mif, concurrentImports, budgetCapacity)
	}
}
