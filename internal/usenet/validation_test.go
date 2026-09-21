package usenet

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/pool"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/javi11/nntppool/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSelectSegmentsForValidation(t *testing.T) {
	// Use a deterministic RNG for predictability in middle segments.
	rng := rand.New(rand.NewSource(1))
	previousRandPerm := randPerm
	randPerm = rng.Perm
	t.Cleanup(func() {
		randPerm = previousRandPerm
	})

	// Create 100 dummy segments
	segments := make([]*metapb.SegmentData, 100)
	for i := range 100 {
		segments[i] = &metapb.SegmentData{Id: fmt.Sprintf("seg%d", i)}
	}

	t.Run("100 percent", func(t *testing.T) {
		selected := selectSegmentsForValidation(segments, 100)
		assert.Equal(t, 100, len(selected))
	})

	t.Run("10 percent", func(t *testing.T) {
		selected := selectSegmentsForValidation(segments, 10)
		// 10% of 100 = 10 segments
		assert.Equal(t, 10, len(selected))

		// Should include first 3
		assert.Equal(t, "seg0", selected[0].Id)
		assert.Equal(t, "seg1", selected[1].Id)
		assert.Equal(t, "seg2", selected[2].Id)

		// Should include last 2
		found98 := false
		found99 := false
		for _, s := range selected {
			if s.Id == "seg98" {
				found98 = true
			}
			if s.Id == "seg99" {
				found99 = true
			}
		}
		assert.True(t, found98, "Should include seg98")
		assert.True(t, found99, "Should include seg99")
	})

	t.Run("minimum 5", func(t *testing.T) {
		// 1% of 100 = 1 segment, but minimum is 5
		selected := selectSegmentsForValidation(segments, 1)
		assert.Equal(t, 5, len(selected))
	})

	t.Run("large files honor the configured percentage", func(t *testing.T) {
		// Regression for #812: a hard 55-sample cap made every sub-100%
		// setting sample the same handful of segments on large releases.
		largeSegments := make([]*metapb.SegmentData, 20000)
		for i := range 20000 {
			largeSegments[i] = &metapb.SegmentData{Id: fmt.Sprintf("seg%d", i)}
		}

		assert.Equal(t, 2000, len(selectSegmentsForValidation(largeSegments, 10)))
		assert.Equal(t, 10000, len(selectSegmentsForValidation(largeSegments, 50)))
		// Higher percentages must sample strictly more than lower ones.
		assert.Greater(t,
			len(selectSegmentsForValidation(largeSegments, 25)),
			len(selectSegmentsForValidation(largeSegments, 10)),
		)
	})
}

// validationTestPoolManager is a minimal pool.Manager for validation tests.
// It wraps a fakepool.Client and no-ops everything else.
type validationTestPoolManager struct {
	client pool.NntpClient
}

var _ pool.Manager = (*validationTestPoolManager)(nil)

func (m *validationTestPoolManager) GetPool() (pool.NntpClient, error)        { return m.client, nil }
func (m *validationTestPoolManager) SetProviders(_ []nntppool.Provider) error { return nil }
func (m *validationTestPoolManager) ClearPool() error                         { return nil }
func (m *validationTestPoolManager) HasPool() bool                            { return true }
func (m *validationTestPoolManager) GetMetrics() (pool.MetricsSnapshot, error) {
	return pool.MetricsSnapshot{}, nil
}
func (m *validationTestPoolManager) ResetMetrics(_ context.Context, _, _ bool) error { return nil }
func (m *validationTestPoolManager) ResetProviderErrors(_ context.Context) error     { return nil }
func (m *validationTestPoolManager) IncArticlesDownloaded()                          {}
func (m *validationTestPoolManager) UpdateDownloadProgress(_ string, _ int64)        {}
func (m *validationTestPoolManager) IncArticlesPosted()                              {}
func (m *validationTestPoolManager) AddProvider(_ nntppool.Provider) error           { return nil }
func (m *validationTestPoolManager) RemoveProvider(_ string) error                   { return nil }
func (m *validationTestPoolManager) ResetProviderQuota(_ context.Context, _ string) error {
	return nil
}
func (m *validationTestPoolManager) SetProviderIDs(_ map[string]string) {}
func (m *validationTestPoolManager) AcquireImportSlot(_ context.Context) (func(), error) {
	return func() {}, nil
}
func (m *validationTestPoolManager) SetAdmissionCap(_ int) {}
func (m *validationTestPoolManager) AcquireImportConnection(_ context.Context) (func(), error) {
	return func() {}, nil
}
func (m *validationTestPoolManager) SetImportConnCapacity(_ int)                 {}
func (m *validationTestPoolManager) ImportConnCapacity() int                     { return 0 }
func (m *validationTestPoolManager) SetStreamSource(_ pool.StreamActivitySource) {}
func (m *validationTestPoolManager) NotifyStreamChange()                         {}
func (m *validationTestPoolManager) StatSweepConcurrency(conservative int) int   { return conservative }

// TestValidateSegmentAvailability_TransientErrorsAreInconclusive is the
// regression guard for #861: a STAT that fails for an operational reason
// (dead connection, timeout, quota, auth, 502, exhausted pool) says nothing
// about whether the article exists. Counting it as missing let a transient
// backup-provider failure be written into the file's permanent hole map.
func TestValidateSegmentAvailability_TransientErrorsAreInconclusive(t *testing.T) {
	operational := []struct {
		name string
		err  error
	}{
		{"connection died", nntppool.ErrConnectionDied},
		{"quota exceeded", nntppool.ErrQuotaExceeded},
		{"service unavailable", nntppool.ErrServiceUnavailable},
		{"auth rejected", nntppool.ErrAuthRejected},
		{"deadline exceeded", context.DeadlineExceeded},
		{"providers exhausted", fmt.Errorf("nntp: all providers exhausted: %w", nntppool.ErrConnectionDied)},
	}

	for _, tc := range operational {
		t.Run(tc.name, func(t *testing.T) {
			fp := fakepool.New()
			mgr := &validationTestPoolManager{client: fp}
			const id = "flaky@host"
			fp.SetBehavior(id, fakepool.SegmentBehavior{Err: tc.err})

			results, err := ValidateSegmentAvailabilityBatch(
				context.Background(), [][]string{{id}}, mgr, BatchOptions{MaxConnections: 1, Timeout: 5 * time.Second})
			assert.NoError(t, err)
			require.Len(t, results, 1)

			assert.Zero(t, results[0].MissingCount, "operational error must not count as missing")
			assert.Empty(t, results[0].MissingIDs, "operational error must not yield a hole id")
			assert.Equal(t, 1, results[0].UnresolvedCount)
			assert.True(t, results[0].Inconclusive())
			assert.ErrorIs(t, results[0].Err, tc.err)
		})
	}
}

func (m *validationTestPoolManager) SetStreamHeadroom(int)                      {}
func (m *validationTestPoolManager) SpeculativeBudget() *pool.SpeculativeBudget { return nil }

// TestValidateSegmentAvailability_ArticleNotFoundIsMissing keeps the genuine
// miss path intact: only 430/423 proves the article is gone.
func TestValidateSegmentAvailability_ArticleNotFoundIsMissing(t *testing.T) {
	fp := fakepool.New()
	mgr := &validationTestPoolManager{client: fp}
	const id = "gone@host"
	fp.SetBehavior(id, fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})

	results, err := ValidateSegmentAvailabilityBatch(
		context.Background(), [][]string{{id}}, mgr, BatchOptions{MaxConnections: 1, Timeout: 5 * time.Second})
	assert.NoError(t, err)
	require.Len(t, results, 1)

	assert.Equal(t, 1, results[0].MissingCount)
	assert.Equal(t, []string{id}, results[0].MissingIDs)
	assert.Zero(t, results[0].UnresolvedCount)
	assert.False(t, results[0].Inconclusive())
}

// TestValidateSegmentAvailabilityBatch_InconclusiveIsPerFile verifies one
// file's transient failure does not taint its batch siblings' verdicts.
func TestValidateSegmentAvailabilityBatch_InconclusiveIsPerFile(t *testing.T) {
	fp := fakepool.New()
	mgr := &validationTestPoolManager{client: fp}
	fp.SetBehavior("flaky@host", fakepool.SegmentBehavior{Err: nntppool.ErrConnectionDied})
	fp.SetBehavior("gone@host", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})

	results, err := ValidateSegmentAvailabilityBatch(context.Background(), [][]string{
		{"ok@host"},
		{"flaky@host"},
		{"gone@host"},
	}, mgr, BatchOptions{MaxConnections: 3, Timeout: 5 * time.Second})
	assert.NoError(t, err)
	require.Len(t, results, 3)

	assert.False(t, results[0].Inconclusive())
	assert.Zero(t, results[0].MissingCount)

	assert.True(t, results[1].Inconclusive())
	assert.Zero(t, results[1].MissingCount)

	assert.False(t, results[2].Inconclusive())
	assert.Equal(t, 1, results[2].MissingCount)
}

// TestValidateSegmentAvailabilityBatch_UnreportedSegmentsAreInconclusive
// covers the other half of #861: StatMany emits nothing for ids whose
// per-chunk deadline elapsed before they were dispatched. Treating that
// silence as "checked and present" made a truncated sweep declare a file
// healthy on no evidence at all — it must instead mark the result
// inconclusive.
func TestValidateSegmentAvailabilityBatch_UnreportedSegmentsAreInconclusive(t *testing.T) {
	fp := fakepool.New()
	// Every stat blocks past the chunk deadline, so the ids are abandoned
	// without ever reporting a result.
	fp.SetDefaultBehavior(fakepool.SegmentBehavior{Latency: 200 * time.Millisecond})
	mgr := &validationTestPoolManager{client: fp}

	results, err := ValidateSegmentAvailabilityBatch(
		context.Background(), [][]string{{"a@host", "b@host"}}, mgr,
		BatchOptions{MaxConnections: 2, Timeout: 10 * time.Millisecond})
	assert.NoError(t, err)
	require.Len(t, results, 1)

	assert.Zero(t, results[0].MissingCount)
	assert.Empty(t, results[0].MissingIDs)
	assert.Equal(t, 2, results[0].UnresolvedCount)
	assert.True(t, results[0].Inconclusive())
	assert.ErrorIs(t, results[0].Err, context.DeadlineExceeded)
}
