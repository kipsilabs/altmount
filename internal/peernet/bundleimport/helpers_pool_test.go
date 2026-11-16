package bundleimport

import (
	"context"

	"github.com/kipsilabs/altmount/internal/pool"
)

// fakePoolManager serves one client; the embedded interface is nil, so a
// method the stack should not call panics instead of silently passing.
type fakePoolManager struct {
	pool.Manager
	client pool.NntpClient
}

func (m *fakePoolManager) GetPool() (pool.NntpClient, error)           { return m.client, nil }
func (m *fakePoolManager) HasPool() bool                               { return true }
func (m *fakePoolManager) IncArticlesDownloaded()                      {}
func (m *fakePoolManager) UpdateDownloadProgress(_ string, _ int64)    {}
func (m *fakePoolManager) SpeculativeBudget() *pool.SpeculativeBudget  { return nil }
func (m *fakePoolManager) NotifyStreamChange()                         {}
func (m *fakePoolManager) SetStreamSource(_ pool.StreamActivitySource) {}
func (m *fakePoolManager) AcquireImportSlot(_ context.Context) (func(), error) {
	return func() {}, nil
}
func (m *fakePoolManager) AcquireImportConnection(_ context.Context) (func(), error) {
	return func() {}, nil
}
func (m *fakePoolManager) StatSweepConcurrency(c int) int { return c }
