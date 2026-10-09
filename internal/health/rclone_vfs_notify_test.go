package health

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type vfsCall struct {
	VFSName string
	Dirs    []string
}

type mockRcloneClient struct {
	mu           sync.Mutex
	refreshCalls []vfsCall
	forgetCalls  []vfsCall
}

func (m *mockRcloneClient) RefreshDir(ctx context.Context, vfsName string, dirs []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.refreshCalls = append(m.refreshCalls, vfsCall{VFSName: vfsName, Dirs: dirs})
	return nil
}

func (m *mockRcloneClient) ForgetDir(ctx context.Context, vfsName string, dirs []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.forgetCalls = append(m.forgetCalls, vfsCall{VFSName: vfsName, Dirs: dirs})
	return nil
}

func (m *mockRcloneClient) GetMountInfo(provider string) (any, bool) {
	return nil, false
}

func (m *mockRcloneClient) IsReady() bool {
	return true
}

func TestHealthChecker_NotifyRcloneVFS(t *testing.T) {
	mockRclone := &mockRcloneClient{}
	cfg := &config.Config{
		MountType: config.MountTypeRCloneExternal,
		RClone: config.RCloneConfig{
			VFSName: "altmount_vfs",
		},
	}

	checker := &HealthChecker{
		rcloneClient: mockRclone,
		configGetter: func() *config.Config { return cfg },
	}

	checker.NotifyRcloneVFS("movies/Fight Club (1999)/Fight.Club.1999.mkv")

	// Allow async goroutine to execute
	require.Eventually(t, func() bool {
		mockRclone.mu.Lock()
		defer mockRclone.mu.Unlock()
		return len(mockRclone.refreshCalls) == 1
	}, 2*time.Second, 20*time.Millisecond)

	mockRclone.mu.Lock()
	defer mockRclone.mu.Unlock()
	assert.Equal(t, "altmount_vfs", mockRclone.refreshCalls[0].VFSName)
	assert.Equal(t, []string{"movies/Fight Club (1999)"}, mockRclone.refreshCalls[0].Dirs)
	assert.Empty(t, mockRclone.forgetCalls)
}

func TestHealthChecker_NotifyRcloneVFSDirs_Deduplication(t *testing.T) {
	mockRclone := &mockRcloneClient{}
	cfg := &config.Config{
		MountType: config.MountTypeRClone,
		RClone: config.RCloneConfig{
			VFSName: "altmount_vfs",
		},
	}

	checker := &HealthChecker{
		rcloneClient: mockRclone,
		configGetter: func() *config.Config { return cfg },
	}

	checker.NotifyRcloneVFSDirs([]string{"movies/DirA", "movies/DirB", "movies/DirA"})

	require.Eventually(t, func() bool {
		mockRclone.mu.Lock()
		defer mockRclone.mu.Unlock()
		return len(mockRclone.refreshCalls) == 1
	}, 2*time.Second, 20*time.Millisecond)

	mockRclone.mu.Lock()
	defer mockRclone.mu.Unlock()
	assert.Equal(t, []string{"movies/DirA", "movies/DirB"}, mockRclone.refreshCalls[0].Dirs)
	assert.Empty(t, mockRclone.forgetCalls)
}

func TestHealthChecker_NotifyRcloneVFSForget(t *testing.T) {
	mockRclone := &mockRcloneClient{}
	cfg := &config.Config{
		MountType: config.MountTypeRCloneExternal,
		RClone: config.RCloneConfig{
			VFSName: "altmount_vfs",
		},
	}

	checker := &HealthChecker{
		rcloneClient: mockRclone,
		configGetter: func() *config.Config { return cfg },
	}

	checker.NotifyRcloneVFSForget("tv/SpongeBob.S17E15/ep.mkv")

	require.Eventually(t, func() bool {
		mockRclone.mu.Lock()
		defer mockRclone.mu.Unlock()
		return len(mockRclone.forgetCalls) == 1
	}, 2*time.Second, 20*time.Millisecond)

	mockRclone.mu.Lock()
	defer mockRclone.mu.Unlock()
	assert.Equal(t, "altmount_vfs", mockRclone.forgetCalls[0].VFSName)
	assert.Equal(t, []string{"tv/SpongeBob.S17E15"}, mockRclone.forgetCalls[0].Dirs)
	// Deletion must not trigger an eager re-enumeration
	assert.Empty(t, mockRclone.refreshCalls)
}

func TestHealthChecker_NotifyRcloneVFSDirsForget_Deduplication(t *testing.T) {
	mockRclone := &mockRcloneClient{}
	cfg := &config.Config{
		MountType: config.MountTypeRClone,
		RClone: config.RCloneConfig{
			VFSName: "altmount_vfs",
		},
	}

	checker := &HealthChecker{
		rcloneClient: mockRclone,
		configGetter: func() *config.Config { return cfg },
	}

	checker.NotifyRcloneVFSDirsForget([]string{"movies/DirA", "movies/DirB", "movies/DirA", ""})

	require.Eventually(t, func() bool {
		mockRclone.mu.Lock()
		defer mockRclone.mu.Unlock()
		return len(mockRclone.forgetCalls) == 1
	}, 2*time.Second, 20*time.Millisecond)

	mockRclone.mu.Lock()
	defer mockRclone.mu.Unlock()
	assert.Equal(t, []string{"movies/DirA", "movies/DirB", "/"}, mockRclone.forgetCalls[0].Dirs)
	assert.Empty(t, mockRclone.refreshCalls)
}
