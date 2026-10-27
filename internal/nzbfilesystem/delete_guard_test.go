package nzbfilesystem

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/metadata"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func deleteGuardFixture(t *testing.T, settings string) (*MetadataRemoteFile, *metadata.MetadataService) {
	t.Helper()
	cfg := config.DefaultConfig()
	require.NoError(t, yaml.Unmarshal([]byte(settings), &cfg.SABnzbd))
	ms := metadata.NewMetadataService(t.TempDir())
	return &MetadataRemoteFile{metadataService: ms, configGetter: func() *config.Config { return cfg }}, ms
}

func TestRemoveFile_RecursiveCategoryWalk(t *testing.T) {
	for _, limit := range []int{3, -1} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			mrf, ms := deleteGuardFixture(t, fmt.Sprintf("delete_guard_max_folders: %d\ndelete_guard_window_minutes: 10", limit))
			for i := range 6 {
				writeStreamMeta(t, ms, fmt.Sprintf("complete/tv/release%d/movie.mkv", i))
			}
			for i := range 6 {
				name := fmt.Sprintf("complete/tv/release%d/movie.mkv", i)
				_, err := mrf.RemoveFile(context.Background(), name)
				if limit > 0 && i >= limit {
					require.ErrorIs(t, err, os.ErrPermission)
					require.True(t, ms.FileExists(name))
					_, err = mrf.RemoveFile(context.Background(), fmt.Sprintf("complete/tv/release%d", i))
					require.ErrorIs(t, err, os.ErrPermission)
					require.True(t, ms.FileExists(name))
				} else {
					require.NoError(t, err)
					require.False(t, ms.FileExists(name))
				}
			}
		})
	}
}

func TestRemoveFile_DeleteGuardDefaultsAndDisabledWindow(t *testing.T) {
	for _, settings := range []string{"", "delete_guard_max_folders: 0\ndelete_guard_window_minutes: 0", "delete_guard_max_folders: 1\ndelete_guard_window_minutes: -1"} {
		t.Run(settings, func(t *testing.T) {
			mrf, ms := deleteGuardFixture(t, settings)
			for i := range 51 {
				name := fmt.Sprintf("complete/tv/release%d/file.mkv", i)
				writeStreamMeta(t, ms, name)
				_, err := mrf.RemoveFile(context.Background(), name)
				if i == 50 && settings != "delete_guard_max_folders: 1\ndelete_guard_window_minutes: -1" {
					require.ErrorIs(t, err, os.ErrPermission)
				} else {
					require.NoError(t, err)
				}
			}
		})
	}
}

func TestRemoveFile_SeasonPackAndOutsideCompleteDir(t *testing.T) {
	mrf, ms := deleteGuardFixture(t, "delete_guard_max_folders: 1")
	for i := range 60 {
		writeStreamMeta(t, ms, fmt.Sprintf("complete/tv/pack/episode%d.mkv", i))
	}
	for i := range 60 {
		_, err := mrf.RemoveFile(context.Background(), fmt.Sprintf("complete/tv/pack/episode%d.mkv", i))
		require.NoError(t, err)
	}
	for _, name := range []string{"elsewhere/tv/one/file.mkv", "complete-other/tv/two/file.mkv", "complete/movies/one/file.mkv"} {
		writeStreamMeta(t, ms, name)
		_, err := mrf.RemoveFile(context.Background(), name)
		require.NoError(t, err)
	}
	writeStreamMeta(t, ms, "complete/tv/next/file.mkv")
	_, err := mrf.RemoveFile(context.Background(), "complete/tv/next/file.mkv")
	require.ErrorIs(t, err, os.ErrPermission)
}

func TestRemove_PermissionPreservesHealthRecord(t *testing.T) {
	repo, db, ms := setupStreamHealthEnv(t)
	_, err := db.Exec(`INSERT INTO file_health (file_path, status) VALUES ('corrupted_metadata', 'healthy')`)
	require.NoError(t, err)
	nfs := NewNzbFilesystem(&MetadataRemoteFile{metadataService: ms, healthRepository: repo})
	require.ErrorIs(t, nfs.Remove(context.Background(), "corrupted_metadata"), os.ErrPermission)
	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM file_health WHERE file_path = 'corrupted_metadata'`).Scan(&count))
	require.Equal(t, 1, count)
}

func TestDeleteGuard_CooldownRequiresQuietWindow(t *testing.T) {
	var g deleteGuard
	now := time.Now()
	window := 10 * time.Minute
	r, _, err := g.reserve("tv", "one", 1, window, now)
	require.NoError(t, err)
	g.finish(r, true, now)
	_, warn, err := g.reserve("tv", "two", 1, window, now)
	require.ErrorIs(t, err, os.ErrPermission)
	require.True(t, warn)
	// Even a previously admitted release is blocked once the guard trips.
	_, warn, err = g.reserve("tv", "one", 1, window, now.Add(time.Second))
	require.ErrorIs(t, err, os.ErrPermission)
	require.False(t, warn)
	_, warn, err = g.reserve("tv", "two", 1, window, now.Add(window))
	require.ErrorIs(t, err, os.ErrPermission)
	require.True(t, warn)
	_, _, err = g.reserve("tv", "two", 1, window, now.Add(2*window))
	require.NoError(t, err)
}

func TestDeleteGuard_FailedAndInFlightDeletes(t *testing.T) {
	var g deleteGuard
	now := time.Now()
	window := time.Minute
	one, _, err := g.reserve("tv", "one", 1, window, now)
	require.NoError(t, err)
	other, _, err := g.reserve("tv", "one", 1, window, now)
	require.NoError(t, err)
	g.finish(one, false, now)
	// One failed sibling must not free the other sibling's in-flight slot,
	// even if its operation takes longer than the window.
	_, _, err = g.reserve("tv", "two", 1, window, now.Add(2*window))
	require.ErrorIs(t, err, os.ErrPermission)
	g.finish(other, false, now.Add(2*window))
	_, _, err = g.reserve("tv", "two", 1, window, now.Add(3*window))
	require.NoError(t, err)

	var failed deleteGuard
	r, _, err := failed.reserve("tv", "failed", 1, window, now)
	require.NoError(t, err)
	failed.finish(r, false, now)
	_, _, err = failed.reserve("tv", "next", 1, window, now)
	require.NoError(t, err, "a failed delete must return its slot immediately")
}

func TestDeleteGuard_SuccessExpiresAndFailureKeepsSuccessfulSlot(t *testing.T) {
	var g deleteGuard
	now := time.Now()
	r, _, err := g.reserve("tv", "one", 1, time.Minute, now)
	require.NoError(t, err)
	g.finish(r, true, now)
	r, _, err = g.reserve("tv", "one", 1, time.Minute, now.Add(time.Second))
	require.NoError(t, err)
	g.finish(r, false, now.Add(time.Second))
	_, _, err = g.reserve("tv", "two", 1, time.Minute, now.Add(time.Minute))
	require.NoError(t, err, "a failed sibling must not extend the successful deletion window")
}

func TestDeleteGuard_ParallelWalkersShareLimit(t *testing.T) {
	var g deleteGuard
	var wg sync.WaitGroup
	results := make(chan *deleteReservation, 30)
	for i := range 30 {
		wg.Go(func() {
			r, _, _ := g.reserve("tv", fmt.Sprint(i), 3, time.Minute, time.Now())
			results <- r
		})
	}
	wg.Wait()
	close(results)
	admitted := 0
	for r := range results {
		if r != nil {
			admitted++
			g.finish(r, true, time.Now())
		}
	}
	require.Equal(t, 3, admitted)
}

func TestRemoveFile_DeleteGuardPaths(t *testing.T) {
	for _, base := range []string{"/complete", "/"} {
		t.Run(base, func(t *testing.T) {
			mrf, ms := deleteGuardFixture(t, "complete_dir: "+base+"\ndelete_guard_max_folders: 1")
			prefix := "tv/"
			if base != "/" {
				prefix = "complete/" + prefix
			}
			_, err := mrf.RemoveFile(context.Background(), prefix+"missing/file.mkv")
			require.NoError(t, err)
			writeStreamMeta(t, ms, prefix+"loose.mkv")
			_, err = mrf.RemoveFile(context.Background(), prefix+"loose.mkv")
			require.NoError(t, err)
			writeStreamMeta(t, ms, prefix+"release/file.mkv")
			_, err = mrf.RemoveFile(context.Background(), prefix+"release")
			require.ErrorIs(t, err, os.ErrPermission)
			require.True(t, ms.FileExists(prefix+"release/file.mkv"))
		})
	}
}

func TestRemoveFile_ProtectsUnconfiguredCategoryAndCompleteAncestors(t *testing.T) {
	for _, tc := range []struct{ base, target, file string }{
		{"/complete", "complete/old-tv", "complete/old-tv/release/file.mkv"},
		{"/downloads/complete", "downloads", "downloads/complete/tv/release/file.mkv"},
		{"/", "old-tv", "old-tv/release/file.mkv"},
	} {
		t.Run(tc.target, func(t *testing.T) {
			mrf, ms := deleteGuardFixture(t, "complete_dir: "+tc.base)
			writeStreamMeta(t, ms, tc.file)
			_, err := mrf.RemoveFile(context.Background(), tc.target)
			require.NoError(t, err)
			require.True(t, ms.FileExists(tc.file), "recursive directory delete must preserve protected categories")
		})
	}
}

func TestRemove_GuardPreservesRefusedFileHealth(t *testing.T) {
	repo, db, ms := setupStreamHealthEnv(t)
	cfg := config.DefaultConfig()
	cfg.SABnzbd.DeleteGuardMaxFolders = 1
	nfs := NewNzbFilesystem(&MetadataRemoteFile{metadataService: ms, healthRepository: repo, configGetter: func() *config.Config { return cfg }})
	for _, release := range []string{"one", "two"} {
		name := "complete/tv/" + release + "/file.mkv"
		writeStreamMeta(t, ms, name)
		_, err := db.Exec(`INSERT INTO file_health (file_path, status) VALUES (?, 'healthy')`, name)
		require.NoError(t, err)
	}
	require.NoError(t, nfs.Remove(context.Background(), "complete/tv/one/file.mkv"))
	require.ErrorIs(t, nfs.Remove(context.Background(), "complete/tv/two/file.mkv"), os.ErrPermission)
	require.True(t, ms.FileExists("complete/tv/two/file.mkv"))
	var name string
	require.NoError(t, db.QueryRow(`SELECT file_path FROM file_health`).Scan(&name))
	require.Equal(t, "complete/tv/two/file.mkv", name)
}
