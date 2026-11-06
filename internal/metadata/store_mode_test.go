//go:build unix

package metadata

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// WriteStore goes through os.CreateTemp, which always creates 0600, so a
// backup job running under another uid could not read the resulting .nzbz
// files. The file must end up 0644, whatever the process umask, both for a new
// ref and when an existing ref is replaced.
func TestStoreService_WriteStore_FileMode0644(t *testing.T) {
	old := syscall.Umask(0o077)
	defer syscall.Umask(old)

	ss := NewStoreService(t.TempDir())
	ref := filepath.Join(t.TempDir(), "sub", "rel.nzbz")

	require.NoError(t, ss.WriteStore(ref, sampleStore()))
	fi, err := os.Stat(ref)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o644), fi.Mode().Perm(), "mode after first write")

	// Overwrite an existing ref: the rename replaces it with a fresh temp file.
	require.NoError(t, os.Chmod(ref, 0o600))
	require.NoError(t, ss.WriteStore(ref, sampleStore()))
	fi, err = os.Stat(ref)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o644), fi.Mode().Perm(), "mode after overwrite")
}
