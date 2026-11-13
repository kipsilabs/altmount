//go:build !windows

package identity

import (
	"io/fs"
	"os"
	"syscall"
)

// ownedByCurrentUser reports whether the file belongs to the effective user.
// Where the owner cannot be determined it reports true.
func ownedByCurrentUser(fi fs.FileInfo) bool {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return true
	}
	return int(st.Uid) == os.Geteuid()
}
