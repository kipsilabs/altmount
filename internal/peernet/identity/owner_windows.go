//go:build windows

package identity

import "io/fs"

// ownedByCurrentUser cannot be checked portably on Windows; permissions are
// governed by ACLs there.
func ownedByCurrentUser(fs.FileInfo) bool { return true }
