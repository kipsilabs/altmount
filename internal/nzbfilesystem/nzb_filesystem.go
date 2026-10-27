package nzbfilesystem

import (
	"context"
	"errors"
	"io/fs"
	"os"

	"github.com/kipsilabs/altmount/internal/slogutil"
	"github.com/kipsilabs/altmount/internal/utils"
	"github.com/spf13/afero"
)

// NzbFilesystem implements afero.Fs interface directly using the metadata service
type NzbFilesystem struct {
	remoteFile *MetadataRemoteFile
}

// NewNzbFilesystem creates a new filesystem backed directly by metadata
func NewNzbFilesystem(remoteFile *MetadataRemoteFile) *NzbFilesystem {
	return &NzbFilesystem{
		remoteFile: remoteFile,
	}
}

// Name returns the filesystem name
func (nfs *NzbFilesystem) Name() string {
	return "altmount"
}

// Open opens a file for reading
func (nfs *NzbFilesystem) Open(ctx context.Context, name string) (afero.File, error) {
	ctx = slogutil.With(ctx, "file_name", name)

	// Try to open with NZB remote file
	ok, file, err := nfs.remoteFile.OpenFile(ctx, name)
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, os.ErrNotExist
	}

	return file, nil
}

// OpenFile opens a file with specified flags and permissions
func (nfs *NzbFilesystem) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (afero.File, error) {
	// Only allow read operations
	if flag != os.O_RDONLY {
		return nil, os.ErrPermission
	}

	// Check for COPY operations from context
	// Block COPY operations entirely - they should use MOVE instead
	if isCopy, ok := ctx.Value(utils.IsCopy).(bool); ok && isCopy {
		return nil, os.ErrPermission
	}

	return nfs.Open(ctx, name)
}

// Stat returns file information
func (nfs *NzbFilesystem) Stat(ctx context.Context, name string) (fs.FileInfo, error) {
	ok, info, err := nfs.remoteFile.Stat(ctx, name)
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, os.ErrNotExist
	}

	return info, nil
}

// Remove removes a file (not supported)
func (nfs *NzbFilesystem) Remove(ctx context.Context, name string) (err error) {
	defer func() {
		if !errors.Is(err, os.ErrPermission) && nfs.remoteFile.healthRepository != nil {
			_ = nfs.remoteFile.healthRepository.DeleteHealthRecord(ctx, name)
		}
	}()

	ok, err := nfs.remoteFile.RemoveFile(ctx, name)
	if err != nil {
		return err
	}

	if !ok {
		return os.ErrNotExist
	}

	return nil
}

// RemoveAll removes a file and any children
func (nfs *NzbFilesystem) RemoveAll(ctx context.Context, name string) error {
	err := nfs.Remove(ctx, name)
	if err != nil && os.IsNotExist(err) {
		// If the file/directory is already gone, consider it a success
		// This prevents Sonarr/Radarr from crashing when trying to delete folders we've already cleaned up
		return nil
	}
	return err
}

// Rename renames a file (not supported)
func (nfs *NzbFilesystem) Rename(ctx context.Context, oldName, newName string) error {
	ok, err := nfs.remoteFile.RenameFile(ctx, oldName, newName)
	if err != nil {
		return err
	}

	if !ok {
		return os.ErrNotExist
	}

	return nil
}

// Mkdir creates a directory (not supported - read-only filesystem)
func (nfs *NzbFilesystem) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	return nfs.remoteFile.Mkdir(ctx, name, perm)
}

// MkdirAll creates a directory and all parent directories (not supported)
func (nfs *NzbFilesystem) MkdirAll(ctx context.Context, name string, perm os.FileMode) error {
	return nfs.remoteFile.MkdirAll(ctx, name, perm)
}
