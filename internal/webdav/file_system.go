package webdav

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"

	"github.com/kipsilabs/altmount/internal/nzbfilesystem"
	"github.com/kipsilabs/altmount/internal/slogutil"
)

type fileSystem struct {
	nzbFs *nzbfilesystem.NzbFilesystem
}

func nzbToWebdavFS(nzbFs *nzbfilesystem.NzbFilesystem) FileSystem {
	return &fileSystem{
		nzbFs: nzbFs,
	}
}

func (fs *fileSystem) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	return fs.nzbFs.Mkdir(ctx, name, perm)
}

func (fs *fileSystem) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (File, error) {
	return fs.nzbFs.OpenFile(ctx, name, flag, perm)
}

func (fs *fileSystem) RemoveAll(ctx context.Context, name string) error {
	return fs.nzbFs.RemoveAll(ctx, name)
}

func (fs *fileSystem) Rename(ctx context.Context, oldName, newName string) error {
	return fs.nzbFs.Rename(ctx, oldName, newName)
}

func (fs *fileSystem) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	return fs.nzbFs.Stat(ctx, name)
}

// HTTPError represents an HTTP error with a specific status code
type HTTPError struct {
	StatusCode int
	Message    string
	Err        error
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", e.StatusCode, e.Message)
}

func (e *HTTPError) Unwrap() error {
	return e.Err
}

// customErrorHandler wraps a FileSystem and maps our custom errors to HTTP status codes.
// Mkdir, RemoveAll, Rename, and Stat are promoted from the embedded FileSystem.
type customErrorHandler struct {
	FileSystem
}

func (c *customErrorHandler) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (File, error) {
	slog.DebugContext(ctx, "WebDAV opening file", "name", name)
	file, err := c.FileSystem.OpenFile(ctx, name, flag, perm)
	if err != nil {
		return nil, c.mapError(err)
	}

	ctx = slogutil.With(
		ctx,
		"file_name", name,
	)

	return &errorHandlingFile{
		File: file,
		ctx:  ctx,
	}, nil
}

// mapError converts our custom errors to appropriate HTTP errors
func (c *customErrorHandler) mapError(err error) error {
	var partialErr *nzbfilesystem.PartialContentError
	var corruptedErr *nzbfilesystem.CorruptedFileError

	if errors.As(err, &partialErr) {
		return &HTTPError{
			StatusCode: http.StatusPartialContent,
			Message:    "Partial content available due to missing articles",
			Err:        err,
		}
	}

	if errors.As(err, &corruptedErr) || errors.Is(err, nzbfilesystem.ErrFileIsCorrupted) {
		return &HTTPError{
			StatusCode: http.StatusNotFound,
			Message:    "File unavailable due to missing articles",
			Err:        err,
		}
	}

	// An unsatisfiable client Range is a request error (416), never a
	// corruption verdict: the file is fine, the window is not.
	if errors.Is(err, nzbfilesystem.ErrInvalidRange) {
		return &HTTPError{
			StatusCode: http.StatusRequestedRangeNotSatisfiable,
			Message:    "Requested range not satisfiable",
			Err:        err,
		}
	}

	return err
}

// errorHandlingFile wraps a File and handles read errors from our virtual files
type errorHandlingFile struct {
	File
	ctx context.Context
}

func (f *errorHandlingFile) Read(p []byte) (int, error) {
	n, err := f.File.Read(p)
	if err != nil && err != io.EOF {
		var partialErr *nzbfilesystem.PartialContentError
		var corruptedErr *nzbfilesystem.CorruptedFileError

		if errors.As(err, &partialErr) {
			slog.WarnContext(f.ctx, "Partial content due to missing articles",
				"bytes_read", partialErr.BytesRead,
				"total_expected", partialErr.TotalExpected)
			return n, &HTTPError{
				StatusCode: http.StatusPartialContent,
				Message:    "Partial content available due to missing articles",
				Err:        err,
			}
		}

		if errors.As(err, &corruptedErr) {
			slog.ErrorContext(f.ctx, "File corrupted",
				"total_expected", corruptedErr.TotalExpected,
				"error", corruptedErr.UnderlyingErr)
			return n, &HTTPError{
				StatusCode: http.StatusServiceUnavailable,
				Message:    "File unavailable due to missing articles",
				Err:        err,
			}
		}

		// An unsatisfiable client Range surfaces as 416, not 503: the
		// file is healthy, the requested window is not.
		if errors.Is(err, nzbfilesystem.ErrInvalidRange) {
			return n, &HTTPError{
				StatusCode: http.StatusRequestedRangeNotSatisfiable,
				Message:    "Requested range not satisfiable",
				Err:        err,
			}
		}
	}

	return n, err
}
