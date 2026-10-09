package nzbfilesystem

import (
	"errors"
	"fmt"

	"github.com/kipsilabs/altmount/internal/config"
)

// File system constants
const (
	// RootPath represents the root directory path
	RootPath = "/"

	// corruptedDirName is the metadata-root folder holding corrupted safety copies.
	corruptedDirName = "corrupted_metadata"
)

// Error constants
var (
	ErrInvalidWhence = errors.New("seek: invalid whence")
	ErrSeekNegative  = errors.New("seek: negative position")
	ErrSeekTooFar    = errors.New("seek: too far")
)

// Article availability error types

// PartialContentError represents an error where some articles are missing but some content was read
type PartialContentError struct {
	BytesRead     int64
	TotalExpected int64
	UnderlyingErr error
}

func (e *PartialContentError) Error() string {
	return fmt.Sprintf("partial content: read %d/%d bytes, underlying error: %v",
		e.BytesRead, e.TotalExpected, e.UnderlyingErr)
}

func (e *PartialContentError) Unwrap() error {
	return e.UnderlyingErr
}

// CorruptedFileError represents an error where no articles could be read (complete failure)
type CorruptedFileError struct {
	TotalExpected int64
	UnderlyingErr error
}

func (e *CorruptedFileError) Error() string {
	return fmt.Sprintf("corrupted file: no content available from %d expected bytes, underlying error: %v",
		e.TotalExpected, e.UnderlyingErr)
}

func (e *CorruptedFileError) Unwrap() error {
	return e.UnderlyingErr
}

// Error message constants
var (
	ErrCannotRemoveRoot    = errors.New("cannot remove root directory")
	ErrNotDirectory        = errors.New("not a directory")
	ErrCannotReadDirectory = errors.New("cannot read from directory")
	ErrNegativeOffset      = errors.New("negative offset")
	ErrMissmatchedSegments = errors.New("missmatched segments for file size")
	ErrNoUsenetPool        = errors.New("usenet connection pool not configured")
	ErrNoCipherConfig      = errors.New("no cipher configured for encryption")
	ErrNoEncryptionParams  = errors.New("no NZB data available for encryption parameters")
	ErrFileIsCorrupted     = errors.New("file is corrupted, there are some missing segments")
	ErrFileClosed          = errors.New("file closed")
	// ErrInvalidRange is returned when a client-supplied byte range cannot be
	// satisfied (negative start after normalization, start past EOF, empty
	// window). It is a request error, never a corruption verdict, and must
	// not reach the health/repair pipeline.
	ErrInvalidRange = errors.New("invalid byte range")
	// ErrReadTimeout is returned when a single read exceeds
	// streaming.read_timeout_seconds. Deliberately not a context error: the
	// FUSE handles map context errors to EINTR (a retryable interruption),
	// whereas a timed-out read is a genuine failure and must surface as EIO.
	ErrReadTimeout  = errors.New("read timed out")
	ErrMetadataGone = errors.New("file metadata was removed underneath this handle by a repair")
)

// Database operation error message templates
const (
	ErrMsgFailedCreateDecryptReader = "failed to create decrypt reader: %w"
	ErrMsgFailedWrapEncryption      = "failed to wrap reader with encryption: %w"
)

// defaultStreamReadTimeout mirrors config.DefaultStreamReadTimeout so this
// package can reason about the fallback without importing it at every call.
const defaultStreamReadTimeout = config.DefaultStreamReadTimeout
