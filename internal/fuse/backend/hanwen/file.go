//go:build linux

package hanwen

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync/atomic"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/kipsilabs/altmount/internal/fuse/backend"
	"github.com/kipsilabs/altmount/internal/nzbfilesystem"
	"github.com/kipsilabs/altmount/internal/utils"
)

// ensure File implements fs.Node* interfaces
var _ fs.NodeOpener = (*File)(nil)
var _ fs.NodeGetattrer = (*File)(nil)
var _ fs.NodeReader = (*File)(nil)
var _ fs.NodeSetattrer = (*File)(nil)

// File represents a file in the FUSE filesystem.
type File struct {
	fs.Inode
	nzbfs         nzbFS
	streamTracker backend.StreamTracker
	path          string
	logger        *slog.Logger
	// size is the last known file size, used only as a fallback when the
	// per-open Stat fails. Retained nodes can be opened concurrently, so it
	// must be accessed atomically.
	size         atomic.Int64
	uid          uint32
	gid          uint32
	asyncBufSize int // per-handle read-ahead buffer size in bytes; 0 = disabled
	noModTime    bool
}

// Getattr implements fs.NodeGetattrer.
func (f *File) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	info, err := f.nzbfs.Stat(ctx, f.path)
	if err != nil {
		if !os.IsNotExist(err) {
			f.logger.ErrorContext(ctx, "File Getattr failed", "path", f.path, "error", err)
		}
		return translateError(err)
	}

	fillAttr(info, &out.Attr, f.uid, f.gid, f.noModTime)
	out.Ino = f.Inode.StableAttr().Ino
	return 0
}

// Setattr implements fs.NodeSetattrer (no-op success).
func (f *File) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	return f.Getattr(ctx, fh, out)
}

// Open implements fs.NodeOpener.
func (f *File) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if flags&syscall.O_ACCMODE != syscall.O_RDONLY {
		return nil, 0, syscall.EACCES
	}

	// Register before opening so the downloader can attribute NNTP bytes to
	// this handle without creating a second stream in the metadata layer.
	fileSize := f.size.Load()
	if fi, statErr := f.nzbfs.Stat(ctx, f.path); statErr == nil {
		fileSize = fi.Size()
	}
	var stream *nzbfilesystem.ActiveStream
	if f.streamTracker != nil {
		stream = f.streamTracker.AddStream(f.path, "FUSE", "FUSE", "", "", fileSize)
	}
	ctx = context.WithValue(ctx, utils.SuppressStreamTrackingKey, true)
	if stream != nil {
		ctx = context.WithValue(ctx, utils.StreamIDKey, stream.ID)
	}

	// The handle outlives this request, whose context go-fuse may cancel later.
	aferoFile, err := f.nzbfs.Open(context.WithoutCancel(ctx), f.path)
	if err != nil {
		if stream != nil {
			f.streamTracker.Remove(stream.ID)
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			f.logger.DebugContext(ctx, "File Open canceled", "path", f.path)
			return nil, 0, syscall.EINTR
		}

		f.logger.ErrorContext(ctx, "File Open failed", "path", f.path, "error", err)
		return nil, 0, syscall.EIO
	}

	if fi, statErr := aferoFile.Stat(); statErr == nil {
		fileSize = fi.Size()
		f.size.Store(fileSize)
	}

	// Attach a read-ahead buffer for sufficiently large files. The buffer
	// stays in passthrough mode until sustained sequential reads are observed,
	// so header probes and seek/scrub bursts never allocate it.
	var asyncBuf *backend.AsyncReadBuffer
	if rac, ok := aferoFile.(readAtContexter); ok {
		asyncBuf = newHandleAsyncBuffer(ctx, rac, f.asyncBufSize, fileSize, f.logger)
	}

	handle := NewHandle(aferoFile, f.logger, f.path, stream, f.streamTracker, asyncBuf)

	// Use DIRECT_IO when file size is unknown/zero to prevent the kernel
	// from caching pages with stale size metadata (rclone mount2 pattern).
	fuseFlags := uint32(fuse.FOPEN_KEEP_CACHE)
	if fileSize <= 0 {
		fuseFlags = uint32(fuse.FOPEN_DIRECT_IO)
	}
	return handle, fuseFlags, 0
}

// Read implements fs.NodeReader.
func (f *File) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	handle := fh.(*Handle)
	return handle.Read(ctx, dest, off)
}

func newHandleAsyncBuffer(
	openCtx context.Context,
	src readAtContexter,
	asyncBufSize int,
	fileSize int64,
	logger *slog.Logger,
) *backend.AsyncReadBuffer {
	if asyncBufSize <= 0 || fileSize <= int64(asyncBufSize) {
		return nil
	}
	return backend.NewAsyncReadBuffer(context.WithoutCancel(openCtx), src, asyncBufSize, fileSize, logger)
}
