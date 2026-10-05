package importer

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/javi11/nzbparser"
	"github.com/javi11/rardecode/v2"
	"github.com/kipsilabs/altmount/internal/importer/archive/rar"
	"github.com/kipsilabs/altmount/internal/importer/filesystem"
	"github.com/kipsilabs/altmount/internal/importer/parser"
	"github.com/kipsilabs/altmount/internal/importer/rarname"
	"github.com/kipsilabs/altmount/internal/importer/utils/nzbtrim"
	"github.com/kipsilabs/altmount/internal/nzbfile"
	"github.com/kipsilabs/altmount/internal/usenet"
)

const (
	maxNestedNzbDepth    = 4
	maxNestedArchiveSize = 32 << 20
	maxNestedNzbSize     = 8 << 20
)

// resolveNestedNzb only probes small, archive-only posts. Media imports keep
// their existing streaming path, including the prohibition on compressed media.
func (proc *Processor) resolveNestedNzb(ctx context.Context, parsed *parser.ParsedNzb) ([]byte, error) {
	var archives []parser.ParsedFile
	var total int64
	for _, file := range parsed.Files {
		// Respect an archive type already recognized by the existing detector,
		// even when the poster's filename misleadingly ends in .zip.
		if file.Is7zArchive {
			return nil, nil
		}
		if file.IsRarArchive || strings.EqualFold(filepath.Ext(file.Filename), ".zip") {
			archives = append(archives, file)
			total += file.Size
		} else if !file.IsPar2Archive && !nestedNzbSidecar(file.Filename) {
			return nil, nil
		}
	}
	if len(archives) == 0 || total > maxNestedArchiveSize {
		return nil, nil
	}
	// Grouping may reconcile reposted or obfuscated volume names. Index those
	// names in the filesystem too, so archive readers can follow the next part.
	var rarFiles, normalized []parser.ParsedFile
	for _, file := range archives {
		if file.IsRarArchive {
			rarFiles = append(rarFiles, file)
		} else {
			normalized = append(normalized, file)
		}
	}
	for _, group := range rar.GroupArchivesByBaseName(rarFiles) {
		normalized = append(normalized, group...)
	}
	archives = normalized
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	cache := filesystem.NewImportSegmentCache(maxNestedArchiveSize)
	ufs := filesystem.NewUsenetFileSystem(ctx, proc.poolManager, archives, 1, nil,
		time.Duration(proc.configGetter().Import.ReadTimeoutSeconds)*time.Second, cache)
	return readNestedNzb(ctx, ufs, archives, parsed.GetPassword())
}

func nestedNzbSidecar(name string) bool {
	switch strings.ToLower(filepath.Ext(name)) {
	case ".nfo", ".txt", ".sfv", ".srr", ".par2":
		return true
	}
	return false
}

// readNestedNzb inspects headers before decompressing. It never extracts
// archive paths onto disk and refuses to choose between multiple NZBs.
func readNestedNzb(ctx context.Context, source fs.FS, archives []parser.ParsedFile, password string) ([]byte, error) {
	type candidate struct {
		name     string
		open     func() (io.ReadCloser, error)
		oversize bool
	}
	var candidates []candidate
	var rarFiles []parser.ParsedFile
	for _, file := range archives {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if file.IsRarArchive {
			rarFiles = append(rarFiles, file)
			continue
		}
		f, err := source.Open(file.Filename)
		if err != nil {
			return nil, nestedProbeError(ctx, err)
		}
		defer f.Close()
		ra, ok := f.(io.ReaderAt)
		if !ok {
			return nil, fmt.Errorf("ZIP source does not support seeking")
		}
		zr, err := zip.NewReader(ra, file.Size)
		if err != nil {
			return nil, nestedProbeError(ctx, err)
		}
		for _, entry := range zr.File {
			if entry.FileInfo().IsDir() {
				continue
			}
			if nzbtrim.HasNzbExtension(entry.Name) {
				candidates = append(candidates, candidate{name: entry.Name, open: entry.Open, oversize: entry.UncompressedSize64 > maxNestedNzbSize})
			} else if !nestedNzbSidecar(entry.Name) {
				return nil, nil
			}
		}
	}
	for _, group := range rar.GroupArchivesByBaseName(rarFiles) {
		first := group[0].Filename
		_, firstNumber, _ := rarname.VolumeNumber(first)
		for _, file := range group[1:] {
			if _, number, ok := rarname.VolumeNumber(file.Filename); ok && number < firstNumber {
				first, firstNumber = file.Filename, number
			}
		}
		opts := []rardecode.Option{rardecode.FileSystem(source), rardecode.MaxDictionarySize(maxNestedArchiveSize), rardecode.MaxVolumes(len(group)), rardecode.ParallelRead(false)}
		if password != "" {
			opts = append(opts, rardecode.Password(password))
		}
		// Normal media imports tolerate missing volume tails once their headers
		// were collected. A wrapper probe must preserve that behavior, while
		// extraction below still checks every byte and checksum of the NZB.
		listOpts := append(append([]rardecode.Option{}, opts...),
			rardecode.ParallelRead(true), rardecode.MaxConcurrentVolumes(1),
			rardecode.TolerateVolumeTailError(usenet.IsArticleNotFound))
		entries, err := rardecode.List(first, listOpts...)
		if err != nil {
			return nil, nestedProbeError(ctx, err)
		}
		for _, entry := range entries {
			if entry.IsDir {
				continue
			}
			if nzbtrim.HasNzbExtension(entry.Name) {
				oversize := entry.UnPackedSize > maxNestedNzbSize
				// Opening the indexed file follows its packed offsets across
				// volumes. Solid entries need the preceding decoder state.
				if !entry.Solid {
					candidates = append(candidates, candidate{name: entry.Name, open: entry.Open, oversize: oversize})
					continue
				}
				name := entry.Name
				candidates = append(candidates, candidate{name: name, oversize: oversize, open: func() (io.ReadCloser, error) {
					r, err := rardecode.OpenReader(first, opts...)
					if err != nil {
						return nil, err
					}
					for {
						if err := ctx.Err(); err != nil {
							r.Close()
							return nil, err
						}
						h, err := r.Next()
						if err != nil {
							r.Close()
							return nil, err
						}
						if h.Name == name {
							return r, nil
						}
						// Explicitly bound decompression of preceding solid entries.
						if _, err := readNestedNzbBytes(r); err != nil {
							r.Close()
							return nil, err
						}
					}
				}})
			} else if !strings.HasSuffix(entry.Name, "/") && !nestedNzbSidecar(entry.Name) {
				return nil, nil
			}
		}
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	if len(candidates) != 1 {
		return nil, NewNonRetryableError("archive contains multiple NZBs; cannot select a release", nil)
	}
	c := candidates[0]
	// Validate payload limits only after all entries have confirmed this is
	// an NZB wrapper, rather than an ordinary archive with an NZB sidecar.
	if c.oversize {
		return nil, NewNonRetryableError("embedded NZB exceeds size limit", nil)
	}
	r, err := c.open()
	if err != nil {
		return nil, err
	}
	defer r.Close()
	data, err := readNestedNzbBytes(r)
	if err != nil {
		return nil, err
	}
	if nzbfile.IsGzipped(c.name) {
		gr, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, NewNonRetryableError("invalid embedded gzip NZB", err)
		}
		defer gr.Close()
		data, err = readNestedNzbBytes(gr)
		if err != nil {
			return nil, err
		}
	}
	n, err := nzbparser.Parse(bytes.NewReader(data))
	if err != nil {
		return nil, NewNonRetryableError("invalid embedded NZB", err)
	}
	if len(n.Files) == 0 {
		return nil, NewNonRetryableError("embedded NZB contains no files", nil)
	}
	return data, nil
}

// Conclusive format errors leave normal archive group isolation and repair
// handling in charge. Transport failures must remain retryable: treating a
// temporarily unreadable wrapper as media could permanently reject it.
func nestedProbeError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if rar.IsCorruptionError(err) {
		return nil
	}
	for _, structural := range []error{
		zip.ErrFormat, fs.ErrNotExist, rardecode.ErrNoSig,
		rardecode.ErrBadVolumeNumber, rardecode.ErrVerMismatch,
		rardecode.ErrNoArchiveBlock, rardecode.ErrInvalidHeaderOff,
		rardecode.ErrUnknownVersion, rardecode.ErrUnknownDecoder,
		rardecode.ErrUnsupportedDecoder, rardecode.ErrBadPassword,
		rardecode.ErrArchiveEncrypted,
	} {
		if errors.Is(err, structural) {
			return nil
		}
	}
	return err
}

func readNestedNzbBytes(r io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, maxNestedNzbSize+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxNestedNzbSize {
		return nil, NewNonRetryableError("embedded NZB exceeds size limit", nil)
	}
	return data, nil
}

// Keep the queue's path and download identity, replacing its contents atomically
// so retries, repairs and fallback downloads all refer to the resolved release.
func persistNestedNzb(path string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".nested-nzb-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if nzbfile.IsGzipped(path) {
		gz := gzip.NewWriter(f)
		if _, err := gz.Write(data); err != nil {
			return err
		}
		if err := gz.Close(); err != nil {
			return err
		}
	} else if _, err := f.Write(data); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}
