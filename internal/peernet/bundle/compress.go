package bundle

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

// Compression: zstd at its highest level, single-threaded, with a window of
// MaxWindowSize. No dictionary is used: the bulk of a bundle is high-entropy
// message-IDs, which a dictionary cannot predict, while the repetitive layout
// (sizes, offsets, references) is run-length coded in the schema itself and
// costs a few bytes per file. See TestBundleSizes for the measured sizes.

// Compress compresses canonical bundle bytes for the wire. The result never
// exceeds MaxCompressedSize.
func Compress(canonical []byte) ([]byte, error) {
	if len(canonical) > MaxDecompressedSize {
		return nil, fmt.Errorf("%w: %d bytes uncompressed", ErrTooLarge, len(canonical))
	}
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedBestCompression),
		zstd.WithEncoderConcurrency(1),
		zstd.WithWindowSize(MaxWindowSize),
		zstd.WithEncoderCRC(true),
	)
	if err != nil {
		return nil, fmt.Errorf("bundle: zstd encoder: %w", err)
	}
	defer func() { _ = enc.Close() }()
	out := enc.EncodeAll(canonical, nil)
	if len(out) > MaxCompressedSize {
		return nil, fmt.Errorf("%w: %d bytes compressed", ErrTooLarge, len(out))
	}
	return out, nil
}

// Decompress returns the canonical bytes of a wire bundle. The input may not
// exceed MaxCompressedSize, a frame may not ask for a window above
// MaxWindowSize, and decoding stops with ErrTooLarge as soon as the output
// would pass MaxDecompressedSize, so a tiny input cannot make it allocate a
// large buffer. Decoding is synchronous: no goroutine outlives the call.
func Decompress(wire []byte) ([]byte, error) {
	if len(wire) > MaxCompressedSize {
		return nil, fmt.Errorf("%w: %d bytes compressed", ErrTooLarge, len(wire))
	}
	if len(wire) == 0 {
		return nil, fmt.Errorf("%w: empty input", ErrMalformed)
	}
	dec, err := zstd.NewReader(bytes.NewReader(wire),
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderMaxWindow(MaxWindowSize),
		zstd.WithDecoderMaxMemory(MaxDecompressedSize),
		zstd.WithDecoderLowmem(true),
	)
	if err != nil {
		return nil, fmt.Errorf("%w: zstd: %v", ErrMalformed, err)
	}
	defer dec.Close()
	out, err := readBounded(dec, declaredSize(wire))
	switch {
	case len(out) > MaxDecompressedSize:
		return nil, fmt.Errorf("%w: more than %d bytes uncompressed", ErrTooLarge, MaxDecompressedSize)
	case errors.Is(err, zstd.ErrDecoderSizeExceeded), errors.Is(err, zstd.ErrWindowSizeExceeded):
		return nil, fmt.Errorf("%w: zstd limits: %v", ErrTooLarge, err)
	case err != nil:
		return nil, fmt.Errorf("%w: zstd: %v", ErrMalformed, err)
	case len(out) == 0:
		return nil, fmt.Errorf("%w: no data", ErrMalformed)
	}
	return out, nil
}

// readBounded reads r in fixed chunks, up to MaxDecompressedSize+1 bytes, and
// joins them once, so peak memory stays near twice the output rather than the
// several times that a growing buffer costs.
func readBounded(r io.Reader, declared int) ([]byte, error) {
	if declared > 0 {
		// The frame states its content size: read into one exact buffer (one
		// byte of slack detects a frame that lies and keeps going).
		out := make([]byte, declared+1)
		n, err := io.ReadFull(r, out)
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			err = nil
		}
		return out[:n], err
	}
	const chunk = 1 << 20
	var chunks [][]byte
	total := 0
	cur := make([]byte, 0, chunk)
	var err error
	for total <= MaxDecompressedSize {
		if len(cur) == cap(cur) {
			chunks = append(chunks, cur)
			cur = make([]byte, 0, min(chunk, MaxDecompressedSize+1-total))
		}
		var n int
		n, err = r.Read(cur[len(cur):cap(cur)])
		cur = cur[:len(cur)+n]
		total += n
		if err != nil {
			break
		}
	}
	if errors.Is(err, io.EOF) {
		err = nil
	}
	chunks = append(chunks, cur)
	out := make([]byte, 0, total)
	for _, c := range chunks {
		out = append(out, c...)
	}
	return out, err
}

// declaredSize returns the content size a frame header states, or 0 when it
// states none or more than MaxDecompressedSize (the streaming path then
// enforces the bound).
func declaredSize(wire []byte) int {
	var h zstd.Header
	if err := h.Decode(wire); err != nil || !h.HasFCS || h.FrameContentSize == 0 || h.FrameContentSize > MaxDecompressedSize {
		return 0
	}
	return int(h.FrameContentSize)
}
