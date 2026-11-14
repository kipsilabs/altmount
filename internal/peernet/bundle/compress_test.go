package bundle

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"runtime"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

// zeroBomb returns a valid zstd stream that expands to n zero bytes.
func zeroBomb(t testing.TB, n int, window int) []byte {
	t.Helper()
	var buf bytes.Buffer
	opts := []zstd.EOption{zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest)}
	if window > 0 {
		opts = append(opts, zstd.WithWindowSize(window))
	}
	enc, err := zstd.NewWriter(&buf, opts...)
	if err != nil {
		t.Fatal(err)
	}
	chunk := make([]byte, 1<<20)
	for left := n; left > 0; {
		c := min(left, len(chunk))
		if _, err := enc.Write(chunk[:c]); err != nil {
			t.Fatal(err)
		}
		left -= c
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestCompressRoundTrip(t *testing.T) {
	raw := mustEncode(t, goodBundle(t))
	wire, err := Compress(raw)
	if err != nil {
		t.Fatalf("Compress: %v", err)
	}
	got, err := Decompress(wire)
	if err != nil || !bytes.Equal(got, raw) {
		t.Fatalf("Decompress: %v equal=%v", err, bytes.Equal(got, raw))
	}
}

func TestDecompressRejects(t *testing.T) {
	good, _ := Compress([]byte("hello hello hello hello"))
	tests := []struct {
		name string
		in   []byte
		want error
	}{
		{"empty", nil, ErrMalformed},
		{"garbage", []byte("this is not zstd at all, just text"), ErrMalformed},
		{"truncated frame", good[:len(good)-2], ErrMalformed},
		{"input over 8 MB", make([]byte, MaxCompressedSize+1), ErrTooLarge},
		{"bomb over 64 MB", zeroBomb(t, MaxDecompressedSize+1, 0), ErrTooLarge},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Decompress(tc.in); !errors.Is(err, tc.want) {
				t.Fatalf("Decompress = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestDecompressAcceptsExactLimit(t *testing.T) {
	out, err := Decompress(zeroBomb(t, MaxDecompressedSize, 0))
	if err != nil || len(out) != MaxDecompressedSize {
		t.Fatalf("len %d err %v", len(out), err)
	}
}

func TestDecompressBombMemoryIsBounded(t *testing.T) {
	bomb := zeroBomb(t, 512<<20, 0) // 512 MiB of zeros in a few KB
	if len(bomb) > 1<<20 {
		t.Fatalf("bomb is %d bytes, expected tiny", len(bomb))
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err := Decompress(bomb)
	runtime.ReadMemStats(&after)
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("got %v", err)
	}
	if delta := after.TotalAlloc - before.TotalAlloc; delta > 4*MaxDecompressedSize {
		t.Fatalf("allocated %d bytes for a bomb; limit %d", delta, 4*MaxDecompressedSize)
	}
}

func TestDecompressRejectsOversizedWindow(t *testing.T) {
	// A frame that asks the decoder for a 128 MB window must be refused even
	// though its content is small.
	data := zeroBomb(t, 1<<20, 128<<20)
	if _, err := Decompress(data); err == nil {
		t.Fatal("128 MB window accepted")
	}
}

func TestCompressRejectsTooLarge(t *testing.T) {
	if _, err := Compress(make([]byte, MaxDecompressedSize+1)); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversized input: %v", err)
	}
	noise := make([]byte, MaxCompressedSize+(1<<20))
	if _, err := io.ReadFull(rand.Reader, noise); err != nil {
		t.Fatal(err)
	}
	if _, err := Compress(noise); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("incompressible output over 8 MB: %v", err)
	}
}

func TestDecompressLeavesNoGoroutines(t *testing.T) {
	wire, err := Compress(mustEncode(t, goodBundle(t)))
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	before := runtime.NumGoroutine()
	for i := 0; i < 200; i++ {
		if _, err := Decompress(wire); err != nil {
			t.Fatal(err)
		}
		_, _ = Decompress([]byte("junk"))
		_, _ = Decompress(zeroBomb(t, MaxDecompressedSize+1, 0)[:100])
	}
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if after := runtime.NumGoroutine(); after > before {
		t.Fatalf("goroutines %d -> %d", before, after)
	}
}

// TestBundleSizes documents the measured size of typical releases. Message-IDs
// are random, so they dominate; layout (sizes, offsets, references) is run
// length coded and costs a few bytes per file.
func TestBundleSizes(t *testing.T) {
	for _, tc := range []struct {
		name    string
		gb      int64
		maxWire int
	}{{"1GB", 1, 90 << 10}, {"50GB", 50, 3 << 20}} {
		n := int(tc.gb<<30/716_800) + 1
		b := syntheticBundle(t, n, 716_800)
		raw := mustEncode(t, b)
		wire, err := Compress(raw)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%s: %d segments, canonical %d bytes, compressed %d bytes", tc.name, n, len(raw), len(wire))
		if len(wire) > tc.maxWire {
			t.Fatalf("%s: compressed %d bytes, want <= %d", tc.name, len(wire), tc.maxWire)
		}
	}
}
