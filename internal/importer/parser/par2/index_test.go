package par2_test

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/kipsilabs/altmount/internal/importer/parser/par2"
	"github.com/kipsilabs/altmount/internal/testsupport/par2gen"
)

func TestParseIndexRoundTrip(t *testing.T) {
	dataA := bytes.Repeat([]byte("abcdefgh"), 1000) // 8000 B -> 8 slices of 1024
	dataB := bytes.Repeat([]byte("01234567"), 700)  // 5600 B -> 6 slices of 1024
	set := par2gen.BuildFull(1024, []par2gen.FileEntry{
		{Name: "a.rar", Content: dataA},
		{Name: "b.rar", Content: dataB},
	}, 4)

	streams := []io.Reader{bytes.NewReader(set.Index)}
	for _, v := range set.Volumes {
		streams = append(streams, bytes.NewReader(v))
	}
	idx, err := par2.ParseIndex(streams)
	if err != nil {
		t.Fatal(err)
	}
	if idx.SliceSize != 1024 {
		t.Fatalf("slice size = %d", idx.SliceSize)
	}
	if len(idx.RecoveryIDs) != 2 {
		t.Fatalf("recovery set size = %d", len(idx.RecoveryIDs))
	}
	if !bytes.Contains(set.Index, append(idx.RecoveryIDs[0][:16:16], idx.RecoveryIDs[1][:]...)) {
		t.Fatal("RecoveryIDs not in Main packet order")
	}
	total := 0
	for _, id := range idx.RecoveryIDs {
		fd, ok := idx.Files[id]
		if !ok {
			t.Fatalf("missing FileDesc for recovery-set member %x", id)
		}
		checks, ok := idx.SliceChecks[id]
		if !ok {
			t.Fatalf("missing IFSC for recovery-set member %x", id)
		}
		wantSlices := int((fd.Length + 1023) / 1024)
		if len(checks) != wantSlices {
			t.Fatalf("file %q: %d slice checks, want %d", fd.Name, len(checks), wantSlices)
		}
		total += len(checks)
	}
	if total != 14 {
		t.Fatalf("total slices = %d, want 14", total)
	}
	if len(idx.Recovery) != 4 {
		t.Fatalf("recovery slices = %d, want 4", len(idx.Recovery))
	}
	seen := map[uint32]bool{}
	for _, r := range idx.Recovery {
		if seen[r.Exponent] {
			t.Fatalf("duplicate exponent %d", r.Exponent)
		}
		seen[r.Exponent] = true
		if r.BodyOffset <= 0 {
			t.Fatalf("recovery slice exponent %d has BodyOffset %d", r.Exponent, r.BodyOffset)
		}
		if r.FileIndex <= 0 || r.FileIndex >= len(streams) {
			t.Fatalf("recovery slice exponent %d has FileIndex %d", r.Exponent, r.FileIndex)
		}
	}
}

// The recovery payload located by a RecoverySliceRef must be exactly the bytes
// the generator produced: re-read the payload via the recorded offset and
// compare against an independently computed recovery slice in the solver test
// (Task 2). Here we only verify offsets point at plausible payloads.
func TestParseIndexRecoveryOffsets(t *testing.T) {
	content := bytes.Repeat([]byte{0xAB}, 4096)
	set := par2gen.BuildFull(512, []par2gen.FileEntry{{Name: "x.bin", Content: content}}, 2)

	streams := []io.Reader{bytes.NewReader(set.Index)}
	for _, v := range set.Volumes {
		streams = append(streams, bytes.NewReader(v))
	}
	idx, err := par2.ParseIndex(streams)
	if err != nil {
		t.Fatal(err)
	}
	all := append([][]byte{set.Index}, set.Volumes...)
	for _, r := range idx.Recovery {
		src := all[r.FileIndex]
		if int64(len(src)) < r.BodyOffset+int64(idx.SliceSize) {
			t.Fatalf("payload for exponent %d out of bounds", r.Exponent)
		}
	}
}

func TestParseIndexErrors(t *testing.T) {
	// no Main packet: index containing only FileDescs
	legacy := par2gen.Build(par2gen.FileEntry{Name: "a", Content: []byte("hello")})
	if _, err := par2.ParseIndex([]io.Reader{bytes.NewReader(legacy)}); err == nil {
		t.Fatal("want error for missing Main packet")
	}
}

// A PAR2 set whose volumes are partly unreachable must still yield the
// recovery slices that ARE reachable. Failing on the first dead volume throws
// away usable recovery data and turns a repairable release into a lost one.
func TestParseIndexToleratesUnreadableVolumes(t *testing.T) {
	content := bytes.Repeat([]byte{0x5C}, 4096)
	set := par2gen.BuildFull(1024, []par2gen.FileEntry{{Name: "a.bin", Content: content}}, 4)

	streams := []io.Reader{bytes.NewReader(set.Index)}
	for i, v := range set.Volumes {
		if i == 0 {
			// First recovery volume is unreachable (dead article).
			streams = append(streams, errReader{err: errors.New("nntp: 430 no such article")})
			continue
		}
		streams = append(streams, bytes.NewReader(v))
	}

	idx, err := par2.ParseIndex(streams)
	if err != nil {
		t.Fatalf("ParseIndex must tolerate an unreadable volume: %v", err)
	}
	if len(idx.Recovery) != 3 {
		t.Fatalf("recovery slices = %d, want 3 (the reachable ones)", len(idx.Recovery))
	}
	if idx.SliceSize != 1024 {
		t.Fatalf("SliceSize = %d", idx.SliceSize)
	}
}

// An unreadable INDEX file (no Main packet anywhere) is still fatal: without
// it there is no slice size or recovery-set membership to plan from.
func TestParseIndexFailsWhenIndexUnreadable(t *testing.T) {
	if _, err := par2.ParseIndex([]io.Reader{errReader{err: errors.New("nntp: 430 no such article")}}); err == nil {
		t.Fatal("want error when no stream yields a Main packet")
	}
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

// findPacket returns the offset of the first packet of the given type in a
// raw PAR2 stream, or -1.
func findPacket(data []byte, packetType [16]byte) int {
	magic := []byte("PAR2\x00PKT")
	for at := 0; ; {
		i := bytes.Index(data[at:], magic)
		if i < 0 {
			return -1
		}
		at += i
		if at+64 > len(data) {
			return -1
		}
		if bytes.Equal(data[at+48:at+64], packetType[:]) {
			return at
		}
		at += len(magic)
	}
}

// PAR2 repeats metadata packets across files so damage in one copy is
// survivable. A packet whose body no longer matches its header MD5 must be
// dropped rather than trusted: here the corrupt IFSC copy arrives after the
// good one and must not overwrite it.
func TestParseIndexDropsPacketWithBadHash(t *testing.T) {
	content := bytes.Repeat([]byte{0x5C}, 4096)
	set := par2gen.BuildFull(1024, []par2gen.FileEntry{{Name: "a.bin", Content: content}}, 1)

	good, err := par2.ParseIndex([]io.Reader{bytes.NewReader(set.Index)})
	if err != nil {
		t.Fatal(err)
	}

	corrupt := append([]byte(nil), set.Index...)
	at := findPacket(corrupt, par2gen.PacketTypeIFSCT())
	if at < 0 {
		t.Fatal("no IFSC packet in generated index")
	}
	// Flip a CRC32 byte of the first slice entry (body = FileID[16] + 20-byte
	// entries of MD5[16]+CRC32[4]).
	corrupt[at+64+16+16] ^= 0xFF

	idx, err := par2.ParseIndex([]io.Reader{bytes.NewReader(set.Index), bytes.NewReader(corrupt)})
	if err != nil {
		t.Fatal(err)
	}
	id := idx.RecoveryIDs[0]
	if got, want := idx.SliceChecks[id][0].CRC32, good.SliceChecks[id][0].CRC32; got != want {
		t.Fatalf("corrupt IFSC copy overwrote the good one: crc %08x, want %08x", got, want)
	}
}

// Damage inside a volume must cost only the packets it hit: the parser must
// resynchronise on the next packet magic and keep reading. Here a stream
// carries two RecvSlic packets and the first one's header is destroyed.
func TestParseIndexResyncsWithinDamagedStream(t *testing.T) {
	content := make([]byte, 4096) // zero content: recovery payloads carry no fake magic
	set := par2gen.BuildFull(1024, []par2gen.FileEntry{{Name: "a.bin", Content: content}}, 3)

	damaged := append([]byte(nil), set.Volumes[0]...)
	damaged[0] ^= 0xFF // break the first packet's magic
	damaged = append(damaged, set.Volumes[1]...)

	idx, err := par2.ParseIndex([]io.Reader{
		bytes.NewReader(set.Index),
		bytes.NewReader(damaged),
		bytes.NewReader(set.Volumes[2]),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(idx.Recovery) != 2 {
		t.Fatalf("recovery refs = %d, want 2 (the packets damage did not hit)", len(idx.Recovery))
	}
	// The ref recovered by resync must locate its payload exactly.
	for _, ref := range idx.Recovery {
		if ref.FileIndex != 1 {
			continue
		}
		want := set.Volumes[1][len(set.Volumes[1])-1024:]
		got := damaged[ref.BodyOffset : ref.BodyOffset+1024]
		if !bytes.Equal(got, want) {
			t.Fatal("resynced RecvSlic ref points at the wrong payload bytes")
		}
	}
}

// A stream that starts with garbage — a dead article zero-filled by the read
// path — must still yield the packets behind it.
func TestParseIndexResyncsPastLeadingGarbage(t *testing.T) {
	content := bytes.Repeat([]byte{0x11}, 4096)
	set := par2gen.BuildFull(1024, []par2gen.FileEntry{{Name: "a.bin", Content: content}}, 1)

	stream := append(make([]byte, 1500), set.Index...)
	idx, err := par2.ParseIndex([]io.Reader{bytes.NewReader(stream)})
	if err != nil {
		t.Fatalf("ParseIndex must resync past leading garbage: %v", err)
	}
	if idx.SliceSize != 1024 {
		t.Fatalf("SliceSize = %d, want 1024", idx.SliceSize)
	}
}

// Every RecvSlic ref must carry its packet's MD5 and set ID so the payload's
// integrity can be verified when it is finally fetched. The hash covers
// everything after the header's MD5 field: setID + type + exponent + payload.
func TestParseIndexRecordsPacketIntegrity(t *testing.T) {
	content := bytes.Repeat([]byte{0xAB}, 4096)
	set := par2gen.BuildFull(512, []par2gen.FileEntry{{Name: "x.bin", Content: content}}, 2)

	streams := []io.Reader{bytes.NewReader(set.Index)}
	for _, v := range set.Volumes {
		streams = append(streams, bytes.NewReader(v))
	}
	idx, err := par2.ParseIndex(streams)
	if err != nil {
		t.Fatal(err)
	}
	all := append([][]byte{set.Index}, set.Volumes...)
	for _, ref := range idx.Recovery {
		if ref.PacketMD5 == ([16]byte{}) {
			t.Fatalf("ref exponent %d has no PacketMD5", ref.Exponent)
		}
		payload := all[ref.FileIndex][ref.BodyOffset : ref.BodyOffset+int64(idx.SliceSize)]
		sum := md5.New()
		sum.Write(ref.SetID[:])
		sum.Write(par2.PacketTypeRecoverySlice[:])
		var exp [4]byte
		binary.LittleEndian.PutUint32(exp[:], ref.Exponent)
		sum.Write(exp[:])
		sum.Write(payload)
		if !bytes.Equal(sum.Sum(nil), ref.PacketMD5[:]) {
			t.Fatalf("ref exponent %d: PacketMD5 does not match the packet bytes", ref.Exponent)
		}
	}
}

func parseUntilFixture(t *testing.T) ([]io.Reader, int) {
	t.Helper()
	data := bytes.Repeat([]byte("abcdefgh"), 1000)
	set := par2gen.BuildFull(1024, []par2gen.FileEntry{{Name: "a.rar", Content: data}}, 6)
	streams := []io.Reader{bytes.NewReader(set.Index)}
	for _, v := range set.Volumes {
		streams = append(streams, bytes.NewReader(v))
	}
	return streams, len(set.Volumes)
}

func TestParseIndexUntilStopsOnceEnough(t *testing.T) {
	streams, _ := parseUntilFixture(t)
	idx, err := par2.ParseIndexUntil(streams, func(i *par2.Index) bool { return len(i.Recovery) >= 2 })
	if err != nil {
		t.Fatal(err)
	}
	if len(idx.Recovery) != 2 {
		t.Fatalf("recovery slices = %d, want the parse to stop at 2", len(idx.Recovery))
	}
	if len(idx.RecoveryIDs) != 1 || len(idx.Files) != 1 {
		t.Fatalf("metadata incomplete: ids=%d files=%d", len(idx.RecoveryIDs), len(idx.Files))
	}
}

func TestParseIndexUntilReadsEverythingWhenNeverEnough(t *testing.T) {
	streams, volumes := parseUntilFixture(t)
	idx, err := par2.ParseIndexUntil(streams, func(*par2.Index) bool { return false })
	if err != nil {
		t.Fatal(err)
	}
	if len(idx.Recovery) != volumes {
		t.Fatalf("recovery slices = %d, want %d", len(idx.Recovery), volumes)
	}
}

// Stopping must not skip the only copy of the metadata: until it is complete the
// parse keeps going, however many recovery slices it has already seen.
func TestParseIndexUntilWaitsForCompleteMetadata(t *testing.T) {
	streams, volumes := parseUntilFixture(t)
	// Recovery volumes first: they carry no metadata here, the index comes last.
	reordered := append(append([]io.Reader{}, streams[1:]...), streams[0])
	idx, err := par2.ParseIndexUntil(reordered, func(*par2.Index) bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	if len(idx.Recovery) != volumes || len(idx.Files) != 1 {
		t.Fatalf("recovery=%d files=%d, want every stream read", len(idx.Recovery), len(idx.Files))
	}
}
