package par2gen

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"hash/crc32"
	"slices"
	"sort"
	"sync"

	"github.com/javi11/gopar-turbo/gf2p16"
)

// FullSet is a complete, spec-valid PAR2 set: an index file (Main + FileDesc
// + IFSC packets) and one volume file per recovery slice (a RecvSlic packet).
type FullSet struct {
	Index   []byte
	Volumes [][]byte
}

// BuildFull produces a spec-valid PAR2 set over the given files. Recovery
// data is computed with the PAR2 Vandermonde construction over GF(2^16),
// independently of the production solver's incremental fold path, so the two
// can validate each other in round-trip tests.
func BuildFull(sliceSize int, entries []FileEntry, numRecovery int) FullSet {
	if sliceSize <= 0 || sliceSize%4 != 0 {
		panic("par2gen: slice size must be a positive multiple of 4")
	}

	type member struct {
		entry   FileEntry
		fileID  [16]byte
		hash16k [16]byte
	}
	members := make([]member, len(entries))
	for i, e := range entries {
		id, h16k := fileIdentity(e)
		members[i] = member{entry: e, fileID: id, hash16k: h16k}
	}
	// Recovery-set order: FileID ascending as a little-endian integer, the way
	// par2cmdline compares them (last byte most significant).
	sort.Slice(members, func(i, j int) bool {
		a, b := members[i].fileID, members[j].fileID
		slices.Reverse(a[:])
		slices.Reverse(b[:])
		return bytes.Compare(a[:], b[:]) < 0
	})

	// Split every file into zero-padded slices, in recovery-set order.
	var allSlices [][]byte
	sliceChecks := make([][]sliceCheck, len(members))
	for i, m := range members {
		content := m.entry.Content
		for off := 0; off < len(content); off += sliceSize {
			sl := make([]byte, sliceSize)
			copy(sl, content[off:])
			allSlices = append(allSlices, sl)
			sliceChecks[i] = append(sliceChecks[i], sliceCheck{
				md5:   md5.Sum(sl),
				crc32: crc32.ChecksumIEEE(sl),
			})
		}
	}

	// Main packet body.
	var mainBody bytes.Buffer
	binary.Write(&mainBody, binary.LittleEndian, uint64(sliceSize)) //nolint:errcheck
	binary.Write(&mainBody, binary.LittleEndian, uint32(len(members)))
	for _, m := range members {
		mainBody.Write(m.fileID[:])
	}
	// RecoverySetID = MD5 of the Main packet body, per spec.
	recoverySetID := md5.Sum(mainBody.Bytes())

	var index bytes.Buffer
	writePacket(&index, recoverySetID, PacketTypePARMain(), mainBody.Bytes())
	for _, m := range members {
		writePacket(&index, recoverySetID, PacketTypeFileDescT(), fileDescBody(m.entry, m.fileID, m.hash16k))
	}
	for i, m := range members {
		var body bytes.Buffer
		body.Write(m.fileID[:])
		for _, c := range sliceChecks[i] {
			body.Write(c.md5[:])
			binary.Write(&body, binary.LittleEndian, c.crc32) //nolint:errcheck
		}
		writePacket(&index, recoverySetID, PacketTypeIFSCT(), body.Bytes())
	}

	// Recovery slices: exponent e in [0, numRecovery).
	volumes := make([][]byte, 0, numRecovery)
	for e := range numRecovery {
		acc := make([]byte, sliceSize)
		for j, sl := range allSlices {
			gf2p16.MulAndAddByteSliceLE(VandermondeBase(j).Pow(uint32(e)), sl, acc)
		}
		var body bytes.Buffer
		binary.Write(&body, binary.LittleEndian, uint32(e)) //nolint:errcheck
		body.Write(acc)
		var vol bytes.Buffer
		writePacket(&vol, recoverySetID, PacketTypeRecvSlicT(), body.Bytes())
		volumes = append(volumes, vol.Bytes())
	}

	return FullSet{Index: index.Bytes(), Volumes: volumes}
}

type sliceCheck struct {
	md5   [16]byte
	crc32 uint32
}

// fileIdentity computes (FileID, Hash16k) exactly as writeFileDesc does.
func fileIdentity(e FileEntry) (fileID, hash16k [16]byte) {
	padded := make([]byte, 16384)
	copy(padded, e.Content)
	hash16k = md5.Sum(padded)

	var src bytes.Buffer
	src.Write(hash16k[:])
	var lenBuf [8]byte
	binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(e.Content)))
	src.Write(lenBuf[:])
	src.Write([]byte(e.Name))
	fileID = md5.Sum(src.Bytes())
	return fileID, hash16k
}

// fileDescBody builds a FileDesc packet body for the entry.
func fileDescBody(e FileEntry, fileID, hash16k [16]byte) []byte {
	fileMD5 := md5.Sum(e.Content)
	nameBytes := []byte(e.Name)
	alignedNameLen := (len(nameBytes) + 3) &^ 3
	paddedName := make([]byte, alignedNameLen)
	copy(paddedName, nameBytes)

	var body bytes.Buffer
	body.Write(fileID[:])
	body.Write(fileMD5[:])
	body.Write(hash16k[:])
	binary.Write(&body, binary.LittleEndian, uint64(len(e.Content))) //nolint:errcheck
	body.Write(paddedName)
	return body.Bytes()
}

// writePacket emits one PAR2 packet: 64-byte header + body. The packet MD5
// covers bytes 32..end (RecoverySetID + type + body).
func writePacket(w *bytes.Buffer, recoverySetID [16]byte, packetType [16]byte, body []byte) {
	magic := [8]byte{'P', 'A', 'R', '2', 0, 'P', 'K', 'T'}
	totalLen := uint64(64 + len(body))

	var md5Input bytes.Buffer
	md5Input.Write(recoverySetID[:])
	md5Input.Write(packetType[:])
	md5Input.Write(body)
	packetMD5 := md5.Sum(md5Input.Bytes())

	w.Write(magic[:])
	binary.Write(w, binary.LittleEndian, totalLen) //nolint:errcheck
	w.Write(packetMD5[:])
	w.Write(recoverySetID[:])
	w.Write(packetType[:])
	w.Write(body)
}

// Packet type constants, duplicated from the par2 parser package to avoid a
// testsupport -> production import in this direction (the parser tests import
// par2gen, so par2gen importing the parser would be a cycle).
func PacketTypePARMain() [16]byte {
	return [16]byte{'P', 'A', 'R', ' ', '2', '.', '0', 0, 'M', 'a', 'i', 'n', 0, 0, 0, 0}
}
func PacketTypeFileDescT() [16]byte {
	return [16]byte{'P', 'A', 'R', ' ', '2', '.', '0', 0, 'F', 'i', 'l', 'e', 'D', 'e', 's', 'c'}
}
func PacketTypeIFSCT() [16]byte {
	return [16]byte{'P', 'A', 'R', ' ', '2', '.', '0', 0, 'I', 'F', 'S', 'C', 0, 0, 0, 0}
}
func PacketTypeRecvSlicT() [16]byte {
	return [16]byte{'P', 'A', 'R', ' ', '2', '.', '0', 0, 'R', 'e', 'c', 'v', 'S', 'l', 'i', 'c'}
}

// VandermondeBase returns the j-th PAR2 Vandermonde generator: 2^i in
// GF(2^16), skipping exponents divisible by 3, 5, 17 or 257 (elements whose
// order is below 65535). Grows a cached table on demand.
func VandermondeBase(j int) gf2p16.T {
	basesMu.Lock()
	defer basesMu.Unlock()
	for len(bases) <= j {
		i := nextExp
		nextExp++
		if i%3 == 0 || i%5 == 0 || i%17 == 0 || i%257 == 0 {
			continue
		}
		bases = append(bases, gf2p16.T(2).Pow(uint32(i)))
	}
	return bases[j]
}

var (
	basesMu sync.Mutex
	bases   []gf2p16.T
	nextExp int
)
