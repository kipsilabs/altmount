package archivehdr

import (
	"encoding/binary"
	"hash/crc32"
)

// crc32sum is the little-endian CRC-32 of b, as the RAR 5 block header stores it.
func crc32sum(b []byte) [4]byte {
	var out [4]byte
	binary.LittleEndian.PutUint32(out[:], crc32.ChecksumIEEE(b))
	return out
}
