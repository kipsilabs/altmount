package par2

// PAR2 packet type identifiers
// Reference: https://github.com/akalin/gopar/blob/main/par2/packet.go
var (
	// PacketTypePARMain is the main packet type "PAR 2.0\0Main\0\0\0\0"
	PacketTypePARMain = [16]byte{'P', 'A', 'R', ' ', '2', '.', '0', 0, 'M', 'a', 'i', 'n', 0, 0, 0, 0}

	// PacketTypeFileDesc is the file description packet type "PAR 2.0\0FileDesc"
	PacketTypeFileDesc = [16]byte{'P', 'A', 'R', ' ', '2', '.', '0', 0, 'F', 'i', 'l', 'e', 'D', 'e', 's', 'c'}

	// PacketTypeIFSC is the input file slice checksum packet type "PAR 2.0\0IFSC\0\0\0\0"
	PacketTypeIFSC = [16]byte{'P', 'A', 'R', ' ', '2', '.', '0', 0, 'I', 'F', 'S', 'C', 0, 0, 0, 0}

	// PacketTypeRecoverySlice is the recovery slice packet type "PAR 2.0\0RecvSlic"
	PacketTypeRecoverySlice = [16]byte{'P', 'A', 'R', ' ', '2', '.', '0', 0, 'R', 'e', 'c', 'v', 'S', 'l', 'i', 'c'}

	// PacketTypeCreator is the creator packet type "PAR 2.0\0Creator\0"
	PacketTypeCreator = [16]byte{'P', 'A', 'R', ' ', '2', '.', '0', 0, 'C', 'r', 'e', 'a', 't', 'o', 'r', 0}
)

// MagicBytes is the PAR2 magic signature "PAR2\0PKT"
var MagicBytes = [8]byte{'P', 'A', 'R', '2', 0, 'P', 'K', 'T'}

// PacketHeader represents the header of a PAR2 packet
// All PAR2 packets begin with this 64-byte header
// Reference: https://github.com/akalin/gopar/blob/main/par2/packet.go
type PacketHeader struct {
	Magic      [8]byte  // "PAR2\0PKT"
	Length     uint64   // Total packet length including header (must be multiple of 4)
	MD5Hash    [16]byte // MD5 hash of entire packet except first 32 bytes
	RecoveryID [16]byte // Recovery Set ID - same for all packets in a PAR2 set
	Type       [16]byte // Packet type identifier
}

// FileDescriptor represents a file description from a PAR2 FileDesc packet
// Reference: https://github.com/akalin/gopar/blob/main/par2/file_description_packet.go
type FileDescriptor struct {
	FileID  [16]byte // Unique file identifier (MD5 of [Hash16k, Length, Name])
	FileMD5 [16]byte // MD5 hash of entire file content
	Hash16k [16]byte // MD5 hash of first 16KB of file (for matching)
	Length  uint64   // File length in bytes
	Name    string   // Original filename (variable length, null-terminated, 4-byte aligned)
	// SameHead lists other set members of the same length whose first 16KB hash
	// identically. Lookups are keyed by Hash16k, so they would otherwise be lost.
	SameHead []*FileDescriptor
}

const (
	// PacketHeaderSize is the size of the PAR2 packet header in bytes
	PacketHeaderSize = 64

	// MinFileDescPacketSize is the minimum size for a file description packet
	// Header (64) + FileID (16) + FileMD5 (16) + Hash16k (16) + Length (8) = 120 bytes
	MinFileDescPacketSize = 120
)
