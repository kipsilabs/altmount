package par2

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
)

// SliceCheck is one input slice's checksums from an IFSC packet. Checksums
// are computed over the slice zero-padded to the set's slice size.
type SliceCheck struct {
	MD5   [16]byte
	CRC32 uint32
}

// RecoverySliceRef locates one RecvSlic payload without loading it: the
// payload lives at BodyOffset within the FileIndex-th input stream and is
// exactly SliceSize bytes long.
//
// PacketMD5 and SetID let the payload's integrity be verified when it is
// finally fetched: the packet hash covers SetID + Type + exponent + payload,
// so a corrupt payload can be rejected before it seeds the solver.
type RecoverySliceRef struct {
	Exponent   uint32
	FileIndex  int
	BodyOffset int64
	PacketMD5  [16]byte
	SetID      [16]byte
}

// Index is the parsed, recovery-relevant view of a PAR2 set: everything
// needed to plan and run a repair except the recovery payloads themselves.
type Index struct {
	SliceSize   uint64
	RecoveryIDs [][16]byte // recovery-set FileIDs in Main packet order, which numbers the global slices
	Files       map[[16]byte]FileDescriptor
	SliceChecks map[[16]byte][]SliceCheck
	Recovery    []RecoverySliceRef
	// Partial is set when the parse stopped before reading every stream, so
	// the set may hold recovery slices that Recovery doesn't list.
	Partial bool
}

// maxIndexPacketSize bounds a packet's declared length; anything larger is a
// corrupt length field.
const maxIndexPacketSize = 1 << 30

// maxMetadataPacketBody bounds the packets that are read into memory whole.
// The largest of them is a file's slice checksums, twenty bytes per slice;
// anything past this is a corrupt length field wearing a metadata type.
const maxMetadataPacketBody = 64 << 20

// ParseIndex scans PAR2 packets across the given streams (typically the
// .par2 index file followed by every .volXX+YY.par2 file, in a stable order —
// RecoverySliceRef.FileIndex refers to positions in this slice). Recovery
// slice payloads are skipped, not loaded; only their locations are recorded.
//
// Damage costs only the packets it hits: every metadata packet is verified
// against its own MD5 and dropped on mismatch (PAR2 repeats packets across
// files precisely so another copy can fill in), and a corrupt header is
// skipped by scanning for the next packet magic. Recovery slice packets are
// not hashed here — that would download every payload — so their refs carry
// the packet MD5 for verification at fetch time instead.
func ParseIndex(streams []io.Reader) (*Index, error) {
	return ParseIndexUntil(streams, nil)
}

// ParseIndexUntil is ParseIndex that stops opening streams once enough reports
// the set is planable. enough is asked between streams, and only once idx has
// complete metadata; nil reads every stream. A parse that stops early lists only
// the recovery slices it has read.
func ParseIndexUntil(streams []io.Reader, enough func(*Index) bool) (*Index, error) {
	idx := &Index{
		Files:       make(map[[16]byte]FileDescriptor),
		SliceChecks: make(map[[16]byte][]SliceCheck),
	}
	var mainSeen bool

	// A PAR2 set is routinely served from usenet, where individual recovery
	// volumes may be unreachable. An unreadable stream contributes nothing but
	// must not abort the parse: the remaining volumes still carry usable
	// recovery slices, and dropping them turns a repairable release into a
	// lost one. Only a set with no Main packet at all is fatal (checked below).
	var streamErrs []string
	seenExp := make(map[uint32]bool)
	for fi, stream := range streams {
		if err := parseIndexStream(stream, fi, idx, seenExp, &mainSeen); err != nil {
			streamErrs = append(streamErrs, fmt.Sprintf("stream %d: %v", fi, err))
		}
		if enough != nil && fi < len(streams)-1 && mainSeen && validateIndex(idx) == nil && enough(idx) {
			idx.Partial = true
			break
		}
	}

	if !mainSeen {
		if len(streamErrs) > 0 {
			return nil, fmt.Errorf("par2 index: no Main packet found (%s)", strings.Join(streamErrs, "; "))
		}
		return nil, fmt.Errorf("par2 index: no Main packet found")
	}
	if err := validateIndex(idx); err != nil {
		return nil, err
	}
	return idx, nil
}

// parseIndexStream merges one stream's packets into idx. It returns an error
// only when the underlying reader fails; damaged packets are skipped by
// resynchronising on the next packet magic, and a cleanly truncated tail ends
// the stream silently.
func parseIndexStream(stream io.Reader, fi int, idx *Index, seenExp map[uint32]bool, mainSeen *bool) error {
	cr := &countingReader{r: stream}
	var header [PacketHeaderSize]byte
	for {
		if _, err := io.ReadFull(cr, header[:]); err != nil {
			if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil // fewer bytes left than a header: no packet however read
			}
			return err
		}

		if [8]byte(header[:8]) != MagicBytes {
			// Not a packet boundary. The next magic may begin inside the bytes
			// just read, so they go back into the scan.
			found, err := cr.resync(header[1:])
			if err != nil || !found {
				return err
			}
			continue
		}

		length := binary.LittleEndian.Uint64(header[8:16])
		if length < PacketHeaderSize || length%4 != 0 || length > maxIndexPacketSize {
			// A bad length is not recoverable by guessing; step past this
			// magic and look for the next packet.
			found, err := cr.resync(header[8:])
			if err != nil || !found {
				return err
			}
			continue
		}
		bodyLen := int64(length) - PacketHeaderSize

		var packetType [16]byte
		copy(packetType[:], header[48:64])

		switch packetType {
		case PacketTypeRecoverySlice:
			if bodyLen < 4 {
				found, err := cr.resync(nil)
				if err != nil || !found {
					return err
				}
				continue
			}
			var expBuf [4]byte
			if _, err := io.ReadFull(cr, expBuf[:]); err != nil {
				if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
					return nil
				}
				return err
			}
			exponent := binary.LittleEndian.Uint32(expBuf[:])
			offset := cr.pos()
			if err := cr.skip(bodyLen - 4); err != nil {
				// Truncated payload: the ref would point at bytes the stream
				// does not carry, so it is not recorded.
				if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
					return nil
				}
				return err
			}
			if !seenExp[exponent] {
				seenExp[exponent] = true
				ref := RecoverySliceRef{Exponent: exponent, FileIndex: fi, BodyOffset: offset}
				copy(ref.PacketMD5[:], header[16:32])
				copy(ref.SetID[:], header[32:48])
				idx.Recovery = append(idx.Recovery, ref)
			}

		case PacketTypePARMain, PacketTypeFileDesc, PacketTypeIFSC:
			if bodyLen > maxMetadataPacketBody {
				// Not a metadata packet whatever its type says: a corrupt
				// length field, stepped past like any other.
				found, err := cr.resync(header[8:])
				if err != nil || !found {
					return err
				}
				continue
			}
			body := make([]byte, bodyLen)
			if _, err := io.ReadFull(cr, body); err != nil {
				if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
					return nil
				}
				return err
			}
			// The packet hash covers everything after the hash field itself.
			sum := md5.New()
			sum.Write(header[32:PacketHeaderSize])
			sum.Write(body)
			if !bytes.Equal(sum.Sum(nil), header[16:32]) {
				continue // damaged copy; another file carries this packet again
			}
			absorbMetadataPacket(packetType, header, body, idx, mainSeen)

		default:
			// Unknown packet (e.g. Creator): its content is unused, so it is
			// skipped without being read or verified.
			if err := cr.skip(bodyLen); err != nil {
				if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
					return nil
				}
				return err
			}
		}
	}
}

// absorbMetadataPacket dispatches one hash-verified metadata packet body. A
// body the parsers reject despite the hash is a spec violation in the file
// itself; the packet is dropped like a damaged one.
func absorbMetadataPacket(packetType [16]byte, header [PacketHeaderSize]byte, body []byte, idx *Index, mainSeen *bool) {
	switch packetType {
	case PacketTypePARMain:
		if err := parseMainBody(bytes.NewReader(body), int64(len(body)), idx); err == nil {
			*mainSeen = true
		}
	case PacketTypeFileDesc:
		var h PacketHeader
		copy(h.Magic[:], header[0:8])
		h.Length = binary.LittleEndian.Uint64(header[8:16])
		copy(h.MD5Hash[:], header[16:32])
		copy(h.RecoveryID[:], header[32:48])
		copy(h.Type[:], header[48:64])
		if desc, err := NewPacketReader(bytes.NewReader(body)).ReadFileDescriptor(&h); err == nil {
			idx.Files[desc.FileID] = *desc
		}
	case PacketTypeIFSC:
		_ = parseIFSCBody(bytes.NewReader(body), int64(len(body)), idx)
	}
}

// parseMainBody reads slice size + recovery-set FileIDs. Non-recovery-set
// FileIDs at the tail of the body are skipped.
func parseMainBody(r io.Reader, bodyLen int64, idx *Index) error {
	if bodyLen < 12 {
		return fmt.Errorf("main packet body too small: %d", bodyLen)
	}
	var head struct {
		SliceSize uint64
		NumFiles  uint32
	}
	if err := binary.Read(r, binary.LittleEndian, &head); err != nil {
		return fmt.Errorf("read main packet: %w", err)
	}
	if head.SliceSize == 0 || head.SliceSize%4 != 0 {
		return fmt.Errorf("invalid slice size %d", head.SliceSize)
	}
	rest := bodyLen - 12
	if int64(head.NumFiles)*16 > rest {
		return fmt.Errorf("main packet declares %d recovery files but body has %d bytes", head.NumFiles, rest)
	}
	idx.SliceSize = head.SliceSize
	idx.RecoveryIDs = make([][16]byte, head.NumFiles)
	for i := range idx.RecoveryIDs {
		if _, err := io.ReadFull(r, idx.RecoveryIDs[i][:]); err != nil {
			return fmt.Errorf("read recovery-set FileID %d: %w", i, err)
		}
	}
	return nil
}

// parseIFSCBody reads a file's per-slice checksums.
func parseIFSCBody(r io.Reader, bodyLen int64, idx *Index) error {
	if bodyLen < 16 || (bodyLen-16)%20 != 0 {
		return fmt.Errorf("ifsc packet body has invalid size %d", bodyLen)
	}
	var fileID [16]byte
	if _, err := io.ReadFull(r, fileID[:]); err != nil {
		return fmt.Errorf("read IFSC FileID: %w", err)
	}
	n := (bodyLen - 16) / 20
	checks := make([]SliceCheck, n)
	buf := make([]byte, 20)
	for i := range checks {
		if _, err := io.ReadFull(r, buf); err != nil {
			return fmt.Errorf("read IFSC entry %d: %w", i, err)
		}
		copy(checks[i].MD5[:], buf[:16])
		checks[i].CRC32 = binary.LittleEndian.Uint32(buf[16:])
	}
	idx.SliceChecks[fileID] = checks
	return nil
}

// validateIndex checks that every recovery-set member has a FileDesc and a
// slice-count-consistent IFSC.
func validateIndex(idx *Index) error {
	for _, id := range idx.RecoveryIDs {
		fd, ok := idx.Files[id]
		if !ok {
			return fmt.Errorf("par2 index: recovery-set member %x has no FileDesc packet", id)
		}
		checks, ok := idx.SliceChecks[id]
		if !ok {
			return fmt.Errorf("par2 index: file %q has no IFSC packet", fd.Name)
		}
		want := int((fd.Length + idx.SliceSize - 1) / idx.SliceSize)
		if len(checks) != want {
			return fmt.Errorf("par2 index: file %q has %d slice checksums, want %d", fd.Name, len(checks), want)
		}
	}
	return nil
}

// countingReader tracks the absolute byte position within a stream so
// RecvSlic payload offsets can be recorded, and carries the scan state that
// lets the parser resynchronise after damage. When the underlying stream can
// Seek, skips avoid reading the skipped bytes — this is what keeps ParseIndex
// from downloading every recovery payload when streams are lazy article
// readers over NNTP.
type countingReader struct {
	r io.Reader
	// n is the caller-visible position: bytes consumed via Read and skip.
	n int64
	// unread holds bytes already pulled from r (by resync) that the caller
	// has not consumed yet. They sit at position n.
	unread []byte
}

// pos is the position the next Read will consume from.
func (c *countingReader) pos() int64 { return c.n }

func (c *countingReader) Read(p []byte) (int, error) {
	if len(c.unread) > 0 {
		k := copy(p, c.unread)
		c.unread = c.unread[k:]
		c.n += int64(k)
		return k, nil
	}
	k, err := c.r.Read(p)
	c.n += int64(k)
	return k, err
}

// skip advances past n bytes, seeking when possible.
func (c *countingReader) skip(n int64) error {
	if n <= 0 {
		return nil
	}
	if u := int64(len(c.unread)); u > 0 {
		k := min(u, n)
		c.unread = c.unread[k:]
		c.n += k
		n -= k
		if n == 0 {
			return nil
		}
	}
	if s, ok := c.r.(io.Seeker); ok {
		if _, err := s.Seek(n, io.SeekCurrent); err == nil {
			c.n += n
			return nil
		}
	}
	k, err := io.CopyN(io.Discard, c.r, n)
	c.n += k
	if err == nil && k < n {
		err = io.ErrUnexpectedEOF
	}
	return err
}

// resyncChunk is the window resync reads per pass while looking for the next
// packet magic. Damage past a packet boundary is scanned through in these
// steps; nothing else is bounded by it.
const resyncChunk = 32 << 10

// resync scans forward for the next packet magic and reports whether one was
// found; the caller's next Read then starts exactly at it. pending is the run
// of already-consumed bytes the magic may begin inside (the suspect header,
// minus whatever prefix is known not to start a packet).
//
// Damage does not respect packet boundaries, so after a packet that could not
// be trusted the next one has to be found rather than calculated.
func (c *countingReader) resync(pending []byte) (bool, error) {
	// Hand the pending bytes back: they were counted as consumed when read,
	// but the scan may leave a suffix of them unconsumed.
	c.n -= int64(len(pending))
	buf := make([]byte, 0, len(pending)+len(c.unread)+resyncChunk)
	buf = append(buf, pending...)
	buf = append(buf, c.unread...)
	c.unread = nil

	for {
		if i := bytes.Index(buf, MagicBytes[:]); i >= 0 {
			c.n += int64(i) // the garbage before the magic is consumed
			c.unread = buf[i:]
			return true, nil
		}
		// The magic could straddle the window's end, so the bytes it could
		// begin in stay for the next pass.
		if keep := len(MagicBytes) - 1; len(buf) > keep {
			drop := len(buf) - keep
			c.n += int64(drop)
			buf = append(buf[:0], buf[drop:]...)
		}
		chunk := make([]byte, resyncChunk)
		k, err := c.r.Read(chunk)
		if k > 0 {
			buf = append(buf, chunk[:k]...)
		}
		if err != nil || k == 0 {
			c.n += int64(len(buf))
			if err == nil || err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
				return false, nil
			}
			return false, err
		}
	}
}
