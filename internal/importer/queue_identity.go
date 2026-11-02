package importer

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/javi11/nzbparser"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/importer/parser"
	"github.com/kipsilabs/altmount/internal/nzbfile"
)

// contentIdentityVersion namespaces the hash input so a future identity scheme
// cannot collide with values already stored in import_queue.content_identity.
const contentIdentityVersion = "altmount/content-identity/v1"

// nzbPar2Pattern matches PAR2 recovery files, which do not carry release
// payload and must not influence the admission identity.
var nzbPar2Pattern = regexp.MustCompile(`(?i)\.par2$|\.p\d+$|\.vol\d+\+\d+\.par2$`)

// payloadFileIdentity is the per-file admission fingerprint of one NZB payload
// file. size is the sum of the file's segment bytes.
type payloadFileIdentity struct {
	size     int64
	identity string
}

// analyzeNzbForQueue calculates the payload size and the admission identity for
// an NZB or STRM file. The identity is the fingerprint of the largest payload
// file (the feature), because an import_queue row represents a release whose
// storage_path is the release folder. PAR2 recovery files are ignored so a
// re-packed NZB carrying the same feature under a different name still matches.
//
// The identity never uses the NZB path, file name, download URL, GUID, or
// title. Re-uploads with new Usenet message-IDs are a different article set and
// deliberately do not match.
func (s *Service) analyzeNzbForQueue(filePath string) (int64, string, error) {
	if strings.HasSuffix(strings.ToLower(filePath), ".strm") {
		file, err := os.Open(filePath)
		if err != nil {
			return 0, "", NewNonRetryableError("failed to open file for size calculation", err)
		}
		defer file.Close()
		size, err := s.calculateStrmFileSize(file)
		return size, "", err
	}

	file, err := nzbfile.Open(filePath)
	if err != nil {
		return 0, "", NewNonRetryableError("failed to open file for size calculation", err)
	}
	defer file.Close()

	n, err := nzbparser.Parse(file)
	if err != nil {
		return 0, "", NewNonRetryableError("failed to parse NZB XML for size calculation", err)
	}
	return analyzeParsedNzb(n)
}

// analyzeParsedNzb returns the total payload size (excluding PAR2 files) and
// the identity of the largest payload file. Ties are broken by identity so the
// choice is independent of file order in the NZB.
func analyzeParsedNzb(n *nzbparser.Nzb) (int64, string, error) {
	if len(n.Files) == 0 {
		return 0, "", NewNonRetryableError("NZB file contains no files", nil)
	}

	parser.SanitizeNzbFilenames(n)

	var totalSize int64
	candidates := make([]payloadFileIdentity, 0, len(n.Files))
	for _, file := range n.Files {
		if nzbPar2Pattern.MatchString(file.Filename) {
			continue
		}

		segmentIDs, size := orderedSegmentIDs(file.Segments)
		totalSize += size
		if size <= 0 || len(segmentIDs) == 0 {
			continue
		}
		candidates = append(candidates, payloadFileIdentity{
			size:     size,
			identity: payloadFileIdentityHash(size, segmentIDs),
		})
	}

	if len(candidates) == 0 {
		return totalSize, "", nil
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].size != candidates[j].size {
			return candidates[i].size > candidates[j].size
		}
		return candidates[i].identity < candidates[j].identity
	})
	return totalSize, candidates[0].identity, nil
}

// orderedSegmentIDs returns the file's size in segment bytes and its non-empty
// message-IDs in article order. The nzb parser already sorts by segment number,
// but equal or missing numbers leave the order unspecified, so sorting here by
// (number, ID) keeps two equivalent NZBs hashing identically. A file with any
// empty message-ID cannot be fingerprinted reliably and yields no IDs.
func orderedSegmentIDs(segments nzbparser.NzbSegments) ([]string, int64) {
	ordered := make([]nzbparser.NzbSegment, len(segments))
	copy(ordered, segments)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].Number != ordered[j].Number {
			return ordered[i].Number < ordered[j].Number
		}
		return ordered[i].ID < ordered[j].ID
	})

	ids := make([]string, 0, len(ordered))
	var size int64
	for _, segment := range ordered {
		size += int64(segment.Bytes)
		id := strings.TrimSpace(segment.ID)
		if id == "" {
			// A partial message-ID list would make the identity inconsistent
			// between otherwise identical NZBs; refuse to fingerprint.
			return nil, size
		}
		ids = append(ids, id)
	}
	return ids, size
}

// payloadFileIdentityHash hashes the file size and ordered message-IDs with
// explicit length prefixes so boundary shifts cannot produce the same input.
func payloadFileIdentityHash(size int64, segmentIDs []string) string {
	h := sha256.New()
	h.Write([]byte(contentIdentityVersion))
	h.Write([]byte{0})

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(size))
	h.Write(buf[:])
	for _, id := range segmentIDs {
		binary.BigEndian.PutUint64(buf[:], uint64(len(id)))
		h.Write(buf[:])
		h.Write([]byte(id))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// recordContentIdentity stores the admission identity for a row that reached
// completion without one. It is best-effort: a missing or unreadable NZB only
// means this import cannot be used for de-duplication later.
func (s *Service) recordContentIdentity(ctx context.Context, item *database.ImportQueueItem) {
	if s.database == nil || item == nil || item.NzbPath == "" {
		return
	}
	existing, err := s.database.Repository.GetContentIdentity(ctx, item.ID)
	if err != nil {
		s.log.WarnContext(ctx, "Failed to read content identity before completion",
			"queue_id", item.ID, "error", err)
		return
	}
	if existing != "" {
		return
	}
	_, identity, err := s.analyzeNzbForQueue(item.NzbPath)
	if err != nil || identity == "" {
		return
	}
	if err := s.database.Repository.SetContentIdentity(ctx, item.ID, identity); err != nil {
		s.log.WarnContext(ctx, "Failed to record content identity at completion",
			"queue_id", item.ID, "error", err)
	}
}
