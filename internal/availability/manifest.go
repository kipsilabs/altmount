package availability

import (
	"fmt"
	"math"
	"strings"

	metapb "github.com/javi11/altmount/internal/metadata/proto"
)

// BuildManifestIdentity returns the v1 digest for resolved, eligible main-file metadata.
// It intentionally ignores paths and ordinary PAR2 sidecars.
func BuildManifestIdentity(metadata *metapb.FileMetadata) (string, error) {
	if metadata == nil {
		return "", fmt.Errorf("manifest metadata is nil")
	}
	if metadata.Encryption != metapb.Encryption_NONE || metadata.Password != "" || metadata.Salt != "" || len(metadata.AesKey) > 0 || len(metadata.AesIv) > 0 {
		return "", fmt.Errorf("encrypted manifest is unsupported")
	}
	if len(metadata.NestedSources) > 0 {
		return "", fmt.Errorf("nested manifest is unsupported")
	}
	if len(metadata.SharedOuterSources) > 0 {
		return "", fmt.Errorf("shared outer sources are unsupported")
	}
	if len(metadata.ClipBoundaries) > 0 {
		return "", fmt.Errorf("clip boundaries are unsupported")
	}
	if len(metadata.SegmentData) == 0 {
		return "", fmt.Errorf("manifest has no main segments")
	}

	fields := []string{"availability-manifest-v1", itoa64(metadata.FileSize), itoa(len(metadata.SegmentData))}
	for index, segment := range metadata.SegmentData {
		if segment == nil {
			return "", fmt.Errorf("manifest segment %d is nil", index)
		}
		id := normalizeMessageID(segment.Id)
		if id == "" {
			return "", fmt.Errorf("manifest segment %d has an empty message ID", index)
		}
		if segment.StartOffset < 0 || segment.EndOffset < 0 || segment.StartOffset > segment.EndOffset || segment.EndOffset == math.MaxInt64 {
			return "", fmt.Errorf("manifest segment %d has invalid offsets", index)
		}
		span := segment.EndOffset - segment.StartOffset + 1
		if span <= 0 || segment.SegmentSize <= 0 || span != segment.SegmentSize {
			return "", fmt.Errorf("manifest segment %d has an invalid size", index)
		}
		fields = append(fields, id, itoa64(segment.SegmentSize), itoa64(segment.StartOffset), itoa64(segment.EndOffset))
	}
	return sha256Digest([]byte(canonicalFields(fields...))), nil
}

func normalizeMessageID(messageID string) string {
	messageID = strings.TrimSpace(messageID)
	if len(messageID) >= 2 && messageID[0] == '<' && messageID[len(messageID)-1] == '>' {
		messageID = strings.TrimSpace(messageID[1 : len(messageID)-1])
	}
	return messageID
}
