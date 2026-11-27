package api

import (
	"encoding/json"
	"regexp"
	"strings"

	"github.com/kipsilabs/altmount/internal/database"
)

// stremioImporterSuffixPattern is the exact shape stremioPlayNzbFilename
// appends to a Stremio play's NZB name: sha256(downloadURL)[:8] rendered as 16
// lowercase hex digits. It is only a fallback for rows whose metadata predates
// the release key. When a release key is present, the suffix must match its
// digest, so a title that itself ends in hex is never mistaken for the
// importer's suffix.
var stremioImporterSuffixPattern = regexp.MustCompile(`-[0-9a-f]{16}$`)

// stremioReleaseKeyDigest returns the hex digest at the end of a release key.
// stremioReleaseKey formats it as source:indexer:<digest>.
func stremioReleaseKeyDigest(releaseKey string) string {
	if releaseKey == "" {
		return ""
	}
	if i := strings.LastIndex(releaseKey, ":"); i >= 0 {
		return releaseKey[i+1:]
	}
	return releaseKey
}

// stremioOriginatedMetadata reports whether queue metadata was written by the
// Stremio play handler. It is the only writer of these fields.
func stremioOriginatedMetadata(metadata *string) bool {
	if metadata == nil || *metadata == "" {
		return false
	}
	var m stremioQueueMetadata
	if json.Unmarshal([]byte(*metadata), &m) != nil {
		return false
	}
	return m.ContentKey != "" || m.ReleaseKey != ""
}

func stremioPlayDownloadID(downloadID *string) bool {
	return downloadID != nil && strings.HasPrefix(*downloadID, stremioDownloadIDPrefix)
}

// stremioQueueItemOriginated reports whether a queue row came from the Stremio
// play handler. Radarr, Sonarr, uploads and direct NZB streams leave both
// markers empty, so their display names must stay byte-for-byte unchanged.
func stremioQueueItemOriginated(item *database.ImportQueueItem) bool {
	return item != nil && (stremioPlayDownloadID(item.DownloadID) || stremioOriginatedMetadata(item.Metadata))
}

// stremioHistoryOriginated reports whether an import-history row came from the
// Stremio play handler. The history row copies the queue row's download_id and
// metadata.
func stremioHistoryOriginated(h *database.ImportHistory) bool {
	return h != nil && (stremioPlayDownloadID(h.DownloadID) || stremioOriginatedMetadata(h.Metadata))
}

// splitNzbExtension splits name into its stem and the .nzb/.nzb.gz extension
// the display name preserves.
func splitNzbExtension(name string) (stem, ext string) {
	lower := strings.ToLower(name)
	switch {
	case strings.HasSuffix(lower, ".nzb.gz"):
		cut := len(name) - len(".nzb.gz")
		return name[:cut], name[cut:]
	case strings.HasSuffix(lower, ".nzb"):
		cut := len(name) - len(".nzb")
		return name[:cut], name[cut:]
	case strings.HasSuffix(lower, ".gz"):
		cut := len(name) - len(".gz")
		return name[:cut], name[cut:]
	}
	return name, ""
}

// trimStremioImporterSuffix removes the play handler's collision suffix from a
// display name while preserving the NZB extension. A release key proves the
// suffix belongs to the importer; without one (legacy rows) only the
// generator's exact 16-hex shape is trimmed.
func trimStremioImporterSuffix(name, releaseKey string) string {
	stem, ext := splitNzbExtension(name)
	digest := stremioReleaseKeyDigest(releaseKey)
	if digest != "" {
		if len(digest) >= 16 {
			marker := "-" + digest[:16]
			if strings.HasSuffix(stem, marker) {
				return strings.TrimSuffix(stem, marker) + ext
			}
		}
		// A release key was present but did not match: the trailing text is
		// part of the release name, not the importer's hash.
		return name
	}
	if loc := stremioImporterSuffixPattern.FindStringIndex(stem); loc != nil {
		return stem[:loc[0]] + ext
	}
	return name
}

// queueItemDisplayName is the filename the queue UI shows. It never changes
// nzb_path; only the Stremio play handler's collision hash is hidden.
func queueItemDisplayName(item *database.ImportQueueItem) string {
	if item == nil {
		return ""
	}
	name := item.NzbPath
	if i := strings.LastIndexAny(name, `/\`); i >= 0 {
		name = name[i+1:]
	}
	if strings.HasSuffix(strings.ToLower(name), ".gz") {
		name = name[:len(name)-3]
	}
	if !stremioQueueItemOriginated(item) {
		return name
	}
	releaseKey, _ := stremioQueueItemReleaseKey(item)
	return trimStremioImporterSuffix(name, releaseKey)
}

// importHistoryDisplayName is the NZB name the history UI shows. It never
// changes the stored history row or its nzb_name field.
func importHistoryDisplayName(h *database.ImportHistory) string {
	if h == nil {
		return ""
	}
	if !stremioHistoryOriginated(h) {
		return h.NzbName
	}
	releaseKey := ""
	if h.Metadata != nil {
		var m stremioQueueMetadata
		if json.Unmarshal([]byte(*h.Metadata), &m) == nil {
			releaseKey = m.ReleaseKey
		}
	}
	return trimStremioImporterSuffix(h.NzbName, releaseKey)
}
