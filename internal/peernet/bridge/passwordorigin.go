package bridge

import (
	"context"
	"log/slog"

	"github.com/kipsilabs/altmount/internal/importer"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/peernet/contentid"
)

// MetadataReader reads resolved file metadata (nil, nil when absent).
type MetadataReader interface {
	ReadFileMetadata(virtualPath string) (*metapb.FileMetadata, error)
}

// PasswordOriginStore persists which files' passwords came with their NZB.
type PasswordOriginStore interface {
	MarkFromNZB(ctx context.Context, fileIDs []string) error
}

type pwRecorder struct {
	meta  MetadataReader
	store PasswordOriginStore
}

// NewNZBPasswordRecorder returns the importer's hook that records which
// written files carry key material derived from a password that came with the
// NZB's own content. Only files that carry an AES key (their own or an outer
// volume's) are recorded, by File ID, so the record survives renames. It never
// sees the password and never fails an import.
func NewNZBPasswordRecorder(meta MetadataReader, store PasswordOriginStore) importer.NZBPasswordRecorder {
	if meta == nil || store == nil {
		return nil
	}
	return pwRecorder{meta: meta, store: store}
}

func hasKeyMaterial(m *metapb.FileMetadata) bool {
	if len(m.GetAesKey()) > 0 {
		return true
	}
	for _, o := range m.GetSharedOuterSources() {
		if len(o.GetAesKey()) > 0 {
			return true
		}
	}
	for _, o := range m.GetNestedSources() {
		if len(o.GetAesKey()) > 0 {
			return true
		}
	}
	return false
}

func (r pwRecorder) RecordNZBPasswordFiles(ctx context.Context, paths []string) {
	ids := make([]string, 0, len(paths))
	for _, p := range paths {
		m, err := r.meta.ReadFileMetadata(p)
		if err != nil || m == nil || !hasKeyMaterial(m) {
			continue
		}
		id, err := contentid.FileID(m)
		if err != nil {
			continue
		}
		ids = append(ids, id.String())
	}
	if len(ids) == 0 {
		return
	}
	if err := r.store.MarkFromNZB(ctx, ids); err != nil {
		slog.WarnContext(ctx, "Could not record where an archive password came from", "files", len(ids), "error", err)
	}
}
