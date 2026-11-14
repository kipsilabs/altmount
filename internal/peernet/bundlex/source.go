// Package bundlex builds, serves and fetches peer-network bundles (level 3).
//
// The builder reads local data only through the narrow BuildSource and
// Library interfaces defined here; nothing else in the peernet packages
// receives full file metadata. The serve route answers a single request
// shape (one network, one Bundle ID this node announced there) and the fetch
// client downloads from the publisher of a signed release record only, then
// verifies the bytes with bundle.Verify against the IDs of the signed record.
package bundlex

import (
	"context"
	"strings"

	"github.com/kipsilabs/altmount/internal/metadata"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/peernet/contentid"
)

// BuildSource is the builder's only view of local import data. It is the one
// place the peernet code reads full per-file metadata (segments, layout and
// encryption fields); keep it out of every other package's reach.
type BuildSource interface {
	// ReadFile returns the fully resolved metadata of a virtual file, or
	// (nil, nil) when it is absent.
	ReadFile(virtualPath string) (*metapb.FileMetadata, error)
	// NZBSegmentIDs returns the message-IDs of every NZB file of the release
	// the file belongs to, in NZB order. It returns (nil, nil) when the file
	// has no release store (or only one the metadata migration rebuilt), so
	// the release has no NZB segment list. No store path is exposed.
	NZBSegmentIDs(virtualPath string) ([][]string, error)
	// NZBPassword reports whether the key material of the file derives from a
	// password present inside the NZB content itself (a meta password), and
	// returns that password. A password spelled in the NZB file name, given by
	// the user, an ARR or the API, or one whose origin is not recorded, is not
	// from the NZB.
	NZBPassword(virtualPath string, m *metapb.FileMetadata) (password string, fromNZB bool)
}

// PatchIndex says whether a local PAR2 patch exists for an article.
type PatchIndex interface {
	Has(messageID string) bool
}

// NoPatches is a PatchIndex for a node without a patch store.
type NoPatches struct{}

// Has implements PatchIndex.
func (NoPatches) Has(string) bool { return false }

// Library lists the indexed files of a release.
type Library interface {
	// ReleaseFiles returns the stored scheme (database.PeernetScheme*) and the
	// virtual paths of the release's files, one per distinct File ID.
	ReleaseFiles(ctx context.Context, releaseHex string) (scheme int, paths []string, err error)
}

// PasswordOrigins says whether a file's archive password came with the NZB's
// own content (the importer recorded it at import; see
// database.PeernetPasswordOriginRepository). Anything it does not know is "no".
type PasswordOrigins interface {
	FromNZB(ctx context.Context, fileID string) (bool, error)
}

type metadataSource struct {
	ms      *metadata.MetadataService
	origins PasswordOrigins
}

// NewMetadataSource adapts the metadata service to BuildSource.
//
// The metadata records the AES key and IV of an archive file but not the
// password they were derived from, nor where it came from. The importer
// records, for new imports only, that a file's password came with the NZB's own
// content (origins); every other file, including all imports from before that
// record existed, is "not from the NZB", and a release with key material is
// then not shareable.
func NewMetadataSource(ms *metadata.MetadataService, origins PasswordOrigins) BuildSource {
	return metadataSource{ms: ms, origins: origins}
}

func (s metadataSource) ReadFile(p string) (*metapb.FileMetadata, error) {
	return s.ms.ReadFileMetadata(p)
}

func (s metadataSource) NZBSegmentIDs(p string) ([][]string, error) {
	m, err := s.ms.ReadFileMetadata(p)
	if err != nil || m == nil {
		return nil, err
	}
	if m.StoreRef == "" || migratedStore(m.StoreRef) {
		return nil, nil
	}
	store, err := s.ms.Store().ReadStore(m.StoreRef)
	if err != nil || store == nil {
		return nil, err
	}
	out := make([][]string, len(store.Files))
	for i, f := range store.Files {
		if f == nil {
			continue
		}
		ids := make([]string, len(f.Segments))
		for j, sg := range f.Segments {
			if sg != nil {
				ids[j] = sg.Id
			}
		}
		out[i] = ids
	}
	return out, nil
}

// NZBPassword answers from the origin the importer recorded for the file's
// File ID (so a rename or move does not lose it). The password text is never
// stored anywhere, so the answer is always ("", fromNZB): the bundle then
// carries the key and IV, which is all a receiver needs to play the release.
// A file with no recorded origin, a lookup error or no origin store is not
// from the NZB.
func (s metadataSource) NZBPassword(_ string, m *metapb.FileMetadata) (string, bool) {
	if s.origins == nil || m == nil {
		return "", false
	}
	id, err := contentid.FileID(m)
	if err != nil {
		return "", false
	}
	from, err := s.origins.FromNZB(context.Background(), id.String())
	if err != nil || !from {
		return "", false
	}
	return "", true
}

func migratedStore(ref string) bool {
	for _, part := range strings.Split(strings.ReplaceAll(ref, "\\", "/"), "/") {
		if part == "_migrated" {
			return true
		}
	}
	return false
}
