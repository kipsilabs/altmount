package contentid

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"sync"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/metadata"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
)

// ErrMetadataNotFound means a virtual path has no metadata file.
var ErrMetadataNotFound = errors.New("contentid: metadata not found")

const defaultBatchSize = 200

// Source is the read-only view of the metadata tree the indexer needs.
type Source interface {
	// ReadFileMetadata returns resolved metadata, or (nil, nil) if absent.
	ReadFileMetadata(virtualPath string) (*metapb.FileMetadata, error)
	// ReadStore returns the shared NZB store at ref.
	ReadStore(ref string) (*metapb.NzbStore, error)
	// ListDirectoryAll returns sub-directories and file names (without the
	// .meta suffix) of a metadata directory.
	ListDirectoryAll(virtualPath string) ([]fs.FileInfo, []string, error)
}

// Repository is the persistence the indexer needs; it is satisfied by
// *database.PeernetContentRepository.
type Repository interface {
	Upsert(ctx context.Context, e database.PeernetContentEntry) error
	UpsertBatch(ctx context.Context, entries []database.PeernetContentEntry) error
	MissingPaths(ctx context.Context, candidates []string) ([]string, error)
	Delete(ctx context.Context, filePath string) error
	DeleteByPrefix(ctx context.Context, dir string) error
	RenamePrefix(ctx context.Context, oldDir, newDir string) error
	ListPage(ctx context.Context, after string, limit int) ([]database.PeernetContentEntry, error)
	GetByPath(ctx context.Context, filePath string) (database.PeernetContentEntry, bool, error)
	RefByPath(ctx context.Context, filePath string) (database.PeernetContentRef, bool, error)
	RefsByPrefix(ctx context.Context, dir string, limit int) ([]database.PeernetContentRef, error)
	GroupSummaries(ctx context.Context, after string, limit int) ([]database.PeernetGroupSummary, error)
	GroupCounts(ctx context.Context, keys []string) (map[string]int, error)
	GroupMembers(ctx context.Context, keys []string) ([]database.PeernetContentEntry, error)
	ApplyDerived(ctx context.Context, updates []database.PeernetDerivedUpdate) (int, error)
	ClearDerivedGroup(ctx context.Context, key string, batch int) ([]string, error)
	StillIndexed(ctx context.Context, releaseIDs []string) (map[string]bool, error)
}

// ServiceSource adapts a metadata.MetadataService to Source.
type ServiceSource struct{ ms *metadata.MetadataService }

// NewServiceSource wraps ms.
func NewServiceSource(ms *metadata.MetadataService) *ServiceSource { return &ServiceSource{ms: ms} }

// ReadFileMetadata implements Source.
func (s *ServiceSource) ReadFileMetadata(p string) (*metapb.FileMetadata, error) {
	return s.ms.ReadFileMetadata(p)
}

// ReadStore implements Source.
func (s *ServiceSource) ReadStore(ref string) (*metapb.NzbStore, error) {
	return s.ms.Store().ReadStore(ref)
}

// ListDirectoryAll implements Source.
func (s *ServiceSource) ListDirectoryAll(p string) ([]fs.FileInfo, []string, error) {
	return s.ms.ListDirectoryAll(p)
}

// Option configures an Indexer.
type Option func(*Indexer)

// WithBatchSize sets how many paths a backfill batch scans. Values below 1 are
// ignored.
func WithBatchSize(n int) Option {
	return func(i *Indexer) {
		if n > 0 {
			i.batchSize = n
		}
	}
}

// WithProgress registers a callback that receives the running totals of a
// Backfill after every batch. It must return quickly.
func WithProgress(fn func(BackfillResult)) Option {
	return func(i *Indexer) { i.progress = fn }
}

// Indexer computes File and Release IDs for virtual files and stores them in
// the local peernet content index.
type Indexer struct {
	src       Source
	repo      Repository
	batchSize int
	progress  func(BackfillResult)
	derive    *deriveState

	// Files of one release share one store; hashing a large store once per
	// file would be quadratic, so the most recent result is remembered. It is
	// keyed by the store's identity: a rewritten store is a new object.
	mu          sync.Mutex
	lastStore   *metapb.NzbStore
	lastRelease ID
}

// NewIndexer builds an Indexer.
func NewIndexer(src Source, repo Repository, opts ...Option) *Indexer {
	i := &Indexer{src: src, repo: repo, batchSize: defaultBatchSize, derive: newDeriveState()}
	for _, o := range opts {
		o(i)
	}
	return i
}

func (i *Indexer) releaseID(store *metapb.NzbStore) (ID, error) {
	i.mu.Lock()
	if i.lastStore == store && store != nil {
		id := i.lastRelease
		i.mu.Unlock()
		return id, nil
	}
	i.mu.Unlock()

	id, err := ReleaseID(store)
	if err != nil {
		return ID{}, err
	}
	i.mu.Lock()
	i.lastStore, i.lastRelease = store, id
	i.mu.Unlock()
	return id, nil
}

// migratedDir is the folder the metadata migration puts its rebuilt stores in
// (see metadata.MigrationWorker). Those stores are assembled from the files
// that survived import, not from the release's NZB.
const migratedDir = "_migrated"

// migratedStore reports whether ref is a store the migration rebuilt. Its
// segment list is partial, so it cannot name a release.
func migratedStore(ref string) bool {
	for _, part := range strings.Split(filepath.ToSlash(ref), "/") {
		if part == migratedDir {
			return true
		}
	}
	return false
}

// groupKey is the local-only key shared by the files of one import: a hash of
// the source NZB path the importer recorded in each file's metadata (it holds
// the queue item's own folder, so it is unique per import). It is empty when
// the metadata names no source. It is never transmitted and never part of an
// ID: only the File IDs of the group are.
func groupKey(sourceNzbPath string) string {
	p := strings.TrimSpace(strings.ReplaceAll(sourceNzbPath, "\\", "/"))
	if p == "" {
		return ""
	}
	sum := sha256.Sum256([]byte("altmount-peernet/import-group/v1\x00" + p))
	return hex.EncodeToString(sum[:])
}

// Entry computes the index row for a virtual path without storing it. The
// release ID is empty (scheme none) when the metadata has no shared store, or
// only one the migration rebuilt from the surviving files; such files get a
// library-derived ID later, from their group (see DeriveAll).
func (i *Indexer) Entry(ctx context.Context, virtualPath string) (database.PeernetContentEntry, error) {
	var zero database.PeernetContentEntry
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	m, err := i.src.ReadFileMetadata(virtualPath)
	if err != nil {
		return zero, fmt.Errorf("read metadata %q: %w", virtualPath, err)
	}
	if m == nil {
		return zero, fmt.Errorf("%w: %q", ErrMetadataNotFound, virtualPath)
	}
	fileID, err := FileID(m)
	if err != nil {
		return zero, fmt.Errorf("file id %q: %w", virtualPath, err)
	}
	e := database.PeernetContentEntry{FilePath: virtualPath, FileID: fileID.String(), GroupKey: groupKey(m.SourceNzbPath)}
	if m.StoreRef != "" && !migratedStore(m.StoreRef) {
		store, err := i.src.ReadStore(m.StoreRef)
		if err != nil {
			return zero, fmt.Errorf("read store for %q: %w", virtualPath, err)
		}
		switch rid, err := i.releaseID(store); {
		case err == nil:
			e.ReleaseID, e.ReleaseScheme = rid.String(), database.PeernetSchemeNZB
		case errors.Is(err, ErrNoSegments):
			// A store with no real article cannot name a release; keep the file ID.
		default:
			return zero, fmt.Errorf("release id %q: %w", virtualPath, err)
		}
	}
	return e, nil
}

// sameIdentity reports whether a stored row already says what a freshly
// computed entry says. A derived ID (scheme files) is not part of the entry:
// it belongs to the row's group, so a row that has one is the same as an entry
// without a store-based ID as long as the file and the group are unchanged.
func sameIdentity(row, e database.PeernetContentEntry) bool {
	if row.FileID != e.FileID || row.GroupKey != e.GroupKey {
		return false
	}
	if e.ReleaseScheme == database.PeernetSchemeNZB {
		return row.ReleaseScheme == database.PeernetSchemeNZB && row.ReleaseID == e.ReleaseID
	}
	return row.ReleaseScheme != database.PeernetSchemeNZB
}

// IndexPath computes and stores the IDs of one virtual file. A file whose row
// already says the same is not written again (health checks and status
// updates rewrite metadata all the time), so the row never flickers and no
// group is re-derived for nothing. A changed row marks its group, and the old
// one if the group moved, for the next derive pass.
func (i *Indexer) IndexPath(ctx context.Context, virtualPath string) error {
	e, err := i.Entry(ctx, virtualPath)
	if err != nil {
		return err
	}
	old, had, err := i.repo.GetByPath(ctx, virtualPath)
	if err != nil {
		return fmt.Errorf("read index row %q: %w", virtualPath, err)
	}
	if had && sameIdentity(old, e) {
		return nil
	}
	if had && keepsBundleGrouping(old, e) {
		// The release identity came from a verified bundle; the metadata of a
		// bundle import cannot recompute it or its group.
		return nil
	}
	if err := i.repo.Upsert(ctx, e); err != nil {
		return err
	}
	if had {
		i.noteChanged(old, e)
	} else {
		i.noteChanged(database.PeernetContentEntry{}, e)
	}
	return nil
}
