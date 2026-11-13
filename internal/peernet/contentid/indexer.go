package contentid

import (
	"context"
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

	// Files of one release share one store; hashing a large store once per
	// file would be quadratic, so the most recent result is remembered. It is
	// keyed by the store's identity: a rewritten store is a new object.
	mu          sync.Mutex
	lastStore   *metapb.NzbStore
	lastRelease ID
}

// NewIndexer builds an Indexer.
func NewIndexer(src Source, repo Repository, opts ...Option) *Indexer {
	i := &Indexer{src: src, repo: repo, batchSize: defaultBatchSize}
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

// Entry computes the index row for a virtual path without storing it. The
// release ID is empty when the metadata has no shared store, or only one the
// migration rebuilt from the surviving files.
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
	e := database.PeernetContentEntry{FilePath: virtualPath, FileID: fileID.String()}
	if m.StoreRef != "" && !migratedStore(m.StoreRef) {
		store, err := i.src.ReadStore(m.StoreRef)
		if err != nil {
			return zero, fmt.Errorf("read store for %q: %w", virtualPath, err)
		}
		switch rid, err := i.releaseID(store); {
		case err == nil:
			e.ReleaseID = rid.String()
		case errors.Is(err, ErrNoSegments):
			// A store with no real article cannot name a release; keep the file ID.
		default:
			return zero, fmt.Errorf("release id %q: %w", virtualPath, err)
		}
	}
	return e, nil
}

// IndexPath computes and stores the IDs of one virtual file.
func (i *Indexer) IndexPath(ctx context.Context, virtualPath string) error {
	e, err := i.Entry(ctx, virtualPath)
	if err != nil {
		return err
	}
	return i.repo.Upsert(ctx, e)
}
