package bundleimport

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/contentverify"
	"github.com/kipsilabs/altmount/internal/metadata"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/nzbfilesystem"
	"github.com/kipsilabs/altmount/internal/nzbfilesystem/segcache"
	"github.com/kipsilabs/altmount/internal/pool"
)

// FSStagerDeps are what the real stager reads through.
type FSStagerDeps struct {
	// Pool and Config are the same the serving stack uses.
	Pool   pool.Manager
	Config config.ConfigGetter
	// Cache is the shared segment cache (may be nil).
	Cache *segcache.Source
	// Root is the directory staging areas are created in. It must be outside
	// the metadata root, so nothing staged can ever be listed or mounted.
	Root string
}

// FSStager stages files in a private metadata service rooted in a temporary
// directory and opens them through a real NzbFilesystem built over it, which
// is the same reader stack that will later serve the files. The staging
// metadata service has no file observer (so nothing is indexed), no health
// repository (so a read problem cannot write health state) and no ARR or
// rclone client.
type FSStager struct{ deps FSStagerDeps }

// NewFSStager creates the real stager.
func NewFSStager(deps FSStagerDeps) *FSStager { return &FSStager{deps: deps} }

type noopTracker struct{}

func (noopTracker) Add(_, _, _, _, _ string, _ int64) string { return "" }
func (noopTracker) UpdateProgress(string, int64)             {}
func (noopTracker) UpdateDownloadProgress(string, int64)     {}
func (noopTracker) UpdateCurrentOffset(string, int64)        {}
func (noopTracker) UpdateBufferedOffset(string, int64)       {}
func (noopTracker) Remove(string)                            {}
func (noopTracker) IncArticlesDownloaded()                   {}
func (noopTracker) IncArticlesPosted()                       {}

type fsStaging struct {
	dir string
	ms  *metadata.MetadataService
	fs  *nzbfilesystem.NzbFilesystem
}

// NewStaging creates a fresh, empty staging area.
func (s *FSStager) NewStaging() (Staging, error) {
	if s.deps.Pool == nil || s.deps.Config == nil {
		return nil, errors.New("bundleimport: staging needs a pool and a configuration")
	}
	root := s.deps.Root
	if root == "" {
		root = filepath.Join(os.TempDir(), "altmount-peernet-staging")
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("bundleimport: create staging root: %w", err)
	}
	dir, err := os.MkdirTemp(root, "stage-")
	if err != nil {
		return nil, fmt.Errorf("bundleimport: create staging area: %w", err)
	}
	ms := metadata.NewMetadataService(dir)
	remote := nzbfilesystem.NewMetadataRemoteFile(ms, nil, nil, nil, s.deps.Pool, s.deps.Config, noopTracker{}, s.deps.Cache)
	return &fsStaging{dir: dir, ms: ms, fs: nzbfilesystem.NewNzbFilesystem(remote)}, nil
}

func (s *fsStaging) WriteFileMetadata(virtualPath string, m *metapb.FileMetadata) error {
	return s.ms.WriteFileMetadata(virtualPath, m)
}

func (s *fsStaging) Opener() contentverify.Opener { return s.fs }

func (s *fsStaging) Close() error { return os.RemoveAll(s.dir) }
