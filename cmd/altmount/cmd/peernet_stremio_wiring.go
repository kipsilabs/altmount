package cmd

import (
	"context"
	"errors"

	"github.com/kipsilabs/altmount/internal/api"
	"github.com/kipsilabs/altmount/internal/peernet/bundleimport"
	"github.com/kipsilabs/altmount/internal/peernet/service"
)

var errPeerImporterNotReady = errors.New("peer bundle importer is not available")

// peerCandidateService is the part of the peer network service the Stremio
// support needs.
type peerCandidateService interface {
	PeerCandidates(ctx context.Context, imdbID string, season, episode *int) []service.PeerCandidate
	BundleOffers(ctx context.Context, releaseID string) ([]service.Offer, error)
}

// importerProvider returns the bundle importer (nil until the feature was
// enabled); *peernetBundleHost has this shape.
type importerProvider interface {
	Importer() *bundleimport.Importer
}

// peerDirs says where a peer import of a category and title is written.
type peerDirs interface {
	PeerImportDir(category, title string) string
}

// stremioPeerPlayback is the Stremio addon's view of the peer network: the
// candidates for a media key, whether a release can be fetched at all, and the
// import of one release into the folder a normal import of its category uses.
type stremioPeerPlayback struct {
	svc  peerCandidateService
	host importerProvider
	dirs peerDirs
}

func (p stremioPeerPlayback) Candidates(ctx context.Context, imdbID string, season, episode *int) []service.PeerCandidate {
	return p.svc.PeerCandidates(ctx, imdbID, season, episode)
}

// HasOffer reports whether a stored signed release record offers a bundle of
// the release from a peer this node can dial. It reads the local store only.
func (p stremioPeerPlayback) HasOffer(ctx context.Context, releaseID string) bool {
	offers, err := p.svc.BundleOffers(ctx, releaseID)
	if err != nil {
		return false
	}
	for _, o := range offers {
		if o.Fetchable && !o.Own {
			return true
		}
	}
	return false
}

func (p stremioPeerPlayback) Play(ctx context.Context, req api.PeerPlayRequest) (api.PeerPlayResult, error) {
	imp := p.host.Importer()
	if imp == nil {
		return api.PeerPlayResult{}, errPeerImporterNotReady
	}
	res, err := imp.ImportRelease(ctx, req.ReleaseID, bundleimport.Target{
		Category: req.Category, Metadata: req.Metadata, DownloadID: req.DownloadID,
		DirFunc: func(title string) string { return p.dirs.PeerImportDir(req.Category, title) },
	})
	if err != nil {
		return api.PeerPlayResult{}, err
	}
	return api.PeerPlayResult{Paths: res.Paths, Sizes: res.Sizes}, nil
}
