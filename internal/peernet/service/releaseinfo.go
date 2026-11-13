package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/nzbname"
	"github.com/kipsilabs/altmount/internal/peernet/publish"
	"github.com/kipsilabs/altmount/internal/peernet/records"
)

const (
	infoTTL      = 10 * time.Minute
	infoCacheMax = 256
	// maxReleaseFiles bounds how many metadata files are read for one release.
	// A release with more indexed files than this is not described (it would
	// otherwise cost an unbounded number of reads for one record).
	maxReleaseFiles = 500
	// originProbeFiles is how many files of a release are tried when the
	// first has no import bookkeeping.
	originProbeFiles = 5
	// maxMediaMetadataBytes caps the import metadata JSON that is parsed.
	maxMediaMetadataBytes = 64 << 10
)

var errNoReleaseInfo = errors.New("no release information available")

// infoSource builds release-level data for a release record.
//
// Sources, in order of reliability:
//   - total size, file count and post date come from the metadata of every
//     indexed file of the release. Metadata lives as long as the file does, so
//     this works after the import history was cleared or aged out.
//   - title, category, indexer and media IDs come from the import bookkeeping
//     (see database.PeernetOriginRepository). Without it the title falls back
//     to the stored NZB name; category, indexer and media IDs stay unknown,
//     which the publisher treats conservatively while exclusions are set.
type infoSource struct {
	content *database.PeernetContentRepository
	origins *database.PeernetOriginRepository
	meta    ReleaseFileReader
	now     func() time.Time

	mu    sync.Mutex
	cache map[string]infoEntry
}

type infoEntry struct {
	info publish.ReleaseInfo
	at   time.Time
}

func newInfoSource(content *database.PeernetContentRepository, origins *database.PeernetOriginRepository, meta ReleaseFileReader) *infoSource {
	return &infoSource{content: content, origins: origins, meta: meta, now: time.Now, cache: map[string]infoEntry{}}
}

// forRelease describes the release releaseHex. hintPath, when set, is the file
// whose check triggered the call; its bookkeeping is consulted first.
func (s *infoSource) forRelease(ctx context.Context, releaseHex, hintPath string) (publish.ReleaseInfo, error) {
	if info, ok := s.cached(releaseHex); ok {
		return info, nil
	}
	entries, err := s.content.FindByReleaseID(ctx, releaseHex)
	if err != nil {
		return publish.ReleaseInfo{}, fmt.Errorf("list release files: %w", err)
	}
	if len(entries) == 0 || len(entries) > maxReleaseFiles {
		return publish.ReleaseInfo{}, errNoReleaseInfo
	}

	var info publish.ReleaseInfo
	var nzbName, namePassword string
	secrets := map[string]struct{}{}
	paths := make([]string, 0, len(entries))
	for _, e := range entries {
		fm, err := s.meta.ReadReleaseFile(e.FilePath)
		if err != nil || fm == nil {
			// All or nothing: a partial listing would publish an understated
			// file count and size (and cache it).
			return publish.ReleaseInfo{}, errNoReleaseInfo
		}
		paths = append(paths, e.FilePath)
		if pw := strings.TrimSpace(fm.Password); pw != "" {
			secrets[pw] = struct{}{}
		}
		info.FileCount++
		info.TotalSize += fm.FileSize
		if d := time.Unix(fm.ReleaseDate, 0); fm.ReleaseDate > 0 && d.After(info.PostDate) {
			info.PostDate = d.UTC()
		}
		if nzbName == "" && fm.SourceNzbPath != "" {
			nzbName = nzbname.ReleaseName(fm.SourceNzbPath, true)
			namePassword = nzbname.Password(fm.SourceNzbPath)
		}
	}
	if info.FileCount == 0 {
		return publish.ReleaseInfo{}, errNoReleaseInfo
	}

	origin, err := s.origin(ctx, hintPath, paths)
	if err != nil {
		return publish.ReleaseInfo{}, err
	}
	info.Title = origin.Title
	if info.Title == "" {
		// An NZB can be named after its media file; a title has no extension.
		info.Title = nzbname.StripMediaExtension(nzbName)
	}
	if strings.TrimSpace(info.Title) == "" {
		return publish.ReleaseInfo{}, errNoReleaseInfo
	}
	info.Category, info.Indexer = origin.Category, origin.Indexer
	for _, pw := range []string{origin.NamePassword, namePassword} {
		if pw != "" {
			secrets[pw] = struct{}{}
		}
	}
	for pw := range secrets {
		info.Secrets = append(info.Secrets, pw)
	}
	info.Media = mediaFromMetadata(origin.QueueMetadata)

	s.store(releaseHex, info)
	return info, nil
}

// origin returns the first non-empty origin among the hint path and the first
// few files of the release.
func (s *infoSource) origin(ctx context.Context, hint string, paths []string) (database.PeernetOrigin, error) {
	try := make([]string, 0, originProbeFiles+1)
	if hint != "" {
		try = append(try, hint)
	}
	for _, p := range paths {
		if len(try) >= originProbeFiles+1 {
			break
		}
		if p != hint {
			try = append(try, p)
		}
	}
	var best database.PeernetOrigin
	for _, p := range try {
		o, err := s.origins.OriginForPath(ctx, p)
		if err != nil {
			return database.PeernetOrigin{}, fmt.Errorf("read import origin: %w", err)
		}
		if o.Title != "" || o.Category != "" {
			return o, nil
		}
		if best.Indexer == "" {
			best = o
		}
	}
	return best, nil
}

func (s *infoSource) cached(key string) (publish.ReleaseInfo, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.cache[key]
	if !ok || s.now().Sub(e.at) > infoTTL {
		return publish.ReleaseInfo{}, false
	}
	return e.info, true
}

func (s *infoSource) store(key string, info publish.ReleaseInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.cache) >= infoCacheMax {
		clear(s.cache)
	}
	s.cache[key] = infoEntry{info: info, at: s.now()}
}

var imdbRe = regexp.MustCompile(`^tt[0-9]{1,10}$`)

// stremioMetadata mirrors the JSON a Stremio-originated import stores in its
// queue metadata. Only the media identifiers are read.
type stremioMetadata struct {
	IMDbID  string `json:"imdb_id"`
	Type    string `json:"type"`
	Season  int64  `json:"season"`
	Episode int64  `json:"episode"`
	TMDBID  int64  `json:"tmdbId"`
	TVDBID  int64  `json:"tvdbId"`
}

const maxUint32 = 1<<32 - 1

func positiveU32(v int64) uint32 {
	if v <= 0 || v > maxUint32 {
		return 0
	}
	return uint32(v)
}

// mediaFromMetadata extracts media identifiers from import metadata JSON. It
// is defensive: anything unparsable, oversized or implausible yields no
// identifiers (or just the valid ones), never an error. Imports that did not
// originate from Stremio carry none.
func mediaFromMetadata(raw string) records.MediaIDs {
	if raw == "" || len(raw) > maxMediaMetadataBytes {
		return records.MediaIDs{}
	}
	var m stremioMetadata
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return records.MediaIDs{}
	}
	var out records.MediaIDs
	if imdbRe.MatchString(m.IMDbID) {
		out.IMDb = m.IMDbID
	}
	out.TMDB, out.TVDB = positiveU32(m.TMDBID), positiveU32(m.TVDBID)
	if strings.EqualFold(strings.TrimSpace(m.Type), "series") {
		if v := positiveU32(m.Season); v != 0 {
			out.Season = &v
		}
		if v := positiveU32(m.Episode); v != 0 {
			out.Episode = &v
		}
	}
	return out
}
