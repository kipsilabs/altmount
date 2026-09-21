package parser

import (
	"context"
	"encoding/base64"
	stderrors "errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/encryption"
	"github.com/kipsilabs/altmount/internal/encryption/rclone"
	"github.com/kipsilabs/altmount/internal/errors"
	"github.com/kipsilabs/altmount/internal/holes"
	"github.com/kipsilabs/altmount/internal/importer/parser/fileinfo"
	"github.com/kipsilabs/altmount/internal/importer/parser/par2"
	"github.com/kipsilabs/altmount/internal/importer/rarname"
	"github.com/kipsilabs/altmount/internal/importer/utils/nzbtrim"
	"github.com/kipsilabs/altmount/internal/metadata"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/pool"
	"github.com/kipsilabs/altmount/internal/usenet"
	"github.com/kipsilabs/altmount/internal/progress"
	"github.com/kipsilabs/altmount/internal/slogutil"
	"github.com/javi11/nntppool/v5"
	"github.com/javi11/nzbparser"
	concpool "github.com/sourcegraph/conc/pool"
)

// maxFetchGoroutines bounds how many fetch goroutines a single parse phase
// spawns. It is a memory/scheduler bound only — the actual number of in-flight
// NNTP body fetches is governed by the pool manager's global import connection
// budget, which adapts to pool capacity and stream activity.
const maxFetchGoroutines = 100

// First-segment fetches retry transient failures (connection exhaustion,
// timeouts) so a momentary hiccup doesn't permanently drop a volume from a
// large multi-volume set. A real article-not-found is not retried.
const (
	maxFirstSegmentFetchAttempts = 3
	firstSegmentRetryBackoff     = 300 * time.Millisecond
)

// headerProbeAttempts bounds how many leading articles are tried for a file's
// yEnc header when the first one is gone. Every article of a multipart post
// repeats =ybegin name/size and states its own =ypart offset, so a dead first
// article need not drop the file; the layout is recoverable from the next one.
const headerProbeAttempts = 4

// FirstSegmentData holds cached data from the first segment of an NZB file
// This avoids redundant fetching when both PAR2 extraction and file parsing need the same data
type FirstSegmentData struct {
	File                *nzbparser.NzbFile // Reference to the NZB file (for groups, subject, metadata)
	Headers             nntppool.YEncMeta  // yEnc headers (FileName, FileSize, PartSize)
	RawBytes            []byte             // Up to 16KB of raw data for PAR2 detection (may be less if segment is smaller)
	MissingFirstSegment bool               // True if first segment download failed (article not found, etc.)
	IsArticleNotFound   bool               // True only when 430 Not Found (permanent); false for timeouts/transient
	SkippedFirstSegment bool               // True when the fetch was intentionally skipped (clean-named multipart file); Headers/RawBytes are empty by design, not by failure
	OriginalIndex       int                // Original position in the parsed NZB file list
	// FirstArticleMissingID is set when the first article is gone but the yEnc
	// header was recovered from a later one. Headers are valid (PartSize is the
	// derived first-part size); RawBytes is empty, so magic-byte and PAR2
	// Hash16k matching are unavailable for this file.
	FirstArticleMissingID string
}

// SegmentStore is where decoded articles are kept for the streaming readers:
// warm-up writes the articles it fetches, and the PAR2 index read serves from
// it. Mirrors usenet.SegmentStore.
type SegmentStore interface {
	Get(messageID string) ([]byte, bool)
	Put(messageID string, data []byte) error
}

// resolvedStore is the current segment store, or nil when caching is off.
func (p *Parser) resolvedStore() usenet.SegmentStore {
	if p.segmentStore == nil {
		return nil
	}
	if store := p.segmentStore(); store != nil {
		return store
	}
	return nil
}

// Parser handles NZB file parsing
type Parser struct {
	poolManager pool.Manager        // Pool manager for dynamic pool access
	getConfig   config.ConfigGetter // Returns current config for connection limits
	log         *slog.Logger        // Logger for debug/error messages
	heads       *headCache          // what earlier parses learned from the wire
	// segmentStore resolves the streaming segment store at fetch time (its
	// capacity and tiers follow config), or nil when caching is off.
	segmentStore func() SegmentStore
}

// articleDateOf returns when an NZB file's articles were posted, for the pool's
// per-provider retention limits. A missing or non-positive date in the NZB
// yields the zero time, which applies no retention policy — better than
// guessing an age from a header the poster may never have set.
func articleDateOf(f *nzbparser.NzbFile) time.Time {
	if f == nil || f.Date <= 0 {
		return time.Time{}
	}
	return time.Unix(int64(f.Date), 0)
}

// warmReleaseDate returns one article date for a batch that spans several
// files: the oldest of them. Files in a release are posted within minutes of
// each other, so the choice rarely matters; when it does, the oldest is the
// conservative one — a provider whose retention stops short of it is demoted
// for the batch rather than being handed ids it may no longer hold.
func warmReleaseDate(files []nzbparser.NzbFile) time.Time {
	var oldest time.Time
	for i := range files {
		at := articleDateOf(&files[i])
		if at.IsZero() {
			continue
		}
		if oldest.IsZero() || at.Before(oldest) {
			oldest = at
		}
	}
	return oldest
}

// SetSegmentStore lets first articles fetched at import be published to the
// streaming segment store, so the cold open that follows is a cache hit.
func (p *Parser) SetSegmentStore(resolve func() SegmentStore) {
	p.segmentStore = resolve
}

// Use conc pool for parallel processing with proper error handling
type fileResult struct {
	parsedFile *ParsedFile
	err        error
}

// NewParser creates a new NZB parser
func NewParser(poolManager pool.Manager, getConfig config.ConfigGetter) *Parser {
	return &Parser{
		poolManager: poolManager,
		getConfig:   getConfig,
		log:         slog.Default().With("component", "nzb-parser"),
		heads:       newHeadCache(defaultHeadCacheBytes, defaultHeadCacheTTL),
	}
}

// ParseFile parses an NZB file from a reader.
// progressTracker, if non-nil, receives incremental updates as first segments are fetched (the
// longest phase). It is safe to pass nil — updates are skipped.
func (p *Parser) ParseFile(ctx context.Context, r io.Reader, nzbPath string, progressTracker progress.ProgressTracker) (*ParsedNzb, error) {
	n, err := nzbparser.Parse(r)
	if err != nil {
		return nil, errors.NewNonRetryableError("failed to parse NZB XML", err)
	}

	if len(n.Files) == 0 {
		return nil, errors.NewNonRetryableError("NZB file contains no files", nil)
	}

	SanitizeNzbFilenames(n)

	return p.ParseNzb(ctx, n, nzbPath, progressTracker, ParseOptions{})
}

// SanitizeNzbFilenames normalizes poster-controlled filenames in place at the
// nzbparser boundary so every consumer (the pre-parse fast-fail probe, the parser,
// persisted metadata, and serve-time volume following) sees a canonical name. Call
// it immediately after nzbparser.Parse and before any code reads NzbFile.Filename.
// The raw subject remains available on NzbFile.Subject.
func SanitizeNzbFilenames(n *nzbparser.Nzb) {
	if n == nil {
		return
	}
	for i := range n.Files {
		n.Files[i].Filename = nzbtrim.TrimSurroundingQuotes(n.Files[i].Filename)
	}
}

// ParseNzb processes an already-parsed *nzbparser.Nzb, performing all network
// operations (first-segment fetches, PAR2 extraction, yEnc normalisation).
// opts carries knowledge collected before the network phase, e.g. file indexes
// whose segments are already known to be missing from a pre-parse Stat check.
func (p *Parser) ParseNzb(ctx context.Context, n *nzbparser.Nzb, nzbPath string, progressTracker progress.ProgressTracker, opts ParseOptions) (*ParsedNzb, error) {
	ctx = slogutil.With(ctx, "nzb_path", nzbPath)

	// Safety timeout for the entire network phase.
	// Parsing large NZBs with many missing articles can sometimes hang in NNTP body fetching.
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	parsed := &ParsedNzb{
		Path:     nzbPath,
		Filename: filepath.Base(nzbPath),
		Files:    make([]ParsedFile, 0, len(n.Files)),
	}

	// Build the shared NzbStore and segment index for v3 format.
	// Must be built from the raw *nzbparser.Nzb BEFORE any per-file processing
	// (which may filter/reorder files). The index maps message-id → flat store index.
	parsed.Store, parsed.SegmentIndex = metadata.BuildStore(n)

	// Extract password from metadata, filename tags, subject lines, or raw NZB meta
	if pwd := extractNzbPassword(n, nzbPath); pwd != "" {
		parsed.SetPassword(pwd)
	}

	// Fetch first segment data for all files in parallel
	// This cache is used by both PAR2 extraction and file parsing to avoid redundant fetches
	firstSegmentCache, notFoundIDs, err := p.fetchAllFirstSegments(ctx, n.Files, progressTracker, opts)
	if err != nil {
		return nil, err
	}

	// PAR2 descriptor matching is only worth network I/O when (a) the NZB actually
	// contains a PAR2 index AND (b) at least one fetched file has an untrustworthy
	// name that the descriptors could recover — either an individually obfuscated name
	// or a member of a .partNN.rar set whose volumes all have distinct (obfuscated) bases
	// (hasObfuscatedVolumeSet). When every name is already clean, downloading the PAR2
	// index and completing files to 16KB would only confirm what we already trust — skip
	// both entirely.
	par2MatchingUseful := p.hasPar2IndexCandidate(firstSegmentCache) &&
		(anyFileNeedsPar2Matching(firstSegmentCache) || hasObfuscatedVolumeSet(firstSegmentCache))
	if par2MatchingUseful {
		p.complete16KBReads(ctx, firstSegmentCache, notFoundIDs)
	}

	// Create a map of first segment ID to yEnc info for optimization in normalizeSegmentSizesWithYenc.
	// PartSize avoids re-fetching the first segment's headers; FileSize (total decoded file
	// size from "=ybegin size=") lets normalization derive the LAST part's size arithmetically,
	// eliminating the per-file last-segment fetch that would otherwise drain a full body.
	firstSegmentSizeCache := make(map[string]firstSegmentYencInfo)
	// warmFirstSegmentBytes carries each fetched file's decoded first-segment bytes
	// (keyed by first-segment ID) into the archive analysis phase, so a volume's
	// header read — which starts at offset 0 — is served from memory instead of
	// re-fetching a segment already pulled over the wire here. Skipped/missing first
	// segments contribute nothing; those volumes are read lazily by the analyzer.
	warmFirstSegmentBytes := make(map[string][]byte)
	for _, data := range firstSegmentCache {
		if data != nil && data.File != nil && !data.MissingFirstSegment && len(data.File.Segments) > 0 {
			if data.Headers.PartSize > 0 {
				firstSegmentSizeCache[data.File.Segments[0].ID] = firstSegmentYencInfo{
					PartSize: data.Headers.PartSize,
					FileSize: data.Headers.FileSize,
				}
			}
			if !data.SkippedFirstSegment && len(data.RawBytes) > 0 {
				warmFirstSegmentBytes[data.File.Segments[0].ID] = data.RawBytes
			}
		}
	}

	// Extract PAR2 file descriptors before processing files
	// This provides accurate filename and size information via MD5 hash matching
	// Convert firstSegmentCache to par2.FirstSegmentData format
	// Skip files with missing first segments as they cannot be matched
	par2Cache := make([]*par2.FirstSegmentData, 0, len(firstSegmentCache))
	for _, data := range firstSegmentCache {
		if data == nil || data.File == nil || data.MissingFirstSegment || data.FirstArticleMissingID != "" {
			continue
		}
		par2Cache = append(par2Cache, &par2.FirstSegmentData{
			File:     data.File,
			RawBytes: data.RawBytes,
		})
	}

	// Run PAR2 descriptor extraction in parallel with a one-shot representative
	// yEnc-header fetch for a middle segment. The representative PartSize is
	// reused as the "standard part size" during per-file normalization, cutting
	// one network call per multi-segment file.
	var (
		par2Descriptors     map[[16]byte]*par2.FileDescriptor
		par2Err             error
		nzbStandardPartSize int64
	)

	repSeg, repGroups, haveRep := pickRepresentativeMiddleSegment(firstSegmentCache, notFoundIDs)

	g, gctx := errgroup.WithContext(ctx)
	// Only download the PAR2 index when descriptor matching can change an outcome
	// (see par2MatchingUseful above). A nil descriptor map is handled downstream.
	if par2MatchingUseful {
		g.Go(func() error {
			key := descriptorKey(par2Cache)
			if cached, ok := p.heads.getDescriptors(key); ok {
				par2Descriptors = cached
				return nil
			}
			par2Descriptors, par2Err = par2.GetFileDescriptors(gctx, par2Cache, p.poolManager, p.resolvedStore())
			if par2Err == nil {
				p.heads.putDescriptors(key, par2Descriptors)
			}
			return nil
		})
	}
	if haveRep && p.poolManager != nil && p.poolManager.HasPool() {
		g.Go(func() error {
			h, err := p.fetchYencHeaders(gctx, repSeg, repGroups, warmReleaseDate(n.Files))
			if err != nil {
				p.log.DebugContext(gctx, "Representative yEnc header fetch failed, falling back to per-file normalization", "error", err)
				return nil
			}
			if h.PartSize > 0 {
				nzbStandardPartSize = int64(h.PartSize)
			}
			return nil
		})
	}
	_ = g.Wait()

	if par2Err != nil {
		if stderrors.Is(par2Err, context.Canceled) {
			return nil, errors.NewNonRetryableError("extracting PAR2 file descriptors canceled", par2Err)
		}
		p.log.WarnContext(ctx, "Failed to extract PAR2 file descriptors", "error", par2Err)
	}

	// For files whose first segment we intentionally skipped, fill in the first-segment
	// decoded size from the NZB-wide representative middle-segment PartSize. In a uniform
	// multipart yEnc post the first part equals the middle part size, so this is exact.
	// If no representative is available (e.g. the representative fetch failed),
	// normalizeSegmentSizesWithYenc falls back to fetching the first segment header,
	// which preserves correctness at the cost of the saved fetch.
	if nzbStandardPartSize > 0 {
		for _, data := range firstSegmentCache {
			if data != nil && data.SkippedFirstSegment && data.File != nil && len(data.File.Segments) > 0 {
				// Skipped files have no yEnc headers. The poster's subject line
				// often states the exact size, which lets normalization derive
				// the last part instead of fetching it; when it doesn't, FileSize
				// stays 0 and the last segment is fetched as before.
				firstSegmentSizeCache[data.File.Segments[0].ID] = firstSegmentYencInfo{
					PartSize: nzbStandardPartSize,
					FileSize: declaredFileSize(data.File),
				}
			}
		}
	}

	// Extract file information using priority-based filename selection
	// Convert firstSegmentCache to fileinfo format
	// Skip files with missing first segments as they cannot be processed
	filesWithFirstSegment := make([]*fileinfo.NzbFileWithFirstSegment, 0, len(firstSegmentCache))
	for _, data := range firstSegmentCache {
		// Skip files with missing first segment data
		// These files can't be properly processed (no PAR2 matching, no yEnc size data, no magic bytes)
		if data == nil || data.File == nil || data.MissingFirstSegment {
			continue
		}

		subjectHeader := ""
		if s, err := nzbparser.ParseSubject(data.File.Subject); err == nil {
			subjectHeader = nzbtrim.TrimSurroundingQuotes(s.Header)
		}

		filesWithFirstSegment = append(filesWithFirstSegment, &fileinfo.NzbFileWithFirstSegment{
			NzbFile:       data.File,
			Headers:       &data.Headers,
			First16KB:     data.RawBytes,
			ReleaseDate:   time.Unix(int64(data.File.Date), 0),
			SubjectHeader: subjectHeader,
			OriginalIndex: data.OriginalIndex,
		})
	}

	// Get file infos with priority-based filename selection
	// GetFileInfos processes ALL files including PAR2 files; SeparateFiles handles the split
	fileInfos := fileinfo.GetFileInfos(filesWithFirstSegment, par2Descriptors, parsed.Filename)
	if len(fileInfos) == 0 {
		p.log.WarnContext(ctx, "Failed to get file infos from network, falling back to NZB XML data",
			"nzb_path", nzbPath)
		fileInfos = p.fallbackGetFileInfos(n.Files)
	}

	if len(fileInfos) == 0 {
		return nil, errors.NewNonRetryableError("NZB file contains no valid files. This can be caused because the file has missing segments in your providers.", nil)
	}

	maxParse := max(min(len(fileInfos), 20), 1)
	concPool := concpool.NewWithResults[fileResult]().WithMaxGoroutines(maxParse).WithContext(ctx)

	// Process files in parallel using conc pool
	for _, info := range fileInfos {
		concPool.Go(func(ctx context.Context) (fileResult, error) {
			parsedFile, err := p.parseFile(ctx, n.Meta, parsed.Filename, info, firstSegmentSizeCache, warmFirstSegmentBytes, nzbStandardPartSize, notFoundIDs, parsed.SegmentIndex)

			return fileResult{
				parsedFile: parsedFile,
				err:        err,
			}, nil
		})
	}

	// Wait for all goroutines to complete and collect results
	results, err := concPool.Wait()
	if err != nil {
		if stderrors.Is(err, context.Canceled) {
			return nil, errors.NewNonRetryableError("parsing canceled", err)
		}

		return nil, errors.NewNonRetryableError("failed to get file infos", err)
	}

	// Check for errors and collect valid results
	var parsedFiles []*ParsedFile
	for _, result := range results {
		if result.err != nil {
			slog.InfoContext(ctx, "Failed to parse file", "error", result.err)
			continue
		}
		parsedFiles = append(parsedFiles, result.parsedFile)
	}

	// Check if all files are PAR2 files - indicates missing segments
	if len(parsedFiles) > 0 {
		allPar2 := true
		for _, pf := range parsedFiles {
			if !pf.IsPar2Archive {
				allPar2 = false
				break
			}
		}

		if allPar2 {
			return nil, errors.NewNonRetryableError("NZB file contains only PAR2 files. This indicates that there are missing segments in your providers.", nil)
		}
	}

	// Aggregate results in the original order
	// Note: OriginalIndex is already set from the original n.Files order during parsing
	deadFirstArticle := make(map[int]string)
	for _, data := range firstSegmentCache {
		if data != nil && data.FirstArticleMissingID != "" {
			deadFirstArticle[data.OriginalIndex] = data.FirstArticleMissingID
		}
	}
	for _, parsedFile := range parsedFiles {
		parsed.Files = append(parsed.Files, *parsedFile)
		parsed.TotalSize += parsedFile.Size
		parsed.SegmentsCount += len(parsedFile.Segments)
		if segID, ok := deadFirstArticle[parsedFile.OriginalIndex]; ok {
			if parsed.DegradedFiles == nil {
				parsed.DegradedFiles = make(map[string]string)
			}
			parsed.DegradedFiles[filepath.Base(parsedFile.Filename)] = segID
		}
	}

	// Determine NZB type based on content analysis
	parsed.Type = p.determineNzbType(parsed.Files)

	// Propagate archive type to confirmed archive parts only.
	// For split archives only the first volume contains the magic-byte header, so
	// Is7zArchive / IsRarArchive may be false on subsequent parts even though they
	// are archive parts. Correct that now that we know the NZB type.
	// Propagation is gated on existing detection (magic bytes or extension) so that
	// non-archive sidecars (.txt, .nfo, etc.) are never wrongly classified.
	p.propagateArchiveType(parsed)

	return parsed, nil
}

// parseFile processes a single file entry from the NZB
// Uses fileInfo for filename, size, and type information
// firstSegmentSizeCache contains pre-fetched yEnc info (PartSize + total FileSize) for first segments to avoid redundant fetching.
// nzbStandardPartSize, when >0, is the yEnc PartSize of a representative middle segment in the NZB;
// it lets normalization skip the per-file second-segment fetch.
func (p *Parser) parseFile(ctx context.Context, meta map[string]string, nzbFilename string, info *fileinfo.FileInfo, firstSegmentSizeCache map[string]firstSegmentYencInfo, warmFirstSegmentBytes map[string][]byte, nzbStandardPartSize int64, notFoundIDs map[string]struct{}, segmentIndex map[string]int64) (*ParsedFile, error) {
	if len(info.NzbFile.Segments) == 0 {
		return nil, fmt.Errorf("file has no segments")
	}

	sort.Sort(info.NzbFile.Segments)

	// Normalize segment sizes using yEnc PartSize headers if needed
	// This handles cases where NZB segment sizes include yEnc encoding overhead
	if p.poolManager != nil && p.poolManager.HasPool() {
		// Look up cached first segment yEnc info to avoid redundant fetching
		// Safe to access Segments[0] since files without segments are filtered earlier
		cachedFirstSegment := firstSegmentSizeCache[info.NzbFile.Segments[0].ID]

		err := p.normalizeSegmentSizesWithYenc(ctx, info.NzbFile.Segments, cachedFirstSegment, nzbStandardPartSize, notFoundIDs, articleDateOf(&info.NzbFile))
		if err != nil {
			if stderrors.Is(err, nntppool.ErrArticleNotFound) {
				// A segment required to determine the real (decoded) sizes is missing
				// from every provider. Importing the file with the NZB's un-normalized
				// (yEnc-encoded) byte counts would compute wrong segment offsets and
				// produce a corrupt media file (#681), so skip the whole file. The
				// caller's aggregation loop logs and continues, so the rest of the
				// release still imports normally.
				return nil, fmt.Errorf("failed to normalize segment sizes for %q: %w", info.Filename, err)
			}
			// Any other normalization failure (e.g. a present but non-yEnc article that
			// yields no part size) is non-fatal: the NZB-declared segment sizes remain
			// the best available source. Log and continue with them, as before.
			p.log.WarnContext(ctx, "Failed to normalize segment sizes with yEnc headers",
				"error", err,
				"segments", len(info.NzbFile.Segments))
		}
	}

	// Convert segments
	segments := make([]*metapb.SegmentData, len(info.NzbFile.Segments))

	for i, seg := range info.NzbFile.Segments {
		segments[i] = &metapb.SegmentData{
			Id:          seg.ID,
			StartOffset: int64(0),
			EndOffset:   int64(seg.Bytes - 1),
			SegmentSize: int64(seg.Bytes),
		}
	}

	// Also build SegmentRefs for v3 store-based format
	var segmentRefs []*metapb.SegmentRef
	if segmentIndex != nil {
		segmentRefs = make([]*metapb.SegmentRef, len(info.NzbFile.Segments))
		for i, seg := range info.NzbFile.Segments {
			segmentRefs[i] = &metapb.SegmentRef{
				StoreIndex:  segmentIndex[seg.ID],
				StartOffset: 0,
				EndOffset:   int64(seg.Bytes - 1),
			}
		}
	}

	// Get file size from fileInfo (priority-based: PAR2 > yEnc headers)
	var totalSize int64

	if info.FileSize != nil {
		totalSize = *info.FileSize
	}

	// Sanity check: Ensure totalSize is at least the sum of its segments.
	// This prevents "seek beyond file size" errors when yEnc headers report incorrect sizes.
	var segmentSum int64
	for _, seg := range info.NzbFile.Segments {
		segmentSum += int64(seg.Bytes)
	}

	if totalSize < segmentSum {
		totalSize = segmentSum
	}

	// Usenet Drive files parsing
	var (
		password string
		salt     string
	)
	if meta != nil {
		if pwd, ok := meta["password"]; ok && pwd != "" {
			password = pwd
		}
		if s, ok := meta["salt"]; ok && s != "" {
			salt = s
		}
	}

	// Use filename from fileInfo (priority-based: PAR2 > Subject > yEnc headers)
	filename := info.Filename
	enc := metapb.Encryption_NONE // Default to no encryption
	var nzbdavID string
	var aesKey []byte
	var aesIv []byte

	// Extract extra metadata from subject if present (nzbdav compatibility)
	if strings.HasPrefix(info.NzbFile.Subject, "NZBDAV_ID:") {
		parts := strings.SplitSeq(info.NzbFile.Subject, " ")
		for part := range parts {
			if after, ok := strings.CutPrefix(part, "NZBDAV_ID:"); ok {
				nzbdavID = after
			} else if after, ok := strings.CutPrefix(part, "AES_KEY:"); ok {
				keyStr := after
				if key, err := base64.StdEncoding.DecodeString(keyStr); err == nil {
					aesKey = key
					enc = metapb.Encryption_AES
				}
			} else if after, ok := strings.CutPrefix(part, "AES_IV:"); ok {
				ivStr := after
				if iv, err := base64.StdEncoding.DecodeString(ivStr); err == nil {
					aesIv = iv
				}
			} else if after, ok := strings.CutPrefix(part, "DECODED_SIZE:"); ok {
				if size, err := strconv.ParseInt(after, 10, 64); err == nil && size > 0 {
					totalSize = size
				}
			}
		}
	}

	// Check metadata for overrides
	if meta != nil {
		if metaFilename, ok := meta["file_name"]; ok && metaFilename != "" {
			if fSize, ok := meta["file_size"]; ok {
				// This is a usenet-drive nzb with one file
				metaFilename = nzbtrim.TrimNzbExtension(nzbFilename)

				if fe, ok := meta["file_extension"]; ok {
					metaFilename = metaFilename + fe
				} else {
					fileExt := filepath.Ext(metaFilename)
					if fileExt == "" {
						if fe, ok := meta["file_extension"]; ok {
							metaFilename = metaFilename + fe
						}
					}
				}

				fSizeInt, err := strconv.ParseInt(fSize, 10, 64)
				if err != nil {
					return nil, errors.NewNonRetryableError("failed to parse file size", err)
				}

				totalSize = fSizeInt
			}

			// This will add support for rclone encrypted files
			if strings.HasSuffix(strings.ToLower(metaFilename), rclone.EncFileExtension) {
				filename = metaFilename[:len(metaFilename)-4]
				enc = metapb.Encryption_RCLONE

				decSize, err := rclone.DecryptedSize(totalSize)
				if err != nil {
					return nil, errors.NewNonRetryableError("failed to get decrypted size", err)
				}

				totalSize = decSize
			} else {
				filename = metaFilename
			}
		}

		if metaCipher, ok := meta["cipher"]; ok && metaCipher != "" {
			if metaCipher == string(encryption.RCloneCipherType) {
				enc = metapb.Encryption_RCLONE
			}
		}
	}

	// Use RAR/7z detection from fileInfo (includes magic byte detection)
	parsedFile := &ParsedFile{
		Subject:       info.NzbFile.Subject,
		Filename:      filename,
		Size:          totalSize,
		Segments:      segments,
		SegmentRefs:   segmentRefs,
		Groups:        info.NzbFile.Groups,
		IsRarArchive:  info.IsRar,
		Is7zArchive:   info.Is7z,
		Encryption:    enc,
		Password:      password,
		Salt:          salt,
		AesKey:        aesKey,
		AesIv:         aesIv,
		ReleaseDate:   info.ReleaseDate,
		IsPar2Archive: info.IsPar2Archive,
		OriginalIndex: info.OriginalIndex,
		NzbdavID:      nzbdavID,
	}

	// Attach the warm first-segment bytes (decoded leading payload at offset 0)
	// so the archive analysis phase can serve this file's header read from memory.
	// Keyed by the same first-segment ID domain as firstSegmentSizeCache.
	if len(info.NzbFile.Segments) > 0 {
		if b, ok := warmFirstSegmentBytes[info.NzbFile.Segments[0].ID]; ok {
			parsedFile.FirstSegmentBytes = b
		}
	}

	return parsedFile, nil
}

// skipEligibleVideoExtensions is a deliberately narrow set of unambiguous video
// container extensions whose type can be trusted from the name alone. It excludes
// ambiguous extensions that IsVideoFile accepts (.bin, .dat, .img, .iso, .ifo, .nsv, …),
// since those can be archives or disc images that need magic-byte inspection.
var skipEligibleVideoExtensions = map[string]struct{}{
	".mkv": {}, ".mp4": {}, ".avi": {}, ".m4v": {}, ".mov": {}, ".wmv": {},
	".mpg": {}, ".mpeg": {}, ".ts": {}, ".m2ts": {}, ".webm": {}, ".flv": {},
	".vob": {}, ".mk3d": {}, ".m2v": {}, ".divx": {}, ".ogv": {}, ".rmvb": {},
}

// shouldSkipFirstSegmentFetch reports whether a file's first segment can be left
// undownloaded because its NZB subject filename is already trustworthy. This is the
// SABnzbd-style deobfuscation gate (see fileinfo.IsProbablyObfuscated), applied as a
// pre-fetch decision rather than a post-fetch naming tie-breaker.
//
// We only skip multipart files with at least 3 segments: such a file is itself a valid
// source for the NZB-wide representative middle-segment PartSize, so a representative is
// guaranteed to exist and the skipped first segment's decoded size can be filled in
// exactly (the first part of a uniform multipart yEnc post equals the middle part size).
//
// We only skip files we can confidently identify from their name alone: a file with a
// recognized video extension and a non-obfuscated name. Restricting to known video
// extensions (rather than any valid-length extension) is deliberate — it excludes the
// cases that genuinely need their first segment for magic-byte detection: PAR2 files,
// RAR/7z archives, and ambiguous split-volume names like "Movie.2020.001" or
// "Movie.mkv.001" whose archive nature is only visible in the bytes. Those keep their
// fetch. Video files are also where the bandwidth savings matter most.
func shouldSkipFirstSegmentFetch(file *nzbparser.NzbFile) bool {
	if file == nil || len(file.Segments) < 3 {
		return false
	}

	name := file.Filename
	if name == "" {
		return false
	}

	// Only an unambiguous video container extension is trusted from the name
	// alone — or a 7z continuation volume: 7z analysis reads the first volume's
	// head and the last volume's tail, so the leading bytes of every other
	// volume are never looked at and buy nothing.
	if _, ok := skipEligibleVideoExtensions[strings.ToLower(filepath.Ext(name))]; !ok &&
		!isSevenZipContinuationVolume(name) && !isPar2RecoveryVolume(file) {
		return false
	}

	// Don't trust an obfuscated name even with a video extension.
	if fileinfo.IsProbablyObfuscated(name) {
		return false
	}

	// Uniform-first-part guard: the skip is only safe when the first segment is a full
	// part the same size as the other non-last parts. Verify locally using the NZB's own
	// encoded byte counts — no network needed.
	return firstSegmentEncodedSizeUniform(file.Segments)
}

// isPar2RecoveryVolume reports a .par2 file too large to be an index: only
// index files (≤ par2.MaxIndexSegments) are ever read for descriptors, so a
// recovery volume's leading bytes are never looked at.
func isPar2RecoveryVolume(file *nzbparser.NzbFile) bool {
	return strings.HasSuffix(strings.ToLower(file.Filename), ".par2") && len(file.Segments) > par2.MaxIndexSegments
}

// sevenZipContinuationPattern matches split 7z volumes after the first
// (.7z.002, .7z.003, …); the index is checked separately so .7z.001 and .7z stay
// excluded.
var sevenZipContinuationPattern = regexp.MustCompile(`(?i)\.7z\.(\d{3,})$`)

func isSevenZipContinuationVolume(name string) bool {
	m := sevenZipContinuationPattern.FindStringSubmatch(name)
	if m == nil {
		return false
	}
	idx, err := strconv.Atoi(m[1])
	return err == nil && idx > 1
}

// firstSegmentEncodedSizeUniform reports whether the first segment's NZB-reported
// (encoded) size matches the other full segments, implying its decoded size equals the
// standard middle-part size. yEnc encoding overhead is uniform per input byte, so equal
// encoded sizes imply equal decoded sizes. The last segment is the remainder and is
// excluded from the comparison.
func firstSegmentEncodedSizeUniform(segments nzbparser.NzbSegments) bool {
	n := len(segments)
	if n < 3 {
		return false
	}

	first := segments[0].Bytes
	if first <= 0 {
		return false
	}

	mids := make([]int, 0, n-2)
	for i := 1; i < n-1; i++ {
		if segments[i].Bytes > 0 {
			mids = append(mids, segments[i].Bytes)
		}
	}
	if len(mids) == 0 {
		return false
	}
	sort.Ints(mids)
	median := mids[len(mids)/2]
	if median <= 0 {
		return false
	}

	diff := first - median
	if diff < 0 {
		diff = -diff
	}
	// Allow 1% tolerance for minor per-part yEnc overhead variation.
	return float64(diff) <= 0.01*float64(median)
}

// fetchAllFirstSegments fetches the first segment data for all files in parallel.
// Returns a slice of FirstSegmentData, a set of segment IDs that returned 430 Not Found
// (permanent — safe to skip in subsequent fetches), and any fatal error.
// opts.BrokenFileIndexes short-circuits Body calls for known-broken file indexes.
// opts.KnownMissingSegmentIDs pre-seeds notFoundIDs to skip redundant network calls.
func (p *Parser) fetchAllFirstSegments(ctx context.Context, files []nzbparser.NzbFile, progressTracker progress.ProgressTracker, opts ParseOptions) ([]*FirstSegmentData, map[string]struct{}, error) {
	cache := make([]*FirstSegmentData, 0, len(files))
	notFoundIDs := make(map[string]struct{})

	// Seed notFoundIDs with IDs already known to be missing from a pre-parse Stat check.
	for id := range opts.KnownMissingSegmentIDs {
		notFoundIDs[id] = struct{}{}
	}

	// Return empty cache if no pool manager available
	if p.poolManager == nil || !p.poolManager.HasPool() {
		return cache, notFoundIDs, nil
	}

	cp, err := p.poolManager.GetPool()
	if err != nil {
		p.log.DebugContext(context.Background(), "Failed to get connection pool for first segment fetching", "error", err)
		return cache, notFoundIDs, nil
	}

	// Use conc pool for parallel fetching — I/O-bound, so use more than NumCPU
	type fetchResult struct {
		segmentID  string
		isNotFound bool // true when 430 Not Found (permanent)
		data       *FirstSegmentData
		err        error
		// missingIDs are articles found dead while probing past a missing first
		// article; recorded even though the file itself survived.
		missingIDs []string
	}

	// Goroutine bound only — the real fetch bound is the pool manager's global
	// import connection budget, acquired per Body call below.
	maxFetch := max(min(min(len(files), p.getConfig().TotalProviderConnections()), maxFetchGoroutines), 1)
	concPool := concpool.NewWithResults[fetchResult]().WithMaxGoroutines(maxFetch).WithContext(ctx)

	// Atomic counter for progress tracking — incremented by each goroutine on completion
	var doneCount atomic.Int64
	totalFiles := len(files)

	// Fetch first segment of each file in parallel
	for idx, file := range files {
		// Capture the index and file for the goroutine
		// Use &file to heap-allocate the copy, preventing use-after-free
		// when the goroutine accesses it after the loop iteration ends
		originalIndex := idx
		fileToFetch := &file

		concPool.Go(func(ctx context.Context) (fetchResult, error) {
			defer func() {
				if progressTracker != nil {
					progressTracker.Update(int(doneCount.Add(1)), totalFiles)
				}
			}()
			ctx = slogutil.With(ctx, "file", fileToFetch.Filename)

			// Skip files without segments
			if len(fileToFetch.Segments) == 0 {
				return fetchResult{
					segmentID: fileToFetch.Subject,
					data: &FirstSegmentData{
						File:                fileToFetch,
						MissingFirstSegment: true,
						OriginalIndex:       originalIndex,
					},
					err: fmt.Errorf("file has no segments"),
				}, nil
			}

			// Short-circuit files flagged as broken by a pre-parse Stat check —
			// mark missing without a Body call, seeding notFoundIDs via the result.
			if _, broken := opts.BrokenFileIndexes[originalIndex]; broken {
				return fetchResult{
					segmentID:  fileToFetch.Segments[0].ID,
					isNotFound: true,
					data: &FirstSegmentData{
						File:                fileToFetch,
						MissingFirstSegment: true,
						IsArticleNotFound:   true,
						OriginalIndex:       originalIndex,
					},
				}, nil
			}

			// Deobfuscation gate: when the subject filename is trustworthy and the file
			// is a uniform multipart file, skip the body download entirely. The decoded
			// first-segment size is filled in later from the NZB-wide representative
			// part size; naming/type come from the subject. Saves one full-segment
			// transfer per clean-named multipart file.
			if shouldSkipFirstSegmentFetch(fileToFetch) {
				return fetchResult{
					segmentID: fileToFetch.Segments[0].ID,
					data: &FirstSegmentData{
						File:                fileToFetch,
						SkippedFirstSegment: true,
						OriginalIndex:       originalIndex,
					},
				}, nil
			}

			firstSegment := fileToFetch.Segments[0]

			var (
				result   *nntppool.ArticleBody
				fetchErr error
			)
			if _, known := opts.KnownMissingSegmentIDs[firstSegment.ID]; known {
				fetchErr = nntppool.ErrArticleNotFound
			} else {
				result, fetchErr = p.fetchBodyWithRetry(ctx, cp, firstSegment.ID, articleDateOf(fileToFetch))
			}

			if fetchErr != nil && stderrors.Is(fetchErr, nntppool.ErrArticleNotFound) {
				// The first article is gone; the header lives in every article
				// of a multipart post, so ask the next few before giving up.
				p.log.DebugContext(ctx, "missing segment",
					"segment_id", firstSegment.ID,
					"error", fetchErr,
				)
				data, missing := p.probeHeaderFromLaterArticle(ctx, cp, fileToFetch, originalIndex, opts.KnownMissingSegmentIDs)
				if data != nil {
					return fetchResult{
						segmentID:  firstSegment.ID,
						isNotFound: true,
						data:       data,
						missingIDs: missing,
					}, nil
				}
				return fetchResult{
					segmentID:  firstSegment.ID,
					isNotFound: true,
					missingIDs: missing,
					data: &FirstSegmentData{
						File:                fileToFetch,
						MissingFirstSegment: true,
						IsArticleNotFound:   true,
						OriginalIndex:       originalIndex,
					},
					err: fmt.Errorf("failed to get body: %w", fetchErr),
				}, nil
			}
			if fetchErr != nil {
				p.log.DebugContext(ctx, "missing segment",
					"segment_id", firstSegment.ID,
					"error", fetchErr,
				)
				return fetchResult{
					segmentID: firstSegment.ID,
					data: &FirstSegmentData{
						File:                fileToFetch,
						MissingFirstSegment: true,
						OriginalIndex:       originalIndex,
					},
					err: fmt.Errorf("failed to get body: %w", fetchErr),
				}, nil
			}

			headers := result.YEnc

			// Use decoded bytes from result (up to 16KB for PAR2 detection).
			// 16KB completion from subsequent segments is deferred — it's only
			// needed if the NZB actually contains PAR2 descriptors, and that
			// can only be decided after all first segments are back.
			const maxRead = 16 * 1024
			rawBytes := result.Bytes
			if len(rawBytes) > maxRead {
				rawBytes = rawBytes[:maxRead]
			}

			return fetchResult{
				segmentID: firstSegment.ID,
				data: &FirstSegmentData{
					File:          fileToFetch,
					Headers:       headers,
					RawBytes:      rawBytes,
					OriginalIndex: originalIndex,
				},
			}, nil
		})
	}

	// Wait for all fetches to complete
	results, err := concPool.Wait()
	if err != nil {
		if stderrors.Is(err, context.Canceled) {
			return nil, notFoundIDs, errors.NewNonRetryableError("fetching first segments canceled", err)
		}

		return nil, notFoundIDs, errors.NewNonRetryableError("failed to fetch first segments", err)
	}

	// Build cache from all fetches (successful and failed)
	// Also collect permanently-missing segment IDs to skip redundant calls later
	for _, result := range results {
		for _, id := range result.missingIDs {
			notFoundIDs[id] = struct{}{}
		}
		if result.err == nil && result.isNotFound && result.segmentID != "" {
			// First article dead, header recovered from a later one.
			notFoundIDs[result.segmentID] = struct{}{}
		}
		if result.err != nil {
			if result.isNotFound && result.segmentID != "" {
				notFoundIDs[result.segmentID] = struct{}{}
			}
			// Add the data with MissingFirstSegment=true to track the failure
			if result.data != nil {
				cache = append(cache, result.data)
			}
			continue
		}

		cache = append(cache, result.data)
	}

	for _, data := range cache {
		if data == nil || data.File == nil || data.MissingFirstSegment {
			continue
		}

		if len(data.RawBytes) == 0 {
			p.log.WarnContext(context.Background(), "First segment has no data",
				"file", data.File.Subject)
		}
	}

	return cache, notFoundIDs, nil
}

// WarmFirstSegments fetches the first article of every file that ParseNzb
// would fetch, into the head cache, and returns when they have all landed or
// ctx ends. It exists so the fetch can overlap the fast-fail probe instead of
// following it: on a healthy release the parse pass then finds every head
// already cached. Errors are not reported; the parse pass diagnoses them.
func (p *Parser) WarmFirstSegments(ctx context.Context, files []nzbparser.NzbFile) {
	if ctx.Err() != nil || p.poolManager == nil || !p.poolManager.HasPool() {
		return
	}
	cp, err := p.poolManager.GetPool()
	if err != nil {
		return
	}
	maxFetch := max(min(min(len(files), p.getConfig().TotalProviderConnections()), maxFetchGoroutines), 1)
	warm := concpool.New().WithMaxGoroutines(maxFetch).WithContext(ctx)
	for i := range files {
		file := &files[i]
		if len(file.Segments) == 0 || shouldSkipFirstSegmentFetch(file) {
			continue
		}
		id := file.Segments[0].ID
		at := articleDateOf(file)
		warm.Go(func(ctx context.Context) error {
			_, _ = p.fetchBodyWithRetry(ctx, cp, id, at)
			return nil
		})
	}
	// The parse fetches one middle segment's yEnc header for the release-wide
	// part size, after every first segment is in; fetched here it overlaps the
	// probe and the parse finds it in the head cache.
	if seg, groups, ok := representativeMiddleSegment(files); ok {
		at := warmReleaseDate(files)
		warm.Go(func(ctx context.Context) error {
			_, _ = p.fetchYencHeaders(ctx, seg, groups, at)
			return nil
		})
	}

	// Store-only fetches: nothing in the parse reads them, so they run behind
	// the wait the parse blocks on, detached from the caller's cancellation
	// (which fires as soon as the awaited warm-up returns) but bounded.
	var storeOnly []string
	if primary := p.primaryVideoToWarm(files); primary >= 0 {
		storeOnly = append(storeOnly, files[primary].Segments[0].ID)
	}
	storeOnly = append(storeOnly, p.sevenZipTailToWarm(files)...)
	if len(storeOnly) > 0 {
		bg, cancel := context.WithTimeout(context.WithoutCancel(ctx), storeOnlyWarmTimeout)
		go func() {
			defer cancel()
			fill := concpool.New().WithMaxGoroutines(maxFetch).WithContext(bg)
			at := warmReleaseDate(files)
			for _, id := range storeOnly {
				fill.Go(func(ctx context.Context) error {
					_, _ = p.fetchBodyWithRetry(ctx, cp, id, at)
					return nil
				})
			}
			_ = fill.Wait()
		}()
	}
	_ = warm.Wait()
}

// storeOnlyWarmTimeout bounds the detached warm fetches that only fill the
// segment store.
const storeOnlyWarmTimeout = 20 * time.Second

// representativeMiddleSegment mirrors pickRepresentativeMiddleSegment on the
// raw NZB: the second segment of the first file with at least three, skipping
// files whose first segment is a declared gap. The parse re-derives its own
// choice from what it actually fetched; this only warms the likely answer.
func representativeMiddleSegment(files []nzbparser.NzbFile) (nzbparser.NzbSegment, []string, bool) {
	for i := range files {
		f := &files[i]
		if len(f.Segments) < 3 || holes.IsPlaceholderID(f.Segments[0].ID) || holes.IsPlaceholderID(f.Segments[1].ID) {
			continue
		}
		return f.Segments[1], f.Groups, true
	}
	return nzbparser.NzbSegment{}, nil, false
}

// sevenZipTailToWarm is the last two segment ids of the highest-numbered
// .7z.NNN volume, or nil. 7z analysis opens the archive by reading the first
// volume's head and then the end header at the very end of the last volume;
// with a segment store to keep the articles in, both become cache hits instead
// of two serial provider round trips on the import's critical path.
func (p *Parser) sevenZipTailToWarm(files []nzbparser.NzbFile) []string {
	if p.segmentStore == nil || p.segmentStore() == nil {
		return nil
	}
	lastVolume, lastIdx := -1, -1
	for i := range files {
		m := sevenZipContinuationPattern.FindStringSubmatch(files[i].Filename)
		if m == nil {
			continue
		}
		if n, err := strconv.Atoi(m[1]); err == nil && n > lastVolume {
			lastVolume, lastIdx = n, i
		}
	}
	if lastIdx < 0 {
		return nil
	}
	segs := files[lastIdx].Segments
	var ids []string
	for i := len(segs) - 1; i > 0 && i >= len(segs)-2; i-- {
		ids = append(ids, segs[i].ID)
	}
	return ids
}

// primaryVideoToWarm is the index of the largest video file whose first
// segment the warm-up would otherwise skip, or -1. Clean-named videos skip the
// fetch to save bandwidth, but the largest one is what a player opens first:
// when a segment store can keep the article, warming just that file turns the
// cold open into a cache hit for one article's worth of bandwidth.
func (p *Parser) primaryVideoToWarm(files []nzbparser.NzbFile) int {
	if p.segmentStore == nil || p.segmentStore() == nil {
		return -1
	}
	best := -1
	for i := range files {
		file := &files[i]
		if !shouldSkipFirstSegmentFetch(file) {
			continue
		}
		if _, video := skipEligibleVideoExtensions[strings.ToLower(filepath.Ext(file.Filename))]; !video {
			continue
		}
		if best < 0 || file.Bytes > files[best].Bytes {
			best = i
		}
	}
	return best
}

// fetchBodyWithRetry fetches one article, retrying transient failures. A
// genuine article-not-found (430/423) is permanent and returned at once:
// transient errors — connection exhaustion, timeouts, resets — must not be
// mistaken for a missing article, or one hiccup shatters a multi-volume set.
func (p *Parser) fetchBodyWithRetry(ctx context.Context, cp pool.NntpClient, segmentID string, articleDate time.Time) (*nntppool.ArticleBody, error) {
	if holes.IsPlaceholderID(segmentID) {
		// A gap placeholder names no article; asking a provider would only
		// buy a slow 430.
		return nil, nntppool.ErrArticleNotFound
	}
	if head, ok := p.heads.get(segmentID); ok && len(head.bytes) > 0 {
		return &nntppool.ArticleBody{MessageID: segmentID, Bytes: head.bytes, YEnc: head.meta}, nil
	}
	var (
		result   *nntppool.ArticleBody
		fetchErr error
	)
	for attempt := 1; attempt <= maxFirstSegmentFetchAttempts; attempt++ {
		// Take a token from the global import connection budget before the
		// per-attempt timeout starts, so queue wait never burns the deadline.
		releaseConn, acquireErr := p.poolManager.AcquireImportConnection(ctx)
		if acquireErr != nil {
			return nil, acquireErr
		}
		c, cancel := context.WithTimeout(ctx, time.Second*30)
		result, fetchErr = cp.Fetch(c, nntppool.Req{
			MessageID:   segmentID,
			ArticleDate: articleDate,
		})
		cancel()
		releaseConn()

		if fetchErr == nil {
			if p.poolManager != nil {
				p.poolManager.IncArticlesDownloaded()
				p.poolManager.UpdateDownloadProgress("", int64(len(result.Bytes)))
			}
			p.heads.put(segmentID, articleHead{meta: result.YEnc, bytes: clipHead(result.Bytes)})
			if p.segmentStore != nil {
				if store := p.segmentStore(); store != nil {
					_ = store.Put(segmentID, result.Bytes)
				}
			}
			return result, nil
		}
		if stderrors.Is(fetchErr, nntppool.ErrArticleNotFound) {
			return nil, fetchErr
		}
		if attempt < maxFirstSegmentFetchAttempts {
			select {
			case <-ctx.Done():
			case <-time.After(time.Duration(attempt) * firstSegmentRetryBackoff):
			}
		}
	}
	return nil, fetchErr
}

// probeHeaderFromLaterArticle recovers a file's yEnc header from articles
// 1..headerProbeAttempts-1 after the first one was found missing. It returns
// the recovered data (nil when every probe failed) and the ids of the articles
// found dead on the way. Only 430s are walked past: a transient failure ends
// the probe, since the file would be dropped on a guess otherwise.
func (p *Parser) probeHeaderFromLaterArticle(ctx context.Context, cp pool.NntpClient, file *nzbparser.NzbFile, originalIndex int, knownMissing map[string]struct{}) (*FirstSegmentData, []string) {
	var missing []string
	for i := 1; i < len(file.Segments) && i < headerProbeAttempts; i++ {
		if ctx.Err() != nil {
			break
		}
		seg := file.Segments[i]
		if _, known := knownMissing[seg.ID]; known {
			continue
		}
		result, err := p.fetchBodyWithRetry(ctx, cp, seg.ID, articleDateOf(file))
		if err != nil {
			if stderrors.Is(err, nntppool.ErrArticleNotFound) {
				missing = append(missing, seg.ID)
				continue
			}
			break
		}
		headers := result.YEnc
		headers.PartSize = firstPartSizeFromLaterArticle(headers, i)
		p.log.InfoContext(ctx, "Recovered yEnc header from a later article; first article is missing",
			"file", file.Filename, "first_segment", file.Segments[0].ID, "header_segment", seg.ID)
		return &FirstSegmentData{
			File:                  file,
			Headers:               headers,
			OriginalIndex:         originalIndex,
			FirstArticleMissingID: file.Segments[0].ID,
		}, missing
	}
	return nil, missing
}

// clipHead copies at most the leading 16 KiB of an article for the cache: that
// is all any later phase reads from a first segment (PAR2 Hash16k, magic
// bytes, archive headers).
func clipHead(b []byte) []byte {
	const maxHead = 16 * 1024
	if len(b) > maxHead {
		b = b[:maxHead]
	}
	return append([]byte(nil), b...)
}

// firstPartSizeFromLaterArticle derives the first part's decoded size from an
// article at index>0: its =ypart begin sits exactly index whole parts into the
// file, so begin/index is the uniform part size. nntppool reports PartBegin
// already converted to a 0-based offset. This also holds when the probed
// article is the last, shorter one, where its own PartSize would be wrong for
// the first part.
func firstPartSizeFromLaterArticle(meta nntppool.YEncMeta, index int) int64 {
	if index > 0 && meta.PartBegin > 0 {
		if step := meta.PartBegin / int64(index); step > 0 {
			return step
		}
	}
	return meta.PartSize
}

// pickRepresentativeMiddleSegment picks one "middle" segment (the second
// segment of a multi-segment, non-missing, non-404 file) whose yEnc header
// size can serve as the NZB-wide standard PartSize. Files produced by the
// same encoder share this value, so one fetch replaces one-per-file fetches.
func pickRepresentativeMiddleSegment(cache []*FirstSegmentData, notFoundIDs map[string]struct{}) (nzbparser.NzbSegment, []string, bool) {
	for _, d := range cache {
		if d == nil || d.File == nil || d.MissingFirstSegment {
			continue
		}
		if len(d.File.Segments) < 3 {
			continue
		}
		seg := d.File.Segments[1]
		if _, known404 := notFoundIDs[seg.ID]; known404 {
			continue
		}
		return seg, d.File.Groups, true
	}
	return nzbparser.NzbSegment{}, nil, false
}

// hasPar2IndexCandidate reports whether any cached first segment looks like a
// PAR2 index file (magic bytes + small segment count).
func (p *Parser) hasPar2IndexCandidate(cache []*FirstSegmentData) bool {
	for _, d := range cache {
		if d == nil || d.File == nil || d.MissingFirstSegment {
			continue
		}
		if len(d.File.Segments) == 0 || len(d.File.Segments) > par2.MaxIndexSegments {
			continue
		}
		if par2.HasMagicBytes(d.RawBytes) {
			return true
		}
	}
	return false
}

// isPar2SidecarExtension reports whether the filename has a small companion-file
// extension that never benefits from PAR2 Hash16k matching.
func isPar2SidecarExtension(filename string) bool {
	switch filepath.Ext(strings.ToLower(filename)) {
	case ".nfo", ".txt", ".srt", ".sub", ".jpg", ".jpeg", ".png", ".nzb", ".sfv", ".md5":
		return true
	}
	return false
}

// needsPar2Matching reports whether a file would actually benefit from PAR2
// descriptor matching (Hash16k → real filename and exact size). Only fetched files
// whose subject name is untrustworthy need it; clean names are trusted, the same
// philosophy as shouldSkipFirstSegmentFetch. Skipped/missing files have no first-16KB
// bytes to hash and can never be matched; PAR2 files describe others, not themselves.
func needsPar2Matching(d *FirstSegmentData) bool {
	if d == nil || d.File == nil || d.MissingFirstSegment || d.SkippedFirstSegment || d.FirstArticleMissingID != "" {
		return false
	}
	if par2.HasMagicBytes(d.RawBytes) {
		return false
	}
	name := d.File.Filename
	if fileinfo.IsPar2File(name) || isPar2SidecarExtension(name) {
		return false
	}
	// An empty, extension-less, or obfuscated name is exactly what PAR2
	// descriptors can recover. Anything else is trusted as-is.
	return name == "" || !fileinfo.HasValidExtensionLength(name) || fileinfo.IsProbablyObfuscated(name)
}

// anyFileNeedsPar2Matching reports whether at least one file in the NZB would
// benefit from PAR2 descriptor matching. When false, the PAR2 index download and
// the 16KB completion fan-out are skipped entirely — descriptors would only confirm
// names we already trust.
func anyFileNeedsPar2Matching(cache []*FirstSegmentData) bool {
	return slices.ContainsFunc(cache, needsPar2Matching)
}

// hasObfuscatedVolumeSet reports whether the NZB contains a multi-volume .partNN.rar set
// whose volumes each carry a distinct base name — the fingerprint of per-volume filename
// obfuscation (US8yidqp….part01.rar, BtEPCuoF….part02.rar, …). These random bases defeat
// the per-file obfuscation heuristic (mixed case with '-'/'_' separators reads as a clean,
// readable name), so needsPar2Matching never flags them; yet a numbered set in which every
// volume's base differs is unambiguous, because a real multi-volume set shares one base.
//
// When such a set exists, PAR2 descriptors can recover the real, shared volume names
// (Hash16k → name), letting grouping reassemble the set — so PAR2 matching is worth the
// network I/O even though no individual filename looks obfuscated. The "all bases distinct"
// rule keeps the trigger tight: one clean set has a single base, and two clean sets share a
// few bases across many volumes — neither trips it.
func hasObfuscatedVolumeSet(cache []*FirstSegmentData) bool {
	bases := make(map[string]struct{})
	count := 0
	for _, d := range cache {
		if d == nil || d.File == nil || d.MissingFirstSegment {
			continue
		}
		name := d.File.Filename
		if fileinfo.IsPar2File(name) {
			continue
		}
		// Only the part scheme (.partNN.rar); divergent-base obfuscation in the roll/numeric
		// schemes is rarer and left out to keep the trigger tight.
		if scheme, _, ok := rarname.VolumeNumber(name); !ok || scheme != rarname.SchemePart {
			continue
		}
		key, ok := rarname.SetKey(name)
		if !ok {
			continue
		}
		bases[key] = struct{}{}
		count++
	}
	return count >= 2 && len(bases) == count
}

// needs16KBCompletion decides whether a file is worth completing up to 16KB
// from additional segments. Only files that need PAR2 matching (untrustworthy
// names) qualify — clean names are trusted and sidecars/PAR2 files never benefit
// from Hash16k matching.
func needs16KBCompletion(d *FirstSegmentData, maxRead int) bool {
	if !needsPar2Matching(d) {
		return false
	}
	if len(d.RawBytes) >= maxRead {
		return false
	}
	if len(d.File.Segments) <= 1 {
		return false
	}
	return true
}

// complete16KBReads fetches additional segments for files whose first segment
// returned less than 16KB. Only called when the NZB actually contains PAR2
// descriptors that could match the resulting MD5(first16KB). Best-effort:
// missing or failed segments leave RawBytes as-is.
func (p *Parser) complete16KBReads(ctx context.Context, cache []*FirstSegmentData, notFoundIDs map[string]struct{}) {
	const maxRead = 16 * 1024
	if p.poolManager == nil || !p.poolManager.HasPool() {
		return
	}
	cp, err := p.poolManager.GetPool()
	if err != nil {
		return
	}

	var targets []*FirstSegmentData
	for _, d := range cache {
		if needs16KBCompletion(d, maxRead) {
			targets = append(targets, d)
		}
	}
	if len(targets) == 0 {
		return
	}

	// Goroutine bound only — the real fetch bound is the pool manager's global
	// import connection budget, acquired per Body call below.
	maxFetch := max(min(min(len(targets), p.getConfig().TotalProviderConnections()), maxFetchGoroutines), 1)
	pool := concpool.New().WithMaxGoroutines(maxFetch).WithContext(ctx)
	for _, d := range targets {
		pool.Go(func(ctx context.Context) error {
			// Determine additional segments needed based on NZB-reported bytes
			bytesRead := len(d.RawBytes)
			estimatedTotal := bytesRead
			var segsNeeded []nzbparser.NzbSegment
			for i := 1; i < len(d.File.Segments) && estimatedTotal < maxRead; i++ {
				seg := d.File.Segments[i]
				if _, known404 := notFoundIDs[seg.ID]; known404 {
					continue
				}
				segsNeeded = append(segsNeeded, seg)
				estimatedTotal += seg.Bytes
			}
			if len(segsNeeded) == 0 {
				return nil
			}

			articleDate := articleDateOf(d.File)
			segResults := make([][]byte, len(segsNeeded))
			g, gctx := errgroup.WithContext(ctx)
			for i, seg := range segsNeeded {
				g.Go(func() error {
					// Budget token first, so queue wait never burns the deadline.
					releaseConn, err := p.poolManager.AcquireImportConnection(gctx)
					if err != nil {
						return nil // best-effort
					}
					defer releaseConn()
					segCtx, segCancel := context.WithTimeout(gctx, time.Second*30)
					defer segCancel()
					sr, err := cp.Fetch(segCtx, nntppool.Req{
						MessageID:   seg.ID,
						ArticleDate: articleDate,
					})
					if err != nil {
						return nil // best-effort
					}
					if p.poolManager != nil {
						p.poolManager.IncArticlesDownloaded()
						p.poolManager.UpdateDownloadProgress("", int64(len(sr.Bytes)))
					}
					segResults[i] = sr.Bytes
					return nil
				})
			}
			_ = g.Wait()

			buffer := make([]byte, maxRead)
			copy(buffer, d.RawBytes)
			for _, segBytes := range segResults {
				if len(segBytes) == 0 || bytesRead >= maxRead {
					break
				}
				n := copy(buffer[bytesRead:], segBytes)
				bytesRead += n
			}
			d.RawBytes = buffer[:bytesRead]
			p.heads.put(d.File.Segments[0].ID, articleHead{meta: d.Headers, bytes: clipHead(d.RawBytes)})
			return nil
		})
	}
	_ = pool.Wait()
}

// fetchYencHeaders fetches the yenc header to get the actual part size for a specific segment.
// It uses BodyAsync with io.Discard + onMeta to return headers as soon as =ybegin/=ypart
// lines are parsed, without waiting for the full article body to transfer.
func (p *Parser) fetchYencHeaders(ctx context.Context, segment nzbparser.NzbSegment, groups []string, articleDate time.Time) (nntppool.YEncMeta, error) {
	if p.poolManager == nil {
		return nntppool.YEncMeta{}, errors.NewNonRetryableError("no pool manager available", nil)
	}

	if head, ok := p.heads.get(segment.ID); ok && head.meta.PartSize > 0 {
		return head.meta, nil
	}
	if holes.IsPlaceholderID(segment.ID) {
		return nntppool.YEncMeta{}, nntppool.ErrArticleNotFound
	}

	cp, err := p.poolManager.GetPool()
	if err != nil {
		return nntppool.YEncMeta{}, errors.NewNonRetryableError("no connection pool available", err)
	}

	// onMeta fires after =ybegin/=ypart parsing (~first 2 lines),
	// while the body continues draining to io.Discard in the background.
	metaCh := make(chan nntppool.YEncMeta, 1)
	resultCh := cp.FetchAsync(ctx, nntppool.Req{
		MessageID:   segment.ID,
		Writer:      io.Discard,
		ArticleDate: articleDate,
		OnMeta: func(meta nntppool.YEncMeta) {
			metaCh <- meta
		},
	})

	// Wait for either: headers via onMeta (fast), full result (error or no yEnc), or context cancel.
	select {
	case headers := <-metaCh:
		if headers.PartSize <= 0 {
			return nntppool.YEncMeta{}, errors.NewNonRetryableError("invalid part size from yenc header", nil)
		}

		if p.poolManager != nil {
			p.poolManager.IncArticlesDownloaded()
			p.poolManager.UpdateDownloadProgress("", int64(headers.PartSize))
		}
		p.heads.put(segment.ID, articleHead{meta: headers})

		return headers, nil
	case result := <-resultCh:
		// BodyAsync completed before onMeta fired — either error or non-yEnc article
		if result.Err != nil {
			return nntppool.YEncMeta{}, errors.NewNonRetryableError("failed to get body", result.Err)
		}

		if p.poolManager != nil {
			p.poolManager.IncArticlesDownloaded()
			p.poolManager.UpdateDownloadProgress("", int64(result.Body.YEnc.PartSize))
		}

		// onMeta didn't fire but body completed — use headers from result
		headers := result.Body.YEnc
		if headers.PartSize <= 0 {
			return nntppool.YEncMeta{}, errors.NewNonRetryableError("invalid part size from yenc header", nil)
		}
		p.heads.put(segment.ID, articleHead{meta: headers})
		return headers, nil
	case <-ctx.Done():
		return nntppool.YEncMeta{}, errors.NewNonRetryableError("context canceled", ctx.Err())
	}
}

// firstSegmentYencInfo carries the pre-fetched yEnc metadata of a file's first segment.
// Zero fields mean "unknown" — normalizeSegmentSizesWithYenc falls back to network
// fetches for anything it cannot resolve from this struct.
type firstSegmentYencInfo struct {
	PartSize int64 // decoded size of the first part (from =ybegin/=ypart)
	FileSize int64 // total decoded file size (from "=ybegin size="); enables last-part derivation
}

// deriveLastPartSize computes the decoded size of a multipart file's last yEnc part from
// the total file size, avoiding a full-segment network transfer just to read its headers
// (nntppool drains the entire body even for header-only fetches). In a multipart yEnc
// post every part except the last has the same size, so:
//
//	last = fileSize − firstPartSize − (numSegments−2) × standardPartSize
//
// Returns ok=false whenever the inputs are unknown or the result is implausible
// (≤0 or larger than a full part) — callers must then fetch the last segment's headers.
func deriveLastPartSize(fileSize, firstPartSize, standardPartSize int64, numSegments int) (int64, bool) {
	if fileSize <= 0 || firstPartSize <= 0 || numSegments < 2 {
		return 0, false
	}

	var last, maxAllowed int64
	if numSegments == 2 {
		last = fileSize - firstPartSize
		maxAllowed = firstPartSize
	} else {
		if standardPartSize <= 0 {
			return 0, false
		}
		last = fileSize - firstPartSize - int64(numSegments-2)*standardPartSize
		maxAllowed = standardPartSize
	}

	if last <= 0 || last > maxAllowed {
		return 0, false
	}
	return last, true
}

// normalizeSegmentSizesWithYenc normalizes segment sizes using yEnc PartSize headers.
// This handles cases where NZB segment sizes include yEnc overhead.
// firstSegment carries the pre-fetched first-segment PartSize and total FileSize; when
// FileSize is known the last part's size is derived arithmetically instead of fetched
// (each header fetch drains a full segment body over the wire).
// nzbStandardPartSize, when >0, is a representative middle-segment PartSize shared across the NZB;
// passing it here skips the per-file second-segment network call for files with 3+ segments.
// notFoundIDs is the set of segment IDs known to return 430; those are skipped without a network call.
func (p *Parser) normalizeSegmentSizesWithYenc(ctx context.Context, segments []nzbparser.NzbSegment, firstSegment firstSegmentYencInfo, nzbStandardPartSize int64, notFoundIDs map[string]struct{}, articleDate time.Time) error {
	firstPartSize := firstSegment.PartSize
	fileSize := firstSegment.FileSize
	if firstPartSize <= 0 {
		if _, known404 := notFoundIDs[segments[0].ID]; known404 {
			return fmt.Errorf("first segment %s is known not found: %w", segments[0].ID, nntppool.ErrArticleNotFound)
		}
		// Fetch PartSize from first segment if not in cache. The same headers carry the
		// total file size, which enables last-part derivation below.
		firstPartHeaders, err := p.fetchYencHeaders(ctx, segments[0], nil, articleDate)
		if err != nil {
			return fmt.Errorf("failed to fetch first segment yEnc part size: %w", err)
		}
		firstPartSize = firstPartHeaders.PartSize
		if fileSize <= 0 {
			fileSize = firstPartHeaders.FileSize
		}
	}

	if len(segments) == 1 {
		segments[0].Bytes = int(firstPartSize)
		return nil
	}

	// Handle files with exactly 2 segments (first and last only)
	if len(segments) == 2 {
		segments[0].Bytes = int(firstPartSize)

		if last, ok := deriveLastPartSize(fileSize, firstPartSize, 0, 2); ok {
			segments[1].Bytes = int(last)
			return nil
		}

		if _, known404 := notFoundIDs[segments[1].ID]; known404 {
			return fmt.Errorf("second segment %s is known not found: %w", segments[1].ID, nntppool.ErrArticleNotFound)
		}
		// Fetch PartSize from last segment
		lastPartHeaders, err := p.fetchYencHeaders(ctx, segments[1], nil, articleDate)
		if err != nil {
			return fmt.Errorf("failed to fetch last segment yEnc part size: %w", err)
		}
		segments[1].Bytes = int(lastPartHeaders.PartSize)

		return nil
	}

	// Determine the standard (middle-segment) part size and the actual last-segment size.
	// The standard size is either reused from the NZB-wide representative fetch,
	// or fetched once per file when the shared value is unavailable. The last-segment
	// size is derived from the total file size when known, otherwise fetched.
	lastSegmentIndex := len(segments) - 1

	standardPartSize := nzbStandardPartSize
	var lastPartSize int64

	if standardPartSize <= 0 && fileSize <= 0 {
		// Neither a shared part size nor a total file size: both the second and last
		// segments must be fetched — do it in parallel as before.
		if _, known404 := notFoundIDs[segments[1].ID]; known404 {
			return fmt.Errorf("second segment %s is known not found: %w", segments[1].ID, nntppool.ErrArticleNotFound)
		}
		if _, known404 := notFoundIDs[segments[lastSegmentIndex].ID]; known404 {
			return fmt.Errorf("last segment %s is known not found: %w", segments[lastSegmentIndex].ID, nntppool.ErrArticleNotFound)
		}
		var secondPartHeaders, lastPartHeaders nntppool.YEncMeta
		g, gctx := errgroup.WithContext(ctx)
		g.Go(func() error {
			h, err := p.fetchYencHeaders(gctx, segments[1], nil, articleDate)
			if err != nil {
				return fmt.Errorf("failed to fetch second segment yEnc part size: %w", err)
			}
			secondPartHeaders = h
			return nil
		})
		g.Go(func() error {
			h, err := p.fetchYencHeaders(gctx, segments[lastSegmentIndex], nil, articleDate)
			if err != nil {
				return fmt.Errorf("failed to fetch last segment yEnc part size: %w", err)
			}
			lastPartHeaders = h
			return nil
		})
		if err := g.Wait(); err != nil {
			return err
		}
		standardPartSize = secondPartHeaders.PartSize
		lastPartSize = lastPartHeaders.PartSize
	} else {
		if standardPartSize <= 0 {
			// No NZB-wide representative — fetch this file's second segment once.
			if _, known404 := notFoundIDs[segments[1].ID]; known404 {
				return fmt.Errorf("second segment %s is known not found: %w", segments[1].ID, nntppool.ErrArticleNotFound)
			}
			h, err := p.fetchYencHeaders(ctx, segments[1], nil, articleDate)
			if err != nil {
				return fmt.Errorf("failed to fetch second segment yEnc part size: %w", err)
			}
			standardPartSize = h.PartSize
		}

		if last, ok := deriveLastPartSize(fileSize, firstPartSize, standardPartSize, len(segments)); ok {
			lastPartSize = last
		} else {
			if _, known404 := notFoundIDs[segments[lastSegmentIndex].ID]; known404 {
				return fmt.Errorf("last segment %s is known not found: %w", segments[lastSegmentIndex].ID, nntppool.ErrArticleNotFound)
			}
			h, err := p.fetchYencHeaders(ctx, segments[lastSegmentIndex], nil, articleDate)
			if err != nil {
				return fmt.Errorf("failed to fetch last segment yEnc part size: %w", err)
			}
			lastPartSize = h.PartSize
		}
	}

	// Apply the sizes:
	// - First segment: use its actual size
	segments[0].Bytes = int(firstPartSize)

	// - Middle segments (indices 1 through n-2): use standard size from second segment
	for i := 1; i < len(segments)-1; i++ {
		segments[i].Bytes = int(standardPartSize)
	}

	// - Last segment: use its actual (fetched or derived) size
	segments[lastSegmentIndex].Bytes = int(lastPartSize)

	return nil
}

// fallbackGetFileInfos is a "dumb" fallback that extracts file info directly from NZB XML
// without any network validation. This is used when the first segments are missing.
func (p *Parser) fallbackGetFileInfos(files []nzbparser.NzbFile) []*fileinfo.FileInfo {
	fileInfos := make([]*fileinfo.FileInfo, 0)

	for i, file := range files {
		// Basic PAR2 skip
		if fileinfo.IsPar2File(file.Filename) {
			continue
		}

		// Skip files without segments
		if len(file.Segments) == 0 {
			continue
		}

		// Calculate basic size from segments
		var size int64
		for _, seg := range file.Segments {
			size += int64(seg.Bytes)
		}

		// Create a basic FileInfo
		info := &fileinfo.FileInfo{
			NzbFile:       file,
			Filename:      file.Filename,
			ReleaseDate:   time.Unix(int64(file.Date), 0),
			IsPar2Archive: false,
			FileSize:      &size,
			IsRar:         fileinfo.HasRarMagic(nil) || fileinfo.IsRarFile(file.Filename),
			Is7z:          fileinfo.Is7zFile(file.Filename),
			OriginalIndex: i,
		}

		fileInfos = append(fileInfos, info)
	}

	return fileInfos
}

// determineNzbType analyzes the parsed files to determine the NZB type
func (p *Parser) determineNzbType(files []ParsedFile) NzbType {
	// Exclude PAR2 files — a single media file + N PAR2 files is still a single-file NZB
	var mediaFiles []ParsedFile
	for _, f := range files {
		if !f.IsPar2Archive && !fileinfo.IsPar2File(f.Filename) {
			mediaFiles = append(mediaFiles, f)
		}
	}
	if len(mediaFiles) == 0 {
		return NzbTypeMultiFile // all-PAR2 edge case; allPar2 check handles this earlier
	}
	files = mediaFiles

	if len(files) == 1 {
		// Single file NZB
		if files[0].IsRarArchive {
			return NzbTypeRarArchive
		}
		if files[0].Is7zArchive {
			return NzbType7zArchive
		}
		return NzbTypeSingleFile
	}

	// Multiple files - check if any are RAR or 7zip archives
	hasRarFiles := false
	has7zFiles := false
	for _, file := range files {
		if file.IsRarArchive {
			hasRarFiles = true
		}
		if file.Is7zArchive {
			has7zFiles = true
		}
	}

	// Prioritize RAR if both types exist (shouldn't normally happen)
	if hasRarFiles {
		return NzbTypeRarArchive
	}
	if has7zFiles {
		return NzbType7zArchive
	}

	return NzbTypeMultiFile
}

var (
	passwordBracketsRegex = regexp.MustCompile(`(?i)\{\{\s*([^{}]+?)\s*\}\}`)
	passwordSlashRegex    = regexp.MustCompile(`(?i)/(?:password|pwd)[:=]\s*([^\s/]+)`)
	passwordMetaRegex     = regexp.MustCompile(`(?i)<meta\b[^>]*type\s*=\s*["']?(?:password|pass|pwd|password_hash)["']?[^>]*>([\s\S]*?)<\/meta>`)
)

func extractNzbPassword(n *nzbparser.Nzb, nzbPath string) string {
	if n == nil {
		return ""
	}
	// 1. Check n.Meta with case-insensitive keys
	if n.Meta != nil {
		for k, v := range n.Meta {
			if strings.EqualFold(k, "password") || strings.EqualFold(k, "pass") || strings.EqualFold(k, "pwd") || strings.EqualFold(k, "password_hash") {
				if trimmed := strings.TrimSpace(v); trimmed != "" {
					return trimmed
				}
			}
		}
	}
	// 2. Check filename for {{password}} or /password:xyz/ patterns
	baseName := filepath.Base(nzbPath)
	if m := passwordBracketsRegex.FindStringSubmatch(baseName); len(m) > 1 {
		if trimmed := strings.TrimSpace(m[1]); trimmed != "" {
			return trimmed
		}
	}
	if m := passwordSlashRegex.FindStringSubmatch(baseName); len(m) > 1 {
		if trimmed := strings.TrimSpace(m[1]); trimmed != "" {
			return trimmed
		}
	}
	// 3. Check file subjects for {{password}} or /password:xyz/
	for _, f := range n.Files {
		if m := passwordBracketsRegex.FindStringSubmatch(f.Subject); len(m) > 1 {
			if trimmed := strings.TrimSpace(m[1]); trimmed != "" {
				return trimmed
			}
		}
		if m := passwordSlashRegex.FindStringSubmatch(f.Subject); len(m) > 1 {
			if trimmed := strings.TrimSpace(m[1]); trimmed != "" {
				return trimmed
			}
		}
	}
	// 4. Check NZB file on disk if available (for <meta type="password"> outside <head>)
	if nzbPath != "" {
		if data, err := os.ReadFile(nzbPath); err == nil && len(data) > 0 {
			limit := len(data)
			if limit > 64*1024 {
				limit = 64 * 1024
			}
			if m := passwordMetaRegex.FindSubmatch(data[:limit]); len(m) > 1 {
				if trimmed := strings.TrimSpace(string(m[1])); trimmed != "" {
					return trimmed
				}
			}
		}
	}
	return ""
}

// propagateArchiveType sets the archive-type flag on non-PAR2 files that are
// archive parts (including split continuation volumes with numeric or extensionless names).
// Non-archive sidecars (.txt, .nfo, .sfv, etc.) are excluded so they are processed as regular files.
func (p *Parser) propagateArchiveType(parsed *ParsedNzb) {
	switch parsed.Type {
	case NzbType7zArchive:
		for i := range parsed.Files {
			f := &parsed.Files[i]
			if !f.IsPar2Archive && !fileinfo.IsPar2File(f.Filename) && !isPar2SidecarExtension(f.Filename) {
				f.Is7zArchive = true
			}
		}
	case NzbTypeRarArchive:
		for i := range parsed.Files {
			f := &parsed.Files[i]
			if !f.IsPar2Archive && !fileinfo.IsPar2File(f.Filename) && !isPar2SidecarExtension(f.Filename) {
				f.IsRarArchive = true
			}
		}
	}
}

// GetMetadata extracts metadata from the NZB head section
func (p *Parser) GetMetadata(nzbXML *nzbparser.Nzb) map[string]string {
	metadata := make(map[string]string)

	if nzbXML.Meta == nil {
		return metadata
	}

	return nzbXML.Meta
}

// ValidateNzb performs basic validation on the parsed NZB
func (p *Parser) ValidateNzb(parsed *ParsedNzb) error {
	if parsed.TotalSize <= 0 {
		return errors.NewNonRetryableError("invalid NZB: total size is zero", nil)
	}

	if parsed.SegmentsCount <= 0 {
		return errors.NewNonRetryableError("invalid NZB: no segments found", nil)
	}

	for i, file := range parsed.Files {
		if len(file.Segments) == 0 {
			return errors.NewNonRetryableError(fmt.Sprintf("invalid NZB: file %d has no segments", i), nil)
		}

		if file.Size <= 0 {
			return errors.NewNonRetryableError(fmt.Sprintf("invalid NZB: file %d has invalid size", i), nil)
		}

		if len(file.Groups) == 0 {
			return errors.NewNonRetryableError(fmt.Sprintf("invalid NZB: file %d has no groups", i), nil)
		}
	}

	return nil
}
