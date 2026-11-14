// Package bundleimport accepts a verified peer bundle as a local import.
//
// A bundle is a sanitised finished import: segment lists and a resolved file
// layout. Accepting one lets this node mount a release without downloading or
// parsing archives. Everything in a bundle is hostile input, so Accept
// re-validates it, applies local policy, probes the release against the user's
// own providers, proves through the real reader stack that the start of the
// main media file decodes, and only then writes anything the user can see.
// What it writes is marked as coming from the peer network and held back from
// the ARRs and from publishing until a full health check of every file passed.
//
// No archive parser and no key derivation run anywhere in this package: the
// layout is already resolved, and a wrong AES key just yields bytes that fail
// the media probe.
//
// The package defines the narrow interfaces it needs; internal/health and
// internal/importer never import it (see internal/peernet/bridge).
package bundleimport

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/kipsilabs/altmount/internal/contentverify"
	"github.com/kipsilabs/altmount/internal/database"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/peernet/bundle"
)

// Origin names where a bundle came from. It is kept only so a bad bundle can be
// reported; it is never shown as a link.
type Origin struct {
	NetworkID   string
	PublisherID string
	// Open says the bundle came from the open network (anyone can join it).
	Open bool
	// Corroborators is how many distinct network ranges announced this very
	// Bundle ID.
	Corroborators int
	// BundleID is the Bundle ID (64 hex) the bundle was fetched under; it keys
	// the negative cache of layouts that failed local verification.
	BundleID string
}

// Fetcher returns a bundle that is already hash- and ID-verified (bundle.Verify)
// for a Release ID, with the network and publisher it came from. It is
// implemented by the peer network service and injected later.
type Fetcher interface {
	FetchBundle(ctx context.Context, releaseID string) (*bundle.Bundle, Origin, error)
}

// FetchOptions are the caller's requirements for the offers, applied before
// any peer is contacted.
type FetchOptions struct {
	// AllowOpen says offers of the open network may be taken at all.
	AllowOpen bool
	// MinOpenCorroborators is how many distinct network ranges must announce an
	// open-network offer's Bundle ID.
	MinOpenCorroborators int
}

// OptionFetcher is optionally implemented by a Fetcher: it filters the offers
// by the caller's requirements before dialing anything.
type OptionFetcher interface {
	FetchBundleWith(ctx context.Context, releaseID string, o FetchOptions) (*bundle.Bundle, Origin, error)
}

// BundleFailedReporter is optionally implemented by a BadBundleReporter: it is
// told that a Bundle ID's layout failed local verification (an attributable
// contradiction), so the same layout is skipped from every publisher for a
// while.
type BundleFailedReporter interface {
	ReportFailedBundle(ctx context.Context, bundleID, releaseID string)
}

// BadBundleReporter is told when a publisher served a bundle whose release
// failed its full local check, so the network side can back that publisher off.
// It must return quickly and never fail.
type BadBundleReporter interface {
	ReportBadBundle(ctx context.Context, origin Origin, releaseID string)
}

// BundleVerifiedReporter is optionally implemented by a BadBundleReporter: it
// is told when every file of an import from a peer bundle passed its full
// check, so an earlier penalty of that publisher can be cleared.
type BundleVerifiedReporter interface {
	ReportVerifiedBundle(ctx context.Context, origin Origin, releaseID string)
}

// ProbeFile is one file of the release probe: every real segment id the file
// reads through (own segments and outer archive volumes), in order.
type ProbeFile struct {
	Name       string
	SegmentIDs []string
}

// ReleaseProber is the importer's release probe (a STAT sample against the
// user's own providers) with the same sampling rules as a normal import. It
// returns ErrProbeMissing when sampled articles are gone, ErrProbeInconclusive
// when the providers could not answer, nil when the sample is reachable.
type ReleaseProber interface {
	ProbeRelease(ctx context.Context, files []ProbeFile) error
}

var (
	// ErrProbeMissing means a sampled article does not exist on the providers.
	ErrProbeMissing = errors.New("bundleimport: sampled articles are missing")
	// ErrProbeInconclusive means the providers could not answer.
	ErrProbeInconclusive = errors.New("bundleimport: release probe inconclusive")
)

// Library is the local metadata store the accepted files are written to. The
// real metadata service satisfies it, so every path chokepoint and observer
// (the content index) applies.
type Library interface {
	FileExists(virtualPath string) bool
	ListDirectory(virtualPath string) ([]string, error)
	ReadFileMetadata(virtualPath string) (*metapb.FileMetadata, error)
	WriteFileMetadata(virtualPath string, m *metapb.FileMetadata) error
	DeleteFileMetadata(ctx context.Context, virtualPath string) error
}

// ContentIndex is the local content index (the database repository). Accept
// records the bundle's verified Release ID and scheme in it for the files it
// writes, because the written metadata cannot reproduce them.
type ContentIndex interface {
	UpsertBatch(ctx context.Context, entries []database.PeernetContentEntry) error
	Delete(ctx context.Context, filePath string) error
}

// Stager makes a hidden, throwaway place where a bundle's layout can be read
// through the real serving stack before anything the user can see is written.
type Stager interface {
	NewStaging() (Staging, error)
}

// Staging is one hidden staging area. Nothing in it is ever listed, mounted or
// indexed; Close removes all of it.
type Staging interface {
	WriteFileMetadata(virtualPath string, m *metapb.FileMetadata) error
	Opener() contentverify.Opener
	Close() error
}

// State is the persisted peer-bundle bookkeeping (the database repository).
type State interface {
	Upsert(ctx context.Context, rows []database.PeernetBundleImport) error
	Get(ctx context.Context, filePath string) (database.PeernetBundleImport, bool, error)
	Delete(ctx context.Context, paths []string) error
	PendingAmong(ctx context.Context, paths []string) (map[string]database.PeernetBundleImport, error)
	MarkPassed(ctx context.Context, filePath string) (bool, error)
	MarkFailed(ctx context.Context, filePath string) (bool, error)
	ClaimAnnouncement(ctx context.Context, queueID int64) (database.PeernetBundleImport, bool, error)
	ReleaseAnnouncement(ctx context.Context, queueID int64) error
	QueuesAwaitingAnnouncement(ctx context.Context) ([]int64, error)
	QueueHasPending(ctx context.Context, queueID int64) (bool, error)
	SetQueue(ctx context.Context, paths []string, queueID int64) error
	MoveFile(ctx context.Context, from, to string) (bool, error)
	MoveDir(ctx context.Context, from, to string) error
	DeleteDir(ctx context.Context, dir string) error
	PendingCounts(ctx context.Context, staleBefore time.Time) (pending, stale int, err error)
	StalePending(ctx context.Context, staleBefore time.Time, limit int) ([]database.PeernetBundleImport, error)
	CommitRows(ctx context.Context, paths []string) error
	ListUncommitted(ctx context.Context, limit int) ([]database.PeernetBundleImport, error)
	ListByQueue(ctx context.Context, queueID int64) ([]database.PeernetBundleImport, error)
	ListAfter(ctx context.Context, afterPath string, limit int) ([]database.PeernetBundleImport, error)
	OrphanedHolds(ctx context.Context) ([]int64, error)
	SetBundleID(ctx context.Context, paths []string, bundleID string) error
	BundleIDOf(ctx context.Context, filePath string) (string, error)
	SettledHolds(ctx context.Context) ([]int64, error)
}

// Announcement is what the ARRs are told about once every file of a bundle
// import passed its full check.
type Announcement struct {
	QueueID       int64
	Category      string
	TargetPath    string
	ResultingPath string
}

// Announcer lifts the hold on a queue item (it stops being hidden from the
// SABnzbd-compatible history) and notifies the ARRs. It is implemented by the
// importer; it is called at most once per import.
type Announcer interface {
	AnnounceVerifiedImport(ctx context.Context, a Announcement) error
}

// FailureHandler is what ends a peer import that failed its full check (or
// never got one): the Announcer implemented by the importer also provides it.
// Every method is idempotent.
type FailureHandler interface {
	// FailImport turns the held queue item into a failed one with a short
	// message, so the SABnzbd-compatible history reports it as failed.
	FailImport(ctx context.Context, queueID int64, reason string) error
	// RemoveFailedFile removes one file of a failed import the way the health
	// worker removes a corrupted file: metadata, library copy and health record.
	RemoveFailedFile(ctx context.Context, virtualPath string) error
	// ReleaseHold lifts the hold of a queue item that no file refers to any more.
	ReleaseHold(ctx context.Context, queueID int64) error
}

// Requeuer is optionally implemented by the FailureHandler. It puts the held
// queue item of a fast-path import (one the user supplied an NZB for) back for
// a normal import, with the peer fast path off for that item once, and reports
// whether it did. A direct import has no NZB and is not requeueable (false).
type Requeuer interface {
	RequeueForNormalImport(ctx context.Context, queueID int64) (bool, error)
}

// Recorder creates the queue and history entries of a direct import (one with
// no NZB) so it looks like a normal import to the rest of the system. It
// returns the queue id used to hold the ARR announcement.
type Recorder interface {
	RecordPeerImport(ctx context.Context, rec ImportRecord) (queueID int64, err error)
}

// ImportRecord describes a finished direct import.
type ImportRecord struct {
	Title         string
	Category      string
	TargetPath    string
	ResultingPath string
	WrittenPaths  []string
	TotalSize     int64
	ReleaseID     string
	// SkipArr is the caller's own wish not to notify the ARRs.
	SkipArr bool
	// Metadata and DownloadID come from the caller's Target.
	Metadata   string
	DownloadID string
}

// Reason is why a bundle was rejected.
type Reason string

// Rejection reasons. Every one has a counter.
const (
	ReasonInvalid           Reason = "invalid"            // structure or identity check failed
	ReasonTooLarge          Reason = "too_large"          // over the local size limit
	ReasonTooManyFiles      Reason = "too_many_files"     // over the local file-count limit
	ReasonBadName           Reason = "bad_name"           // file name is not a plain sanitised name
	ReasonBadTarget         Reason = "bad_target"         // the local target folder is unusable
	ReasonCollision         Reason = "collision"          // a different file already sits at a target path
	ReasonHealthDisabled    Reason = "health_disabled"    // no full check can ever run
	ReasonProbeMissing      Reason = "probe_missing"      // release probe found missing articles
	ReasonProbeInconclusive Reason = "probe_inconclusive" // release probe could not answer
	ReasonNoMediaFile       Reason = "no_media_file"      // nothing the media probe can verify
	ReasonMediaInvalid      Reason = "media_invalid"      // media header did not decode
	ReasonMediaMissing      Reason = "media_missing"      // media head article missing
	ReasonMediaInconclusive Reason = "media_inconclusive" // media probe could not complete
	ReasonStaging           Reason = "staging_failed"     // hidden staging area failed
	ReasonWriteFailed       Reason = "write_failed"       // local write failed (rolled back)
	ReasonNZBMismatch       Reason = "nzb_mismatch"       // bundle does not match the user's NZB
	ReasonUnavailable       Reason = "unavailable"        // no bundle could be fetched
	ReasonNoNetwork         Reason = "no_network"         // feature not wired
	ReasonSizeImplausible   Reason = "size_implausible"   // declared sizes do not fit the user's NZB
	ReasonYencMismatch      Reason = "yenc_mismatch"      // sampled article headers contradict the layout
	ReasonStructureInvalid  Reason = "structure_invalid"  // media structure check failed beyond the first bytes
	ReasonForbiddenFile     Reason = "forbidden_file"     // a file a media server or the OS could act on
	ReasonSidecarTooLarge   Reason = "sidecar_too_large"  // a subtitle or nfo over its size cap
	// ReasonLayoutNotAllowedOpen: the open network only carries plain layouts.
	ReasonLayoutNotAllowedOpen Reason = "layout_not_allowed_open"
	// ReasonContainerNotVerifiable: the open network only carries media whose
	// container has a real structural check.
	ReasonContainerNotVerifiable Reason = "container_not_verifiable"
	// ReasonTooManyTables: the layout has more segment tables than the yEnc
	// sampling budget can cover at the minimum rate.
	ReasonTooManyTables Reason = "too_many_tables"
)

// RejectError is the typed rejection Accept returns.
type RejectError struct {
	Reason Reason
	Err    error
}

func (e *RejectError) Error() string {
	if e.Err == nil {
		return "bundle rejected: " + string(e.Reason)
	}
	return fmt.Sprintf("bundle rejected: %s: %v", e.Reason, e.Err)
}

// Unwrap exposes the cause.
func (e *RejectError) Unwrap() error { return e.Err }

func reject(r Reason, err error) *RejectError { return &RejectError{Reason: r, Err: err} }

// ReasonOf returns the rejection reason of err, or "" when it is not one.
func ReasonOf(err error) Reason {
	var re *RejectError
	if errors.As(err, &re) {
		return re.Reason
	}
	return ""
}

// Policy is the local acceptance policy.
type Policy struct {
	MaxTotalSize int64
	MaxFiles     int
	// MaxMediaProbes caps how many media files are probed through the layout.
	MaxMediaProbes int
	// ProbeTimeout bounds the read of one media head.
	MediaProbeTimeout time.Duration
	// AcceptTimeout bounds the whole of Accept.
	AcceptTimeout time.Duration
	// MaxYencSamples caps the articles whose yEnc headers are read, MaxYencBytes
	// the declared bytes those reads cost (the pool reads whole articles) and
	// YencTimeout the time of the whole sampling. Every segment table gets its
	// first, last and at least one random interior article; a layout whose
	// tables do not fit the cap at that rate is rejected, never left unsampled.
	MaxYencSamples int
	MaxYencBytes   int64
	YencTimeout    time.Duration
	// MaxMediaFiles caps the media files a bundle may write (each is verified).
	MaxMediaFiles int
	// MaxSidecars caps the small allow-listed files (subtitles, nfo) written.
	MaxSidecars int
	// MaxSubtitleBytes and MaxNfoBytes cap one sidecar file.
	MaxSubtitleBytes int64
	MaxNfoBytes      int64
}

// DefaultPolicy returns the default local limits.
func DefaultPolicy() Policy {
	return Policy{
		MaxTotalSize:      int64(2) << 40, // 2 TiB
		MaxFiles:          2_000,
		MaxMediaProbes:    4,
		MediaProbeTimeout: 30 * time.Second,
		AcceptTimeout:     2 * time.Minute,
		MaxYencSamples:    600,
		MaxYencBytes:      512 << 20,
		YencTimeout:       90 * time.Second,
		MaxMediaFiles:     40,
		MaxSidecars:       50,
		MaxSubtitleBytes:  16 << 20,
		MaxNfoBytes:       1 << 20,
	}
}

// MediaProbes returns the media probe cap (at least 1).
func (p Policy) MediaProbes() int { return max(p.MaxMediaProbes, 1) }

// MediaTimeout returns the per-file media probe timeout.
func (p Policy) MediaTimeout() time.Duration {
	if p.MediaProbeTimeout <= 0 {
		return 30 * time.Second
	}
	return p.MediaProbeTimeout
}

// Target is where an accepted bundle goes. It is decided locally by the
// caller, never taken from the bundle.
type Target struct {
	// Dir is the local virtual folder the files go into (for example
	// "movies/Release.Name"). Required.
	Dir string
	// Category is the import category of the queue item or the caller's choice.
	Category string
	// QueueID is the queue item this import belongs to (0: none yet).
	QueueID int64
	// TargetPath is the queue item's forced scan path, kept for the later ARR
	// notification.
	TargetPath string
	// SkipArr is set when the caller never wants the ARRs told.
	SkipArr bool
	// Metadata is the queue metadata JSON of the import (the media ids a
	// Stremio-originated import stores, so the record this node later publishes
	// carries them) and DownloadID its download id. Both are caller-supplied
	// local data, never taken from the bundle; they may be empty.
	Metadata   string
	DownloadID string
	// DirFunc, when set and Dir is empty, chooses the local folder from the
	// bundle's sanitised title (the caller applies its normal folder rules).
	DirFunc func(title string) string
}

// Request is one bundle to accept.
type Request struct {
	Bundle *bundle.Bundle
	Origin Origin
	Target Target
	// ExpectedNZB, when set, is the segment list of the NZB the user supplied
	// (one slice per NZB file, in NZB order). The bundle's own list must equal
	// it.
	ExpectedNZB [][]string
	// ExpectedNZBSizes, when set with ExpectedNZB, is the yEnc-ENCODED size of
	// every segment of the user's NZB (same shape). Encoded sizes run a few
	// percent above decoded ones, so they only bound the bundle's declared
	// decoded sizes; they cannot prove them.
	ExpectedNZBSizes [][]int64
	// ExpectedNZBNames, when set with ExpectedNZB, is the subject file name of
	// every NZB entry ("" when its subject has none). The first sampled
	// article of a table must carry the same yEnc name as its NZB entry, which
	// binds the table to that entry.
	ExpectedNZBNames []string
}

// Result describes an accepted bundle.
type Result struct {
	Title string
	// Dir is the folder the files were written to.
	Dir string
	// Paths are the virtual paths of the files, in bundle order.
	Paths []string
	// Sizes are the file sizes, parallel to Paths.
	Sizes []int64
	// TotalSize is the sum of the file sizes.
	TotalSize int64
	// AlreadyImported is set when every file was already present locally with
	// the same File ID (nothing was written).
	AlreadyImported bool
}

// Counters is a snapshot of the accept counters.
type Counters struct {
	Accepted         uint64
	AlreadyImported  uint64
	Rejected         map[Reason]uint64
	FullCheckPassed  uint64
	FullCheckFailed  uint64
	Announced        uint64
	BadBundleReports uint64
}

type counters struct {
	mu       sync.Mutex
	accepted uint64
	already  uint64
	rejected map[Reason]uint64
	passed   uint64
	failed   uint64
	announce uint64
	reports  uint64
}

func (c *counters) add(f func(*counters)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	f(c)
}

func (c *counters) snapshot() Counters {
	c.mu.Lock()
	defer c.mu.Unlock()
	rej := make(map[Reason]uint64, len(c.rejected))
	for k, v := range c.rejected {
		rej[k] = v
	}
	return Counters{Accepted: c.accepted, AlreadyImported: c.already, Rejected: rej, FullCheckPassed: c.passed,
		FullCheckFailed: c.failed, Announced: c.announce, BadBundleReports: c.reports}
}

// YencSamples returns the sampling bound with its default.
func (p Policy) YencSamples() int {
	return defaultInt(p.MaxYencSamples, DefaultPolicy().MaxYencSamples)
}

// YencBytes is the byte budget of the yEnc sampling.
func (p Policy) YencBytes() int64 {
	if p.MaxYencBytes <= 0 {
		return DefaultPolicy().MaxYencBytes
	}
	return p.MaxYencBytes
}

// YencTimeoutOrDefault is the time budget of the yEnc sampling.
func (p Policy) YencTimeoutOrDefault() time.Duration {
	if p.YencTimeout <= 0 {
		return DefaultPolicy().YencTimeout
	}
	return p.YencTimeout
}
