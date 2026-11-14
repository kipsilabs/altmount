package bundleimport

import (
	"errors"
	"golang.org/x/sync/singleflight"
	"sync"
	"time"
)

// Config holds the collaborators of an Importer. Fetcher, Recorder, Announcer
// and Reporter may be nil: the matching step is then skipped (no fetch, no
// direct import, nothing announced, nothing reported).
type Config struct {
	Library Library
	Stager  Stager
	Prober  ReleaseProber
	State   State
	// Index, when set, receives the verified release identity of the files.
	Index   ContentIndex
	Fetcher Fetcher
	// Headers reads sampled articles' yEnc headers before anything is written.
	Headers       SegmentHeaders
	Recorder      Recorder
	Announcer     Announcer
	Reporter      BadBundleReporter
	HealthEnabled func() bool
	// AllowedExtensions returns the importer's allowed file extensions
	// (config import.allowed_file_extensions); empty allows everything.
	AllowedExtensions func() []string
	// WillBeChecked says whether the health system will ever create a health
	// record (and so a full check) for a written path under the current import
	// strategy; nil means yes for every path.
	WillBeChecked func(path string) bool
	Policy        Policy
	Now           func() time.Time
}

// Importer accepts bundles. It is safe for concurrent use.
type Importer struct {
	cfg      Config
	cnt      counters
	reported map[string]bool
	// reportedMu guards reported.
	reportedMu sync.Mutex
	flight     singleflight.Group

	tmu     sync.Mutex
	targets map[string]*targetLock

	// crashHook and crashed are a test seam: a hook that returns true at a
	// step of write abandons it without any undo, as a process death would.
	crashHook func(step string) bool
	crashed   bool
}

var errCrashed = errors.New("bundleimport: simulated crash")

func (i *Importer) crashAt(step string) bool {
	if i.crashHook == nil || !i.crashHook(step) {
		return false
	}
	i.crashed = true
	return true
}

type targetLock struct {
	mu   sync.Mutex
	refs int
}

// New builds an Importer. A zero Policy means DefaultPolicy.
func New(cfg Config) *Importer {
	if cfg.Policy == (Policy{}) {
		cfg.Policy = DefaultPolicy()
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Importer{cfg: cfg, cnt: counters{rejected: map[Reason]uint64{}}, targets: map[string]*targetLock{}}
}

// Counters returns a snapshot of the counters.
func (i *Importer) Counters() Counters { return i.cnt.snapshot() }
