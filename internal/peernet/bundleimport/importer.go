package bundleimport

import (
	"errors"
	"fmt"
	"golang.org/x/sync/singleflight"
	"strings"
	"sync"
	"time"
)

// Config holds the collaborators of an Importer. Library, Stager, Prober,
// State, Headers and HealthEnabled are required (see Missing; NewChecked
// refuses a Config without them). Index, Fetcher, Recorder, Announcer and
// Reporter may be nil: the matching step is then skipped (nothing indexed, no
// fetch, no direct import, nothing announced, nothing reported).
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

// Missing lists the required collaborators that are nil. Without one the
// accept pipeline cannot work (a nil State or Library panics at first use, a
// nil Headers rejects every bundle at its first sample). The optional ones are
// listed on Config.
func (c Config) Missing() []string {
	var out []string
	add := func(nilDep bool, name string) {
		if nilDep {
			out = append(out, name)
		}
	}
	add(c.Library == nil, "Library")
	add(c.Stager == nil, "Stager")
	add(c.Prober == nil, "Prober")
	add(c.State == nil, "State")
	add(c.Headers == nil, "Headers")
	add(c.HealthEnabled == nil, "HealthEnabled")
	return out
}

// Importer accepts bundles. It is safe for concurrent use.
type Importer struct {
	cfg      Config
	cnt      counters
	reported map[string]bool
	// reportedMu guards reported.
	reportedMu sync.Mutex
	flight     singleflight.Group
	// mismatched holds the paths whose header sample contradicted the layout.
	mismatched sync.Map

	// writeMu: a write holds it shared from its first state row to its commit;
	// Reconcile takes it exclusively, so it can never roll back a live write.
	writeMu sync.RWMutex

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

// NewChecked is New that refuses a Config with a required collaborator
// missing: an importer built without one would panic or reject everything at
// first use.
func NewChecked(cfg Config) (*Importer, error) {
	if missing := cfg.Missing(); len(missing) > 0 {
		return nil, fmt.Errorf("bundleimport: required collaborators missing: %s", strings.Join(missing, ", "))
	}
	return New(cfg), nil
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
