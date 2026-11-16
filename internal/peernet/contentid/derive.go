package contentid

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/kipsilabs/altmount/internal/database"
)

// Defaults of the derive pass. The pass is a keyset scan over the group index:
// memory is bounded by deriveRowBatch rows (a single group may exceed it, up to
// MaxDerivedReleaseFiles), and every write is a short transaction of at most one
// batch.
const (
	defaultDeriveGroups = 500
	defaultDeriveRows   = 5000
	defaultMaxDirty     = 50000
	maxPendingOrphans   = 50000
	maxDirRefs          = 20000
	progressEveryGroups = 20000
)

// DeriveResult summarises one derive pass.
type DeriveResult struct {
	Groups        int // groups examined
	Assigned      int // rows whose derived ID was set or changed
	Cleared       int // rows that lost a derived ID (group over the cap)
	OversizeFiles int // files in groups over the cap (no ID)
}

// deriveState is the incremental bookkeeping of the derive pass: which groups
// changed since the last pass, which derived IDs may have been superseded, and
// the knobs.
type deriveState struct {
	run sync.Mutex // one pass at a time

	mu      sync.Mutex
	dirty   map[string]struct{}
	full    bool
	orphans map[string]struct{}

	maxGroup   int
	groupBatch int
	rowBatch   int
	maxDirty   int
	onDirty    func()
	onOrphans  func(ctx context.Context, releaseIDs []string)

	oversize atomic.Int64
}

func newDeriveState() *deriveState {
	return &deriveState{
		dirty: map[string]struct{}{}, orphans: map[string]struct{}{},
		maxGroup: MaxDerivedReleaseFiles, groupBatch: defaultDeriveGroups, rowBatch: defaultDeriveRows, maxDirty: defaultMaxDirty,
	}
}

// WithMaxGroupFiles lowers the file-count cap of a derived release (it can
// never exceed MaxDerivedReleaseFiles).
func WithMaxGroupFiles(n int) Option {
	return func(i *Indexer) {
		if n > 0 && n <= MaxDerivedReleaseFiles {
			i.derive.maxGroup = n
		}
	}
}

// WithOrphanHandler registers the callback that receives derived release IDs
// that no indexed file carries any more (their group changed or vanished), so
// the records published under them can be withdrawn. It runs on the goroutine
// of the pass and must return quickly.
func WithOrphanHandler(fn func(ctx context.Context, releaseIDs []string)) Option {
	return func(i *Indexer) { i.derive.onOrphans = fn }
}

// WithDirtyNotify registers a non-blocking callback run when a group is marked
// for re-derivation, so an owner can schedule DeriveDirty (debounced).
func WithDirtyNotify(fn func()) Option { return func(i *Indexer) { i.derive.onDirty = fn } }

// WithDeriveBatch sets how many groups one derive page lists and how many rows
// one members read may hold. Values below 1 are ignored.
func WithDeriveBatch(groups, rows int) Option {
	return func(i *Indexer) {
		if groups > 0 {
			i.derive.groupBatch = groups
		}
		if rows > 0 {
			i.derive.rowBatch = rows
		}
	}
}

// WithMaxDirty bounds the number of remembered dirty groups; beyond it the
// next DeriveDirty does a full pass instead.
func WithMaxDirty(n int) Option {
	return func(i *Indexer) {
		if n > 0 {
			i.derive.maxDirty = n
		}
	}
}

// OversizeFiles is the number of files that the last full pass left without a
// release ID because their group is over the cap.
func (i *Indexer) OversizeFiles() int { return int(i.derive.oversize.Load()) }

// HasDirty reports whether a DeriveDirty pass has work.
func (i *Indexer) HasDirty() bool {
	d := i.derive
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.full || len(d.dirty) > 0 || len(d.orphans) > 0
}

func (d *deriveState) markDirty(key string) {
	if key == "" {
		return
	}
	d.mu.Lock()
	switch {
	case d.full:
	case len(d.dirty) >= d.maxDirty:
		d.full, d.dirty = true, map[string]struct{}{}
	default:
		d.dirty[key] = struct{}{}
	}
	notify := d.onDirty
	d.mu.Unlock()
	if notify != nil {
		notify()
	}
}

func (d *deriveState) addOrphan(id string) {
	if id == "" {
		return
	}
	d.mu.Lock()
	if len(d.orphans) < maxPendingOrphans {
		d.orphans[id] = struct{}{}
	}
	d.mu.Unlock()
}

// noteChanged records that old (the zero row when there was none) was replaced
// by e.
func (i *Indexer) noteChanged(old, e database.PeernetContentEntry) {
	d := i.derive
	if old.FilePath != "" {
		if old.ReleaseScheme == database.PeernetSchemeFiles {
			d.addOrphan(old.ReleaseID)
		}
		d.markDirty(old.GroupKey)
	}
	if e.ReleaseScheme != database.PeernetSchemeNZB {
		d.markDirty(e.GroupKey)
	}
}

// noteRemoved records that a row with this identity left the index.
func (i *Indexer) noteRemoved(ref database.PeernetContentRef) {
	d := i.derive
	if ref.ReleaseScheme == database.PeernetSchemeFiles {
		d.addOrphan(ref.ReleaseID)
	}
	d.markDirty(ref.GroupKey)
}

// markAllDirty asks for a full pass.
func (i *Indexer) markAllDirty() {
	d := i.derive
	d.mu.Lock()
	d.full, d.dirty = true, map[string]struct{}{}
	notify := d.onDirty
	d.mu.Unlock()
	if notify != nil {
		notify()
	}
}

func (d *deriveState) take() (keys []string, full bool, orphans []string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for k := range d.dirty {
		keys = append(keys, k)
	}
	for id := range d.orphans {
		orphans = append(orphans, id)
	}
	full = d.full
	d.dirty, d.orphans, d.full = map[string]struct{}{}, map[string]struct{}{}, false
	sort.Strings(keys)
	return keys, full, orphans
}

func (d *deriveState) putBack(keys []string, full bool, orphans []string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.full = d.full || full
	for _, k := range keys {
		if len(d.dirty) < d.maxDirty {
			d.dirty[k] = struct{}{}
		} else {
			d.full = true
		}
	}
	for _, id := range orphans {
		if len(d.orphans) < maxPendingOrphans {
			d.orphans[id] = struct{}{}
		}
	}
}

// DeriveDirty re-derives only the groups that changed since the last pass (the
// observer and reconcile mark them) and reports superseded IDs. With nothing
// dirty it does no database work. If too many groups changed, or a directory
// removal was too large to track, it runs a full pass instead.
func (i *Indexer) DeriveDirty(ctx context.Context) (DeriveResult, error) {
	d := i.derive
	d.run.Lock()
	defer d.run.Unlock()
	keys, full, orphans := d.take()
	if len(keys) == 0 && !full && len(orphans) == 0 {
		return DeriveResult{}, nil
	}
	cands := map[string]struct{}{}
	for _, id := range orphans {
		cands[id] = struct{}{}
	}
	var res DeriveResult
	var err error
	if full {
		res, err = i.deriveAll(ctx, cands)
		if err != nil {
			d.putBack(keys, true, nil) // the pass did not finish: all of it is still owed
		}
	} else {
		done := 0
		for done < len(keys) && err == nil {
			if err = ctx.Err(); err != nil {
				break
			}
			end := min(done+d.groupBatch, len(keys))
			var counts map[string]int
			if counts, err = i.repo.GroupCounts(ctx, keys[done:end]); err != nil {
				err = fmt.Errorf("count dirty groups: %w", err)
				break
			}
			err = i.applyGroups(ctx, keys[done:end], counts, &res, cands)
			if err == nil {
				done = end
			}
		}
		if err != nil {
			d.putBack(keys[done:], false, nil)
		}
	}
	if ferr := i.flushOrphans(ctx, cands); ferr != nil && err == nil {
		err = ferr
	}
	return res, err
}

// DeriveAll walks every import group of the index once and gives each group
// its derived Release ID (see ReleaseIDFromFiles). It reads only the index
// table: no metadata and no store is read.
//
// Cost: one keyset page of group counts per deriveGroupBatch groups and one
// members read per deriveRowBatch rows, so O(files) rows read in total and
// O(1) pages of memory; writes touch only rows whose ID changes, each batch in
// one short transaction. It can be cancelled between batches and run again: a
// second pass over an unchanged index writes nothing.
func (i *Indexer) DeriveAll(ctx context.Context) (DeriveResult, error) {
	d := i.derive
	d.run.Lock()
	defer d.run.Unlock()
	// A full pass covers everything that was dirty before it started.
	keys, full, orphans := d.take()
	cands := map[string]struct{}{}
	for _, id := range orphans {
		cands[id] = struct{}{}
	}
	res, err := i.deriveAll(ctx, cands)
	if err != nil {
		d.putBack(keys, full, nil) // unfinished: what it was to cover is still owed
	}
	if ferr := i.flushOrphans(ctx, cands); ferr != nil && err == nil {
		err = ferr
	}
	return res, err
}

func (i *Indexer) deriveAll(ctx context.Context, cands map[string]struct{}) (DeriveResult, error) {
	d := i.derive
	var res DeriveResult
	after := ""
	nextLog := progressEveryGroups
	for {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		page, err := i.repo.GroupSummaries(ctx, after, d.groupBatch)
		if err != nil {
			return res, fmt.Errorf("list groups: %w", err)
		}
		if len(page) == 0 {
			break
		}
		after = page[len(page)-1].Key
		keys := make([]string, len(page))
		counts := make(map[string]int, len(page))
		for j, g := range page {
			keys[j], counts[g.Key] = g.Key, g.Files
		}
		if err := i.applyGroups(ctx, keys, counts, &res, cands); err != nil {
			return res, err
		}
		if res.Groups >= nextLog {
			slog.InfoContext(ctx, "Peernet library release id derivation progress", "groups", res.Groups, "assigned", res.Assigned)
			nextLog = res.Groups + progressEveryGroups
		}
	}
	d.oversize.Store(int64(res.OversizeFiles))
	if res.Assigned > 0 || res.Cleared > 0 {
		slog.InfoContext(ctx, "Peernet library release ids derived",
			"groups", res.Groups, "assigned", res.Assigned, "cleared", res.Cleared, "oversize_files", res.OversizeFiles)
	}
	return res, nil
}

// applyGroups derives the groups in keys (counts says how many files each has;
// an absent or zero count is an empty group). Superseded derived IDs are added
// to cands.
func (i *Indexer) applyGroups(ctx context.Context, keys []string, counts map[string]int, res *DeriveResult, cands map[string]struct{}) error {
	d := i.derive
	var batch []string
	rows := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		err := i.deriveBatch(ctx, batch, res, cands)
		batch, rows = batch[:0], 0
		return err
	}
	for _, k := range keys {
		n := counts[k]
		switch {
		case n == 0:
			continue
		case n > d.maxGroup:
			res.Groups++
			res.OversizeFiles += n
			old, err := i.repo.ClearDerivedGroup(ctx, k, 0)
			for _, id := range old {
				cands[id] = struct{}{}
			}
			if err != nil {
				return fmt.Errorf("clear oversize group: %w", err)
			}
			continue
		}
		if rows+n > d.rowBatch && len(batch) > 0 {
			if err := flush(); err != nil {
				return err
			}
		}
		batch = append(batch, k)
		rows += n
	}
	return flush()
}

// deriveBatch reads the members of the given groups and writes the IDs.
func (i *Indexer) deriveBatch(ctx context.Context, keys []string, res *DeriveResult, cands map[string]struct{}) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	members, err := i.repo.GroupMembers(ctx, keys)
	if err != nil {
		return fmt.Errorf("read group members: %w", err)
	}
	var updates []database.PeernetDerivedUpdate
	for start := 0; start < len(members); {
		end := start
		for end < len(members) && members[end].GroupKey == members[start].GroupKey {
			end++
		}
		group := members[start:end]
		start = end
		res.Groups++
		ids := make([]ID, 0, len(group))
		ok := true
		for _, m := range group {
			id, err := ParseID(m.FileID)
			if err != nil {
				ok = false
				break
			}
			ids = append(ids, id)
		}
		if !ok {
			continue
		}
		rid, err := ReleaseIDFromFiles(ids)
		if err != nil {
			continue // empty or over the cap: no ID; the caller routes those groups elsewhere
		}
		want := rid.String()
		for _, m := range group {
			if m.ReleaseScheme == database.PeernetSchemeFiles && m.ReleaseID != want {
				cands[m.ReleaseID] = struct{}{}
			}
			if m.ReleaseID != want || m.ReleaseScheme != database.PeernetSchemeFiles {
				updates = append(updates, database.PeernetDerivedUpdate{
					FilePath: m.FilePath, FileID: m.FileID, GroupKey: m.GroupKey, ReleaseID: want})
			}
		}
	}
	if len(updates) == 0 {
		return nil
	}
	n, err := i.repo.ApplyDerived(ctx, updates)
	if err != nil {
		return fmt.Errorf("write derived release ids: %w", err)
	}
	res.Assigned += n
	return nil
}

// flushOrphans hands the superseded IDs that no indexed file carries any more
// to the orphan handler. It runs even when the pass was cancelled.
func (i *Indexer) flushOrphans(ctx context.Context, cands map[string]struct{}) error {
	d := i.derive
	if len(cands) == 0 {
		return nil
	}
	ids := make([]string, 0, len(cands))
	for id := range cands {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	ctx = context.WithoutCancel(ctx)
	live, err := i.repo.StillIndexed(ctx, ids)
	if err != nil {
		d.putBack(nil, false, ids)
		return fmt.Errorf("check superseded release ids: %w", err)
	}
	var dead []string
	for _, id := range ids {
		if !live[id] {
			dead = append(dead, id)
		}
	}
	if len(dead) > 0 && d.onOrphans != nil {
		d.onOrphans(ctx, dead)
	}
	return nil
}
