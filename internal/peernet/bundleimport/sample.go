package bundleimport

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"

	"github.com/javi11/nntppool/v5"

	"github.com/kipsilabs/altmount/internal/importer/parser/fileinfo"
	"github.com/kipsilabs/altmount/internal/peernet/bundle"
	"github.com/kipsilabs/altmount/internal/pool"
)

// YencHeader is what an article's yEnc headers say about its place in a file:
// the decoded size of the part, its 0-based offset in the file and the size of
// the whole file.
type YencHeader struct {
	// Name is the file name of the =ybegin header ("" when absent).
	Name      string
	PartBegin int64
	PartSize  int64
	FileSize  int64
}

// SegmentHeaders reads the yEnc headers of one article from the user's own
// providers.
type SegmentHeaders interface {
	SegmentHeader(ctx context.Context, messageID string) (YencHeader, error)
}

type poolHeaders struct{ p pool.Manager }

// NewPoolHeaderReader reads headers through the connection pool. The pool
// cannot stop after the headers, so a read costs the whole article (about
// 0.75 MB); the sample count and bytes are therefore bounded by Policy.
func NewPoolHeaderReader(p pool.Manager) SegmentHeaders { return poolHeaders{p: p} }

func (h poolHeaders) SegmentHeader(ctx context.Context, id string) (YencHeader, error) {
	cli, err := h.p.GetPool()
	if err != nil {
		return YencHeader{}, err
	}
	body, err := cli.Fetch(ctx, nntppool.Req{MessageID: id, Writer: io.Discard})
	if err != nil {
		return YencHeader{}, err
	}
	return YencHeader{Name: body.YEnc.FileName, PartBegin: body.YEnc.PartBegin, PartSize: body.YEnc.PartSize, FileSize: body.YEnc.FileSize}, nil
}

// sampleTable is one segment table read as a whole file or archive volume.
type sampleTable struct {
	name   string
	ids    []string
	sizes  []int64
	prefix []int64 // prefix[i] = sum of sizes[:i]
	total  int64
	key    bundle.TableKey
	// main marks the table of a media file (denser sampling).
	main bool
	// nzbName is the file name the user's NZB subject gives the NZB entry this
	// table reads ("" when unknown); the first article's yEnc name must agree.
	nzbName string
}

// sample is one article to read. A mandatory one must be answered for the
// bundle to be accepted; an optional one that ran out of time is dropped.
type sample struct {
	table, pos int
	must       bool
}

// entryNames maps each pool position of a nzb-scheme bundle to the subject
// file name of its NZB entry ("" when there is none).
func entryNameAt(b *bundle.Bundle, names []string, pos int64) string {
	if len(names) != len(b.ReleaseFiles) {
		return ""
	}
	var at int64
	for e, n := range b.ReleaseFiles {
		if pos < at+int64(n) {
			return names[e]
		}
		at += int64(n)
	}
	return ""
}

// tablesOf lists the file and outer-volume tables of a bundle. A table is
// identified by the hash of its whole reference list (with sizes and ranges),
// so a table shared by several files is sampled once and two different tables
// never collapse because they start with the same article.
func tablesOf(b *bundle.Bundle, names []string) []sampleTable {
	var out []sampleTable
	seen := map[bundle.TableKey]struct{}{}
	add := func(name string, segs []bundle.Segment, main bool) {
		if len(segs) == 0 {
			return
		}
		key := bundle.KeyOf(segs)
		if _, dup := seen[key]; dup {
			return
		}
		seen[key] = struct{}{}
		t := sampleTable{name: name, key: key, main: main, nzbName: entryNameAt(b, names, segs[0].Ref),
			ids: make([]string, len(segs)), sizes: make([]int64, len(segs)), prefix: make([]int64, len(segs))}
		for i, s := range segs {
			t.ids[i], t.sizes[i], t.prefix[i] = b.Pool[s.Ref], s.Size, t.total
			t.total += s.Size
		}
		out = append(out, t)
	}
	mainFile := mainMediaFile(b)
	for i := range b.Files {
		f := &b.Files[i]
		add(f.Name, f.Segments, i == mainFile)
		for j := range f.Outer {
			add(fmt.Sprintf("%s (volume %d)", f.Name, j+1), f.Outer[j].Segments, i == mainFile)
		}
	}
	return out
}

// mainMediaFile is the index of the largest media file of the bundle (the one
// whose tables are always sampled in full), or -1.
func mainMediaFile(b *bundle.Bundle) int {
	best := -1
	for i := range b.Files {
		if fileinfo.IsVerifiableMediaFile(b.Files[i].Name) && (best < 0 || b.Files[i].Size > b.Files[best].Size) {
			best = i
		}
	}
	return best
}

// randN returns a uniform value in [0, n) from the operating system's random
// source, so a publisher cannot predict which articles will be read.
func randN(n int) int {
	if n <= 1 {
		return 0
	}
	var buf [8]byte
	for {
		if _, err := rand.Read(buf[:]); err != nil {
			panic("bundleimport: no system randomness: " + err.Error())
		}
		v := binary.BigEndian.Uint64(buf[:])
		// Rejection sampling keeps the result unbiased.
		if limit := ^uint64(0) - (^uint64(0))%uint64(n); v < limit {
			return int(v % uint64(n))
		}
	}
}

func shuffle[T any](s []T) {
	for i := len(s) - 1; i > 0; i-- {
		j := randN(i + 1)
		s[i], s[j] = s[j], s[i]
	}
}

// errTooManyTables means the mandatory samples do not fit the absolute ceiling.
var errTooManyTables = errors.New("more segment tables than the sampling budget covers")

const (
	// yencFloor is the smallest byte cap of the accept-time sampling.
	yencFloor int64 = 64 << 20
	// yencFractionDiv makes the cap one percent of the release size.
	yencFractionDiv int64 = 100
	// mediaPerSegments: a table of the main media file gets one interior article
	// per this many segments, at least three.
	mediaPerSegments = 500
)

// yencCap is the byte cap of the sampling for a release of the given declared
// size: one percent of it, at least yencFloor and at most the ceiling.
func yencCap(release, ceiling int64) int64 {
	return min(max(release/yencFractionDiv, yencFloor), ceiling)
}

// releaseBytes is the declared size of all tables.
func releaseBytes(tables []sampleTable) int64 {
	var n int64
	for i := range tables {
		n += tables[i].total
	}
	return n
}

// planMust picks the samples that are never skipped: the LAST article of every
// table (a size lie anywhere in a table shifts every later offset, so the last
// article's part offset and the table total expose it), and for the tables of
// the main media file the first article and random interior ones too (one per
// 500 segments, at least three).
func planMust(tables []sampleTable) (plan []sample, bytes int64) {
	taken := map[sample]struct{}{}
	push := func(t, pos int) bool {
		s := sample{table: t, pos: pos, must: true}
		if _, dup := taken[sample{table: t, pos: pos}]; dup {
			return false
		}
		taken[sample{table: t, pos: pos}] = struct{}{}
		plan = append(plan, s)
		bytes += tables[t].sizes[pos]
		return true
	}
	for t := range tables {
		n := len(tables[t].ids)
		if n == 0 {
			continue
		}
		push(t, n-1)
		if !tables[t].main {
			continue
		}
		push(t, 0)
		want := min(max(3, n/mediaPerSegments), max(n-2, 0))
		for k, tries := 0, 0; k < want && tries < 32*want+32; tries++ {
			if push(t, 1+randN(n-2)) {
				k++
			}
		}
	}
	return plan, bytes
}

// planOptional spends what is left of the budget on the first article of
// randomly chosen tables and then on one random interior article of randomly
// chosen tables. taken lists the samples already planned.
func planOptional(tables []sampleTable, taken []sample, maxArticles int, maxBytes int64) []sample {
	have := make(map[sample]struct{}, len(taken))
	for _, s := range taken {
		have[sample{table: s.table, pos: s.pos}] = struct{}{}
	}
	var plan []sample
	var bytes int64
	order := make([]int, len(tables))
	for i := range order {
		order[i] = i
	}
	try := func(t, pos int) bool {
		key := sample{table: t, pos: pos}
		if _, dup := have[key]; dup {
			return true
		}
		if len(plan) >= maxArticles || bytes+tables[t].sizes[pos] > maxBytes {
			return false
		}
		have[key] = struct{}{}
		plan = append(plan, key)
		bytes += tables[t].sizes[pos]
		return true
	}
	order = slices.DeleteFunc(order, func(t int) bool { return tables[t].main }) // already read in full
	shuffle(order)
	for _, t := range order {
		if len(tables[t].ids) > 1 && !try(t, 0) {
			return plan
		}
	}
	shuffle(order)
	for _, t := range order {
		if n := len(tables[t].ids); n > 2 && !try(t, 1+randN(n-2)) {
			return plan
		}
	}
	return plan
}

// planSamples is the whole plan: the mandatory samples, then as much of the
// optional ones as the byte cap (one percent of the release, between 64 MiB
// and the ceiling, raised to fit the mandatory ones) and the article limit
// allow. It fails only when the mandatory samples alone exceed the ceiling or
// the article limit.
func planSamples(tables []sampleTable, limit int, ceiling int64) ([]sample, error) {
	must, mustBytes := planMust(tables)
	if len(must) > limit || mustBytes > ceiling {
		return nil, fmt.Errorf("%w: %d tables need %d articles (%d bytes), ceiling %d articles and %d bytes",
			errTooManyTables, len(tables), len(must), mustBytes, limit, ceiling)
	}
	capBytes := max(yencCap(releaseBytes(tables), ceiling), mustBytes)
	return append(must, planOptional(tables, must, limit-len(must), capBytes-mustBytes)...), nil
}

// SubjectFilename returns the quoted file name of an NZB subject, or "".
func SubjectFilename(subject string) string {
	j := strings.LastIndexByte(subject, '"')
	if j <= 0 {
		return ""
	}
	i := strings.LastIndexByte(subject[:j], '"')
	if i < 0 {
		return ""
	}
	return strings.TrimSpace(subject[i+1 : j])
}

// verifyYenc reads the yEnc headers of sampled articles and requires each to
// agree with the layout: decoded part size equals the declared size, the part
// begins at the cumulative offset the layout implies, and the file total equals
// the sum of the table's sizes. When the user's NZB is present the first
// article's yEnc file name must also agree with the subject file name of the
// NZB entry the table reads. This is sampling, not proof: the bytes between
// sampled articles are unchecked. attributable says the publisher's layout
// contradicts the articles (not merely the local NZB, or a missing article).
//
// It is also the one place the accept-time reads share a budget: the mandatory
// samples (the last article of every table, more for the main media file), then
// the archive-extent header checks, then as many optional samples (first and
// interior articles of other tables, at random) as the remaining budget allows.
// The returned report says which files have extents that could not be bound to
// an archive header.
func (i *Importer) verifyYenc(ctx context.Context, b *bundle.Bundle, names []string, stage stageFunc) (rep extentReport, reason Reason, attributable bool, err error) {
	if i.cfg.Headers == nil {
		return rep, ReasonProbeInconclusive, false, errors.New("no way to read article headers")
	}
	tables := tablesOf(b, names)
	picks := pickExtents(b)
	reserve := extentReserve(b, picks)
	must, mustBytes := planMust(tables)
	limit, ceiling := i.cfg.Policy.YencSamples(), i.cfg.Policy.YencBytes()
	if len(must) > limit || mustBytes+reserve > ceiling {
		return rep, ReasonTooManyTables, false, fmt.Errorf("%w: %d tables need %d articles (%d bytes plus %d for archive headers), ceiling %d articles and %d bytes",
			errTooManyTables, len(tables), len(must), mustBytes, reserve, limit, ceiling)
	}
	bud := newCostBudget(max(yencCap(releaseBytes(tables), ceiling), mustBytes+reserve))
	_ = bud.charge(mustBytes)
	ctx, cancel := context.WithTimeout(ctx, i.cfg.Policy.YencTimeoutOrDefault())
	defer cancel()
	if reason, attributable, err := judge(i.runSamples(ctx, tables, must)); err != nil {
		return rep, reason, attributable, err
	}
	if len(picks) > 0 {
		st, r, serr := stage()
		if serr != nil {
			return rep, r, false, serr
		}
		if rep, reason, attributable, err = i.verifyExtents(ctx, b, st, picks, bud); err != nil {
			return rep, reason, attributable, err
		}
	}
	optional := planOptional(tables, must, limit-len(must), bud.left())
	for _, s := range optional {
		_ = bud.charge(tables[s.table].sizes[s.pos])
	}
	reason, attributable, err = judge(i.runSamples(ctx, tables, optional))
	return rep, reason, attributable, err
}

// judge turns the verdicts of a run into one: a contradiction outranks a
// missing article.
func judge(results []sampleVerdict) (Reason, bool, error) {
	var first *sampleVerdict
	for k := range results {
		r := &results[k]
		if r.reason == ReasonYencMismatch {
			return r.reason, r.attributable, r.err
		}
		if r.reason != "" && first == nil {
			first = r
		}
	}
	if first != nil {
		return first.reason, false, first.err
	}
	return "", false, nil
}

type sampleVerdict struct {
	reason       Reason
	err          error
	attributable bool
}

// runSamples reads the planned articles' headers (four at a time) and compares
// each with the layout of its table.
func (i *Importer) runSamples(ctx context.Context, tables []sampleTable, plan []sample) []sampleVerdict {
	results := make([]sampleVerdict, len(plan))
	var wg sync.WaitGroup
	sem := make(chan struct{}, 4)
	for k, s := range plan {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			t := tables[s.table]
			h, err := i.cfg.Headers.SegmentHeader(ctx, t.ids[s.pos])
			switch {
			case errors.Is(err, nntppool.ErrArticleNotFound):
				results[k] = sampleVerdict{reason: ReasonProbeMissing, err: fmt.Errorf("%s: article %d is missing", t.name, s.pos)}
			case err != nil && !s.must && (errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)):
				// An optional sample that ran out of time is dropped, not a failure.
			case err != nil:
				results[k] = sampleVerdict{reason: ReasonProbeInconclusive, err: fmt.Errorf("%s: %w", t.name, err)}
			case h.PartSize != t.sizes[s.pos] || h.PartBegin != t.prefix[s.pos] || h.FileSize != t.total:
				results[k] = sampleVerdict{reason: ReasonYencMismatch, attributable: true, err: fmt.Errorf("%s: article %d says part %d at %d of %d, layout says %d at %d of %d",
					t.name, s.pos, h.PartSize, h.PartBegin, h.FileSize, t.sizes[s.pos], t.prefix[s.pos], t.total)}
			case s.pos == 0 && t.nzbName != "" && h.Name != "" && !strings.EqualFold(h.Name, t.nzbName):
				results[k] = sampleVerdict{reason: ReasonYencMismatch, err: fmt.Errorf("%s: first article is named %q, your NZB names this entry %q", t.name, h.Name, t.nzbName)}
			}
		}()
	}
	wg.Wait()
	return results
}
