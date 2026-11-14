package bundleimport

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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

type sample struct{ table, pos int }

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
	for i := range b.Files {
		f := &b.Files[i]
		add(f.Name, f.Segments, fileinfo.IsVerifiableMediaFile(f.Name))
		for j := range f.Outer {
			add(fmt.Sprintf("%s (volume %d)", f.Name, j+1), f.Outer[j].Segments, false)
		}
	}
	return out
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

// errTooManyTables means the minimum sampling rate does not fit the budget.
var errTooManyTables = errors.New("more segment tables than the sampling budget covers")

// interiorFor returns how many random interior articles a table gets at least
// (one) and in total: tables of media files get one per 500 segments, at least
// three.
func interiorFor(t sampleTable) (minimum, total int) {
	n := len(t.ids) - 2
	if n <= 0 {
		return 0, 0
	}
	total = 1
	if t.main {
		total = max(3, len(t.ids)/500)
	}
	return 1, min(total, n)
}

// planSamples picks, per table, the first and last segment and at least one
// uniformly random interior one (more for the tables of media files), within a
// cap on the number of articles and on the bytes read. A budget that cannot
// cover every table at the minimum rate is an error, never a silent skip.
func planSamples(tables []sampleTable, limit int, maxBytes int64) ([]sample, error) {
	var plan []sample
	var bytes int64
	taken := map[sample]struct{}{}
	push := func(t, pos int) bool {
		s := sample{t, pos}
		if _, dup := taken[s]; dup {
			return false
		}
		taken[s] = struct{}{}
		plan = append(plan, s)
		bytes += tables[t].sizes[pos]
		return true
	}
	pick := func(t, count int) {
		n := len(tables[t].ids) - 2
		for k, tries := 0, 0; k < count && tries < 32*count+32; tries++ {
			if push(t, 1+randN(n)) {
				k++
			}
		}
	}
	extra := make([]int, len(tables))
	for t := range tables {
		n := len(tables[t].ids)
		push(t, 0)
		if n > 1 {
			push(t, n-1)
		}
		lo, hi := interiorFor(tables[t])
		pick(t, lo)
		extra[t] = hi - lo
	}
	if len(plan) > limit || bytes > maxBytes {
		return nil, fmt.Errorf("%w: %d tables need %d articles (%d bytes), budget %d articles and %d bytes",
			errTooManyTables, len(tables), len(plan), bytes, limit, maxBytes)
	}
	order := make([]int, len(tables))
	for i := range order {
		order[i] = i
	}
	shuffle(order)
	for _, t := range order {
		for k := 0; k < extra[t] && len(plan) < limit && bytes+tables[t].sizes[0] <= maxBytes; k++ {
			pick(t, 1)
		}
	}
	return plan, nil
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
func (i *Importer) verifyYenc(ctx context.Context, b *bundle.Bundle, names []string) (reason Reason, attributable bool, err error) {
	if i.cfg.Headers == nil {
		return ReasonProbeInconclusive, false, errors.New("no way to read article headers")
	}
	tables := tablesOf(b, names)
	plan, perr := planSamples(tables, i.cfg.Policy.YencSamples(), i.cfg.Policy.YencBytes())
	if perr != nil {
		return ReasonTooManyTables, false, perr
	}
	ctx, cancel := context.WithTimeout(ctx, i.cfg.Policy.YencTimeoutOrDefault())
	defer cancel()
	results := i.runSamples(ctx, tables, plan)
	var first *sampleVerdict
	for k := range results {
		r := &results[k]
		if r.reason == ReasonYencMismatch {
			return r.reason, r.attributable, r.err // a contradiction outranks a missing article
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
