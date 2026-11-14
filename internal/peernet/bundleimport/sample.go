package bundleimport

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/javi11/nntppool/v5"

	"github.com/kipsilabs/altmount/internal/peernet/bundle"
	"github.com/kipsilabs/altmount/internal/pool"
)

// YencHeader is what an article's yEnc headers say about its place in a file:
// the decoded size of the part, its 0-based offset in the file and the size of
// the whole file.
type YencHeader struct {
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
// 0.75 MB); the sample count is therefore bounded by Policy.YencSamples.
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
	return YencHeader{PartBegin: body.YEnc.PartBegin, PartSize: body.YEnc.PartSize, FileSize: body.YEnc.FileSize}, nil
}

// sampleTable is one segment table read as a whole file or archive volume.
type sampleTable struct {
	name   string
	ids    []string
	sizes  []int64
	prefix []int64 // prefix[i] = sum of sizes[:i]
	total  int64
}

type sample struct{ table, pos int }

// tablesOf lists the file and outer-volume tables of a bundle, each outer
// volume once even when several files share it.
func tablesOf(b *bundle.Bundle) []sampleTable {
	var out []sampleTable
	seen := map[string]struct{}{}
	add := func(name string, segs []bundle.Segment) {
		if len(segs) == 0 {
			return
		}
		first := b.Pool[segs[0].Ref]
		if _, dup := seen[first]; dup {
			return
		}
		seen[first] = struct{}{}
		t := sampleTable{name: name, ids: make([]string, len(segs)), sizes: make([]int64, len(segs)), prefix: make([]int64, len(segs))}
		for i, s := range segs {
			t.ids[i], t.sizes[i], t.prefix[i] = b.Pool[s.Ref], s.Size, t.total
			t.total += s.Size
		}
		out = append(out, t)
	}
	for i := range b.Files {
		f := &b.Files[i]
		add(f.Name, f.Segments)
		for j := range f.Outer {
			add(fmt.Sprintf("%s (volume %d)", f.Name, j+1), f.Outer[j].Segments)
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

// planSamples picks, per table, the first and last segment and perRandom
// random ones in between, within a total cap. When the cap is smaller than the
// number of tables, a random subset of tables is read.
func planSamples(tables []sampleTable, perRandom, limit int) []sample {
	order := make([]int, len(tables))
	for i := range order {
		order[i] = i
	}
	shuffle(order)
	var plan []sample
	taken := map[sample]struct{}{}
	push := func(t, pos int) {
		s := sample{t, pos}
		if _, dup := taken[s]; dup || len(plan) >= limit {
			return
		}
		taken[s] = struct{}{}
		plan = append(plan, s)
	}
	for _, t := range order {
		n := len(tables[t].ids)
		push(t, 0)
		if n > 1 {
			push(t, n-1)
		}
	}
	for _, t := range order {
		n := len(tables[t].ids)
		for k, tries := 0, 0; k < perRandom && n > 2 && tries < 16*perRandom; tries++ {
			before := len(plan)
			push(t, 1+randN(n-2))
			if len(plan) > before {
				k++
			}
		}
	}
	return plan
}

// verifyYenc reads the yEnc headers of sampled articles and requires each to
// agree with the layout: decoded part size equals the declared size, the part
// begins at the cumulative offset the layout implies, and the file total equals
// the sum of the table's sizes. A mismatch is the publisher's doing.
func (i *Importer) verifyYenc(ctx context.Context, b *bundle.Bundle) (Reason, error) {
	if i.cfg.Headers == nil {
		return ReasonProbeInconclusive, errors.New("no way to read article headers")
	}
	tables := tablesOf(b)
	plan := planSamples(tables, i.cfg.Policy.YencRandom(), i.cfg.Policy.YencSamples())
	type verdict struct {
		reason Reason
		err    error
	}
	results := make([]verdict, len(plan))
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
				results[k] = verdict{ReasonProbeMissing, fmt.Errorf("%s: article %d is missing", t.name, s.pos)}
			case err != nil:
				results[k] = verdict{ReasonProbeInconclusive, fmt.Errorf("%s: %w", t.name, err)}
			case h.PartSize != t.sizes[s.pos] || h.PartBegin != t.prefix[s.pos] || h.FileSize != t.total:
				results[k] = verdict{ReasonYencMismatch, fmt.Errorf("%s: article %d says part %d at %d of %d, layout says %d at %d of %d",
					t.name, s.pos, h.PartSize, h.PartBegin, h.FileSize, t.sizes[s.pos], t.prefix[s.pos], t.total)}
			}
		}()
	}
	wg.Wait()
	var first *verdict
	for k := range results {
		r := &results[k]
		if r.reason == ReasonYencMismatch {
			return r.reason, r.err // a contradiction outranks a missing article
		}
		if r.reason != "" && first == nil {
			first = r
		}
	}
	if first != nil {
		return first.reason, first.err
	}
	return "", nil
}
