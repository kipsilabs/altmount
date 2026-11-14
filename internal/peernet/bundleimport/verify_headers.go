package bundleimport

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/kipsilabs/altmount/internal/holes"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
)

// Header sample of the forced full check: 1 percent of a file's articles, at
// least 20 and at most 500 (never more than the file has).
const (
	headerSampleMin = 20
	headerSampleMax = 500
)

func headerSampleSize(total int) int {
	return min(total, min(max(total/100, headerSampleMin), headerSampleMax))
}

// tablesOfMeta lists the segment tables a stored file reads: its own and each
// shared outer volume. The stored metadata of a file read from several archive
// volumes is one flat segment list that no longer says where the volumes are, so
// the boundaries are recovered from the ranges the way the builder does (a
// segment that starts inside its article, one that ends before its article does,
// one longer than the one before). Each recovered piece is then checked relative
// to where its first sampled article begins in its posted file; a table with no
// boundary is one posted file and is checked as before.
func tablesOfMeta(name string, m *metapb.FileMetadata) []sampleTable {
	var out []sampleTable
	add := func(label string, segs []*metapb.SegmentData, split bool) {
		var cuts []int
		if split {
			cuts = metaBoundaries(segs)
		}
		from := 0
		for k := 0; k <= len(cuts); k++ {
			to := len(segs)
			if k < len(cuts) {
				to = cuts[k]
			}
			t := sampleTable{name: label, rel: len(cuts) > 0}
			if len(cuts) > 0 {
				t.name = fmt.Sprintf("%s (part %d of %d)", label, k+1, len(cuts)+1)
			}
			for _, s := range segs[from:to] {
				if s == nil || s.Id == "" || holes.IsPlaceholderID(s.Id) {
					return // a table with a hole cannot be described by one file
				}
				t.ids = append(t.ids, s.Id)
				t.sizes = append(t.sizes, s.SegmentSize)
				t.prefix = append(t.prefix, t.total)
				t.total += s.SegmentSize
			}
			if len(t.ids) > 0 {
				out = append(out, t)
			}
			from = to
		}
	}
	add(name, m.SegmentData, true)
	for j, o := range m.SharedOuterSources {
		add(fmt.Sprintf("%s (volume %d)", name, j+1), o.Segments, false)
	}
	return out
}

// metaBoundaries returns the indices at which a flat segment list starts a new
// part.
func metaBoundaries(segs []*metapb.SegmentData) []int {
	var cuts []int
	for i := 1; i < len(segs); i++ {
		a, b := segs[i-1], segs[i]
		if a == nil || b == nil {
			continue
		}
		if b.StartOffset > 0 || a.EndOffset < a.SegmentSize-1 || b.SegmentSize > a.SegmentSize {
			cuts = append(cuts, i)
		}
	}
	return cuts
}

// VerifyPeerHeaders is the optional extra of the forced full check of a pending
// peer import (it implements health.PeerHeaderVerifier). The STAT-only check
// cannot see a size or offset lie, so a further random sample of the file's
// articles, proportional to its size, has its yEnc headers read and compared
// with the recorded layout (part size, part offset, table total). It returns
// nil when the sample agrees (or the file is not a pending peer import), an
// error wrapping health.ErrPeerLayoutMismatch when it contradicts the layout
// (attributable to the publisher), and another error when it could not be
// completed. This is sampling, not proof.
func (i *Importer) VerifyPeerHeaders(ctx context.Context, filePath string) error {
	if i.cfg.State == nil || i.cfg.Headers == nil {
		return nil
	}
	row, ok, err := i.cfg.State.Get(ctx, filePath)
	if err != nil || !ok || row.State != "pending" || i.isForeign(ctx, row) {
		return nil
	}
	m, err := i.cfg.Library.ReadFileMetadata(filePath)
	if err != nil || m == nil {
		return nil
	}
	tables := tablesOfMeta(filePath, m)
	var total int
	for _, t := range tables {
		total += len(t.ids)
	}
	if total == 0 {
		return nil
	}
	var plan []sample
	var bytes int64
	picked := map[sample]struct{}{}
	for want, tries := headerSampleSize(total), 0; len(plan) < want && tries < 20*want; tries++ {
		g := randN(total)
		ti := 0
		for g >= len(tables[ti].ids) {
			g -= len(tables[ti].ids)
			ti++
		}
		s := sample{table: ti, pos: g, must: true}
		if _, dup := picked[s]; dup {
			continue
		}
		if bytes+tables[ti].sizes[g] > fullCheckHeaderBytes {
			break
		}
		picked[s] = struct{}{}
		plan = append(plan, s)
		bytes += tables[ti].sizes[g]
	}
	ctx, cancel := context.WithTimeout(ctx, i.cfg.Policy.YencTimeoutOrDefault())
	defer cancel()
	verdicts := i.runSamples(ctx, tables, plan, newRelState())
	var inconclusive error
	for _, v := range verdicts {
		switch v.reason {
		case "":
		case ReasonYencMismatch:
			i.mismatched.Store(filePath, struct{}{})
			slog.WarnContext(ctx, "peer import: article header contradicts the layout", "file_path", filePath, "detail", v.err)
			return &LayoutMismatchError{Detail: v.err.Error()}
		default:
			if inconclusive == nil {
				inconclusive = v.err
			}
		}
	}
	return inconclusive
}

// takeMismatch reports whether a header sample contradicted the layout of the
// file, and forgets it.
func (i *Importer) takeMismatch(path string) bool {
	_, ok := i.mismatched.LoadAndDelete(path)
	return ok
}

// LayoutMismatchError says the sampled articles contradict the recorded
// layout. The health worker recognises it by PeerLayoutMismatch.
type LayoutMismatchError struct{ Detail string }

func (e *LayoutMismatchError) Error() string {
	return "article headers contradict the imported layout: " + e.Detail
}

// PeerLayoutMismatch marks the error for the health worker.
func (e *LayoutMismatchError) PeerLayoutMismatch() bool { return true }
