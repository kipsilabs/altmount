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
// shared outer volume.
func tablesOfMeta(name string, m *metapb.FileMetadata) []sampleTable {
	var out []sampleTable
	add := func(label string, segs []*metapb.SegmentData) {
		t := sampleTable{name: label}
		for _, s := range segs {
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
	}
	add(name, m.SegmentData)
	for j, o := range m.SharedOuterSources {
		add(fmt.Sprintf("%s (volume %d)", name, j+1), o.Segments)
	}
	return out
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
		s := sample{ti, g}
		if _, dup := picked[s]; dup {
			continue
		}
		if bytes+tables[ti].sizes[g] > i.cfg.Policy.YencBytes() {
			break
		}
		picked[s] = struct{}{}
		plan = append(plan, s)
		bytes += tables[ti].sizes[g]
	}
	ctx, cancel := context.WithTimeout(ctx, i.cfg.Policy.YencTimeoutOrDefault())
	defer cancel()
	verdicts := i.runSamples(ctx, tables, plan)
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
