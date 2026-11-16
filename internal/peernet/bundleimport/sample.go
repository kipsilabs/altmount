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
	// Prefix is the first bytes of the decoded article (at most
	// headerPrefixCap), when the reader kept them: the archive header walk of a
	// part reads its volume's first article from here instead of fetching it a
	// second time.
	Prefix []byte
	// Bytes is how many decoded bytes the read really delivered (what the
	// download cost), whatever the headers claim; 0 when the reader did not
	// count.
	Bytes int64
}

// headerPrefixCap is how much of an article the header reader keeps.
const headerPrefixCap = 64 << 10

// prefixWriter keeps the first headerPrefixCap bytes written and drops the rest.
type prefixWriter struct {
	buf []byte
	n   int64
}

func (w *prefixWriter) Write(p []byte) (int, error) {
	w.n += int64(len(p))
	if room := headerPrefixCap - len(w.buf); room > 0 {
		w.buf = append(w.buf, p[:min(room, len(p))]...)
	}
	return len(p), nil
}

// SegmentHeaders reads the yEnc headers of one article from the user's own
// providers.
type SegmentHeaders interface {
	SegmentHeader(ctx context.Context, messageID string) (YencHeader, error)
}

type poolHeaders struct{ p pool.Manager }

// NewPoolHeaderReader reads headers through the connection pool. The pool
// cannot stop after the headers: a writer that fails early leaves the rest of the
// body on the wire (the next reply on that connection then desynchronises), and
// cancelling the request makes the connection drain the whole body anyway (it
// only drops the connection when more than 1 MiB remains, which an article of
// about 0.75 MB never does). A read therefore costs the whole article; the sample
// count and bytes are bounded by Policy and charged as actually delivered.
func NewPoolHeaderReader(p pool.Manager) SegmentHeaders { return poolHeaders{p: p} }

func (h poolHeaders) SegmentHeader(ctx context.Context, id string) (YencHeader, error) {
	ac := articleCacheFrom(ctx)
	if a, ok := ac.get(id); ok {
		return YencHeader{Name: a.head.YEnc.FileName, PartBegin: a.head.YEnc.PartBegin, PartSize: a.head.YEnc.PartSize, FileSize: a.head.YEnc.FileSize,
			Prefix: a.body[:min(len(a.body), headerPrefixCap)], Bytes: 0}, nil
	}
	cli, err := h.p.GetPool()
	if err != nil {
		return YencHeader{}, err
	}
	// A token of the pool-wide import budget first, as the importer does for every
	// body it fetches, so reading headers never takes more connections than an
	// import would and shrinks while streams are being played.
	release, err := h.p.AcquireImportConnection(ctx)
	if err != nil {
		return YencHeader{}, err
	}
	defer release()
	w := &prefixWriter{}
	cw := &captureWriter{over: ac == nil}
	body, err := cli.Fetch(ctx, nntppool.Req{MessageID: id, Writer: io.MultiWriter(w, cw)})
	if err != nil {
		return YencHeader{}, err
	}
	if ac != nil && !cw.over && int64(len(cw.buf)) == w.n {
		ac.put(id, *body, cw.buf)
	}
	return YencHeader{Name: body.YEnc.FileName, PartBegin: body.YEnc.PartBegin, PartSize: body.YEnc.PartSize, FileSize: body.YEnc.FileSize, Prefix: w.buf, Bytes: w.n}, nil
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
	// part is set for a table that is one part of a multi-part file. A part
	// that is not a whole posted file (rel) is checked relative to where its
	// first article begins in that file, which the sampled headers reveal; a
	// whole one is checked as before (offset 0, file total = sum of the part).
	part bool
	rel  bool
	// mainPart marks a part of the main media file: its interior samples are
	// planned over all the parts together (planMust).
	mainPart bool
	// file and index place a part table in its bundle file.
	file, index int
	// fileID is the File ID of the file a part table belongs to (the canonical
	// evidence of a disagreement among the part's articles is fileID:index).
	fileID string
	// need0 makes the first article a mandatory sample (the header binding of
	// the part needs where it begins).
	need0 bool
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
	add := func(name string, segs []bundle.Segment, main bool, tag func(*sampleTable)) {
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
		if tag != nil {
			tag(&t)
		}
		out = append(out, t)
	}
	ents := newEntryIndex(b)
	mainFile := mainMediaFile(b)
	for i := range b.Files {
		f := &b.Files[i]
		if len(f.Parts) == 0 {
			add(f.Name, f.Segments, i == mainFile, nil)
		}
		for k, ps := range partsOfSliced(f) {
			add(fmt.Sprintf("%s (part %d of %d)", f.Name, k+1, len(f.Parts)), ps, false, func(t *sampleTable) {
				t.part, t.file, t.index, t.mainPart = true, i, k, i == mainFile
				t.fileID = f.FileID.String()
				t.rel = !ents.wholeYenc(ps)
			})
		}
		for j := range f.Outer {
			add(fmt.Sprintf("%s (volume %d)", f.Name, j+1), f.Outer[j].Segments, i == mainFile, nil)
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

// sampleConcurrency is how many article reads of one sampling run may be in
// flight. The pool reader takes a token of the import connection budget for each,
// so the width that really runs is what that budget allows (it shrinks while
// streams play), never more than the importer itself would use.
const sampleConcurrency = 16

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
		if tables[t].need0 {
			push(t, 0)
		}
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
	planMainParts(tables, push)
	return plan, bytes
}

// planMainParts plans the interior samples of the parts of the main media file as
// if they were one table: one per mediaPerSegments segments of the whole file, at
// least three, drawn uniformly over all their interior articles.
func planMainParts(tables []sampleTable, push func(t, pos int) bool) {
	var at []int // table of every interior position, flattened
	var pos []int
	for t := range tables {
		if !tables[t].mainPart {
			continue
		}
		for p := 1; p < len(tables[t].ids)-1; p++ {
			at, pos = append(at, t), append(pos, p)
		}
	}
	want := min(max(3, (len(at)+2*countParts(tables))/mediaPerSegments), len(at))
	for k, tries := 0, 0; k < want && tries < 32*want+32; tries++ {
		g := randN(len(at))
		if push(at[g], pos[g]) {
			k++
		}
	}
}

func countParts(tables []sampleTable) int {
	n := 0
	for i := range tables {
		if tables[i].mainPart {
			n++
		}
	}
	return n
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

// readableName says a file name looks like one a person or a release tool
// chose: it has an extension and a stem that is not one unbroken run of letters
// and digits of 16 or more (the shape of a random obfuscation name). The subject
// and the yEnc name of an obfuscated post are two unrelated random strings, so
// comparing them says nothing and an honest bundle must not be refused for it.
func readableName(n string) bool {
	dot := strings.LastIndexByte(n, '.')
	if dot <= 0 || dot == len(n)-1 || len(n)-dot > 8 {
		return false
	}
	stem := n[:dot]
	return len(stem) < 16 || strings.ContainsAny(stem, " ._-[]()")
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
	rep.unbound = map[int]string{}
	rep.sizeEquation = map[int]bool{}
	rep.orderRequired = map[int]string{}
	if i.cfg.Headers == nil {
		return rep, ReasonProbeInconclusive, false, errors.New("no way to read article headers")
	}
	if articleCacheFrom(ctx) == nil {
		ctx = withArticleCache(ctx, newArticleCache(articleCacheBytes))
	}
	tables := tablesOf(b, names)
	picks := pickExtents(b)
	partPicks := pickParts(b)
	markNeed0(tables)
	reserve := extentReserve(b, picks) + partReserve(b, partPicks)
	must, mustBytes := planMust(tables)
	limit, ceiling := i.cfg.Policy.YencSamples(), i.cfg.Policy.YencBytes()
	if len(must) > limit || mustBytes+reserve > ceiling {
		return rep, ReasonTooManyTables, false, fmt.Errorf("%w: %d tables need %d articles (%d bytes plus %d for archive headers), ceiling %d articles and %d bytes",
			errTooManyTables, len(tables), len(must), mustBytes, reserve, limit, ceiling)
	}
	bud := newCostBudget(max(yencCap(releaseBytes(tables), ceiling), mustBytes+reserve))
	bud.onProgress = progressFrom(ctx)
	defer func() { rep.articles, rep.bytes, rep.bud = bud.articles(), bud.spent(), bud }()
	lease, lerr := i.cfg.Verify.Begin(verifyClassOf(ctx), bud.limit)
	if lerr != nil {
		return rep, ReasonVerifyBudget, false, lerr
	}
	defer func() {
		if err != nil {
			lease.End(bud.spent())
			return
		}
		// The lease stays open through the media probe, which reads through the
		// same staging area; the accept ends it.
		rep.lease = lease
	}()
	ctx, cancel := context.WithTimeout(ctx, i.cfg.Policy.YencTimeoutOrDefault())
	defer cancel()
	rel := newRelState()
	if reason, attributable, err := judge(i.runSamplesBudget(ctx, tables, must, rel, bud)); err != nil {
		return rep, reason, attributable, err
	}
	if len(picks) > 0 || len(partPicks) > 0 {
		st, r, serr := stage()
		if serr != nil {
			return rep, r, false, serr
		}
		if len(picks) > 0 {
			if rep, reason, attributable, err = i.verifyExtents(ctx, b, st, picks, bud); err != nil {
				return rep, reason, attributable, err
			}
		}
		if reason, attributable, err = i.verifyParts(ctx, b, tables, st, partPicks, rel, bud, &rep); err != nil {
			return rep, reason, attributable, err
		}
	}
	optional := planOptional(tables, must, limit-len(must), bud.left())
	reason, attributable, err = judge(i.runSamplesBudget(ctx, tables, optional, rel, bud))
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
	// hdr is the article's header when it was read.
	hdr YencHeader
	ok  bool
}

// relState collects, across the runs of one accept, what the sampled articles
// of the parts that are slices of a larger posted file say: where the part's
// first article begins in the posted file (every sample implies it) and the
// posted file's total size. Every sample of one part must agree.
type relState struct {
	mu    sync.Mutex
	base  map[int]int64
	total map[int]int64
	// first is the 0-based offset of article 0 of a table, when it was sampled.
	first map[int]int64
	// name is the yEnc file name of article 0 of a table, when it was sampled
	// and the table is a part; prefix is that article's first decoded bytes.
	name   map[int]string
	prefix map[int][]byte
}

func newRelState() *relState {
	return &relState{base: map[int]int64{}, total: map[int]int64{}, first: map[int]int64{},
		name: map[int]string{}, prefix: map[int][]byte{}}
}

// judgeOne compares one sample with what the earlier samples of its part said.
func (st *relState) judgeOne(t *sampleTable, s sample, v *sampleVerdict) {
	if !v.ok || v.reason != "" {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	h := v.hdr
	if s.pos == 0 {
		st.first[s.table] = h.PartBegin
		if t.part {
			st.name[s.table], st.prefix[s.table] = h.Name, h.Prefix
		}
	}
	if !t.rel {
		return
	}
	base := h.PartBegin - t.prefix[s.pos]
	bad := base < 0 || base+t.total > h.FileSize
	if b0, ok := st.base[s.table]; ok && (b0 != base || st.total[s.table] != h.FileSize) {
		bad = true
	}
	if bad {
		*v = sampleVerdict{reason: ReasonYencMismatch, attributable: true, err: evidenceError{fmt.Errorf("%s: article %d begins at %d of %d, which does not fit the other articles of the part (%d bytes in the layout)",
			t.name, s.pos, h.PartBegin, h.FileSize, t.total), partEvidence(t.fileID, t.index, t.key)}}
		return
	}
	st.base[s.table], st.total[s.table] = base, h.FileSize
}

// runSamples reads the planned articles' headers with a budget of its own (the
// forced full check's sample: fullCheckHeaderBytes of really delivered bytes).
func (i *Importer) runSamples(ctx context.Context, tables []sampleTable, plan []sample, rel *relState) []sampleVerdict {
	bud := newCostBudget(fullCheckHeaderBytes)
	defer func() { i.verifyDebit(bud.spent()) }()
	return i.runSamplesBudget(ctx, tables, plan, rel, bud)
}

// runSamplesBudget reads the planned articles' headers (sampleConcurrency at a
// time) and compares each with the layout of its table as soon as it arrives.
// What each read really delivered is charged to bud. The first contradiction, or
// a budget spent by articles larger than declared (which contradicts the
// layout's sizes), ends the run: samples not yet started are never fetched.
func (i *Importer) runSamplesBudget(ctx context.Context, tables []sampleTable, plan []sample, rel *relState, bud *costBudget) []sampleVerdict {
	results := make([]sampleVerdict, len(plan))
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	if rel == nil {
		rel = newRelState()
	}
	ran := make([]bool, len(plan))
	var wg sync.WaitGroup
	sem := make(chan struct{}, sampleConcurrency)
	for k, s := range plan {
		sem <- struct{}{}
		if ctx.Err() != nil {
			<-sem
			break
		}
		ran[k] = true
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			results[k] = i.readSample(ctx, tables[s.table], s, bud)
			rel.judgeOne(&tables[s.table], s, &results[k])
			if results[k].reason == ReasonYencMismatch {
				stop()
			}
		}()
	}
	wg.Wait()
	for k, s := range plan {
		if !ran[k] && s.must {
			// Never started: the run was cut (the contradiction that cut it
			// outranks this) or its time ran out, which a mandatory sample must
			// not pass as an answer.
			results[k] = sampleVerdict{reason: ReasonProbeInconclusive, err: fmt.Errorf("%s: article %d was not read: %w", tables[s.table].name, s.pos, context.Cause(ctx))}
		}
	}
	return results
}

// evidenceError marks an error with what identifies the contradicting piece of
// evidence (an article's message-ID, or a part's table for a disagreement among
// its articles, so one corrupt article is never counted as two).
type evidenceError struct {
	error
	id string
}

func (e evidenceError) Unwrap() error { return e.error }

// evidenceOf returns the evidence key of err. The key is canonical and never
// free text (text can carry numbers parsed from article bytes, so one corrupt
// article could read as two contradictions): a contradicting article's
// message-ID, or fileID:part-index when no single article is at fault. An error
// that names no evidence counts as one anonymous piece, so it can never reach
// the blacklist threshold alone.
func evidenceOf(err error) string {
	var ee evidenceError
	if errors.As(err, &ee) {
		return ee.id
	}
	return "unkeyed"
}

// partEvidence is the evidence key of a disagreement among the articles of one
// part (or of a part against its archive header): the file and the part. A
// table with no file identity falls back to its table key.
func partEvidence(fileID string, index int, key bundle.TableKey) string {
	if fileID == "" {
		return fmt.Sprintf("table:%x", key[:8])
	}
	return fmt.Sprintf("%s:%d", fileID, index)
}

// readSample reads one article's header and judges it against its table.
func (i *Importer) readSample(ctx context.Context, t sampleTable, s sample, bud *costBudget) sampleVerdict {
	h, err := i.cfg.Headers.SegmentHeader(ctx, t.ids[s.pos])
	if err == nil {
		if cerr := bud.chargeActual(h.Bytes); cerr != nil {
			// Only an article that itself delivers more than the layout declares
			// contradicts it. A budget that other reads had spent already is this
			// node's own accounting.
			if h.Bytes > t.sizes[s.pos] {
				return sampleVerdict{reason: ReasonYencMismatch, attributable: true, err: evidenceError{fmt.Errorf("%s: article %d delivered %d bytes, the layout declares %d, and the reads so far used up the verification budget", t.name, s.pos, h.Bytes, t.sizes[s.pos]), t.ids[s.pos]}}
			}
			return sampleVerdict{reason: ReasonVerifyBudget, err: fmt.Errorf("%s: article %d: the verification budget is spent", t.name, s.pos)}
		}
	}
	switch {
	case errors.Is(err, nntppool.ErrArticleNotFound):
		return sampleVerdict{reason: ReasonProbeMissing, err: fmt.Errorf("%s: article %d is missing", t.name, s.pos)}
	case err != nil && !s.must && (errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)):
		// An optional sample that ran out of time is dropped, not a failure.
		return sampleVerdict{}
	case err != nil:
		return sampleVerdict{reason: ReasonProbeInconclusive, err: fmt.Errorf("%s: %w", t.name, err)}
	case t.rel && h.PartSize != t.sizes[s.pos]:
		return sampleVerdict{reason: ReasonYencMismatch, attributable: true, err: evidenceError{fmt.Errorf("%s: article %d says part %d, layout says %d", t.name, s.pos, h.PartSize, t.sizes[s.pos]), t.ids[s.pos]}}
	case !t.rel && (h.PartSize != t.sizes[s.pos] || h.PartBegin != t.prefix[s.pos] || h.FileSize != t.total):
		return sampleVerdict{reason: ReasonYencMismatch, attributable: true, err: evidenceError{fmt.Errorf("%s: article %d says part %d at %d of %d, layout says %d at %d of %d",
			t.name, s.pos, h.PartSize, h.PartBegin, h.FileSize, t.sizes[s.pos], t.prefix[s.pos], t.total), t.ids[s.pos]}}
	case s.pos == 0 && t.nzbName != "" && h.Name != "" && readableName(t.nzbName) && readableName(h.Name) && !strings.EqualFold(h.Name, t.nzbName):
		return sampleVerdict{reason: ReasonYencMismatch, err: fmt.Errorf("%s: first article is named %q, your NZB names this entry %q", t.name, h.Name, t.nzbName)}
	}
	return sampleVerdict{hdr: h, ok: true}
}
