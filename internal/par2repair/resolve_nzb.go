package par2repair

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/javi11/nntppool/v5"
	"github.com/javi11/nzbparser"

	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
)

// ResolveFromNzb builds a repair plan straight from a parsed NZB, for releases
// that were never imported — an archive set dropped at import because its
// volumes had missing articles is exactly that case, and it is where PAR2
// repair is most valuable.
//
// The output is identical in shape to Resolve's, so the planner, solver, job
// and patch store are shared; only the source of the file/segment layout
// differs. Patches are keyed by message ID, so a patch written here is the
// same one the streaming read path uses after the release imports.
func ResolveFromNzb(
	ctx context.Context,
	n *nzbparser.Nzb,
	deadSegmentIDs []string,
	fetch ArticleFetcher,
	caps Caps,
	log *slog.Logger,
	progress JobProgress,
) (*Resolution, error) {
	if n == nil || len(n.Files) == 0 {
		return nil, fmt.Errorf("%w: empty NZB", ErrUnrepairable)
	}

	// Split the NZB into PAR2 files and candidate content files.
	var par2Entries, contentEntries []nzbparser.NzbFile
	for _, f := range n.Files {
		if isPar2Filename(nzbFileName(f)) {
			par2Entries = append(par2Entries, f)
		} else {
			contentEntries = append(contentEntries, f)
		}
	}
	if len(par2Entries) == 0 {
		return nil, fmt.Errorf("%w: NZB carries no PAR2 files", ErrUnrepairable)
	}
	if len(contentEntries) == 0 {
		return nil, fmt.Errorf("%w: NZB carries no content files", ErrUnrepairable)
	}

	// Smallest PAR2 file first so the index (which carries Main/FileDesc/IFSC)
	// is parsed before the recovery volumes.
	sort.Slice(par2Entries, func(i, j int) bool {
		return nzbFileBytes(par2Entries[i]) < nzbFileBytes(par2Entries[j])
	})

	cache := newArticleCache(resolveCacheCap)
	var par2Files []SetFile
	for _, f := range par2Entries {
		par2Files = append(par2Files, nzbEntryToSetFile(f, nil))
	}

	// Match recovery-set members to NZB content entries, reusing the shared
	// matcher by presenting the entries as NzbStore rows.
	store := &metapb.NzbStore{}
	for _, f := range contentEntries {
		entry := &metapb.NzbFileEntry{Subject: nzbSubjectFor(f)}
		for _, s := range f.Segments {
			entry.Segments = append(entry.Segments, &metapb.NzbSeg{
				Id:     s.ID,
				Number: int32(s.Number),
				Bytes:  int64(s.Bytes),
			})
		}
		store.Files = append(store.Files, entry)
	}

	dead := map[string]bool{}
	for _, id := range deadSegmentIDs {
		if id != "" {
			dead[normalizeMsgID(id)] = true
		}
	}
	if err := releaseSizePrecheck(store.Files, par2Files, caps); err != nil {
		return nil, err
	}

	started := time.Now()
	// store carries only content entries here, so nothing to exclude.
	hidden, err := statSweepBudgeted(ctx, fetch, releaseArticleIDs(store, par2Files), dead,
		newDamageBudget(store.Files, nil, caps), progress)
	if err != nil {
		return nil, err
	}
	caps.ExpectedHiddenArticles = hidden
	log.InfoContext(ctx, "PAR2 repair liveness check complete",
		"dead_articles", len(dead), "hidden_estimate", hidden, "duration", time.Since(started).Round(time.Millisecond))
	// store carries only content entries here, so nothing to exclude.
	if err := ratioPrecheck(store.Files, nil, dead, caps); err != nil {
		return nil, err
	}

	// The NZB declares yEnc-ENCODED segment sizes — a few percent above the
	// decoded payloads. Fine for thresholds, fatal for the byte-exact stream
	// offsets the PAR2 parser and the recovery payload reads rely on: every
	// article boundary would drift and every recovery payload would fail its
	// packet MD5. Content members are sized during matching (sizeArticles);
	// the PAR2 files themselves are sized here.
	if err := sizePar2SetFiles(ctx, fetch, par2Files, dead, cache, log); err != nil {
		return nil, err
	}

	idx, files, err := planSet(ctx, fetch, store, par2Files, dead, cache, log, progress)
	if err != nil {
		return nil, err
	}

	plan, err := BuildPlan(idx, files, caps)
	if err != nil {
		return nil, err
	}
	log.InfoContext(ctx, "PAR2 repair plan built",
		"missing_slices", len(plan.Missing), "recovery_rows", len(plan.Recovery),
		"spares", len(plan.SpareRecovery), "spill_to_disk", plan.SpillToDisk)
	return &Resolution{Plan: plan, Index: idx, Par2Files: par2Files}, nil
}

// sizePar2SetFiles replaces the PAR2 files' declared (encoded) article sizes
// with decoded ones, probed from the articles themselves: usenet posts split
// each file at one uniform decoded part size, so one live non-final article
// gives every middle article's size and the final article is probed directly.
// The probed payloads land in the shared cache, so the parse that follows
// reuses them instead of refetching.
//
// Dead articles cannot be probed. They read as zeros either way (the parser
// drops whatever packets they damaged), so a file whose probes are all dead
// keeps its encoded sizes; only its own packets are affected, never the
// stream offsets of sibling PAR2 files.
func sizePar2SetFiles(
	ctx context.Context,
	fetch ArticleFetcher,
	files []SetFile,
	dead map[string]bool,
	cache *articleCache,
	log *slog.Logger,
) error {
	probe := func(id string) (int64, error) {
		payload, err := fetchCached(ctx, fetch, id, cache)
		if err != nil {
			if errors.Is(err, nntppool.ErrArticleNotFound) {
				dead[id] = true
				return -1, nil
			}
			return -1, fmt.Errorf("par2repair: probe par2 article %s: %w", id, err)
		}
		return int64(len(payload)), nil
	}

	// Warm every probe target up front: the sizing loop below is sequential
	// and would otherwise pay one fetch round-trip per file per probe.
	for i := range files {
		if n := len(files[i].Articles); n > 0 {
			for _, a := range []Article{files[i].Articles[0], files[i].Articles[n-1]} {
				if !dead[a.MessageID] {
					cache.warm(ctx, fetch, a.MessageID)
				}
			}
		}
	}

	for i := range files {
		f := &files[i]
		n := len(f.Articles)
		if n == 0 {
			continue
		}

		// Final article first: its decoded size is not uniform.
		lastSize := int64(-1)
		if last := f.Articles[n-1]; !dead[last.MessageID] {
			var err error
			if lastSize, err = probe(last.MessageID); err != nil {
				return err
			}
		}

		partSize := int64(-1)
		for j := 0; j < n-1 && partSize < 0; j++ {
			if a := f.Articles[j]; !dead[a.MessageID] {
				var err error
				if partSize, err = probe(a.MessageID); err != nil {
					return err
				}
			}
		}

		if n > 1 && partSize < 0 {
			log.WarnContext(ctx, "PAR2 file's non-final articles are all dead; keeping encoded sizes",
				"first_article", f.Articles[0].MessageID)
			continue
		}
		var total int64
		for j := range f.Articles {
			switch {
			case j < n-1:
				f.Articles[j].Size = partSize
			case lastSize >= 0:
				f.Articles[j].Size = lastSize
				// else: dead final article keeps its encoded size; it reads as
				// zeros and only its own trailing packets are lost.
			}
			total += f.Articles[j].Size
		}
		f.Length = uint64(total)
	}
	return nil
}

// nzbEntryToSetFile converts one NZB file entry into a SetFile. dead may be nil.
func nzbEntryToSetFile(f nzbparser.NzbFile, dead map[string]bool) SetFile {
	sf := SetFile{}
	var total int64
	for _, s := range f.Segments {
		id := normalizeMsgID(s.ID)
		sf.Articles = append(sf.Articles, Article{
			MessageID: id,
			Size:      int64(s.Bytes),
			Dead:      dead[id],
		})
		total += int64(s.Bytes)
	}
	sf.Length = uint64(total)
	return sf
}

// nzbFileName returns the entry's filename, falling back to the subject when
// the parser could not extract one (fully obfuscated posts).
func nzbFileName(f nzbparser.NzbFile) string {
	if f.Filename != "" {
		return f.Filename
	}
	return subjectFilename(f.Subject)
}

// nzbSubjectFor returns a subject that carries the filename, so the shared
// matcher's filename pass can work even when the original subject is
// obfuscated but the parser recovered a name.
func nzbSubjectFor(f nzbparser.NzbFile) string {
	if f.Filename != "" && !strings.Contains(f.Subject, f.Filename) {
		return f.Subject + " " + f.Filename
	}
	return f.Subject
}

func nzbFileBytes(f nzbparser.NzbFile) int64 {
	if f.Bytes > 0 {
		return f.Bytes
	}
	var total int64
	for _, s := range f.Segments {
		total += int64(s.Bytes)
	}
	return total
}

// par2FilenamePattern matches .par2 and .volNNN+NNN.par2 names.
var par2FilenamePattern = regexp.MustCompile(`(?i)\.par2$`)

// isPar2Filename reports whether a filename names a PAR2 file.
func isPar2Filename(name string) bool {
	return par2FilenamePattern.MatchString(strings.TrimSpace(name))
}

// subjectQuotedName extracts a "quoted filename" from a usenet subject.
var subjectQuotedName = regexp.MustCompile(`"([^"]+)"`)

// subjectFilename returns the quoted filename in a subject, or the raw subject
// when it carries no quoted name.
func subjectFilename(subject string) string {
	if m := subjectQuotedName.FindStringSubmatch(subject); len(m) == 2 {
		return m[1]
	}
	return subject
}
