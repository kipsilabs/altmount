package parser

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/javi11/nntppool/v5"
	"github.com/javi11/nzbparser"
	concpool "github.com/sourcegraph/conc/pool"

	"github.com/kipsilabs/altmount/internal/pool"
)

// pastEndStatTimeout is the per-article budget for checking extras, scaled to
// the whole batch by pool.StatManyTimeout.
const pastEndStatTimeout = 10 * time.Second

const (
	pastEndStatAttempts       = 5
	pastEndStatsPerConnection = 2
)

// pastEndRetryBackoff is multiplied by the attempt number between retries.
var pastEndRetryBackoff = time.Second

// pastEndCandidate is a file listing more articles than its size needs.
type pastEndCandidate struct {
	index int // position in the caller's list
	file  *nzbparser.NzbFile
	keep  int
}

// articlesNeeded reports how many articles a file of fileSize needs when it
// lists more than that. ok is false when the sizes are unknown or nothing is extra.
func articlesNeeded(file *nzbparser.NzbFile, partSize, fileSize int64) (keep int, ok bool) {
	if partSize <= 0 || fileSize <= 0 {
		return 0, false
	}
	needed := (fileSize + partSize - 1) / partSize
	if needed < 1 || needed >= int64(len(file.Segments)) {
		return 0, false
	}
	return int(needed), true
}

// confirmDeadPastEnd returns the candidates whose extra articles are all
// confirmed gone. They carry none of the file's bytes, so they are not damage.
//
// The yEnc size alone is not trusted: it can be wrong, and trimming on a wrong
// size would cut real data. An article that is alive, or whose state is
// unknown, leaves its file untouched.
//
// A release can list hundreds of extras and a 430 is slow to answer, so every
// file's extras go out as one batch rather than one at a time.
func (p *Parser) confirmDeadPastEnd(ctx context.Context, cp pool.NntpClient, candidates []pastEndCandidate, articleDate time.Time) []pastEndCandidate {
	var ids []string
	for _, c := range candidates {
		for _, seg := range c.file.Segments[c.keep:] {
			ids = append(ids, seg.ID)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	// A 430 holds its connection's pipeline slot for a while, so a batch sized for
	// a wider pool swamps a small one and most checks expire unanswered.
	cfg := p.getConfig()
	concurrency := min(p.poolManager.StatSweepConcurrency(cfg.StatConcurrency()), max(cfg.TotalProviderConnections()*pastEndStatsPerConnection, 1))
	dead := make(map[string]bool, len(ids))
	alive := make(map[string]bool)
	remaining := ids
	for attempt := 1; attempt <= pastEndStatAttempts && len(remaining) > 0; attempt++ {
		statCtx, cancel := context.WithTimeout(ctx, pool.StatManyTimeout(len(remaining), concurrency, pastEndStatTimeout))
		for result := range cp.ExistsMany(statCtx, remaining, nntppool.ManyOptions{Concurrency: concurrency, ArticleDate: articleDate}) {
			switch {
			case errors.Is(result.Err, nntppool.ErrArticleNotFound):
				dead[result.MessageID] = true
			case result.Err == nil:
				alive[result.MessageID] = true
			}
		}
		cancel()

		// A big batch of slow 430s makes some STATs expire unanswered. That
		// says nothing about the article, so ask again, less hard.
		unanswered := remaining[:0:0]
		for _, id := range remaining {
			if !dead[id] && !alive[id] {
				unanswered = append(unanswered, id)
			}
		}
		remaining = unanswered
		concurrency = max(concurrency/2, 1)
		if len(remaining) > 0 && attempt < pastEndStatAttempts {
			select {
			case <-ctx.Done():
				remaining = nil
			case <-time.After(time.Duration(attempt) * pastEndRetryBackoff):
			}
		}
	}

	confirmed := make([]pastEndCandidate, 0, len(candidates))
	for _, c := range candidates {
		allDead := true
		for _, seg := range c.file.Segments[c.keep:] {
			allDead = allDead && dead[seg.ID]
		}
		if allDead {
			confirmed = append(confirmed, c)
		}
	}
	return confirmed
}

// FindDeadArticlesPastEnd returns, by file index, how many articles to keep for
// each file with dead articles listed past its end. It only reads files, so it
// is safe alongside WarmFirstSegments; apply the result with TrimArticles once
// the warm-up has finished. Each file costs a first-article header fetch, so
// call it only for a release already known to have a missing article.
func (p *Parser) FindDeadArticlesPastEnd(ctx context.Context, files []nzbparser.NzbFile) map[int]int {
	if p.poolManager == nil || !p.poolManager.HasPool() {
		return nil
	}
	cp, err := p.poolManager.GetPool()
	if err != nil {
		return nil
	}
	var mu sync.Mutex
	var candidates []pastEndCandidate
	maxFetch := max(min(min(len(files), p.getConfig().TotalProviderConnections()), maxFetchGoroutines), 1)
	work := concpool.New().WithMaxGoroutines(maxFetch).WithContext(ctx)
	for i := range files {
		file := &files[i]
		if len(file.Segments) < 2 {
			continue
		}
		work.Go(func(ctx context.Context) error {
			head, err := p.fetchYencHeaders(ctx, file.Segments[0], file.Groups, articleDateOf(file))
			if err != nil {
				return nil
			}
			if keep, ok := articlesNeeded(file, int64(head.PartSize), int64(head.FileSize)); ok {
				mu.Lock()
				candidates = append(candidates, pastEndCandidate{index: i, file: file, keep: keep})
				mu.Unlock()
			}
			return nil
		})
	}
	_ = work.Wait()

	found := make(map[int]int)
	for _, c := range p.confirmDeadPastEnd(ctx, cp, candidates, warmReleaseDate(files)) {
		found[c.index] = c.keep
	}
	return found
}

// TrimArticles applies FindDeadArticlesPastEnd's result to files.
func (p *Parser) TrimArticles(ctx context.Context, files []nzbparser.NzbFile, keep map[int]int) {
	for i, n := range keep {
		p.trimArticles(ctx, &files[i], n)
	}
}

func (p *Parser) trimArticles(ctx context.Context, file *nzbparser.NzbFile, keep int) {
	dropped := file.Segments[keep:]
	for _, seg := range dropped {
		file.Bytes -= int64(seg.Bytes)
	}
	p.log.InfoContext(ctx, "Ignoring dead articles listed past the end of a file",
		"file", file.Filename, "kept", keep, "dropped", len(dropped))
	file.Segments = file.Segments[:keep]
}

// trimDeadArticlesPastEnd drops dead past-the-end articles the sampled
// availability check did not happen to hit, before normalization tries to read
// a last article that does not exist. Sizes come from what the parse already
// holds, so this costs nothing unless a file lists too many articles.
func (p *Parser) trimDeadArticlesPastEnd(ctx context.Context, cache []*FirstSegmentData, sizes map[string]firstSegmentYencInfo, articleDate time.Time) {
	if p.poolManager == nil || !p.poolManager.HasPool() {
		return
	}
	cp, err := p.poolManager.GetPool()
	if err != nil {
		return
	}
	var candidates []pastEndCandidate
	for _, data := range cache {
		if data == nil || data.File == nil || len(data.File.Segments) < 2 {
			continue
		}
		info, ok := sizes[data.File.Segments[0].ID]
		if !ok {
			continue
		}
		if keep, ok := articlesNeeded(data.File, int64(info.PartSize), int64(info.FileSize)); ok {
			candidates = append(candidates, pastEndCandidate{file: data.File, keep: keep})
		}
	}
	for _, c := range p.confirmDeadPastEnd(ctx, cp, candidates, articleDate) {
		p.trimArticles(ctx, c.file, c.keep)
	}
}
