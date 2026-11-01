package parser

import (
	"context"
	"errors"
	"sync"

	"github.com/javi11/nntppool/v5"
	"github.com/javi11/nzbparser"
	concpool "github.com/sourcegraph/conc/pool"

	"github.com/kipsilabs/altmount/internal/pool"
)

// deadArticlesPastEnd reports how many leading articles of file to keep when
// it lists articles past the end of the file and every one of those is
// confirmed gone. They carry none of the file's bytes, so they are not damage.
//
// The yEnc size alone is not trusted: it can be wrong, and trimming on a wrong
// size would cut real data. An article that is alive, or whose state is
// unknown, leaves the file untouched.
func deadArticlesPastEnd(ctx context.Context, cp pool.NntpClient, file *nzbparser.NzbFile, partSize, fileSize int64) (keep int, ok bool) {
	if partSize <= 0 || fileSize <= 0 {
		return 0, false
	}
	needed := (fileSize + partSize - 1) / partSize
	if needed < 1 || needed >= int64(len(file.Segments)) {
		return 0, false
	}
	at := articleDateOf(file)
	for _, seg := range file.Segments[needed:] {
		_, err := cp.Exists(ctx, nntppool.Req{MessageID: seg.ID, ArticleDate: at})
		if !errors.Is(err, nntppool.ErrArticleNotFound) {
			return 0, false
		}
	}
	return int(needed), true
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
	found := make(map[int]int)
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
			if keep, ok := deadArticlesPastEnd(ctx, cp, file, int64(head.PartSize), int64(head.FileSize)); ok {
				mu.Lock()
				found[i] = keep
				mu.Unlock()
			}
			return nil
		})
	}
	_ = work.Wait()
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
func (p *Parser) trimDeadArticlesPastEnd(ctx context.Context, cache []*FirstSegmentData, sizes map[string]firstSegmentYencInfo) {
	if p.poolManager == nil || !p.poolManager.HasPool() {
		return
	}
	cp, err := p.poolManager.GetPool()
	if err != nil {
		return
	}
	for _, data := range cache {
		if data == nil || data.File == nil || len(data.File.Segments) < 2 {
			continue
		}
		info, ok := sizes[data.File.Segments[0].ID]
		if !ok {
			continue
		}
		if keep, ok := deadArticlesPastEnd(ctx, cp, data.File, int64(info.PartSize), int64(info.FileSize)); ok {
			p.trimArticles(ctx, data.File, keep)
		}
	}
}
