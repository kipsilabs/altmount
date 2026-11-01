package parser

import (
	"context"
	"fmt"
	"testing"

	"github.com/javi11/nntppool/v5"
	"github.com/javi11/nzbparser"

	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
)

const pastEndPartSize = 1000

// pastEndFile lists five articles for a file that needs three.
func pastEndFile(fp *fakepool.Client, name string, fileSize int64) nzbparser.NzbFile {
	file := nzbparser.NzbFile{Filename: name}
	for i := range 5 {
		id := fmt.Sprintf("%s-%d", name, i)
		file.Segments = append(file.Segments, nzbparser.NzbSegment{ID: id, Number: i + 1, Bytes: 1100})
		file.Bytes += 1100
		fp.SetBehavior(id, fakepool.SegmentBehavior{YEnc: nntppool.YEncMeta{PartSize: pastEndPartSize, FileSize: fileSize}})
	}
	return file
}

func setDead(fp *fakepool.Client, ids ...string) {
	for _, id := range ids {
		fp.SetBehavior(id, fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	}
}

func TestFindDeadArticlesPastEnd(t *testing.T) {
	tests := []struct {
		name     string
		fileSize int64
		dead     []string
		wantKeep int // 0 means the file must be left alone
	}{
		{"every extra is dead", 3000, []string{"f-3", "f-4"}, 3},
		{"short last part still needs its article", 2500, []string{"f-3", "f-4"}, 3},
		{"an extra is alive", 3000, []string{"f-4"}, 0},
		{"no extras", 5000, []string{"f-4"}, 0},
		{"unknown file size", 0, []string{"f-3", "f-4"}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fp := fakepool.New()
			files := []nzbparser.NzbFile{pastEndFile(fp, "f", tt.fileSize)}
			setDead(fp, tt.dead...)
			p := NewParser(newFakeFullPoolManager(fp), stormConfigGetter(4))

			keep := p.FindDeadArticlesPastEnd(context.Background(), files)
			if got := keep[0]; got != tt.wantKeep {
				t.Fatalf("keep = %d, want %d", got, tt.wantKeep)
			}
			if len(files[0].Segments) != 5 {
				t.Fatal("finding must not change the file: the warm-up may be reading it")
			}
		})
	}
}

func TestTrimArticlesDropsExtrasAndTheirBytes(t *testing.T) {
	fp := fakepool.New()
	files := []nzbparser.NzbFile{pastEndFile(fp, "f", 3000)}
	p := NewParser(newFakeFullPoolManager(fp), stormConfigGetter(4))

	p.TrimArticles(context.Background(), files, map[int]int{0: 3})

	if len(files[0].Segments) != 3 || files[0].Bytes != 3300 {
		t.Fatalf("segments = %d, bytes = %d, want 3 and 3300", len(files[0].Segments), files[0].Bytes)
	}
}

// The sampled availability check can miss the extras, so parsing drops them too.
func TestParseTrimDropsExtrasTheSampleMissed(t *testing.T) {
	fp := fakepool.New()
	file := pastEndFile(fp, "f", 3000)
	setDead(fp, "f-3", "f-4")
	p := NewParser(newFakeFullPoolManager(fp), stormConfigGetter(4))
	sizes := map[string]firstSegmentYencInfo{"f-0": {PartSize: pastEndPartSize, FileSize: 3000}}

	p.trimDeadArticlesPastEnd(context.Background(), []*FirstSegmentData{{File: &file}}, sizes)

	if len(file.Segments) != 3 {
		t.Fatalf("segments = %d, want 3", len(file.Segments))
	}
}
