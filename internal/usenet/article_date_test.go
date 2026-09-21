package usenet

import (
	"context"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/pool"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/kipsilabs/altmount/internal/testsupport/segments"
)

const articleDateSegSize = 1024

// The reader must declare its file's post date on every request it makes, so
// the pool can apply per-provider retention limits. A reader that forwarded
// nothing would silently disable the feature for the whole streaming path.
func TestReaderForwardsArticleDate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	posted := time.Now().Add(-400 * 24 * time.Hour).Truncate(time.Second)

	fp := fakepool.New()
	fp.SetDefaultBehavior(fakepool.SegmentBehavior{Bytes: make([]byte, articleDateSegSize)})

	rg := buildEagerRange(ctx, t, 2, articleDateSegSize)
	getter := func() (pool.NntpClient, error) { return fp, nil }
	ur, err := NewUsenetReader(ctx, getter, rg, 1, noopMetrics{}, "date-stream", nil,
		WithArticleDate(posted), withFlightMap(newFlightMap()))
	if err != nil {
		t.Fatalf("NewUsenetReader: %v", err)
	}
	defer func() { _ = ur.Close() }()

	if _, err := ur.Read(make([]byte, 16)); err != nil {
		t.Fatalf("Read: %v", err)
	}

	got, ok := fp.ArticleDateFor(segments.MessageID(0))
	if !ok {
		t.Fatal("the first segment was never requested")
	}
	if !got.Equal(posted) {
		t.Fatalf("ArticleDate = %v, want %v", got, posted)
	}
}

// Without the option the date stays zero, which applies no retention policy —
// the behavior files whose metadata predates release_date must keep.
func TestReaderWithoutArticleDateSendsZero(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fp := fakepool.New()
	fp.SetDefaultBehavior(fakepool.SegmentBehavior{Bytes: make([]byte, articleDateSegSize)})

	rg := buildEagerRange(ctx, t, 1, articleDateSegSize)
	ur := newReaderForTest(t, ctx, fp, rg, 1)

	if _, err := ur.Read(make([]byte, 16)); err != nil {
		t.Fatalf("Read: %v", err)
	}

	got, ok := fp.ArticleDateFor(segments.MessageID(0))
	if !ok {
		t.Fatal("the first segment was never requested")
	}
	if !got.IsZero() {
		t.Fatalf("ArticleDate = %v, want the zero time (no retention policy)", got)
	}
}
