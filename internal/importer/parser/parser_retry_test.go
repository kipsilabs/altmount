package parser

import (
	"context"
	"testing"

	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/javi11/nntppool/v5"
	"github.com/javi11/nzbparser"
)

// TestParseNzbRetriesTransientFirstSegmentFetch is the regression test for
// obfuscated multi-volume imports that failed because a couple of volumes'
// first-segment fetches hit a TRANSIENT error ("all providers exhausted") and
// were treated as permanently missing, shattering the set. A transient failure
// must be retried; the volume must survive once a retry succeeds.
func TestParseNzbRetriesTransientFirstSegmentFetch(t *testing.T) {
	const name = "Movie.2020.1080p.BluRay.mkv"

	fp := fakepool.New()
	// Fail the first two attempts transiently, then succeed on the third
	// (maxFirstSegmentFetchAttempts == 3).
	fp.SetBehavior("seg-0", fakepool.SegmentBehavior{
		FailFirst: 2,
		Bytes:     make([]byte, 16),
	})

	pm := newFakeFullPoolManager(fp)
	p := NewParser(pm, stormConfigGetter(2))

	n := &nzbparser.Nzb{
		Files: nzbparser.NzbFiles{
			{
				Filename: name,
				Segments: nzbparser.NzbSegments{
					{Bytes: 12345, Number: 1, ID: "seg-0"},
				},
			},
		},
	}

	parsed, err := p.ParseNzb(context.Background(), n, "test.nzb", nil, ParseOptions{})
	if err != nil {
		t.Fatalf("ParseNzb error = %v", err)
	}
	if len(parsed.Files) != 1 {
		t.Fatalf("len(parsed.Files) = %d, want 1 (volume recovered by retry)", len(parsed.Files))
	}
	if got := parsed.Files[0].Filename; got != name {
		t.Fatalf("kept file = %q, want %q", got, name)
	}
	// 2 transient failures + 1 success = 3 Body calls for the single first segment.
	if calls := fp.BodyCalls(); calls != 3 {
		t.Errorf("BodyCalls = %d, want 3 (two retries then success)", calls)
	}
}

// TestParseNzbDropsFileWhenTransientRetriesExhausted verifies that a file whose
// first segment keeps failing past the retry budget is dropped, while healthy
// siblings still import — and that all attempts were made (retry, not one-shot).
func TestParseNzbDropsFileWhenTransientRetriesExhausted(t *testing.T) {
	const (
		brokenName  = "Broken.2020.1080p.BluRay.mkv"
		healthyName = "Good.2020.1080p.BluRay.mkv"
	)

	fp := fakepool.New()
	// Never succeeds within the retry budget (3 attempts).
	fp.SetBehavior("broken-0", fakepool.SegmentBehavior{FailFirst: 99, Bytes: make([]byte, 16)})
	fp.SetBehavior("good-0", fakepool.SegmentBehavior{Bytes: make([]byte, 16)})

	pm := newFakeFullPoolManager(fp)
	p := NewParser(pm, stormConfigGetter(2))

	n := &nzbparser.Nzb{
		Files: nzbparser.NzbFiles{
			{Filename: brokenName, Segments: nzbparser.NzbSegments{{Bytes: 12345, Number: 1, ID: "broken-0"}}},
			{Filename: healthyName, Segments: nzbparser.NzbSegments{{Bytes: 12345, Number: 1, ID: "good-0"}}},
		},
	}

	parsed, err := p.ParseNzb(context.Background(), n, "test.nzb", nil, ParseOptions{})
	if err != nil {
		t.Fatalf("ParseNzb error = %v", err)
	}
	if len(parsed.Files) != 1 || parsed.Files[0].Filename != healthyName {
		t.Fatalf("parsed.Files = %+v, want only %q", parsed.Files, healthyName)
	}
	// broken-0: 3 attempts (all fail) + good-0: 1 = 4 Body calls total.
	if calls := fp.BodyCalls(); calls != 4 {
		t.Errorf("BodyCalls = %d, want 4 (broken retried 3x, good once)", calls)
	}
}

// TestParseNzbDoesNotRetryArticleNotFound verifies that a genuine missing
// article (430/423) is NOT retried — it is a permanent miss, so retrying would
// only waste network round-trips.
func TestParseNzbDoesNotRetryArticleNotFound(t *testing.T) {
	const (
		missingName = "Missing.2020.1080p.BluRay.mkv"
		healthyName = "Good.2020.1080p.BluRay.mkv"
	)

	fp := fakepool.New()
	fp.SetBehavior("missing-0", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	fp.SetBehavior("good-0", fakepool.SegmentBehavior{Bytes: make([]byte, 16)})

	pm := newFakeFullPoolManager(fp)
	p := NewParser(pm, stormConfigGetter(2))

	n := &nzbparser.Nzb{
		Files: nzbparser.NzbFiles{
			{Filename: missingName, Segments: nzbparser.NzbSegments{{Bytes: 12345, Number: 1, ID: "missing-0"}}},
			{Filename: healthyName, Segments: nzbparser.NzbSegments{{Bytes: 12345, Number: 1, ID: "good-0"}}},
		},
	}

	parsed, err := p.ParseNzb(context.Background(), n, "test.nzb", nil, ParseOptions{})
	if err != nil {
		t.Fatalf("ParseNzb error = %v", err)
	}
	if len(parsed.Files) != 1 || parsed.Files[0].Filename != healthyName {
		t.Fatalf("parsed.Files = %+v, want only %q", parsed.Files, healthyName)
	}
	// missing-0: 1 attempt (no retry on 430) + good-0: 1 = 2 Body calls total.
	if calls := fp.BodyCalls(); calls != 2 {
		t.Errorf("BodyCalls = %d, want 2 (no retry on ErrArticleNotFound)", calls)
	}
}
