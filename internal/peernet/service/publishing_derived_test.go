package service

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/records"
)

const derivedPath2 = "movies/Some.Movie.2020/movie.nfo"

func ownRecords(t *testing.T, f *pubFixture) (rel *records.ReleaseRecord, atts []*records.HealthAttestation) {
	t.Helper()
	page, err := f.recs.ListOwnSince(context.Background(), netid.Open().String(), 0, 50)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range page.Rows {
		env, err := records.Peek(row.Payload)
		if err != nil {
			t.Fatal(err)
		}
		if env.Kind == records.KindRelease {
			if rel, err = records.DecodeReleaseRecord(row.Payload); err != nil {
				t.Fatal(err)
			}
		} else {
			a, err := records.DecodeHealthAttestation(row.Payload)
			if err != nil {
				t.Fatal(err)
			}
			atts = append(atts, a)
		}
	}
	return rel, atts
}

// An old-format import (no NZB store, so a library-derived release ID) is
// published like any other: a release record and an attestation that say the
// ID is files-derived, with size and file count from the grouped files and a
// title taken from the source NZB name when the import history is gone.
func TestOldFormatFilePublishesFilesSchemeRecords(t *testing.T) {
	f := newPubInstance(t, level(2), nil)
	derived := hx("d")
	f.seedFile(t, func(s *seed) {
		s.release, s.scheme = derived, database.PeernetSchemeFiles
		s.nzbName, s.category, s.indexer = "", "", "" // import history is gone
		s.sourceNzb = "/data/.nzbs/12/Some.Movie.2020.1080p.mkv.nzb.gz"
		s.size = 3 << 30
	})
	f.seedFile(t, func(s *seed) {
		s.path, s.file, s.release, s.scheme = derivedPath2, hx("c"), derived, database.PeernetSchemeFiles
		s.nzbName, s.category, s.indexer = "", "", ""
		s.sourceNzb = "/data/.nzbs/12/Some.Movie.2020.1080p.mkv.nzb.gz"
		s.size = 1 << 20
	})
	f.start(t)
	f.svc.NotifyHealthy(healthyNotice())
	f.drain(t)

	if pub := f.status(t).Publish; pub.Published["release"] != 1 || pub.Published["attestation"] != 1 || pub.Errors != 0 {
		t.Fatalf("publish status: %+v", pub)
	}
	rel, atts := ownRecords(t, f)
	if rel == nil || len(atts) != 1 {
		t.Fatalf("records: %v %d", rel, len(atts))
	}
	if rel.Scheme() != records.SchemeFiles || atts[0].Scheme() != records.SchemeFiles {
		t.Fatalf("schemes %d / %d, want files for both", rel.Scheme(), atts[0].Scheme())
	}
	if rel.FileCount != 2 || rel.TotalSize != 3<<30+1<<20 {
		t.Fatalf("count %d size %d: must come from the grouped files", rel.FileCount, rel.TotalSize)
	}
	if rel.Title != "Some.Movie.2020.1080p" {
		t.Fatalf("title %q", rel.Title)
	}
	if hex.EncodeToString(rel.ReleaseID[:]) != derived {
		t.Fatalf("release id %x", rel.ReleaseID)
	}
}

// The same file indexed twice (an import kept under two folders) is one file of
// the release, not two.
func TestReleaseInfoCountsADuplicatedFileOnce(t *testing.T) {
	f := newPubInstance(t, level(2), nil)
	derived := hx("d")
	for _, p := range []string{pubPath, "movies/Copy/movie.mkv"} {
		f.seedFile(t, func(s *seed) { s.path, s.release, s.scheme = p, derived, database.PeernetSchemeFiles })
	}
	f.start(t)
	f.svc.NotifyHealthy(healthyNotice())
	f.drain(t)
	rel, _ := ownRecords(t, f)
	if rel == nil || rel.FileCount != 1 {
		t.Fatalf("release: %+v", rel)
	}
}
