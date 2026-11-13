package service

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/publish"
	"github.com/kipsilabs/altmount/internal/peernet/records"
)

// The signer the service hands out must satisfy the publisher's seam.
var _ publish.Signer = RecordSigner(nil)

// capturingSink records every encoded record it is given.
type capturingSink struct {
	mu   sync.Mutex
	recs [][]byte
}

func (s *capturingSink) PublishLocal(_ context.Context, _ netid.NetworkID, enc []byte) error {
	s.mu.Lock()
	s.recs = append(s.recs, append([]byte(nil), enc...))
	s.mu.Unlock()
	return nil
}

func (s *capturingSink) all() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]byte(nil), s.recs...)
}

func (s *capturingSink) kinds(t *testing.T) (rel, att int) {
	t.Helper()
	for _, r := range s.all() {
		env, err := records.Peek(r)
		if err != nil {
			t.Fatal(err)
		}
		if env.Kind == records.KindRelease {
			rel++
		} else {
			att++
		}
	}
	return
}

const (
	markerDir      = "MARKERDIR-9f3a"
	markerFile     = "MARKERFILE-77c1"
	markerCategory = "MARKERCAT-51de"
	markerIndexer  = "MARKERIDX-a04b"
	markerDownload = "MARKERDLID-6b72"
	markerARR      = "MARKERARR-e813"
)

var allMarkers = []string{markerDir, markerFile, markerCategory, markerIndexer, markerDownload, markerARR}

// privacyPipeline is a real pipeline (real publisher, real signer, real SQLite
// repositories) whose sink captures the bytes that would leave the node.
func privacyPipeline(t *testing.T, f *pubFixture, sink *capturingSink) *pipeline {
	t.Helper()
	key, err := identity.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	conn, d := f.db.Connection(), f.db.Dialect()
	p, err := newPipeline(pipelineConfig{
		Signer: nodeSigner{key: key}, Sink: sink,
		Networks:   func() []publish.Network { return []publish.Network{{ID: netid.Open(), Level: 2}} },
		Exclusions: f.excl.get, Backbones: f.bb,
		State: f.state, Content: f.content, Origins: database.NewPeernetOriginRepository(conn, d), Meta: f.meta,
	})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func seedMarkedFile(t *testing.T, f *pubFixture) string {
	t.Helper()
	p := "tv/" + markerDir + "/" + markerFile + ".mkv"
	f.seedFile(t, func(s *seed) {
		s.path = p
		s.category, s.indexer, s.downloadID = markerCategory, markerIndexer, markerDownload
		s.metadata = `{"imdb_id":"tt0133093","type":"movie","tmdbId":603,"arr_instance":"` + markerARR + `","download_id":"` + markerDownload + `"}`
	})
	return p
}

func noticeFor(path string) HealthyNotice {
	n := healthyNotice()
	n.FilePath = path
	return n
}

func TestNoLocalNameLeavesTheNodeInAnyRecord(t *testing.T) {
	f := newPubInstance(t, level(2), nil)
	path := seedMarkedFile(t, f)
	sink := &capturingSink{}
	p := privacyPipeline(t, f, sink)

	p.handle(context.Background(), noticeFor(path))

	if rel, att := sink.kinds(t); rel != 1 || att != 1 {
		t.Fatalf("want one release and one attestation record, got %d/%d (status %+v)", rel, att, p.stats.snapshot())
	}
	for i, raw := range sink.all() {
		low := bytes.ToLower(raw)
		for _, m := range allMarkers {
			if bytes.Contains(low, []byte(strings.ToLower(m))) {
				t.Errorf("record %d contains %q", i, m)
			}
		}
		for _, frag := range []string{"tv/", ".mkv", "/nzbs", "history", "movies"} {
			if bytes.Contains(low, []byte(frag)) {
				t.Errorf("record %d contains the path fragment %q", i, frag)
			}
		}
	}
}

func TestExcludedCategoryOrIndexerProducesNoRecordAtAll(t *testing.T) {
	for name, ex := range map[string]publish.Exclusions{
		"category": {Categories: []string{strings.ToLower(markerCategory)}},
		"indexer":  {Indexers: []string{"  " + markerIndexer + " "}},
	} {
		t.Run(name, func(t *testing.T) {
			f := newPubInstance(t, level(2), nil)
			path := seedMarkedFile(t, f)
			f.excl.set(ex)
			sink := &capturingSink{}
			p := privacyPipeline(t, f, sink)
			p.handle(context.Background(), noticeFor(path))
			if n := len(sink.all()); n != 0 {
				t.Fatalf("%d record(s) produced for an excluded release", n)
			}
			if rows, _ := f.state.ListReleasesPublishedBefore(context.Background(), time.Now().Add(time.Hour), 10); len(rows) != 0 {
				t.Fatal("state was written for an excluded release")
			}
		})
	}
}

func TestPatchedFileYieldsAtMostTheReleaseRecord(t *testing.T) {
	f := newPubInstance(t, level(2), nil)
	path := seedMarkedFile(t, f)
	sink := &capturingSink{}
	p := privacyPipeline(t, f, sink)
	n := noticeFor(path)
	n.PatchedByPAR2 = true
	p.handle(context.Background(), n)
	if rel, att := sink.kinds(t); rel != 1 || att != 0 {
		t.Fatalf("patched file: %d release / %d attestation", rel, att)
	}
}

func TestNoRecognisedBackboneYieldsNoAttestation(t *testing.T) {
	for name, set := range map[string]func(*fakeBackboneSource){
		"empty known list":      func(b *fakeBackboneSource) { b.known = nil },
		"provider not in list":  func(b *fakeBackboneSource) { b.provider = []string{"Some Free Text Label"} },
		"no provider has group": func(b *fakeBackboneSource) { b.provider = nil },
	} {
		t.Run(name, func(t *testing.T) {
			f := newPubInstance(t, level(2), nil)
			set(f.bb)
			path := seedMarkedFile(t, f)
			sink := &capturingSink{}
			p := privacyPipeline(t, f, sink)
			p.handle(context.Background(), noticeFor(path))
			if rel, att := sink.kinds(t); rel != 1 || att != 0 {
				t.Fatalf("%d release / %d attestation", rel, att)
			}
		})
	}
}

func TestNoNetworkAtLevelTwoBuildsNothing(t *testing.T) {
	f := newPubInstance(t, level(1), nil)
	path := seedMarkedFile(t, f)
	sink := &capturingSink{}
	p := privacyPipeline(t, f, sink)
	p.cfg.Networks = func() []publish.Network { return nil }
	p.submit(noticeFor(path))
	if !p.idle() || len(sink.all()) != 0 || p.stats.snapshot().Skipped["not_publishing"] != 1 {
		t.Fatalf("nothing may be queued or built: %+v", p.stats.snapshot())
	}
}

const markerPassword = "MARKERPW-c2d4"

func TestNoPasswordMarkerLeavesTheNodeInAnyRecord(t *testing.T) {
	cases := map[string]func(*seed){
		"history name with brackets": func(s *seed) { s.nzbName = "Rel.Name.2020{{" + markerPassword + "}}.nzb" },
		"history name with spaces":   func(s *seed) { s.nzbName = "Rel.Name.2020{{ " + markerPassword + " }}.nzb" },
		"history name slash form":    func(s *seed) { s.nzbName = "Rel.Name.2020/password:" + markerPassword + ".nzb" },
		"fallback queue path": func(s *seed) {
			s.nzbName = ""
			s.sourceNzb = "/data/nzbs/1234-Rel.Name.2020{{" + markerPassword + "}}.nzbz"
		},
	}
	for name, mod := range cases {
		t.Run(name, func(t *testing.T) {
			f := newPubInstance(t, level(2), nil)
			f.seedFile(t, mod)
			sink := &capturingSink{}
			p := privacyPipeline(t, f, sink)
			p.handle(context.Background(), noticeFor(pubPath))
			if rel, _ := sink.kinds(t); rel != 1 {
				t.Fatalf("want a release record, got status %+v", p.stats.snapshot())
			}
			for i, raw := range sink.all() {
				low := bytes.ToLower(raw)
				for _, frag := range []string{strings.ToLower(markerPassword), "{{", "}}", "1234-", ".nzb"} {
					if bytes.Contains(low, []byte(frag)) {
						t.Errorf("record %d contains %q", i, frag)
					}
				}
			}
		})
	}
}

func TestTitleThatIsThePasswordIsNotPublished(t *testing.T) {
	f := newPubInstance(t, level(2), nil)
	f.seedFile(t, func(s *seed) { s.nzbName = "hunter2secret.nzb"; s.password = "hunter2secret" })
	sink := &capturingSink{}
	p := privacyPipeline(t, f, sink)
	p.handle(context.Background(), noticeFor(pubPath))
	if n := len(sink.all()); n != 0 {
		t.Fatalf("%d record(s) published for a release titled with its password", n)
	}
	if got := p.stats.snapshot().Skipped["title_contains_password"]; got == 0 {
		t.Fatalf("no title_contains_password skip: %+v", p.stats.snapshot())
	}
}
