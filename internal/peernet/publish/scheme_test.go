package publish

import (
	"context"
	"testing"

	"github.com/kipsilabs/altmount/internal/peernet/records"
)

func schemes(t *testing.T, sink *fakeSink) map[records.Kind]uint8 {
	t.Helper()
	out := map[records.Kind]uint8{}
	for _, s := range sink.got {
		env, err := records.Peek(s.enc)
		if err != nil {
			t.Fatal(err)
		}
		out[env.Kind] = env.ReleaseScheme
	}
	return out
}

// A release identified by its files says so in both records, so a receiver
// never mistakes it for an NZB-derived ID.
func TestPublishCarriesReleaseScheme(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   uint8
		want uint8
	}{{"default is nzb", 0, records.SchemeNZB}, {"nzb", records.SchemeNZB, records.SchemeNZB}, {"files", records.SchemeFiles, records.SchemeFiles}} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t)
			inf := info()
			inf.ReleaseScheme = tc.in
			if _, err := e.publisher(t).Publish(context.Background(), healthy(), inf); err != nil {
				t.Fatal(err)
			}
			got := schemes(t, e.sink)
			if len(got) != 2 {
				t.Fatalf("published kinds %v", got)
			}
			for k, s := range got {
				if s != tc.want {
					t.Errorf("%v record has scheme %d, want %d", k, s, tc.want)
				}
			}
		})
	}
}

func TestReannounceCarriesReleaseScheme(t *testing.T) {
	e := newEnv(t)
	inf := info()
	inf.ReleaseScheme = records.SchemeFiles
	o := e.publisher(t).ReannounceRelease(context.Background(), openNet, relID, inf)
	if o.Err != nil || o.Skip != SkipNone {
		t.Fatalf("outcome %+v", o)
	}
	if got := schemes(t, e.sink)[records.KindRelease]; got != records.SchemeFiles {
		t.Fatalf("re-announced record has scheme %d, want files", got)
	}
}

func TestPublishRejectsUnknownScheme(t *testing.T) {
	e := newEnv(t)
	inf := info()
	inf.ReleaseScheme = 7
	if _, err := e.publisher(t).Publish(context.Background(), healthy(), inf); err == nil {
		t.Fatal("an unknown scheme must not become a record")
	}
	if len(e.sink.got) != 0 {
		t.Fatal("nothing may be sunk")
	}
}
