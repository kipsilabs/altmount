package records

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/kipsilabs/altmount/internal/peernet/netid"
)

func TestReleaseRoundTrip(t *testing.T) {
	k := testKey(t)
	nw := netid.Open()
	rec, enc := mustRelease(t, k, nw)
	if len(enc) > MaxEncodedSize {
		t.Fatalf("encoded %d bytes", len(enc))
	}
	got, err := DecodeReleaseRecord(enc)
	if err != nil {
		t.Fatal(err)
	}
	if err := got.Verify(testNow, nw); err != nil {
		t.Fatal(err)
	}
	if got.Title != rec.Title || got.TotalSize != rec.TotalSize || got.FileCount != 4 ||
		got.Media.IMDb != "tt1234567" || *got.Media.Season != 0 || *got.Media.Episode != 12 ||
		got.Media.TMDB != 603 || got.Media.TVDB != 78804 || got.BundleID != BundleID(fill32(3)) ||
		!got.PostDate.Equal(rec.PostDate) || got.PublisherID != k.NodeID() {
		t.Fatalf("mismatch: %+v", got)
	}
	again, err := got.Encode()
	if err != nil || !bytes.Equal(again, enc) {
		t.Fatalf("re-encode differs: %v", err)
	}
}

func TestReleaseMinimalOptional(t *testing.T) {
	in := releaseInput()
	in.BundleID = BundleID{}
	in.Media = MediaIDs{}
	in.PostDate = time.Time{}
	_, enc, err := BuildReleaseRecord(testKey(t), netid.Open(), in, testNow)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeReleaseRecord(enc)
	if err != nil {
		t.Fatal(err)
	}
	if got.Media.Season != nil || got.Media.Episode != nil || !got.PostDate.IsZero() || got.Media.IMDb != "" {
		t.Fatalf("optional fields appeared: %+v", got)
	}
}

func TestAttestationRoundTrip(t *testing.T) {
	k := testKey(t)
	nw := netid.Open()
	_, enc := mustAttestation(t, k, nw)
	got, err := DecodeHealthAttestation(enc)
	if err != nil {
		t.Fatal(err)
	}
	if err := got.Verify(testNow, nw); err != nil {
		t.Fatal(err)
	}
	if strings.Join(got.Backbones, ",") != "abavia,omicron" {
		t.Fatalf("backbones not sorted: %v", got.Backbones)
	}
	if !got.MediaHeaderVerified || got.TotalSegments != 1000 || got.SegmentsChecked != 200 || got.MissingCount != 0 {
		t.Fatalf("mismatch: %+v", got)
	}
	again, err := got.Encode()
	if err != nil || !bytes.Equal(again, enc) {
		t.Fatalf("re-encode differs: %v", err)
	}
}

func TestBuildReleaseCleansAndRejects(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*ReleaseRecord)
		title  string // expected cleaned title when no error
		err    error
	}{
		{"cleans separators and controls", func(r *ReleaseRecord) { r.Title = "a/b\\c\x00d\te\u202ef" }, "a b c d e f", nil},
		{"invalid utf8 dropped", func(r *ReleaseRecord) { r.Title = "ab\xffcd" }, "abcd", nil},
		{"truncated", func(r *ReleaseRecord) { r.Title = strings.Repeat("é", 400) }, "", nil},
		{"empty after cleaning", func(r *ReleaseRecord) { r.Title = "/\\\x00" }, "", ErrInvalidField},
		{"bad imdb", func(r *ReleaseRecord) { r.Media.IMDb = "tt12345678901" }, "", ErrInvalidField},
		{"zero release id", func(r *ReleaseRecord) { r.ReleaseID = ReleaseID{} }, "", ErrInvalidField},
		{"zero files", func(r *ReleaseRecord) { r.FileCount = 0 }, "", ErrInvalidField},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			in := releaseInput()
			c.mutate(&in)
			r, _, err := BuildReleaseRecord(testKey(t), netid.Open(), in, testNow)
			if c.err != nil {
				if !errors.Is(err, c.err) {
					t.Fatalf("err = %v, want %v", err, c.err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if c.title != "" && r.Title != c.title {
				t.Fatalf("title %q want %q", r.Title, c.title)
			}
			if len(r.Title) > MaxTitleLen {
				t.Fatalf("title too long: %d", len(r.Title))
			}
		})
	}
}

func TestBuildAttestationNormalisesBackbones(t *testing.T) {
	in := attestationInput()
	in.Backbones = []string{" Omicron ", "abavia", "OMICRON"}
	a, _, err := BuildHealthAttestation(testKey(t), netid.Open(), in, testNow)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(a.Backbones, ",") != "abavia,omicron" {
		t.Fatalf("got %v", a.Backbones)
	}
	if in.Backbones[0] != " Omicron " {
		t.Fatal("builder mutated its input")
	}
	bad := [][]string{nil, {"bad label"}, {"a/b"}, {strings.Repeat("a", MaxBackboneLen+1)}, {""}}
	for _, b := range bad {
		in.Backbones = b
		if _, _, err := BuildHealthAttestation(testKey(t), netid.Open(), in, testNow); !errors.Is(err, ErrInvalidField) {
			t.Fatalf("%v: err = %v", b, err)
		}
	}
	many := make([]string, 0, MaxBackbones+1)
	for i := 0; i <= MaxBackbones; i++ {
		many = append(many, "b"+strings.Repeat("x", i))
	}
	in.Backbones = many
	if _, _, err := BuildHealthAttestation(testKey(t), netid.Open(), in, testNow); !errors.Is(err, ErrInvalidField) {
		t.Fatalf("too many backbones: %v", err)
	}
}

// TestDecodeRejectsInvalidFields crafts records (bypassing the builder) that
// violate field rules; the decoder must reject, never repair.
func TestDecodeRejectsInvalidFields(t *testing.T) {
	k := testKey(t)
	rel := func(f func(*ReleaseRecord)) []byte {
		r, _ := mustRelease(t, k, netid.Open())
		f(r)
		return r.marshal()
	}
	att := func(f func(*HealthAttestation)) []byte {
		a, _ := mustAttestation(t, k, netid.Open())
		f(a)
		return a.marshal()
	}
	relCases := map[string]func(*ReleaseRecord){
		"slash":          func(r *ReleaseRecord) { r.Title = "a/b" },
		"backslash":      func(r *ReleaseRecord) { r.Title = "a\\b" },
		"nul":            func(r *ReleaseRecord) { r.Title = "a\x00b" },
		"control":        func(r *ReleaseRecord) { r.Title = "a\nb" },
		"bidi":           func(r *ReleaseRecord) { r.Title = "a\u202eb" },
		"bidi isolate":   func(r *ReleaseRecord) { r.Title = "a\u2066b" },
		"line separator": func(r *ReleaseRecord) { r.Title = "a b" },
		"bad utf8":       func(r *ReleaseRecord) { r.Title = "a\xffb" },
		"empty title":    func(r *ReleaseRecord) { r.Title = "" },
		"long title":     func(r *ReleaseRecord) { r.Title = strings.Repeat("a", MaxTitleLen+1) },
		"imdb no tt":     func(r *ReleaseRecord) { r.Media.IMDb = "1234567" },
		"imdb letters":   func(r *ReleaseRecord) { r.Media.IMDb = "ttabc" },
		"imdb long":      func(r *ReleaseRecord) { r.Media.IMDb = "tt12345678901" },
		"imdb empty id":  func(r *ReleaseRecord) { r.Media.IMDb = "tt" },
		"imdb upper":     func(r *ReleaseRecord) { r.Media.IMDb = "TT123" },
		"size negative":  func(r *ReleaseRecord) { r.TotalSize = -1 },
		"size huge":      func(r *ReleaseRecord) { r.TotalSize = MaxTotalSize + 1 },
		"files zero":     func(r *ReleaseRecord) { r.FileCount = 0 },
		"files many":     func(r *ReleaseRecord) { r.FileCount = MaxFileCount + 1 },
		"season big":     func(r *ReleaseRecord) { r.Media.Season = u32(MaxSeason + 1) },
		"episode big":    func(r *ReleaseRecord) { r.Media.Episode = u32(MaxEpisode + 1) },
		"zero network":   func(r *ReleaseRecord) { r.NetworkID = netid.NetworkID{} },
		"zero release":   func(r *ReleaseRecord) { r.ReleaseID = ReleaseID{} },
		"short pubkey":   func(r *ReleaseRecord) { r.PublisherKey = r.PublisherKey[:31] },
		"short sig":      func(r *ReleaseRecord) { r.Signature = r.Signature[:63] },
		"expiry before":  func(r *ReleaseRecord) { r.ExpiresAt = r.CreatedAt },
		"too long life":  func(r *ReleaseRecord) { r.ExpiresAt = r.CreatedAt.Add(RecordLifetime + time.Second) },
		"time zero":      func(r *ReleaseRecord) { r.CreatedAt = time.Time{} },
	}
	for n, f := range relCases {
		t.Run("release/"+n, func(t *testing.T) {
			if _, err := DecodeReleaseRecord(rel(f)); !errors.Is(err, ErrInvalidField) {
				t.Fatalf("err = %v, want ErrInvalidField", err)
			}
		})
	}
	attCases := map[string]func(*HealthAttestation){
		"zero file":        func(a *HealthAttestation) { a.FileID = FileID{} },
		"zero total":       func(a *HealthAttestation) { a.TotalSegments = 0 },
		"huge total":       func(a *HealthAttestation) { a.TotalSegments = MaxSegments + 1 },
		"checked>total":    func(a *HealthAttestation) { a.SegmentsChecked = a.TotalSegments + 1 },
		"zero checked":     func(a *HealthAttestation) { a.SegmentsChecked = 0 },
		"missing>total":    func(a *HealthAttestation) { a.MissingCount = a.TotalSegments + 1 },
		"no backbones":     func(a *HealthAttestation) { a.Backbones = nil },
		"unsorted":         func(a *HealthAttestation) { a.Backbones = []string{"b", "a"} },
		"duplicate":        func(a *HealthAttestation) { a.Backbones = []string{"a", "a"} },
		"upper":            func(a *HealthAttestation) { a.Backbones = []string{"Abc"} },
		"bad charset":      func(a *HealthAttestation) { a.Backbones = []string{"a b"} },
		"slash label":      func(a *HealthAttestation) { a.Backbones = []string{"a/b"} },
		"long label":       func(a *HealthAttestation) { a.Backbones = []string{strings.Repeat("a", MaxBackboneLen+1)} },
		"empty label":      func(a *HealthAttestation) { a.Backbones = []string{""} },
		"checked time bad": func(a *HealthAttestation) { a.CheckedAt = time.Time{} },
		"long validity":    func(a *HealthAttestation) { a.ExpiresAt = a.CreatedAt.Add(AttestationValidity + time.Second) },
	}
	for n, f := range attCases {
		t.Run("attestation/"+n, func(t *testing.T) {
			if _, err := DecodeHealthAttestation(att(f)); !errors.Is(err, ErrInvalidField) {
				t.Fatalf("err = %v, want ErrInvalidField", err)
			}
		})
	}
	t.Run("attestation/too many backbones", func(t *testing.T) {
		b := att(func(a *HealthAttestation) {
			a.Backbones = nil
			for i := 0; i <= MaxBackbones; i++ {
				a.Backbones = append(a.Backbones, "b"+strings.Repeat("x", i))
			}
		})
		if _, err := DecodeHealthAttestation(b); !errors.Is(err, ErrInvalidField) && !errors.Is(err, ErrTooLarge) {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestDecodeRejectsWireViolations(t *testing.T) {
	_, enc := mustRelease(t, testKey(t), netid.Open())
	if enc[0] != 0x08 || enc[1] != 0x01 {
		t.Fatalf("unexpected layout %x", enc[:2])
	}
	cat := func(parts ...[]byte) []byte { return bytes.Join(parts, nil) }
	cases := map[string]struct {
		in  []byte
		err error
	}{
		"empty":                {nil, ErrMalformed},
		"truncated":            {enc[:len(enc)-5], ErrMalformed},
		"trailing garbage":     {cat(enc, []byte{0}), ErrMalformed},
		"unknown field":        {cat(enc, protowire.AppendVarint(protowire.AppendTag(nil, 31, protowire.VarintType), 1)), ErrMalformed},
		"unknown low field":    {cat(enc[:2], protowire.AppendVarint(protowire.AppendTag(nil, 9, protowire.VarintType), 1), enc[2:]), ErrMalformed},
		"duplicate version":    {cat(enc[:2], enc), ErrMalformed},
		"non-minimal varint":   {cat([]byte{0x08, 0x81, 0x00}, enc[2:]), ErrMalformed},
		"version 2":            {cat([]byte{0x08, 0x02}, enc[2:]), ErrUnsupportedVersion},
		"version 0":            {cat([]byte{0x08, 0x00}, enc[2:]), ErrUnsupportedVersion},
		"wrong wire type":      {cat([]byte{0x0a, 0x01, 0x01}, enc[2:]), ErrMalformed},
		"fixed32 wire type":    {cat(enc[:2], protowire.AppendFixed32(protowire.AppendTag(nil, 2, protowire.Fixed32Type), 1), enc[4:]), ErrMalformed},
		"missing required":     {cat(enc[:2], enc[4:]), ErrMalformed},
		"attestation kind":     {mustAttBytes(t), ErrWrongKind},
		"oversize":             {make([]byte, MaxEncodedSize+1), ErrTooLarge},
		"zero-length ID field": {cat(enc[:4], []byte{0x1a, 0x00}, enc[4:]), ErrMalformed},
	}
	for n, c := range cases {
		t.Run(n, func(t *testing.T) {
			if _, err := DecodeReleaseRecord(c.in); !errors.Is(err, c.err) {
				t.Fatalf("err = %v, want %v", err, c.err)
			}
		})
	}
	// Out-of-order: swap two adjacent fields (version after kind).
	swapped := cat(enc[2:4], enc[:2], enc[4:])
	if _, err := DecodeReleaseRecord(swapped); !errors.Is(err, ErrMalformed) {
		t.Fatalf("out of order: %v", err)
	}
	if _, err := DecodeHealthAttestation(enc); !errors.Is(err, ErrWrongKind) {
		t.Fatalf("release as attestation: %v", err)
	}
}

func mustAttBytes(t *testing.T) []byte {
	_, b := mustAttestation(t, testKey(t), netid.Open())
	return b
}

func TestVerifyRules(t *testing.T) {
	k := testKey(t)
	open := netid.Open()
	other := netid.FromRootKey(k.PublicKey())

	relCases := []struct {
		name   string
		now    time.Time
		arrive netid.NetworkID
		mutate func(*ReleaseRecord)
		err    error
	}{
		{"ok", testNow, open, nil, nil},
		{"ok within skew", testNow.Add(-4 * time.Minute), open, nil, nil},
		{"wrong network", testNow, other, nil, ErrWrongNetwork},
		{"future", testNow.Add(-MaxFutureSkew - 2*time.Second), open, nil, ErrFromFuture},
		{"expired", testNow.Add(RecordLifetime), open, nil, ErrExpired},
		{"just before expiry", testNow.Add(RecordLifetime - time.Second), open, nil, nil},
		{"tampered title", testNow, open, func(r *ReleaseRecord) { r.Title = "other" }, ErrBadSignature},
		{"tampered size", testNow, open, func(r *ReleaseRecord) { r.TotalSize++ }, ErrBadSignature},
		{"tampered expiry", testNow, open, func(r *ReleaseRecord) { r.ExpiresAt = r.ExpiresAt.Add(time.Hour) }, ErrBadSignature},
		{"node id mismatch", testNow, open, func(r *ReleaseRecord) { r.PublisherID[0] ^= 1 }, ErrNodeIDMismatch},
		{"foreign key", testNow, open, func(r *ReleaseRecord) { r.PublisherKey = testKey(t).PublicKey() }, ErrNodeIDMismatch},
		{"no signature", testNow, open, func(r *ReleaseRecord) { r.Signature = nil }, ErrBadSignature},
	}
	for _, c := range relCases {
		t.Run("release/"+c.name, func(t *testing.T) {
			r, _ := mustRelease(t, k, open)
			if c.mutate != nil {
				c.mutate(r)
			}
			if err := r.Verify(c.now, c.arrive); !errors.Is(err, c.err) {
				t.Fatalf("err = %v, want %v", err, c.err)
			}
		})
	}

	attCases := []struct {
		name   string
		now    time.Time
		arrive netid.NetworkID
		mutate func(*HealthAttestation)
		err    error
	}{
		{"ok", testNow, open, nil, nil},
		{"wrong network", testNow, other, nil, ErrWrongNetwork},
		{"future", testNow.Add(-MaxFutureSkew - 2*time.Second), open, nil, ErrFromFuture},
		{"expired", testNow.Add(AttestationValidity), open, nil, ErrExpired},
		{"stale", testNow.Add(AttestationValidity - time.Hour + time.Second), open, nil, ErrStale},
		{"checked after created", testNow, open, func(a *HealthAttestation) { a.CheckedAt = a.CreatedAt.Add(time.Second) }, ErrCheckedAfterCreated},
		{"tampered missing", testNow, open, func(a *HealthAttestation) { a.MissingCount = 5 }, ErrBadSignature},
		{"tampered backbones", testNow, open, func(a *HealthAttestation) { a.Backbones = []string{"x"} }, ErrBadSignature},
		{"node id mismatch", testNow, open, func(a *HealthAttestation) { a.PublisherID[5] ^= 1 }, ErrNodeIDMismatch},
	}
	for _, c := range attCases {
		t.Run("attestation/"+c.name, func(t *testing.T) {
			a, _ := mustAttestation(t, k, open)
			if c.mutate != nil {
				c.mutate(a)
			}
			if err := a.Verify(c.now, c.arrive); !errors.Is(err, c.err) {
				t.Fatalf("err = %v, want %v", err, c.err)
			}
		})
	}
}

func TestBuildAttestationTimeRules(t *testing.T) {
	in := attestationInput()
	in.CheckedAt = testNow.Add(time.Minute)
	if _, _, err := BuildHealthAttestation(testKey(t), netid.Open(), in, testNow); !errors.Is(err, ErrCheckedAfterCreated) {
		t.Fatalf("future check: %v", err)
	}
	in.CheckedAt = testNow.Add(-AttestationValidity - time.Hour)
	if _, _, err := BuildHealthAttestation(testKey(t), netid.Open(), in, testNow); !errors.Is(err, ErrStale) {
		t.Fatalf("stale check: %v", err)
	}
	in = attestationInput()
	in.MissingCount = 7
	a, enc, err := BuildHealthAttestation(testKey(t), netid.Open(), in, testNow)
	if err != nil || a.Healthy() {
		t.Fatalf("missing attestation: %v healthy=%v", err, a != nil && a.Healthy())
	}
	if _, err := DecodeHealthAttestation(enc); err != nil {
		t.Fatal(err)
	}
}

func TestCrossNetworkReplay(t *testing.T) {
	k := testKey(t)
	r, _ := mustRelease(t, k, netid.Open())
	priv := netid.FromRootKey(k.PublicKey())
	// Re-labelling the record for another network invalidates the signature.
	moved := *r
	moved.NetworkID = priv
	enc := moved.marshal()
	got, err := DecodeReleaseRecord(enc)
	if err != nil {
		t.Fatal(err)
	}
	if err := got.Verify(testNow, priv); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("relabelled: %v", err)
	}
	// Delivering the genuine record on another network is rejected too.
	if err := r.Verify(testNow, priv); !errors.Is(err, ErrWrongNetwork) {
		t.Fatalf("arrived on wrong network: %v", err)
	}
}

func TestCrossTypeReplay(t *testing.T) {
	k := testKey(t)
	nw := netid.Open()
	a, _ := mustAttestation(t, k, nw)
	r, _ := mustRelease(t, k, nw)

	// Attestation signature on a release record carrying identical headers.
	forged := *r
	forged.Header = a.Header
	if err := forged.Verify(testNow, nw); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("attestation sig on release: %v", err)
	}
	// Release signature on an attestation.
	forgedA := *a
	forgedA.Header = r.Header
	if err := forgedA.Verify(testNow, nw); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("release sig on attestation: %v", err)
	}
	// Identical signing input under the other tag never verifies: sign the
	// release body with the attestation tag.
	sig, err := k.Sign(attestationTag, signingInput(r.NetworkID, r.body()))
	if err != nil {
		t.Fatal(err)
	}
	r2 := *r
	r2.Signature = sig
	if err := r2.Verify(testNow, nw); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("wrong tag: %v", err)
	}
	if releaseTag == attestationTag {
		t.Fatal("tags must differ")
	}
}

func TestEverySingleBitFlipRejected(t *testing.T) {
	k := testKey(t)
	nw := netid.Open()
	_, relEnc := mustRelease(t, k, nw)
	_, attEnc := mustAttestation(t, k, nw)
	for name, enc := range map[string][]byte{"release": relEnc, "attestation": attEnc} {
		for i := 0; i < len(enc)*8; i++ {
			mut := bytes.Clone(enc)
			mut[i/8] ^= 1 << (i % 8)
			if name == "release" {
				r, err := DecodeReleaseRecord(mut)
				if err == nil && r.Verify(testNow, nw) == nil {
					t.Fatalf("%s: bit %d flip accepted", name, i)
				}
			} else {
				a, err := DecodeHealthAttestation(mut)
				if err == nil && a.Verify(testNow, nw) == nil {
					t.Fatalf("%s: bit %d flip accepted", name, i)
				}
			}
		}
	}
}

func TestPeekEnvelope(t *testing.T) {
	k := testKey(t)
	nw := netid.Open()
	r, relEnc := mustRelease(t, k, nw)
	a, attEnc := mustAttestation(t, k, nw)

	e, err := Peek(relEnc)
	if err != nil {
		t.Fatal(err)
	}
	if e.Kind != KindRelease || e.NetworkID != nw || e.ReleaseID != r.ReleaseID || e.FileID != (FileID{}) ||
		e.PublisherID != k.NodeID() || !e.CreatedAt.Equal(r.CreatedAt) || !e.ExpiresAt.Equal(r.ExpiresAt) ||
		e.ContentHash != ContentHash(relEnc) {
		t.Fatalf("release envelope: %+v", e)
	}
	e, err = Peek(attEnc)
	if err != nil {
		t.Fatal(err)
	}
	if e.Kind != KindAttestation || e.FileID != a.FileID || e.ReleaseID != a.ReleaseID {
		t.Fatalf("attestation envelope: %+v", e)
	}
	if ContentHash(relEnc) == ContentHash(attEnc) {
		t.Fatal("hash collision")
	}
	for _, bad := range [][]byte{nil, {1, 2, 3}, make([]byte, MaxEncodedSize+1), relEnc[:20]} {
		if _, err := Peek(bad); err == nil {
			t.Fatalf("peek accepted %x", bad)
		}
	}
	if KindRelease.String() != "release" || KindAttestation.String() != "attestation" || Kind(9).String() == "" {
		t.Fatal("kind strings")
	}
}

func TestBuildRejectsNilKeyAndZeroNetwork(t *testing.T) {
	if _, _, err := BuildReleaseRecord(nil, netid.Open(), releaseInput(), testNow); err == nil {
		t.Fatal("nil key accepted")
	}
	if _, _, err := BuildHealthAttestation(nil, netid.Open(), attestationInput(), testNow); err == nil {
		t.Fatal("nil key accepted")
	}
	if _, _, err := BuildReleaseRecord(testKey(t), netid.NetworkID{}, releaseInput(), testNow); !errors.Is(err, ErrInvalidField) {
		t.Fatalf("zero network: %v", err)
	}
}

func TestEncodeRejectsInvalid(t *testing.T) {
	r, _ := mustRelease(t, testKey(t), netid.Open())
	r.Title = "a/b"
	if _, err := r.Encode(); !errors.Is(err, ErrInvalidField) {
		t.Fatalf("release: %v", err)
	}
	a, _ := mustAttestation(t, testKey(t), netid.Open())
	a.TotalSegments = 0
	if _, err := a.Encode(); !errors.Is(err, ErrInvalidField) {
		t.Fatalf("attestation: %v", err)
	}
}

func TestEncodedSizeWithinLimit(t *testing.T) {
	in := releaseInput()
	in.Title = strings.Repeat("x", MaxTitleLen)
	_, enc, err := BuildReleaseRecord(testKey(t), netid.Open(), in, testNow)
	if err != nil || len(enc) > MaxEncodedSize {
		t.Fatalf("max release: %d %v", len(enc), err)
	}
	ain := attestationInput()
	ain.Backbones = nil
	for i := 0; i < MaxBackbones; i++ {
		ain.Backbones = append(ain.Backbones, string(rune('a'+i))+strings.Repeat("z", MaxBackboneLen-1))
	}
	_, enc, err = BuildHealthAttestation(testKey(t), netid.Open(), ain, testNow)
	if err != nil || len(enc) > MaxEncodedSize {
		t.Fatalf("max attestation: %d %v", len(enc), err)
	}
}
