package records

import (
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
)

var testNow = time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)

func testKey(t testing.TB) *identity.Key {
	t.Helper()
	k, err := identity.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func fill32(v byte) [32]byte {
	var a [32]byte
	for i := range a {
		a[i] = v
	}
	return a
}

func u32(v uint32) *uint32 { return &v }

func releaseInput() ReleaseRecord {
	return ReleaseRecord{
		BundleID:  BundleID(fill32(3)),
		ReleaseID: ReleaseID(fill32(2)),
		Title:     "Some.Release.2024.1080p",
		TotalSize: 5 << 30,
		FileCount: 4,
		PostDate:  testNow.Add(-48 * time.Hour),
		Media: MediaIDs{
			IMDb: "tt1234567", TMDB: 603, TVDB: 78804,
			Season: u32(0), Episode: u32(12),
		},
	}
}

func attestationInput() HealthAttestation {
	return HealthAttestation{
		ReleaseID:           ReleaseID(fill32(2)),
		FileID:              FileID(fill32(4)),
		CheckedAt:           testNow.Add(-time.Hour),
		TotalSegments:       1000,
		SegmentsChecked:     200,
		MissingCount:        0,
		MediaHeaderVerified: true,
		Backbones:           []string{"omicron", "abavia"},
	}
}

func mustRelease(t testing.TB, k *identity.Key, nw netid.NetworkID) (*ReleaseRecord, []byte) {
	t.Helper()
	r, b, err := BuildReleaseRecord(k, nw, releaseInput(), testNow)
	if err != nil {
		t.Fatal(err)
	}
	return r, b
}

func mustAttestation(t testing.TB, k *identity.Key, nw netid.NetworkID) (*HealthAttestation, []byte) {
	t.Helper()
	a, b, err := BuildHealthAttestation(k, nw, attestationInput(), testNow)
	if err != nil {
		t.Fatal(err)
	}
	return a, b
}
