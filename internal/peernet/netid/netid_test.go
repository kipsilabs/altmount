package netid

import (
	"crypto/ed25519"
	"crypto/rand"
	"strings"
	"testing"
)

func TestOpenStableAndNonZero(t *testing.T) {
	if Open().IsZero() || !Open().Equal(Open()) || !Open().IsOpen() {
		t.Fatal("open id broken")
	}
}

func TestFromRootKeyBoundToKey(t *testing.T) {
	a, _, _ := ed25519.GenerateKey(rand.Reader)
	b, _, _ := ed25519.GenerateKey(rand.Reader)
	if FromRootKey(a).Equal(FromRootKey(b)) {
		t.Fatal("different keys, same id")
	}
	if !FromRootKey(a).Equal(FromRootKey(a)) {
		t.Fatal("not deterministic")
	}
	if FromRootKey(a).Equal(Open()) || FromRootKey(a).IsOpen() {
		t.Fatal("private equals open")
	}
}

func TestParseRoundTripAndStrict(t *testing.T) {
	id := Open()
	got, err := ParseNetworkID(id.String())
	if err != nil || !got.Equal(id) {
		t.Fatal(err)
	}
	for _, s := range []string{"", id.String()[:63], id.String() + "0", strings.ToUpper(id.String()), strings.Repeat("g", 64), " " + id.String()[1:]} {
		if _, err := ParseNetworkID(s); err == nil {
			t.Errorf("accepted %q", s)
		}
	}
}

func TestText(t *testing.T) {
	var n NetworkID
	if err := n.UnmarshalText([]byte(Open().String())); err != nil || !n.Equal(Open()) {
		t.Fatal(err)
	}
	if err := n.UnmarshalText([]byte("x")); err == nil {
		t.Fatal("accepted")
	}
	b, _ := n.MarshalText()
	if string(b) != n.String() || n.IsZero() {
		t.Fatal("marshal")
	}
}
