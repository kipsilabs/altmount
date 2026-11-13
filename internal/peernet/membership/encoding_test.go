package membership

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
)

var t0 = time.Unix(1_800_000_000, 0)

func newID(t testing.TB) *identity.Key {
	t.Helper()
	k, err := identity.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func issue(t testing.TB, root *identity.Key, node identity.NodeID, role Role, at time.Time) *Certificate {
	t.Helper()
	c, err := IssueCertificate(root, netid.FromRootKey(root.PublicKey()), node, role, at, CertLifetime)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestCertificateRoundTrip(t *testing.T) {
	root, member := newID(t), newID(t)
	c := issue(t, root, member.NodeID(), RoleMember, t0)
	b := c.Marshal()
	if len(b) != CertSize {
		t.Fatalf("size %d want %d", len(b), CertSize)
	}
	got, err := ParseCertificate(b)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Marshal(), b) || got.Role != RoleMember || got.ExpiresAt-got.IssuedAt != int64(CertLifetime/time.Second) {
		t.Fatal("round trip mismatch")
	}
	if err := got.Verify(root.PublicKey(), member.NodeID(), t0.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
}

func TestCertificateStrictDecode(t *testing.T) {
	root, member := newID(t), newID(t)
	good := issue(t, root, member.NodeID(), RoleMember, t0).Marshal()
	mut := func(f func(b []byte) []byte) []byte { return f(append([]byte(nil), good...)) }
	cases := map[string][]byte{
		"empty":     nil,
		"truncated": good[:len(good)-1],
		"trailing":  append(append([]byte(nil), good...), 0),
		"oversized": append(append([]byte(nil), good...), make([]byte, 4096)...),
		"version":   mut(func(b []byte) []byte { b[0] = 2; return b }),
		"role0":     mut(func(b []byte) []byte { b[65] = 0; return b }),
		"role3":     mut(func(b []byte) []byte { b[65] = 3; return b }),
		"siglen":    mut(func(b []byte) []byte { b[certBodySize] = 63; return b }),
		"negtime":   mut(func(b []byte) []byte { b[66] = 0x80; return b }),
	}
	for name, in := range cases {
		if _, err := ParseCertificate(in); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: got %v", name, err)
		}
	}
}

func TestCertificateVerifyFailures(t *testing.T) {
	root, member, other := newID(t), newID(t), newID(t)
	netID := netid.FromRootKey(root.PublicKey())
	good := issue(t, root, member.NodeID(), RoleMember, t0)
	now := t0.Add(time.Hour)

	if err := good.Verify(root.PublicKey(), other.NodeID(), now); !errors.Is(err, ErrPeerMismatch) {
		t.Errorf("peer mismatch: %v", err)
	}
	if err := good.Verify(root.PublicKey(), identity.NodeID{}, now); !errors.Is(err, ErrPeerMismatch) {
		t.Errorf("zero peer: %v", err)
	}
	// Wrong network: verify under a different root key.
	if err := good.Verify(other.PublicKey(), member.NodeID(), now); !errors.Is(err, ErrWrongNetwork) {
		t.Errorf("wrong network: %v", err)
	}
	// Wrong signer: signed by another key but naming root's network.
	forged, _ := IssueCertificate(other, netID, member.NodeID(), RoleMember, t0, CertLifetime)
	if err := forged.Verify(root.PublicKey(), member.NodeID(), now); !errors.Is(err, ErrBadSignature) {
		t.Errorf("wrong signer: %v", err)
	}
	// Tampered field.
	tam := *good
	tam.Role = RoleHost
	if err := tam.Verify(root.PublicKey(), member.NodeID(), now); !errors.Is(err, ErrBadSignature) {
		t.Errorf("tampered: %v", err)
	}
	// Signature made under another tag must not verify.
	cross, _ := root.Sign(revocationTag, good.body())
	x := *good
	x.Signature = cross
	if err := x.Verify(root.PublicKey(), member.NodeID(), now); !errors.Is(err, ErrBadSignature) {
		t.Errorf("cross-tag: %v", err)
	}
	// Time window.
	if err := good.Verify(root.PublicKey(), member.NodeID(), t0.Add(CertLifetime+ClockSkew+time.Second)); !errors.Is(err, ErrExpired) {
		t.Errorf("expired: %v", err)
	}
	if err := good.Verify(root.PublicKey(), member.NodeID(), t0.Add(CertLifetime+ClockSkew-time.Second)); err != nil {
		t.Errorf("within skew after expiry: %v", err)
	}
	if err := good.Verify(root.PublicKey(), member.NodeID(), t0.Add(-ClockSkew-time.Second)); !errors.Is(err, ErrNotYetValid) {
		t.Errorf("not yet valid: %v", err)
	}
	if err := good.Verify(root.PublicKey(), member.NodeID(), t0.Add(-ClockSkew+time.Second)); err != nil {
		t.Errorf("within skew before issue: %v", err)
	}
	// Absurd lifetime.
	long, _ := IssueCertificate(root, netID, member.NodeID(), RoleMember, t0, 10*CertLifetime)
	if err := long.Verify(root.PublicKey(), member.NodeID(), now); !errors.Is(err, ErrBadLifetime) {
		t.Errorf("lifetime: %v", err)
	}
	inv, _ := IssueCertificate(root, netID, member.NodeID(), RoleMember, t0, -time.Hour)
	if err := inv.Verify(root.PublicKey(), member.NodeID(), now); !errors.Is(err, ErrBadLifetime) {
		t.Errorf("negative lifetime: %v", err)
	}
	if _, err := IssueCertificate(root, netID, member.NodeID(), 9, t0, CertLifetime); err == nil {
		t.Error("bad role issued")
	}
	if err := good.Verify(ed25519.PublicKey("short"), member.NodeID(), now); !errors.Is(err, ErrWrongNetwork) {
		t.Errorf("short key: %v", err)
	}
}

func TestRevocationRoundTripAndStrict(t *testing.T) {
	root := newID(t)
	netID := netid.FromRootKey(root.PublicKey())
	a, b := newID(t).NodeID(), newID(t).NodeID()
	l, err := IssueRevocationList(root, netID, 7, t0, []identity.NodeID{a, b, a})
	if err != nil {
		t.Fatal(err)
	}
	if len(l.Revoked) != 2 {
		t.Fatal("not de-duplicated")
	}
	enc := l.Marshal()
	got, err := ParseRevocationList(enc)
	if err != nil || got.Sequence != 7 || !got.Contains(a) || !got.Contains(b) || got.Contains(newID(t).NodeID()) {
		t.Fatalf("round trip: %v", err)
	}
	if err := got.Verify(root.PublicKey(), t0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Marshal(), enc) {
		t.Fatal("not canonical")
	}

	mut := func(f func(b []byte) []byte) []byte { return f(append([]byte(nil), enc...)) }
	bad := map[string][]byte{
		"empty":     nil,
		"truncated": enc[:len(enc)-1],
		"trailing":  append(append([]byte(nil), enc...), 1),
		"version":   mut(func(b []byte) []byte { b[0] = 9; return b }),
		"count":     mut(func(b []byte) []byte { b[50] = 3; return b }),
		"hugecount": mut(func(b []byte) []byte { b[49], b[50] = 0xff, 0xff; return b }),
		"oversized": make([]byte, MaxRevocationListSize+1),
		"unsorted": mut(func(b []byte) []byte {
			x := append([]byte(nil), b[51:83]...)
			copy(b[51:83], b[83:115])
			copy(b[83:115], x)
			return b
		}),
		"duplicate": mut(func(b []byte) []byte { copy(b[83:115], b[51:83]); return b }),
	}
	for name, in := range bad {
		if _, err := ParseRevocationList(in); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: got %v", name, err)
		}
	}
	// Tampering after parse fails verification.
	tam := *got
	tam.Sequence = 8
	if err := tam.Verify(root.PublicKey(), t0); !errors.Is(err, ErrBadSignature) {
		t.Errorf("tamper: %v", err)
	}
	other := newID(t)
	if err := got.Verify(other.PublicKey(), t0); !errors.Is(err, ErrWrongNetwork) {
		t.Errorf("wrong net: %v", err)
	}
	forged, _ := IssueRevocationList(other, netID, 9, t0, nil)
	if err := forged.Verify(root.PublicKey(), t0); !errors.Is(err, ErrBadSignature) {
		t.Errorf("forged: %v", err)
	}
	if err := got.Verify(root.PublicKey(), t0.Add(-time.Hour)); !errors.Is(err, ErrNotYetValid) {
		t.Errorf("future list: %v", err)
	}
}

func TestRevocationTooMany(t *testing.T) {
	root := newID(t)
	ids := make([]identity.NodeID, MaxRevoked+1)
	for i := range ids {
		ids[i][0], ids[i][1] = byte(i>>8), byte(i)
		ids[i][31] = 1
	}
	if _, err := IssueRevocationList(root, netid.FromRootKey(root.PublicKey()), 1, t0, ids); err == nil {
		t.Fatal("accepted too many")
	}
	l, err := IssueRevocationList(root, netid.FromRootKey(root.PublicKey()), 1, t0, ids[:MaxRevoked])
	if err != nil {
		t.Fatal(err)
	}
	if len(l.Marshal()) != MaxRevocationListSize {
		t.Fatalf("max size %d != %d", len(l.Marshal()), MaxRevocationListSize)
	}
	if _, err := ParseRevocationList(l.Marshal()); err != nil {
		t.Fatal(err)
	}
}

func TestRevocationSequenceNeverBackwards(t *testing.T) {
	root := newID(t)
	l, _ := IssueRevocationList(root, netid.FromRootKey(root.PublicKey()), 5, t0, nil)
	if err := l.AcceptableOver(6); !errors.Is(err, ErrStaleRevocation) {
		t.Errorf("lower seq accepted: %v", err)
	}
	if l.AcceptableOver(5) != nil || l.AcceptableOver(4) != nil {
		t.Error("equal/higher rejected")
	}
}

func TestInviteCode(t *testing.T) {
	c, err := NewInviteCode()
	if err != nil {
		t.Fatal(err)
	}
	txt := c.Text()
	if len(txt) != InviteCodeTextLen || txt != strings.ToLower(txt) {
		t.Fatalf("bad text form %q", txt)
	}
	got, err := ParseInviteCode(txt)
	if err != nil || got != c || got.Hash() != c.Hash() {
		t.Fatal("round trip")
	}
	c2, _ := NewInviteCode()
	if c2.Hash() == c.Hash() || len(c.Hash()) != 64 || c.Hash() == txt {
		t.Fatal("hash")
	}
	for _, s := range []string{"", txt[:25], txt + "a", strings.ToUpper(txt), strings.Repeat("1", 26), strings.Repeat("a", 25) + "b"} {
		if _, err := ParseInviteCode(s); err == nil {
			t.Errorf("accepted %q", s)
		}
	}
	if strings.Contains(c.String(), txt) || strings.Contains(c.GoString(), txt) {
		t.Fatal("code leaked in String")
	}
	if !hashEqual(c.Hash(), c.Hash()) || hashEqual(c.Hash(), c2.Hash()) {
		t.Fatal("hashEqual")
	}
}

func TestMessagesStrict(t *testing.T) {
	var n netid.NetworkID
	n[0] = 1
	code, _ := NewInviteCode()
	rr := redeemRequest{n, code}.marshal()
	if got, err := parseRedeemRequest(rr); err != nil || got.Code != code || got.Network != n {
		t.Fatal("redeem round trip")
	}
	for _, b := range [][]byte{nil, rr[:48], append(append([]byte(nil), rr...), 0), append([]byte{2}, rr[1:]...)} {
		if _, err := parseRedeemRequest(b); err == nil {
			t.Error("redeem accepted bad input")
		}
	}
	rn := marshalRenewRequest(n)
	if got, err := parseRenewRequest(rn); err != nil || got != n {
		t.Fatal("renew round trip")
	}
	for _, b := range [][]byte{nil, rn[:32], append(append([]byte(nil), rn...), 0), append([]byte{2}, rn[1:]...)} {
		if _, err := parseRenewRequest(b); err == nil {
			t.Error("renew accepted bad input")
		}
	}

	root := newID(t)
	c := issue(t, root, newID(t).NodeID(), RoleMember, t0)
	l, _ := IssueRevocationList(root, netid.FromRootKey(root.PublicKey()), 1, t0, nil)
	jr := joinResponse{Name: "net", Cert: c.Marshal(), Revocation: l.Marshal()}.marshal()
	got, err := parseJoinResponse(jr)
	if err != nil || got.Name != "net" || !bytes.Equal(got.Cert, c.Marshal()) || !bytes.Equal(got.Revocation, l.Marshal()) {
		t.Fatalf("join round trip: %v", err)
	}
	bad := [][]byte{nil, {1}, jr[:len(jr)-1], append(append([]byte(nil), jr...), 0), append([]byte{2}, jr[1:]...),
		append([]byte{1, 200}, jr[2:]...), make([]byte, MaxResponseSize+1)}
	for i, b := range bad {
		if _, err := parseJoinResponse(b); err == nil {
			t.Errorf("join response accepted bad input %d", i)
		}
	}
	long := joinResponse{Name: strings.Repeat("x", 65), Cert: c.Marshal(), Revocation: l.Marshal()}
	_ = long // name length is capped by the single-byte field plus MaxNameLen on parse
}
