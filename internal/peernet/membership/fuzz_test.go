package membership

import (
	"bytes"
	"testing"

	"github.com/kipsilabs/altmount/internal/peernet/identity"

	"github.com/kipsilabs/altmount/internal/peernet/netid"
)

func FuzzParseCertificate(f *testing.F) {
	root := newID(f)
	f.Add(issue(f, root, newID(f).NodeID(), RoleMember, t0).Marshal())
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		c, err := ParseCertificate(b)
		if err != nil {
			return
		}
		if !bytes.Equal(c.Marshal(), b) {
			t.Fatal("accepted non-canonical certificate")
		}
	})
}

func FuzzParseRevocationList(f *testing.F) {
	root := newID(f)
	l, _ := IssueRevocationList(root, netid.FromRootKey(root.PublicKey()), 3, t0,
		[]identity.NodeID{newID(f).NodeID(), newID(f).NodeID()})
	f.Add(l.Marshal())
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		l, err := ParseRevocationList(b)
		if err != nil {
			return
		}
		if !bytes.Equal(l.Marshal(), b) {
			t.Fatal("accepted non-canonical list")
		}
	})
}

func FuzzParseLink(f *testing.F) {
	_, s := goodLink(f)
	f.Add(s)
	f.Add("")
	f.Add(linkPrefix + "v=1")
	f.Fuzz(func(t *testing.T, in string) {
		l, err := ParseLink(in)
		if err != nil {
			return
		}
		out, err := l.Encode()
		if err != nil || out != in {
			t.Fatalf("accepted non-canonical link (re-encode err=%v)", err)
		}
	})
}

func FuzzParseInviteCode(f *testing.F) {
	c, _ := NewInviteCode()
	f.Add(c.Text())
	f.Fuzz(func(t *testing.T, s string) {
		c, err := ParseInviteCode(s)
		if err == nil && c.Text() != s {
			t.Fatal("non-canonical code accepted")
		}
	})
}

func FuzzParseMessages(f *testing.F) {
	root := newID(f)
	c := issue(f, root, newID(f).NodeID(), RoleMember, t0)
	l, _ := IssueRevocationList(root, netid.FromRootKey(root.PublicKey()), 1, t0, nil)
	code, _ := NewInviteCode()
	f.Add(joinResponse{Name: "n", Cert: c.Marshal(), Revocation: l.Marshal()}.marshal())
	f.Add(redeemRequest{Code: code}.marshal())
	f.Fuzz(func(t *testing.T, b []byte) {
		if r, err := parseJoinResponse(b); err == nil {
			if !bytes.Equal(r.marshal(), b) && len(r.Name) <= MaxNameLen {
				t.Fatal("non-canonical response")
			}
		}
		if r, err := parseRedeemRequest(b); err == nil && !bytes.Equal(r.marshal(), b) {
			t.Fatal("non-canonical redeem")
		}
		if n, err := parseRenewRequest(b); err == nil && !bytes.Equal(marshalRenewRequest(n), b) {
			t.Fatal("non-canonical renew")
		}
	})
}
