package membership

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/kipsilabs/altmount/internal/peernet/netid"
)

func goodLink(t testing.TB) (Link, string) {
	t.Helper()
	root := newID(t)
	code, _ := NewInviteCode()
	l := Link{NetworkID: netid.FromRootKey(root.PublicKey()), RootKey: root.PublicKey(),
		HostAddr: "host.example.org:7777", HostNode: newID(t).NodeID(), Code: code}
	s, err := l.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return l, s
}

func TestLinkRoundTrip(t *testing.T) {
	l, s := goodLink(t)
	if !strings.HasPrefix(s, "altmountnet://join?v=1&n=") || len(s) > MaxLinkLen {
		t.Fatalf("bad link shape %d", len(s))
	}
	got, err := ParseLink(s)
	if err != nil {
		t.Fatal(err)
	}
	if got.NetworkID != l.NetworkID || got.HostNode != l.HostNode || got.Code != l.Code ||
		got.HostAddr != l.HostAddr || !got.RootKey.Equal(l.RootKey) {
		t.Fatal("mismatch")
	}
	if strings.Contains(fmt.Sprintf("%v %+v %#v %s", got, got, got, got), l.Code.Text()) {
		t.Fatal("link String leaks code")
	}
	// IPv6 literal.
	l.HostAddr = "[2001:db8::1]:80"
	s6, err := l.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if g, err := ParseLink(s6); err != nil || g.HostAddr != "[2001:db8::1]:80" {
		t.Fatalf("ipv6: %v", err)
	}
}

func TestLinkHostile(t *testing.T) {
	l, s := goodLink(t)
	other, _ := goodLink(t)
	repl := func(old, new string) string { return strings.Replace(s, old, new, 1) }
	cases := map[string]string{
		"empty":        "",
		"scheme":       strings.Replace(s, "altmountnet", "https", 1),
		"prefixonly":   linkPrefix,
		"oversized":    s + strings.Repeat("a", MaxLinkLen),
		"extra field":  s + "&x=1",
		"missing":      s[:strings.LastIndex(s, "&")],
		"reordered":    linkPrefix + "n=" + l.NetworkID.String() + "&v=1" + s[strings.Index(s, "&k="):],
		"dup":          s + "&c=" + l.Code.Text(),
		"version":      repl("v=1", "v=2"),
		"emptyvalue":   repl("&c="+l.Code.Text(), "&c="),
		"upper net":    repl(l.NetworkID.String(), strings.ToUpper(l.NetworkID.String())),
		"short net":    repl(l.NetworkID.String(), l.NetworkID.String()[:62]),
		"bad addr":     repl("host.example.org:7777", "host.example.org:0"),
		"addr path":    repl("host.example.org:7777", "host.example.org:7777/x"),
		"addr userinf": repl("host.example.org:7777", "u@host.example.org:7777"),
		"addr upper":   repl("host.example.org:7777", "Host.Example.org:7777"),
		"addr pct":     repl("host.example.org:7777", "host%2eexample.org:7777"),
		"addr ctl":     repl("host.example.org:7777", "host.example.org:7777\n"),
		"addr amp":     repl("host.example.org:7777", "a&b.org:7777"),
		"short key":    repl("&k=", "&k=AAAA"),
		"b64 padding":  strings.Replace(s, "&a=", "=&a=", 1),
		"bad node":     repl(l.HostNode.String(), "zz"+l.HostNode.String()[2:]),
		"bad code":     repl(l.Code.Text(), strings.ToUpper(l.Code.Text())),
		"newline":      s + "\n",
		"space":        s + " ",
		"nul":          s + "\x00",
		"other key":    strings.Replace(s, "&k="+kOf(s), "&k="+kOf(mustEncode(t, other)), 1),
	}
	for name, in := range cases {
		if _, err := ParseLink(in); err == nil {
			t.Errorf("%s: accepted", name)
		} else if strings.Contains(err.Error(), l.Code.Text()) {
			t.Errorf("%s: error leaks code", name)
		}
	}
	if _, err := ParseLink(strings.Replace(s, "&k="+kOf(s), "&k="+kOf(mustEncode(t, other)), 1)); !errors.Is(err, ErrWrongNetwork) {
		t.Errorf("key not bound to network id: %v", err)
	}
}

func mustEncode(t testing.TB, l Link) string {
	s, err := l.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func kOf(s string) string {
	i := strings.Index(s, "&k=") + 3
	return s[i : i+43]
}

func TestLinkEncodeRejects(t *testing.T) {
	l, _ := goodLink(t)
	bad := l
	bad.HostAddr = "http://x"
	if _, err := bad.Encode(); err == nil {
		t.Error("bad address encoded")
	}
	bad = l
	bad.NetworkID = netid.Open()
	if _, err := bad.Encode(); !errors.Is(err, ErrWrongNetwork) {
		t.Error("unbound id encoded")
	}
}
