package discovery

import (
	"strings"
	"testing"

	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

func FuzzDecodeHello(f *testing.F) {
	f.Add([]byte(`{"address":"8.8.8.8:1"}`))
	f.Add([]byte(`{"address":""}`))
	f.Add([]byte(`{"address":"x"} {}`))
	f.Add([]byte(`null`))
	f.Fuzz(func(t *testing.T, b []byte) {
		addr, err := DecodeHello(b)
		if err != nil {
			return
		}
		if len(b) > MaxHelloBody || len(addr) > 255 {
			t.Fatalf("limits violated: %d %d", len(b), len(addr))
		}
		// Anything accepted must also survive the address policy without panic.
		_, _ = transport.CheckPeerAddress(addr, false, false)
	})
}

func FuzzDecodePeers(f *testing.F) {
	f.Add([]byte(`{"peers":[{"node_id":"` + strings.Repeat("a", 64) + `","address":"8.8.8.8:1"}]}`))
	f.Add([]byte(`{"peers":null}`))
	f.Add([]byte(`{"peers":[]}x`))
	f.Fuzz(func(t *testing.T, b []byte) {
		es, err := DecodePeers(b)
		if err != nil {
			return
		}
		if len(b) > MaxPeersResponseBytes || len(es) > MaxPeerEntries {
			t.Fatalf("limits violated")
		}
		self, from := identity.NodeID{1}, identity.NodeID{2}
		out := sanitizeEntries(es, self, from, false)
		seen := map[identity.NodeID]bool{}
		for _, p := range out {
			if p.node.IsZero() || p.node.Equal(self) || p.node.Equal(from) || seen[p.node] {
				t.Fatalf("bad sanitized entry %+v", p)
			}
			seen[p.node] = true
			pa, err := transport.CheckLearnedAddress(p.addr, false, false)
			if err != nil || pa.HostPort() != p.addr || !pa.IP.IsValid() || transport.PortDenied(pa.Port) {
				t.Fatalf("sanitized address not canonical/allowed: %q", p.addr)
			}
		}
	})
}

func FuzzSanitizeEntries(f *testing.F) {
	f.Add(strings.Repeat("b", 64), "8.8.8.8:80", false)
	f.Add(strings.Repeat("b", 64), "10.0.0.1:80", true)
	f.Add("nothex", "[::1]:1", false)
	f.Fuzz(func(t *testing.T, id, addr string, priv bool) {
		out := sanitizeEntries([]PeerEntry{{NodeID: id, Address: addr}}, identity.NodeID{}, identity.NodeID{}, false)
		for _, p := range out {
			// Whatever the network allows, exchange never yields a private
			// address, a hostname or a denied port.
			if _, err := transport.CheckLearnedAddress(p.addr, priv, false); err != nil {
				t.Fatalf("accepted %q: %v", p.addr, err)
			}
			if _, err := transport.CheckPeerAddress(p.addr, false, false); err != nil {
				t.Fatalf("accepted non-public %q: %v", p.addr, err)
			}
		}
	})
}
