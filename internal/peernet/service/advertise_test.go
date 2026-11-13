package service

import (
	"context"
	"strings"
	"testing"

	"github.com/kipsilabs/altmount/internal/database"
)

func TestAdvertiseWarning(t *testing.T) {
	open := database.PeernetNetwork{Role: database.PeernetRoleOpen}
	priv := func(level int, allow bool) database.PeernetNetwork {
		return database.PeernetNetwork{Role: database.PeernetRoleHost, ExposureLevel: level, AllowPrivateAddresses: allow}
	}
	cases := []struct {
		name  string
		adv   string
		rows  []database.PeernetNetwork
		level int
		want  string // substring, "" for no warning
	}{
		{"none set", "", []database.PeernetNetwork{open}, 1, ""},
		{"public ok", "8.8.8.8:8470", []database.PeernetNetwork{open}, 1, ""},
		{"private on open network", "192.168.1.5:8470", []database.PeernetNetwork{open}, 1, "private"},
		{"private but open network off", "192.168.1.5:8470", []database.PeernetNetwork{open}, 0, ""},
		{"private on allow-private network", "192.168.1.5:8470", []database.PeernetNetwork{open, priv(1, true)}, 0, ""},
		{"private on strict private network", "192.168.1.5:8470", []database.PeernetNetwork{priv(1, false)}, 0, "private"},
		{"loopback always", "127.0.0.1:8470", []database.PeernetNetwork{priv(1, true)}, 0, "loopback"},
		{"local name", "nas.local:8470", []database.PeernetNetwork{open}, 1, "private"},
	}
	for _, tc := range cases {
		got := advertiseWarning(tc.adv, tc.rows, Settings{OpenLevel: tc.level}, false)
		if (tc.want == "") != (got == "") || !strings.Contains(got, tc.want) {
			t.Errorf("%s: got %q, want substring %q", tc.name, got, tc.want)
		}
		if tc.adv != "" && strings.Contains(got, tc.adv) {
			t.Errorf("%s: warning leaks the address", tc.name)
		}
	}
}

func TestCreateNetworkDefaultsTheHostAddressToTheAdvertiseAddress(t *testing.T) {
	for name, tc := range map[string]struct{ adv, want string }{
		"public":  {"8.8.8.8:8470", "8.8.8.8:8470"},
		"private": {"192.168.1.5:8470", ""}, // a new network does not allow private addresses
		"none":    {"", ""},
	} {
		t.Run(name, func(t *testing.T) {
			i := newInstance(t, Settings{Enabled: true, AcceptIncoming: true, AdvertiseAddress: tc.adv})
			i.start(t)
			ns, err := i.svc.CreateNetwork(context.Background(), "Club")
			if err != nil {
				t.Fatal(err)
			}
			if ns.HostAddress != tc.want {
				t.Fatalf("host address = %q, want %q", ns.HostAddress, tc.want)
			}
		})
	}
}
