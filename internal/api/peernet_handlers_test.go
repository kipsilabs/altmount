package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	fiberlogger "github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/discovery"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/membership"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/service"
	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

const (
	testLink = "altmountnet://join?v=1&n=SECRETLINK"
	hex64    = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
)

type fakePeernet struct {
	err      error
	status   service.Status
	lastName string
	lastLink string
	lastUpd  service.NetworkUpdate
	lastTTL  time.Duration
	lastAddr string
	calls    []string
}

func (f *fakePeernet) rec(n string) error { f.calls = append(f.calls, n); return f.err }

func (f *fakePeernet) Status(context.Context) (service.Status, error) {
	return f.status, f.rec("status")
}
func (f *fakePeernet) Peers(context.Context, netid.NetworkID) ([]discovery.PeerStatus, error) {
	return []discovery.PeerStatus{
		{NodeID: hex64, Address: "a:1", Source: "user", Verified: true, FirstSeen: time.Unix(100, 0)},
		{Address: "b:2", Source: "user", Unresolved: true, LastError: "boom"},
	}, f.rec("peers")
}
func (f *fakePeernet) ConfirmPeer(_ context.Context, _ netid.NetworkID, a string) error {
	f.lastAddr = a
	return f.rec("confirm")
}
func (f *fakePeernet) CreateNetwork(_ context.Context, n string) (service.NetworkStatus, error) {
	f.lastName = n
	return service.NetworkStatus{ID: hex64, Name: n, Kind: "private", Role: "host"}, f.rec("create")
}
func (f *fakePeernet) JoinNetwork(_ context.Context, l string, _ bool) (service.NetworkStatus, error) {
	f.lastLink = l
	return service.NetworkStatus{ID: hex64, Kind: "private", Role: "member"}, f.rec("join")
}
func (f *fakePeernet) UpdateNetwork(_ context.Context, _ netid.NetworkID, u service.NetworkUpdate) (service.NetworkStatus, error) {
	f.lastUpd = u
	return service.NetworkStatus{ID: hex64, Kind: "private", Role: "host", CertExpiresAt: time.Unix(1000, 0)}, f.rec("update")
}
func (f *fakePeernet) DeleteNetwork(context.Context, netid.NetworkID) error { return f.rec("delete") }
func (f *fakePeernet) ListInvites(context.Context, netid.NetworkID) ([]service.Invite, error) {
	return []service.Invite{{ID: hex64, Alias: "bob", CreatedAt: time.Unix(10, 0), ExpiresAt: time.Unix(20, 0)}}, f.rec("invites")
}
func (f *fakePeernet) CreateInvite(_ context.Context, _ netid.NetworkID, ttl time.Duration, _ string) (string, error) {
	f.lastTTL = ttl
	return testLink, f.rec("createInvite")
}
func (f *fakePeernet) RevokeInvite(context.Context, netid.NetworkID, string) error {
	return f.rec("revoke")
}
func (f *fakePeernet) ListMembers(context.Context, netid.NetworkID) ([]service.Member, error) {
	return []service.Member{{NodeID: hex64, Alias: "m", JoinedAt: time.Unix(5, 0)}}, f.rec("members")
}
func (f *fakePeernet) RemoveMember(context.Context, netid.NetworkID, identity.NodeID) error {
	return f.rec("remove")
}

func peernetApp(p PeernetService) *fiber.App {
	s := &Server{peernet: p}
	app := fiber.New()
	s.registerPeernetRoutes(app.Group("/api"))
	return app
}

func doReq(t *testing.T, app *fiber.App, method, path, body string) (int, map[string]any, string) {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	return resp.StatusCode, m, string(raw)
}

func errMsg(m map[string]any) string {
	e, _ := m["error"].(map[string]any)
	s, _ := e["message"].(string)
	return s
}

func TestPeernetStatusShape(t *testing.T) {
	f := &fakePeernet{status: service.Status{Enabled: true, NodeID: hex64, Listening: true, ListenPort: 8470,
		AcceptIncoming: true, Networks: []service.NetworkStatus{{ID: hex64, Name: "Open network", Kind: "open", Role: "open", Level: 1}},
		Backfill: service.BackfillStatus{Scanned: 3}}}
	code, m, raw := doReq(t, peernetApp(f), "GET", "/api/peernet/status", "")
	require.Equal(t, 200, code, raw)
	d := m["data"].(map[string]any)
	for _, k := range []string{"enabled", "node_id", "listening", "listen_port", "listen_error", "accept_incoming", "networks", "backfill"} {
		assert.Contains(t, d, k)
	}
	n := d["networks"].([]any)[0].(map[string]any)
	assert.Nil(t, n["cert_expires_at"])
	assert.Equal(t, "", n["host_address"])
	assert.Contains(t, n["peers"], "node_id_changed")
	assert.EqualValues(t, 3, d["backfill"].(map[string]any)["scanned"])
}

func TestPeernetDisabledState(t *testing.T) {
	f := &fakePeernet{}
	code, m, _ := doReq(t, peernetApp(f), "GET", "/api/peernet/status", "")
	require.Equal(t, 200, code)
	d := m["data"].(map[string]any)
	assert.Equal(t, false, d["enabled"])
	assert.Equal(t, "", d["node_id"])
	assert.Empty(t, d["networks"])

	f.err = service.ErrDisabled
	for _, tc := range []struct{ method, path, body string }{
		{"POST", "/api/peernet/networks", `{"name":"x"}`},
		{"POST", "/api/peernet/networks/join", `{"link":"` + testLink + `"}`},
		{"PATCH", "/api/peernet/networks/" + hex64, `{"level":1}`},
		{"DELETE", "/api/peernet/networks/" + hex64, ``},
		{"POST", "/api/peernet/networks/" + hex64 + "/invites", `{"ttl_hours":1}`},
		{"DELETE", "/api/peernet/networks/" + hex64 + "/invites/" + hex64, ``},
		{"DELETE", "/api/peernet/networks/" + hex64 + "/members/" + hex64, ``},
		{"POST", "/api/peernet/networks/" + hex64 + "/peers/confirm", `{"address":"a.example:1"}`},
	} {
		code, m, raw := doReq(t, peernetApp(f), tc.method, tc.path, tc.body)
		assert.Equal(t, 409, code, tc.method+" "+tc.path)
		assert.Equal(t, "Peer network is disabled", errMsg(m), raw)
	}
}

func TestPeernetNotWired(t *testing.T) {
	code, _, _ := doReq(t, peernetApp(nil), "GET", "/api/peernet/status", "")
	assert.Equal(t, 503, code)
}

func TestPeernetListEndpoints(t *testing.T) {
	app := peernetApp(&fakePeernet{})
	code, m, raw := doReq(t, app, "GET", "/api/peernet/networks/"+hex64+"/peers", "")
	require.Equal(t, 200, code, raw)
	ps := m["data"].([]any)
	require.Len(t, ps, 2)
	p0 := ps[0].(map[string]any)
	assert.Equal(t, "1970-01-01T00:01:40Z", p0["first_seen"])
	assert.Nil(t, p0["last_success"])
	assert.Equal(t, "", p0["last_error"])
	assert.Equal(t, "", ps[1].(map[string]any)["node_id"])

	code, m, raw = doReq(t, app, "GET", "/api/peernet/networks/"+hex64+"/invites", "")
	require.Equal(t, 200, code, raw)
	inv := m["data"].([]any)[0].(map[string]any)
	assert.Nil(t, inv["used_at"])
	assert.NotContains(t, raw, "code")
	assert.NotContains(t, raw, "link")

	code, m, raw = doReq(t, app, "GET", "/api/peernet/networks/"+hex64+"/members", "")
	require.Equal(t, 200, code, raw)
	mem := m["data"].([]any)[0].(map[string]any)
	assert.Nil(t, mem["revoked_at"])
	assert.NotNil(t, mem["joined_at"])
}

func TestPeernetMutationsHappyPath(t *testing.T) {
	f := &fakePeernet{}
	app := peernetApp(f)

	code, _, raw := doReq(t, app, "POST", "/api/peernet/networks", `{"name":"  Club  "}`)
	require.Equal(t, 201, code, raw)
	assert.Equal(t, "Club", f.lastName)

	code, _, raw = doReq(t, app, "POST", "/api/peernet/networks/join", `{"link":"`+testLink+`","allow_private_addresses":true}`)
	require.Equal(t, 201, code, raw)
	assert.Equal(t, testLink, f.lastLink)
	assert.NotContains(t, raw, "SECRETLINK")

	code, m, raw := doReq(t, app, "PATCH", "/api/peernet/networks/"+hex64, `{"level":2,"host_address":"host.example:8470"}`)
	require.Equal(t, 200, code, raw)
	require.NotNil(t, f.lastUpd.Level)
	assert.Equal(t, 2, *f.lastUpd.Level)
	assert.Equal(t, "host.example:8470", *f.lastUpd.HostAddress)
	assert.NotNil(t, m["data"].(map[string]any)["cert_expires_at"])

	code, _, raw = doReq(t, app, "DELETE", "/api/peernet/networks/"+hex64, "")
	assert.Equal(t, 204, code, raw)
	code, _, _ = doReq(t, app, "POST", "/api/peernet/networks/"+hex64+"/peers/confirm", `{"address":"Host.Example:8470"}`)
	assert.Equal(t, 204, code)
	assert.Equal(t, "host.example:8470", f.lastAddr)
	code, _, _ = doReq(t, app, "DELETE", "/api/peernet/networks/"+hex64+"/invites/"+hex64, "")
	assert.Equal(t, 204, code)
	code, _, _ = doReq(t, app, "DELETE", "/api/peernet/networks/"+hex64+"/members/"+hex64, "")
	assert.Equal(t, 204, code)
}

func TestPeernetInviteLinkOnlyInCreateResponse(t *testing.T) {
	f := &fakePeernet{}
	app := peernetApp(f)
	code, m, raw := doReq(t, app, "POST", "/api/peernet/networks/"+hex64+"/invites", `{"ttl_hours":24,"alias":"bob"}`)
	require.Equal(t, 201, code, raw)
	assert.Equal(t, testLink, m["data"].(map[string]any)["link"])
	assert.Equal(t, 24*time.Hour, f.lastTTL)
	for _, p := range []string{"/invites", "/members", "/peers"} {
		_, _, body := doReq(t, app, "GET", "/api/peernet/networks/"+hex64+p, "")
		assert.NotContains(t, body, "SECRETLINK", p)
	}
	_, _, body := doReq(t, app, "GET", "/api/peernet/status", "")
	assert.NotContains(t, body, "SECRETLINK")
}

func TestPeernetValidationFailures(t *testing.T) {
	long := strings.Repeat("a", 65)
	cases := []struct{ name, method, path, body string }{
		{"short id", "GET", "/api/peernet/networks/abc/peers", ""},
		{"upper id", "GET", "/api/peernet/networks/" + strings.ToUpper(hex64) + "/peers", ""},
		{"bad invite id", "DELETE", "/api/peernet/networks/" + hex64 + "/invites/xyz", ""},
		{"bad node id", "DELETE", "/api/peernet/networks/" + hex64 + "/members/xyz", ""},
		{"create bad json", "POST", "/api/peernet/networks", `{`},
		{"create empty name", "POST", "/api/peernet/networks", `{"name":"  "}`},
		{"create long name", "POST", "/api/peernet/networks", `{"name":"` + long + `"}`},
		{"create control name", "POST", "/api/peernet/networks", `{"name":"a\u0000b"}`},
		{"join empty", "POST", "/api/peernet/networks/join", `{"link":""}`},
		{"join huge", "POST", "/api/peernet/networks/join", `{"link":"` + strings.Repeat("a", 600) + `"}`},
		{"patch level", "PATCH", "/api/peernet/networks/" + hex64, `{"level":4}`},
		{"patch negative", "PATCH", "/api/peernet/networks/" + hex64, `{"level":-1}`},
		{"patch address", "PATCH", "/api/peernet/networks/" + hex64, `{"host_address":"https://x"}`},
		{"patch empty", "PATCH", "/api/peernet/networks/" + hex64, `{}`},
		{"ttl zero", "POST", "/api/peernet/networks/" + hex64 + "/invites", `{"ttl_hours":0}`},
		{"ttl huge", "POST", "/api/peernet/networks/" + hex64 + "/invites", `{"ttl_hours":721}`},
		{"alias long", "POST", "/api/peernet/networks/" + hex64 + "/invites", `{"ttl_hours":1,"alias":"` + long + `"}`},
		{"confirm bad addr", "POST", "/api/peernet/networks/" + hex64 + "/peers/confirm", `{"address":"nope"}`},
		{"body too large", "POST", "/api/peernet/networks", `{"name":"` + strings.Repeat("a", 5000) + `"}`},
	}
	for _, tc := range cases {
		f := &fakePeernet{}
		code, m, raw := doReq(t, peernetApp(f), tc.method, tc.path, tc.body)
		assert.Equal(t, 400, code, tc.name+": "+raw)
		assert.Equal(t, false, m["success"], tc.name)
		assert.Empty(t, f.calls, tc.name+" reached the service")
	}
}

func TestPeernetErrorMappingHidesInternals(t *testing.T) {
	cases := []struct {
		err  error
		code int
	}{
		{service.ErrOpenNetwork, 400},
		{service.ErrAcceptIncomingRequired, 400},
		{membership.ErrUnknownNetwork, 404},
		{database.ErrPeernetNotFound, 404},
		{membership.ErrNotHost, 400},
		{membership.ErrNoHostAddress, 400},
		{membership.ErrAlreadyJoined, 409},
		{membership.ErrMalformed, 400},
		{membership.ErrDenied, 400},
		{discovery.ErrUnknownNetwork, 400},
		{discovery.ErrNothingToConfirm, 409},
		{errors.New("dial tcp 10.1.2.3:99: " + testLink), 500},
	}
	for _, tc := range cases {
		f := &fakePeernet{err: tc.err}
		code, _, raw := doReq(t, peernetApp(f), "POST", "/api/peernet/networks/join", `{"link":"`+testLink+`"}`)
		assert.Equal(t, tc.code, code, tc.err.Error())
		assert.NotContains(t, raw, "SECRETLINK", raw)
		assert.NotContains(t, raw, "10.1.2.3")
	}
}

func TestPeernetOpenNetworkGuards(t *testing.T) {
	f := &fakePeernet{err: service.ErrOpenNetwork}
	app := peernetApp(f)
	code, m, _ := doReq(t, app, "PATCH", "/api/peernet/networks/"+hex64, `{"level":2}`)
	assert.Equal(t, 400, code)
	assert.Contains(t, errMsg(m), "open network")
	code, _, _ = doReq(t, app, "DELETE", "/api/peernet/networks/"+hex64, "")
	assert.Equal(t, 400, code)
}

func TestPatchPeernetConfigSection(t *testing.T) {
	cfg := config.DefaultConfig()
	manager := config.NewManager(cfg, filepath.Join(t.TempDir(), "config.yaml"))
	s := &Server{configManager: manager}
	app := fiber.New()
	app.Patch("/api/config/:section", s.handlePatchConfigSection)

	patch := func(body string) (int, string) {
		req := httptest.NewRequest(http.MethodPatch, "/api/config/peernet", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := app.Test(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(raw)
	}

	code, raw := patch(`{"peernet":{"enabled":true,"accept_incoming":true,"listen_port":9000,"advertise_address":"example.org:9000",` +
		`"open_network":{"level":1,"peers":["peer.example:8470"]},"sharing":{"excluded_categories":[],"excluded_indexers":[]},"min_corroboration":3}}`)
	require.Equal(t, 200, code, raw)
	got := manager.GetConfig()
	assert.True(t, got.PeernetEnabled())
	assert.Equal(t, 9000, got.PeernetListenPort())
	assert.Equal(t, config.PeernetLevel(1), got.PeernetOpenNetworkLevel())
	assert.Equal(t, []string{"peer.example:8470"}, got.Peernet.OpenNetwork.Peers)
	assert.Contains(t, raw, `"peernet"`)

	for name, body := range map[string]string{
		"port too low":  `{"peernet":{"enabled":true,"listen_port":80,"min_corroboration":2,"open_network":{"level":0,"peers":[]}}}`,
		"bad level":     `{"peernet":{"enabled":true,"listen_port":9000,"min_corroboration":2,"open_network":{"level":7,"peers":[]}}}`,
		"bad peer":      `{"peernet":{"enabled":true,"listen_port":9000,"min_corroboration":2,"open_network":{"level":1,"peers":["https://x/y"]}}}`,
		"loopback peer": `{"peernet":{"enabled":true,"listen_port":9000,"min_corroboration":2,"open_network":{"level":1,"peers":["127.0.0.1:8470"]}}}`,
		"bad corrob":    `{"peernet":{"enabled":true,"listen_port":9000,"min_corroboration":0,"open_network":{"level":0,"peers":[]}}}`,
		"bad advertise": `{"peernet":{"enabled":true,"listen_port":9000,"advertise_address":"a b:1","min_corroboration":2,"open_network":{"level":0,"peers":[]}}}`,
	} {
		code, raw := patch(body)
		assert.Equal(t, 400, code, name+": "+raw)
	}
	assert.Equal(t, 9000, manager.GetConfig().PeernetListenPort(), "rejected patch must not change config")
}

func TestPeernetAddressesUseTheTransportValidator(t *testing.T) {
	long := strings.Repeat("a", 60) + "." + strings.Repeat("b", 60) + "." + strings.Repeat("c", 60) + "." +
		strings.Repeat("d", 60) + ".example:65535" // valid host, 257 bytes in all
	for _, a := range []string{"example.1com:80", "host:+80", "host:000080", long, "1.2.3:80"} {
		f := &fakePeernet{}
		code, _, raw := doReq(t, peernetApp(f), "POST", "/api/peernet/networks/"+hex64+"/peers/confirm",
			`{"address":"`+a+`"}`)
		assert.Equal(t, 400, code, a+": "+raw)
		assert.Empty(t, f.calls, a)
	}
	f := &fakePeernet{}
	code, _, raw := doReq(t, peernetApp(f), "POST", "/api/peernet/networks/"+hex64+"/peers/confirm",
		`{"address":"Peer.Example.org:8470"}`)
	assert.Equal(t, 204, code, raw)
	assert.Equal(t, "peer.example.org:8470", f.lastAddr)
}

func TestPeernetOpenNetworkLevelErrorMapped(t *testing.T) {
	f := &fakePeernet{err: membership.ErrOpenNetwork}
	code, _, _ := doReq(t, peernetApp(f), "PATCH", "/api/peernet/networks/"+hex64, `{"level":2}`)
	assert.Equal(t, 400, code)
}

// Item 21: no request body (invite link, invite code) reaches any log, on
// success or on any error path, including the request logging middleware.
func TestPeernetSecretsNeverLogged(t *testing.T) {
	var logs bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(old) })

	var access bytes.Buffer
	run := func(f *fakePeernet, method, path, body string) {
		s := &Server{peernet: f}
		app := fiber.New()
		app.Use(fiberlogger.New(fiberlogger.Config{Output: &access}))
		s.registerPeernetRoutes(app.Group("/api"))
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := app.Test(req)
		require.NoError(t, err)
		resp.Body.Close()
	}
	link := `altmountnet://join?v=1&n=SECRETLINK&code=SECRETCODE`
	join := "/api/peernet/networks/join"
	invites := "/api/peernet/networks/" + hex64 + "/invites"

	for _, err := range []error{
		nil, membership.ErrDenied, membership.ErrMalformed, transport.ErrAddressRefused,
		errors.New("dial tcp 10.1.2.3:99: " + link), // unexpected: 500
	} {
		run(&fakePeernet{err: err}, "POST", join, `{"link":"`+link+`"}`)
		run(&fakePeernet{err: err}, "POST", invites, `{"ttl_hours":1,"alias":"SECRETALIAS"}`)
	}
	run(&fakePeernet{}, "POST", join, `{"link":"`+link+`"`)                            // malformed JSON
	run(&fakePeernet{}, "POST", join, `{"link":"`+link+strings.Repeat("a", 5000)+`"}`) // too large
	run(&fakePeernet{}, "POST", join, `{"link":"`+link+strings.Repeat("a", 600)+`"}`)  // too long
	run(&fakePeernet{err: errors.New(link)}, "POST", join, `{"link":"`+link+`","extra":"SECRETEXTRA"}`)

	out := logs.String() + access.String()
	for _, secret := range []string{"SECRETLINK", "SECRETCODE", "SECRETALIAS", "SECRETEXTRA", "altmountnet://"} {
		assert.NotContains(t, out, secret)
	}
	assert.Contains(t, logs.String(), "Peer network request failed", "the unexpected failure is still logged")
}

func TestPeernetLoopbackHasItsOwnMessage(t *testing.T) {
	f := &fakePeernet{err: transport.ErrLoopbackRefused}
	code, m, raw := doReq(t, peernetApp(f), "POST", "/api/peernet/networks/join", `{"link":"`+testLink+`","allow_private_addresses":true}`)
	assert.Equal(t, 400, code, raw)
	e := m["error"].(map[string]any)
	assert.Contains(t, strings.ToLower(e["details"].(string)), "loopback")
	assert.NotContains(t, e["details"], "only allowed for private networks")
	// Plain private refusal keeps its message.
	f = &fakePeernet{err: transport.ErrAddressRefused}
	_, m, _ = doReq(t, peernetApp(f), "POST", "/api/peernet/networks/join", `{"link":"`+testLink+`"}`)
	assert.Contains(t, m["error"].(map[string]any)["details"], "only allowed for private networks")
}

func TestPeernetStatusCarriesAdvertiseAndRemovalState(t *testing.T) {
	f := &fakePeernet{status: service.Status{Enabled: true, AdvertiseWarning: "careful",
		Networks: []service.NetworkStatus{{ID: hex64, Kind: "private", Role: "member", Revoked: true}}}}
	_, m, raw := doReq(t, peernetApp(f), "GET", "/api/peernet/status", "")
	d := m["data"].(map[string]any)
	assert.Equal(t, "careful", d["advertise_warning"], raw)
	assert.Equal(t, true, d["networks"].([]any)[0].(map[string]any)["revoked"], raw)
}

func TestPeernetStatusCarriesPublishCounters(t *testing.T) {
	f := &fakePeernet{status: service.Status{Enabled: true, Publish: service.PublishStatus{
		Published: map[string]int64{"release": 2, "attestation": 1}, Skipped: map[string]int64{"excluded_category": 4},
		Dropped: 3, Errors: 1, Reannounced: 5}}}
	code, m, raw := doReq(t, peernetApp(f), "GET", "/api/peernet/status", "")
	require.Equal(t, 200, code, raw)
	p := m["data"].(map[string]any)["publish"].(map[string]any)
	assert.EqualValues(t, 2, p["published"].(map[string]any)["release"])
	assert.EqualValues(t, 4, p["skipped"].(map[string]any)["excluded_category"])
	assert.EqualValues(t, 3, p["dropped"])
	assert.EqualValues(t, 1, p["errors"])
	assert.EqualValues(t, 5, p["reannounced"])

	// With nothing counted the shape is still stable: empty objects, not null.
	code, m, raw = doReq(t, peernetApp(&fakePeernet{status: service.Status{Enabled: true}}), "GET", "/api/peernet/status", "")
	require.Equal(t, 200, code, raw)
	p = m["data"].(map[string]any)["publish"].(map[string]any)
	assert.Empty(t, p["published"])
	assert.NotNil(t, p["published"])
}

func TestPeernetStatusCarriesRecordsAndAdvice(t *testing.T) {
	last := time.Unix(1_700_000_000, 0)
	rs := service.RecordStatus{
		Releases: struct{ Local, Peer int }{Local: 2, Peer: 30}, Attestations: struct{ Local, Peer int }{Local: 1, Peer: 9},
		Publishers: 4, LastPull: last, Stored: 40, Duplicates: 7, Rejected: map[string]int64{"bad_signature": 2},
		PeersBackedOff: 1, PeerBackoffs: 3,
	}
	f := &fakePeernet{status: service.Status{Enabled: true,
		Networks: []service.NetworkStatus{{ID: hex64, Kind: "open", Role: "open", Level: 1, Records: rs}},
		Advice: service.AdviceStatus{Given: map[string]int64{"reduce_sample_to_floor": 5}, ReduceApplied: 4, SkipsApplied: 2,
			RecheckScheduled: 1, LookupErrors: 3, BudgetRemaining: 6}}}
	code, m, raw := doReq(t, peernetApp(f), "GET", "/api/peernet/status", "")
	require.Equal(t, 200, code, raw)
	d := m["data"].(map[string]any)

	r := d["networks"].([]any)[0].(map[string]any)["records"].(map[string]any)
	assert.EqualValues(t, 2, r["releases"].(map[string]any)["local"])
	assert.EqualValues(t, 30, r["releases"].(map[string]any)["peer"])
	assert.EqualValues(t, 1, r["attestations"].(map[string]any)["local"])
	assert.EqualValues(t, 9, r["attestations"].(map[string]any)["peer"])
	assert.EqualValues(t, 4, r["publishers"])
	assert.Equal(t, last.UTC().Format(time.RFC3339), r["last_pull"])
	assert.EqualValues(t, 40, r["stored"])
	assert.EqualValues(t, 7, r["duplicates"])
	assert.EqualValues(t, 2, r["rejected"].(map[string]any)["bad_signature"])
	assert.EqualValues(t, 1, r["peers_backed_off"])
	assert.EqualValues(t, 3, r["peer_backoffs"])

	a := d["advice"].(map[string]any)
	assert.EqualValues(t, 5, a["given"].(map[string]any)["reduce_sample_to_floor"])
	assert.EqualValues(t, 4, a["reduce_applied"])
	assert.EqualValues(t, 2, a["skips_applied"])
	assert.EqualValues(t, 1, a["recheck_scheduled"])
	assert.EqualValues(t, 3, a["lookup_errors"])
	assert.EqualValues(t, 6, a["budget_remaining"])

	// Empty state keeps a stable shape: objects, a null last_pull, no nulls
	// where maps are expected.
	code, m, raw = doReq(t, peernetApp(&fakePeernet{status: service.Status{Enabled: true,
		Networks: []service.NetworkStatus{{ID: hex64, Kind: "open", Role: "open"}}}}), "GET", "/api/peernet/status", "")
	require.Equal(t, 200, code, raw)
	d = m["data"].(map[string]any)
	r = d["networks"].([]any)[0].(map[string]any)["records"].(map[string]any)
	assert.Nil(t, r["last_pull"])
	assert.NotNil(t, r["rejected"])
	assert.NotNil(t, d["advice"].(map[string]any)["given"])
}

func TestPeernetStatusReportsLibrarySharing(t *testing.T) {
	f := &fakePeernet{status: service.Status{Enabled: true, NodeID: hex64,
		Library: service.LibraryStatus{Files: 10, WithReleaseID: 4, FileIDOnly: 5, NotIndexable: 1}}}
	code, m, raw := doReq(t, peernetApp(f), "GET", "/api/peernet/status", "")
	require.Equal(t, 200, code, raw)
	lib, ok := m["data"].(map[string]any)["library"].(map[string]any)
	require.True(t, ok, raw)
	assert.EqualValues(t, 10, lib["files"])
	assert.EqualValues(t, 4, lib["with_release_id"])
	assert.EqualValues(t, 5, lib["file_id_only"])
	assert.EqualValues(t, 1, lib["not_indexable"])
}
