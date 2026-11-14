package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gofiber/fiber/v2"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/peernet/discovery"
	"github.com/kipsilabs/altmount/internal/peernet/identity"
	"github.com/kipsilabs/altmount/internal/peernet/membership"
	"github.com/kipsilabs/altmount/internal/peernet/netid"
	"github.com/kipsilabs/altmount/internal/peernet/service"
	"github.com/kipsilabs/altmount/internal/peernet/transport"
)

const (
	peernetMaxNameRunes  = 64
	peernetMaxAliasRunes = 64
	peernetMaxTTLHours   = 30 * 24
	peernetMaxBody       = 4096
)

// registerPeernetRoutes mounts the peer network management routes on the
// authenticated API group. The peer protocol itself never touches this app.
func (s *Server) registerPeernetRoutes(api fiber.Router) {
	g := api.Group("/peernet")
	g.Get("/status", s.handlePeernetStatus)
	g.Post("/networks", s.handlePeernetCreateNetwork)
	g.Post("/networks/join", s.handlePeernetJoinNetwork)
	g.Get("/networks/:id/peers", s.handlePeernetListPeers)
	g.Post("/networks/:id/peers/confirm", s.handlePeernetConfirmPeer)
	g.Patch("/networks/:id", s.handlePeernetUpdateNetwork)
	g.Delete("/networks/:id", s.handlePeernetDeleteNetwork)
	g.Get("/networks/:id/invites", s.handlePeernetListInvites)
	g.Post("/networks/:id/invites", s.handlePeernetCreateInvite)
	g.Delete("/networks/:id/invites/:inviteId", s.handlePeernetRevokeInvite)
	g.Get("/networks/:id/members", s.handlePeernetListMembers)
	g.Delete("/networks/:id/members/:nodeId", s.handlePeernetRemoveMember)
}

// ---- response shapes (snake_case, nullable ISO timestamps) ----

type peernetPeerCountsDTO struct {
	Total         int `json:"total"`
	Verified      int `json:"verified"`
	Active        int `json:"active"`
	NodeIDChanged int `json:"node_id_changed"`
	Unresolved    int `json:"unresolved"`
}

type peernetNetworkDTO struct {
	ID                    string               `json:"id"`
	Name                  string               `json:"name"`
	Kind                  string               `json:"kind"`
	Role                  string               `json:"role"`
	Level                 int                  `json:"level"`
	AllowPrivateAddresses bool                 `json:"allow_private_addresses"`
	HostAddress           string               `json:"host_address"`
	CertExpiresAt         *string              `json:"cert_expires_at"`
	Revoked               bool                 `json:"revoked"`
	RevokedAt             *string              `json:"revoked_at"`
	Peers                 peernetPeerCountsDTO `json:"peers"`
	// Records is the record exchange state: counts only, never record content.
	Records peernetRecordsDTO `json:"records"`
}

type peernetRecordCountsDTO struct {
	Local int `json:"local"`
	Peer  int `json:"peer"`
}

type peernetRecordsDTO struct {
	Releases       peernetRecordCountsDTO `json:"releases"`
	Attestations   peernetRecordCountsDTO `json:"attestations"`
	Publishers     int                    `json:"publishers"`
	LastPull       *string                `json:"last_pull"`
	Stored         int64                  `json:"stored"`
	Duplicates     int64                  `json:"duplicates"`
	Rejected       map[string]int64       `json:"rejected"`
	PeersBackedOff int                    `json:"peers_backed_off"`
	PeerBackoffs   int64                  `json:"peer_backoffs"`
}

// peernetAdviceDTO counts what the consuming side did since start.
type peernetAdviceDTO struct {
	Given            map[string]int64 `json:"given"`
	ReduceApplied    int64            `json:"reduce_applied"`
	SkipsApplied     int64            `json:"skips_applied"`
	RecheckScheduled int64            `json:"recheck_scheduled"`
	LookupErrors     int64            `json:"lookup_errors"`
	BudgetRemaining  int              `json:"budget_remaining"`
}

func toAdviceDTO(a service.AdviceStatus) peernetAdviceDTO {
	out := peernetAdviceDTO{Given: map[string]int64{}, ReduceApplied: a.ReduceApplied, SkipsApplied: a.SkipsApplied,
		RecheckScheduled: a.RecheckScheduled, LookupErrors: a.LookupErrors, BudgetRemaining: a.BudgetRemaining}
	for k, v := range a.Given {
		out.Given[k] = v
	}
	return out
}

func toRecordsDTO(r service.RecordStatus) peernetRecordsDTO {
	out := peernetRecordsDTO{
		Releases:       peernetRecordCountsDTO{Local: r.Releases.Local, Peer: r.Releases.Peer},
		Attestations:   peernetRecordCountsDTO{Local: r.Attestations.Local, Peer: r.Attestations.Peer},
		Publishers:     r.Publishers,
		LastPull:       isoOrNil(r.LastPull),
		Stored:         r.Stored,
		Duplicates:     r.Duplicates,
		Rejected:       map[string]int64{},
		PeersBackedOff: r.PeersBackedOff,
		PeerBackoffs:   r.PeerBackoffs,
	}
	for k, v := range r.Rejected {
		out.Rejected[k] = v
	}
	return out
}

type peernetBackfillDTO struct {
	Running bool `json:"running"`
	Scanned int  `json:"scanned"`
	Indexed int  `json:"indexed"`
	Skipped int  `json:"skipped"`
}

// peernetLibraryDTO says how many library files can be shared: those with a
// Release ID, either one from the release's NZB (with_nzb_release_id) or one
// derived from the files of the import (with_derived_release_id, for imports
// that predate the NZB store). The others (file_id_only) could not be grouped:
// no_source_nzb, group_too_large or pending (the background pass has not
// reached them yet), or could not be identified at all (not_indexable).
type peernetLibraryDTO struct {
	Files                int `json:"files"`
	WithReleaseID        int `json:"with_release_id"`
	WithNZBReleaseID     int `json:"with_nzb_release_id"`
	WithDerivedReleaseID int `json:"with_derived_release_id"`
	FileIDOnly           int `json:"file_id_only"`
	NotIndexable         int `json:"not_indexable"`
	NoSourceNZB          int `json:"no_source_nzb"`
	GroupTooLarge        int `json:"group_too_large"`
	Pending              int `json:"pending"`
}

type peernetStatusDTO struct {
	Enabled        bool   `json:"enabled"`
	NodeID         string `json:"node_id"`
	Listening      bool   `json:"listening"`
	ListenPort     int    `json:"listen_port"`
	ListenError    string `json:"listen_error"`
	Error          string `json:"error"`
	AcceptIncoming bool   `json:"accept_incoming"`
	// AdvertiseWarning explains why the advertise address is not used; empty
	// when it is fine. It never contains the address.
	AdvertiseWarning string              `json:"advertise_warning"`
	Networks         []peernetNetworkDTO `json:"networks"`
	Backfill         peernetBackfillDTO  `json:"backfill"`
	Library          peernetLibraryDTO   `json:"library"`
	Publish          peernetPublishDTO   `json:"publish"`
	Advice           peernetAdviceDTO    `json:"advice"`
	Bundles          peernetBundlesDTO   `json:"bundles"`
}

// peernetBundlesDTO counts bundle building, serving, fetching and importing
// since start. It holds no bundle content, path, name or address.
type peernetBundlesDTO struct {
	Built              int64            `json:"built"`
	CacheHits          int64            `json:"cache_hits"`
	BuildFailures      int64            `json:"build_failures"`
	Cached             int              `json:"cached"`
	CachedBytes        int              `json:"cached_bytes"`
	Announced          int              `json:"announced"`
	NotShareable       map[string]int64 `json:"not_shareable"`
	Served             int64            `json:"served"`
	ServedBytes        int64            `json:"served_bytes"`
	ServeRefused       map[string]int64 `json:"serve_refused"`
	Fetched            int64            `json:"fetched"`
	FetchRateLimited   int64            `json:"fetch_rate_limited"`
	FetchFailed        map[string]int64 `json:"fetch_failed"`
	FetchBackedOff     int              `json:"fetch_backed_off"`
	FullCheckReports   int64            `json:"full_check_reports"`
	FullCheckBackedOff int              `json:"full_check_backed_off"`
	Import             peernetImportDTO `json:"import"`
}

type peernetImportDTO struct {
	Accepted         int64            `json:"accepted"`
	AlreadyImported  int64            `json:"already_imported"`
	Rejected         map[string]int64 `json:"rejected"`
	FullCheckPassed  int64            `json:"full_check_passed"`
	FullCheckFailed  int64            `json:"full_check_failed"`
	Announced        int64            `json:"announced"`
	BadBundleReports int64            `json:"bad_bundle_reports"`
	Pending          int64            `json:"pending"`
	Stale            int64            `json:"stale"`
}

func copyCounts(m map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func toBundlesDTO(b service.BundleStatus) peernetBundlesDTO {
	return peernetBundlesDTO{
		Built: b.Built, CacheHits: b.CacheHits, BuildFailures: b.BuildFailures, Cached: b.Cached, CachedBytes: b.CachedBytes,
		Announced: b.Announced, NotShareable: copyCounts(b.NotShareable), Served: b.Served, ServedBytes: b.ServedBytes,
		ServeRefused: copyCounts(b.ServeRefused), Fetched: b.Fetched, FetchRateLimited: b.FetchRateLimited,
		FetchFailed: copyCounts(b.FetchFailed), FetchBackedOff: b.FetchBackedOff,
		FullCheckReports: b.FullCheckReports, FullCheckBackedOff: b.FullCheckBackedOff,
		Import: peernetImportDTO{Accepted: b.Import.Accepted, AlreadyImported: b.Import.AlreadyImported,
			Rejected: copyCounts(b.Import.Rejected), FullCheckPassed: b.Import.FullCheckPassed,
			FullCheckFailed: b.Import.FullCheckFailed, Announced: b.Import.Announced, BadBundleReports: b.Import.BadBundleReports,
			Pending: b.Import.Pending, Stale: b.Import.Stale},
	}
}

// peernetPublishDTO counts what the publishing pipeline did since start. It
// holds no record content, path or name.
type peernetPublishDTO struct {
	Published   map[string]int64 `json:"published"`
	Skipped     map[string]int64 `json:"skipped"`
	Dropped     int64            `json:"dropped"`
	Errors      int64            `json:"errors"`
	Reannounced int64            `json:"reannounced"`
}

func toPublishDTO(p service.PublishStatus) peernetPublishDTO {
	out := peernetPublishDTO{Published: map[string]int64{}, Skipped: map[string]int64{},
		Dropped: p.Dropped, Errors: p.Errors, Reannounced: p.Reannounced}
	for k, v := range p.Published {
		out.Published[k] = v
	}
	for k, v := range p.Skipped {
		out.Skipped[k] = v
	}
	return out
}

type peernetPeerDTO struct {
	NodeID        string  `json:"node_id"`
	Address       string  `json:"address"`
	Source        string  `json:"source"`
	Verified      bool    `json:"verified"`
	FirstSeen     *string `json:"first_seen"`
	LastSuccess   *string `json:"last_success"`
	Failures      int     `json:"failures"`
	NodeIDChanged bool    `json:"node_id_changed"`
	Unresolved    bool    `json:"unresolved"`
	LastError     string  `json:"last_error"`
}

type peernetInviteDTO struct {
	ID        string  `json:"id"`
	Alias     string  `json:"alias"`
	CreatedAt *string `json:"created_at"`
	ExpiresAt *string `json:"expires_at"`
	UsedAt    *string `json:"used_at"`
	UsedBy    string  `json:"used_by"`
}

type peernetMemberDTO struct {
	NodeID        string  `json:"node_id"`
	Alias         string  `json:"alias"`
	JoinedAt      *string `json:"joined_at"`
	CertExpiresAt *string `json:"cert_expires_at"`
	RevokedAt     *string `json:"revoked_at"`
}

func isoOrNil(t time.Time) *string {
	if t.IsZero() {
		return nil
	}
	s := t.UTC().Format(time.RFC3339)
	return &s
}

func toNetworkDTO(n service.NetworkStatus) peernetNetworkDTO {
	return peernetNetworkDTO{
		ID: n.ID, Name: n.Name, Kind: n.Kind, Role: n.Role, Level: n.Level,
		AllowPrivateAddresses: n.AllowPrivateAddresses, HostAddress: n.HostAddress,
		CertExpiresAt: isoOrNil(n.CertExpiresAt), Revoked: n.Revoked, RevokedAt: isoOrNil(n.RevokedAt),
		Peers: peernetPeerCountsDTO{Total: n.Peers.Total, Verified: n.Peers.Verified, Active: n.Peers.Active,
			NodeIDChanged: n.Peers.NodeIDChanged, Unresolved: n.Peers.Unresolved},
		Records: toRecordsDTO(n.Records),
	}
}

// ---- helpers ----

// peernetAvailable answers 503 when no service is wired in.
func (s *Server) peernetAvailable(c *fiber.Ctx) bool {
	if s.peernet == nil {
		_ = RespondServiceUnavailable(c, "Peer network is not available", "")
		return false
	}
	return true
}

func parsePeernetNetworkID(c *fiber.Ctx) (netid.NetworkID, bool) {
	raw := c.Params("id")
	if !netid.IsHexID(raw, true) {
		_ = RespondValidationError(c, "Invalid network ID", "The network ID must be 64 lowercase hex characters.")
		return netid.NetworkID{}, false
	}
	id, err := netid.ParseNetworkID(raw)
	if err != nil {
		_ = RespondValidationError(c, "Invalid network ID", "")
		return netid.NetworkID{}, false
	}
	return id, true
}

// parsePeernetBody decodes a small JSON body. Errors never echo the body
// (it may hold an invite link).
func parsePeernetBody(c *fiber.Ctx, dst any) bool {
	body := c.Body()
	if len(body) > peernetMaxBody {
		_ = RespondValidationError(c, "Request body is too large", "")
		return false
	}
	if err := json.Unmarshal(body, dst); err != nil {
		_ = RespondValidationError(c, "Invalid JSON in request body", "")
		return false
	}
	return true
}

// cleanPeernetText validates user text: trimmed, non-empty unless allowEmpty,
// within maxRunes, valid UTF-8 and free of control and format characters. The
// character rule is the repository's (database.CleanPeerText); text it would
// alter is rejected here instead of silently changed.
func cleanPeernetText(v string, maxRunes int, allowEmpty bool) (string, bool) {
	v = strings.TrimSpace(v)
	if !utf8.ValidString(v) || utf8.RuneCountInString(v) > maxRunes || (v == "" && !allowEmpty) {
		return "", false
	}
	if database.CleanPeerText(v, math.MaxInt) != v {
		return "", false
	}
	return v, true
}

// validPeernetAddress validates with the transport's own validator, the single
// definition of a usable peer address, and returns the canonical form.
func validPeernetAddress(a string) (string, bool) {
	pa, err := transport.ValidateAddress(a)
	if err != nil {
		return "", false
	}
	return pa.HostPort(), true
}

// respondPeernetError maps service errors to human-readable responses. Raw
// error text is never returned: it could carry an address or link fragment.
func respondPeernetError(c *fiber.Ctx, err error) error {
	return respondPeernetErrorFor(c, err, false)
}

// respondPeernetErrorFor is respondPeernetError; with secret set (the join and
// invite endpoints) an unexpected error is logged by type only, because its
// text could carry an invite link or code.
func respondPeernetErrorFor(c *fiber.Ctx, err error, secret bool) error {
	switch {
	case errors.Is(err, service.ErrDisabled):
		return RespondConflict(c, "Peer network is disabled", "Enable it in the peer network settings first.")
	case errors.Is(err, service.ErrAcceptIncomingRequired):
		return RespondValidationError(c, "Hosting a private network requires accepting incoming connections",
			"Turn on \"accept incoming connections\" first.")
	case errors.Is(err, service.ErrOpenNetwork):
		return RespondValidationError(c, "The open network cannot be changed here",
			"Change the open network's level in the peer network configuration; it cannot be deleted.")
	case errors.Is(err, membership.ErrUnknownNetwork), errors.Is(err, service.ErrNotFound),
		errors.Is(err, database.ErrPeernetNotFound):
		return RespondNotFound(c, "Network, invite or member", "")
	case errors.Is(err, membership.ErrNotHost):
		return RespondValidationError(c, "Only the host of a private network can do this", "")
	case errors.Is(err, membership.ErrNotMember):
		return RespondValidationError(c, "This network is not one you joined", "")
	case errors.Is(err, membership.ErrNoHostAddress):
		return RespondValidationError(c, "Set the network's host address first",
			"Set a host address on the network or configure an advertise address.")
	case errors.Is(err, membership.ErrInviteTTL):
		return RespondValidationError(c, "Invalid invite lifetime", "")
	case errors.Is(err, membership.ErrOpenNetwork):
		return RespondValidationError(c, "The open network cannot be changed here",
			"Change the open network's level in the peer network configuration; it cannot be deleted.")
	case errors.Is(err, membership.ErrInvalidLevel):
		return RespondValidationError(c, "Invalid exposure level", "")
	case errors.Is(err, membership.ErrAlreadyJoined):
		return RespondConflict(c, "You already know this network", "")
	case errors.Is(err, membership.ErrRevocationFull):
		return RespondConflict(c, "The revocation list is full", "")
	case errors.Is(err, discovery.ErrUnknownNetwork):
		return RespondValidationError(c, "This network is not active", "Set its exposure level above Off first.")
	case errors.Is(err, discovery.ErrNothingToConfirm):
		return RespondConflict(c, "There is no node ID change to confirm for this address", "")
	case errors.Is(err, membership.ErrMalformed), errors.Is(err, membership.ErrWrongNetwork):
		return RespondValidationError(c, "This invite link is not valid", "")
	case errors.Is(err, membership.ErrDenied), errors.Is(err, membership.ErrBadSignature),
		errors.Is(err, membership.ErrRevoked), errors.Is(err, membership.ErrExpired):
		return RespondValidationError(c, "The host did not accept this request",
			"The invite may be used, expired or revoked, or the host could not be verified.")
	case errors.Is(err, transport.ErrLoopbackRefused):
		return RespondValidationError(c, "The address is not allowed",
			"Loopback addresses (127.0.0.1, localhost) are never allowed, even for private networks. "+
				"Use the machine's LAN or public address.")
	case errors.Is(err, transport.ErrAddressRefused):
		return RespondValidationError(c, "The address is not allowed",
			"Private and local addresses are only allowed for private networks that enable them.")
	case errors.Is(err, transport.ErrNodeIDMismatch):
		return RespondValidationError(c, "The peer at that address is not the expected node", "")
	}
	if secret {
		slog.ErrorContext(c.Context(), "Peer network request failed", "path", c.Route().Path,
			"error_type", fmt.Sprintf("%T", err))
	} else {
		slog.ErrorContext(c.Context(), "Peer network request failed", "error", err)
	}
	return RespondInternalError(c, "Peer network request failed", "")
}

// ---- handlers ----

func (s *Server) handlePeernetStatus(c *fiber.Ctx) error {
	if !s.peernetAvailable(c) {
		return nil
	}
	st, err := s.peernet.Status(c.Context())
	if err != nil {
		return respondPeernetError(c, err)
	}
	out := peernetStatusDTO{
		Enabled: st.Enabled, NodeID: st.NodeID, Listening: st.Listening, ListenPort: st.ListenPort,
		ListenError: st.ListenError, Error: st.Error, AcceptIncoming: st.AcceptIncoming,
		AdvertiseWarning: st.AdvertiseWarning,
		Networks:         make([]peernetNetworkDTO, 0, len(st.Networks)),
		Backfill: peernetBackfillDTO{Running: st.Backfill.Running, Scanned: st.Backfill.Scanned,
			Indexed: st.Backfill.Indexed, Skipped: st.Backfill.Skipped},
		Library: peernetLibraryDTO{Files: st.Library.Files, WithReleaseID: st.Library.WithReleaseID,
			WithNZBReleaseID: st.Library.WithNZBReleaseID, WithDerivedReleaseID: st.Library.WithDerivedReleaseID,
			FileIDOnly: st.Library.FileIDOnly, NotIndexable: st.Library.NotIndexable, NoSourceNZB: st.Library.NoSourceNZB,
			GroupTooLarge: st.Library.GroupTooLarge, Pending: st.Library.Pending},
		Publish: toPublishDTO(st.Publish),
		Advice:  toAdviceDTO(st.Advice),
		Bundles: toBundlesDTO(st.Bundles),
	}
	for _, n := range st.Networks {
		out.Networks = append(out.Networks, toNetworkDTO(n))
	}
	return RespondSuccess(c, out)
}

func (s *Server) handlePeernetListPeers(c *fiber.Ctx) error {
	if !s.peernetAvailable(c) {
		return nil
	}
	id, ok := parsePeernetNetworkID(c)
	if !ok {
		return nil
	}
	peers, err := s.peernet.Peers(c.Context(), id)
	if err != nil {
		return respondPeernetError(c, err)
	}
	out := make([]peernetPeerDTO, 0, len(peers))
	for _, p := range peers {
		out = append(out, peernetPeerDTO{
			NodeID: p.NodeID, Address: p.Address, Source: p.Source, Verified: p.Verified,
			FirstSeen: isoOrNil(p.FirstSeen), LastSuccess: isoOrNil(p.LastSuccess), Failures: p.Failures,
			NodeIDChanged: p.NodeIDChanged, Unresolved: p.Unresolved, LastError: p.LastError,
		})
	}
	return RespondSuccess(c, out)
}

func (s *Server) handlePeernetConfirmPeer(c *fiber.Ctx) error {
	if !s.peernetAvailable(c) {
		return nil
	}
	id, ok := parsePeernetNetworkID(c)
	if !ok {
		return nil
	}
	var body struct {
		Address string `json:"address"`
	}
	if !parsePeernetBody(c, &body) {
		return nil
	}
	addr, ok := validPeernetAddress(strings.TrimSpace(body.Address))
	if !ok {
		return RespondValidationError(c, "Invalid address", "Use host:port.")
	}
	if err := s.peernet.ConfirmPeer(c.Context(), id, addr); err != nil {
		return respondPeernetError(c, err)
	}
	return RespondNoContent(c)
}

func (s *Server) handlePeernetCreateNetwork(c *fiber.Ctx) error {
	if !s.peernetAvailable(c) {
		return nil
	}
	var body struct {
		Name string `json:"name"`
	}
	if !parsePeernetBody(c, &body) {
		return nil
	}
	name, ok := cleanPeernetText(body.Name, peernetMaxNameRunes, false)
	if !ok {
		return RespondValidationError(c, "Invalid network name", "Use 1 to 64 characters without control characters.")
	}
	n, err := s.peernet.CreateNetwork(c.Context(), name)
	if err != nil {
		return respondPeernetError(c, err)
	}
	return RespondCreated(c, toNetworkDTO(n))
}

func (s *Server) handlePeernetJoinNetwork(c *fiber.Ctx) error {
	if !s.peernetAvailable(c) {
		return nil
	}
	var body struct {
		Link                  string `json:"link"`
		AllowPrivateAddresses bool   `json:"allow_private_addresses"`
	}
	if !parsePeernetBody(c, &body) {
		return nil
	}
	link := strings.TrimSpace(body.Link)
	if link == "" || len(link) > membership.MaxLinkLen {
		return RespondValidationError(c, "This invite link is not valid", "")
	}
	n, err := s.peernet.JoinNetwork(c.Context(), link, body.AllowPrivateAddresses)
	if err != nil {
		return respondPeernetErrorFor(c, err, true)
	}
	return RespondCreated(c, toNetworkDTO(n))
}

func (s *Server) handlePeernetUpdateNetwork(c *fiber.Ctx) error {
	if !s.peernetAvailable(c) {
		return nil
	}
	id, ok := parsePeernetNetworkID(c)
	if !ok {
		return nil
	}
	var body struct {
		Level                 *int    `json:"level"`
		AllowPrivateAddresses *bool   `json:"allow_private_addresses"`
		HostAddress           *string `json:"host_address"`
	}
	if !parsePeernetBody(c, &body) {
		return nil
	}
	upd := service.NetworkUpdate{Level: body.Level, AllowPrivateAddresses: body.AllowPrivateAddresses}
	if body.Level != nil && (*body.Level < 0 || *body.Level > 3) {
		return RespondValidationError(c, "Invalid exposure level", "The level must be between 0 and 3.")
	}
	if body.HostAddress != nil {
		addr, ok := validPeernetAddress(strings.TrimSpace(*body.HostAddress))
		if !ok {
			return RespondValidationError(c, "Invalid host address", "Use host:port.")
		}
		upd.HostAddress = &addr
	}
	if upd.Level == nil && upd.AllowPrivateAddresses == nil && upd.HostAddress == nil {
		return RespondValidationError(c, "Nothing to change", "")
	}
	n, err := s.peernet.UpdateNetwork(c.Context(), id, upd)
	if err != nil {
		return respondPeernetError(c, err)
	}
	return RespondSuccess(c, toNetworkDTO(n))
}

func (s *Server) handlePeernetDeleteNetwork(c *fiber.Ctx) error {
	if !s.peernetAvailable(c) {
		return nil
	}
	id, ok := parsePeernetNetworkID(c)
	if !ok {
		return nil
	}
	if err := s.peernet.DeleteNetwork(c.Context(), id); err != nil {
		return respondPeernetError(c, err)
	}
	return RespondNoContent(c)
}

func (s *Server) handlePeernetListInvites(c *fiber.Ctx) error {
	if !s.peernetAvailable(c) {
		return nil
	}
	id, ok := parsePeernetNetworkID(c)
	if !ok {
		return nil
	}
	invites, err := s.peernet.ListInvites(c.Context(), id)
	if err != nil {
		return respondPeernetError(c, err)
	}
	out := make([]peernetInviteDTO, 0, len(invites))
	for _, i := range invites {
		out = append(out, peernetInviteDTO{ID: i.ID, Alias: i.Alias, CreatedAt: isoOrNil(i.CreatedAt),
			ExpiresAt: isoOrNil(i.ExpiresAt), UsedAt: isoOrNil(i.UsedAt), UsedBy: i.UsedBy})
	}
	return RespondSuccess(c, out)
}

func (s *Server) handlePeernetCreateInvite(c *fiber.Ctx) error {
	if !s.peernetAvailable(c) {
		return nil
	}
	id, ok := parsePeernetNetworkID(c)
	if !ok {
		return nil
	}
	var body struct {
		TTLHours int    `json:"ttl_hours"`
		Alias    string `json:"alias"`
	}
	if !parsePeernetBody(c, &body) {
		return nil
	}
	if body.TTLHours < 1 || body.TTLHours > peernetMaxTTLHours {
		return RespondValidationError(c, "Invalid invite lifetime", "Use 1 to 720 hours.")
	}
	alias, ok := cleanPeernetText(body.Alias, peernetMaxAliasRunes, true)
	if !ok {
		return RespondValidationError(c, "Invalid alias", "Use at most 64 characters without control characters.")
	}
	link, err := s.peernet.CreateInvite(c.Context(), id, time.Duration(body.TTLHours)*time.Hour, alias)
	if err != nil {
		return respondPeernetErrorFor(c, err, true)
	}
	// The link is returned exactly once and never logged.
	c.Set("Cache-Control", "no-store")
	return RespondCreated(c, fiber.Map{"link": link})
}

func (s *Server) handlePeernetRevokeInvite(c *fiber.Ctx) error {
	if !s.peernetAvailable(c) {
		return nil
	}
	id, ok := parsePeernetNetworkID(c)
	if !ok {
		return nil
	}
	inviteID := c.Params("inviteId")
	if !netid.IsHexID(inviteID, true) {
		return RespondValidationError(c, "Invalid invite ID", "")
	}
	if err := s.peernet.RevokeInvite(c.Context(), id, inviteID); err != nil {
		return respondPeernetError(c, err)
	}
	return RespondNoContent(c)
}

func (s *Server) handlePeernetListMembers(c *fiber.Ctx) error {
	if !s.peernetAvailable(c) {
		return nil
	}
	id, ok := parsePeernetNetworkID(c)
	if !ok {
		return nil
	}
	members, err := s.peernet.ListMembers(c.Context(), id)
	if err != nil {
		return respondPeernetError(c, err)
	}
	out := make([]peernetMemberDTO, 0, len(members))
	for _, m := range members {
		out = append(out, peernetMemberDTO{NodeID: m.NodeID, Alias: m.Alias, JoinedAt: isoOrNil(m.JoinedAt),
			CertExpiresAt: isoOrNil(m.CertExpiresAt), RevokedAt: isoOrNil(m.RevokedAt)})
	}
	return RespondSuccess(c, out)
}

func (s *Server) handlePeernetRemoveMember(c *fiber.Ctx) error {
	if !s.peernetAvailable(c) {
		return nil
	}
	id, ok := parsePeernetNetworkID(c)
	if !ok {
		return nil
	}
	raw := c.Params("nodeId")
	if !netid.IsHexID(raw, true) {
		return RespondValidationError(c, "Invalid node ID", "The node ID must be 64 lowercase hex characters.")
	}
	node, err := identity.ParseNodeID(raw)
	if err != nil {
		return RespondValidationError(c, "Invalid node ID", "")
	}
	if err := s.peernet.RemoveMember(c.Context(), id, node); err != nil {
		return respondPeernetError(c, err)
	}
	return RespondNoContent(c)
}
