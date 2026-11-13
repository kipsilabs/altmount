package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"unicode"
)

// Network roles stored in peernet_networks.role.
const (
	PeernetRoleHost   = "host"
	PeernetRoleMember = "member"
	PeernetRoleOpen   = "open"
)

// Hard caps for peer-originated text stored by this repository.
const (
	PeernetMaxAliasRunes       = 64
	PeernetMaxDisplayNameRunes = 64
)

// Errors returned by PeernetNetworkRepository. Callers use errors.Is.
var (
	ErrPeernetNotFound     = errors.New("peernet: not found")
	ErrPeernetExists       = errors.New("peernet: already exists")
	ErrPeernetInviteDenied = errors.New("peernet: invite not redeemable")
	ErrPeernetStale        = errors.New("peernet: revocation list sequence went backwards")
)

// PeernetNetwork is one network this instance hosts or has joined, or the
// open network. Times are Unix seconds.
type PeernetNetwork struct {
	NetworkID   string
	DisplayName string
	Role        string
	// ExposureLevel is the level of a host or member network. It is unused for
	// the open network: that level lives in the configuration, and the open
	// row's column is never meaningful (the membership layer refuses to set it).
	ExposureLevel         int
	AllowPrivateAddresses bool
	HostAddress           string
	HostNodeID            string
	RootPublicKey         []byte
	OwnCertificate        []byte
	RevocationList        []byte
	RevocationSeq         uint64
	// RevokedAt is when a member learned the host removed it (Unix seconds,
	// 0 when it did not). Only ever set on joined networks.
	RevokedAt int64
	CreatedAt int64
	UpdatedAt int64
}

// PeernetInvite is the at-rest form of an invite: only a hash of the code.
type PeernetInvite struct {
	NetworkID string
	CodeHash  string
	Alias     string
	CreatedAt int64
	ExpiresAt int64
	UsedAt    int64 // 0 when unused
	UsedBy    string
}

// PeernetMember is a member as seen by a host. Times are Unix seconds.
type PeernetMember struct {
	NetworkID     string
	NodeID        string
	Alias         string
	JoinedAt      int64
	CertIssuedAt  int64
	CertExpiresAt int64
	RevokedAt     int64 // 0 when not revoked
}

// PeernetNetworkRepository stores private/open network state (tables
// peernet_networks, peernet_invites, peernet_members).
type PeernetNetworkRepository struct {
	db *dialectAwareDB
}

// NewPeernetNetworkRepository creates a repository over conn.
func NewPeernetNetworkRepository(conn *sql.DB, d Dialect) *PeernetNetworkRepository {
	return &PeernetNetworkRepository{db: newDialectAwareDB(conn, d)}
}

// CleanPeerText strips control and format characters and invalid UTF-8 from a
// peer-originated string, trims it and caps it at maxRunes runes.
func CleanPeerText(s string, maxRunes int) string {
	var b strings.Builder
	n := 0
	for _, r := range s {
		if r == unicode.ReplacementChar || unicode.IsControl(r) || unicode.Is(unicode.Cf, r) ||
			r == ' ' || r == ' ' {
			continue
		}
		if n >= maxRunes {
			break
		}
		b.WriteRune(r)
		n++
	}
	return strings.TrimSpace(b.String())
}

// rowsAffected returns the affected row count, surfacing the driver error
// instead of treating it as zero rows.
func rowsAffected(res sql.Result, what string) (int64, error) {
	c, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("%s: rows affected: %w", what, err)
	}
	return c, nil
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// seqToDB converts a revocation sequence to the signed column type, refusing
// values that would wrap.
func seqToDB(seq uint64) (int64, error) {
	if seq > math.MaxInt64 {
		return 0, errors.New("peernet network: revocation sequence out of range")
	}
	return int64(seq), nil
}

const networkCols = `network_id, display_name, role, exposure_level, allow_private_addresses,
	host_address, host_node_id, root_public_key, own_certificate, revocation_list,
	revocation_seq, revoked_at, created_at, updated_at`

func scanNetwork(s rowScanner) (*PeernetNetwork, error) {
	var n PeernetNetwork
	var allow int
	var seq int64
	if err := s.Scan(&n.NetworkID, &n.DisplayName, &n.Role, &n.ExposureLevel, &allow,
		&n.HostAddress, &n.HostNodeID, &n.RootPublicKey, &n.OwnCertificate, &n.RevocationList,
		&seq, &n.RevokedAt, &n.CreatedAt, &n.UpdatedAt); err != nil {
		return nil, err
	}
	if seq < 0 {
		return nil, errors.New("peernet network: negative revocation sequence")
	}
	n.AllowPrivateAddresses = allow != 0
	n.RevocationSeq = uint64(seq)
	return &n, nil
}

func validRole(r string) bool {
	return r == PeernetRoleHost || r == PeernetRoleMember || r == PeernetRoleOpen
}

// CreateNetwork inserts n. It returns ErrPeernetExists if the ID is taken.
// The display name is cleaned before storage.
func (r *PeernetNetworkRepository) CreateNetwork(ctx context.Context, n PeernetNetwork) error {
	if !isHexID(n.NetworkID) || !validRole(n.Role) {
		return errors.New("peernet network: invalid id or role")
	}
	if n.ExposureLevel < 0 || n.ExposureLevel > 3 {
		return errors.New("peernet network: invalid exposure level")
	}
	seq, err := seqToDB(n.RevocationSeq)
	if err != nil {
		return err
	}
	allow := 0
	if n.AllowPrivateAddresses {
		allow = 1
	}
	res, err := r.db.ExecContext(ctx, `INSERT INTO peernet_networks (`+networkCols+`)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (network_id) DO NOTHING`,
		n.NetworkID, CleanPeerText(n.DisplayName, PeernetMaxDisplayNameRunes), n.Role, n.ExposureLevel, allow,
		n.HostAddress, n.HostNodeID, n.RootPublicKey, n.OwnCertificate, n.RevocationList,
		seq, n.RevokedAt, n.CreatedAt, n.UpdatedAt)
	if err != nil {
		return fmt.Errorf("create peernet network: %w", err)
	}
	c, err := rowsAffected(res, "create peernet network")
	if err != nil {
		return err
	}
	if c == 0 {
		return ErrPeernetExists
	}
	return nil
}

// GetNetwork returns the network or ErrPeernetNotFound.
func (r *PeernetNetworkRepository) GetNetwork(ctx context.Context, id string) (*PeernetNetwork, error) {
	n, err := scanNetwork(r.db.QueryRowContext(ctx,
		`SELECT `+networkCols+` FROM peernet_networks WHERE network_id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrPeernetNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get peernet network: %w", err)
	}
	return n, nil
}

// ListNetworks returns all networks ordered by creation time then ID.
func (r *PeernetNetworkRepository) ListNetworks(ctx context.Context) ([]PeernetNetwork, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT `+networkCols+` FROM peernet_networks ORDER BY created_at, network_id`)
	if err != nil {
		return nil, fmt.Errorf("list peernet networks: %w", err)
	}
	defer rows.Close()
	var out []PeernetNetwork
	for rows.Next() {
		n, err := scanNetwork(rows)
		if err != nil {
			return nil, fmt.Errorf("scan peernet network: %w", err)
		}
		out = append(out, *n)
	}
	return out, rows.Err()
}

// DeleteNetwork removes the network with its invites and members.
func (r *PeernetNetworkRepository) DeleteNetwork(ctx context.Context, id string) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("delete peernet network: begin: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit
	for _, q := range []string{
		`DELETE FROM peernet_invites WHERE network_id = ?`,
		`DELETE FROM peernet_members WHERE network_id = ?`,
	} {
		if _, err := tx.ExecContext(ctx, q, id); err != nil {
			return fmt.Errorf("delete peernet network children: %w", err)
		}
	}
	res, err := tx.ExecContext(ctx, `DELETE FROM peernet_networks WHERE network_id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete peernet network: %w", err)
	}
	c, err := rowsAffected(res, "delete peernet network")
	if err != nil {
		return err
	}
	if c == 0 {
		return ErrPeernetNotFound
	}
	return tx.Commit()
}

func (r *PeernetNetworkRepository) updateOne(ctx context.Context, id, set string, now int64, args ...any) error {
	// set is always a compile-time literal from this file.
	full := append(append([]any{}, args...), now, id)
	res, err := r.db.ExecContext(ctx,
		`UPDATE peernet_networks SET `+set+`, updated_at = ? WHERE network_id = ?`, full...)
	if err != nil {
		return fmt.Errorf("update peernet network: %w", err)
	}
	c, err := rowsAffected(res, "update peernet network")
	if err != nil {
		return err
	}
	if c == 0 {
		return ErrPeernetNotFound
	}
	return nil
}

// NetworkSettingsUpdate is a partial change to a network's settings; nil
// fields are kept.
type NetworkSettingsUpdate struct {
	Level                 *int
	AllowPrivateAddresses *bool
	HostAddress           *string
}

// UpdateSettings applies every non-nil field in one UPDATE statement, so the
// change is all-or-nothing. It validates first and returns ErrPeernetNotFound
// for an unknown network.
func (r *PeernetNetworkRepository) UpdateSettings(ctx context.Context, id string, u NetworkSettingsUpdate, now int64) error {
	var sets []string
	var args []any
	if u.Level != nil {
		if *u.Level < 0 || *u.Level > 3 {
			return errors.New("peernet network: invalid exposure level")
		}
		sets, args = append(sets, `exposure_level = ?`), append(args, *u.Level)
	}
	if u.AllowPrivateAddresses != nil {
		sets, args = append(sets, `allow_private_addresses = ?`), append(args, boolToInt(*u.AllowPrivateAddresses))
	}
	if u.HostAddress != nil {
		sets, args = append(sets, `host_address = ?`), append(args, *u.HostAddress)
	}
	if len(sets) == 0 {
		return errors.New("peernet network: nothing to update")
	}
	// sets holds only literals from this function.
	return r.updateOne(ctx, id, strings.Join(sets, ", "), now, args...)
}

// SetExposureLevel sets the level (0-3).
func (r *PeernetNetworkRepository) SetExposureLevel(ctx context.Context, id string, level int, now int64) error {
	if level < 0 || level > 3 {
		return errors.New("peernet network: invalid exposure level")
	}
	return r.updateOne(ctx, id, `exposure_level = ?`, now, level)
}

// SetAllowPrivateAddresses sets the per-network private address switch.
func (r *PeernetNetworkRepository) SetAllowPrivateAddresses(ctx context.Context, id string, allow bool, now int64) error {
	v := 0
	if allow {
		v = 1
	}
	return r.updateOne(ctx, id, `allow_private_addresses = ?`, now, v)
}

// SetHostAddress stores the host's advertised address.
func (r *PeernetNetworkRepository) SetHostAddress(ctx context.Context, id, addr string, now int64) error {
	return r.updateOne(ctx, id, `host_address = ?`, now, addr)
}

// SetOwnCertificate stores this node's certificate blob for the network.
func (r *PeernetNetworkRepository) SetOwnCertificate(ctx context.Context, id string, cert []byte, now int64) error {
	return r.updateOne(ctx, id, `own_certificate = ?`, now, cert)
}

// SetRevoked records that this node was removed from the network by its host.
// The first revocation time is kept.
func (r *PeernetNetworkRepository) SetRevoked(ctx context.Context, id string, at, now int64) error {
	res, err := r.db.ExecContext(ctx, `UPDATE peernet_networks
		SET revoked_at = CASE WHEN revoked_at = 0 THEN ? ELSE revoked_at END, updated_at = ?
		WHERE network_id = ?`, at, now, id)
	if err != nil {
		return fmt.Errorf("set peernet network revoked: %w", err)
	}
	c, err := rowsAffected(res, "set peernet network revoked")
	if err != nil {
		return err
	}
	if c == 0 {
		return ErrPeernetNotFound
	}
	return nil
}

// SetDisplayName stores a cleaned display name.
func (r *PeernetNetworkRepository) SetDisplayName(ctx context.Context, id, name string, now int64) error {
	return r.updateOne(ctx, id, `display_name = ?`, now, CleanPeerText(name, PeernetMaxDisplayNameRunes))
}

// SetRevocationList stores a revocation list blob atomically, refusing a
// sequence lower than the one held (ErrPeernetStale).
func (r *PeernetNetworkRepository) SetRevocationList(ctx context.Context, id string, seq uint64, blob []byte, now int64) error {
	dbSeq, err := seqToDB(seq)
	if err != nil {
		return err
	}
	res, err := r.db.ExecContext(ctx, `UPDATE peernet_networks
		SET revocation_list = ?, revocation_seq = ?, updated_at = ?
		WHERE network_id = ? AND revocation_seq <= ?`, blob, dbSeq, now, id, dbSeq)
	if err != nil {
		return fmt.Errorf("set peernet revocation list: %w", err)
	}
	c, err := rowsAffected(res, "set peernet revocation list")
	if err != nil {
		return err
	}
	if c == 0 {
		if _, gerr := r.GetNetwork(ctx, id); gerr != nil {
			return gerr
		}
		return ErrPeernetStale
	}
	return nil
}

// CreateInvite stores an invite by code hash.
func (r *PeernetNetworkRepository) CreateInvite(ctx context.Context, inv PeernetInvite) error {
	if !isHexID(inv.NetworkID) || !isHexID(inv.CodeHash) {
		return errors.New("peernet invite: invalid id or hash")
	}
	if _, err := r.db.ExecContext(ctx, `INSERT INTO peernet_invites
		(network_id, code_hash, alias, created_at, expires_at) VALUES (?, ?, ?, ?, ?)`,
		inv.NetworkID, inv.CodeHash, CleanPeerText(inv.Alias, PeernetMaxAliasRunes), inv.CreatedAt, inv.ExpiresAt); err != nil {
		return fmt.Errorf("create peernet invite: %w", err)
	}
	return nil
}

// ListInvites returns all invites of a network, newest first.
func (r *PeernetNetworkRepository) ListInvites(ctx context.Context, networkID string) ([]PeernetInvite, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT network_id, code_hash, alias, created_at, expires_at, used_at, used_by
		FROM peernet_invites WHERE network_id = ? ORDER BY created_at DESC, code_hash`, networkID)
	if err != nil {
		return nil, fmt.Errorf("list peernet invites: %w", err)
	}
	defer rows.Close()
	var out []PeernetInvite
	for rows.Next() {
		var i PeernetInvite
		var used sql.NullInt64
		if err := rows.Scan(&i.NetworkID, &i.CodeHash, &i.Alias, &i.CreatedAt, &i.ExpiresAt, &used, &i.UsedBy); err != nil {
			return nil, fmt.Errorf("scan peernet invite: %w", err)
		}
		i.UsedAt = used.Int64
		out = append(out, i)
	}
	return out, rows.Err()
}

// DeleteInvite removes an invite by hash (revocation). ErrPeernetNotFound if absent.
func (r *PeernetNetworkRepository) DeleteInvite(ctx context.Context, networkID, codeHash string) error {
	res, err := r.db.ExecContext(ctx,
		`DELETE FROM peernet_invites WHERE network_id = ? AND code_hash = ?`, networkID, codeHash)
	if err != nil {
		return fmt.Errorf("delete peernet invite: %w", err)
	}
	c, err := rowsAffected(res, "delete peernet invite")
	if err != nil {
		return err
	}
	if c == 0 {
		return ErrPeernetNotFound
	}
	return nil
}

// RedeemInvite atomically consumes the invite and registers nodeID as a
// member, or re-issues the certificate window of an existing non-revoked
// member, in one transaction. Any reason it cannot succeed (unknown, expired,
// used, node revoked) returns the same ErrPeernetInviteDenied.
func (r *PeernetNetworkRepository) RedeemInvite(ctx context.Context, networkID, codeHash, nodeID string,
	now, certIssuedAt, certExpiresAt int64) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("redeem peernet invite: begin: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit
	res, err := tx.ExecContext(ctx, `UPDATE peernet_invites SET used_at = ?, used_by = ?
		WHERE network_id = ? AND code_hash = ? AND used_at IS NULL AND expires_at > ?`,
		now, nodeID, networkID, codeHash, now)
	if err != nil {
		return fmt.Errorf("redeem peernet invite: %w", err)
	}
	if c, err := rowsAffected(res, "redeem peernet invite"); err != nil {
		return err
	} else if c != 1 {
		return ErrPeernetInviteDenied
	}
	var alias string
	if err := tx.QueryRowContext(ctx, `SELECT alias FROM peernet_invites WHERE network_id = ? AND code_hash = ?`,
		networkID, codeHash).Scan(&alias); err != nil {
		return fmt.Errorf("redeem peernet invite: read alias: %w", err)
	}
	// A node that is already a member (it lost or let lapse its certificate)
	// gets a fresh certificate window and keeps its join time and alias; a
	// revoked node is never re-admitted.
	res, err = tx.ExecContext(ctx, `INSERT INTO peernet_members
		(network_id, node_id, alias, joined_at, cert_issued_at, cert_expires_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT (network_id, node_id) DO UPDATE SET
			cert_issued_at = excluded.cert_issued_at, cert_expires_at = excluded.cert_expires_at
		WHERE peernet_members.revoked_at IS NULL`,
		networkID, nodeID, alias, now, certIssuedAt, certExpiresAt)
	if err != nil {
		return fmt.Errorf("redeem peernet invite: add member: %w", err)
	}
	if c, err := rowsAffected(res, "redeem peernet invite"); err != nil {
		return err
	} else if c != 1 {
		return ErrPeernetInviteDenied // rolls back: the invite stays unused
	}
	return tx.Commit()
}

const memberCols = `network_id, node_id, alias, joined_at, cert_issued_at, cert_expires_at, revoked_at`

func scanMember(s rowScanner) (*PeernetMember, error) {
	var m PeernetMember
	var rev sql.NullInt64
	if err := s.Scan(&m.NetworkID, &m.NodeID, &m.Alias, &m.JoinedAt, &m.CertIssuedAt, &m.CertExpiresAt, &rev); err != nil {
		return nil, err
	}
	m.RevokedAt = rev.Int64
	return &m, nil
}

// GetMember returns a member row or ErrPeernetNotFound.
func (r *PeernetNetworkRepository) GetMember(ctx context.Context, networkID, nodeID string) (*PeernetMember, error) {
	m, err := scanMember(r.db.QueryRowContext(ctx,
		`SELECT `+memberCols+` FROM peernet_members WHERE network_id = ? AND node_id = ?`, networkID, nodeID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrPeernetNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get peernet member: %w", err)
	}
	return m, nil
}

// ListMembers returns all member rows of a network (revoked included).
func (r *PeernetNetworkRepository) ListMembers(ctx context.Context, networkID string) ([]PeernetMember, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT `+memberCols+
		` FROM peernet_members WHERE network_id = ? ORDER BY joined_at, node_id`, networkID)
	if err != nil {
		return nil, fmt.Errorf("list peernet members: %w", err)
	}
	defer rows.Close()
	var out []PeernetMember
	for rows.Next() {
		m, err := scanMember(rows)
		if err != nil {
			return nil, fmt.Errorf("scan peernet member: %w", err)
		}
		out = append(out, *m)
	}
	return out, rows.Err()
}

// SetMemberCertificate records the validity window of the latest certificate
// issued to a non-revoked member. It returns ErrPeernetNotFound otherwise.
func (r *PeernetNetworkRepository) SetMemberCertificate(ctx context.Context, networkID, nodeID string, issuedAt, expiresAt int64) error {
	res, err := r.db.ExecContext(ctx, `UPDATE peernet_members SET cert_issued_at = ?, cert_expires_at = ?
		WHERE network_id = ? AND node_id = ? AND revoked_at IS NULL`, issuedAt, expiresAt, networkID, nodeID)
	if err != nil {
		return fmt.Errorf("set peernet member certificate: %w", err)
	}
	c, err := rowsAffected(res, "set peernet member certificate")
	if err != nil {
		return err
	}
	if c == 0 {
		return ErrPeernetNotFound
	}
	return nil
}

// RevokeMember marks a member revoked. It returns ErrPeernetNotFound if the
// member is unknown or already revoked.
func (r *PeernetNetworkRepository) RevokeMember(ctx context.Context, networkID, nodeID string, at int64) error {
	res, err := r.db.ExecContext(ctx, `UPDATE peernet_members SET revoked_at = ?
		WHERE network_id = ? AND node_id = ? AND revoked_at IS NULL`, at, networkID, nodeID)
	if err != nil {
		return fmt.Errorf("revoke peernet member: %w", err)
	}
	c, err := rowsAffected(res, "revoke peernet member")
	if err != nil {
		return err
	}
	if c == 0 {
		return ErrPeernetNotFound
	}
	return nil
}

// ListRevokedSince returns the node IDs revoked at or after since.
func (r *PeernetNetworkRepository) ListRevokedSince(ctx context.Context, networkID string, since int64) ([]string, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT node_id FROM peernet_members
		WHERE network_id = ? AND revoked_at IS NOT NULL AND revoked_at >= ? ORDER BY node_id`, networkID, since)
	if err != nil {
		return nil, fmt.Errorf("list peernet revoked: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// GetInvite returns one invite by hash or ErrPeernetNotFound.
func (r *PeernetNetworkRepository) GetInvite(ctx context.Context, networkID, codeHash string) (*PeernetInvite, error) {
	var i PeernetInvite
	var used sql.NullInt64
	err := r.db.QueryRowContext(ctx, `SELECT network_id, code_hash, alias, created_at, expires_at, used_at, used_by
		FROM peernet_invites WHERE network_id = ? AND code_hash = ?`, networkID, codeHash).
		Scan(&i.NetworkID, &i.CodeHash, &i.Alias, &i.CreatedAt, &i.ExpiresAt, &used, &i.UsedBy)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrPeernetNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get peernet invite: %w", err)
	}
	i.UsedAt = used.Int64
	return &i, nil
}

// ListOrphanPeerNetworkIDs returns the network IDs that still have rows in
// peernet_peers although no such network exists any more (a cleanup that was
// interrupted). Callers clear them through the peer repository.
func (r *PeernetNetworkRepository) ListOrphanPeerNetworkIDs(ctx context.Context) ([]string, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT DISTINCT network_id FROM peernet_peers
		WHERE network_id NOT IN (SELECT network_id FROM peernet_networks) ORDER BY network_id`)
	if err != nil {
		return nil, fmt.Errorf("list orphan peernet peer networks: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan orphan peernet network: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
