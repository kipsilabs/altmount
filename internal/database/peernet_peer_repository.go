package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"net"
	"net/netip"
	"strings"
	"time"
)

// Peer sources stored in peernet_peers.source.
const (
	// PeernetSourceUser is an address the user entered. Never evicted.
	PeernetSourceUser = "user"
	// PeernetSourceExchange is an address learned from another peer.
	PeernetSourceExchange = "exchange"
	// PeernetSourceInbound is an address a connecting peer advertised.
	PeernetSourceInbound = "inbound"
	// PeernetSourceLocal is an address reported by a local discovery source
	// (trusted configuration, not a peer). Counted against its own cap and
	// never evicted.
	PeernetSourceLocal = "local"
)

// ErrPeernetPeerCapReached is returned by Insert when a cap is full and no
// stale unverified peer can be evicted to make room.
var ErrPeernetPeerCapReached = errors.New("peernet peers: cap reached")

// PeernetPeer is one known peer in one network. Time fields are zero when
// unset. FirstSeen is written once by Insert and never changed afterwards.
type PeernetPeer struct {
	NetworkID string // 64 lowercase hex
	NodeID    string // 64 lowercase hex
	Address   string
	Source    string
	// Verified means this instance dialled Address and the server there
	// proved NodeID.
	Verified    bool
	FirstSeen   time.Time
	LastSeen    time.Time
	LastSuccess time.Time
	Failures    int
	NextAttempt time.Time
	// CandidateAddress is an advertised, not yet verified replacement for
	// Address of a peer whose current address is verified or user-entered.
	CandidateAddress string
	// ChangedNodeID is set on a user-entered peer when its address presented a
	// different Node ID than the pinned one.
	ChangedNodeID string
}

// PeernetPeerCaps are hard row caps. A zero or missing value means no cap.
type PeernetPeerCaps struct {
	MaxTotal     int
	MaxPerSource map[string]int
	// MaxUnverifiedPerPrefix caps unverified exchange and inbound rows per
	// address prefix (/24 IPv4, /48 IPv6). Beyond it the prefix's own stalest
	// rows are evicted, so one range cannot crowd out the table.
	MaxUnverifiedPerPrefix int
}

// PeernetPeerCounts summarises a network's rows.
type PeernetPeerCounts struct {
	Total, Verified, User, Exchange, Inbound, Changed int
}

// PeernetPeerRepository stores known peers (table peernet_peers).
type PeernetPeerRepository struct {
	db *dialectAwareDB
}

// NewPeernetPeerRepository creates a repository over conn.
func NewPeernetPeerRepository(conn *sql.DB, d Dialect) *PeernetPeerRepository {
	return &PeernetPeerRepository{db: newDialectAwareDB(conn, d)}
}

type peerQuerier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

const peerColumns = `network_id, node_id, address, source, verified, first_seen, last_seen, last_success,
	failures, next_attempt, candidate_address, changed_node_id`

func unixOrZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}

func timeOrZero(u int64) time.Time {
	if u == 0 {
		return time.Time{}
	}
	return time.Unix(u, 0)
}

func scanPeer(s rowScanner) (PeernetPeer, error) {
	var p PeernetPeer
	var verified int
	var first, last, success, next int64
	if err := s.Scan(&p.NetworkID, &p.NodeID, &p.Address, &p.Source, &verified, &first, &last, &success,
		&p.Failures, &next, &p.CandidateAddress, &p.ChangedNodeID); err != nil {
		return PeernetPeer{}, err
	}
	p.Verified = verified != 0
	p.FirstSeen, p.LastSeen, p.LastSuccess, p.NextAttempt = timeOrZero(first), timeOrZero(last), timeOrZero(success), timeOrZero(next)
	return p, nil
}

func validPeerSource(s string) bool {
	return s == PeernetSourceUser || s == PeernetSourceExchange || s == PeernetSourceInbound || s == PeernetSourceLocal
}

func (p PeernetPeer) validate() error {
	if !isHexID(p.NetworkID) || p.NetworkID != strings.ToLower(p.NetworkID) {
		return errors.New("peernet peers: invalid network id")
	}
	if !isHexID(p.NodeID) || p.NodeID != strings.ToLower(p.NodeID) {
		return errors.New("peernet peers: invalid node id")
	}
	if p.Address == "" || len(p.Address) > 255 {
		return errors.New("peernet peers: invalid address")
	}
	if !validPeerSource(p.Source) {
		return fmt.Errorf("peernet peers: invalid source %q", p.Source)
	}
	if p.FirstSeen.IsZero() {
		return errors.New("peernet peers: first seen required")
	}
	return nil
}

// Insert adds p if no row exists for (network, node) and returns true. An
// existing row is left completely untouched (in particular FirstSeen) and
// Insert returns false. Caps are enforced in one transaction that is
// serialised per network (an advisory lock on PostgreSQL, the database write
// lock on SQLite), so concurrent inserts can neither overshoot a cap nor fail
// with a unique violation: the row itself is written with ON CONFLICT DO
// NOTHING and RowsAffected decides the result on both dialects.
//
// When a cap is full the stalest unverified exchange-learned peer is evicted
// first, then the stalest unverified inbound peer, then the worst verified
// exchange or inbound peer (most consecutive failures, then oldest success).
// User-entered and local-source peers are never evicted. If nothing can be
// evicted a non-user insert fails with ErrPeernetPeerCapReached.
// User-entered peers are always inserted. Unverified exchange and inbound
// rows are additionally capped per address prefix (see
// PeernetPeerCaps.MaxUnverifiedPerPrefix).
func (r *PeernetPeerRepository) Insert(ctx context.Context, p PeernetPeer, caps PeernetPeerCaps) (bool, error) {
	if err := p.validate(); err != nil {
		return false, err
	}
	if p.LastSeen.IsZero() {
		p.LastSeen = p.FirstSeen
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("insert peernet peer: begin: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit

	if err := r.lockNetwork(ctx, tx, p.NetworkID); err != nil {
		return false, err
	}
	var one int
	switch err := tx.QueryRowContext(ctx, `SELECT 1 FROM peernet_peers WHERE network_id = ? AND node_id = ?`,
		p.NetworkID, p.NodeID).Scan(&one); {
	case err == nil:
		return false, nil
	case !errors.Is(err, sql.ErrNoRows):
		return false, fmt.Errorf("insert peernet peer: lookup: %w", err)
	}
	if err := enforceInsertCaps(ctx, tx, p, caps); err != nil {
		return false, err
	}
	// A row inserted already verified counts from its first success.
	var firstVerified int64
	if p.Verified {
		firstVerified = unixOrZero(p.LastSuccess)
		if firstVerified == 0 {
			firstVerified = unixOrZero(p.FirstSeen)
		}
	}
	res, err := tx.ExecContext(ctx, `INSERT INTO peernet_peers (`+peerColumns+`, first_verified) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT (network_id, node_id) DO NOTHING`,
		p.NetworkID, p.NodeID, p.Address, p.Source, boolInt(p.Verified), unixOrZero(p.FirstSeen), unixOrZero(p.LastSeen),
		unixOrZero(p.LastSuccess), p.Failures, unixOrZero(p.NextAttempt), p.CandidateAddress, p.ChangedNodeID, firstVerified)
	if err != nil {
		return false, fmt.Errorf("insert peernet peer: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return false, nil
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("insert peernet peer: commit: %w", err)
	}
	return true, nil
}

// lockNetwork serialises the transaction against other inserts into the same
// network before the first read.
func (r *PeernetPeerRepository) lockNetwork(ctx context.Context, tx *dialectAwareTx, network string) error {
	if r.db.dialect.IsPostgres() {
		// Transaction-scoped: released at commit or rollback.
		if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(?)`, networkLockKey(network)); err != nil {
			return fmt.Errorf("insert peernet peer: lock: %w", err)
		}
		return nil
	}
	// Take the SQLite write lock before the first read. A read-then-write
	// transaction cannot be upgraded when another writer got in between, and
	// fails at once with "database is locked".
	if _, err := tx.ExecContext(ctx, `DELETE FROM peernet_peers WHERE 1 = 0`); err != nil {
		return fmt.Errorf("insert peernet peer: lock: %w", err)
	}
	return nil
}

// networkLockKey derives the advisory lock key for a network ID.
func networkLockKey(network string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte("peernet_peers:" + network))
	return int64(h.Sum64())
}

func enforceInsertCaps(ctx context.Context, tx peerQuerier, p PeernetPeer, caps PeernetPeerCaps) error {
	learned := p.Source == PeernetSourceExchange || p.Source == PeernetSourceInbound
	if learned && !p.Verified && caps.MaxUnverifiedPerPrefix > 0 {
		if err := trimUnverifiedPrefix(ctx, tx, p, caps.MaxUnverifiedPerPrefix); err != nil {
			return err
		}
	}
	if caps.MaxPerSource != nil && caps.MaxPerSource[p.Source] > 0 && p.Source != PeernetSourceUser {
		if err := makeRoom(ctx, tx, p.NetworkID, p.Source, caps.MaxPerSource[p.Source], p.Source); err != nil {
			return err
		}
	}
	if caps.MaxTotal > 0 {
		err := makeRoom(ctx, tx, p.NetworkID, "", caps.MaxTotal, "")
		if err != nil && p.Source != PeernetSourceUser {
			return err
		}
	}
	return nil
}

// peerAddrPrefix groups an address by the network that controls it: /24 for
// IPv4, /48 for IPv6, the lower-cased host for a hostname.
func peerAddrPrefix(address string) string {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		host = address
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return strings.ToLower(host)
	}
	ip = ip.Unmap()
	bits := 48
	if ip.Is4() {
		bits = 24
	}
	pre, err := ip.Prefix(bits)
	if err != nil {
		return host
	}
	return pre.String()
}

// trimUnverifiedPrefix makes room for one more unverified learned row in p's
// address prefix by evicting that prefix's stalest unverified rows, so one
// address range can only ever churn its own rows.
func trimUnverifiedPrefix(ctx context.Context, q peerQuerier, p PeernetPeer, limit int) error {
	rows, err := q.QueryContext(ctx, `SELECT node_id, address FROM peernet_peers
		WHERE network_id = ? AND verified = 0 AND source IN ('exchange','inbound')
		ORDER BY last_seen ASC, first_seen ASC, node_id ASC`, p.NetworkID)
	if err != nil {
		return fmt.Errorf("list unverified peernet peers: %w", err)
	}
	want := peerAddrPrefix(p.Address)
	var same []string
	for rows.Next() {
		var node, addr string
		if err := rows.Scan(&node, &addr); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan unverified peernet peer: %w", err)
		}
		if peerAddrPrefix(addr) == want {
			same = append(same, node)
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	_ = rows.Close()
	for len(same) >= limit {
		if _, err := q.ExecContext(ctx, `DELETE FROM peernet_peers WHERE network_id = ? AND node_id = ?`, p.NetworkID, same[0]); err != nil {
			return fmt.Errorf("evict peernet peer: %w", err)
		}
		same = same[1:]
	}
	return nil
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// makeRoom ensures fewer than limit rows exist in the scope (all rows of the
// network when countSource is empty, else that source's rows), evicting
// evictable rows (restricted to evictSource when set).
func makeRoom(ctx context.Context, q peerQuerier, network, countSource string, limit int, evictSource string) error {
	for {
		var n int
		var err error
		if countSource == "" {
			err = q.QueryRowContext(ctx, `SELECT COUNT(*) FROM peernet_peers WHERE network_id = ?`, network).Scan(&n)
		} else {
			err = q.QueryRowContext(ctx, `SELECT COUNT(*) FROM peernet_peers WHERE network_id = ? AND source = ?`,
				network, countSource).Scan(&n)
		}
		if err != nil {
			return fmt.Errorf("count peernet peers: %w", err)
		}
		if n < limit {
			return nil
		}
		victim, err := pickVictim(ctx, q, network, evictSource)
		if err != nil {
			return err
		}
		if _, err := q.ExecContext(ctx, `DELETE FROM peernet_peers WHERE network_id = ? AND node_id = ?`, network, victim); err != nil {
			return fmt.Errorf("evict peernet peer: %w", err)
		}
	}
}

// pickVictim chooses the row to evict: the stalest unverified learned peer,
// else the worst verified learned peer. User-entered and local-source peers
// are never candidates.
func pickVictim(ctx context.Context, q peerQuerier, network, evictSource string) (string, error) {
	const unverified = `SELECT node_id FROM peernet_peers WHERE network_id = ? AND verified = 0 AND source IN ('exchange','inbound')`
	const verified = `SELECT node_id FROM peernet_peers WHERE network_id = ? AND verified = 1 AND source IN ('exchange','inbound')`
	const unverifiedOrder = ` ORDER BY CASE source WHEN 'exchange' THEN 0 ELSE 1 END, last_seen ASC, first_seen ASC, node_id ASC LIMIT 1`
	const verifiedOrder = ` ORDER BY failures DESC, last_success ASC, first_seen ASC, node_id ASC LIMIT 1`
	for _, c := range []struct{ base, order string }{{unverified, unverifiedOrder}, {verified, verifiedOrder}} {
		query, args := c.base, []any{network}
		if evictSource != "" {
			query += ` AND source = ?`
			args = append(args, evictSource)
		}
		var victim string
		err := q.QueryRowContext(ctx, query+c.order, args...).Scan(&victim)
		if err == nil {
			return victim, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return "", fmt.Errorf("select peernet eviction victim: %w", err)
		}
	}
	return "", ErrPeernetPeerCapReached
}

// Get returns the peer or nil when absent.
func (r *PeernetPeerRepository) Get(ctx context.Context, network, node string) (*PeernetPeer, error) {
	p, err := scanPeer(r.db.QueryRowContext(ctx,
		`SELECT `+peerColumns+` FROM peernet_peers WHERE network_id = ? AND node_id = ?`, network, node))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get peernet peer: %w", err)
	}
	return &p, nil
}

// GetUserByAddress returns the user-entered peer pinned to address, or nil.
func (r *PeernetPeerRepository) GetUserByAddress(ctx context.Context, network, address string) (*PeernetPeer, error) {
	p, err := scanPeer(r.db.QueryRowContext(ctx,
		`SELECT `+peerColumns+` FROM peernet_peers WHERE network_id = ? AND source = 'user' AND address = ?
		 ORDER BY first_seen LIMIT 1`, network, address))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get user peernet peer: %w", err)
	}
	return &p, nil
}

func (r *PeernetPeerRepository) query(ctx context.Context, q string, args ...any) ([]PeernetPeer, error) {
	rows, err := r.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("query peernet peers: %w", err)
	}
	defer rows.Close()
	var out []PeernetPeer
	for rows.Next() {
		p, err := scanPeer(rows)
		if err != nil {
			return nil, fmt.Errorf("scan peernet peer: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// List returns every peer of the network, user-entered first, then by node ID.
func (r *PeernetPeerRepository) List(ctx context.Context, network string) ([]PeernetPeer, error) {
	return r.query(ctx, `SELECT `+peerColumns+` FROM peernet_peers WHERE network_id = ?
		ORDER BY CASE source WHEN 'user' THEN 0 WHEN 'inbound' THEN 2 ELSE 1 END, node_id`, network)
}

// ListDue returns up to limit peers whose next attempt is at or before now,
// ordered user-entered first, then verified, then the rest; fewest failures
// first within a class; oldest next attempt first after that.
func (r *PeernetPeerRepository) ListDue(ctx context.Context, network string, now time.Time, limit int) ([]PeernetPeer, error) {
	if limit <= 0 {
		return nil, nil
	}
	return r.query(ctx, `SELECT `+peerColumns+` FROM peernet_peers WHERE network_id = ? AND next_attempt <= ?
		ORDER BY CASE WHEN source = 'user' THEN 0 WHEN verified = 1 THEN 1 ELSE 2 END, failures, next_attempt, node_id
		LIMIT ?`, network, now.Unix(), limit)
}

// ListVerified returns up to limit verified peers with fewer than maxFailures
// consecutive failures, in random order.
func (r *PeernetPeerRepository) ListVerified(ctx context.Context, network string, maxFailures, limit int) ([]PeernetPeer, error) {
	if limit <= 0 {
		return nil, nil
	}
	return r.query(ctx, `SELECT `+peerColumns+` FROM peernet_peers WHERE network_id = ? AND verified = 1 AND failures < ?
		ORDER BY RANDOM() LIMIT ?`, network, maxFailures, limit)
}

func (r *PeernetPeerRepository) exec(ctx context.Context, what, q string, args ...any) (int64, error) {
	res, err := r.db.ExecContext(ctx, q, args...)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", what, err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// Touch records that the peer was seen at now. FirstSeen is not touched.
func (r *PeernetPeerRepository) Touch(ctx context.Context, network, node string, now time.Time) error {
	_, err := r.exec(ctx, "touch peernet peer",
		`UPDATE peernet_peers SET last_seen = ? WHERE network_id = ? AND node_id = ? AND last_seen < ?`,
		now.Unix(), network, node, now.Unix())
	return err
}

// SetUnverifiedAddress replaces the address of an unverified, non-user peer.
// It reports whether a row changed.
func (r *PeernetPeerRepository) SetUnverifiedAddress(ctx context.Context, network, node, address string, now time.Time) (bool, error) {
	n, err := r.exec(ctx, "set peernet peer address",
		`UPDATE peernet_peers SET address = ?, last_seen = ?, candidate_address = '', failures = 0, next_attempt = 0
		 WHERE network_id = ? AND node_id = ? AND verified = 0 AND source <> 'user' AND address <> ?`,
		address, now.Unix(), network, node, address)
	return n > 0, err
}

// SetCandidateAddress records an advertised replacement address that still
// has to be verified by dialling.
func (r *PeernetPeerRepository) SetCandidateAddress(ctx context.Context, network, node, address string, now time.Time) error {
	_, err := r.exec(ctx, "set peernet peer candidate",
		`UPDATE peernet_peers SET candidate_address = ?, last_seen = ?, next_attempt = 0
		 WHERE network_id = ? AND node_id = ? AND source <> 'user'`, address, now.Unix(), network, node)
	return err
}

// RecordSuccess marks a successful outbound dial that proved the node ID. A
// non-empty address replaces the stored one (a verified candidate) and clears
// the candidate. The peer becomes verified and its failure count resets; the
// time of its first verification is written once.
func (r *PeernetPeerRepository) RecordSuccess(ctx context.Context, network, node, address string, now, next time.Time) error {
	_, err := r.exec(ctx, "record peernet peer success",
		`UPDATE peernet_peers SET verified = 1, last_success = ?, last_seen = ?, failures = 0, next_attempt = ?,
		 first_verified = CASE WHEN first_verified = 0 THEN ? ELSE first_verified END,
		 address = CASE WHEN ? <> '' THEN ? ELSE address END,
		 candidate_address = CASE WHEN ? <> '' THEN '' ELSE candidate_address END
		 WHERE network_id = ? AND node_id = ?`,
		now.Unix(), now.Unix(), next.Unix(), now.Unix(), address, address, address, network, node)
	return err
}

// RecordFailure increments the failure count and schedules the next attempt
// at now plus backoff(failures). It returns the new failure count (0 when the
// peer is absent). Plain statements, no read-then-write transaction: the
// increment is atomic and the schedule is derived from the count it produced.
func (r *PeernetPeerRepository) RecordFailure(ctx context.Context, network, node string, now time.Time, backoff func(failures int) time.Duration) (int, error) {
	n, err := r.exec(ctx, "record peernet peer failure",
		`UPDATE peernet_peers SET failures = failures + 1 WHERE network_id = ? AND node_id = ?`, network, node)
	if err != nil || n == 0 {
		return 0, err
	}
	var f int
	if err := r.db.QueryRowContext(ctx, `SELECT failures FROM peernet_peers WHERE network_id = ? AND node_id = ?`,
		network, node).Scan(&f); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("record peernet peer failure: read: %w", err)
	}
	if _, err := r.exec(ctx, "schedule peernet peer retry",
		`UPDATE peernet_peers SET next_attempt = ? WHERE network_id = ? AND node_id = ?`,
		now.Add(backoff(f)).Unix(), network, node); err != nil {
		return f, err
	}
	return f, nil
}

// SetChangedNode flags a user-entered peer whose address presented a different
// Node ID (an empty value clears the flag). The pinned node ID is kept.
func (r *PeernetPeerRepository) SetChangedNode(ctx context.Context, network, node, changedTo string) error {
	_, err := r.exec(ctx, "set peernet peer changed node",
		`UPDATE peernet_peers SET changed_node_id = ? WHERE network_id = ? AND node_id = ? AND source = 'user'`,
		changedTo, network, node)
	return err
}

// PromoteToUser turns an existing peer into a user-entered one at address.
func (r *PeernetPeerRepository) PromoteToUser(ctx context.Context, network, node, address string) error {
	_, err := r.exec(ctx, "promote peernet peer",
		`UPDATE peernet_peers SET source = 'user', address = ?, candidate_address = '', changed_node_id = ''
		 WHERE network_id = ? AND node_id = ?`, address, network, node)
	return err
}

// PromoteToLocal marks an existing learned (exchange or inbound) peer as
// coming from a local discovery source. The stored address is not changed.
func (r *PeernetPeerRepository) PromoteToLocal(ctx context.Context, network, node string) error {
	_, err := r.exec(ctx, "promote peernet peer to local",
		`UPDATE peernet_peers SET source = 'local' WHERE network_id = ? AND node_id = ? AND source IN ('exchange','inbound')`,
		network, node)
	return err
}

// Delete removes one peer.
func (r *PeernetPeerRepository) Delete(ctx context.Context, network, node string) error {
	_, err := r.exec(ctx, "delete peernet peer", `DELETE FROM peernet_peers WHERE network_id = ? AND node_id = ?`, network, node)
	return err
}

// DeleteUserNotIn removes user-entered peers whose address is not in keep and
// returns how many were removed.
func (r *PeernetPeerRepository) DeleteUserNotIn(ctx context.Context, network string, keep []string) (int64, error) {
	peers, err := r.query(ctx, `SELECT `+peerColumns+` FROM peernet_peers WHERE network_id = ? AND source = 'user'`, network)
	if err != nil {
		return 0, err
	}
	keepSet := make(map[string]struct{}, len(keep))
	for _, a := range keep {
		keepSet[a] = struct{}{}
	}
	var n int64
	for _, p := range peers {
		if _, ok := keepSet[p.Address]; ok {
			continue
		}
		if err := r.Delete(ctx, network, p.NodeID); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// PeernetKnownMaxSilence is how recently a verified peer must have completed a
// mutual-TLS contact to still count as known.
const PeernetKnownMaxSilence = 7 * 24 * time.Hour

// KnownFor reports whether the node has been verified for at least d: a
// completed mutual-TLS contact proved its node ID at least d before now (the
// first such contact is written once and never reset by peer exchange), and
// its last such contact is within PeernetKnownMaxSilence of now. Peer-exchange
// entries are stored unverified and so are never known.
func (r *PeernetPeerRepository) KnownFor(ctx context.Context, network, node string, d time.Duration, now time.Time) (bool, error) {
	var firstVerified, lastSuccess int64
	err := r.db.QueryRowContext(ctx,
		`SELECT first_verified, last_success FROM peernet_peers WHERE network_id = ? AND node_id = ? AND verified = 1`,
		network, node).Scan(&firstVerified, &lastSuccess)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("peernet peer known for: %w", err)
	}
	return firstVerified > 0 && firstVerified <= now.Add(-d).Unix() &&
		lastSuccess >= now.Add(-PeernetKnownMaxSilence).Unix(), nil
}

// KnownAmong is KnownFor for many nodes at once: it returns the nodes (of
// those asked about) that are known by the same rule. Unknown nodes are
// absent from the result.
func (r *PeernetPeerRepository) KnownAmong(ctx context.Context, network string, nodes []string, d time.Duration, now time.Time) (map[string]bool, error) {
	out := map[string]bool{}
	err := r.knownRows(ctx, network, nodes, d, now, func(node, _ string) { out[node] = true })
	return out, err
}

// KnownPrefixes is KnownAmong that also returns, per known node, the network
// prefix (/24 IPv4, /48 IPv6) of the address this node verified it at. Nodes
// whose address yields no prefix are left out: they cannot be told apart from
// each other and so count as nothing.
func (r *PeernetPeerRepository) KnownPrefixes(ctx context.Context, network string, nodes []string, d time.Duration, now time.Time) (map[string]string, error) {
	out := map[string]string{}
	err := r.knownRows(ctx, network, nodes, d, now, func(node, address string) {
		if p := peerAddrPrefix(address); address != "" && p != "" {
			out[node] = p
		}
	})
	return out, err
}

func (r *PeernetPeerRepository) knownRows(ctx context.Context, network string, nodes []string, d time.Duration, now time.Time, each func(node, address string)) error {
	const chunk = 200
	for start := 0; start < len(nodes); start += chunk {
		part := nodes[start:min(start+chunk, len(nodes))]
		args := make([]any, 0, len(part)+3)
		args = append(args, network, now.Add(-d).Unix(), now.Add(-PeernetKnownMaxSilence).Unix())
		for _, n := range part {
			args = append(args, n)
		}
		q := `SELECT node_id, address FROM peernet_peers WHERE network_id = ? AND verified = 1
			AND first_verified > 0 AND first_verified <= ? AND last_success >= ?
			AND node_id IN (?` + strings.Repeat(",?", len(part)-1) + `)`
		rows, err := r.db.QueryContext(ctx, q, args...)
		if err != nil {
			return fmt.Errorf("peernet peers known among: %w", err)
		}
		for rows.Next() {
			var id, address string
			if err := rows.Scan(&id, &address); err != nil {
				rows.Close()
				return fmt.Errorf("scan peernet known peer: %w", err)
			}
			each(id, address)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("peernet peers known among: %w", err)
		}
		rows.Close()
	}
	return nil
}

// Counts returns row counts for the network.
func (r *PeernetPeerRepository) Counts(ctx context.Context, network string) (PeernetPeerCounts, error) {
	var c PeernetPeerCounts
	rows, err := r.db.QueryContext(ctx,
		`SELECT source, verified, CASE WHEN changed_node_id <> '' THEN 1 ELSE 0 END, COUNT(*)
		 FROM peernet_peers WHERE network_id = ? GROUP BY source, verified, 3`, network)
	if err != nil {
		return c, fmt.Errorf("count peernet peers: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var src string
		var verified, changed, n int
		if err := rows.Scan(&src, &verified, &changed, &n); err != nil {
			return c, fmt.Errorf("scan peernet counts: %w", err)
		}
		c.Total += n
		if verified != 0 {
			c.Verified += n
		}
		if changed != 0 {
			c.Changed += n
		}
		switch src {
		case PeernetSourceUser:
			c.User += n
		case PeernetSourceExchange:
			c.Exchange += n
		case PeernetSourceInbound:
			c.Inbound += n
		}
	}
	return c, rows.Err()
}

// Prune deletes non-user peers with at least minFailures consecutive failures
// whose last success (or first-seen time if they never succeeded) is before
// olderThan. It returns the number removed.
func (r *PeernetPeerRepository) Prune(ctx context.Context, network string, olderThan time.Time, minFailures int) (int64, error) {
	return r.exec(ctx, "prune peernet peers",
		`DELETE FROM peernet_peers WHERE network_id = ? AND source <> 'user' AND failures >= ?
		 AND (CASE WHEN last_success > 0 THEN last_success ELSE first_seen END) < ?`,
		network, minFailures, olderThan.Unix())
}
