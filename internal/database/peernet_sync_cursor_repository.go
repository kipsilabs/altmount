package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// PeernetSyncCursor is the sync position with one peer in one network. Both
// values are sequence numbers, never timestamps: PulledSeq is local to the
// remote peer, PushedSeq to this instance. Zero means nothing synced yet.
type PeernetSyncCursor struct {
	NetworkID  string
	PeerNodeID string
	PulledSeq  int64
	PushedSeq  int64
	// RemoteEpoch is the sequence-space epoch the peer reported when PulledSeq
	// was last reset; empty when none was recorded.
	RemoteEpoch string
	UpdatedAt   time.Time
}

// PeernetSyncCursorRepository stores sync cursors (table peernet_sync_cursors).
type PeernetSyncCursorRepository struct {
	db  *dialectAwareDB
	now func() time.Time
}

// NewPeernetSyncCursorRepository creates a repository over conn.
func NewPeernetSyncCursorRepository(conn *sql.DB, d Dialect) *PeernetSyncCursorRepository {
	return NewPeernetSyncCursorRepositoryWithClock(conn, d, time.Now)
}

// NewPeernetSyncCursorRepositoryWithClock is the same with an injectable clock.
func NewPeernetSyncCursorRepositoryWithClock(conn *sql.DB, d Dialect, now func() time.Time) *PeernetSyncCursorRepository {
	return &PeernetSyncCursorRepository{db: newDialectAwareDB(conn, d), now: now}
}

func validCursorArgs(network, peer string, seq int64) error {
	if !isLowerHexID(network) {
		return errors.New("peernet cursors: invalid network id")
	}
	if !isLowerHexID(peer) {
		return errors.New("peernet cursors: invalid peer node id")
	}
	if seq < 0 {
		return errors.New("peernet cursors: negative seq")
	}
	return nil
}

// Get returns the cursor, or a zero-valued one when the peer has none yet.
func (r *PeernetSyncCursorRepository) Get(ctx context.Context, networkID, peerNodeID string) (PeernetSyncCursor, error) {
	if err := validCursorArgs(networkID, peerNodeID, 0); err != nil {
		return PeernetSyncCursor{}, err
	}
	c := PeernetSyncCursor{NetworkID: networkID, PeerNodeID: peerNodeID}
	var updated int64
	err := r.db.QueryRowContext(ctx, `SELECT pulled_seq, pushed_seq, remote_epoch, updated_at FROM peernet_sync_cursors
		WHERE network_id = ? AND peer_node_id = ?`, networkID, peerNodeID).Scan(&c.PulledSeq, &c.PushedSeq, &c.RemoteEpoch, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return c, nil
	}
	if err != nil {
		return PeernetSyncCursor{}, fmt.Errorf("get peernet cursor: %w", err)
	}
	c.UpdatedAt = timeOrZero(updated)
	return c, nil
}

// SetPullCursor records the highest remote seq pulled. A value not above the
// stored one is ignored (the call succeeds): a cursor only moves backwards
// through ResetPullCursor.
func (r *PeernetSyncCursorRepository) SetPullCursor(ctx context.Context, networkID, peerNodeID string, seq int64) error {
	return r.setMonotonic(ctx, "pulled_seq", networkID, peerNodeID, seq)
}

// SetPushCursor records the highest local seq pushed to the peer, with the same
// never-backwards rule as SetPullCursor.
func (r *PeernetSyncCursorRepository) SetPushCursor(ctx context.Context, networkID, peerNodeID string, seq int64) error {
	return r.setMonotonic(ctx, "pushed_seq", networkID, peerNodeID, seq)
}

func (r *PeernetSyncCursorRepository) setMonotonic(ctx context.Context, column, networkID, peerNodeID string, seq int64) error {
	if err := validCursorArgs(networkID, peerNodeID, seq); err != nil {
		return err
	}
	// column is one of two compile-time constants, never caller input.
	insertCols, insertVals := "pulled_seq, pushed_seq", "?, 0"
	if column == "pushed_seq" {
		insertVals = "0, ?"
	}
	q := `INSERT INTO peernet_sync_cursors (network_id, peer_node_id, ` + insertCols + `, updated_at)
		VALUES (?, ?, ` + insertVals + `, ?)
		ON CONFLICT (network_id, peer_node_id) DO UPDATE SET
			` + column + ` = CASE WHEN excluded.` + column + ` > peernet_sync_cursors.` + column + `
				THEN excluded.` + column + ` ELSE peernet_sync_cursors.` + column + ` END,
			updated_at = excluded.updated_at`
	if _, err := r.db.ExecContext(ctx, q, networkID, peerNodeID, seq, r.now().Unix()); err != nil {
		return fmt.Errorf("set peernet cursor: %w", err)
	}
	return nil
}

// ResetPullCursor sets the pull cursor to seq even when that moves it
// backwards. Use it when a peer reports a lower max seq (its database was
// reset). The push cursor is left alone.
func (r *PeernetSyncCursorRepository) ResetPullCursor(ctx context.Context, networkID, peerNodeID string, seq int64) error {
	if err := validCursorArgs(networkID, peerNodeID, seq); err != nil {
		return err
	}
	if _, err := r.db.ExecContext(ctx, `INSERT INTO peernet_sync_cursors (network_id, peer_node_id, pulled_seq, pushed_seq, updated_at)
		VALUES (?, ?, ?, 0, ?)
		ON CONFLICT (network_id, peer_node_id) DO UPDATE SET
			pulled_seq = excluded.pulled_seq, updated_at = excluded.updated_at`,
		networkID, peerNodeID, seq, r.now().Unix()); err != nil {
		return fmt.Errorf("reset peernet pull cursor: %w", err)
	}
	return nil
}

// Delete removes one peer's cursor. An absent row is not an error.
func (r *PeernetSyncCursorRepository) Delete(ctx context.Context, networkID, peerNodeID string) error {
	if err := validCursorArgs(networkID, peerNodeID, 0); err != nil {
		return err
	}
	if _, err := r.db.ExecContext(ctx, `DELETE FROM peernet_sync_cursors WHERE network_id = ? AND peer_node_id = ?`,
		networkID, peerNodeID); err != nil {
		return fmt.Errorf("delete peernet cursor: %w", err)
	}
	return nil
}

// ResetPullCursorEpoch is ResetPullCursor that also records the epoch the peer
// reported, so a later change of that epoch is detected. The push cursor is
// left alone.
func (r *PeernetSyncCursorRepository) ResetPullCursorEpoch(ctx context.Context, networkID, peerNodeID string, seq int64, epoch string) error {
	if err := validCursorArgs(networkID, peerNodeID, seq); err != nil {
		return err
	}
	if len(epoch) > 64 || !isCursorEpoch(epoch) {
		return errors.New("peernet cursors: invalid epoch")
	}
	if _, err := r.db.ExecContext(ctx, `INSERT INTO peernet_sync_cursors (network_id, peer_node_id, pulled_seq, pushed_seq, remote_epoch, updated_at)
		VALUES (?, ?, ?, 0, ?, ?)
		ON CONFLICT (network_id, peer_node_id) DO UPDATE SET
			pulled_seq = excluded.pulled_seq, remote_epoch = excluded.remote_epoch, updated_at = excluded.updated_at`,
		networkID, peerNodeID, seq, epoch, r.now().Unix()); err != nil {
		return fmt.Errorf("reset peernet pull cursor: %w", err)
	}
	return nil
}

func isCursorEpoch(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'z') {
			return false
		}
	}
	return true
}

// DeleteOrphans removes the cursors of peers that no longer have a peer row
// (pruned, removed, or their network was deleted) and returns how many.
func (r *PeernetSyncCursorRepository) DeleteOrphans(ctx context.Context) (int64, error) {
	res, err := r.db.ExecContext(ctx, `DELETE FROM peernet_sync_cursors WHERE NOT EXISTS (
		SELECT 1 FROM peernet_peers p
		WHERE p.network_id = peernet_sync_cursors.network_id AND p.node_id = peernet_sync_cursors.peer_node_id)`)
	if err != nil {
		return 0, fmt.Errorf("delete orphan peernet cursors: %w", err)
	}
	return rowsAffected(res, "delete orphan peernet cursors")
}
