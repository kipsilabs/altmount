package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// PeernetPasswordOriginRepository stores which files were imported with a
// password that came with the NZB's own content (table peernet_password_origin).
type PeernetPasswordOriginRepository struct{ db *dialectAwareDB }

// NewPeernetPasswordOriginRepository creates a repository over conn.
func NewPeernetPasswordOriginRepository(conn *sql.DB, d Dialect) *PeernetPasswordOriginRepository {
	return &PeernetPasswordOriginRepository{db: newDialectAwareDB(conn, d)}
}

// MarkFromNZB records that the files with these File IDs were imported with a
// password that came with the NZB's own content. It is idempotent. An ID that
// is not a well-formed File ID is an error and nothing is written.
func (r *PeernetPasswordOriginRepository) MarkFromNZB(ctx context.Context, fileIDs []string) error {
	for _, id := range fileIDs {
		if !isHexID(id) {
			return errors.New("peernet password origin: bad file id")
		}
	}
	now := time.Now().Unix()
	for _, id := range fileIDs {
		if _, err := r.db.ExecContext(ctx, `INSERT INTO peernet_password_origin (file_id, recorded_at) VALUES (?, ?)
			ON CONFLICT(file_id) DO NOTHING`, strings.ToLower(id), now); err != nil {
			return fmt.Errorf("peernet password origin: mark: %w", err)
		}
	}
	return nil
}

// FromNZB reports whether the file with this File ID was imported with a
// password that came with the NZB's own content. Anything unrecorded, and any
// malformed ID, is false.
func (r *PeernetPasswordOriginRepository) FromNZB(ctx context.Context, fileID string) (bool, error) {
	if !isHexID(fileID) {
		return false, nil
	}
	var one int
	err := r.db.QueryRowContext(ctx, `SELECT 1 FROM peernet_password_origin WHERE file_id = ?`, strings.ToLower(fileID)).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("peernet password origin: read: %w", err)
	}
	return true, nil
}
