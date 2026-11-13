package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
)

// Scopes of peernet_record_counts rows.
const (
	countScopeNetwork   = 1 // key ''
	countScopePublisher = 2 // key publisher node ID
	countScopeRelayer   = 3 // key relayer node ID, peer-sourced rows only
)

// recordRef is the part of a stored row the counters need.
type recordRef struct {
	seq       int64
	publisher string
	source    int
	relayer   string
}

func getCount(ctx context.Context, tx *dialectAwareTx, network string, scope int, key string) (int64, error) {
	var n int64
	err := tx.QueryRowContext(ctx, `SELECT n FROM peernet_record_counts WHERE network_id = ? AND scope = ? AND key = ?`,
		network, scope, key).Scan(&n)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("peernet records: read count: %w", err)
	}
	return n, nil
}

func addCount(ctx context.Context, tx *dialectAwareTx, network string, scope int, key string, delta int64) error {
	if delta == 0 {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO peernet_record_counts (network_id, scope, key, n) VALUES (?, ?, ?, ?)
		ON CONFLICT (network_id, scope, key) DO UPDATE SET n = peernet_record_counts.n + excluded.n`,
		network, scope, key, delta); err != nil {
		return fmt.Errorf("peernet records: update count: %w", err)
	}
	if delta < 0 {
		if _, err := tx.ExecContext(ctx, `DELETE FROM peernet_record_counts WHERE network_id = ? AND scope = ? AND key = ? AND n <= 0`,
			network, scope, key); err != nil {
			return fmt.Errorf("peernet records: prune count: %w", err)
		}
	}
	return nil
}

// addRowCounts adds delta rows of ref to the network, publisher and (for peer
// rows) relayer counters.
func addRowCounts(ctx context.Context, tx *dialectAwareTx, network string, ref recordRef, delta int64) error {
	if err := addCount(ctx, tx, network, countScopeNetwork, "", delta); err != nil {
		return err
	}
	if err := addCount(ctx, tx, network, countScopePublisher, ref.publisher, delta); err != nil {
		return err
	}
	if ref.source == PeernetRecordSourcePeer {
		return addCount(ctx, tx, network, countScopeRelayer, ref.relayer, delta)
	}
	return nil
}

// largestRelayer returns the relayer holding the most rows of the network.
func largestRelayer(ctx context.Context, tx *dialectAwareTx, network string) (string, bool, error) {
	var key string
	err := tx.QueryRowContext(ctx, `SELECT key FROM peernet_record_counts WHERE network_id = ? AND scope = ?
		ORDER BY n DESC LIMIT 1`, network, countScopeRelayer).Scan(&key)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("peernet records: largest relayer: %w", err)
	}
	return key, true, nil
}

// selectRefs reads up to limit rows of the network matching cond. The rows are
// fully read and closed before it returns, so the caller can write on tx.
func selectRefs(ctx context.Context, tx *dialectAwareTx, network, cond string, args []any, order string, limit int) ([]recordRef, error) {
	q := `SELECT seq, publisher_node_id, source, relayer_node_id FROM peernet_records WHERE network_id = ? AND ` +
		cond + ` ORDER BY ` + order + ` LIMIT ?`
	rows, err := tx.QueryContext(ctx, q, append(append([]any{network}, args...), limit)...)
	if err != nil {
		return nil, fmt.Errorf("peernet records: select rows: %w", err)
	}
	defer rows.Close()
	var out []recordRef
	for rows.Next() {
		var ref recordRef
		if err := rows.Scan(&ref.seq, &ref.publisher, &ref.source, &ref.relayer); err != nil {
			return nil, fmt.Errorf("peernet records: scan row: %w", err)
		}
		out = append(out, ref)
	}
	return out, rows.Err()
}

// deleteRefs deletes the rows and decrements the counters by exactly the
// number of rows that were really deleted.
func deleteRefs(ctx context.Context, tx *dialectAwareTx, network string, refs []recordRef) error {
	type key struct {
		publisher, relayer string
		source             int
	}
	groups := map[key]int64{}
	for _, ref := range refs {
		res, err := tx.ExecContext(ctx, `DELETE FROM peernet_records WHERE network_id = ? AND seq = ?`, network, ref.seq)
		if err != nil {
			return fmt.Errorf("peernet records: delete row: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("peernet records: delete row: %w", err)
		}
		if n > 0 {
			groups[key{ref.publisher, ref.relayer, ref.source}] += n
		}
	}
	for k, n := range groups {
		if err := addRowCounts(ctx, tx, network, recordRef{publisher: k.publisher, source: k.source, relayer: k.relayer}, -n); err != nil {
			return err
		}
	}
	return nil
}

// PeernetCountMismatch describes one maintained counter that disagrees with
// the rows.
type PeernetCountMismatch struct {
	Scope      string // "network", "publisher" or "relayer"
	Key        string
	Stored     int64
	Actual     int64
	NetworkID  string
	Reconciled bool
}

type countKey struct {
	scope int
	key   string
}

func scopeName(s int) string {
	switch s {
	case countScopeNetwork:
		return "network"
	case countScopePublisher:
		return "publisher"
	}
	return "relayer"
}

// actualCounts derives the counters of one network from its rows.
func actualCounts(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, network string) (map[countKey]int64, error) {
	out := map[countKey]int64{}
	rows, err := q.QueryContext(ctx, `SELECT publisher_node_id, source, relayer_node_id, COUNT(*) FROM peernet_records
		WHERE network_id = ? GROUP BY publisher_node_id, source, relayer_node_id`, network)
	if err != nil {
		return nil, fmt.Errorf("peernet records: derive counts: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var pub, rel string
		var source int
		var n int64
		if err := rows.Scan(&pub, &source, &rel, &n); err != nil {
			return nil, fmt.Errorf("peernet records: derive counts: %w", err)
		}
		out[countKey{countScopeNetwork, ""}] += n
		out[countKey{countScopePublisher, pub}] += n
		if source == PeernetRecordSourcePeer {
			out[countKey{countScopeRelayer, rel}] += n
		}
	}
	return out, rows.Err()
}

func storedCounts(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, network string) (map[countKey]int64, error) {
	out := map[countKey]int64{}
	rows, err := q.QueryContext(ctx, `SELECT scope, key, n FROM peernet_record_counts WHERE network_id = ?`, network)
	if err != nil {
		return nil, fmt.Errorf("peernet records: read counts: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var scope int
		var key string
		var n int64
		if err := rows.Scan(&scope, &key, &n); err != nil {
			return nil, fmt.Errorf("peernet records: read counts: %w", err)
		}
		out[countKey{scope, key}] = n
	}
	return out, rows.Err()
}

func diffCounts(network string, stored, actual map[countKey]int64) []PeernetCountMismatch {
	var out []PeernetCountMismatch
	seen := map[countKey]struct{}{}
	for k, a := range actual {
		seen[k] = struct{}{}
		if stored[k] != a {
			out = append(out, PeernetCountMismatch{NetworkID: network, Scope: scopeName(k.scope), Key: k.key, Stored: stored[k], Actual: a})
		}
	}
	for k, st := range stored {
		if _, ok := seen[k]; !ok && st != 0 {
			out = append(out, PeernetCountMismatch{NetworkID: network, Scope: scopeName(k.scope), Key: k.key, Stored: st})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Scope != out[j].Scope {
			return out[i].Scope < out[j].Scope
		}
		return out[i].Key < out[j].Key
	})
	return out
}

// CheckCounts compares the maintained counters of a network with its rows
// without changing anything. An empty result means they agree.
func (r *PeernetRecordRepository) CheckCounts(ctx context.Context, networkID string) ([]PeernetCountMismatch, error) {
	if !isLowerHexID(networkID) {
		return nil, errors.New("peernet records: invalid network id")
	}
	actual, err := actualCounts(ctx, r.db, networkID)
	if err != nil {
		return nil, err
	}
	stored, err := storedCounts(ctx, r.db, networkID)
	if err != nil {
		return nil, err
	}
	return diffCounts(networkID, stored, actual), nil
}

// ReconcileCounts re-derives the counters of a network from its rows under the
// network lock and corrects any difference. It returns what it corrected. It
// is the cheap periodic safety net behind the in-transaction bookkeeping.
func (r *PeernetRecordRepository) ReconcileCounts(ctx context.Context, networkID string) ([]PeernetCountMismatch, error) {
	if !isLowerHexID(networkID) {
		return nil, errors.New("peernet records: invalid network id")
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("reconcile peernet counts: begin: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit
	if err := r.lockNetwork(ctx, tx, networkID); err != nil {
		return nil, err
	}
	actual, err := actualCounts(ctx, tx, networkID)
	if err != nil {
		return nil, err
	}
	stored, err := storedCounts(ctx, tx, networkID)
	if err != nil {
		return nil, err
	}
	diff := diffCounts(networkID, stored, actual)
	if len(diff) == 0 {
		return nil, nil
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM peernet_record_counts WHERE network_id = ?`, networkID); err != nil {
		return nil, fmt.Errorf("reconcile peernet counts: clear: %w", err)
	}
	for k, n := range actual {
		if _, err := tx.ExecContext(ctx, `INSERT INTO peernet_record_counts (network_id, scope, key, n) VALUES (?, ?, ?, ?)`,
			networkID, k.scope, k.key, n); err != nil {
			return nil, fmt.Errorf("reconcile peernet counts: write: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("reconcile peernet counts: commit: %w", err)
	}
	for i := range diff {
		diff[i].Reconciled = true
	}
	return diff, nil
}
