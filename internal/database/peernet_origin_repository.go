package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/kipsilabs/altmount/internal/nzbname"
)

// PeernetOrigin is what the import bookkeeping knows about where one library
// file came from. Every field may be empty. It is read-only input for the
// peer network's publishing exclusions and release records; Category and
// Indexer are never published.
type PeernetOrigin struct {
	// Title is the release (NZB) name, as the import recorded it.
	Title string
	// Category is the import category (for example the ARR's).
	Category string
	// Indexer is the indexer the release came from; "" when unknown.
	Indexer string
	// QueueMetadata is the raw JSON metadata stored with the import. Only
	// Stremio-originated imports carry media identifiers in it. The caller
	// must parse it defensively.
	QueueMetadata string
	// NamePassword is a password spelled in the NZB name (for example
	// "Name{{pw}}.nzb"). It exists only so the publisher can refuse a title
	// that still contains it: never store, log or publish it.
	NamePassword string
}

// PeernetOriginRepository resolves a virtual file path to its PeernetOrigin
// from import_history, import_queue and file_health.
type PeernetOriginRepository struct {
	db *dialectAwareDB
}

// NewPeernetOriginRepository creates a repository over conn.
func NewPeernetOriginRepository(conn *sql.DB, d Dialect) *PeernetOriginRepository {
	return &PeernetOriginRepository{db: newDialectAwareDB(conn, d)}
}

// maxOriginAncestors bounds how many parent folders of a file are looked up.
const maxOriginAncestors = 8

// originCandidates returns p followed by its ancestor folders, nearest first,
// at most maxOriginAncestors of them. p is already normalised (no leading or
// trailing slash).
func originCandidates(p string) []string {
	out := []string{p}
	for len(out) <= maxOriginAncestors {
		i := strings.LastIndex(p, "/")
		if i <= 0 {
			break
		}
		p = p[:i]
		out = append(out, p)
	}
	return out
}

func knownIndexer(s string) bool {
	s = strings.TrimSpace(s)
	return s != "" && !strings.EqualFold(s, "unknown")
}

// cleanQueueName returns the plain release name of a queued NZB path: the
// importer's "<id>-" prefix, the NZB suffix and any password are removed.
func cleanQueueName(nzbPath string) string { return nzbname.ReleaseName(nzbPath, true) }

// OriginForPath returns the origin of the file at virtualPath. A file nothing
// is known about yields the zero PeernetOrigin and no error.
//
// The newest import_history row for the path is the primary source: it
// outlives the queue row. Fields it lacks are filled from the queue row it
// points at (when that still exists) and, for the indexer and metadata, from
// file_health. An indexer of "Unknown" counts as no indexer.
func (r *PeernetOriginRepository) OriginForPath(ctx context.Context, virtualPath string) (PeernetOrigin, error) {
	p := normalizeHealthPath(virtualPath)
	if p == "" {
		return PeernetOrigin{}, errors.New("peernet origin: empty file path")
	}
	var o PeernetOrigin
	var nzbID sql.NullInt64
	var name, category, indexer, meta sql.NullString
	// Single-file imports record the file's own path; multi-file and archive
	// imports record the release folder. So the file's path and each of its
	// ancestor folders are candidates: the longest (nearest) path wins, then
	// the newest row. An IN list of exact paths keeps the lookup on the
	// virtual_path index.
	cands := originCandidates(p)
	in := strings.TrimSuffix(strings.Repeat("?,", len(cands)), ",")
	args := make([]any, len(cands))
	for i, c := range cands {
		args[i] = c
	}
	err := r.db.QueryRowContext(ctx,
		`SELECT nzb_id, nzb_name, category, indexer, CAST(metadata AS TEXT) FROM import_history
		 WHERE virtual_path IN (`+in+`) ORDER BY LENGTH(virtual_path) DESC, completed_at DESC, id DESC LIMIT 1`, args...).
		Scan(&nzbID, &name, &category, &indexer, &meta)
	switch {
	case errors.Is(err, sql.ErrNoRows):
	case err != nil:
		return PeernetOrigin{}, fmt.Errorf("peernet origin: read import history: %w", err)
	default:
		o.Title = nzbname.ReleaseName(name.String, false)
		o.NamePassword = nzbname.Password(name.String)
		o.Category = strings.TrimSpace(category.String)
		if knownIndexer(indexer.String) {
			o.Indexer = strings.TrimSpace(indexer.String)
		}
		o.QueueMetadata = meta.String
	}

	if nzbID.Valid {
		var qPath, qCat, qIdx, qMeta sql.NullString
		err := r.db.QueryRowContext(ctx,
			`SELECT nzb_path, category, indexer, CAST(metadata AS TEXT) FROM import_queue WHERE id = ?`, nzbID.Int64).
			Scan(&qPath, &qCat, &qIdx, &qMeta)
		switch {
		case errors.Is(err, sql.ErrNoRows):
		case err != nil:
			return PeernetOrigin{}, fmt.Errorf("peernet origin: read import queue: %w", err)
		default:
			if o.Title == "" && qPath.String != "" {
				o.Title = cleanQueueName(qPath.String)
				if o.NamePassword == "" {
					o.NamePassword = nzbname.Password(qPath.String)
				}
			}
			if o.Category == "" {
				o.Category = strings.TrimSpace(qCat.String)
			}
			if o.Indexer == "" && knownIndexer(qIdx.String) {
				o.Indexer = strings.TrimSpace(qIdx.String)
			}
			if o.QueueMetadata == "" {
				o.QueueMetadata = qMeta.String
			}
		}
	}

	var hIdx, hMeta sql.NullString
	err = r.db.QueryRowContext(ctx,
		`SELECT indexer, CAST(metadata AS TEXT) FROM file_health WHERE file_path = ?`, p).Scan(&hIdx, &hMeta)
	switch {
	case errors.Is(err, sql.ErrNoRows):
	case err != nil:
		return PeernetOrigin{}, fmt.Errorf("peernet origin: read file health: %w", err)
	default:
		if o.Indexer == "" && knownIndexer(hIdx.String) {
			o.Indexer = strings.TrimSpace(hIdx.String)
		}
		if o.QueueMetadata == "" {
			o.QueueMetadata = hMeta.String
		}
	}
	return o, nil
}
