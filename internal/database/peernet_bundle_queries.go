package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Bundle query bounds.
const (
	// PeernetBundleOffersMaxLimit caps BundleOffers results.
	PeernetBundleOffersMaxLimit = 100
	// PeernetBundleMismatchMaxLimit caps ListBundleLevelMismatch results.
	PeernetBundleMismatchMaxLimit = 200
)

// PeernetBundleOffer is one stored release record that announces a bundle.
// The Bundle ID comes from the signed record.
type PeernetBundleOffer struct {
	NetworkID       string
	PublisherNodeID string
	BundleID        string
	// ReleaseScheme is PeernetSchemeNZB or PeernetSchemeFiles: how the record's
	// release ID was derived, and so how the bundle is verified.
	ReleaseScheme int
	CreatedAt     time.Time
	Source        int
}

// BundleOffers returns the non-expired release records of releaseID that
// announce a bundle, across the given networks (at most
// PeernetMaxNetworksPerQuery), newest first and at most
// min(limit, PeernetBundleOffersMaxLimit). It is served by the
// (network, release, kind) index.
func (r *PeernetRecordRepository) BundleOffers(ctx context.Context, networkIDs []string, releaseID string, limit int) ([]PeernetBundleOffer, error) {
	switch {
	case !isLowerHexID(releaseID):
		return nil, errors.New("peernet records: invalid release id")
	case len(networkIDs) == 0:
		return nil, errors.New("peernet records: no networks")
	case len(networkIDs) > PeernetMaxNetworksPerQuery:
		return nil, ErrPeernetTooManyIDs
	case limit < 1:
		return nil, errors.New("peernet records: limit must be positive")
	}
	limit = min(limit, PeernetBundleOffersMaxLimit)
	args := make([]any, 0, len(networkIDs)+4)
	for _, n := range networkIDs {
		if !isLowerHexID(n) {
			return nil, errors.New("peernet records: invalid network id")
		}
		args = append(args, n)
	}
	args = append(args, releaseID, PeernetRecordKindRelease, r.now().Unix(), limit)
	rows, err := r.db.QueryContext(ctx,
		`SELECT network_id, publisher_node_id, bundle_id, release_scheme, created_at, source
		 FROM peernet_records
		 WHERE network_id IN (?`+strings.Repeat(",?", len(networkIDs)-1)+`) AND release_id = ? AND kind = ?
		   AND bundle_id <> '' AND expires_at > ?
		 ORDER BY created_at DESC, seq DESC, network_id LIMIT ?`, args...)
	if err != nil {
		return nil, fmt.Errorf("query peernet bundle offers: %w", err)
	}
	defer rows.Close()
	var out []PeernetBundleOffer
	for rows.Next() {
		var o PeernetBundleOffer
		var created int64
		if err := rows.Scan(&o.NetworkID, &o.PublisherNodeID, &o.BundleID, &o.ReleaseScheme, &created, &o.Source); err != nil {
			return nil, fmt.Errorf("scan peernet bundle offer: %w", err)
		}
		o.CreatedAt = timeOrZero(created)
		out = append(out, o)
	}
	return out, rows.Err()
}

// GetReleaseByBundle returns the release-record state of the release this node
// announced in network with the given Bundle ID. It is the only way a bundle
// request is matched: an ID this node did not announce there is not found.
func (r *PeernetPublishStateRepository) GetReleaseByBundle(ctx context.Context, network, bundleID string) (PeernetPublishState, bool, error) {
	if !isLowerHexID(network) || !isLowerHexID(bundleID) {
		return PeernetPublishState{}, false, errors.New("peernet publish state: invalid network or bundle id")
	}
	s := PeernetPublishState{NetworkID: network, Kind: PeernetRecordKindRelease, BundleID: bundleID}
	var published int64
	err := r.db.QueryRowContext(ctx,
		`SELECT release_id, published_at, origin_category, origin_indexer, bundle_level FROM peernet_publish_state
		 WHERE network_id = ? AND bundle_id = ? AND kind = ? ORDER BY published_at DESC LIMIT 1`,
		network, bundleID, PeernetRecordKindRelease).
		Scan(&s.ReleaseID, &published, &s.OriginCategory, &s.OriginIndexer, &s.BundleLevel)
	if errors.Is(err, sql.ErrNoRows) {
		return PeernetPublishState{}, false, nil
	}
	if err != nil {
		return PeernetPublishState{}, false, fmt.Errorf("get peernet publish state by bundle: %w", err)
	}
	s.PublishedAt = timeOrZero(published)
	return s, true, nil
}

// ListBundleLevelMismatch returns up to limit release-record states of network
// whose publication does not match the network's current level: with
// wantBundle set (level 3) the records published without a bundle decision,
// with it clear (level below 3) the records published at level 3. These are the
// records to announce again. Oldest first.
func (r *PeernetPublishStateRepository) ListBundleLevelMismatch(ctx context.Context, network string, wantBundle bool, limit int) ([]PeernetPublishState, error) {
	if !isLowerHexID(network) {
		return nil, errors.New("peernet publish state: invalid network id")
	}
	if limit < 1 {
		return nil, errors.New("peernet publish state: limit must be positive")
	}
	limit = min(limit, PeernetBundleMismatchMaxLimit)
	cond := `bundle_level >= ?`
	if wantBundle {
		cond = `bundle_level < ?`
	}
	rows, err := r.db.QueryContext(ctx,
		`SELECT release_id, published_at, bundle_id, bundle_level FROM peernet_publish_state
		 WHERE network_id = ? AND kind = ? AND `+cond+` ORDER BY published_at, release_id LIMIT ?`,
		network, PeernetRecordKindRelease, PeernetBundleLevel, limit)
	if err != nil {
		return nil, fmt.Errorf("list peernet bundle level mismatch: %w", err)
	}
	defer rows.Close()
	var out []PeernetPublishState
	for rows.Next() {
		s := PeernetPublishState{NetworkID: network, Kind: PeernetRecordKindRelease}
		var published int64
		if err := rows.Scan(&s.ReleaseID, &published, &s.BundleID, &s.BundleLevel); err != nil {
			return nil, fmt.Errorf("scan peernet bundle level mismatch: %w", err)
		}
		s.PublishedAt = timeOrZero(published)
		out = append(out, s)
	}
	return out, rows.Err()
}

// PeernetBundleLevel is the exposure level at which bundles are announced.
const PeernetBundleLevel = 3

// MarkBundleStale clears the bundle decision of a release record's state, so
// the next level-3 sweep announces it again. The announced Bundle ID stays
// until the new record replaces it.
func (r *PeernetPublishStateRepository) MarkBundleStale(ctx context.Context, network, release string) error {
	if !isLowerHexID(network) || !isLowerHexID(release) {
		return errors.New("peernet publish state: invalid network or release id")
	}
	if _, err := r.db.ExecContext(ctx,
		`UPDATE peernet_publish_state SET bundle_level = 0 WHERE network_id = ? AND release_id = ? AND kind = ?`,
		network, release, PeernetRecordKindRelease); err != nil {
		return fmt.Errorf("mark peernet bundle stale: %w", err)
	}
	return nil
}

// CountAnnouncedBundles returns how many release records this node announced
// with a Bundle ID, over every network.
func (r *PeernetPublishStateRepository) CountAnnouncedBundles(ctx context.Context) (int, error) {
	var n int
	if err := r.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM peernet_publish_state WHERE kind = ? AND bundle_id <> ''`,
		PeernetRecordKindRelease).Scan(&n); err != nil {
		return 0, fmt.Errorf("count peernet announced bundles: %w", err)
	}
	return n, nil
}

// ListUnpublishedReleases returns up to limit release IDs of the content index
// that have no release-record publish state on network: releases that became
// eligible while the network did not publish (level below 2).
func (r *PeernetPublishStateRepository) ListUnpublishedReleases(ctx context.Context, network string, limit int) ([]string, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT DISTINCT c.release_id FROM peernet_content_index c
		WHERE c.release_id <> ''
		  AND NOT EXISTS (SELECT 1 FROM peernet_publish_state s
		                  WHERE s.network_id = ? AND s.kind = ? AND s.release_id = c.release_id)
		ORDER BY c.release_id LIMIT ?`, network, PeernetRecordKindRelease, limit)
	if err != nil {
		return nil, fmt.Errorf("peernet publish state: list unpublished releases: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
