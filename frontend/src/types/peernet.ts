// Peer network types. These mirror the backend contract for the `peernet`
// config section and the /peernet/* endpoints. Timestamps are ISO strings or
// null. Every string coming from status, peers, members and invites is
// untrusted text from other machines and must be rendered as plain text only.

export type PeernetLevel = 0 | 1 | 2 | 3;

export interface PeernetOpenNetworkConfig {
	level: PeernetLevel;
	peers: string[];
	/** Use open-network layouts to skip archive analysis on NZB imports (default false). */
	fast_path: boolean;
}

export interface PeernetSharingConfig {
	excluded_categories: string[];
	excluded_indexers: string[];
}

export interface PeernetConfig {
	enabled: boolean;
	accept_incoming: boolean;
	listen_port: number;
	advertise_address: string;
	open_network: PeernetOpenNetworkConfig;
	sharing: PeernetSharingConfig;
	min_corroboration: number;
}

export type PeernetNetworkKind = "open" | "private";
export type PeernetNetworkRole = "open" | "host" | "member";

export interface PeernetPeerCounts {
	total: number;
	verified: number;
	active: number;
	node_id_changed: number;
	unresolved: number;
}

export interface PeernetNetworkStatus {
	id: string;
	name: string;
	kind: PeernetNetworkKind;
	role: PeernetNetworkRole;
	level: PeernetLevel;
	allow_private_addresses: boolean;
	host_address: string;
	cert_expires_at: string | null;
	/** True on a joined network whose host removed this node. */
	revoked: boolean;
	revoked_at: string | null;
	peers: PeernetPeerCounts;
	/** Absent on a backend without metadata sharing. */
	records?: PeernetRecordsStatus;
}

export interface PeernetRecordCounts {
	local: number;
	peer: number;
}

export interface PeernetRecordsStatus {
	releases?: PeernetRecordCounts;
	attestations?: PeernetRecordCounts;
	publishers?: number;
	/** RFC3339 time of the last successful pull, or null when none yet. */
	last_pull?: string | null;
	stored?: number;
	duplicates?: number;
	/** Rejection reason key to count. Keys are machine names. */
	rejected?: Record<string, number>;
	peers_backed_off?: number;
	peer_backoffs?: number;
}

/** What the publishing pipeline did since start. Keys of the maps are machine names. */
export interface PeernetPublishStatus {
	/** Record kind ("release", "attestation") to count. */
	published?: Record<string, number>;
	/** Skip reason key to count. */
	skipped?: Record<string, number>;
	dropped?: number;
	errors?: number;
	reannounced?: number;
}

/** How received peer data shaped local health checks. */
export interface PeernetAdviceStatus {
	/** Advice kind to count: none, reduce_sample_to_floor, may_skip_one_check, recheck_sooner. */
	given?: Record<string, number>;
	reduce_applied?: number;
	skips_applied?: number;
	recheck_scheduled?: number;
	lookup_errors?: number;
	budget_remaining?: number;
}

export interface PeernetBackfillStatus {
	running: boolean;
	scanned: number;
	indexed: number;
	skipped: number;
}

/**
 * How many library files can be shared (those with a Release ID): either one
 * from the release's NZB, or one derived from the files of an older import.
 * The rest (file_id_only) could not be grouped: no_source_nzb, group_too_large
 * or pending; not_indexable files could not be identified at all.
 */
export interface PeernetLibraryStatus {
	files: number;
	with_release_id: number;
	with_nzb_release_id: number;
	with_derived_release_id: number;
	file_id_only: number;
	not_indexable: number;
	no_source_nzb: number;
	group_too_large: number;
	pending: number;
}

/** Bundle building, serving, fetching and importing since start. Every field is optional for an older backend; map keys are machine names. */
export interface PeernetBundleImportStatus {
	accepted?: number;
	already_imported?: number;
	rejected?: Record<string, number>;
	full_check_passed?: number;
	full_check_failed?: number;
	announced?: number;
	bad_bundle_reports?: number;
	/** Files that still owe their full check. */
	pending?: number;
	/** Pending past the maximum age: failed, never announced. */
	stale?: number;
}

export interface PeernetBundlesStatus {
	built?: number;
	cache_hits?: number;
	build_failures?: number;
	cached?: number;
	cached_bytes?: number;
	announced?: number;
	not_shareable?: Record<string, number>;
	served?: number;
	served_bytes?: number;
	serve_refused?: Record<string, number>;
	fetched?: number;
	fetch_rate_limited?: number;
	fetch_failed?: Record<string, number>;
	fetch_backed_off?: number;
	full_check_reports?: number;
	full_check_backed_off?: number;
	import?: PeernetBundleImportStatus;
}

export interface PeernetStatus {
	enabled: boolean;
	node_id: string;
	listening: boolean;
	listen_port: number;
	listen_error: string;
	error: string;
	accept_incoming: boolean;
	/** Why the advertise address is not used; empty when it is fine. */
	advertise_warning: string;
	networks: PeernetNetworkStatus[];
	backfill: PeernetBackfillStatus;
	library?: PeernetLibraryStatus;
	publish?: PeernetPublishStatus;
	advice?: PeernetAdviceStatus;
	bundles?: PeernetBundlesStatus;
}

export type PeernetPeerSource = "user" | "exchange" | "inbound";

export interface PeernetPeer {
	node_id: string;
	address: string;
	source: PeernetPeerSource;
	verified: boolean;
	first_seen: string | null;
	last_success: string | null;
	failures: number;
	node_id_changed: boolean;
	unresolved: boolean;
	last_error: string;
}

export interface PeernetInvite {
	id: string;
	alias: string;
	created_at: string | null;
	expires_at: string | null;
	used_at: string | null;
	used_by: string;
}

export interface PeernetMember {
	node_id: string;
	alias: string;
	joined_at: string | null;
	cert_expires_at: string | null;
	revoked_at: string | null;
}

export interface PeernetCreateNetworkRequest {
	name: string;
}

export interface PeernetJoinNetworkRequest {
	link: string;
	allow_private_addresses: boolean;
}

export interface PeernetUpdateNetworkRequest {
	level?: PeernetLevel;
	allow_private_addresses?: boolean;
	host_address?: string;
}

export interface PeernetCreateInviteRequest {
	ttl_hours: number;
	alias: string;
}

export interface PeernetInviteLink {
	link: string;
}
