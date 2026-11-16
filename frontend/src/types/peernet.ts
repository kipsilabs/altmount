// Peer network types. These mirror the backend contract for the `peernet`
// config section and the /peernet/* endpoints. Timestamps are ISO strings or
// null. Every string coming from status, peers, members and invites is
// untrusted text from other machines and must be rendered as plain text only.

export type PeernetLevel = 0 | 1 | 2 | 3;

export interface PeernetOpenNetworkConfig {
	level: PeernetLevel;
	peers: string[];
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

/** How many library files can be shared (those with a Release ID). */
export interface PeernetLibraryStatus {
	files: number;
	with_release_id: number;
	file_id_only: number;
	not_indexable: number;
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
