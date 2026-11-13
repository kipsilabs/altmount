import type { PeernetLevel, PeernetLibraryStatus } from "../../../types/peernet";

// Plain-language labels for the machine keys the status endpoint reports. The
// lookups use Maps so a key such as "__proto__" can never reach a prototype;
// unknown keys fall back to their own text, which is rendered as React text.

const REJECTION_LABELS: ReadonlyMap<string, string> = new Map([
	["oversize", "Too large"],
	["malformed", "Unreadable"],
	["unsupported_version", "Newer format than this version understands"],
	["invalid", "Failed validation"],
	["wrong_network", "Meant for another network"],
	["from_future", "Dated in the future"],
	["expired", "Expired"],
	["stale", "Too old"],
	["bad_signature", "Invalid signature"],
	["self_publisher", "Echo of this node's own record"],
	["not_own_key", "Not signed by the sending peer"],
	["quota_publisher", "Publisher over its limit"],
	["quota_storage", "Local storage limit reached"],
]);

const SKIP_LABELS: ReadonlyMap<string, string> = new Map([
	["level_below_share_metadata", "Network level does not share"],
	["excluded_category", "Category is excluded"],
	["excluded_indexer", "Indexer is excluded"],
	["unknown_indexer", "Indexer unknown while indexers are excluded"],
	["unknown_category", "Category unknown while categories are excluded"],
	["title_contains_password", "Title contained a password"],
	["peer_bundle_not_fully_checked", "Came from a peer and not fully checked yet"],
	["inconclusive_result", "Check was inconclusive"],
	["unhealthy_not_shared", "Not healthy"],
	["patched_by_par2", "Repaired locally"],
	["no_recognised_backbone", "No recognised provider backbone"],
	["check_too_old", "Check is too old"],
	["not_due", "Not due yet"],
	["not_publishing", "Not publishing to any network"],
	["known_holes", "File has known missing parts"],
	["no_segments", "No checked segments to report"],
	["no_release_id", "Release could not be identified"],
	["no_release_info", "Release details were not available"],
	["not_healthy", "Not healthy locally"],
	["reduced_by_peer_advice", "Check was shortened by network evidence"],
	["patch_status_unknown", "Repair status of the file was not known"],
]);

export function rejectionLabel(key: string): string {
	return REJECTION_LABELS.get(key) ?? key;
}

export function skipLabel(key: string): string {
	return SKIP_LABELS.get(key) ?? key;
}

export interface BreakdownEntry {
	key: string;
	label: string;
	count: number;
}

function validCount(value: unknown): value is number {
	return typeof value === "number" && Number.isFinite(value) && value > 0;
}

/** Non-zero entries of a reason->count map, largest first, with readable labels. */
export function describeBreakdown(
	counts: Readonly<Record<string, number>> | null | undefined,
	label: (key: string) => string,
): BreakdownEntry[] {
	if (!counts || typeof counts !== "object") return [];
	return Object.entries(counts)
		.filter(([, count]) => validCount(count))
		.map(([key, count]) => ({ key, label: label(key), count }))
		.sort((a, b) => b.count - a.count || (a.key < b.key ? -1 : 1));
}

/** Sum of the valid counts in a map. */
export function publishedTotal(
	counts: Readonly<Record<string, number>> | null | undefined,
): number {
	return describeBreakdown(counts, (k) => k).reduce((sum, e) => sum + e.count, 0);
}

function plural(n: number, unit: string): string {
	return `${n} ${unit}${n === 1 ? "" : "s"} ago`;
}

/** "Never" for missing or invalid input; a future time reads as "just now". */
export function formatRelativeTime(
	value: string | null | undefined,
	now: number = Date.now(),
): string {
	if (!value) return "Never";
	const time = new Date(value).getTime();
	if (Number.isNaN(time)) return "Never";
	const seconds = Math.floor((now - time) / 1000);
	if (seconds < 45) return "just now";
	const minutes = Math.floor(seconds / 60);
	if (minutes < 60) return plural(Math.max(minutes, 1), "minute");
	const hours = Math.floor(minutes / 60);
	if (hours < 24) return plural(hours, "hour");
	return plural(Math.floor(hours / 24), "day");
}

interface HasLevel {
	level: number;
}

/** Received-data statistics only mean something once a network receives. */
export function showSharedData(level: number): boolean {
	return level >= 1;
}

/** Level 2 and 3 publish records. */
export function hasAnyPublishingLevel(networks: readonly HasLevel[]): boolean {
	return networks.some((n) => n.level >= 2);
}

export function showPublishingDetails(networks: readonly HasLevel[]): boolean {
	return hasAnyPublishingLevel(networks);
}

/** Health-check advice comes from received data, so it needs a receiving network. */
export function showAdviceBlock(networks: readonly HasLevel[]): boolean {
	return networks.some((n) => n.level >= 1);
}

/** Level 3 (bundles) is not implemented; it behaves like level 2 for records. */
export function isLevelAvailable(level: PeernetLevel): boolean {
	return level !== 3;
}

/**
 * One plain-language line on how much of the library can be shared, or null
 * when there is nothing to say (no files known yet, or all can be shared).
 */
export function libraryShareLine(library: PeernetLibraryStatus | undefined): string | null {
	if (!library) return null;
	const { files, with_release_id: shareable } = library;
	if (![files, shareable].every((n) => Number.isFinite(n) && n >= 0) || files <= 0) return null;
	if (shareable >= files) return null;
	return `${shareable} of ${files} library files can be shared; the rest were imported before sharing data was kept.`;
}

/** The skip key that means no provider backbone could be recognised. */
export const NO_BACKBONE_SKIP_KEY = "no_recognised_backbone";

/**
 * A hint naming the setting that fixes missing attestations, or null while no
 * check was skipped for lack of a recognised backbone.
 */
export function backboneHint(
	skipped: Readonly<Record<string, number>> | null | undefined,
): string | null {
	const n = skipped?.[NO_BACKBONE_SKIP_KEY];
	if (typeof n !== "number" || !Number.isFinite(n) || n <= 0) return null;
	return "Health records are not shared because no provider backbone is recognised. Set a backbone (storage group) on your providers, or use a provider host that is in the known list.";
}
