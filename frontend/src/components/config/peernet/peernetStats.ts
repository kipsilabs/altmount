import type { PeernetLibraryStatus } from "../../../types/peernet";

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
	["peer_status_unknown", "Could not tell whether the file came from a peer bundle"],
	["bundle_unavailable", "Bundle could not be built for this file"],
]);

const NOT_SHAREABLE_LABELS: ReadonlyMap<string, string> = new Map([
	["rclone_encrypted", "Encrypted with rclone"],
	["unsupported_encryption", "Uses an encryption type that cannot be shared"],
	["password_not_from_nzb", "Password did not come inside the NZB"],
	["healed_by_local_patch", "Repaired with local PAR2 patches"],
	["known_holes", "Has known missing parts"],
	["patch_status_unknown", "Repair status was not known"],
	["no_nzb_segment_list", "No stored NZB segment list"],
	["release_id_mismatch", "Release identity did not match"],
	["invalid_metadata", "Stored file details are inconsistent"],
	["too_large", "Too large to bundle"],
	["no_post_date", "No post date known"],
	["no_files", "No files in the release"],
	["self_check_failed", "The built bundle failed its own check"],
	["other", "Other reasons"],
]);

const SERVE_REFUSED_LABELS: ReadonlyMap<string, string> = new Map([
	["unauthenticated", "Caller not authenticated"],
	["level", "Network level does not serve bundles"],
	["denied", "Caller not allowed"],
	["backed_off", "Caller temporarily blocked"],
	["rate_limited", "Caller over its request rate"],
	["busy", "Too many requests at once"],
	["error", "Internal error"],
	["unknown_bundle", "Bundle not known"],
	["excluded", "Release is excluded from sharing"],
	["unavailable", "Bundle could not be built"],
	["byte_budget", "Node-wide data limit reached"],
	["aborted", "Transfer was cut off"],
]);

const FETCH_FAILED_LABELS: ReadonlyMap<string, string> = new Map([
	["refused", "The peer declined to serve this bundle"],
	["bad_status", "Peer answered with an error"],
	["transport", "Connection problem"],
	["truncated", "Download was cut short"],
	["too_large", "Bundle larger than allowed"],
	["verification_failed", "Bundle did not match its signed record"],
	["scheme_mismatch", "Bundle format did not match"],
	["membership", "Peer not a verified member"],
]);

const IMPORT_REJECT_LABELS: ReadonlyMap<string, string> = new Map([
	["invalid", "Failed validation"],
	["bad_name", "Unsafe file name"],
	["too_large", "Over the local size limit"],
	["too_many_files", "Over the local file limit"],
	["health_disabled", "Health checks are turned off"],
	["bad_target", "Target folder unusable"],
	["nzb_mismatch", "Did not match your NZB"],
	["collision", "A different file is already there"],
	["probe_missing", "Articles missing on your providers"],
	["probe_inconclusive", "Article check could not finish"],
	["no_media_file", "No media file to verify"],
	["media_invalid", "Start of the media file was not valid"],
	["media_missing", "Start of the media file is missing"],
	["media_inconclusive", "Media check could not finish"],
	["staging_failed", "Could not prepare files"],
	["write_failed", "Could not write files"],
	["unavailable", "No bundle could be fetched"],
	["no_network", "Peer network not available"],
	["forbidden_file", "Contains a file type that is never accepted"],
	["sidecar_too_large", "A subtitle or info file was too large"],
	["size_implausible", "Declared file sizes do not fit your NZB"],
	["yenc_mismatch", "Sampled articles contradict the bundle"],
	["structure_invalid", "Media file structure is broken"],
	[
		"layout_not_allowed_open",
		"Archive or encrypted layouts are not accepted from the open network",
	],
	[
		"container_not_verifiable",
		"Media container cannot be checked, so the open network does not carry it",
	],
	["too_many_tables", "More archive volumes than the verification budget covers"],
]);

export function notShareableLabel(key: string): string {
	return NOT_SHAREABLE_LABELS.get(key) ?? key;
}
export function serveRefusedLabel(key: string): string {
	return SERVE_REFUSED_LABELS.get(key) ?? key;
}
export function fetchFailLabel(key: string): string {
	return FETCH_FAILED_LABELS.get(key) ?? key;
}
export function importRejectLabel(key: string): string {
	return IMPORT_REJECT_LABELS.get(key) ?? key;
}

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

/** The bundles block needs a network that receives. */
export function showBundlesBlock(networks: readonly HasLevel[]): boolean {
	return networks.some((n) => n.level >= 1);
}

/**
 * Level 3 serves bundles. The serving numbers also show whenever any of them is
 * non-zero (announced, built, served, refused), so they never stay hidden while
 * the API reports them, whatever level a network is shown at.
 */
export function showBundleServing(networks: readonly HasLevel[], bundles?: unknown): boolean {
	if (networks.some((n) => n.level >= 3)) return true;
	if (!bundles || typeof bundles !== "object") return false;
	const b = bundles as Record<string, unknown>;
	return ["announced", "built", "served", "served_bytes", "not_shareable", "serve_refused"].some(
		(key) => anyPositive(b[key]),
	);
}

function positive(n: unknown): boolean {
	return typeof n === "number" && Number.isFinite(n) && n > 0;
}

function anyPositive(value: unknown): boolean {
	if (positive(value)) return true;
	if (value && typeof value === "object") return Object.values(value).some(anyPositive);
	return false;
}

/** True when any counter in the bundles object is non-zero. */
export function bundleHasActivity(bundles: unknown): boolean {
	return anyPositive(bundles);
}

/** Raising a network to level 3 (bundle serving) needs an explicit confirmation. */
export function levelNeedsConfirmation(from: number, to: number): boolean {
	return to === 3 && from !== 3;
}

export const PEER_BADGE_LABEL = "Peer network";
export const PEER_BADGE_VERIFYING_LABEL = "Peer network \u00b7 verifying";
export const PEER_BADGE_VERIFYING_TITLE =
	"Imported from a peer bundle. It is playable now; a full check of every segment is running. It is announced to your ARRs and shared only after that check passes.";
const PEER_BADGE_TITLE = "Imported from a peer bundle.";

export interface PeerBadgeInfo {
	label: string;
	title: string;
}

/** Badge text for an item imported from a peer bundle, or null for any other item. */
export function peerBadge(
	origin: boolean | undefined,
	fullCheckPending: boolean | undefined,
): PeerBadgeInfo | null {
	if (origin !== true) return null;
	return fullCheckPending === true
		? { label: PEER_BADGE_VERIFYING_LABEL, title: PEER_BADGE_VERIFYING_TITLE }
		: { label: PEER_BADGE_LABEL, title: PEER_BADGE_TITLE };
}

function count(n: unknown): number {
	return typeof n === "number" && Number.isFinite(n) && n > 0 ? n : 0;
}

function countedPhrase(n: number, singular: string, plural: string): string | null {
	return n > 0 ? `${n} ${n === 1 ? singular : plural}` : null;
}

/**
 * One plain-language line on how much of the library can be shared, or null
 * when there is nothing to say (no files known yet, or all can be shared).
 * Older imports (no stored NZB) are identified by the files they contain, so
 * they count as shareable; the line names why the remaining files are not.
 */
export function libraryShareLine(library: PeernetLibraryStatus | undefined): string | null {
	if (!library) return null;
	const { files, with_release_id: shareable } = library;
	if (![files, shareable].every((n) => Number.isFinite(n) && n >= 0) || files <= 0) return null;
	if (shareable >= files) return null;

	const older = count(library.with_derived_release_id);
	const shared =
		older > 0
			? `${shareable} of ${files} library files can be shared (${older} of them ${older === 1 ? "is an older import" : "are older imports"}, identified by the files they contain).`
			: `${shareable} of ${files} library files can be shared.`;

	const reasons = [
		countedPhrase(
			count(library.no_source_nzb),
			"has no record of the import it came from",
			"have no record of the import they came from",
		),
		countedPhrase(
			count(library.group_too_large),
			"is in an import with too many files",
			"are in imports with too many files",
		),
		countedPhrase(
			count(library.pending),
			"is still being worked out",
			"are still being worked out",
		),
		countedPhrase(count(library.not_indexable), "could not be read", "could not be read"),
	].filter((r): r is string => r !== null);
	const rest = files - shareable;
	return reasons.length > 0
		? `${shared} The other ${rest} cannot be: ${reasons.join(", ")}.`
		: `${shared} The other ${rest} cannot be.`;
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
