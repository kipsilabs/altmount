// Keys the peer network keeps in a file's error details. They are bookkeeping
// for scheduling, not something to show as a technical error.
const PEER_MARKER_KEYS = ["peer_deferred_at", "peer_reduced_at"] as const;

/**
 * The technical details to show for a file: the raw error details without the
 * peer network's markers, or null when nothing else is left. Details without
 * those markers come back untouched.
 */
export function technicalDetails(raw: string | null | undefined): string | null {
	if (!raw) return null;
	let parsed: unknown;
	try {
		parsed = JSON.parse(raw);
	} catch {
		return raw;
	}
	if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) return raw;
	const record = parsed as Record<string, unknown>;
	if (!PEER_MARKER_KEYS.some((key) => key in record)) return raw;
	const rest = Object.fromEntries(
		Object.entries(record).filter(
			([key]) => !(PEER_MARKER_KEYS as readonly string[]).includes(key),
		),
	);
	return Object.keys(rest).length === 0 ? null : JSON.stringify(rest);
}
