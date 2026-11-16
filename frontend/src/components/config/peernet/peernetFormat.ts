/** Formats an ISO timestamp for display, or a dash when missing or invalid. */
export function formatTimestamp(value: string | null | undefined): string {
	if (!value) return "-";
	const date = new Date(value);
	return Number.isNaN(date.getTime()) ? "-" : date.toLocaleString();
}

/** True when the timestamp parses and is in the past. */
export function isPast(value: string | null | undefined): boolean {
	if (!value) return false;
	const time = new Date(value).getTime();
	return !Number.isNaN(time) && time < Date.now();
}

export type InviteStatus = "unused" | "used" | "expired";

interface InviteTimes {
	used_at: string | null;
	expires_at: string | null;
}

/** Used wins over expired: a redeemed invite stays "used" after its lifetime. */
export function inviteStatus(invite: InviteTimes, now: number = Date.now()): InviteStatus {
	if (invite.used_at) return "used";
	if (invite.expires_at) {
		const expires = new Date(invite.expires_at).getTime();
		if (!Number.isNaN(expires) && expires <= now) return "expired";
	}
	return "unused";
}

/** Only an unused, unexpired invite is worth revoking. */
export function canRevokeInvite(invite: InviteTimes, now: number = Date.now()): boolean {
	return inviteStatus(invite, now) === "unused";
}

/** Shortens a long node ID for display; the full value belongs in a title. */
export function shortNodeId(id: string): string {
	if (id === "") return "-";
	return id.length > 16 ? `${id.slice(0, 8)}...${id.slice(-4)}` : id;
}
