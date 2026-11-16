import type { PeernetConfig, PeernetLevel } from "../../../types/peernet";

export const MAX_OPEN_PEERS = 64;
export const MIN_PORT = 1;
export const MAX_PORT = 65535;
export const MIN_LISTEN_PORT = 1024;
export const MAX_LISTEN_PORT = MAX_PORT;
export const MAX_NAME_LENGTH = 64;
export const MAX_ALIAS_LENGTH = 64;
export const MIN_INVITE_TTL_HOURS = 1;
export const MAX_INVITE_TTL_HOURS = 720;
export const DEFAULT_INVITE_TTL_HOURS = 24;
const MAX_ADDRESS_LENGTH = 255;
const MAX_HOST_LENGTH = 253;
const MAX_LABEL_LENGTH = 63;
export const DEFAULT_LISTEN_PORT = 8470;

export const DEFAULT_PEERNET_CONFIG: PeernetConfig = {
	enabled: false,
	accept_incoming: false,
	listen_port: DEFAULT_LISTEN_PORT,
	advertise_address: "",
	open_network: { level: 0, peers: [], fast_path: false },
	sharing: { excluded_categories: [], excluded_indexers: [] },
	min_corroboration: 3,
};

export interface ExposureLevelInfo {
	level: PeernetLevel;
	name: string;
	description: string;
}

export const EXPOSURE_LEVELS: readonly ExposureLevelInfo[] = [
	{ level: 0, name: "Off", description: "Nothing is received and nothing is shared." },
	{
		level: 1,
		name: "Receive only",
		description:
			"Receives release records and health information from peers and shares none of your releases. Peers can only see that this node is connected, through its node ID and IP address. It can also receive a bundle on demand, when you play or import a release a peer offers; that reveals the request to the peer asked.",
	},
	{
		level: 2,
		name: "Share metadata",
		description:
			"Also publishes a signed record and health result for each of your own healthy releases. Other nodes can see which releases you have, tied to your node ID and IP address. No article IDs leave this instance.",
	},
	{
		level: 3,
		name: "Share bundles",
		description:
			"Also serves finished imports to peers on request, as bundles. The article IDs of your releases leave this node, and peers see which releases you have, tied to your node ID and IP address. Never shared as a bundle: rclone-encrypted files, files repaired by local PAR2 patches, and password-protected releases unless the password came inside the NZB itself on an import made after this feature was enabled.",
	},
];

export const LEVEL_PRIVACY_NOTICE =
	"At this level other nodes can see which releases this instance has, tied to its node ID and IP address.";

export const LEVEL3_PRIVACY_NOTICE =
	"At level 3 this node serves finished imports to peers on request, so the article IDs of your releases leave this node (at level 2 they do not). Peers see which releases you have, tied to your node ID and IP address. Never shared as a bundle: rclone-encrypted files, files repaired by local PAR2 patches, and password-protected releases unless the password came inside the NZB itself on an import made after this feature was enabled.";

export function levelPrivacyNotice(level: number): string {
	return level >= 3 ? LEVEL3_PRIVACY_NOTICE : LEVEL_PRIVACY_NOTICE;
}

// Levels 2 and 3 publish information about this instance's own releases.
export function levelNeedsPrivacyNotice(level: number): boolean {
	return level >= 2;
}

export function isPeernetLevel(value: number): value is PeernetLevel {
	return value === 0 || value === 1 || value === 2 || value === 3;
}

const BAD_CHARS = new Set(["/", "\\", "@", "?", "#", "%", "*"]);
const HEX_GROUP_RE = /^[0-9A-Fa-f]{1,4}$/;

function isValidPort(text: string): boolean {
	if (text.length === 0 || text.length > 5 || !/^\d+$/.test(text)) return false;
	const port = Number(text);
	return port >= MIN_PORT && port <= MAX_PORT;
}

function isDecimalOctet(text: string): boolean {
	if (!/^\d{1,3}$/.test(text)) return false;
	if (text.length > 1 && text.startsWith("0")) return false;
	return Number(text) <= 255;
}

function isDottedQuad(text: string): boolean {
	const parts = text.split(".");
	return parts.length === 4 && parts.every(isDecimalOctet);
}

// Mirrors net/netip ParseAddr for IPv6 (no zones): hex groups of at most 4
// digits, at most one "::" that stands for at least one group, at most 8
// groups, optional trailing dotted IPv4 counting as two groups.
function isValidIPv6(text: string): boolean {
	let rest = text;
	let groups = 0;
	let ellipsis = false;
	if (rest.startsWith("::")) {
		ellipsis = true;
		rest = rest.slice(2);
		if (rest === "") return true;
	}
	while (groups < 8) {
		const m = /^[0-9A-Fa-f]*/.exec(rest);
		const digits = m ? m[0] : "";
		if (digits.length === 0 || digits.length > 4) {
			// An IPv4 tail may begin with decimal digits only, handled below.
			return false;
		}
		const after = rest.slice(digits.length);
		if (after.startsWith(".")) {
			if (!ellipsis && groups !== 6) return false;
			if (groups + 2 > 8) return false;
			if (!isDottedQuad(rest)) return false;
			groups += 2;
			rest = "";
			break;
		}
		if (!HEX_GROUP_RE.test(digits)) return false;
		groups += 1;
		rest = after;
		if (rest === "") break;
		if (!rest.startsWith(":")) return false;
		if (rest.length === 1) return false;
		rest = rest.slice(1);
		if (rest.startsWith(":")) {
			if (ellipsis) return false;
			ellipsis = true;
			rest = rest.slice(1);
			if (rest === "") break;
		}
	}
	if (rest !== "") return false;
	return groups < 8 ? ellipsis : !ellipsis;
}

function isValidHostname(host: string): boolean {
	if (host === "" || host.length > MAX_HOST_LENGTH) return false;
	const labels = host.split(".");
	const last = labels[labels.length - 1];
	if (last === "" || /^\d/.test(last)) return false;
	return labels.every(
		(label) =>
			label.length > 0 &&
			label.length <= MAX_LABEL_LENGTH &&
			!label.startsWith("-") &&
			!label.endsWith("-") &&
			/^[A-Za-z0-9-]+$/.test(label),
	);
}

/**
 * Validates a "host:port" address exactly like the server's
 * transport.ValidateAddress. The value is checked as given (no trimming), so
 * callers must trim at the point of input. Returns an error message or null.
 */
export function validatePeerAddress(address: string): string | null {
	if (address === "") return "Enter an address as host:port.";
	if (address.length > MAX_ADDRESS_LENGTH) {
		return `Address must be at most ${MAX_ADDRESS_LENGTH} characters.`;
	}
	for (const ch of address) {
		const code = ch.charCodeAt(0);
		if (code <= 0x20 || code >= 0x7f) {
			return "Address must not contain spaces or non-ASCII characters.";
		}
		if (BAD_CHARS.has(ch)) return "Use host:port only, without scheme, paths or credentials.";
	}

	const lastColon = address.lastIndexOf(":");
	if (lastColon < 0) return "Address must look like host:port.";
	let host: string;
	if (address.startsWith("[")) {
		const end = address.indexOf("]");
		if (end < 0 || end + 1 !== lastColon) return "Address must look like [ipv6]:port.";
		host = address.slice(1, end);
		if (host.includes("[") || address.slice(end + 1).includes("]")) {
			return "Address must look like [ipv6]:port.";
		}
	} else {
		host = address.slice(0, lastColon);
		if (host.includes(":")) return "Wrap IPv6 hosts in brackets, e.g. [::1]:8470.";
		if (address.includes("[") || address.includes("]")) return "Address must look like host:port.";
	}
	const port = address.slice(lastColon + 1);
	if (host === "" || port === "") return "Address must look like host:port.";
	if (!isValidPort(port)) return `Port must be a number from ${MIN_PORT} to ${MAX_PORT}.`;

	if (address.startsWith("[")) {
		return host.includes(":") && isValidIPv6(host) ? null : "Invalid IPv6 address.";
	}
	if (isDottedQuad(host)) return null;
	return isValidHostname(host) ? null : "Invalid host name.";
}

function splitHostPort(address: string): string {
	const lastColon = address.lastIndexOf(":");
	const host = address.slice(0, lastColon);
	return host.startsWith("[") && host.endsWith("]") ? host.slice(1, -1) : host;
}

// Expands a valid IPv6 literal to eight 16-bit groups (an IPv4 tail counts as
// two). Returns null when the text is not a valid literal.
function ipv6Groups(text: string): number[] | null {
	if (!isValidIPv6(text)) return null;
	let work = text;
	const tail = /(\d+\.\d+\.\d+\.\d+)$/.exec(work);
	if (tail) {
		const o = tail[1].split(".").map(Number);
		work = `${work.slice(0, -tail[1].length)}${((o[0] << 8) | o[1]).toString(16)}:${((o[2] << 8) | o[3]).toString(16)}`;
	}
	const [head, rest] = work.split("::");
	const left = head === "" ? [] : head.split(":");
	const right = rest === undefined || rest === "" ? [] : rest.split(":");
	const fill = rest === undefined ? 0 : 8 - left.length - right.length;
	return [...left, ...Array(fill).fill("0"), ...right].map((g) => Number.parseInt(g, 16));
}

function leadingZeros(groups: readonly number[], count: number): boolean {
	return groups.slice(0, count).every((x) => x === 0);
}

// The IPv4 octets of an address, unmapping IPv4-mapped IPv6 (as the server does).
function ipv4Octets(host: string): number[] | null {
	if (isDottedQuad(host)) return host.split(".").map(Number);
	const g = host.includes(":") ? ipv6Groups(host) : null;
	if (g !== null && leadingZeros(g, 5) && g[5] === 0xffff) {
		return [g[6] >> 8, g[6] & 255, g[7] >> 8, g[7] & 255];
	}
	return null;
}

/** True for loopback IPs, localhost and *.localhost: never reachable by a peer. */
export function isLoopbackAddress(address: string): boolean {
	if (validatePeerAddress(address) !== null) return false;
	const host = splitHostPort(address).toLowerCase();
	if (host === "localhost" || host.endsWith(".localhost")) return true;
	const v4 = ipv4Octets(host);
	if (v4) return v4[0] === 127;
	const g = host.includes(":") ? ipv6Groups(host) : null;
	return g !== null && leadingZeros(g, 7) && g[7] === 1;
}

function isPrivateHost(host: string): boolean {
	const lower = host.toLowerCase();
	if (/\.(local|internal|localdomain)$/.test(lower)) return true;
	const v4 = ipv4Octets(host);
	if (v4) {
		const [a, b] = v4;
		return (
			a === 10 ||
			(a === 172 && b >= 16 && b <= 31) ||
			(a === 192 && b === 168) ||
			(a === 100 && b >= 64 && b <= 127) ||
			(a === 169 && b === 254)
		);
	}
	const g = host.includes(":") ? ipv6Groups(host) : null;
	return g !== null && ((g[0] & 0xfe00) === 0xfc00 || (g[0] & 0xffc0) === 0xfe80);
}

/**
 * A warning for a well-formed address that is private or local: it is saved
 * but the open network never dials it. Null for anything else.
 */
export function peerAddressNotice(address: string): string | null {
	if (validatePeerAddress(address) !== null || isLoopbackAddress(address)) return null;
	return isPrivateHost(splitHostPort(address))
		? "Private or local address: the open network never connects to it. Use it only on a private network that allows private addresses."
		: null;
}

export const LOOPBACK_ERROR =
	"Loopback addresses (127.0.0.1, localhost) are never allowed. Use the machine's LAN or public address.";

/** An open-network peer: a valid address that is not loopback. */
export function validateOpenPeerAddress(address: string): string | null {
	return validatePeerAddress(address) ?? (isLoopbackAddress(address) ? LOOPBACK_ERROR : null);
}

/** A private network's host address: a valid address that is not loopback. */
export function validateHostAddress(address: string): string | null {
	return validateOpenPeerAddress(address);
}

/** The host address inside an invite link, or null when it cannot be read. */
export function linkHostAddress(link: string): string | null {
	const query = link.trim().split("?")[1];
	if (!query) return null;
	for (const part of query.split("&")) {
		if (part.startsWith("a=")) return part.slice(2);
	}
	return null;
}

/** Client-side checks of a join link before it is sent; null when fine. */
export function validateJoinLinkHost(link: string): string | null {
	const host = linkHostAddress(link);
	return host !== null && isLoopbackAddress(host)
		? "This invite points at a loopback address (127.0.0.1 or localhost), which no other machine can reach. Ask the host to use its LAN or public address."
		: null;
}

/** Validates an optional advertise address: empty is allowed. */
export function validateAdvertiseAddress(address: string): string | null {
	return address === "" ? null : validatePeerAddress(address);
}

/** Validates an invite lifetime in hours. */
export function validateInviteTtl(hours: number): string | null {
	if (!Number.isInteger(hours) || hours < MIN_INVITE_TTL_HOURS || hours > MAX_INVITE_TTL_HOURS) {
		return `Use a whole number from ${MIN_INVITE_TTL_HOURS} to ${MAX_INVITE_TTL_HOURS} hours.`;
	}
	return null;
}

export function validateListenPort(port: number): string | null {
	if (!Number.isInteger(port) || port < MIN_LISTEN_PORT || port > MAX_LISTEN_PORT) {
		return `Port must be a whole number from ${MIN_LISTEN_PORT} to ${MAX_LISTEN_PORT}.`;
	}
	return null;
}

/**
 * Checks a candidate against the current list. Returns the trimmed address
 * to add, or an error message.
 */
export function checkNewPeerAddress(
	input: string,
	existing: readonly string[],
): { address: string; error: null } | { address: null; error: string } {
	const address = input.trim();
	const error = validateOpenPeerAddress(address);
	if (error) return { address: null, error };
	if (existing.length >= MAX_OPEN_PEERS) {
		return { address: null, error: `At most ${MAX_OPEN_PEERS} peers are allowed.` };
	}
	const lower = address.toLowerCase();
	if (existing.some((p) => p.toLowerCase() === lower)) {
		return { address: null, error: "This peer is already in the list." };
	}
	return { address, error: null };
}

/** Adds a tag, trimmed; duplicates are matched case-insensitively. */
export function addTag(tags: readonly string[], input: string): string[] {
	const tag = input.trim();
	if (tag === "") return [...tags];
	const lower = tag.toLowerCase();
	if (tags.some((t) => t.toLowerCase() === lower)) return [...tags];
	return [...tags, tag];
}

/** Returns a message for the first problem that blocks saving, or null. */
export function validatePeernetConfig(config: PeernetConfig): string | null {
	const port = validateListenPort(config.listen_port);
	if (port) return port;
	const advertise = validateAdvertiseAddress(config.advertise_address);
	if (advertise) return `Advertise address: ${advertise}`;
	if (!Number.isInteger(config.min_corroboration) || config.min_corroboration < 1) {
		return "Minimum corroboration must be a whole number of at least 1.";
	}
	if (config.open_network.peers.length > MAX_OPEN_PEERS) {
		return `At most ${MAX_OPEN_PEERS} peers are allowed.`;
	}
	for (const peer of config.open_network.peers) {
		const error = validateOpenPeerAddress(peer);
		if (error) return `Peer ${peer}: ${error}`;
	}
	return null;
}

/** Fills missing fields from defaults so the form never sees undefined. */
export function normalizePeernetConfig(raw: Partial<PeernetConfig> | undefined): PeernetConfig {
	const d = DEFAULT_PEERNET_CONFIG;
	const level = raw?.open_network?.level;
	return {
		enabled: raw?.enabled ?? d.enabled,
		accept_incoming: raw?.accept_incoming ?? d.accept_incoming,
		listen_port: raw?.listen_port ?? d.listen_port,
		advertise_address: raw?.advertise_address ?? d.advertise_address,
		open_network: {
			level: level !== undefined && isPeernetLevel(level) ? level : 0,
			peers: [...(raw?.open_network?.peers ?? [])],
			fast_path: raw?.open_network?.fast_path === true,
		},
		sharing: {
			excluded_categories: [...(raw?.sharing?.excluded_categories ?? [])],
			excluded_indexers: [...(raw?.sharing?.excluded_indexers ?? [])],
		},
		min_corroboration: raw?.min_corroboration ?? d.min_corroboration,
	};
}
