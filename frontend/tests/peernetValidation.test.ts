import { describe, expect, test } from "bun:test";
import {
	canRevokeInvite,
	inviteStatus,
	shortNodeId,
} from "../src/components/config/peernet/peernetFormat";
import {
	addTag,
	checkNewPeerAddress,
	isLoopbackAddress,
	levelNeedsPrivacyNotice,
	linkHostAddress,
	MAX_INVITE_TTL_HOURS,
	MAX_OPEN_PEERS,
	normalizePeernetConfig,
	peerAddressNotice,
	validateAdvertiseAddress,
	validateHostAddress,
	validateInviteTtl,
	validateJoinLinkHost,
	validateListenPort,
	validateOpenPeerAddress,
	validatePeerAddress,
	validatePeernetConfig,
} from "../src/components/config/peernet/peernetValidation";

describe("validatePeerAddress", () => {
	test.each([
		"example.com:8470",
		"10.0.0.1:1",
		"[::1]:8470",
		"my-host1.lan:65535",
		"[2001:db8::1]:80",
		"[::]:80",
		"[::ffff:1.2.3.4]:80",
		"[1:2:3:4:5:6:7:8]:80",
		"[1::]:80",
		"a.com:00080",
		"localhost:80",
		`${"a".repeat(63)}.com:80`,
	])("accepts %s", (addr) => expect(validatePeerAddress(addr)).toBeNull());
	test.each([
		"",
		"example.com",
		"https://example.com:8470",
		"user@example.com:8470",
		"example.com:8470/path",
		"exa mple.com:8470",
		"example.com:0",
		"example.com:65536",
		"example.com:abc",
		"example.com:+80",
		"example.com:080000",
		"example.com:",
		"::1:8470",
		"-bad.com:80",
		":80",
		"my-host_1.lan:65535",
		"a_b.com:80",
		"a..b:80",
		"a.-b.com:80",
		"a-.com:80",
		`${"a".repeat(64)}.com:80`,
		"[1]:80",
		"[:]:80",
		"[1.2.3.4]:80",
		"[example.com]:80",
		"[::1:80",
		"[1:2:3:4:5:6:7:8:9]:80",
		"[1::2::3]:80",
		"[12345::]:80",
		"[::g]:80",
		"[1:2:3:4:5:6:7::8]:80",
		"[::1%eth0]:80",
		"example.1:80",
		"1.2.3:80",
		"999.1.1.1:80",
		"0x7f.1:80",
		"01.02.03.04:80",
		"example.com.:80",
		" a.com:80",
		"a.com:80 ",
		"a.com:80*",
		"a\u00e9.com:80",
		"a%b.com:80",
	])("rejects %p", (addr) => expect(validatePeerAddress(addr)).not.toBeNull());
	test("accepts the longest host that fits in 255 characters and rejects 254", () => {
		const l63 = "a".repeat(63);
		const host251 = `${l63}.${l63}.${l63}.${"a".repeat(59)}`;
		expect(host251.length).toBe(251);
		expect(validatePeerAddress(`${host251}:80`)).toBeNull();
		const host254 = `${host251}aaa`;
		expect(validatePeerAddress(`${host254}:80`)).not.toBeNull();
	});
	test("rejects an address longer than 255 characters", () => {
		expect(validatePeerAddress(`${"a".repeat(260)}:80`)).not.toBeNull();
	});
});

describe("validateInviteTtl", () => {
	test("allows 1 to the maximum", () => {
		expect(validateInviteTtl(1)).toBeNull();
		expect(validateInviteTtl(MAX_INVITE_TTL_HOURS)).toBeNull();
		expect(validateInviteTtl(0)).not.toBeNull();
		expect(validateInviteTtl(MAX_INVITE_TTL_HOURS + 1)).not.toBeNull();
		expect(validateInviteTtl(1.5)).not.toBeNull();
		expect(validateInviteTtl(Number.NaN)).not.toBeNull();
	});
});

describe("checkNewPeerAddress", () => {
	test("trims the input", () => {
		expect(checkNewPeerAddress("  a.com:1  ", [])).toEqual({ address: "a.com:1", error: null });
	});
	test("rejects duplicates case-insensitively", () => {
		expect(checkNewPeerAddress("A.com:1", ["a.com:1"]).error).not.toBeNull();
	});
	test("rejects beyond the maximum", () => {
		const full = Array.from({ length: MAX_OPEN_PEERS }, (_, i) => `h${i}.com:1`);
		expect(checkNewPeerAddress("new.com:1", full).error).not.toBeNull();
	});
});

describe("ports and advertise address", () => {
	test("listen port range", () => {
		expect(validateListenPort(1023)).not.toBeNull();
		expect(validateListenPort(1024)).toBeNull();
		expect(validateListenPort(65535)).toBeNull();
		expect(validateListenPort(65536)).not.toBeNull();
		expect(validateListenPort(Number.NaN)).not.toBeNull();
	});
	test("advertise address may be empty", () => {
		expect(validateAdvertiseAddress("")).toBeNull();
		expect(validateAdvertiseAddress("  ")).not.toBeNull();
		expect(validateAdvertiseAddress(" a.com:80")).not.toBeNull();
		expect(validateAdvertiseAddress("bad")).not.toBeNull();
	});
});

describe("levelNeedsPrivacyNotice", () => {
	test("only levels 2 and 3 show the notice", () => {
		expect([0, 1, 2, 3].map(levelNeedsPrivacyNotice)).toEqual([false, false, true, true]);
	});
});

describe("addTag", () => {
	test("trims, ignores empty and duplicate tags", () => {
		expect(addTag(["Movies"], "  tv ")).toEqual(["Movies", "tv"]);
		expect(addTag(["Movies"], "movies")).toEqual(["Movies"]);
		expect(addTag(["Movies"], "   ")).toEqual(["Movies"]);
	});
});

describe("normalizePeernetConfig / validatePeernetConfig", () => {
	test("fills defaults and is valid", () => {
		const cfg = normalizePeernetConfig(undefined);
		expect(cfg.enabled).toBe(false);
		expect(cfg.listen_port).toBe(8470);
		expect(validatePeernetConfig(cfg)).toBeNull();
	});
	test("flags a bad corroboration value", () => {
		const cfg = { ...normalizePeernetConfig(undefined), min_corroboration: 0 };
		expect(validatePeernetConfig(cfg)).not.toBeNull();
	});
});

describe("loopback and private peer addresses", () => {
	test.each([
		"127.0.0.1:80",
		"127.5.6.7:80",
		"[::1]:80",
		"[0:0:0:0:0:0:0:1]:80",
		"[::ffff:127.0.0.1]:80",
		"localhost:80",
		"LocalHost:80",
		"a.localhost:80",
	])("%s is loopback", (addr) => expect(isLoopbackAddress(addr)).toBe(true));
	test.each([
		"192.168.1.2:80",
		"example.com:80",
		"[::2]:80",
		"128.0.0.1:80",
		"nas.local:80",
		"localhostx:80",
	])("%s is not loopback", (addr) => expect(isLoopbackAddress(addr)).toBe(false));
	test("an open-network peer may not be loopback", () => {
		expect(validateOpenPeerAddress("127.0.0.1:8470")).toContain("oopback");
		expect(validateOpenPeerAddress("localhost:8470")).not.toBeNull();
		expect(validateOpenPeerAddress("example.com:8470")).toBeNull();
		expect(validateOpenPeerAddress("192.168.1.2:8470")).toBeNull(); // only a warning
		expect(validateOpenPeerAddress("bad")).not.toBeNull();
	});
	test("checkNewPeerAddress and validatePeernetConfig refuse loopback peers", () => {
		expect(checkNewPeerAddress("127.0.0.1:1", []).error).not.toBeNull();
		const cfg = normalizePeernetConfig({ open_network: { level: 1, peers: ["localhost:9"] } });
		expect(validatePeernetConfig(cfg)).not.toBeNull();
	});
	test.each([
		"10.1.2.3:80",
		"172.16.0.1:80",
		"172.31.255.1:80",
		"192.168.0.9:80",
		"100.64.0.1:80",
		"169.254.1.1:80",
		"[fd00::1]:80",
		"[fe80::1]:80",
		"nas.local:80",
		"box.internal:80",
	])("%s gets a private-address warning", (addr) =>
		expect(peerAddressNotice(addr)?.toLowerCase()).toContain("private"),
	);
	test.each(["8.8.8.8:80", "172.32.0.1:80", "example.com:80", "[2001:4860::1]:80"])(
		"%s gets no warning",
		(addr) => expect(peerAddressNotice(addr)).toBeNull(),
	);
});

describe("join link and host address", () => {
	const link = (a: string) =>
		`altmountnet://join?v=1&n=${"a".repeat(64)}&k=KEY&a=${a}&h=${"b".repeat(64)}&c=CODE`;
	test("reads the host address from a link", () => {
		expect(linkHostAddress(link("192.168.1.5:8470"))).toBe("192.168.1.5:8470");
		expect(linkHostAddress("nonsense")).toBeNull();
	});
	test("a loopback host in a link is refused, others pass", () => {
		expect(validateJoinLinkHost(link("127.0.0.1:8470"))).toContain("loopback");
		expect(validateJoinLinkHost(link("localhost:8470"))).not.toBeNull();
		expect(validateJoinLinkHost(link("192.168.1.5:8470"))).toBeNull();
		expect(validateJoinLinkHost("garbage")).toBeNull(); // the server judges the format
	});
	test("a private network host address may not be loopback", () => {
		expect(validateHostAddress("127.0.0.1:8470")).toContain("oopback");
		expect(validateHostAddress("192.168.1.5:8470")).toBeNull();
		expect(validateHostAddress("bad")).not.toBeNull();
	});
});

describe("invite status", () => {
	const now = Date.parse("2026-01-02T00:00:00Z");
	const base = {
		id: "x",
		alias: "",
		created_at: "2026-01-01T00:00:00Z",
		expires_at: "2026-01-03T00:00:00Z",
		used_at: null,
		used_by: "",
	};
	test("unused, used and expired", () => {
		expect(inviteStatus(base, now)).toBe("unused");
		expect(inviteStatus({ ...base, used_at: "2026-01-01T12:00:00Z", used_by: "n" }, now)).toBe(
			"used",
		);
		expect(inviteStatus({ ...base, expires_at: "2026-01-01T06:00:00Z" }, now)).toBe("expired");
	});
	test("used wins over expired", () => {
		const both = { ...base, expires_at: "2026-01-01T06:00:00Z", used_at: "2026-01-01T01:00:00Z" };
		expect(inviteStatus(both, now)).toBe("used");
	});
	test("only unused, unexpired invites can be revoked", () => {
		expect(canRevokeInvite(base, now)).toBe(true);
		expect(canRevokeInvite({ ...base, used_at: "2026-01-01T12:00:00Z" }, now)).toBe(false);
		expect(canRevokeInvite({ ...base, expires_at: "2026-01-01T06:00:00Z" }, now)).toBe(false);
	});
	test("node IDs are shortened but short text is kept", () => {
		const id = `${"ab".repeat(32)}`;
		expect(shortNodeId(id)).toBe("abababab...abab");
		expect(shortNodeId("short")).toBe("short");
		expect(shortNodeId("")).toBe("-");
	});
});
