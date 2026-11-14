import { describe, expect, test } from "bun:test";
import {
	backboneHint,
	bundleHasActivity,
	describeBreakdown,
	fetchFailLabel,
	formatRelativeTime,
	hasAnyPublishingLevel,
	importRejectLabel,
	levelNeedsConfirmation,
	libraryShareLine,
	notShareableLabel,
	peerBadge,
	publishedTotal,
	rejectionLabel,
	serveRefusedLabel,
	showAdviceBlock,
	showBundleServing,
	showBundlesBlock,
	showPublishingDetails,
	showSharedData,
	skipLabel,
} from "../src/components/config/peernet/peernetStats";
import {
	EXPOSURE_LEVELS,
	levelPrivacyNotice,
} from "../src/components/config/peernet/peernetValidation";
import type { PeernetLibraryStatus } from "../src/types/peernet";

// Every key the backend can emit (internal/peernet/recsync/reasons.go,
// internal/peernet/publish and internal/peernet/service/publishing.go).
const BACKEND_REJECTION_KEYS = [
	"oversize",
	"malformed",
	"unsupported_version",
	"invalid",
	"wrong_network",
	"from_future",
	"expired",
	"stale",
	"bad_signature",
	"self_publisher",
	"not_own_key",
	"quota_publisher",
	"quota_storage",
];
const BACKEND_SKIP_KEYS = [
	"level_below_share_metadata",
	"excluded_category",
	"excluded_indexer",
	"unknown_indexer",
	"unknown_category",
	"peer_bundle_not_fully_checked",
	"inconclusive_result",
	"unhealthy_not_shared",
	"patched_by_par2",
	"no_recognised_backbone",
	"check_too_old",
	"not_due",
	"title_contains_password",
	"not_publishing",
	"known_holes",
	"no_segments",
	"no_release_id",
	"no_release_info",
	"not_healthy",
	"reduced_by_peer_advice",
	"patch_status_unknown",
	"peer_status_unknown",
	"bundle_unavailable",
];

// internal/peernet/bundle/build.go (ShareReason) and bundlex/builder.go.
const BACKEND_NOT_SHAREABLE_KEYS = [
	"rclone_encrypted",
	"unsupported_encryption",
	"password_not_from_nzb",
	"healed_by_local_patch",
	"known_holes",
	"patch_status_unknown",
	"no_nzb_segment_list",
	"release_id_mismatch",
	"invalid_metadata",
	"too_large",
	"no_post_date",
	"no_files",
	"self_check_failed",
	"other",
];
// internal/peernet/bundlex/serve.go
const BACKEND_SERVE_REFUSED_KEYS = [
	"unauthenticated",
	"level",
	"denied",
	"backed_off",
	"rate_limited",
	"busy",
	"error",
	"unknown_bundle",
	"excluded",
	"unavailable",
	"byte_budget",
	"aborted",
];
// internal/peernet/bundlex/fetch.go
const BACKEND_FETCH_FAILED_KEYS = [
	"refused",
	"bad_status",
	"transport",
	"truncated",
	"too_large",
	"verification_failed",
	"scheme_mismatch",
	"membership",
];
// internal/peernet/bundleimport (Reason)
const BACKEND_IMPORT_REJECT_KEYS = [
	"invalid",
	"bad_name",
	"too_large",
	"too_many_files",
	"health_disabled",
	"bad_target",
	"nzb_mismatch",
	"collision",
	"probe_missing",
	"probe_inconclusive",
	"no_media_file",
	"media_invalid",
	"media_missing",
	"media_inconclusive",
	"staging_failed",
	"write_failed",
	"unavailable",
	"no_network",
	"forbidden_file",
	"sidecar_too_large",
	"size_implausible",
	"yenc_mismatch",
	"structure_invalid",
];

describe("bundle reason coverage", () => {
	test("every backend key has a label of its own", () => {
		const groups: [readonly string[], (k: string) => string][] = [
			[BACKEND_NOT_SHAREABLE_KEYS, notShareableLabel],
			[BACKEND_SERVE_REFUSED_KEYS, serveRefusedLabel],
			[BACKEND_FETCH_FAILED_KEYS, fetchFailLabel],
			[BACKEND_IMPORT_REJECT_KEYS, importRejectLabel],
		];
		for (const [keys, label] of groups) {
			for (const key of keys) expect(label(key)).not.toBe(key);
		}
	});
	test("unknown and hostile keys are shown as text without touching prototypes", () => {
		expect(notShareableLabel("brand_new")).toBe("brand_new");
		for (const k of ["__proto__", "constructor", "toString"]) {
			expect(serveRefusedLabel(k)).toBe(k);
			expect(importRejectLabel(k)).toBe(k);
		}
	});
});

describe("bundle block visibility", () => {
	test("block needs a receiving network; serving needs level 3", () => {
		expect(showBundlesBlock([{ level: 0 }])).toBe(false);
		expect(showBundlesBlock([{ level: 1 }])).toBe(true);
		expect(showBundleServing([{ level: 2 }])).toBe(false);
		expect(showBundleServing([{ level: 1 }, { level: 3 }])).toBe(true);
	});
	test("activity detection ignores junk", () => {
		expect(bundleHasActivity(undefined)).toBe(false);
		expect(bundleHasActivity({ fetched: Number.NaN })).toBe(false);
		expect(bundleHasActivity({ served: 1 })).toBe(true);
		expect(bundleHasActivity({ import: { rejected: { invalid: 2 } } })).toBe(true);
	});
});

describe("level confirmation", () => {
	test("only raising to level 3 needs confirmation", () => {
		expect(levelNeedsConfirmation(2, 3)).toBe(true);
		expect(levelNeedsConfirmation(0, 3)).toBe(true);
		expect(levelNeedsConfirmation(3, 3)).toBe(false);
		expect(levelNeedsConfirmation(3, 2)).toBe(false);
		expect(levelNeedsConfirmation(0, 2)).toBe(false);
	});
});

describe("peer badge", () => {
	test("no badge without the flag", () => {
		expect(peerBadge(false, false)).toBeNull();
		expect(peerBadge(undefined, true)).toBeNull();
		expect(peerBadge(undefined, undefined)).toBeNull();
	});
	test("plain and verifying badges", () => {
		expect(peerBadge(true, false)?.label).toBe("Peer network");
		const v = peerBadge(true, true);
		expect(v?.label).toBe("Peer network \u00b7 verifying");
		expect(v?.title).toContain("announced to your ARRs and shared only after that check passes");
	});
});

describe("backend reason coverage", () => {
	test("every rejection reason has a label of its own", () => {
		for (const key of BACKEND_REJECTION_KEYS) {
			expect(rejectionLabel(key)).not.toBe(key);
		}
	});
	test("every skip reason has a label of its own", () => {
		for (const key of BACKEND_SKIP_KEYS) {
			expect(skipLabel(key)).not.toBe(key);
		}
	});
	test("the password and peer advice skips read plainly", () => {
		expect(skipLabel("title_contains_password")).toBe("Title contained a password");
		expect(skipLabel("reduced_by_peer_advice")).toBe("Check was shortened by network evidence");
	});
});

describe("rejectionLabel / skipLabel", () => {
	test("known keys map to plain language", () => {
		expect(rejectionLabel("bad_signature")).toBe("Invalid signature");
		expect(skipLabel("excluded_indexer")).toBe("Indexer is excluded");
	});
	test("unknown keys are returned as-is", () => {
		expect(rejectionLabel("something_new")).toBe("something_new");
		expect(skipLabel("<b>x</b>")).toBe("<b>x</b>");
	});
	test("hostile keys never hit the prototype", () => {
		for (const k of ["__proto__", "constructor", "toString", "hasOwnProperty"]) {
			expect(rejectionLabel(k)).toBe(k);
			expect(skipLabel(k)).toBe(k);
		}
	});
});

describe("describeBreakdown", () => {
	test("empty or absent gives nothing", () => {
		expect(describeBreakdown(undefined, rejectionLabel)).toEqual([]);
		expect(describeBreakdown({}, rejectionLabel)).toEqual([]);
	});
	test("drops zero, negative and non-finite counts and sorts by count", () => {
		const out = describeBreakdown(
			{ stale: 1, expired: 5, malformed: 0, invalid: -2, oversize: Number.NaN },
			rejectionLabel,
		);
		expect(out.map((e) => e.key)).toEqual(["expired", "stale"]);
		expect(out[0]?.count).toBe(5);
	});
	test("handles an own __proto__ key from JSON", () => {
		const parsed = JSON.parse('{"__proto__": 3, "stale": 1}') as Record<string, number>;
		const out = describeBreakdown(parsed, rejectionLabel);
		expect(out.map((e) => e.label)).toEqual(["__proto__", "Too old"]);
		expect(({} as Record<string, unknown>).polluted).toBeUndefined();
	});
});

describe("formatRelativeTime", () => {
	const now = Date.parse("2026-11-13T12:00:00Z");
	test("null, empty and invalid are Never", () => {
		expect(formatRelativeTime(null, now)).toBe("Never");
		expect(formatRelativeTime(undefined, now)).toBe("Never");
		expect(formatRelativeTime("", now)).toBe("Never");
		expect(formatRelativeTime("garbage", now)).toBe("Never");
	});
	test("buckets", () => {
		expect(formatRelativeTime("2026-11-13T11:59:40Z", now)).toBe("just now");
		expect(formatRelativeTime("2026-11-13T11:55:00Z", now)).toBe("5 minutes ago");
		expect(formatRelativeTime("2026-11-13T11:59:00Z", now)).toBe("1 minute ago");
		expect(formatRelativeTime("2026-11-13T10:00:00Z", now)).toBe("2 hours ago");
		expect(formatRelativeTime("2026-11-12T11:00:00Z", now)).toBe("1 day ago");
		expect(formatRelativeTime("2026-11-03T12:00:00Z", now)).toBe("10 days ago");
	});
	test("a future time (clock skew) reads as just now", () => {
		expect(formatRelativeTime("2026-11-13T12:05:00Z", now)).toBe("just now");
	});
});

describe("visibility rules", () => {
	test("shared data shows from level 1", () => {
		expect([0, 1, 2, 3].map(showSharedData)).toEqual([false, true, true, true]);
	});
	test("publishing details need a network at level 2 or higher", () => {
		expect(hasAnyPublishingLevel([])).toBe(false);
		expect(hasAnyPublishingLevel([{ level: 0 }, { level: 1 }])).toBe(false);
		expect(hasAnyPublishingLevel([{ level: 1 }, { level: 2 }])).toBe(true);
		expect(showPublishingDetails([{ level: 3 }])).toBe(true);
		expect(showPublishingDetails([{ level: 1 }])).toBe(false);
	});
	test("advice block shows only when a network receives", () => {
		expect(showAdviceBlock([{ level: 0 }])).toBe(false);
		expect(showAdviceBlock([{ level: 1 }])).toBe(true);
	});
});

describe("publishedTotal", () => {
	test("sums valid counts only", () => {
		expect(publishedTotal(undefined)).toBe(0);
		expect(publishedTotal({ release: 2, attestation: 3, junk: Number.NaN })).toBe(5);
	});
});

describe("level honesty", () => {
	test("level 3 is described plainly with the privacy consequence", () => {
		const d = EXPOSURE_LEVELS[3]?.description ?? "";
		expect(d).not.toContain("Not available");
		expect(d).not.toContain("behaves exactly like level 2");
		expect(d).toContain("article IDs");
		expect(d).toContain("node ID and IP address");
	});
	test("level 3 notice states that article IDs leave the node", () => {
		expect(levelPrivacyNotice(2)).toContain("node ID and IP address");
		expect(levelPrivacyNotice(2)).not.toContain("article IDs leave");
		expect(levelPrivacyNotice(3)).toContain("article IDs");
		expect(levelPrivacyNotice(3)).toContain("rclone");
	});
	test("level 1 states what it reveals and that bundles can be requested", () => {
		expect(EXPOSURE_LEVELS[1]?.description).toContain("node ID and IP address");
		expect(EXPOSURE_LEVELS[1]?.description).toContain("bundle");
	});
});

describe("library sharing line", () => {
	const lib = (over: Partial<PeernetLibraryStatus>): PeernetLibraryStatus => ({
		files: 0,
		with_release_id: 0,
		with_nzb_release_id: 0,
		with_derived_release_id: 0,
		file_id_only: 0,
		not_indexable: 0,
		no_source_nzb: 0,
		group_too_large: 0,
		pending: 0,
		...over,
	});

	test("says how many files can be shared and why the rest cannot", () => {
		expect(
			libraryShareLine(
				lib({
					files: 2358,
					with_release_id: 2000,
					with_nzb_release_id: 41,
					with_derived_release_id: 1959,
					file_id_only: 358,
					no_source_nzb: 300,
					group_too_large: 50,
					pending: 8,
				}),
			),
		).toBe(
			"2000 of 2358 library files can be shared (1959 of them are older imports, identified by the files they contain). " +
				"The other 358 cannot be: 300 have no record of the import they came from, 50 are in imports with too many files, 8 are still being worked out.",
		);
	});
	test("explains older imports without listing reasons when only those are shared", () => {
		expect(
			libraryShareLine(
				lib({
					files: 12,
					with_release_id: 10,
					with_nzb_release_id: 4,
					with_derived_release_id: 6,
					file_id_only: 1,
					no_source_nzb: 1,
					not_indexable: 1,
				}),
			),
		).toBe(
			"10 of 12 library files can be shared (6 of them are older imports, identified by the files they contain). " +
				"The other 2 cannot be: 1 has no record of the import it came from, 1 could not be read.",
		);
	});
	test("keeps singular wording", () => {
		expect(
			libraryShareLine(
				lib({
					files: 2,
					with_release_id: 1,
					with_nzb_release_id: 1,
					file_id_only: 1,
					group_too_large: 1,
				}),
			),
		).toBe(
			"1 of 2 library files can be shared. The other 1 cannot be: 1 is in an import with too many files.",
		);
	});
	test("still reports a shortfall whose reason the server did not give", () => {
		expect(
			libraryShareLine(
				lib({ files: 5, with_release_id: 4, with_nzb_release_id: 4, file_id_only: 1 }),
			),
		).toBe("4 of 5 library files can be shared. The other 1 cannot be.");
	});
	test("is silent when everything is shareable or nothing is known", () => {
		expect(
			libraryShareLine(lib({ files: 5, with_release_id: 5, with_nzb_release_id: 5 })),
		).toBeNull();
		expect(libraryShareLine(lib({}))).toBeNull();
		expect(libraryShareLine(undefined)).toBeNull();
	});
	test("ignores nonsense numbers", () => {
		expect(libraryShareLine(lib({ files: Number.NaN, with_release_id: 1 }))).toBeNull();
		expect(libraryShareLine(lib({ files: 3, with_release_id: -1 }))).toBeNull();
		expect(libraryShareLine(lib({ files: 3, with_release_id: 9 }))).toBeNull();
	});
});

describe("backbone hint", () => {
	test("appears only while checks were skipped for lack of a backbone", () => {
		expect(backboneHint({ no_recognised_backbone: 3 })).toContain("backbone");
		expect(backboneHint({ no_recognised_backbone: 0 })).toBeNull();
		expect(backboneHint({ not_due: 4 })).toBeNull();
		expect(backboneHint(undefined)).toBeNull();
	});
});
