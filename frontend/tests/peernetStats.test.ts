import { describe, expect, test } from "bun:test";
import {
	backboneHint,
	describeBreakdown,
	formatRelativeTime,
	hasAnyPublishingLevel,
	isLevelAvailable,
	libraryShareLine,
	publishedTotal,
	rejectionLabel,
	showAdviceBlock,
	showPublishingDetails,
	showSharedData,
	skipLabel,
} from "../src/components/config/peernet/peernetStats";
import {
	EXPOSURE_LEVELS,
	levelPrivacyNotice,
} from "../src/components/config/peernet/peernetValidation";

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
];

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
	test("level 3 is not available, 0-2 are", () => {
		expect(EXPOSURE_LEVELS.map((l) => isLevelAvailable(l.level))).toEqual([
			true,
			true,
			true,
			false,
		]);
		expect(EXPOSURE_LEVELS[3]?.description).toContain("Not available yet");
	});
	test("level 2 text states the privacy consequence; no bundle claim", () => {
		expect(levelPrivacyNotice(2)).toContain("node ID and IP address");
		expect(levelPrivacyNotice(3)).not.toContain("served");
	});
	test("level 1 states what it reveals", () => {
		expect(EXPOSURE_LEVELS[1]?.description).toContain("node ID and IP address");
	});
});

describe("library sharing line", () => {
	test("says how many files can be shared when some cannot", () => {
		expect(
			libraryShareLine({ files: 2358, with_release_id: 41, file_id_only: 2317, not_indexable: 0 }),
		).toBe(
			"41 of 2358 library files can be shared; the rest were imported before sharing data was kept.",
		);
	});
	test("is silent when everything is shareable or nothing is known", () => {
		expect(
			libraryShareLine({ files: 5, with_release_id: 5, file_id_only: 0, not_indexable: 0 }),
		).toBeNull();
		expect(
			libraryShareLine({ files: 0, with_release_id: 0, file_id_only: 0, not_indexable: 0 }),
		).toBeNull();
		expect(libraryShareLine(undefined)).toBeNull();
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
