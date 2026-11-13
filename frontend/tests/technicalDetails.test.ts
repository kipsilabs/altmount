import { describe, expect, test } from "bun:test";
import { technicalDetails } from "../src/pages/HealthPage/components/technicalDetails";

describe("technicalDetails", () => {
	test("hides a details record that holds only peer markers", () => {
		expect(technicalDetails('{"peer_deferred_at":"2026-11-12T10:00:00Z"}')).toBeNull();
		expect(
			technicalDetails('{"peer_deferred_at":"2026-11-12T10:00:00Z","peer_reduced_at":"x"}'),
		).toBeNull();
	});

	test("keeps the other content when markers are removed", () => {
		expect(
			technicalDetails(
				'{"content_verification":"passed","peer_deferred_at":"2026-11-12T10:00:00Z"}',
			),
		).toBe('{"content_verification":"passed"}');
	});

	test("leaves details without markers exactly as they were", () => {
		for (const raw of ['{"content_verification":"passed"}', "plain text", "[1,2]", "{ not json"]) {
			expect(technicalDetails(raw)).toBe(raw);
		}
	});

	test("nothing in, nothing out", () => {
		expect(technicalDetails(null)).toBeNull();
		expect(technicalDetails(undefined)).toBeNull();
		expect(technicalDetails("")).toBeNull();
	});
});
