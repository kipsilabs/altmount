import { describe, expect, test } from "bun:test";
import { renderToStaticMarkup } from "react-dom/server";
import { HealthItemActionsMenu } from "../src/pages/HealthPage/components/HealthTable/HealthItemActionsMenu";
import { HealthItemCard } from "../src/pages/HealthPage/components/HealthTable/HealthItemCard";
import { HealthTableRow } from "../src/pages/HealthPage/components/HealthTable/HealthTableRow";
import { type FileHealth, HealthPriority } from "../src/types/api";

const missingPar2 = "par2repair: unrepairable: no PAR2 files recorded for this release";
const item: FileHealth = {
	id: 1,
	file_path: "complete/Victoria/Victoria.mkv",
	status: "corrupted",
	last_checked: "2026-10-07T07:08:02Z",
	last_error: missingPar2,
	retry_count: 1,
	max_retries: 2,
	repair_retry_count: 0,
	max_repair_retries: 3,
	created_at: "2026-10-07T07:00:00Z",
	updated_at: "2026-10-07T07:08:02Z",
	priority: HealthPriority.Normal,
	streaming_failure_count: 0,
	is_masked: false,
};

const noop = () => {};
const props = {
	item,
	isSelected: false,
	isCancelPending: false,
	isDirectCheckPending: false,
	isRepairPending: false,
	isDeletePending: false,
	isUnmaskPending: false,
	onSelectChange: noop,
	onCancelCheck: noop,
	onManualCheck: noop,
	onRepair: noop,
	onPar2Repair: noop,
	onDelete: noop,
	onUnmask: noop,
	onSetPriority: noop,
};

describe("PAR2 repair without recovery files", () => {
	test("disables repeat repair attempts and explains why in the action menu", () => {
		const html = renderToStaticMarkup(<HealthItemActionsMenu {...props} />);
		const beforeLabel = html.slice(0, html.indexOf("PAR2 Repair"));
		const button = beforeLabel.slice(beforeLabel.lastIndexOf("<button"));
		expect(button).toContain('disabled=""');
		expect(html).toContain("This NZB contains no PAR2 recovery files.");
	});

	for (const [name, Component] of [
		["desktop row", HealthTableRow],
		["mobile card", HealthItemCard],
	] as const) {
		test(`${name} explains the saved failure after the job is gone`, () => {
			const html = renderToStaticMarkup(<Component {...props} />);
			expect(html).toContain("Repair unavailable");
			expect(html).toContain("This NZB contains no PAR2 recovery files.");
			expect(html).toContain("Use an NZB with matching PAR2 files or replace the release.");
			expect(html).toContain(missingPar2);
		});
	}

	for (const last_error of [
		undefined,
		"nntp: connection timed out",
		"par2repair: unrepairable: damage ratio 0.2 exceeds max_repair_ratio 0.1",
	]) {
		test(`keeps repair available for ${last_error ?? "unknown eligibility"}`, () => {
			const html = renderToStaticMarkup(
				<HealthItemActionsMenu {...props} item={{ ...item, last_error }} />,
			);
			// Start at the button nearest the PAR2 label, excluding earlier disabled actions.
			const beforeLabel = html.slice(0, html.indexOf("PAR2 Repair"));
			const button = beforeLabel.slice(beforeLabel.lastIndexOf("<button"));
			expect(button).not.toContain('disabled=""');
		});
	}
});
