/**
 * Translates a PAR2 repair job's stored error into something a user can act on.
 *
 * The backend stores the raw Go error (e.g. "par2repair: unrepairable: needs
 * 4127 recovery slices, set has 400"), which is precise but says nothing about
 * what the user should do next. Each known failure has exactly one cause and
 * one remedy, so we map them explicitly rather than trying to prettify the
 * string generically. Unknown errors fall through unchanged — a technical
 * message beats a wrong friendly one.
 */

export interface RepairReason {
	/** One-line explanation of why the repair could not run. */
	summary: string;
	/** What the user can do about it, when there is anything. */
	hint?: string;
	/** Recovery files are absent, so repeating this repair cannot help. */
	unavailable?: boolean;
	/** The raw backend error, always kept for support/debugging. */
	detail: string;
}

const PREFIXES = ["par2repair: unrepairable: ", "par2repair: ", "attempts exhausted: "];

function stripPrefixes(raw: string): { message: string; exhausted: boolean } {
	let message = raw;
	let exhausted = false;
	let changed = true;
	while (changed) {
		changed = false;
		for (const prefix of PREFIXES) {
			if (message.startsWith(prefix)) {
				if (prefix === "attempts exhausted: ") {
					exhausted = true;
				}
				message = message.slice(prefix.length);
				changed = true;
			}
		}
	}
	return { message, exhausted };
}

function percent(value: string): string {
	const n = Number.parseFloat(value);
	if (Number.isNaN(n)) {
		return value;
	}
	// Two decimals only when the value is small enough to need them, so 5% does
	// not render as "5.00%".
	const pct = n * 100;
	return `${pct >= 1 ? pct.toFixed(pct % 1 === 0 ? 0 : 1) : pct.toFixed(2)}%`;
}

export function parseRepairReason(raw: string): RepairReason {
	const { message, exhausted } = stripPrefixes(raw);
	if (message === "no PAR2 files recorded for this release") {
		return {
			summary: "This NZB contains no PAR2 recovery files.",
			hint: "Use an NZB with matching PAR2 files or replace the release.",
			unavailable: true,
			detail: raw,
		};
	}

	// Not enough redundancy posted with the release.
	const slices = message.match(/needs (\d+) recovery slices, set has (\d+)/);
	if (slices) {
		return {
			summary: `The release is damaged beyond what its PAR2 files can rebuild — ${slices[1]} recovery blocks are needed, but only ${slices[2]} were posted.`,
			hint: "Nothing can fix this locally. Replace the release from another indexer or source.",
			detail: raw,
		};
	}

	// Damage exceeds the configured policy cap — the one case the user controls.
	const ratio = message.match(/damage ratio ([\d.]+) exceeds max_repair_ratio ([\d.]+)/);
	if (ratio) {
		return {
			summary: `${percent(ratio[1])} of this file is missing, above the ${percent(ratio[2])} repair limit.`,
			hint: "Raise the maximum repair size in Settings → PAR2 Repair to attempt it anyway.",
			detail: raw,
		};
	}

	// The release falls outside the configured size range — user policy.
	const sizeMin = message.match(/release size (\d+) MB is below min_release_size_mb (\d+)/);
	if (sizeMin) {
		return {
			summary: `This release is ${sizeMin[1]} MB, below the ${sizeMin[2]} MB minimum for repairs.`,
			hint: "Lower the minimum release size in Settings → PAR2 Repair to attempt it.",
			detail: raw,
		};
	}
	const sizeMax = message.match(/release size (\d+) MB exceeds max_release_size_mb (\d+)/);
	if (sizeMax) {
		return {
			summary: `This release is ${sizeMax[1]} MB, above the ${sizeMax[2]} MB maximum for repairs.`,
			hint: "A repair downloads the whole release once. Raise the maximum release size in Settings → PAR2 Repair to attempt it.",
			detail: raw,
		};
	}

	// Emitted by older versions only: repairs over the memory budget now spill
	// to disk instead of failing. Kept so historical rows still read sensibly.
	const memory = message.match(
		/(\d+) missing slices need (\d+) bytes of accumulator memory \(budget (\d+)\)/,
	);
	if (memory) {
		const mb = (n: string) => `${Math.ceil(Number(n) / (1024 * 1024))} MB`;
		return {
			summary: `Rebuilding ${memory[1]} blocks needed ${mb(memory[2])} of memory, above the ${mb(memory[3])} budget.`,
			hint: "This limit no longer blocks repairs — large jobs now spill to disk. Retry the repair.",
			detail: raw,
		};
	}

	if (message.startsWith("nothing to repair")) {
		return {
			summary: "No missing articles were found — nothing needed repairing.",
			detail: raw,
		};
	}

	if (message.includes("not resolved to usenet articles")) {
		return {
			summary:
				"The PAR2 files cover articles that are not part of this download, so the recovery data cannot be matched up.",
			hint: "This usually means the NZB is incomplete. Re-download it from your indexer.",
			detail: raw,
		};
	}

	if (message.includes("no live article anywhere in the release")) {
		return {
			summary:
				"Every article of this release is gone from your providers, so there is nothing left to rebuild from.",
			hint: "The release has expired or been removed entirely. Replace it from another indexer.",
			detail: raw,
		};
	}

	if (message.includes("part size") && message.includes("inconsistent")) {
		return {
			summary:
				"The article layout recorded for this release does not match what its PAR2 files describe.",
			hint: "Usually a mismatched or re-posted NZB. Re-download the NZB from your indexer.",
			detail: raw,
		};
	}

	if (message.includes("no Main packet found")) {
		return {
			summary:
				"The release's PAR2 files could not be read — their own articles are missing from your providers.",
			hint: "PAR2 files expire like any other article. Replace the release.",
			detail: raw,
		};
	}

	if (exhausted) {
		return {
			summary: `Gave up after repeated failures: ${message}`,
			hint: "Usually a provider or connection problem. Retry the repair once your providers are healthy.",
			detail: raw,
		};
	}

	return { summary: message, detail: raw };
}
