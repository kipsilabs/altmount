import { formatBytes } from "../../../lib/utils";
import type { PeernetStatus } from "../../../types/peernet";
import {
	backboneHint,
	describeBreakdown,
	fetchFailLabel,
	importRejectLabel,
	libraryShareLine,
	notShareableLabel,
	publishedTotal,
	serveRefusedLabel,
	showAdviceBlock,
	showBundleServing,
	showBundlesBlock,
	showPublishingDetails,
	skipLabel,
} from "./peernetStats";

interface ActivityCardProps {
	status: PeernetStatus | undefined;
}

function count(value: number | undefined): number {
	return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function PublishingBlock({ status }: { status: PeernetStatus }) {
	if (!showPublishingDetails(status.networks)) {
		return (
			<p className="text-base-content/70 text-sm">
				Nothing is published at the current levels. Set a network to level 2 to share records.
			</p>
		);
	}
	const publish = status.publish;
	const skipped = describeBreakdown(publish?.skipped, skipLabel);
	const skippedTotal = skipped.reduce((sum, e) => sum + e.count, 0);
	const dropped = count(publish?.dropped);
	const errors = count(publish?.errors);
	return (
		<div className="space-y-1 text-sm">
			<p>
				Records published since start: {publishedTotal(publish?.published)} (
				{count(publish?.published?.release)} release, {count(publish?.published?.attestation)}{" "}
				health)
			</p>
			{skippedTotal > 0 && (
				<div>
					<p className="text-base-content/70">Not published: {skippedTotal}</p>
					<ul className="list-inside list-disc text-xs">
						{skipped.map((entry) => (
							<li key={entry.key} className="break-words">
								{entry.label}: {entry.count}
							</li>
						))}
					</ul>
				</div>
			)}
			{backboneHint(publish?.skipped) && (
				<p className="text-warning text-xs">{backboneHint(publish?.skipped)}</p>
			)}
			{dropped > 0 && <p className="text-warning">Dropped before sending: {dropped}</p>}
			{errors > 0 && <p className="text-error">Errors while publishing: {errors}</p>}
		</div>
	);
}

interface BreakdownListProps {
	title: string;
	counts: Readonly<Record<string, number>> | undefined;
	label: (key: string) => string;
}

function BreakdownList({ title, counts, label }: BreakdownListProps) {
	const entries = describeBreakdown(counts, label);
	if (entries.length === 0) return null;
	const total = entries.reduce((sum, e) => sum + e.count, 0);
	return (
		<div>
			<p className="text-base-content/70">
				{title}: {total}
			</p>
			<ul className="list-inside list-disc text-xs">
				{entries.map((entry) => (
					<li key={entry.key} className="break-words">
						{entry.label}: {entry.count}
					</li>
				))}
			</ul>
		</div>
	);
}

function BundlesBlock({ status }: { status: PeernetStatus }) {
	const b = status.bundles;
	const imp = b?.import;
	const serving = showBundleServing(status.networks);
	const backedOff = count(b?.fetch_backed_off);
	return (
		<div className="space-y-2 text-sm">
			{serving && (
				<div className="space-y-1">
					<p>Bundles announced: {count(b?.announced)}</p>
					<p>
						Bundles served: {count(b?.served)} ({formatBytes(count(b?.served_bytes))})
					</p>
					<BreakdownList
						title="Not shareable as a bundle"
						counts={b?.not_shareable}
						label={notShareableLabel}
					/>
					<BreakdownList
						title="Refused requests"
						counts={b?.serve_refused}
						label={serveRefusedLabel}
					/>
				</div>
			)}
			<div className="space-y-1">
				<p>Bundles fetched: {count(b?.fetched)}</p>
				<p>Imports accepted: {count(imp?.accepted)}</p>
				<p>
					Awaiting full check: {count(imp?.full_check_passed)} passed,{" "}
					{count(imp?.full_check_failed)} failed
				</p>
				{(imp?.pending ?? 0) > 0 && (
					<p className="text-warning">
						Waiting for a full check: {count(imp?.pending)}
						{(imp?.stale ?? 0) > 0 ? ` (${count(imp?.stale)} waited too long and were failed)` : ""}
					</p>
				)}
				<BreakdownList title="Rejected bundles" counts={imp?.rejected} label={importRejectLabel} />
				<BreakdownList title="Fetch failures" counts={b?.fetch_failed} label={fetchFailLabel} />
				{backedOff > 0 && <p className="text-warning">Publishers backed off: {backedOff}</p>}
			</div>
		</div>
	);
}

function AdviceBlock({ status }: { status: PeernetStatus }) {
	const advice = status.advice;
	return (
		<dl className="grid grid-cols-1 gap-x-6 gap-y-1 text-sm sm:grid-cols-2">
			<div className="flex justify-between gap-2">
				<dt className="text-base-content/70">Checks shortened</dt>
				<dd>{count(advice?.reduce_applied)}</dd>
			</div>
			<div className="flex justify-between gap-2">
				<dt className="text-base-content/70">Checks skipped once</dt>
				<dd>{count(advice?.skips_applied)}</dd>
			</div>
			<div className="flex justify-between gap-2">
				<dt className="text-base-content/70">Rechecks moved forward</dt>
				<dd>{count(advice?.recheck_scheduled)}</dd>
			</div>
			<div className="flex justify-between gap-2">
				<dt className="text-base-content/70">Early rechecks left this hour</dt>
				<dd>{count(advice?.budget_remaining)}</dd>
			</div>
		</dl>
	);
}

export function ActivityCard({ status }: ActivityCardProps) {
	if (!status?.enabled) return null;
	return (
		<div className="card bg-base-100 shadow-lg">
			<div className="card-body space-y-3">
				<h3 className="card-title text-lg">Activity</h3>
				{libraryShareLine(status.library) && (
					<p className="text-base-content/70 text-sm">{libraryShareLine(status.library)}</p>
				)}
				<section className="space-y-1" aria-labelledby="peernet-publishing-title">
					<h4 id="peernet-publishing-title" className="font-medium text-sm">
						Publishing
					</h4>
					<PublishingBlock status={status} />
				</section>
				{showBundlesBlock(status.networks) && (
					<section className="space-y-1" aria-labelledby="peernet-bundles-title">
						<h4 id="peernet-bundles-title" className="font-medium text-sm">
							Bundles
						</h4>
						<BundlesBlock status={status} />
					</section>
				)}
				{showAdviceBlock(status.networks) && (
					<section className="space-y-1" aria-labelledby="peernet-advice-title">
						<h4 id="peernet-advice-title" className="font-medium text-sm">
							Health-check advice
						</h4>
						<AdviceBlock status={status} />
					</section>
				)}
			</div>
		</div>
	);
}
