import type { PeernetNetworkStatus } from "../../../types/peernet";
import { formatTimestamp } from "./peernetFormat";
import {
	describeBreakdown,
	formatRelativeTime,
	rejectionLabel,
	showSharedData,
} from "./peernetStats";

interface SharedDataBlockProps {
	network: PeernetNetworkStatus;
}

function count(value: number | undefined): number {
	return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

export function SharedDataBlock({ network }: SharedDataBlockProps) {
	if (!showSharedData(network.level)) return null;
	const records = network.records;
	if (!records) return null;

	const rejected = describeBreakdown(records.rejected, rejectionLabel);
	const rejectedTotal = rejected.reduce((sum, e) => sum + e.count, 0);
	const backedOff = count(records.peers_backed_off);
	const lastPull = records.last_pull ?? null;

	return (
		<section
			className="space-y-2 rounded-box bg-base-200/50 p-3"
			aria-label={`Shared data for ${network.kind === "open" ? "the open network" : "this network"}`}
		>
			<h4 className="font-medium text-sm">Shared data</h4>
			<dl className="grid grid-cols-1 gap-x-6 gap-y-1 text-sm sm:grid-cols-2">
				<div className="flex justify-between gap-2">
					<dt className="text-base-content/70">Release records</dt>
					<dd>
						{count(records.releases?.local)} own, {count(records.releases?.peer)} received
					</dd>
				</div>
				<div className="flex justify-between gap-2">
					<dt className="text-base-content/70">Health attestations</dt>
					<dd>
						{count(records.attestations?.local)} own, {count(records.attestations?.peer)} received
					</dd>
				</div>
				<div className="flex justify-between gap-2">
					<dt className="text-base-content/70">Publishers</dt>
					<dd>{count(records.publishers)}</dd>
				</div>
				<div className="flex justify-between gap-2">
					<dt className="text-base-content/70">Last successful pull</dt>
					<dd title={lastPull ? formatTimestamp(lastPull) : "Never"}>
						{formatRelativeTime(lastPull)}
					</dd>
				</div>
				{backedOff > 0 && (
					<div className="flex justify-between gap-2">
						<dt className="text-base-content/70">Peers backed off</dt>
						<dd>{backedOff}</dd>
					</div>
				)}
			</dl>
			{rejectedTotal > 0 && (
				<div className="text-sm">
					<p className="text-base-content/70">Rejected records: {rejectedTotal}</p>
					<ul className="list-inside list-disc text-xs">
						{rejected.map((entry) => (
							<li key={entry.key} className="break-words">
								{entry.label}: {entry.count}
							</li>
						))}
					</ul>
				</div>
			)}
		</section>
	);
}
