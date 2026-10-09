import type { RepairReason } from "./par2RepairReason";

export function RepairFailureMessage({ reason }: { reason: RepairReason }) {
	return (
		<div className="space-y-0.5 text-xs">
			<div className="break-words text-error">
				{reason.unavailable ? "Repair unavailable" : "Cannot repair"}: {reason.summary}
			</div>
			{reason.hint && <div className="break-words text-base-content/70">{reason.hint}</div>}
			<details className="text-base-content/60">
				<summary className="cursor-pointer">Repair details</summary>
				<div className="break-all">{reason.detail}</div>
			</details>
		</div>
	);
}
