import { Loader2 } from "lucide-react";
import type { PeernetBackfillStatus } from "../../../types/peernet";

interface BackfillFooterProps {
	backfill: PeernetBackfillStatus | undefined;
}

export function BackfillFooter({ backfill }: BackfillFooterProps) {
	if (!backfill?.running) return null;
	return (
		<div className="alert alert-info alert-soft text-sm" aria-live="polite">
			<Loader2 className="h-5 w-5 shrink-0 animate-spin" aria-hidden="true" />
			<span>
				Building the content index: {backfill.scanned} scanned, {backfill.indexed} indexed,{" "}
				{backfill.skipped} skipped.
			</span>
		</div>
	);
}
