import { peerBadge } from "../config/peernet/peernetStats";

interface PeerBadgeProps {
	origin?: boolean;
	fullCheckPending?: boolean;
}

// Marks an item imported from a peer bundle. Renders nothing for other items,
// so rows without the flag keep their layout.
export function PeerBadge({ origin, fullCheckPending }: PeerBadgeProps) {
	const info = peerBadge(origin, fullCheckPending);
	if (!info) return null;
	return (
		<span
			className="badge badge-info badge-outline badge-sm max-w-full shrink-0 whitespace-normal"
			title={info.title}
			role="note"
			aria-label={`${info.label}. ${info.title}`}
		>
			{info.label}
		</span>
	);
}
