import { AlertTriangle, ShieldCheck } from "lucide-react";
import { useState } from "react";
import { useConfirmPeernetPeer, usePeernetPeers } from "../../../hooks/usePeernet";
import type { PeernetPeer } from "../../../types/peernet";
import { ConfirmModal } from "../../ui/ConfirmModal";
import { LoadingSpinner } from "../../ui/LoadingSpinner";
import { MutationErrorAlert } from "./MutationErrorAlert";
import { PlainText } from "./PlainText";
import { formatTimestamp } from "./peernetFormat";

interface PeerTableProps {
	networkId: string;
}

interface PeerRowProps {
	peer: PeernetPeer;
	onConfirm: (address: string) => void;
	isConfirming: boolean;
}

function PeerRow({ peer, onConfirm, isConfirming }: PeerRowProps) {
	return (
		<tr>
			<td>
				<PlainText value={peer.address} maxWidthClass="max-w-56" mono />
				{peer.unresolved && <span className="badge badge-ghost badge-sm ml-2">unresolved</span>}
				{peer.last_error !== "" && (
					<div className="text-error text-xs">
						<PlainText value={peer.last_error} maxWidthClass="max-w-56" />
					</div>
				)}
			</td>
			<td>
				<span className="badge badge-outline badge-sm">{peer.source}</span>
			</td>
			<td>
				{peer.node_id_changed ? (
					<span className="badge badge-warning badge-sm gap-1">
						<AlertTriangle className="h-3 w-3" aria-hidden="true" />
						Identity changed
					</span>
				) : peer.verified ? (
					<span className="badge badge-success badge-sm gap-1">
						<ShieldCheck className="h-3 w-3" aria-hidden="true" />
						Verified
					</span>
				) : (
					<span className="badge badge-ghost badge-sm">Unverified</span>
				)}
			</td>
			<td className="whitespace-nowrap text-xs">{formatTimestamp(peer.last_success)}</td>
			<td>{peer.failures}</td>
			<td>
				{peer.node_id_changed && (
					<button
						type="button"
						className="btn btn-warning btn-xs"
						disabled={isConfirming}
						onClick={() => onConfirm(peer.address)}
						title="The node at this address now presents a different identity. Confirm only if you expect this, for example after the peer reset its data."
					>
						Confirm
					</button>
				)}
			</td>
		</tr>
	);
}

export function PeerTable({ networkId }: PeerTableProps) {
	const { data, isLoading, error } = usePeernetPeers(networkId);
	const confirm = useConfirmPeernetPeer(networkId);
	const [pendingAddress, setPendingAddress] = useState<string | null>(null);

	const [trustTarget, setTrustTarget] = useState<string | null>(null);

	const handleConfirm = (address: string) => {
		setPendingAddress(address);
		confirm.mutate(address, { onSettled: () => setPendingAddress(null) });
	};

	if (isLoading) return <LoadingSpinner size="sm" />;
	if (error) return <MutationErrorAlert error={error} />;

	const peers = data ?? [];
	const hasChanged = peers.some((p) => p.node_id_changed);

	return (
		<div className="space-y-2">
			<h4 className="font-semibold text-sm">Known peers</h4>
			{hasChanged && (
				<p className="text-base-content/70 text-xs">
					Identity changed: the node at this address now presents a different identity. Confirm to
					trust the new identity, or leave it if you do not expect this.
				</p>
			)}
			<MutationErrorAlert error={confirm.error} />
			{peers.length === 0 ? (
				<p className="text-base-content/60 text-sm">No peers known yet.</p>
			) : (
				<div className="overflow-x-auto">
					<table className="table-sm table">
						<thead>
							<tr>
								<th>Address</th>
								<th>Source</th>
								<th>Status</th>
								<th>Last success</th>
								<th>Failures</th>
								<th>
									<span className="sr-only">Actions</span>
								</th>
							</tr>
						</thead>
						<tbody>
							{peers.map((peer) => (
								<PeerRow
									key={`${peer.node_id}-${peer.address}`}
									peer={peer}
									onConfirm={setTrustTarget}
									isConfirming={confirm.isPending && pendingAddress === peer.address}
								/>
							))}
						</tbody>
					</table>
				</div>
			)}
			<ConfirmModal
				isOpen={trustTarget !== null}
				title="Trust the new identity"
				message={`The node at ${trustTarget ?? "this address"} now presents a different identity than before. Confirming trusts the new identity. Only confirm if you expect this, for example after the peer reset its data.`}
				confirmText="Trust new identity"
				confirmButtonClass="btn-warning"
				onCancel={() => setTrustTarget(null)}
				onConfirm={() => {
					if (trustTarget !== null) handleConfirm(trustTarget);
					setTrustTarget(null);
				}}
			/>
		</div>
	);
}
