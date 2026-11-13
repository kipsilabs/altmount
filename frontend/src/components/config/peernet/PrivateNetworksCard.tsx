import type { PeernetNetworkStatus } from "../../../types/peernet";
import { CreateNetworkForm } from "./CreateNetworkForm";
import { JoinNetworkForm } from "./JoinNetworkForm";
import { PrivateNetworkItem } from "./PrivateNetworkItem";

interface PrivateNetworksCardProps {
	networks: PeernetNetworkStatus[];
	acceptIncoming: boolean;
	isReadOnly?: boolean;
}

export function PrivateNetworksCard({
	networks,
	acceptIncoming,
	isReadOnly = false,
}: PrivateNetworksCardProps) {
	return (
		<div className="card bg-base-100 shadow-lg">
			<div className="card-body space-y-4">
				<h3 className="card-title text-lg">Private networks</h3>
				<p className="text-base-content/70 text-sm">
					Invite-only networks. Changes here apply immediately and do not need Save.
				</p>

				<div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
					<CreateNetworkForm acceptIncoming={acceptIncoming} disabled={isReadOnly} />
					<JoinNetworkForm disabled={isReadOnly} />
				</div>

				{networks.length === 0 ? (
					<p className="text-base-content/60 text-sm">No private networks yet.</p>
				) : (
					<div className="space-y-4">
						{networks.map((network) => (
							<PrivateNetworkItem key={network.id} network={network} disabled={isReadOnly} />
						))}
					</div>
				)}
			</div>
		</div>
	);
}
