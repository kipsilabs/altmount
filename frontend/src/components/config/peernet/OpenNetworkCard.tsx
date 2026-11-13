import type { PeernetConfig, PeernetNetworkStatus } from "../../../types/peernet";
import { LoadingSpinner } from "../../ui/LoadingSpinner";
import { ExposureLevelSelect } from "./ExposureLevelSelect";
import type { NodeState } from "./nodeState";
import { PeerAddressList } from "./PeerAddressList";
import { PeerTable } from "./PeerTable";
import { SharedDataBlock } from "./SharedDataBlock";

interface OpenNetworkCardProps {
	form: PeernetConfig;
	openNetwork: PeernetNetworkStatus | undefined;
	nodeState: NodeState;
	onChange: (patch: Partial<PeernetConfig>) => void;
	disabled?: boolean;
}

export function OpenNetworkCard({
	form,
	openNetwork,
	nodeState,
	onChange,
	disabled = false,
}: OpenNetworkCardProps) {
	const open = form.open_network;
	const corroborationInvalid =
		!Number.isInteger(form.min_corroboration) || form.min_corroboration < 1;

	return (
		<div className="card bg-base-100 shadow-lg">
			<div className="card-body space-y-3">
				<h3 className="card-title text-lg">Open network</h3>
				<p className="text-base-content/70 text-sm">
					Any node can join the open network. Peers are found from the addresses you enter and from
					peer exchange.
				</p>

				<ExposureLevelSelect
					name="open-network"
					value={open.level}
					disabled={disabled}
					onChange={(level) => onChange({ open_network: { ...open, level } })}
				/>

				<PeerAddressList
					peers={open.peers}
					disabled={disabled}
					onChange={(peers) => onChange({ open_network: { ...open, peers } })}
				/>
				<p className="text-base-content/60 text-xs">
					Private, loopback and similar local addresses are not connected to on the open network.
				</p>

				<fieldset className="fieldset">
					<legend className="fieldset-legend">Minimum corroboration</legend>
					<input
						type="number"
						className={`input w-full max-w-32 ${corroborationInvalid ? "input-error" : ""}`}
						aria-label="Minimum corroboration"
						min={1}
						value={Number.isNaN(form.min_corroboration) ? "" : form.min_corroboration}
						disabled={disabled}
						aria-invalid={corroborationInvalid}
						onChange={(e) => onChange({ min_corroboration: e.target.valueAsNumber })}
					/>
					<p className="label whitespace-normal">
						Distinct nodes that must attest a release before it is shown as healthy on the open
						network, and before a check is shortened. Nodes in the same network range count as one.
						Raising it makes the network count for less.
					</p>
				</fieldset>

				{openNetwork ? (
					<>
						<SharedDataBlock network={openNetwork} />
						<PeerTable networkId={openNetwork.id} />
					</>
				) : nodeState === "loading" ? (
					<LoadingSpinner size="sm" />
				) : nodeState === "stopped" ? (
					<p className="text-base-content/60 text-sm">
						The node is not running, so the open network is not active.
					</p>
				) : nodeState === "unsaved" ? (
					<p className="text-base-content/60 text-sm">
						Save to start the open network and see its peers.
					</p>
				) : (
					<p className="text-base-content/60 text-sm">Network status is unavailable.</p>
				)}
			</div>
		</div>
	);
}
