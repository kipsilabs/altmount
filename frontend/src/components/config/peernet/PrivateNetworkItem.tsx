import { Trash2 } from "lucide-react";
import { useEffect, useId, useState } from "react";
import { useDeletePeernetNetwork, useUpdatePeernetNetwork } from "../../../hooks/usePeernet";
import type { PeernetLevel, PeernetNetworkStatus } from "../../../types/peernet";
import { ConfirmModal } from "../../ui/ConfirmModal";
import { ExposureLevelSelect } from "./ExposureLevelSelect";
import { InvitesPanel } from "./InvitesPanel";
import { MembersPanel } from "./MembersPanel";
import { MutationErrorAlert } from "./MutationErrorAlert";
import { PeerTable } from "./PeerTable";
import { PlainText } from "./PlainText";
import { formatTimestamp, isPast } from "./peernetFormat";
import { validateHostAddress } from "./peernetValidation";
import { SharedDataBlock } from "./SharedDataBlock";

interface PrivateNetworkItemProps {
	network: PeernetNetworkStatus;
	disabled?: boolean;
}

export function PrivateNetworkItem({ network, disabled = false }: PrivateNetworkItemProps) {
	const isHost = network.role === "host";
	const update = useUpdatePeernetNetwork(network.id);
	const remove = useDeletePeernetNetwork();
	const [draftLevel, setDraftLevel] = useState<PeernetLevel>(network.level);
	const [hostAddress, setHostAddress] = useState(network.host_address);
	const [confirmOpen, setConfirmOpen] = useState(false);
	const [confirmPrivateOpen, setConfirmPrivateOpen] = useState(false);
	const allowPrivateId = useId();
	const allowPrivateNameId = useId();
	const allowPrivateTextId = useId();
	const hostAddressError = validateHostAddress(hostAddress);

	useEffect(() => setDraftLevel(network.level), [network.level]);
	useEffect(() => setHostAddress(network.host_address), [network.host_address]);

	const counts = network.peers;
	const certExpired = isPast(network.cert_expires_at);

	return (
		<div className="space-y-3 rounded-box border border-base-300 p-4">
			<div className="flex flex-wrap items-center justify-between gap-2">
				<div className="flex min-w-0 items-center gap-2">
					<h4 className="font-semibold">
						<PlainText value={network.name} maxWidthClass="max-w-64" />
					</h4>
					<span className={`badge badge-sm ${isHost ? "badge-primary" : "badge-secondary"}`}>
						{isHost ? "Host" : "Member"}
					</span>
					{network.revoked && <span className="badge badge-error badge-sm">Removed</span>}
				</div>
				<button
					type="button"
					className="btn btn-error btn-outline btn-sm"
					disabled={disabled || remove.isPending}
					onClick={() => setConfirmOpen(true)}
				>
					<Trash2 className="h-4 w-4" aria-hidden="true" />
					{isHost ? "Delete" : "Leave"}
				</button>
			</div>

			{network.revoked && (
				<div className="alert alert-warning text-sm" role="alert">
					<div>
						<div className="font-bold">Removed by the host</div>
						<div>
							The host of this network removed this node
							{network.revoked_at ? ` (${formatTimestamp(network.revoked_at)})` : ""}. It no longer
							takes part in the network and cannot rejoin without a new invite. Leave the network to
							delete it from this list.
						</div>
					</div>
					<button
						type="button"
						className="btn btn-sm"
						disabled={disabled || remove.isPending}
						onClick={() => setConfirmOpen(true)}
					>
						Leave network
					</button>
				</div>
			)}

			{!network.revoked && (
				<>
					<p className="text-base-content/70 text-xs">
						Peers: {counts.total} known, {counts.verified} verified, {counts.active} active
						{counts.node_id_changed > 0 ? `, ${counts.node_id_changed} identity changed` : ""}
						{counts.unresolved > 0 ? `, ${counts.unresolved} unresolved` : ""}.
						{!isHost && (
							<>
								{" "}
								Certificate expires {formatTimestamp(network.cert_expires_at)}
								{certExpired ? " (expired)" : ""}.
							</>
						)}
					</p>

					<SharedDataBlock network={network} />

					<ExposureLevelSelect
						name={`network-${network.id}`}
						value={draftLevel}
						disabled={disabled}
						onChange={setDraftLevel}
					/>
					{draftLevel !== network.level && (
						<button
							type="button"
							className="btn btn-primary btn-sm"
							disabled={disabled || update.isPending}
							onClick={() => update.mutate({ level: draftLevel })}
						>
							Apply level
						</button>
					)}

					<label htmlFor={allowPrivateId} className="label cursor-pointer justify-start gap-3">
						<input
							id={allowPrivateId}
							type="checkbox"
							aria-labelledby={allowPrivateNameId}
							aria-describedby={allowPrivateTextId}
							className="toggle toggle-warning toggle-sm"
							checked={network.allow_private_addresses}
							disabled={disabled || update.isPending}
							onChange={(e) => {
								if (e.target.checked) setConfirmPrivateOpen(true);
								else update.mutate({ allow_private_addresses: false });
							}}
						/>
						<span className="label-text whitespace-normal">
							<span id={allowPrivateNameId}>Allow private addresses.</span>{" "}
							<span id={allowPrivateTextId} className="text-base-content/70">
								Lets peers on LAN (private) addresses connect; only enable for networks you fully
								trust. Loopback is never allowed.
							</span>
						</span>
					</label>

					{isHost && (
						<fieldset className="fieldset">
							<legend className="fieldset-legend">Host address</legend>
							<div className="flex gap-2">
								<input
									type="text"
									className={`input w-full max-w-sm ${hostAddressError ? "input-error" : ""}`}
									placeholder="host:port"
									aria-label="Host address"
									aria-invalid={hostAddressError !== null}
									value={hostAddress}
									disabled={disabled}
									onChange={(e) => setHostAddress(e.target.value.trim())}
								/>
								<button
									type="button"
									className="btn btn-outline"
									disabled={
										disabled ||
										update.isPending ||
										hostAddressError !== null ||
										hostAddress === network.host_address
									}
									onClick={() => update.mutate({ host_address: hostAddress })}
								>
									Save
								</button>
							</div>
							<p
								className={`label whitespace-normal ${hostAddressError && hostAddress !== "" ? "text-error" : ""}`}
							>
								{hostAddress !== "" && hostAddressError
									? hostAddressError
									: "Address written into new invite links."}
							</p>
						</fieldset>
					)}

					<MutationErrorAlert error={update.error} />
					<MutationErrorAlert error={remove.error} />

					{isHost && (
						<>
							<InvitesPanel networkId={network.id} disabled={disabled} />
							<MembersPanel networkId={network.id} disabled={disabled} />
						</>
					)}
					<PeerTable networkId={network.id} />
				</>
			)}

			<ConfirmModal
				isOpen={confirmPrivateOpen}
				title="Allow private addresses"
				message="Peers on LAN and other private addresses will be allowed to connect to this network (loopback is never allowed). Only enable this for networks you fully trust."
				confirmText="Allow private addresses"
				confirmButtonClass="btn-warning"
				onCancel={() => setConfirmPrivateOpen(false)}
				onConfirm={() => {
					setConfirmPrivateOpen(false);
					update.mutate({ allow_private_addresses: true });
				}}
			/>
			<ConfirmModal
				isOpen={confirmOpen}
				title={isHost ? "Delete network" : "Leave network"}
				message={
					isHost
						? "Deleting this network removes all its invites and members. Members will lose access."
						: "You will stop sharing with this network and need a new invite to rejoin."
				}
				confirmText={isHost ? "Delete network" : "Leave network"}
				confirmButtonClass="btn-error"
				onCancel={() => setConfirmOpen(false)}
				onConfirm={() => {
					setConfirmOpen(false);
					remove.mutate(network.id);
				}}
			/>
		</div>
	);
}
