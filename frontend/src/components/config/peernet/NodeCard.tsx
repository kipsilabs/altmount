import { useId } from "react";
import type { PeernetConfig, PeernetStatus } from "../../../types/peernet";
import { CopyButton } from "./CopyButton";
import type { NodeState } from "./nodeState";
import { PlainText } from "./PlainText";
import {
	MAX_LISTEN_PORT,
	MIN_LISTEN_PORT,
	validateAdvertiseAddress,
	validateListenPort,
} from "./peernetValidation";

interface NodeCardProps {
	form: PeernetConfig;
	status: PeernetStatus | undefined;
	nodeState: NodeState;
	onChange: (patch: Partial<PeernetConfig>) => void;
	disabled?: boolean;
}

function ListenerState({
	form,
	status,
}: {
	form: PeernetConfig;
	status: PeernetStatus | undefined;
}) {
	if (!status) {
		return <span className="text-base-content/60">Unknown</span>;
	}
	if (!status.accept_incoming) {
		return <span className="badge badge-ghost">Not accepting incoming connections</span>;
	}
	if (status.listen_error !== "") {
		return (
			<span className="badge badge-error h-auto whitespace-normal py-1" role="alert">
				<PlainText value={`Listener error: ${status.listen_error}`} maxWidthClass="max-w-xs" />
			</span>
		);
	}
	if (status.listening) {
		return <span className="badge badge-success">Listening on port {status.listen_port}</span>;
	}
	return (
		<span className="badge badge-warning">
			Not listening yet{form.accept_incoming ? "" : " (save to apply)"}
		</span>
	);
}

export function NodeCard({ form, status, nodeState, onChange, disabled = false }: NodeCardProps) {
	const acceptLegendId = useId();
	const acceptHelpId = useId();
	const portError = validateListenPort(form.listen_port);
	const advertiseError = validateAdvertiseAddress(form.advertise_address);

	return (
		<div className="card bg-base-100 shadow-lg">
			<div className="card-body space-y-3">
				<h3 className="card-title text-lg">This node</h3>

				<div className="flex flex-wrap items-center gap-2 text-sm">
					<span className="font-medium">Node ID</span>
					{status?.node_id ? (
						<>
							<PlainText value={status.node_id} maxWidthClass="max-w-xs" mono />
							<CopyButton value={status.node_id} label="Copy node ID" />
						</>
					) : nodeState === "stopped" ? (
						<span className="badge badge-warning">Not running</span>
					) : nodeState === "loading" ? (
						<span className="text-base-content/60">Loading...</span>
					) : nodeState === "unavailable" ? (
						<span className="text-base-content/60">Unavailable</span>
					) : (
						<span className="text-base-content/60">Available after saving</span>
					)}
				</div>

				<div className="flex flex-wrap items-center gap-2 text-sm">
					<span className="font-medium">Listener</span>
					<ListenerState form={form} status={status} />
				</div>

				<fieldset className="fieldset">
					<legend id={acceptLegendId} className="fieldset-legend">
						Accept incoming connections
					</legend>
					<label className="label cursor-pointer justify-start gap-3">
						<input
							type="checkbox"
							aria-labelledby={acceptLegendId}
							aria-describedby={acceptHelpId}
							className="toggle toggle-primary"
							checked={form.accept_incoming}
							disabled={disabled}
							onChange={(e) => onChange({ accept_incoming: e.target.checked })}
						/>
						<span className="label-text">Run the peer listener</span>
					</label>
					<p id={acceptHelpId} className="label whitespace-normal">
						Optional. Only required to host a private network; it also helps the open network.
					</p>
				</fieldset>

				<div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
					<fieldset className="fieldset">
						<legend className="fieldset-legend">Listen port</legend>
						<input
							type="number"
							className={`input w-full ${portError ? "input-error" : ""}`}
							aria-label="Listen port"
							min={MIN_LISTEN_PORT}
							max={MAX_LISTEN_PORT}
							value={Number.isNaN(form.listen_port) ? "" : form.listen_port}
							disabled={disabled}
							aria-invalid={portError !== null}
							onChange={(e) => onChange({ listen_port: e.target.valueAsNumber })}
						/>
						<p className={`label whitespace-normal ${portError ? "text-error" : ""}`}>
							{portError ??
								`${MIN_LISTEN_PORT}-${MAX_LISTEN_PORT}. A reverse proxy must pass TCP through.`}
						</p>
					</fieldset>
					<fieldset className="fieldset">
						<legend className="fieldset-legend">Advertise address</legend>
						<input
							type="text"
							className={`input w-full ${advertiseError ? "input-error" : ""}`}
							placeholder="host:port (optional)"
							aria-label="Advertise address"
							value={form.advertise_address}
							disabled={disabled}
							aria-invalid={advertiseError !== null}
							onChange={(e) => onChange({ advertise_address: e.target.value.trim() })}
						/>
						<p className={`label whitespace-normal ${advertiseError ? "text-error" : ""}`}>
							{advertiseError ?? "Address other peers should use to reach you."}
						</p>
						{status?.advertise_warning && !advertiseError && (
							<div className="alert alert-warning alert-soft mt-1 text-sm" role="alert">
								<span className="min-w-0 break-words">{status.advertise_warning}</span>
							</div>
						)}
					</fieldset>
				</div>
			</div>
		</div>
	);
}
