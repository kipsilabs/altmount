import { useQueryClient } from "@tanstack/react-query";
import { Network } from "lucide-react";
import { useEffect, useId, useState } from "react";
import { APIError } from "../../../api/client";
import { peernetKeys, usePeernetStatus } from "../../../hooks/usePeernet";
import type { ConfigResponse } from "../../../types/config";
import type { PeernetConfig } from "../../../types/peernet";
import { ActivityCard } from "./ActivityCard";
import { BackfillFooter } from "./BackfillFooter";
import { NodeCard } from "./NodeCard";
import { deriveNodeState } from "./nodeState";
import { OpenNetworkCard } from "./OpenNetworkCard";
import { PlainText } from "./PlainText";
import { PrivateNetworksCard } from "./PrivateNetworksCard";
import { normalizePeernetConfig, validatePeernetConfig } from "./peernetValidation";
import { SharingExclusionsCard } from "./SharingExclusionsCard";

interface PeernetConfigSectionProps {
	config: ConfigResponse;
	onUpdate?: (section: string, data: PeernetConfig) => Promise<void>;
	isReadOnly?: boolean;
	isUpdating?: boolean;
}

function describeSaveError(err: unknown): string {
	if (err instanceof APIError && err.details && err.details !== err.message) {
		return `${err.message}: ${err.details}`;
	}
	return err instanceof Error ? err.message : "Failed to save";
}

export function PeernetConfigSection({
	config,
	onUpdate,
	isReadOnly = false,
	isUpdating = false,
}: PeernetConfigSectionProps) {
	const enableLegendId = useId();
	const saved = normalizePeernetConfig(config.peernet);
	const [form, setForm] = useState<PeernetConfig>(saved);
	const [saveError, setSaveError] = useState<string | null>(null);

	useEffect(() => {
		setForm(normalizePeernetConfig(config.peernet));
		setSaveError(null);
	}, [config.peernet]);

	const queryClient = useQueryClient();
	const {
		data: status,
		error: statusError,
		isLoading: statusLoading,
	} = usePeernetStatus(saved.enabled);
	const nodeState = deriveNodeState(saved.enabled, status, statusLoading);

	const hasChanges = JSON.stringify(form) !== JSON.stringify(saved);
	const validationError = validatePeernetConfig(form);
	const openNetwork = status?.networks.find((n) => n.kind === "open");

	const handleChange = (patch: Partial<PeernetConfig>) =>
		setForm((prev) => ({ ...prev, ...patch }));

	const handleSave = async () => {
		if (!onUpdate || !hasChanges || validationError) return;
		setSaveError(null);
		try {
			await onUpdate("peernet", form);
			await queryClient.invalidateQueries({ queryKey: peernetKeys.all });
		} catch (err) {
			setSaveError(describeSaveError(err));
		}
	};

	return (
		<div className="space-y-6">
			<div className="card bg-base-100 shadow-lg">
				<div className="card-body space-y-2">
					<h3 className="card-title text-lg">
						<Network className="h-5 w-5" aria-hidden="true" />
						Peer network
					</h3>
					<p className="text-base-content/70 text-sm">
						Lets AltMount instances share which releases are healthy. Media content is never
						transferred, and everything received is verified locally. Off by default: while off,
						nothing listens or connects.
					</p>
					<fieldset className="fieldset">
						<legend id={enableLegendId} className="fieldset-legend">
							Enable peer network
						</legend>
						<label className="label cursor-pointer justify-start gap-3">
							<input
								type="checkbox"
								aria-labelledby={enableLegendId}
								className="toggle toggle-primary"
								checked={form.enabled}
								disabled={isReadOnly}
								onChange={(e) => handleChange({ enabled: e.target.checked })}
							/>
							<span className="label-text">{form.enabled ? "Enabled" : "Off (default)"}</span>
						</label>
					</fieldset>
				</div>
			</div>

			{form.enabled && (
				<>
					{statusError && (
						<div className="alert alert-error alert-soft text-sm" role="alert">
							{statusError.message}
						</div>
					)}
					{status && status.error !== "" && (
						<div className="alert alert-error alert-soft text-sm" role="alert">
							<span className="min-w-0 break-words">
								Peer network failed to start:{" "}
								<PlainText value={status.error} maxWidthClass="max-w-full" />
							</span>
						</div>
					)}
					<NodeCard
						form={form}
						status={status}
						nodeState={nodeState}
						onChange={handleChange}
						disabled={isReadOnly}
					/>
					<ActivityCard status={status} />
					<OpenNetworkCard
						form={form}
						openNetwork={openNetwork}
						nodeState={nodeState}
						onChange={handleChange}
						disabled={isReadOnly}
					/>
					<SharingExclusionsCard
						sharing={form.sharing}
						onChange={(sharing) => handleChange({ sharing })}
						disabled={isReadOnly}
					/>
				</>
			)}

			{validationError && hasChanges && (
				<div className="alert alert-warning alert-soft text-sm" role="alert">
					{validationError}
				</div>
			)}
			{saveError && (
				<div className="alert alert-error alert-soft text-sm" role="alert">
					{saveError}
				</div>
			)}

			<button
				type="button"
				className="btn btn-primary"
				onClick={handleSave}
				disabled={!hasChanges || isUpdating || isReadOnly || validationError !== null}
			>
				{isUpdating ? "Saving..." : "Save Changes"}
			</button>

			<p className="text-base-content/70 text-xs">
				Node, open network and sharing changes need Save. Private network actions below apply
				immediately.
			</p>

			{saved.enabled && nodeState === "running" && (
				<PrivateNetworksCard
					networks={(status?.networks ?? []).filter((n) => n.kind === "private")}
					acceptIncoming={saved.accept_incoming}
					isReadOnly={isReadOnly}
				/>
			)}
			{saved.enabled && nodeState === "loading" && (
				<p className="text-base-content/60 text-sm">Loading private networks...</p>
			)}
			{saved.enabled && nodeState === "stopped" && (
				<p className="text-base-content/60 text-sm">
					The node is not running, so private networks cannot be created or joined.
				</p>
			)}

			<BackfillFooter backfill={status?.backfill} />
		</div>
	);
}
