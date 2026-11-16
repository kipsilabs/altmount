import { Plus, Trash2 } from "lucide-react";
import { useState } from "react";
import {
	useCreatePeernetInvite,
	useDeletePeernetInvite,
	usePeernetInvites,
} from "../../../hooks/usePeernet";
import { LoadingSpinner } from "../../ui/LoadingSpinner";
import { InviteLinkModal } from "./InviteLinkModal";
import { MutationErrorAlert } from "./MutationErrorAlert";
import { PlainText } from "./PlainText";
import { canRevokeInvite, formatTimestamp, inviteStatus, shortNodeId } from "./peernetFormat";
import {
	DEFAULT_INVITE_TTL_HOURS,
	MAX_ALIAS_LENGTH,
	MAX_INVITE_TTL_HOURS,
	MIN_INVITE_TTL_HOURS,
	validateInviteTtl,
} from "./peernetValidation";

interface InvitesPanelProps {
	networkId: string;
	disabled?: boolean;
}

export function InvitesPanel({ networkId, disabled = false }: InvitesPanelProps) {
	const { data, isLoading, error } = usePeernetInvites(networkId);
	const create = useCreatePeernetInvite(networkId);
	const revoke = useDeletePeernetInvite(networkId);
	const [ttl, setTtl] = useState(DEFAULT_INVITE_TTL_HOURS);
	const [alias, setAlias] = useState("");
	const [link, setLink] = useState<string | null>(null);
	const ttlError = validateInviteTtl(ttl);
	const ttlValid = ttlError === null;

	const handleCreate = async () => {
		if (!ttlValid) return;
		try {
			const result = await create.mutateAsync({ ttl_hours: ttl, alias: alias.trim() });
			setLink(result.link);
			setAlias("");
			// The link now lives only in local state; drop it from the mutation.
			create.reset();
		} catch {
			// surfaced through create.error, which must stay visible
		}
	};

	return (
		<div className="space-y-2">
			<h5 className="font-semibold text-sm">Invites</h5>
			<div className="flex flex-wrap items-end gap-2">
				<label className="flex flex-col gap-1 text-xs">
					Valid for (hours)
					<input
						type="number"
						className="input input-sm w-28"
						min={MIN_INVITE_TTL_HOURS}
						max={MAX_INVITE_TTL_HOURS}
						aria-invalid={!ttlValid}
						value={Number.isNaN(ttl) ? "" : ttl}
						disabled={disabled}
						onChange={(e) => setTtl(e.target.valueAsNumber)}
					/>
				</label>
				<label className="flex flex-col gap-1 text-xs">
					Alias (optional)
					<input
						type="text"
						className="input input-sm w-48"
						maxLength={MAX_ALIAS_LENGTH}
						value={alias}
						disabled={disabled}
						onChange={(e) => setAlias(e.target.value)}
					/>
				</label>
				<button
					type="button"
					className="btn btn-primary btn-sm"
					disabled={disabled || !ttlValid || create.isPending}
					onClick={handleCreate}
				>
					<Plus className="h-4 w-4" aria-hidden="true" />
					Create invite
				</button>
			</div>
			{!ttlValid && <p className="text-error text-xs">{ttlError}</p>}
			<MutationErrorAlert error={create.error} />
			<MutationErrorAlert error={revoke.error} />

			{isLoading && <LoadingSpinner size="sm" />}
			{error && <MutationErrorAlert error={error} />}
			{data && data.length === 0 && <p className="text-base-content/60 text-sm">No invites.</p>}
			{data && data.length > 0 && (
				<div className="overflow-x-auto">
					<table className="table-sm table">
						<thead>
							<tr>
								<th>Alias</th>
								<th>Created</th>
								<th>Expires</th>
								<th>Status</th>
								<th>Redeemed</th>
								<th>
									<span className="sr-only">Actions</span>
								</th>
							</tr>
						</thead>
						<tbody>
							{data.map((invite) => {
								const status = inviteStatus(invite);
								return (
									<tr key={invite.id}>
										<td>
											<PlainText value={invite.alias} />
										</td>
										<td className="whitespace-nowrap text-xs">
											{formatTimestamp(invite.created_at)}
										</td>
										<td className="whitespace-nowrap text-xs">
											{formatTimestamp(invite.expires_at)}
										</td>
										<td>
											<span
												className={`badge badge-sm ${
													status === "used"
														? "badge-success"
														: status === "expired"
															? "badge-ghost"
															: "badge-info"
												}`}
											>
												{status === "used" ? "Used" : status === "expired" ? "Expired" : "Unused"}
											</span>
										</td>
										<td className="text-xs">
											{invite.used_at ? (
												<>
													{formatTimestamp(invite.used_at)}{" "}
													<span className="font-mono" title={invite.used_by}>
														{shortNodeId(invite.used_by)}
													</span>
												</>
											) : (
												"-"
											)}
										</td>
										<td>
											{canRevokeInvite(invite) && (
												<button
													type="button"
													className="btn btn-ghost btn-xs text-error"
													aria-label="Revoke invite"
													title="Revoke invite"
													disabled={disabled || revoke.isPending}
													onClick={() => revoke.mutate(invite.id)}
												>
													<Trash2 className="h-4 w-4" aria-hidden="true" />
												</button>
											)}
										</td>
									</tr>
								);
							})}
						</tbody>
					</table>
				</div>
			)}
			<InviteLinkModal link={link} onClose={() => setLink(null)} />
		</div>
	);
}
