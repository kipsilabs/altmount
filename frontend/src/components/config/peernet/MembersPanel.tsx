import { UserMinus } from "lucide-react";
import { useState } from "react";
import { useDeletePeernetMember, usePeernetMembers } from "../../../hooks/usePeernet";
import type { PeernetMember } from "../../../types/peernet";
import { ConfirmModal } from "../../ui/ConfirmModal";
import { LoadingSpinner } from "../../ui/LoadingSpinner";
import { MutationErrorAlert } from "./MutationErrorAlert";
import { PlainText } from "./PlainText";
import { formatTimestamp } from "./peernetFormat";

interface MembersPanelProps {
	networkId: string;
	disabled?: boolean;
}

export function MembersPanel({ networkId, disabled = false }: MembersPanelProps) {
	const { data, isLoading, error } = usePeernetMembers(networkId);
	const remove = useDeletePeernetMember(networkId);
	const [target, setTarget] = useState<PeernetMember | null>(null);

	return (
		<div className="space-y-2">
			<h5 className="font-semibold text-sm">Members</h5>
			{isLoading && <LoadingSpinner size="sm" />}
			{error && <MutationErrorAlert error={error} />}
			<MutationErrorAlert error={remove.error} />
			{data && data.length === 0 && <p className="text-base-content/60 text-sm">No members yet.</p>}
			{data && data.length > 0 && (
				<div className="overflow-x-auto">
					<table className="table-sm table">
						<thead>
							<tr>
								<th>Member</th>
								<th>Joined</th>
								<th>Certificate expires</th>
								<th>Status</th>
								<th>
									<span className="sr-only">Actions</span>
								</th>
							</tr>
						</thead>
						<tbody>
							{data.map((member) => (
								<tr key={member.node_id}>
									<td>
										{member.alias !== "" && <PlainText value={member.alias} />}
										<div>
											<PlainText value={member.node_id} maxWidthClass="max-w-40" mono />
										</div>
									</td>
									<td className="whitespace-nowrap text-xs">{formatTimestamp(member.joined_at)}</td>
									<td className="whitespace-nowrap text-xs">
										{formatTimestamp(member.cert_expires_at)}
									</td>
									<td>
										{member.revoked_at ? (
											<span className="badge badge-error badge-sm">Removed</span>
										) : (
											<span className="badge badge-success badge-sm">Active</span>
										)}
									</td>
									<td>
										{!member.revoked_at && (
											<button
												type="button"
												className="btn btn-ghost btn-xs text-error"
												aria-label="Remove member"
												title="Remove member"
												disabled={disabled || remove.isPending}
												onClick={() => setTarget(member)}
											>
												<UserMinus className="h-4 w-4" aria-hidden="true" />
											</button>
										)}
									</td>
								</tr>
							))}
						</tbody>
					</table>
				</div>
			)}
			<ConfirmModal
				isOpen={target !== null}
				title="Remove member"
				message="The host stops renewing this member's certificate and revokes it. The node loses access to this network."
				confirmText="Remove member"
				confirmButtonClass="btn-error"
				onCancel={() => setTarget(null)}
				onConfirm={() => {
					if (target) remove.mutate(target.node_id);
					setTarget(null);
				}}
			/>
		</div>
	);
}
