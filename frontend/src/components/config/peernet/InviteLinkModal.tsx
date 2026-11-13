import { useEffect, useRef } from "react";
import { CopyButton } from "./CopyButton";

interface InviteLinkModalProps {
	link: string | null;
	onClose: () => void;
}

// Shows a one-time invite link. The link lives only in the parent's transient
// state and is dropped when the modal closes.
export function InviteLinkModal({ link, onClose }: InviteLinkModalProps) {
	const ref = useRef<HTMLDialogElement>(null);

	useEffect(() => {
		const dialog = ref.current;
		if (!dialog) return;
		if (link && !dialog.open) dialog.showModal();
		if (!link && dialog.open) dialog.close();
	}, [link]);

	return (
		<dialog ref={ref} className="modal" onClose={onClose} aria-labelledby="invite-link-title">
			<div className="modal-box">
				<h3 id="invite-link-title" className="font-bold text-lg">
					Invite link
				</h3>
				<p className="py-2 text-sm">
					Copy this link now. It cannot be shown again. Anyone with the link can join once.
				</p>
				<textarea
					className="textarea w-full font-mono text-xs"
					rows={4}
					readOnly
					aria-label="Invite link"
					value={link ?? ""}
					onFocus={(e) => e.currentTarget.select()}
				/>
				<div className="modal-action">
					<CopyButton value={link ?? ""} label="Copy invite link" withText />
					<button type="button" className="btn btn-primary" onClick={onClose}>
						Done
					</button>
				</div>
			</div>
			<form method="dialog" className="modal-backdrop">
				<button type="submit" aria-label="Close">
					close
				</button>
			</form>
		</dialog>
	);
}
