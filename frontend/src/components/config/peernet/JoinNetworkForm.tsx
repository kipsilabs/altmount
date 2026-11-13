import { Eye, EyeOff, LogIn } from "lucide-react";
import { useId, useState } from "react";
import { useJoinPeernetNetwork } from "../../../hooks/usePeernet";
import { MutationErrorAlert } from "./MutationErrorAlert";
import { validateJoinLinkHost } from "./peernetValidation";

interface JoinNetworkFormProps {
	disabled?: boolean;
}

export function JoinNetworkForm({ disabled = false }: JoinNetworkFormProps) {
	const [link, setLink] = useState("");
	const [showLink, setShowLink] = useState(false);
	const [allowPrivate, setAllowPrivate] = useState(false);
	const join = useJoinPeernetNetwork();
	const trimmed = link.trim();
	const allowPrivateId = useId();
	const linkError = validateJoinLinkHost(trimmed);

	const handleSubmit = (e: React.FormEvent) => {
		e.preventDefault();
		if (disabled || trimmed === "" || linkError !== null) return;
		join.mutate(
			{ link: trimmed, allow_private_addresses: allowPrivate },
			{
				onSuccess: () => {
					setLink("");
					setShowLink(false);
					setAllowPrivate(false);
					join.reset();
				},
			},
		);
	};

	return (
		<form className="space-y-2 rounded-box border border-base-300 p-3" onSubmit={handleSubmit}>
			<fieldset className="fieldset">
				<legend className="fieldset-legend">Join network</legend>
				<div className="flex gap-2">
					<input
						type={showLink ? "text" : "password"}
						className="input w-full font-mono text-xs"
						placeholder="Paste invite link"
						aria-label="Invite link"
						autoComplete="new-password"
						data-1p-ignore
						data-lpignore="true"
						name="peernet-invite-link"
						spellCheck={false}
						value={link}
						disabled={disabled}
						onChange={(e) => setLink(e.target.value)}
					/>
					<button
						type="button"
						className="btn btn-ghost btn-square"
						aria-label={showLink ? "Hide invite link" : "Show invite link"}
						aria-pressed={showLink}
						onClick={() => setShowLink((v) => !v)}
					>
						{showLink ? (
							<EyeOff className="h-4 w-4" aria-hidden="true" />
						) : (
							<Eye className="h-4 w-4" aria-hidden="true" />
						)}
					</button>
					<button
						type="submit"
						className="btn btn-primary"
						disabled={disabled || trimmed === "" || linkError !== null || join.isPending}
					>
						<LogIn className="h-4 w-4" aria-hidden="true" />
						Join
					</button>
				</div>
				<p className="label whitespace-normal">The invite link is a secret. Do not share it.</p>
				{linkError && (
					<p className="label whitespace-normal text-error" role="alert">
						{linkError}
					</p>
				)}
			</fieldset>
			<label htmlFor={allowPrivateId} className="label cursor-pointer justify-start gap-3">
				<input
					id={allowPrivateId}
					type="checkbox"
					className="checkbox checkbox-sm"
					checked={allowPrivate}
					disabled={disabled}
					onChange={(e) => setAllowPrivate(e.target.checked)}
				/>
				<span className="label-text whitespace-normal text-xs">
					Allow private network addresses (LAN). Only for networks you trust. Loopback (127.0.0.1,
					localhost) is never allowed.
				</span>
			</label>
			<MutationErrorAlert error={join.error} />
		</form>
	);
}
