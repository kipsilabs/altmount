import { Plus, X } from "lucide-react";
import { useState } from "react";
import {
	checkNewPeerAddress,
	MAX_OPEN_PEERS,
	peerAddressNotice,
	validateOpenPeerAddress,
} from "./peernetValidation";

interface PeerAddressListProps {
	peers: string[];
	onChange: (peers: string[]) => void;
	disabled?: boolean;
}

export function PeerAddressList({ peers, onChange, disabled = false }: PeerAddressListProps) {
	const [input, setInput] = useState("");
	const [error, setError] = useState<string | null>(null);

	const handleAdd = () => {
		const result = checkNewPeerAddress(input, peers);
		if (result.error !== null) {
			setError(result.error);
			return;
		}
		onChange([...peers, result.address]);
		setInput("");
		setError(null);
	};

	return (
		<fieldset className="fieldset">
			<legend className="fieldset-legend">Peer addresses</legend>
			<div className="flex gap-2">
				<input
					type="text"
					className={`input w-full max-w-sm ${error ? "input-error" : ""}`}
					placeholder="host:port"
					aria-label="Peer address"
					aria-invalid={error !== null}
					aria-describedby="peer-address-help"
					value={input}
					disabled={disabled}
					onChange={(e) => {
						setInput(e.target.value);
						setError(null);
					}}
					onKeyDown={(e) => {
						if (e.key === "Enter") {
							e.preventDefault();
							handleAdd();
						}
					}}
				/>
				<button
					type="button"
					className="btn btn-outline"
					onClick={handleAdd}
					disabled={disabled || input.trim() === ""}
				>
					<Plus className="h-4 w-4" aria-hidden="true" />
					Add
				</button>
			</div>
			<p id="peer-address-help" className={`label whitespace-normal ${error ? "text-error" : ""}`}>
				{error ??
					`host:port without scheme or spaces. Up to ${MAX_OPEN_PEERS} (${peers.length} used). A fresh install knows nobody until you add one.`}
			</p>
			<ul className="mt-1 flex flex-wrap gap-2">
				{peers.map((peer) => {
					const peerError = validateOpenPeerAddress(peer);
					const notice = peerError === null ? peerAddressNotice(peer) : null;
					return (
						<li key={peer} className="flex max-w-full flex-col gap-1">
							<span
								className={`badge h-auto gap-1 py-1 font-mono text-xs ${
									peerError ? "badge-error" : notice ? "badge-warning" : "badge-neutral"
								}`}
							>
								<span className="max-w-64 truncate" title={peer}>
									{peer}
								</span>
								<button
									type="button"
									className="cursor-pointer"
									aria-label={`Remove ${peer}`}
									disabled={disabled}
									onClick={() => onChange(peers.filter((p) => p !== peer))}
								>
									<X className="h-3 w-3" aria-hidden="true" />
								</button>
							</span>
							{(peerError ?? notice) && (
								<span
									className={`max-w-72 whitespace-normal text-xs ${peerError ? "text-error" : "text-warning"}`}
								>
									{peerError ?? notice}
								</span>
							)}
						</li>
					);
				})}
			</ul>
		</fieldset>
	);
}
