import { Plus } from "lucide-react";
import { useState } from "react";
import { useCreatePeernetNetwork } from "../../../hooks/usePeernet";
import { MutationErrorAlert } from "./MutationErrorAlert";
import { MAX_NAME_LENGTH } from "./peernetValidation";

interface CreateNetworkFormProps {
	acceptIncoming: boolean;
	disabled?: boolean;
}

export function CreateNetworkForm({ acceptIncoming, disabled = false }: CreateNetworkFormProps) {
	const [name, setName] = useState("");
	const create = useCreatePeernetNetwork();
	const trimmed = name.trim();
	const blocked = disabled || !acceptIncoming;

	const handleSubmit = (e: React.FormEvent) => {
		e.preventDefault();
		if (blocked || trimmed === "") return;
		create.mutate({ name: trimmed }, { onSuccess: () => setName("") });
	};

	return (
		<form className="space-y-2 rounded-box border border-base-300 p-3" onSubmit={handleSubmit}>
			<fieldset className="fieldset">
				<legend className="fieldset-legend">Create network</legend>
				<div className="flex gap-2">
					<input
						type="text"
						className="input w-full"
						placeholder="Network name"
						aria-label="Network name"
						maxLength={MAX_NAME_LENGTH}
						value={name}
						disabled={blocked}
						onChange={(e) => setName(e.target.value)}
					/>
					<button
						type="submit"
						className="btn btn-primary"
						disabled={blocked || trimmed === "" || create.isPending}
					>
						<Plus className="h-4 w-4" aria-hidden="true" />
						Create
					</button>
				</div>
				<p className="label whitespace-normal">
					{acceptIncoming
						? "This instance becomes the host of the new network."
						: "Turn on 'Accept incoming connections' and save to host a private network."}
				</p>
			</fieldset>
			<MutationErrorAlert error={create.error} />
		</form>
	);
}
