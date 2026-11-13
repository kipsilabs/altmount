import { Plus, X } from "lucide-react";
import { useId, useState } from "react";
import { addTag } from "./peernetValidation";

interface TagListEditorProps {
	label: string;
	placeholder: string;
	tags: string[];
	onChange: (tags: string[]) => void;
	disabled?: boolean;
}

export function TagListEditor({
	label,
	placeholder,
	tags,
	onChange,
	disabled = false,
}: TagListEditorProps) {
	const [input, setInput] = useState("");
	const inputId = useId();

	const handleAdd = () => {
		onChange(addTag(tags, input));
		setInput("");
	};

	return (
		<fieldset className="fieldset">
			<legend className="fieldset-legend">{label}</legend>
			<div className="flex gap-2">
				<input
					id={inputId}
					type="text"
					className="input w-full max-w-sm"
					placeholder={placeholder}
					aria-label={label}
					value={input}
					disabled={disabled}
					onChange={(e) => setInput(e.target.value)}
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
			<div className="mt-2 flex flex-wrap gap-2">
				{tags.length === 0 && <span className="text-base-content/60 text-xs">None.</span>}
				{tags.map((tag) => (
					<span key={tag} className="badge badge-neutral gap-1">
						<span className="max-w-48 truncate" title={tag}>
							{tag}
						</span>
						<button
							type="button"
							className="cursor-pointer"
							aria-label={`Remove ${tag}`}
							disabled={disabled}
							onClick={() => onChange(tags.filter((t) => t !== tag))}
						>
							<X className="h-3 w-3" aria-hidden="true" />
						</button>
					</span>
				))}
			</div>
		</fieldset>
	);
}
