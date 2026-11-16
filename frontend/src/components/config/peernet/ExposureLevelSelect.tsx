import { ShieldAlert } from "lucide-react";
import { useEffect, useId, useRef, useState } from "react";
import type { PeernetLevel } from "../../../types/peernet";
import { levelNeedsConfirmation } from "./peernetStats";
import {
	EXPOSURE_LEVELS,
	LEVEL3_PRIVACY_NOTICE,
	levelNeedsPrivacyNotice,
	levelPrivacyNotice,
} from "./peernetValidation";

interface ShareBundlesConfirmProps {
	open: boolean;
	onConfirm: () => void;
	onCancel: () => void;
}

function ShareBundlesConfirm({ open, onConfirm, onCancel }: ShareBundlesConfirmProps) {
	const ref = useRef<HTMLDialogElement>(null);
	const titleId = useId();
	useEffect(() => {
		const dialog = ref.current;
		if (!dialog) return;
		if (open && !dialog.open) dialog.showModal();
		if (!open && dialog.open) dialog.close();
	}, [open]);
	return (
		<dialog ref={ref} className="modal" onClose={onCancel} aria-labelledby={titleId}>
			<div className="modal-box">
				<h3 id={titleId} className="font-bold text-lg">
					Share bundles?
				</h3>
				<p className="py-2 text-sm">{LEVEL3_PRIVACY_NOTICE}</p>
				<div className="modal-action">
					<button type="button" className="btn btn-ghost" onClick={onCancel}>
						Cancel
					</button>
					<button type="button" className="btn btn-primary" onClick={onConfirm}>
						Share bundles
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

interface ExposureLevelSelectProps {
	name: string;
	value: PeernetLevel;
	onChange: (level: PeernetLevel) => void;
	disabled?: boolean;
}

export function ExposureLevelSelect({
	name,
	value,
	onChange,
	disabled = false,
}: ExposureLevelSelectProps) {
	const uid = useId();
	const [confirming, setConfirming] = useState(false);
	const select = (level: PeernetLevel) => {
		if (levelNeedsConfirmation(value, level)) setConfirming(true);
		else onChange(level);
	};
	return (
		<fieldset className="fieldset">
			<legend className="fieldset-legend">Exposure level</legend>
			<div className="space-y-2">
				{EXPOSURE_LEVELS.map((info) => {
					const id = `${uid}-${name}-level-${info.level}`;
					const titleId = `${id}-title`;
					const descId = `${id}-desc`;
					return (
						<label
							key={info.level}
							htmlFor={id}
							className={`flex cursor-pointer items-start gap-3 rounded-box border p-3 ${value === info.level ? "border-primary bg-primary/5" : "border-base-300"}`}
						>
							<input
								id={id}
								type="radio"
								aria-labelledby={titleId}
								aria-describedby={descId}
								name={name}
								className="radio radio-primary radio-sm mt-1"
								checked={value === info.level}
								disabled={disabled}
								onChange={() => select(info.level)}
							/>
							<span className="min-w-0">
								<span id={titleId} className="font-medium text-sm">
									{info.level} - {info.name}
								</span>
								<span id={descId} className="block text-base-content/70 text-xs">
									{info.description}
								</span>
							</span>
						</label>
					);
				})}
			</div>
			{levelNeedsPrivacyNotice(value) && (
				<div className="alert alert-warning mt-2 text-sm" role="note">
					<ShieldAlert className="h-5 w-5 shrink-0" aria-hidden="true" />
					<span>{levelPrivacyNotice(value)}</span>
				</div>
			)}
			<ShareBundlesConfirm
				open={confirming}
				onCancel={() => setConfirming(false)}
				onConfirm={() => {
					setConfirming(false);
					onChange(3);
				}}
			/>
		</fieldset>
	);
}
