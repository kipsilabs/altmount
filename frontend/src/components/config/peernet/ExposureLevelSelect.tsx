import { ShieldAlert } from "lucide-react";
import { useId } from "react";
import type { PeernetLevel } from "../../../types/peernet";
import { isLevelAvailable } from "./peernetStats";
import { EXPOSURE_LEVELS, levelNeedsPrivacyNotice, levelPrivacyNotice } from "./peernetValidation";

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
	return (
		<fieldset className="fieldset">
			<legend className="fieldset-legend">Exposure level</legend>
			<div className="space-y-2">
				{EXPOSURE_LEVELS.map((info) => {
					const id = `${uid}-${name}-level-${info.level}`;
					const titleId = `${id}-title`;
					const descId = `${id}-desc`;
					const unavailable = !isLevelAvailable(info.level);
					return (
						<label
							key={info.level}
							htmlFor={id}
							className={`flex items-start gap-3 rounded-box border p-3 ${
								unavailable ? "cursor-not-allowed opacity-60" : "cursor-pointer"
							} ${value === info.level ? "border-primary bg-primary/5" : "border-base-300"}`}
						>
							<input
								id={id}
								type="radio"
								aria-labelledby={titleId}
								aria-describedby={descId}
								name={name}
								className="radio radio-primary radio-sm mt-1"
								checked={value === info.level}
								disabled={disabled || unavailable}
								onChange={() => onChange(info.level)}
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
		</fieldset>
	);
}
