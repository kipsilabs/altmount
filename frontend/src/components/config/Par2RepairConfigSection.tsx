import { FlaskConical } from "lucide-react";
import { useEffect, useState } from "react";
import type { ConfigResponse, Par2RepairConfig } from "../../types/config";

interface Par2RepairConfigSectionProps {
	config: ConfigResponse;
	onUpdate?: (section: string, data: Par2RepairConfig) => Promise<void>;
	isReadOnly?: boolean;
	isUpdating?: boolean;
}

// ratioToPercent renders the stored fraction as a percentage, rounded to two
// decimals so float noise (0.020000000000000004) never reaches the input.
function ratioToPercent(ratio: number | undefined): number {
	return Math.round((ratio ?? 0.02) * 10000) / 100;
}

// sliderPos maps a percentage value to its position (0–100) on the slider's
// linear 0.5–25 track, so scale labels line up with where the thumb lands.
const SLIDER_MIN = 0.5;
const SLIDER_MAX = 25;
function sliderPos(value: number): number {
	return ((value - SLIDER_MIN) / (SLIDER_MAX - SLIDER_MIN)) * 100;
}

const defaults: Par2RepairConfig = {
	enabled: false,
	max_repair_ratio: 0.02,
	max_memory_mb: 256,
	max_concurrent_jobs: 1,
	max_connections: 10,
	min_release_size_mb: 0,
	max_release_size_mb: 0,
	max_patch_store_mb: 0,
	patch_dir: "",
	arr_first: true,
	repair_on_import: false,
};

export function Par2RepairConfigSection({
	config,
	onUpdate,
	isReadOnly,
	isUpdating,
}: Par2RepairConfigSectionProps) {
	const [data, setData] = useState<Par2RepairConfig>({ ...defaults, ...config.par2_repair });
	const [hasChanges, setHasChanges] = useState(false);

	useEffect(() => {
		setData({ ...defaults, ...config.par2_repair });
		setHasChanges(false);
	}, [config.par2_repair]);

	const handleChange = <K extends keyof Par2RepairConfig>(field: K, value: Par2RepairConfig[K]) => {
		const next = { ...data, [field]: value };
		setData(next);
		setHasChanges(JSON.stringify(next) !== JSON.stringify({ ...defaults, ...config.par2_repair }));
	};

	const handleSave = async () => {
		if (!onUpdate || !hasChanges) return;
		await onUpdate("par2_repair", data);
		setHasChanges(false);
	};

	return (
		<div className="space-y-6">
			<div className="alert alert-warning">
				<FlaskConical className="h-5 w-5 shrink-0" aria-hidden="true" />
				<div className="text-sm">
					<span className="badge badge-warning badge-sm mr-2 font-semibold">Beta</span>
					PAR2 repair is new and off by default. Repairs run in the background and download a full
					release each time, so enable it deliberately and keep an eye on the Health page.
				</div>
			</div>

			<div className="alert alert-info">
				<div className="text-sm">
					When a stream hits a missing (taken-down) article, AltMount reconstructs the missing bytes
					in the background from the release's PAR2 recovery files and serves them byte-exact from
					then on. A repair streams the whole release once over the import connection lane; repaired
					data is stored in the patch directory below and survives restarts.
				</div>
			</div>

			<fieldset className="fieldset">
				<legend className="fieldset-legend">Enable PAR2 Repair</legend>
				<label className="label cursor-pointer justify-start gap-3">
					<input
						type="checkbox"
						className="toggle toggle-primary"
						checked={data.enabled ?? false}
						disabled={isReadOnly}
						onChange={(e) => handleChange("enabled", e.target.checked)}
					/>
					<span className="label-text">
						Repair missing articles automatically (playback holes, health checks)
					</span>
				</label>
			</fieldset>

			<fieldset className="fieldset">
				<legend className="fieldset-legend">PAR2 Fallback for ARR Repair</legend>
				<label className="label cursor-pointer justify-start gap-3">
					<input
						type="checkbox"
						className="toggle toggle-primary"
						checked={data.arr_first ?? true}
						disabled={isReadOnly || !(data.enabled ?? false)}
						onChange={(e) => handleChange("arr_first", e.target.checked)}
					/>
					<span className="label-text">
						Repair corrupted files with PAR2 when nothing is found in the ARRs
					</span>
				</label>
				<p className="label whitespace-normal">
					Corrupted files still go to Radarr/Sonarr first, exactly as before. When the ARRs come up
					empty — no instance tracks the file, none is configured, or ARR repair is disabled — a
					PAR2 repair is queued as the fallback instead of giving up. Degraded files and playback
					holes always repair via PAR2 directly.
				</p>
			</fieldset>

			<fieldset className="fieldset">
				<legend className="fieldset-legend">Repair on Import</legend>
				<label className="label cursor-pointer justify-start gap-3">
					<input
						type="checkbox"
						className="toggle toggle-primary"
						checked={data.repair_on_import ?? false}
						disabled={isReadOnly || !(data.enabled ?? false)}
						onChange={(e) => handleChange("repair_on_import", e.target.checked)}
					/>
					<span className="label-text">
						Repair damaged files as soon as they import, instead of waiting for playback
					</span>
				</label>
				<p className="label whitespace-normal">
					A release's PAR2 volumes are most likely to still be retrievable close to its post date,
					so repairing at import can save files that would be unrepairable months later. Off by
					default: each repair downloads the full release, so importing a large damaged backlog is
					expensive.
				</p>
			</fieldset>

			<div className="grid grid-cols-1 gap-6 sm:grid-cols-2">
				<fieldset className="fieldset sm:col-span-2">
					<legend className="fieldset-legend">Max Repair Percentage</legend>
					<div className="flex items-center justify-between">
						<span className="text-base-content/60 text-xs">
							Share of a file's bytes a repair may reconstruct
						</span>
						<div className="font-black font-mono text-lg text-primary">
							{ratioToPercent(data.max_repair_ratio)}%
						</div>
					</div>
					<div className="space-y-2">
						<input
							type="range"
							min={SLIDER_MIN}
							max={SLIDER_MAX}
							step="0.5"
							value={ratioToPercent(data.max_repair_ratio)}
							className="range range-primary range-sm w-full"
							disabled={isReadOnly}
							onChange={(e) => handleChange("max_repair_ratio", Number(e.target.value) / 100)}
						/>
						<div className="relative h-4 font-black text-base-content/50 text-xs">
							{/* Labels sit at their true positions on the linear 0.5–25 scale.
							    The 2% label is left-anchored (its left edge marks the spot) so
							    it never spills past the track's start. */}
							<span className="absolute" style={{ left: `${sliderPos(2)}%` }}>
								2% (DEFAULT)
							</span>
							<span className="-translate-x-1/2 absolute" style={{ left: `${sliderPos(10)}%` }}>
								10% (TYPICAL PAR2)
							</span>
							<span className="absolute right-0">25%</span>
						</div>
					</div>
					<p className="label whitespace-normal">
						2% matches the zero-fill tolerance; posts rarely carry more than 10% recovery data, and
						the release's PAR2 redundancy is always the hard ceiling.
					</p>
				</fieldset>

				<fieldset className="fieldset">
					<legend className="fieldset-legend">Max Memory (MB)</legend>
					<input
						type="number"
						className="input input-bordered w-full max-w-48 bg-base-100 font-mono text-sm"
						min={16}
						value={data.max_memory_mb ?? 256}
						disabled={isReadOnly}
						onChange={(e) => handleChange("max_memory_mb", Number(e.target.value))}
					/>
					<p className="label whitespace-normal">
						Solver memory budget per job. Repairs needing more still run, spilling to disk instead
						of RAM.
					</p>
				</fieldset>

				<fieldset className="fieldset">
					<legend className="fieldset-legend">Max Concurrent Jobs</legend>
					<input
						type="number"
						className="input input-bordered w-full max-w-48 bg-base-100 font-mono text-sm"
						min={1}
						value={data.max_concurrent_jobs ?? 1}
						disabled={isReadOnly}
						onChange={(e) => handleChange("max_concurrent_jobs", Number(e.target.value))}
					/>
					<p className="label whitespace-normal">
						Each job downloads a full release; more than 1 competes for import bandwidth.
					</p>
				</fieldset>

				<fieldset className="fieldset">
					<legend className="fieldset-legend">Max Connections</legend>
					<input
						type="number"
						className="input input-bordered w-full max-w-48 bg-base-100 font-mono text-sm"
						min={1}
						max={64}
						value={data.max_connections ?? 10}
						disabled={isReadOnly}
						onChange={(e) => handleChange("max_connections", Number(e.target.value))}
					/>
					<p className="label whitespace-normal">
						NNTP connections repair downloads may use (shared across jobs). A repair streams the
						whole release once, so this sets its speed; streaming playback keeps priority
						regardless. Applies live to running repairs.
					</p>
				</fieldset>

				<fieldset className="fieldset">
					<legend className="fieldset-legend">Min Release Size (MB)</legend>
					<input
						type="number"
						className="input input-bordered w-full max-w-48 bg-base-100 font-mono text-sm"
						min={0}
						value={data.min_release_size_mb ?? 0}
						disabled={isReadOnly}
						onChange={(e) => handleChange("min_release_size_mb", Number(e.target.value))}
					/>
					<p className="label whitespace-normal">
						Releases smaller than this are not repaired. 0 = no minimum.
					</p>
				</fieldset>

				<fieldset className="fieldset">
					<legend className="fieldset-legend">Max Release Size (MB)</legend>
					<input
						type="number"
						className="input input-bordered w-full max-w-48 bg-base-100 font-mono text-sm"
						min={0}
						value={data.max_release_size_mb ?? 0}
						disabled={isReadOnly}
						onChange={(e) => handleChange("max_release_size_mb", Number(e.target.value))}
					/>
					<p className="label whitespace-normal">
						Releases larger than this are not repaired — a repair downloads the whole release once.
						0 = no maximum.
					</p>
				</fieldset>

				<fieldset className="fieldset">
					<legend className="fieldset-legend">Patch Store Cap (MB)</legend>
					<input
						type="number"
						className="input input-bordered w-full max-w-48 bg-base-100 font-mono text-sm"
						min={0}
						value={data.max_patch_store_mb ?? 0}
						disabled={isReadOnly}
						onChange={(e) => handleChange("max_patch_store_mb", Number(e.target.value))}
					/>
					<p className="label whitespace-normal">
						Total size of stored repaired data; oldest patches are evicted first. 0 = unlimited.
						Evicted patches regenerate on the next playback.
					</p>
				</fieldset>

				<fieldset className="fieldset">
					<legend className="fieldset-legend">Patch Directory</legend>
					<input
						type="text"
						className="input input-bordered w-full max-w-md bg-base-100 font-mono text-sm"
						placeholder="<metadata_root>/patches"
						value={data.patch_dir ?? ""}
						disabled={isReadOnly}
						onChange={(e) => handleChange("patch_dir", e.target.value)}
					/>
					<p className="label whitespace-normal">
						Where repaired data and the solver's temporary scratch files are stored. Point it at a
						disk with room for large repairs. Empty uses the metadata folder. Applied on restart;
						existing patches are not moved.
					</p>
				</fieldset>
			</div>

			<button
				type="button"
				className="btn btn-primary"
				onClick={handleSave}
				disabled={!hasChanges || isUpdating || isReadOnly}
			>
				{isUpdating ? "Saving..." : "Save Changes"}
			</button>
		</div>
	);
}
