import { Database, Globe, Radio, Rocket, ShieldCheck, Zap } from "lucide-react";
import { useEffect, useState } from "react";
import type { StremioConfig } from "../../../types/config";

interface StreamRoutingCardProps {
	config: StremioConfig;
	onChange: (patch: Partial<StremioConfig>) => void;
	isReadOnly?: boolean;
}

export function StreamRoutingCard({
	config,
	onChange,
	isReadOnly = false,
}: StreamRoutingCardProps) {
	const directStream = config.direct_stream ?? true;
	const reuseLibrary = config.include_library_streams ?? true;
	const skipSearchWhenCached = config.skip_search_when_cached ?? false;
	const fallbackTimeout = config.fallback_timeout_ms ?? 3500;
	const nzbTtlHours = config.nzb_ttl_hours ?? 24;
	const grabCacheHours = config.nzb_grab_cache_hours ?? 24;
	const failedTtlHours = config.failed_release_ttl_hours ?? 24;
	const maxFallbacks = config.max_fallback_releases ?? 2;
	const fastFailHeader = config.fast_fail_header_only ?? true;
	const skipRepair = config.skip_repair ?? false;

	const [libraryCount, setLibraryCount] = useState<number | null>(null);

	useEffect(() => {
		fetch("/api/system/stats")
			.then((res) => (res.ok ? res.json() : null))
			.then((data) => {
				const total = data?.data?.health?.total_files ?? data?.data?.health?.total;
				if (typeof total === "number" && total > 0) {
					setLibraryCount(total);
				}
			})
			.catch(() => {});
	}, []);

	return (
		<div className="min-w-0 overflow-hidden rounded-2xl border-2 border-base-300/80 bg-base-200/60 p-6 shadow-sm">
			{/* Section Header */}
			<div className="flex items-center gap-2.5 border-base-300/50 border-b pb-4">
				<div className="flex h-8 w-8 items-center justify-center rounded-lg bg-primary/10 text-primary">
					<Rocket className="h-4 w-4" />
				</div>
				<div>
					<h3 className="font-bold text-base tracking-tight">
						Stream Delivery & Playback Pipeline
					</h3>
					<p className="text-base-content/60 text-xs">
						Configure stream routing, reverse proxy endpoints, timeouts and fallbacks
					</p>
				</div>
			</div>

			<div className="mt-5 space-y-6">
				{/* Public Base URL Override */}
				<fieldset className="fieldset min-w-0">
					<legend className="fieldset-legend font-semibold text-xs">
						Base Streaming URL (Host Override)
					</legend>
					<div className="relative">
						<div className="pointer-events-none absolute inset-y-0 left-0 flex items-center pl-3">
							<Globe className="h-4 w-4 text-base-content/40" />
						</div>
						<input
							type="url"
							className="input input-bordered w-full min-w-0 pl-9 font-mono text-xs"
							placeholder="https://altmount.mydomain.com"
							value={config.base_url ?? ""}
							disabled={isReadOnly}
							onChange={(e) => onChange({ base_url: e.target.value })}
						/>
					</div>
					<p className="label text-base-content/50 text-xs">
						Set an external domain if Altmount is running behind a reverse proxy (Caddy / Nginx /
						Cloudflare). Leave blank to auto-detect.
					</p>
				</fieldset>

				{/* Feature Toggles */}
				<div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
					<div className="flex items-start justify-between gap-3 rounded-xl border border-base-300 bg-base-100/70 p-4">
						<div className="min-w-0 flex-1">
							<div className="flex items-center gap-2">
								<Radio className="h-4 w-4 text-primary" />
								<span className="font-semibold text-sm">Direct NZB Tunneling</span>
							</div>
							<p className="mt-1 text-base-content/60 text-xs">
								Streams raw segments directly over WebDAV/HTTP without waiting for full download.
							</p>
						</div>
						<input
							type="checkbox"
							className="toggle toggle-primary shrink-0"
							checked={directStream}
							disabled={isReadOnly}
							onChange={(e) => onChange({ direct_stream: e.target.checked })}
						/>
					</div>

					<div className="flex items-start justify-between gap-3 rounded-xl border border-base-300 bg-base-100/70 p-4">
						<div className="min-w-0 flex-1">
							<div className="flex items-center gap-2">
								<Database className="h-4 w-4 text-secondary" />
								<span className="font-semibold text-sm">Reuse Library Releases</span>
							</div>
							<p className="mt-1 text-base-content/60 text-xs">
								Matches Stremio against{" "}
								{libraryCount
									? `all ${libraryCount.toLocaleString()} files`
									: "all completed files"}{" "}
								in your library (Sonarr, Radarr, SABnzbd) for instant 0s playback.
							</p>
						</div>
						<input
							type="checkbox"
							className="toggle toggle-primary shrink-0"
							checked={reuseLibrary}
							disabled={isReadOnly}
							onChange={(e) => onChange({ include_library_streams: e.target.checked })}
						/>
					</div>
				</div>

				<div className="flex items-start justify-between gap-3 rounded-xl border border-base-300 bg-base-100/70 p-4">
					<div className="min-w-0 flex-1">
						<div className="flex items-center gap-2">
							<Zap className="h-4 w-4 text-primary" />
							<label htmlFor="stremio-skip-search-when-cached" className="font-semibold text-sm">
								Skip Search When Cached
							</label>
						</div>
						<p className="mt-1 text-base-content/60 text-xs">
							Return matching cached streams without searching indexers. Searches normally when no
							cached streams are available. Disabled by default.
						</p>
					</div>
					<input
						id="stremio-skip-search-when-cached"
						type="checkbox"
						className="toggle toggle-primary shrink-0"
						checked={skipSearchWhenCached}
						disabled={isReadOnly}
						onChange={(e) => onChange({ skip_search_when_cached: e.target.checked })}
					/>
				</div>

				{/* Timeouts & TTL Controls */}
				<div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
					<fieldset className="fieldset min-w-0">
						<legend className="fieldset-legend font-semibold text-xs">
							Stream Fallback Timeout
						</legend>
						<div className="relative">
							<input
								type="number"
								className="input input-bordered w-full min-w-0 text-xs"
								min={500}
								step={250}
								value={fallbackTimeout}
								disabled={isReadOnly}
								onChange={(e) =>
									onChange({
										fallback_timeout_ms: Math.max(500, Number(e.target.value) || 3500),
									})
								}
							/>
							<span className="pointer-events-none absolute inset-y-0 right-0 flex items-center pr-3 text-base-content/40 text-xs">
								ms
							</span>
						</div>
						<p className="label whitespace-normal text-[11px] text-base-content/50">
							Max wait time before auto-switching to candidate release on stall.
						</p>
					</fieldset>

					<fieldset className="fieldset min-w-0">
						<legend className="fieldset-legend font-semibold text-xs">NZB Cache TTL</legend>
						<div className="relative">
							<input
								type="number"
								className="input input-bordered w-full min-w-0 text-xs"
								min={0}
								value={nzbTtlHours}
								disabled={isReadOnly}
								onChange={(e) =>
									onChange({
										nzb_ttl_hours: Math.max(0, Number(e.target.value) || 0),
									})
								}
							/>
							<span className="pointer-events-none absolute inset-y-0 right-0 flex items-center pr-3 text-base-content/40 text-xs">
								Hours
							</span>
						</div>
						<p className="label whitespace-normal text-[11px] text-base-content/50">
							Duration to retain parsed NZB streams. 0 = keep forever.
						</p>
					</fieldset>

					<fieldset className="fieldset min-w-0">
						<legend className="fieldset-legend font-semibold text-xs">Failed Release TTL</legend>
						<div className="relative">
							<input
								type="number"
								className="input input-bordered w-full min-w-0 text-xs"
								min={0}
								value={failedTtlHours}
								disabled={isReadOnly}
								onChange={(e) =>
									onChange({
										failed_release_ttl_hours: Math.max(0, Number(e.target.value) || 0),
									})
								}
							/>
							<span className="pointer-events-none absolute inset-y-0 right-0 flex items-center pr-3 text-base-content/40 text-xs">
								Hours
							</span>
						</div>
						<p className="label whitespace-normal text-[11px] text-base-content/50">
							Duration to hide dead / DMCAed NZB candidates from stream search.
						</p>
					</fieldset>

					<fieldset className="fieldset min-w-0">
						<legend className="fieldset-legend font-semibold text-xs">NZB Grab Cache</legend>
						<div className="relative">
							<input
								type="number"
								className="input input-bordered w-full min-w-0 text-xs"
								min={0}
								value={grabCacheHours}
								disabled={isReadOnly}
								onChange={(e) =>
									onChange({
										nzb_grab_cache_hours: Math.max(0, Number(e.target.value) || 0),
									})
								}
							/>
							<span className="pointer-events-none absolute inset-y-0 right-0 flex items-center pr-3 text-base-content/40 text-xs">
								Hours
							</span>
						</div>
						<p className="label whitespace-normal text-[11px] text-base-content/50">
							Reuse NZBs already grabbed from the same URL, sparing strict indexers. Completed
							imports only within NZB Cache TTL. 0 = no cache.
						</p>
					</fieldset>
				</div>

				{/* Fallback Releases & Fast Fail Header */}
				<div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
					<fieldset className="fieldset min-w-0">
						<legend className="fieldset-legend font-semibold text-xs">
							Max Fallback Candidates
						</legend>
						<input
							type="number"
							className="input input-bordered w-full min-w-0 text-xs"
							min={0}
							max={4}
							value={maxFallbacks}
							disabled={isReadOnly}
							onChange={(e) =>
								onChange({
									max_fallback_releases: Math.min(4, Math.max(0, Number(e.target.value) || 0)),
								})
							}
						/>
						<p className="label text-[11px] text-base-content/50">
							How many alternative NZB releases to try if the primary stream fails (0-4).
						</p>
					</fieldset>

					<div className="flex items-start justify-between gap-3 rounded-xl border border-base-300 bg-base-100/70 p-4">
						<div className="min-w-0 flex-1">
							<div className="flex items-center gap-2">
								<ShieldCheck className="h-4 w-4 text-success" />
								<span className="font-semibold text-sm">Fast-Fail Dead Header Probe</span>
							</div>
							<p className="mt-1 text-base-content/60 text-xs">
								Immediately abort DMCA / missing article NZBs on header probe without running a slow
								full archive sweep.
							</p>
						</div>
						<input
							type="checkbox"
							className="toggle toggle-primary shrink-0"
							checked={fastFailHeader}
							disabled={isReadOnly}
							onChange={(e) => onChange({ fast_fail_header_only: e.target.checked })}
						/>
					</div>

					<div className="flex items-start justify-between gap-3 rounded-xl border border-base-300 bg-base-100/70 p-4">
						<div className="min-w-0 flex-1">
							<div className="flex items-center gap-2">
								<Zap className="h-4 w-4 text-warning" />
								<span className="font-semibold text-sm">Don't Repair Stremio Downloads</span>
							</div>
							<p className="mt-1 text-base-content/60 text-xs">
								Fail a release that needs a PAR2 repair instead of waiting for one, so playback
								falls back to the next release straight away.
							</p>
						</div>
						<input
							type="checkbox"
							className="toggle toggle-primary shrink-0"
							checked={skipRepair}
							disabled={isReadOnly}
							onChange={(e) => onChange({ skip_repair: e.target.checked })}
						/>
					</div>
				</div>
			</div>
		</div>
	);
}
