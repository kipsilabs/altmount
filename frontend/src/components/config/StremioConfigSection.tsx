import { AlertTriangle, RotateCcw, Save, Tv } from "lucide-react";
import { useEffect, useState } from "react";
import type {
	ConfigResponse,
	ProwlarrConfig,
	StreamScoringConfig,
	StremioConfig,
	StremioIndexersConfig,
} from "../../types/config";
import { LoadingSpinner } from "../ui/LoadingSpinner";
import { FilterExclusionsCard } from "./stremio/FilterExclusionsCard";
import { IndexersConfigCard } from "./stremio/IndexersConfigCard";
import { ReleaseScoreSandbox } from "./stremio/ReleaseScoreSandbox";
import { StreamDisplayCard } from "./stremio/StreamDisplayCard";
import { StreamRoutingCard } from "./stremio/StreamRoutingCard";
import { StremioActionBar } from "./stremio/StremioActionBar";
import { getDefaultScoringConfig, hydrateScoringFromProwlarr } from "./stremio/scoringPresets";
import { TrashScoringCard } from "./stremio/TrashScoringCard";

interface StremioConfigSectionProps {
	config: ConfigResponse;
	onUpdate?: (section: string, data: StremioConfig) => Promise<void>;
	isReadOnly?: boolean;
	isUpdating?: boolean;
}

const DEFAULT_PROWLARR: ProwlarrConfig = {
	enabled: false,
	host: "",
	api_key: "",
	categories: [2000, 2010, 2030, 2040, 2045, 2060, 5000, 5010, 5030, 5040],
	indexers: [],
	preferred_indexers: [],
	preferred_indexer_names: [],
	languages: [],
	preferred_languages: [],
	qualities: [],
	exclude_keywords: [],
	custom_scores: {},
};

function initializeStremioFormData(config: ConfigResponse): StremioConfig {
	const stremio = config.stremio;
	const prowlarr = stremio?.prowlarr ?? DEFAULT_PROWLARR;

	const scoring = hydrateScoringFromProwlarr(stremio?.scoring, prowlarr);
	scoring.custom_formats = scoring.custom_formats.map((format) => ({
		...format,
		patternType: format.patternType ?? format.pattern_type ?? "regex",
		isCustom: format.isCustom ?? format.is_custom ?? false,
	}));
	const indexers: StremioIndexersConfig = stremio?.indexers || {
		provider: "prowlarr",
		user_agent_mode: "auto",
		custom_user_agent: "",
		prowlarr: prowlarr,
		newsnab: [],
	};

	return {
		enabled: stremio?.enabled ?? false,
		addon_name: stremio?.addon_name ?? "AltMount Usenet",
		addon_description: stremio?.addon_description ?? "Stream from Usenet via Prowlarr",
		base_url: stremio?.base_url ?? "",
		direct_stream: stremio?.direct_stream ?? true,
		show_cached_indicator: stremio?.show_cached_indicator ?? true,
		display: stremio?.display ?? { preset: "default" },
		fallback_timeout_ms: stremio?.fallback_timeout_ms ?? 3500,
		max_retries: stremio?.max_retries ?? 2,
		stream_ttl_seconds: stremio?.stream_ttl_seconds ?? 86400,
		nzb_ttl_hours: stremio?.nzb_ttl_hours ?? 24,
		nzb_grab_cache_hours: stremio?.nzb_grab_cache_hours ?? 24,
		failed_release_ttl_hours: stremio?.failed_release_ttl_hours ?? 24,
		max_fallback_releases: stremio?.max_fallback_releases ?? 2,
		fast_fail_header_only: stremio?.fast_fail_header_only ?? true,
		skip_repair: stremio?.skip_repair ?? false,
		include_library_streams: stremio?.include_library_streams ?? true,
		skip_search_when_cached: stremio?.skip_search_when_cached ?? false,
		indexers,
		scoring,
		prowlarr: {
			...DEFAULT_PROWLARR,
			...prowlarr,
			exclude_keywords: scoring.exclude_keywords || prowlarr.exclude_keywords || [],
			preferred_languages: scoring.preferred_languages || prowlarr.preferred_languages || [],
		},
	};
}

export function StremioConfigSection({
	config,
	onUpdate,
	isReadOnly = false,
	isUpdating = false,
}: StremioConfigSectionProps) {
	const [formData, setFormData] = useState<StremioConfig>(() => initializeStremioFormData(config));
	const [initialSnapshot, setInitialSnapshot] = useState<string>(() =>
		JSON.stringify(initializeStremioFormData(config)),
	);
	const [hasChanges, setHasChanges] = useState(false);
	const [saveError, setSaveError] = useState<string | null>(null);

	// Sync from external config updates (e.g. reload)
	useEffect(() => {
		const initialized = initializeStremioFormData(config);
		setFormData(initialized);
		setInitialSnapshot(JSON.stringify(initialized));
		setHasChanges(false);
		setSaveError(null);
	}, [config]);

	const updateFormData = (patch: Partial<StremioConfig>) => {
		setFormData((prev) => {
			const updated = { ...prev, ...patch };
			const isDirty = JSON.stringify(updated) !== initialSnapshot;
			setHasChanges(isDirty);
			return updated;
		});
	};

	const handleScoringChange = (updatedScoring: StreamScoringConfig) => {
		// Sync custom scores and keywords to prowlarr config for backend consistency
		const customScores: Record<string, number> = {};
		for (const format of updatedScoring.custom_formats || []) {
			if (format.enabled && format.pattern) {
				customScores[format.pattern] = format.score;
			}
		}

		setFormData((prev) => {
			const updated: StremioConfig = {
				...prev,
				scoring: updatedScoring,
				prowlarr: {
					...prev.prowlarr,
					exclude_keywords: [...(updatedScoring.exclude_keywords || [])],
					preferred_languages: [...(updatedScoring.preferred_languages || [])],
					custom_scores: customScores,
				},
			};
			const isDirty = JSON.stringify(updated) !== initialSnapshot;
			setHasChanges(isDirty);
			return updated;
		});
	};

	const handleRevert = () => {
		const restored = JSON.parse(initialSnapshot) as StremioConfig;
		setFormData(restored);
		setHasChanges(false);
		setSaveError(null);
	};

	const handleSave = async () => {
		if (!onUpdate || !hasChanges) return;
		setSaveError(null);
		try {
			// Save custom formats cache locally
			if (typeof window !== "undefined" && formData.scoring?.custom_formats) {
				try {
					localStorage.setItem(
						"altmount_stremio_custom_formats",
						JSON.stringify(formData.scoring.custom_formats),
					);
					localStorage.setItem("altmount_stremio_preset", formData.scoring?.preset || "custom");
				} catch {
					// ignore
				}
			}

			// Build synchronized payload
			const customScores: Record<string, number> = {};
			if (formData.scoring?.custom_formats) {
				for (const format of formData.scoring.custom_formats) {
					if (format.enabled && format.pattern) {
						customScores[format.pattern] = format.score;
					}
				}
			}

			const prowlarrSynced = {
				...(formData.indexers?.prowlarr || formData.prowlarr),
				exclude_keywords:
					formData.scoring?.exclude_keywords || formData.prowlarr.exclude_keywords || [],
				preferred_languages:
					formData.scoring?.preferred_languages || formData.prowlarr.preferred_languages || [],
				custom_scores: customScores,
			};

			const payload: StremioConfig = {
				...formData,
				indexers: {
					...formData.indexers,
					provider: formData.indexers?.provider || "prowlarr",
					prowlarr: prowlarrSynced,
					newsnab: formData.indexers?.newsnab || [],
				},
				prowlarr: prowlarrSynced,
			};

			await onUpdate("stremio", payload);
			setInitialSnapshot(JSON.stringify(payload));
			setHasChanges(false);
		} catch (e) {
			setSaveError(e instanceof Error ? e.message : "Failed to save Stremio configuration");
		}
	};

	const scoringConfig = formData.scoring || getDefaultScoringConfig();

	return (
		<div className="mx-auto min-w-0 max-w-4xl space-y-6">
			{/* Page Header */}
			<div className="rounded-2xl border-2 border-base-300/80 bg-base-200/60 p-6">
				<div className="flex items-center gap-3">
					<div className="flex h-10 w-10 items-center justify-center rounded-xl bg-primary text-primary-content shadow-sm">
						<Tv className="h-5 w-5" />
					</div>
					<div>
						<h2 className="font-extrabold text-lg uppercase tracking-tight">
							Stremio Addon Integration
						</h2>
						<p className="text-base-content/60 text-xs">
							Configure Stremio streaming endpoints, search indexers, TRaSH release scoring, and
							stream filters.
						</p>
					</div>
				</div>
			</div>

			{/* 1. Addon Endpoint & Quick Actions */}
			<StremioActionBar
				enabled={formData.enabled}
				baseUrl={formData.base_url || ""}
				downloadKey={config.download_key}
				onToggleEnabled={(enabled) => updateFormData({ enabled })}
				isReadOnly={isReadOnly}
			/>

			{/* 2. Search Providers & Indexer Dispatch (Prowlarr + Direct Newsnab + User-Agents) */}
			<IndexersConfigCard config={formData} onChange={updateFormData} isReadOnly={isReadOnly} />

			{/* 3. Stream Delivery & Playback Pipeline */}
			<StreamRoutingCard config={formData} onChange={updateFormData} isReadOnly={isReadOnly} />

			{/* 3b. How releases are labelled in Stremio */}
			<StreamDisplayCard config={formData} onChange={updateFormData} isReadOnly={isReadOnly} />

			{/* 4. TRaSH Custom Format Scoring & Quality Profiles */}
			<TrashScoringCard
				scoringConfig={scoringConfig}
				onChange={handleScoringChange}
				isReadOnly={isReadOnly}
			/>

			{/* 5. Exclude Keywords & Audio Language Matrix */}
			<FilterExclusionsCard
				scoringConfig={scoringConfig}
				onChange={handleScoringChange}
				isReadOnly={isReadOnly}
			/>

			{/* 6. Real-Time Release Scoring Sandbox */}
			<ReleaseScoreSandbox scoringConfig={scoringConfig} />

			{/* Save Error Alert */}
			{saveError && (
				<div className="alert alert-error text-xs shadow-lg">
					<AlertTriangle className="h-4 w-4 shrink-0" />
					<span>{saveError}</span>
				</div>
			)}

			{/* Sticky Save / Unsaved Changes Floating Bar */}
			{hasChanges && (
				<div className="sticky bottom-6 z-30 flex items-center justify-between gap-4 rounded-2xl border-2 border-primary/40 bg-base-100/95 p-4 shadow-2xl backdrop-blur-md">
					<div className="flex items-center gap-3">
						<span className="relative flex h-3 w-3">
							<span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-warning opacity-75" />
							<span className="relative inline-flex h-3 w-3 rounded-full bg-warning" />
						</span>
						<div>
							<p className="font-bold text-base-content text-sm">Unsaved Stremio Changes</p>
							<p className="text-base-content/60 text-xs">
								You have uncommitted modifications to your Stremio or Indexer configuration.
							</p>
						</div>
					</div>

					<div className="flex items-center gap-2">
						<button
							type="button"
							onClick={handleRevert}
							disabled={isUpdating || isReadOnly}
							className="btn btn-ghost btn-sm gap-1.5"
						>
							<RotateCcw className="h-4 w-4" />
							<span>Discard</span>
						</button>

						<button
							type="button"
							onClick={handleSave}
							disabled={isUpdating || isReadOnly}
							className="btn btn-primary btn-sm gap-2 shadow-sm"
						>
							{isUpdating ? <LoadingSpinner size="sm" /> : <Save className="h-4 w-4" />}
							<span>Save Settings</span>
						</button>
					</div>
				</div>
			)}
		</div>
	);
}
