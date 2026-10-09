import {
	AlertCircle,
	ChevronDown,
	ChevronUp,
	Clock,
	Heart,
	HeartCrack,
	Loader,
	Wrench,
} from "lucide-react";
import { memo, useMemo, useState } from "react";
import { HealthBadge } from "../../../../components/ui/StatusBadge";
import {
	formatFutureTime,
	formatRelativeTime,
	parseHealthErrorDetails,
	truncateText,
} from "../../../../lib/utils";
import { type FileHealth, HealthPriority } from "../../../../types/api";
import { parseRepairReason } from "../par2RepairReason";
import { RepairFailureMessage } from "../RepairFailureMessage";
import { ArticleCheckProgress } from "./ArticleCheckProgress";
import { ContentVerificationBadge } from "./ContentVerificationBadge";
import { HealthItemActionsMenu } from "./HealthItemActionsMenu";
import { PartialCheckBadge } from "./PartialCheckBadge";
import { PlaybackImpactBadge } from "./PlaybackImpactBadge";

interface HealthItemCardProps {
	item: FileHealth;
	isSelected: boolean;
	onSelectChange: (filePath: string, checked: boolean) => void;
	onSetPriority: (id: number, priority: HealthPriority) => void;
	onCancelCheck: (id: number) => void;
	onManualCheck: (id: number, verifyContent?: boolean) => void;
	onRepair: (id: number) => void;
	onPar2Repair?: (filePath: string) => void;
	onDelete: (id: number) => void;
	onUnmask: (id: number) => void;
	onRegenerate?: (filePath: string) => void;
	isCancelPending: boolean;
	isDirectCheckPending: boolean;
	isRepairPending: boolean;
	isDeletePending: boolean;
	isUnmaskPending: boolean;
	isRegeneratePending?: boolean;
}

export const HealthItemCard = memo(function HealthItemCard({
	item,
	isSelected,
	onSelectChange,
	onSetPriority,
	onCancelCheck,
	onManualCheck,
	onRepair,
	onPar2Repair,
	onDelete,
	onUnmask,
	onRegenerate,
	isCancelPending,
	isDirectCheckPending,
	isRepairPending,
	isDeletePending,
	isUnmaskPending,
	isRegeneratePending,
}: HealthItemCardProps) {
	const [isExpanded, setIsExpanded] = useState(false);

	const errorDetails = useMemo(
		() => parseHealthErrorDetails(item.error_details),
		[item.error_details],
	);
	const playbackImpact = errorDetails?.playback_impact ?? null;
	const repairReason = item.last_error?.startsWith("par2repair:")
		? parseRepairReason(item.last_error)
		: null;

	// Reuse status icon logic from HealthTableRow
	const getNextPriority = (current: HealthPriority): HealthPriority => {
		switch (current) {
			case HealthPriority.Normal:
				return HealthPriority.High;
			case HealthPriority.High:
				return HealthPriority.Next;
			case HealthPriority.Next:
				return HealthPriority.Normal;
			default:
				return HealthPriority.Normal;
		}
	};

	let statusIcon: React.ReactNode;
	let iconColorClass = "text-base-content/50";

	switch (item.status) {
		case "healthy":
			statusIcon = <Heart className="h-4 w-4" />;
			iconColorClass = "text-success";
			break;
		case "corrupted":
			statusIcon = <HeartCrack className="h-4 w-4" />;
			iconColorClass = "text-error";
			break;
		case "repair_triggered":
			statusIcon = <Wrench className="h-4 w-4 animate-spin-slow" />;
			iconColorClass = "text-info";
			break;
		case "checking":
			statusIcon = <Loader className="h-4 w-4 animate-spin" />;
			iconColorClass = "text-warning";
			break;
		case "degraded":
			statusIcon = <HeartCrack className="h-4 w-4" />;
			iconColorClass = "text-warning";
			break;
		default:
			statusIcon = <Clock className="h-4 w-4" />;
			iconColorClass = "text-base-content/50";
			break;
	}

	return (
		<div className="card border-2 border-base-300/50 bg-base-100 shadow-md">
			<div className="card-body space-y-3 p-4">
				{/* Header Row: Checkbox + File Info + Actions */}
				<div className="flex items-start gap-3">
					<label className="flex h-11 w-11 shrink-0 cursor-pointer items-center justify-center">
						<input
							type="checkbox"
							className="checkbox"
							checked={isSelected}
							onChange={(e) => onSelectChange(item.file_path, e.target.checked)}
						/>
					</label>

					<div className="min-w-0 flex-1">
						<div className="flex min-w-0 items-center gap-2">
							<span className="shrink-0">
								{statusIcon && <span className={iconColorClass}>{statusIcon}</span>}
							</span>
							<div className="min-w-0 break-all font-bold text-sm">
								{item.file_path.split("/").pop() || ""}
							</div>
						</div>

						{/* Quick Info Pills */}
						<div className="mt-2 flex flex-wrap gap-2">
							<HealthBadge status={item.status} isMasked={item.is_masked} />
							{playbackImpact && <PlaybackImpactBadge impact={playbackImpact} />}
							{errorDetails && <PartialCheckBadge details={errorDetails} />}
							{errorDetails && <ContentVerificationBadge details={errorDetails} />}
							{item.status === "checking" && item.check_progress && (
								<ArticleCheckProgress progress={item.check_progress} />
							)}

							<button
								type="button"
								className="badge badge-sm cursor-pointer transition-transform hover:scale-110"
								onClick={() => onSetPriority(item.id, getNextPriority(item.priority))}
								onKeyDown={(e) => {
									if (e.key === "Enter" || e.key === " ") {
										onSetPriority(item.id, getNextPriority(item.priority));
									}
								}}
								title="Click to cycle priority"
							>
								{item.priority === HealthPriority.Next
									? "Next"
									: item.priority === HealthPriority.High
										? "High"
										: "Normal"}
							</button>

							<span className="badge badge-ghost badge-xs">
								{item.last_checked ? formatRelativeTime(item.last_checked) : "Never checked"}
							</span>

							{item.retry_count > 0 && (
								<span className="badge badge-warning badge-xs" title="Health check retries">
									H: {item.retry_count}/{item.max_retries}
								</span>
							)}

							{(item.status === "repair_triggered" || item.repair_retry_count > 0) && (
								<span className="badge badge-info badge-xs" title="Repair retries">
									R: {item.repair_retry_count}/{item.max_repair_retries}
								</span>
							)}
						</div>
					</div>

					<div className="shrink-0">
						<HealthItemActionsMenu
							item={item}
							isCancelPending={isCancelPending}
							isDirectCheckPending={isDirectCheckPending}
							isRepairPending={isRepairPending}
							isDeletePending={isDeletePending}
							isUnmaskPending={isUnmaskPending}
							isRegeneratePending={isRegeneratePending}
							onCancelCheck={onCancelCheck}
							onManualCheck={onManualCheck}
							onRepair={onRepair}
							onPar2Repair={onPar2Repair}
							onDelete={onDelete}
							onUnmask={onUnmask}
							onRegenerate={onRegenerate}
						/>
					</div>
				</div>

				{/* Error Messages. PAR2 repair verdicts land in last_error when a
				    repair proved impossible; render those in translated form. */}
				{item.last_error && (
					<div className="alert alert-error px-3 py-2">
						<AlertCircle className="h-4 w-4 shrink-0" />
						{repairReason ? (
							<RepairFailureMessage reason={repairReason} />
						) : (
							<span className="text-xs">{truncateText(item.last_error, 100)}</span>
						)}
					</div>
				)}

				{/* Gated on an actual error: a healthy record now carries a
				    content-verification envelope, which the badge above renders. */}
				{item.last_error && item.error_details && item.error_details !== item.last_error && (
					<div className="alert alert-warning px-3 py-2">
						<AlertCircle className="h-4 w-4 shrink-0" />
						<span className="text-xs">Technical: {truncateText(item.error_details, 80)}</span>
					</div>
				)}

				{/* Expandable Details */}
				<button
					type="button"
					className="btn btn-ghost btn-sm w-full justify-between"
					onClick={() => setIsExpanded(!isExpanded)}
				>
					<span className="text-xs">Full Details</span>
					{isExpanded ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />}
				</button>

				{isExpanded && (
					<div className="space-y-2 border-t pt-3 text-xs">
						<div>
							<span className="opacity-70">File Path:</span>
							<div className="mt-1 break-all font-mono text-xs">{item.file_path}</div>
						</div>
						{item.library_path && (
							<div>
								<span className="opacity-70">Library Path:</span>
								<div className="mt-1 break-all font-mono text-xs">{item.library_path}</div>
							</div>
						)}
						{item.metadata && (
							<div>
								<span className="opacity-70">Metadata:</span>
								<div className="mt-1 flex flex-wrap gap-1">
									{(() => {
										try {
											const meta = JSON.parse(item.metadata);
											return (
												<>
													{meta.instanceName && (
														<span className="badge badge-outline badge-xs">
															{meta.instanceName}
														</span>
													)}
													{meta.series?.id && (
														<span className="badge badge-ghost badge-xs" title="Series ID">
															SeriesID: {meta.series.id}
														</span>
													)}
													{meta.movie?.id && (
														<span className="badge badge-ghost badge-xs" title="Movie ID">
															MovieID: {meta.movie.id}
														</span>
													)}
													{meta.series?.tvdbId && (
														<span className="badge badge-ghost badge-xs" title="TVDB ID">
															TVDBID: {meta.series.tvdbId}
														</span>
													)}
													{meta.episodeFile?.id && (
														<span className="badge badge-ghost badge-xs" title="Episode File ID">
															FileID: {meta.episodeFile.id}
														</span>
													)}
													{meta.movie?.tmdbId && (
														<span className="badge badge-ghost badge-xs" title="TMDB ID">
															TMDBID: {meta.movie.tmdbId}
														</span>
													)}
													{meta.movieFile?.id && (
														<span className="badge badge-ghost badge-xs" title="Movie File ID">
															FileID: {meta.movieFile.id}
														</span>
													)}
												</>
											);
										} catch (_e) {
											return null;
										}
									})()}
								</div>
							</div>
						)}
						<div>
							<span className="opacity-70">Next Check:</span>
							<div className="mt-1 text-xs">
								{item.scheduled_check_at
									? formatFutureTime(item.scheduled_check_at)
									: "Not scheduled"}
							</div>
						</div>
						<div>
							<span className="opacity-70">Created:</span>
							<div className="mt-1 text-xs">{formatRelativeTime(item.created_at)}</div>
						</div>
					</div>
				)}
			</div>
		</div>
	);
});
