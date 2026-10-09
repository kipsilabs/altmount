import { Clock, Heart, HeartCrack, Loader, Wrench } from "lucide-react";
import { memo, useMemo } from "react";
import { HealthBadge } from "../../../../components/ui/StatusBadge";
import {
	formatFutureTime,
	formatRelativeTime,
	parseHealthErrorDetails,
} from "../../../../lib/utils";
import { type FileHealth, HealthPriority } from "../../../../types/api";
import { parseRepairReason } from "../par2RepairReason";
import { RepairFailureMessage } from "../RepairFailureMessage";
import { ArticleCheckProgress } from "./ArticleCheckProgress";
import { ContentVerificationBadge } from "./ContentVerificationBadge";
import { HealthItemActionsMenu } from "./HealthItemActionsMenu";
import { PartialCheckBadge } from "./PartialCheckBadge";
import { PlaybackImpactBadge } from "./PlaybackImpactBadge";

interface HealthTableRowProps {
	item: FileHealth;
	isSelected: boolean;
	isCancelPending: boolean;
	isDirectCheckPending: boolean;
	isRepairPending: boolean;
	isDeletePending: boolean;
	isUnmaskPending: boolean;
	isRegeneratePending?: boolean;
	onSelectChange: (filePath: string, checked: boolean) => void;
	onCancelCheck: (id: number) => void;
	onManualCheck: (id: number, verifyContent?: boolean) => void;
	onRepair: (id: number) => void;
	onPar2Repair?: (filePath: string) => void;
	onDelete: (id: number) => void;
	onUnmask: (id: number) => void;
	onSetPriority: (id: number, priority: HealthPriority) => void;
	onRegenerate?: (filePath: string) => void;
}

export const HealthTableRow = memo(function HealthTableRow({
	item,
	isSelected,
	isCancelPending,
	isDirectCheckPending,
	isRepairPending,
	isDeletePending,
	isUnmaskPending,
	isRegeneratePending,
	onSelectChange,
	onCancelCheck,
	onManualCheck,
	onRepair,
	onPar2Repair,
	onDelete,
	onUnmask,
	onSetPriority,
	onRegenerate,
}: HealthTableRowProps) {
	const parsedMetadata = useMemo(() => {
		if (!item.metadata) return null;
		try {
			return JSON.parse(item.metadata);
		} catch (_e) {
			return null;
		}
	}, [item.metadata]);

	const errorDetails = useMemo(
		() => parseHealthErrorDetails(item.error_details),
		[item.error_details],
	);
	const repairReason = useMemo(
		() => (item.last_error?.startsWith("par2repair:") ? parseRepairReason(item.last_error) : null),
		[item.last_error],
	);
	const playbackImpact = errorDetails?.playback_impact ?? null;

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
	let iconColorClass = "text-base-content/50"; // Default color

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
		<tr key={item.id} className={`hover ${isSelected ? "bg-base-200" : ""}`}>
			<td>
				<label className="cursor-pointer">
					<input
						type="checkbox"
						className="checkbox"
						checked={isSelected}
						onChange={(e) => onSelectChange(item.file_path, e.target.checked)}
					/>
				</label>
			</td>
			<td>
				<div className="flex items-center space-x-3">
					<span className={iconColorClass}>{statusIcon}</span>
					<div>
						<div className="flex flex-wrap items-center gap-2">
							<div className="break-all font-bold">{item.file_path.split("/").pop() || ""}</div>
							{item.indexer && (
								<span className="badge badge-primary badge-xs shrink-0 border-primary/30 bg-primary/20 py-1.5 font-bold text-primary tracking-tight">
									{item.indexer}
								</span>
							)}
						</div>
						<div className="break-all text-base-content/70 text-sm">{item.file_path}</div>
					</div>
				</div>
			</td>
			<td>
				<div className="flex flex-col gap-1">
					<div className="cursor-help break-all text-sm" title={item.library_path || undefined}>
						{item.library_path?.split("/").pop() || ""}
					</div>
					{parsedMetadata && (
						<div className="mt-1 flex flex-wrap gap-1">
							{parsedMetadata.instanceName && (
								<span className="badge badge-outline badge-xs">{parsedMetadata.instanceName}</span>
							)}
							{parsedMetadata.series?.id && (
								<span className="badge badge-ghost badge-xs" title="Series ID">
									SeriesID: {parsedMetadata.series.id}
								</span>
							)}
							{parsedMetadata.movie?.id && (
								<span className="badge badge-ghost badge-xs" title="Movie ID">
									MovieID: {parsedMetadata.movie.id}
								</span>
							)}
							{parsedMetadata.series?.tvdbId && (
								<span className="badge badge-ghost badge-xs" title="TVDB ID">
									TVDBID: {parsedMetadata.series.tvdbId}
								</span>
							)}
							{parsedMetadata.episodeFile?.id && (
								<span className="badge badge-ghost badge-xs" title="Episode File ID">
									FileID: {parsedMetadata.episodeFile.id}
								</span>
							)}
							{parsedMetadata.movie?.tmdbId && (
								<span className="badge badge-ghost badge-xs" title="TMDB ID">
									TMDBID: {parsedMetadata.movie.tmdbId}
								</span>
							)}
							{parsedMetadata.movieFile?.id && (
								<span className="badge badge-ghost badge-xs" title="Movie File ID">
									FileID: {parsedMetadata.movieFile.id}
								</span>
							)}
						</div>
					)}
				</div>
			</td>
			<td>
				<div className="flex flex-wrap items-center gap-2">
					<HealthBadge status={item.status} isMasked={item.is_masked} />
					{playbackImpact && <PlaybackImpactBadge impact={playbackImpact} />}
					{errorDetails && <PartialCheckBadge details={errorDetails} />}
					{errorDetails && <ContentVerificationBadge details={errorDetails} />}
					{item.status === "checking" && item.check_progress && (
						<ArticleCheckProgress progress={item.check_progress} />
					)}
				</div>
				{/* Show last_error for repair failures and general errors. PAR2
				    repair verdicts land here when a repair proved impossible;
				    render those in their translated, actionable form. */}
				{repairReason ? (
					<div className="mt-1">
						<RepairFailureMessage reason={repairReason} />
					</div>
				) : (
					item.last_error && (
						<div className="mt-1 break-all text-error text-xs">{item.last_error}</div>
					)
				)}
				{/* Show error_details for additional technical details. Gated on an
				    actual error: a healthy record now carries a content-verification
				    envelope, which the badge above already renders. */}
				{item.last_error && item.error_details && item.error_details !== item.last_error && (
					<div className="mt-1 break-all text-warning text-xs">Technical: {item.error_details}</div>
				)}
			</td>
			<td>
				<div className="flex flex-col gap-1">
					<div className="flex items-center gap-1">
						<button
							type="button"
							className="cursor-pointer transition-transform hover:scale-110"
							onClick={() => onSetPriority(item.id, getNextPriority(item.priority))}
							title="Click to cycle priority"
						>
							{item.priority === HealthPriority.Next ? (
								<div className="badge badge-warning badge-xs">Next</div>
							) : item.priority === HealthPriority.High ? (
								<div className="badge badge-error badge-xs">High</div>
							) : (
								<div className="badge badge-ghost badge-xs">Normal</div>
							)}
						</button>
					</div>
					<div className="flex gap-1">
						<span
							className={`badge badge-xs ${item.retry_count > 0 ? "badge-warning" : "badge-ghost"}`}
							title="Health check retries"
						>
							H:{item.retry_count}
						</span>
						{(item.status === "repair_triggered" || item.repair_retry_count > 0) && (
							<span
								className={`badge badge-xs ${item.repair_retry_count > 0 ? "badge-info" : "badge-ghost"}`}
								title="Repair retries"
							>
								R:{item.repair_retry_count}
							</span>
						)}
					</div>
				</div>
			</td>
			<td>
				<div className="flex flex-col text-xs">
					<div className="tooltip tooltip-left" data-tip="Last checked time">
						<span className="text-base-content/70">
							L: {item.last_checked ? formatRelativeTime(item.last_checked) : "Never"}
						</span>
					</div>
					<div className="tooltip tooltip-left" data-tip="Next scheduled check">
						<span className="text-base-content/50">
							N: {item.scheduled_check_at ? formatFutureTime(item.scheduled_check_at) : "None"}
						</span>
					</div>
				</div>
			</td>
			<td>
				<span className="text-base-content/70 text-xs">{formatRelativeTime(item.created_at)}</span>
			</td>
			<td>
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
			</td>
		</tr>
	);
});
