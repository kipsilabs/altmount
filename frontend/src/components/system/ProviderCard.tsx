import { AlertTriangle, Gauge, RotateCcw } from "lucide-react";
import { formatSpeed } from "../../lib/utils";
import type { ProviderStatus } from "../../types/api";
import { BytesDisplay } from "../ui/BytesDisplay";

function getQuotaProgressColor(provider: ProviderStatus): string {
	if (provider.quota_exceeded) {
		return "progress-error";
	}
	if ((provider.quota_used ?? 0) / (provider.quota_bytes ?? 1) >= 0.9) {
		return "progress-warning";
	}
	return "progress-success";
}

interface ProviderCardProps {
	provider: ProviderStatus;
	className?: string;
	onResetQuota?: (providerId: string) => void;
}

const MS_PER_MINUTE = 1000 * 60;
const MS_PER_HOUR = MS_PER_MINUTE * 60;

function QuotaResetCountdown({ resetAt }: { resetAt: string }) {
	const diffMs = new Date(resetAt).getTime() - Date.now();

	if (diffMs <= 0) {
		return <span>resetting...</span>;
	}

	const hours = Math.floor(diffMs / MS_PER_HOUR);
	const minutes = Math.floor((diffMs % MS_PER_HOUR) / MS_PER_MINUTE);

	if (hours > 24) {
		const days = Math.floor(hours / 24);
		return (
			<span>
				in {days}d {hours % 24}h
			</span>
		);
	}

	return (
		<span>
			in {hours}h {minutes}m
		</span>
	);
}

export function ProviderCard({ provider, className, onResetQuota }: ProviderCardProps) {
	// Calculate connection usage percentage
	const usagePercentage =
		provider.max_connections > 0
			? Math.round((provider.used_connections / provider.max_connections) * 100)
			: 0;

	// Determine state badge color and icon
	const getStateBadge = () => {
		const state = provider.state.toLowerCase();

		switch (state) {
			case "active":
				return {
					color: "badge-success",
					text: "Active",
				};
			case "failed":
			case "failing":
				return {
					color: "badge-error",
					text: "Failed",
				};
			case "pending":
			case "connecting":
				return {
					color: "badge-warning",
					text: "Pending",
				};
			default:
				return {
					color: "badge-ghost",
					text: state,
				};
		}
	};

	const stateBadge = getStateBadge();

	// Determine progress bar color based on usage
	const getProgressColor = () => {
		if (usagePercentage >= 90) return "progress-error";
		if (usagePercentage >= 70) return "progress-warning";
		return "progress-success";
	};

	return (
		<article
			className={`card bg-base-100 shadow-sm transition-shadow ${className || ""}`}
			aria-labelledby={`provider-${provider.host}`}
		>
			<div className="card-body p-4">
				{/* Header with host and state badge */}
				<div className="flex items-start justify-between">
					<div className="min-w-0 flex-1">
						<div className="flex items-center gap-1.5">
							<div
								className={`h-2 w-2 shrink-0 rounded-full ${
									provider.state.toLowerCase() === "active"
										? provider.error_count > 10
											? "bg-warning"
											: "bg-success"
										: "bg-error"
								}`}
							/>
							<h3
								className="truncate font-semibold text-sm leading-none"
								id={`provider-${provider.host}`}
							>
								{provider.host}
							</h3>
						</div>
						<div className="mt-1 flex items-center gap-2">
							<span className={`badge badge-xs font-medium ${stateBadge.color}`}>
								{stateBadge.text}
							</span>
							<span
								className="font-mono text-base-content/40 text-xs"
								title="Provider ID"
							>
								{provider.id}
							</span>
						</div>
					</div>

					<div className="flex items-center gap-2">
						{provider.last_speed_test_mbps > 0 && (
							<div
								className="badge badge-outline badge-sm gap-1 font-mono text-[10px]"
								title={`Last tested ${provider.last_speed_test_time ? new Date(provider.last_speed_test_time).toLocaleString() : "unknown"}`}
							>
								<Gauge className="h-3 w-3" />
								{Math.round(provider.last_speed_test_mbps)} MB/s
							</div>
						)}
					</div>
				</div>

				{/* Connection Info */}
				<div className="mt-4 flex items-center justify-between text-xs">
					<span className="text-base-content/60">Connections</span>
					<span className="font-bold">
						{provider.used_connections} / {provider.max_connections}
					</span>
				</div>
				<progress
					className={`progress mt-1 w-full ${getProgressColor()}`}
					value={usagePercentage}
					max="100"
				/>

				{/* Detailed Stats Grid */}
				<div className="mt-4 grid grid-cols-3 gap-2 border-base-200 border-t pt-3">
					<div className="space-y-0.5">
						<div className="text-[8px] text-base-content/40 uppercase tracking-widest">Speed</div>
						<div className="font-bold font-mono text-primary text-xs">
							{formatSpeed(provider.current_speed_bytes_per_sec)}
						</div>
					</div>
					<div className="space-y-0.5">
						<div className="text-[8px] text-base-content/40 uppercase tracking-widest">Ping</div>
						<div className="font-bold font-mono text-info text-xs">{provider.ping_ms}ms</div>
					</div>
					<div className="space-y-0.5">
						<div className="text-[8px] text-base-content/40 uppercase tracking-widest">Errors</div>
						<div
							className={`font-bold font-mono text-xs ${provider.error_count > 0 ? "text-error" : "text-base-content/20"}`}
						>
							{provider.error_count}
						</div>
					</div>
				</div>

				{/* Total Bytes per provider */}
				<div className="mt-2 space-y-1 border-base-200 border-t pt-2">
					<div className="flex items-center justify-between text-[10px]">
						<div className="flex flex-col">
							<span className="text-base-content/50 uppercase tracking-tight">
								Total Downloaded
							</span>
							{provider.started_at && (
								<span className="text-[8px] text-base-content/30 uppercase tracking-tighter">
									Since {new Date(provider.started_at).toLocaleDateString()}
								</span>
							)}
						</div>
						<span className="font-bold font-mono text-base-content/70">
							<BytesDisplay bytes={provider.byte_count} />
						</span>
					</div>
					<div className="flex items-center justify-between text-[10px]">
						<span className="text-base-content/50 uppercase tracking-tight">Last 24h</span>
						<span className="font-bold font-mono text-primary">
							<BytesDisplay bytes={provider.byte_count_24h} />
						</span>
					</div>
				</div>

				{/* Download Quota */}
				{provider.quota_bytes != null && provider.quota_bytes > 0 && (
					<div className="mt-2 space-y-1 border-base-200 border-t pt-2">
						<div className="flex items-center justify-between text-[10px]">
							<span className="flex items-center gap-1 text-base-content/50 uppercase tracking-tight">
								<Gauge className="h-3 w-3" />
								Quota
								{onResetQuota && (
									<button
										type="button"
										onClick={() => onResetQuota(provider.id)}
										className="btn btn-ghost btn-xs h-4 min-h-0 p-0 text-base-content/30 hover:text-primary"
										title="Reset Quota"
									>
										<RotateCcw className="h-2.5 w-2.5" />
									</button>
								)}
							</span>
							<span className="font-bold font-mono text-base-content/70">
								<BytesDisplay bytes={provider.quota_used || 0} /> /{" "}
								<BytesDisplay bytes={provider.quota_bytes} />
							</span>
						</div>
						<progress
							className={`progress w-full ${getQuotaProgressColor(provider)}`}
							value={provider.quota_used || 0}
							max={provider.quota_bytes}
						/>
						{provider.quota_reset_at && (
							<div className="flex justify-end text-[8px] text-base-content/40 italic">
								Resets <QuotaResetCountdown resetAt={provider.quota_reset_at} />
							</div>
						)}
					</div>
				)}

				{/* Additional Info (Missing Rate) */}
				{provider.missing_count > 0 && (
					<div className="mt-3 flex items-center justify-between rounded-md bg-base-200/50 px-2 py-1 text-[10px]">
						<div className="flex items-center gap-1.5 text-base-content/60">
							<AlertTriangle
								className={`h-3 w-3 ${provider.missing_warning ? "text-warning" : "text-base-content/30"}`}
							/>
							<span>Missing Articles</span>
						</div>
						<div className="flex items-center gap-2">
							<span className="font-bold font-mono">{provider.missing_count}</span>
							<span className="text-base-content/30">•</span>
							<span className="font-mono">{provider.missing_rate_per_minute.toFixed(1)}/min</span>
						</div>
					</div>
				)}
			</div>
		</article>
	);
}
