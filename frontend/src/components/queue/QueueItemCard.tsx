import {
	AlertCircle,
	Box,
	ChevronDown,
	ChevronUp,
	Download,
	FileCode,
	Globe,
	Link2,
	MoreVertical,
	PlayCircle,
	Trash2,
	XCircle,
} from "lucide-react";
import { memo, useState } from "react";
import { formatBytes, formatRelativeTime, truncateText } from "../../lib/utils";
import { type QueueItem, QueueStatus } from "../../types/api";
import { PathDisplay } from "../ui/PathDisplay";
import { PeerBadge } from "../ui/PeerBadge";
import { StatusBadge } from "../ui/StatusBadge";

interface QueueItemCardProps {
	item: QueueItem;
	isSelected: boolean;
	onSelectChange: (id: number, checked: boolean) => void;
	onRetry: (id: number) => void;
	onCancel: (id: number) => void;
	onDelete: (id: number) => void;
	onDownload: (id: number) => void;
	onRegenerateSymlink?: (storagePath: string) => void;
	isRetryPending: boolean;
	isCancelPending: boolean;
	isDeletePending: boolean;
	isRegenerateSymlinkPending?: boolean;
}

export const QueueItemCard = memo(function QueueItemCard({
	item,
	isSelected,
	onSelectChange,
	onRetry,
	onCancel,
	onDelete,
	onDownload,
	onRegenerateSymlink,
	isRetryPending,
	isCancelPending,
	isDeletePending,
	isRegenerateSymlinkPending,
}: QueueItemCardProps) {
	const [isExpanded, setIsExpanded] = useState(false);

	return (
		<div className="card border-2 border-base-300/50 bg-base-100 shadow-md">
			<div className="card-body space-y-3 p-4">
				{/* Header Row: Checkbox + Filename + Actions */}
				<div className="flex min-w-0 items-start gap-3">
					<label className="flex h-11 w-11 shrink-0 cursor-pointer items-center justify-center">
						<input
							type="checkbox"
							className="checkbox"
							checked={isSelected}
							onChange={(e) => onSelectChange(item.id, e.target.checked)}
						/>
					</label>

					<div className="min-w-0 flex-1 overflow-hidden">
						<div className="flex min-w-0 items-center gap-2">
							<FileCode className="mt-0.5 h-4 w-4 shrink-0 text-base-content/60" />
							<div className="min-w-0 flex-1 font-bold text-sm leading-tight">
								<PathDisplay path={item.nzb_display_name} maxLength={80} showFileName />
							</div>
						</div>

						{item.peer_origin && (
							<div className="mt-1">
								<PeerBadge
									origin={item.peer_origin}
									fullCheckPending={item.peer_full_check_pending}
								/>
							</div>
						)}

						{item.indexer && (
							<div className="mt-1 flex items-center gap-1 text-base-content/50 text-xs">
								<Globe className="h-3 w-3 shrink-0" />
								<span className="truncate">{item.indexer}</span>
							</div>
						)}

						{/* Quick Info Pills */}
						<div className="mt-2 flex flex-wrap gap-2">
							{item.category && (
								<span className="badge badge-outline badge-xs py-2 font-semibold uppercase tracking-wide">
									{item.category}
								</span>
							)}
							{item.file_size && (
								<span className="badge badge-ghost badge-xs font-mono">
									{formatBytes(item.file_size)}
								</span>
							)}
							<span className="badge badge-ghost badge-xs">
								{formatRelativeTime(item.created_at)}
							</span>
							{item.retry_count > 0 && (
								<span className="badge badge-warning badge-xs font-bold uppercase tracking-tighter">
									{item.retry_count} Retries
								</span>
							)}
						</div>
					</div>

					<div className="dropdown dropdown-end shrink-0">
						<button
							type="button"
							className="btn btn-ghost btn-sm btn-square h-11 w-11"
							tabIndex={0}
						>
							<MoreVertical className="h-4 w-4" />
						</button>
						<ul className="dropdown-content menu z-[50] w-48 rounded-box border border-base-300 bg-base-100 p-2 shadow-xl">
							{(item.status === QueueStatus.PENDING ||
								item.status === QueueStatus.FAILED ||
								item.status === QueueStatus.COMPLETED) && (
								<li>
									<button type="button" onClick={() => onRetry(item.id)} disabled={isRetryPending}>
										<PlayCircle className="h-4 w-4 text-primary" />
										{item.status === QueueStatus.PENDING ? "Start Now" : "Retry Task"}
									</button>
								</li>
							)}
							{item.status === QueueStatus.PROCESSING && (
								<li>
									<button
										type="button"
										onClick={() => onCancel(item.id)}
										disabled={isCancelPending}
										className="text-warning"
									>
										<XCircle className="h-4 w-4" />
										Cancel Process
									</button>
								</li>
							)}
							<li>
								<button type="button" onClick={() => onDownload(item.id)}>
									<Download className="h-4 w-4" />
									Download NZB
								</button>
							</li>
							{item.status === QueueStatus.COMPLETED &&
								item.storage_path &&
								onRegenerateSymlink && (
									<li>
										<button
											type="button"
											onClick={() => onRegenerateSymlink(item.storage_path as string)}
											disabled={isRegenerateSymlinkPending}
										>
											<Link2 className="h-4 w-4 text-primary" />
											Regenerate Symlink
										</button>
									</li>
								)}
							<div className="divider my-1 text-base-content/70" />
							{item.status !== QueueStatus.PROCESSING && (
								<li>
									<button
										type="button"
										onClick={() => onDelete(item.id)}
										disabled={isDeletePending}
										className="text-error"
									>
										<Trash2 className="h-4 w-4" />
										Delete Record
									</button>
								</li>
							)}
						</ul>
					</div>
				</div>

				{/* Status with Progress Bar */}
				<div className="space-y-2">
					{item.status === QueueStatus.PROCESSING && item.percentage != null ? (
						<div>
							<div className="mb-1 flex justify-between text-xs">
								<StatusBadge status={item.status} />
								<span className="font-bold font-mono opacity-70">{item.percentage}%</span>
							</div>
							<progress
								className="progress progress-primary h-2 w-full"
								value={item.percentage}
								max={100}
							/>
						</div>
					) : (
						<StatusBadge status={item.status} />
					)}

					{item.status === QueueStatus.FAILED && item.error_message && (
						<div className="alert alert-error px-3 py-2">
							<AlertCircle className="h-4 w-4 shrink-0" />
							<span className="text-xs">{truncateText(item.error_message, 100)}</span>
						</div>
					)}
				</div>

				{/* Expandable Secondary Info */}
				{item.target_path && (
					<>
						<button
							type="button"
							className="btn btn-ghost btn-sm w-full justify-between"
							onClick={() => setIsExpanded(!isExpanded)}
						>
							<span className="text-xs">Details</span>
							{isExpanded ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />}
						</button>

						{isExpanded && (
							<div className="space-y-2 border-t pt-3 text-xs">
								<div>
									<span className="flex items-center gap-1 opacity-70">
										<Box className="h-3 w-3" />
										Target Path:
									</span>
									<div className="mt-1 break-all pl-4 font-mono text-xs">{item.target_path}</div>
								</div>
								{item.id && (
									<div>
										<span className="opacity-70">Queue ID:</span>
										<span className="ml-2 font-mono">{item.id}</span>
									</div>
								)}
							</div>
						)}
					</>
				)}
			</div>
		</div>
	);
});
