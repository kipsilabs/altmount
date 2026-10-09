import {
	Download,
	Eye,
	MoreHorizontal,
	PlayCircle,
	RefreshCw,
	ScanSearch,
	Trash2,
	Wrench,
	X,
} from "lucide-react";
import { useState } from "react";
import { apiClient } from "../../../../api/client";
import type { FileHealth } from "../../../../types/api";
import { parseRepairReason } from "../par2RepairReason";

interface HealthItemActionsMenuProps {
	item: FileHealth;
	isCancelPending: boolean;
	isDirectCheckPending: boolean;
	isRepairPending: boolean;
	isDeletePending: boolean;
	isUnmaskPending: boolean;
	isRegeneratePending?: boolean;
	onCancelCheck: (id: number) => void;
	onManualCheck: (id: number, verifyContent?: boolean) => void;
	onRepair: (id: number) => void;
	onPar2Repair?: (filePath: string) => void;
	onDelete: (id: number) => void;
	onUnmask: (id: number) => void;
	onRegenerate?: (filePath: string) => void;
}

export function HealthItemActionsMenu({
	item,
	isCancelPending,
	isDirectCheckPending,
	isRepairPending,
	isDeletePending,
	isUnmaskPending,
	isRegeneratePending,
	onCancelCheck,
	onManualCheck,
	onRepair,
	onPar2Repair,
	onDelete,
	onUnmask,
	onRegenerate,
}: HealthItemActionsMenuProps) {
	const [isDownloadPending, setIsDownloadPending] = useState(false);
	const repairReason = item.last_error?.startsWith("par2repair:")
		? parseRepairReason(item.last_error)
		: null;
	const par2Unavailable = repairReason?.unavailable === true;

	async function handleDownloadNZB() {
		setIsDownloadPending(true);
		try {
			const blob = await apiClient.exportMetadataToNZB(item.file_path);
			const baseName = item.file_path.split("/").pop() ?? item.file_path;
			const nameWithoutExt = baseName.replace(/\.[^/.]+$/, "");
			const url = URL.createObjectURL(blob);
			const a = document.createElement("a");
			a.href = url;
			a.download = `${nameWithoutExt}.nzb`;
			document.body.appendChild(a);
			a.click();
			document.body.removeChild(a);
			URL.revokeObjectURL(url);
		} finally {
			setIsDownloadPending(false);
		}
	}

	return (
		<div className="dropdown dropdown-end">
			<button tabIndex={0} type="button" className="btn btn-ghost btn-sm">
				<MoreHorizontal className="h-4 w-4" />
			</button>
			<ul className="dropdown-content menu z-[50] w-48 rounded-box border border-base-300 bg-base-100 p-2 shadow-xl">
				{item.is_masked && (
					<li>
						<button
							type="button"
							onClick={() => onUnmask(item.id)}
							disabled={isUnmaskPending}
							className="text-success"
						>
							<Eye className="h-4 w-4" />
							Unmask File
						</button>
					</li>
				)}
				{item.status === "checking" ? (
					<li>
						<button
							type="button"
							onClick={() => onCancelCheck(item.id)}
							disabled={isCancelPending}
							className="text-warning"
						>
							<X className="h-4 w-4" />
							Cancel Check
						</button>
					</li>
				) : (
					<>
						<li>
							<button
								type="button"
								onClick={() => onManualCheck(item.id)}
								disabled={isDirectCheckPending}
							>
								<PlayCircle className="h-4 w-4" />
								Retry Check
							</button>
						</li>
						<li>
							<button
								type="button"
								onClick={() => onManualCheck(item.id, true)}
								disabled={isDirectCheckPending}
							>
								<ScanSearch className="h-4 w-4" />
								Verify Media Content
							</button>
						</li>
					</>
				)}
				{onRegenerate && (
					<li>
						<button
							type="button"
							onClick={() => onRegenerate(item.file_path)}
							disabled={isRegeneratePending}
							className="text-primary"
						>
							<RefreshCw className="h-4 w-4" />
							Regenerate Symlink / STRM
						</button>
					</li>
				)}
				<li>
					<button
						type="button"
						onClick={() => onRepair(item.id)}
						disabled={isRepairPending}
						className="text-info"
					>
						<Wrench className="h-4 w-4" />
						Trigger Repair
					</button>
				</li>
				{onPar2Repair && (
					<li>
						<button
							type="button"
							onClick={() => onPar2Repair(item.file_path)}
							disabled={par2Unavailable}
							title={par2Unavailable ? repairReason?.summary : undefined}
							className="text-info"
						>
							<Wrench className="h-4 w-4" />
							PAR2 Repair
						</button>
						{par2Unavailable && (
							<span className="text-base-content/70 text-xs">{repairReason?.summary}</span>
						)}
					</li>
				)}
				<li>
					<button type="button" onClick={handleDownloadNZB} disabled={isDownloadPending}>
						<Download className="h-4 w-4" />
						Download NZB
					</button>
				</li>
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
			</ul>
		</div>
	);
}
