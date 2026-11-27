import {
	CheckCircle2,
	ChevronDown,
	ChevronUp,
	Download,
	FileVideo,
	History,
	Info,
	Monitor,
	Play,
	Smartphone,
	User,
} from "lucide-react";
import { useMemo, useState } from "react";
import { useActiveStreams, useImportHistory, useQueue } from "../../hooks/useApi";
import { useProgressStream } from "../../hooks/useProgressStream";
import { formatBytes, formatDuration, formatRelativeTime, formatSpeed } from "../../lib/utils";
import type { ActiveStream } from "../../types/api";
import { LoadingSpinner } from "../ui/LoadingSpinner";

export function ActivityHub() {
	const [activeTab, setActiveTab] = useState<"playback" | "imports" | "history">("playback");
	const [expandedHistory, setExpandedHistory] = useState<Record<number, boolean>>({});

	const { data: allStreams, isLoading: streamsLoading } = useActiveStreams();

	const { data: queueResponse, isLoading: queueLoading } = useQueue({
		status: "processing",
		limit: 10,
	});
	const { data: importHistory, isLoading: historyLoading } = useImportHistory(
		20,
		activeTab === "history" ? 10000 : 60000,
	);

	const queueItems = queueResponse?.data;
	const hasProcessingItems = (queueItems?.length || 0) > 0;
	const { progress: liveProgress } = useProgressStream({ enabled: hasProcessingItems });

	const toggleHistory = (id: number) => {
		setExpandedHistory((prev) => ({ ...prev, [id]: !prev[id] }));
	};

	// Enrich queue items with live progress and stages
	const enrichedQueueItems = useMemo(() => {
		if (!queueItems) return [];
		return queueItems.map((item) => ({
			...item,
			percentage: liveProgress[item.id]?.percentage ?? item.percentage,
			stage: liveProgress[item.id]?.stage ?? item.stage,
		}));
	}, [queueItems, liveProgress]);

	// Helper to parse user agent for better display
	const getClientApp = (ua?: string) => {
		if (!ua) return { name: "Unknown Client", icon: User };
		const lowUA = ua.toLowerCase();
		if (lowUA.includes("stremio")) return { name: "Stremio", icon: Play };
		if (lowUA.includes("plex")) return { name: "Plex", icon: Play };
		if (lowUA.includes("infuse")) return { name: "Infuse", icon: Play };
		if (lowUA.includes("vlc")) return { name: "VLC", icon: FileVideo };
		if (lowUA.includes("kodi")) return { name: "Kodi", icon: Play };
		if (lowUA.includes("android") || lowUA.includes("iphone"))
			return { name: "Mobile App", icon: Smartphone };
		if (lowUA.includes("mozilla") || lowUA.includes("chrome"))
			return { name: "Web Browser", icon: Monitor };
		return { name: "Media Player", icon: User };
	};

	const getCategoryColor = (category?: string) => {
		if (!category) return "badge-ghost";
		const cat = category.toLowerCase();
		if (cat.includes("movie")) return "badge-primary";
		if (cat.includes("tv")) return "badge-secondary";
		if (cat.includes("audio")) return "badge-accent";
		return "badge-ghost";
	};

	// Group streams by file_path to show "unique playback sessions"
	const groupedStreams = useMemo(() => {
		if (!allStreams) return [];

		// Filter to show only active streaming sessions (WebDAV, FUSE, or API/Stremio)
		const streamingOnly = allStreams.filter((s) => {
			const isSystemSource = s.source === "WebDAV" || s.source === "FUSE" || s.source === "API";
			const isStreaming = s.status === "Streaming";

			// Heuristic: Filter out metadata probes and very short system scans
			const isAtEnd = s.total_size > 0 && s.current_offset > s.total_size - 5 * 1024 * 1024;
			const isTooNew = s.bytes_sent < 5 * 1024 * 1024;
			const ageSeconds = (Date.now() - new Date(s.started_at).getTime()) / 1000;

			if (isAtEnd) return false;
			if (isTooNew && ageSeconds < 5) return false;

			return isSystemSource && (isStreaming || s.status === "Buffering" || s.status === "Stalled");
		});

		const groups: Record<string, ActiveStream> = {};

		for (const stream of streamingOnly) {
			if (!groups[stream.file_path]) {
				groups[stream.file_path] = { ...stream };
			} else {
				// Aggregate data for the same file
				groups[stream.file_path].bytes_sent += stream.bytes_sent;
				groups[stream.file_path].bytes_downloaded += stream.bytes_downloaded;
				groups[stream.file_path].bytes_per_second += stream.bytes_per_second;
				groups[stream.file_path].download_speed += stream.download_speed;
				// Use the highest offset as current position
				if (stream.current_offset > groups[stream.file_path].current_offset) {
					groups[stream.file_path].current_offset = stream.current_offset;
				}
				// Use highest buffered offset
				if (stream.buffered_offset > groups[stream.file_path].buffered_offset) {
					groups[stream.file_path].buffered_offset = stream.buffered_offset;
				}
			}
		}

		return Object.values(groups);
	}, [allStreams]);

	const playbackCount = groupedStreams.length;
	const importCount = queueItems?.length || 0;

	return (
		<div className="card min-h-[400px] bg-base-100 shadow-lg">
			<div className="card-body p-0">
				<div className="tabs tabs-bordered grid w-full min-w-0 grid-cols-3">
					<button
						type="button"
						className={`tab tab-lg inline-flex flex-nowrap items-center justify-center gap-1 whitespace-nowrap px-1 py-2 text-xs sm:gap-2 sm:px-3 sm:text-sm ${activeTab === "playback" ? "tab-active border-primary font-bold text-primary" : ""}`}
						onClick={() => setActiveTab("playback")}
					>
						<Play className="h-3.5 w-3.5 shrink-0 sm:h-4 sm:w-4" aria-hidden="true" />
						<span className="leading-none">Playback</span>
						{playbackCount > 0 && (
							<span className="badge badge-xs badge-primary shrink-0">{playbackCount}</span>
						)}
					</button>
					<button
						type="button"
						className={`tab tab-lg inline-flex flex-nowrap items-center justify-center gap-1 whitespace-nowrap px-1 py-2 text-xs sm:gap-2 sm:px-3 sm:text-sm ${activeTab === "imports" ? "tab-active border-secondary font-bold text-secondary" : ""}`}
						onClick={() => setActiveTab("imports")}
					>
						<Download className="h-3.5 w-3.5 shrink-0 sm:h-4 sm:w-4" aria-hidden="true" />
						<span className="leading-none">Imports</span>
						{importCount > 0 && (
							<span className="badge badge-xs badge-secondary shrink-0">{importCount}</span>
						)}
					</button>
					<button
						type="button"
						className={`tab tab-lg inline-flex flex-nowrap items-center justify-center gap-1 whitespace-nowrap px-1 py-2 text-xs sm:gap-2 sm:px-3 sm:text-sm ${activeTab === "history" ? "tab-active border-accent font-bold text-accent" : ""}`}
						onClick={() => setActiveTab("history")}
					>
						<History className="h-3.5 w-3.5 shrink-0 sm:h-4 sm:w-4" aria-hidden="true" />
						<span className="leading-none">History</span>
					</button>
				</div>

				<div className="max-h-[350px] overflow-y-auto p-4">
					{activeTab === "playback" && (
						<div className="space-y-4">
							{streamsLoading ? (
								<div className="flex justify-center py-10">
									<LoadingSpinner />
								</div>
							) : groupedStreams.length > 0 ? (
								groupedStreams.map((stream) => {
									const position =
										stream.current_offset > 0 ? stream.current_offset : stream.bytes_sent;
									const progress =
										stream.total_size > 0 ? Math.round((position / stream.total_size) * 100) : 0;
									const bufferedProgress =
										stream.total_size > 0
											? Math.round((stream.buffered_offset / stream.total_size) * 100)
											: 0;

									const isStalled =
										stream.status === "Stalled" ||
										(stream.bytes_per_second === 0 && stream.status !== "Buffering");
									const clientApp = getClientApp(stream.user_agent);

									return (
										<div
											key={stream.id}
											className="group flex flex-col gap-2 rounded-lg bg-base-200/30 p-3 transition-colors hover:bg-base-200/50"
										>
											<div className="flex items-center gap-3">
												<div className="relative">
													<FileVideo
														className={`h-8 w-8 shrink-0 ${isStalled ? "text-warning" : "text-primary/70"}`}
													/>
													{!isStalled && (
														<span className="-right-0.5 -bottom-0.5 absolute flex h-2 w-2">
															<span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-success opacity-75" />
															<span className="relative inline-flex h-2 w-2 rounded-full bg-success" />
														</span>
													)}
												</div>
												<div className="min-w-0 flex-1">
													<div className="truncate font-medium text-sm" title={stream.file_path}>
														{stream.file_path.split("/").pop()}
													</div>
													<div className="mt-1 flex flex-wrap items-center gap-x-2 gap-y-0.5">
														<span
															className={`font-bold text-xs ${isStalled ? "text-warning" : "text-success"}`}
														>
															{stream.status.toUpperCase()}
														</span>
														<span className="text-base-content/40 text-xs">•</span>
														<div className="flex items-center gap-1 text-base-content/60 text-xs">
															<clientApp.icon className="h-3 w-3" />
															<span>{clientApp.name}</span>
															{stream.client_ip && (
																<span className="opacity-50">({stream.client_ip})</span>
															)}
														</div>
													</div>
												</div>
												<div className="shrink-0 text-right">
													<div className="flex flex-col items-end">
														<div className="flex items-center gap-1 font-bold text-info text-xs">
															<span className="text-[8px] text-base-content/80">IN:</span>
															{formatSpeed(stream.download_speed)}
														</div>
														<div className="flex items-center gap-1 font-bold font-mono text-primary text-xs">
															<span className="text-[8px] text-base-content/80 text-success">
																OUT:
															</span>
															{formatSpeed(stream.bytes_per_second)}
														</div>
													</div>
													{stream.eta > 0 && !isStalled && (
														<div className="font-mono text-base-content/40 text-xs">
															{formatDuration(stream.eta)} left
														</div>
													)}
												</div>
											</div>

											<div className="mt-1 space-y-1">
												<div className="flex items-center justify-between px-0.5 text-xs">
													<div className="flex items-center gap-2">
														<span className="font-medium text-primary">{progress}%</span>
														<span className="text-base-content/40">•</span>
														<span className="text-base-content/40">
															DL: {formatBytes(stream.bytes_downloaded)}
														</span>
													</div>
													<span className="text-base-content/40">
														{formatBytes(position)} / {formatBytes(stream.total_size)}
													</span>
												</div>
												<div className="relative h-1.5 w-full overflow-hidden rounded-full bg-neutral">
													{bufferedProgress > progress && (
														<div
															className="absolute top-0 left-0 h-full bg-primary/20 transition-all duration-500 ease-out"
															style={{ width: `${bufferedProgress}%` }}
														/>
													)}
													<div
														className="absolute top-0 left-0 h-full bg-primary transition-all duration-500 ease-out"
														style={{ width: `${progress}%` }}
													/>
												</div>
											</div>
										</div>
									);
								})
							) : (
								<div className="py-10 text-center text-base-content/50">
									<Play className="mx-auto mb-2 h-8 w-8 opacity-20" />
									<p>No active streams</p>
								</div>
							)}
						</div>
					)}

					{activeTab === "imports" && (
						<div className="space-y-4">
							{queueLoading ? (
								<div className="flex justify-center py-10">
									<LoadingSpinner />
								</div>
							) : enrichedQueueItems.length > 0 ? (
								enrichedQueueItems.map((item) => {
									const progress = item.percentage ?? 0;
									const isProcessing = progress > 0 && progress < 100;

									return (
										<div
											key={item.id}
											className="group flex flex-col gap-2 rounded-lg bg-base-200/30 p-3 transition-colors hover:bg-base-200/50"
										>
											<div className="flex items-center gap-3">
												<div className="relative">
													<Download
														className={`h-8 w-8 shrink-0 ${isProcessing ? "text-secondary" : "text-base-content/20"}`}
													/>
													{isProcessing && (
														<span className="-top-1 -right-1 absolute flex h-3 w-3">
															<span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-secondary opacity-75" />
															<span className="relative inline-flex h-3 w-3 rounded-full bg-secondary" />
														</span>
													)}
												</div>
												<div className="min-w-0 flex-1">
													<div
														className="truncate font-medium text-sm"
														title={item.nzb_display_name}
													>
														{item.target_path || item.nzb_display_name}
													</div>
													<div className="mt-1 flex items-center gap-2">
														<span className="font-bold text-secondary text-xs">
															{item.stage?.toUpperCase() || "IMPORTING"}
														</span>
														<span className="text-base-content/40 text-xs">•</span>
														<span className="text-base-content/60 text-xs">Queue #{item.id}</span>
														{item.category && (
															<>
																<span className="text-base-content/40 text-xs">•</span>
																<span
																	className={`badge badge-xs ${getCategoryColor(item.category)} border-none`}
																>
																	{item.category}
																</span>
															</>
														)}
													</div>
												</div>
												<div className="shrink-0 text-right">
													<div className="font-bold text-secondary text-sm">{progress}%</div>
													<div className="text-base-content/40 text-xs">
														Attempt {item.retry_count + 1}
													</div>
												</div>
											</div>

											<div className="mt-1 space-y-1">
												<div className="relative h-1.5 w-full overflow-hidden rounded-full bg-neutral">
													<div
														className="absolute top-0 left-0 h-full bg-secondary transition-all duration-500 ease-out"
														style={{ width: `${progress}%` }}
													/>
												</div>
											</div>
										</div>
									);
								})
							) : (
								<div className="py-10 text-center text-base-content/50">
									<Download className="mx-auto mb-2 h-8 w-8 opacity-20" />
									<p>No active imports</p>
								</div>
							)}
						</div>
					)}

					{activeTab === "history" && (
						<div className="max-h-80 space-y-2 overflow-y-auto">
							{historyLoading ? (
								<div className="flex justify-center py-10">
									<LoadingSpinner />
								</div>
							) : importHistory && importHistory.length > 0 ? (
								importHistory.map((item) => {
									const isExpanded = expandedHistory[item.id];
									return (
										<div
											key={item.id}
											className={`flex flex-col overflow-hidden rounded-lg border-success border-l-4 bg-base-200/30 transition-all ${isExpanded ? "ring-1 ring-success/20" : ""}`}
										>
											<button
												type="button"
												onClick={() => toggleHistory(item.id)}
												className="flex items-center justify-between gap-4 p-2 text-left text-sm transition-colors hover:bg-base-200/50"
											>
												<div className="flex min-w-0 items-center gap-3 truncate">
													<CheckCircle2 className="h-4 w-4 shrink-0 text-success" />
													<div className="flex flex-col truncate">
														<span className="truncate font-medium" title={item.file_name}>
															{item.file_name}
														</span>
														<div className="flex items-center gap-2 truncate text-base-content/50 text-xs">
															<span
																className={`badge badge-xs ${getCategoryColor(item.category)} border-none text-[10px]`}
															>
																{item.category || "General"}
															</span>
															<span>•</span>
															<span>{formatBytes(item.file_size)}</span>
														</div>
													</div>
												</div>
												<div className="flex shrink-0 items-center gap-2">
													<span className="whitespace-nowrap text-[11px] text-base-content/40">
														{formatRelativeTime(item.completed_at)}
													</span>
													{isExpanded ? (
														<ChevronUp className="h-3 w-3 opacity-30" />
													) : (
														<ChevronDown className="h-3 w-3 opacity-30" />
													)}
												</div>
											</button>

											{isExpanded && (
												<div className="border-base-300 border-t bg-base-300/20 p-3 text-xs">
													<div className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-2">
														<div className="flex items-center gap-1.5 font-semibold text-base-content/60">
															<History className="h-3 w-3" />
															<span>NZB:</span>
														</div>
														<div className="break-all font-mono opacity-80">{item.nzb_display_name}</div>

														<div className="flex items-center gap-1.5 font-semibold text-base-content/60">
															<Play className="h-3 w-3" />
															<span>Dest:</span>
														</div>
														<div className="break-all font-mono opacity-80">
															{item.virtual_path}
														</div>

														{item.library_path && (
															<>
																<div className="flex items-center gap-1.5 font-semibold text-base-content/60">
																	<CheckCircle2 className="h-3 w-3" />
																	<span>Library:</span>
																</div>
																<div className="break-all font-mono text-success opacity-80">
																	{item.library_path}
																</div>
															</>
														)}

														<div className="flex items-center gap-1.5 font-semibold text-base-content/60">
															<Info className="h-3 w-3" />
															<span>Size:</span>
														</div>
														<div className="opacity-80">
															{formatBytes(item.file_size)} ({item.file_size.toLocaleString()}{" "}
															bytes)
														</div>

														{item.metadata && (
															<>
																<div className="col-span-2 mt-1 border-base-content/10 border-t pt-2 font-bold uppercase tracking-widest opacity-40">
																	Technical Details
																</div>
																{(() => {
																	try {
																		const meta = JSON.parse(item.metadata);
																		return (
																			<>
																				{meta.segment_count && (
																					<>
																						<div className="flex items-center gap-1.5 font-semibold text-base-content/60">
																							<Info className="h-3 w-3" />
																							<span>Segments:</span>
																						</div>
																						<div className="opacity-80">
																							{meta.segment_count} articles
																						</div>
																					</>
																				)}
																				{meta.encryption && (
																					<>
																						<div className="flex items-center gap-1.5 font-semibold text-base-content/60">
																							<Info className="h-3 w-3" />
																							<span>Encryption:</span>
																						</div>
																						<div className="uppercase opacity-80">
																							{meta.encryption}
																						</div>
																					</>
																				)}
																			</>
																		);
																	} catch (_e) {
																		return null;
																	}
																})()}
															</>
														)}
													</div>
												</div>
											)}
										</div>
									);
								})
							) : (
								<div className="py-10 text-center text-base-content/50">
									<History className="mx-auto mb-2 h-8 w-8 opacity-20" />
									<p>No import history</p>
								</div>
							)}
						</div>
					)}
				</div>
			</div>
		</div>
	);
}
