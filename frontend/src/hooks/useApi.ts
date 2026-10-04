import { keepPreviousData, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { apiClient } from "../api/client";
import type { HealthCleanupRequest, HealthPriority } from "../types/api";

// Queue hooks
export const useQueue = (params?: {
	limit?: number;
	offset?: number;
	status?: string;
	since?: string;
	search?: string;
	sort_by?: string;
	sort_order?: "asc" | "desc";
	refetchInterval?: number;
}) => {
	return useQuery({
		queryKey: ["queue", params],
		queryFn: () => apiClient.getQueue(params),
		refetchInterval: params?.refetchInterval,
	});
};

export const useQueueStats = (refetchInterval?: number) => {
	return useQuery({
		queryKey: ["queue", "stats"],
		queryFn: () => apiClient.getQueueStats(),
		refetchInterval,
	});
};

export const useQueueHistory = (days?: number) => {
	return useQuery({
		queryKey: ["queue", "history", days],
		queryFn: () => apiClient.getQueueHistory(days),
		// Refetch historical stats less frequently (every 5 minutes)
		refetchInterval: 5 * 60 * 1000,
	});
};

export const useDeleteQueueItem = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: (id: number) => apiClient.deleteQueueItem(id),
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["queue"] });
		},
	});
};

export const useDeleteBulkQueueItems = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: (ids: number[]) => apiClient.deleteBulkQueueItems(ids),
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["queue"] });
		},
	});
};

export const useRestartBulkQueueItems = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: (ids: number[]) => apiClient.restartBulkQueueItems(ids),
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["queue"] });
			queryClient.invalidateQueries({ queryKey: ["queue", "stats"] });
		},
	});
};

export const useRetryQueueItem = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: (id: number) => apiClient.retryQueueItem(id),
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["queue"] });
		},
	});
};

export const useCancelQueueItem = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: (id: number) => apiClient.cancelQueueItem(id),
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["queue"] });
			queryClient.invalidateQueries({ queryKey: ["queue-stats"] });
		},
	});
};

export const useUpdateQueueItemPriority = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: ({ id, priority }: { id: number; priority: 1 | 2 | 3 }) =>
			apiClient.updateQueueItemPriority(id, priority),
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["queue"] });
		},
	});
};

export const useBulkUpdateQueueItemPriority = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: ({ ids, priority }: { ids: number[]; priority: 1 | 2 | 3 }) =>
			apiClient.bulkUpdateQueueItemPriority(ids, priority),
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["queue"] });
		},
	});
};

export const useBulkCancelQueueItems = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: (ids: number[]) => apiClient.cancelBulkQueueItems(ids),
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["queue"] });
			queryClient.invalidateQueries({ queryKey: ["queue-stats"] });
		},
	});
};

export const useClearCompletedQueue = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: (olderThan?: string) => apiClient.clearCompletedQueue(olderThan),
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["queue"] });
		},
	});
};

export const useClearFailedQueue = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: (olderThan?: string) => apiClient.clearFailedQueue(olderThan),
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["queue"] });
		},
	});
};

export const useClearPendingQueue = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: (olderThan?: string) => apiClient.clearPendingQueue(olderThan),
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["queue"] });
		},
	});
};

// Health hooks
export const useHealth = (params?: {
	limit?: number;
	offset?: number;
	status?: string;
	since?: string;
	search?: string;
	sort_by?: string;
	sort_order?: "asc" | "desc";
	refetchInterval?: number;
}) => {
	return useQuery({
		queryKey: ["health", params],
		queryFn: () => apiClient.getHealth(params),
		// Poll article progress only while visible records are being checked.
		refetchInterval: (query) =>
			query.state.data?.data?.some((item) => item.status === "checking") ? 1000 : false,
	});
};

export const useHealthStats = () => {
	return useQuery({
		queryKey: ["health", "stats"],
		queryFn: () => apiClient.getHealthStats(),
	});
};

export const usePar2RepairJobs = () => {
	return useQuery({
		queryKey: ["par2repair", "jobs"],
		queryFn: () => apiClient.getPar2RepairJobs(),
		// Repairs are started by imports, playback and health checks, none of
		// which the page hears about — polling is the only way this list learns
		// a job exists, so it has to be quick enough to feel live.
		refetchInterval: (query) =>
			query.state.data?.some((job) => job.status === "running") ? 2000 : 5000,
		// The interval pauses while the tab is hidden, and the app disables
		// focus refetching globally; without this the list is frozen at whatever
		// it showed when the user last looked away, and only a manual reload
		// fixes it.
		refetchOnWindowFocus: true,
		staleTime: 0,
	});
};

export const useTriggerPar2Repair = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: (filePath: string) => apiClient.triggerPar2Repair(filePath),
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["par2repair"] });
		},
	});
};

export const useCancelPar2Repair = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: (id: number) => apiClient.cancelPar2Repair(id),
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["par2repair"] });
		},
	});
};

export const useCancelAllPar2Repairs = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: () => apiClient.cancelAllPar2Repairs(),
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["par2repair"] });
		},
	});
};

export const useResetAllHealthChecks = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: () => apiClient.resetAllHealthChecks(),
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["health"] });
			queryClient.invalidateQueries({ queryKey: ["health", "stats"] });
		},
	});
};

interface RegenerateSymlinksParams {
	filePaths?: string[];
	useImportPath?: boolean;
}

export const useRegenerateSymlinks = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: (params?: RegenerateSymlinksParams) =>
			apiClient.regenerateSymlinks(params?.filePaths, params?.useImportPath),
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["health"] });
			queryClient.invalidateQueries({ queryKey: ["health", "stats"] });
		},
	});
};

export const useDeleteHealthItem = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: ({
			id,
			deleteMeta,
			deleteSymlink,
		}: {
			id: number;
			deleteMeta?: boolean;
			deleteSymlink?: boolean;
		}) => apiClient.deleteHealthItem(id, { deleteMeta, deleteSymlink }),
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["health"] });
		},
	});
};

export const useDeleteBulkHealthItems = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: ({
			filePaths,
			deleteMeta,
			deleteSymlink,
		}: {
			filePaths: string[];
			deleteMeta?: boolean;
			deleteSymlink?: boolean;
		}) => apiClient.deleteBulkHealthItems(filePaths, { deleteMeta, deleteSymlink }),
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["health"] });
		},
	});
};

export const useRestartBulkHealthItems = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: (filePaths: string[]) => apiClient.restartBulkHealthItems(filePaths),
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["health"] });
		},
	});
};

export const useRepairBulkHealthItems = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: (filePaths: string[]) => apiClient.repairBulkHealthItems(filePaths),
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["health"] });
		},
	});
};

export const useRepairHealthItem = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: ({ id, resetRepairRetryCount }: { id: number; resetRepairRetryCount?: boolean }) =>
			apiClient.repairHealthItem(id, resetRepairRetryCount),
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["health"] });
		},
	});
};

export const useCleanupHealth = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: (params?: HealthCleanupRequest) => apiClient.cleanupHealth(params),
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["health"] });
		},
	});
};

// ... surrounding code ...
export const useAddHealthCheck = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: (data: { file_path: string; source_nzb_path: string; priority?: HealthPriority }) =>
			apiClient.addHealthCheck(data),
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["health"] });
		},
	});
};

export const useHealthWorkerStatus = () => {
	return useQuery({
		queryKey: ["health", "worker", "status"],
		queryFn: () => apiClient.getHealthWorkerStatus(),
		refetchInterval: 5000,
	});
};

export const usePoolMetrics = () => {
	return useQuery({
		queryKey: ["system", "pool", "metrics"],
		queryFn: () => apiClient.getPoolMetrics(),
		refetchInterval: 5000, // Refetch every 5 seconds
	});
};

export const useProviderHistoricalStats = (days = 30, interval = "daily") => {
	return useQuery({
		queryKey: ["system", "provider-stats", days, interval],
		queryFn: () => apiClient.getProviderHistoricalStats(days, interval),
		refetchInterval: 60000, // Refetch every minute
		// Keep previous range visible while a new range loads (no spinner flash).
		placeholderData: keepPreviousData,
	});
};

export const useProviderSpeedHistory = (days = 30) => {
	return useQuery({
		queryKey: ["system", "provider-speed-history", days],
		queryFn: () => apiClient.getProviderSpeedHistory(days),
		refetchInterval: 60000, // Refetch every minute
		placeholderData: keepPreviousData,
	});
};

export const useTestProviderSpeed = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: (id: string) => apiClient.testProviderSpeed(id),
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["system", "provider-speed-history"] });
			queryClient.invalidateQueries({ queryKey: ["system", "pool", "metrics"] });
		},
	});
};

export const useActiveStreams = () => {
	return useQuery({
		queryKey: ["files", "active-streams"],
		queryFn: () => apiClient.getActiveStreams(),
		refetchInterval: 3000, // Refetch every 3 seconds for liveliness
	});
};

export const useDirectHealthCheck = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: ({ id, verifyContent }: { id: number; verifyContent?: boolean }) =>
			apiClient.directHealthCheck(id, verifyContent),
		onSuccess: () => {
			// Immediately refresh health data to show "checking" status
			queryClient.invalidateQueries({ queryKey: ["health"] });
			queryClient.invalidateQueries({ queryKey: ["health", "worker", "status"] });
		},
	});
};

export const useSetHealthPriority = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: ({ id, priority }: { id: number; priority: HealthPriority }) =>
			apiClient.setHealthPriority(id, priority),
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["health"] });
		},
	});
};

export const useUnmaskHealthItem = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: (id: number) => apiClient.unmaskHealthItem(id),
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["health"] });
		},
	});
};

export const useCancelHealthCheck = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: (id: number) => apiClient.cancelHealthCheck(id),
		onSuccess: () => {
			// Immediately refresh health data to show cancelled status
			queryClient.invalidateQueries({ queryKey: ["health"] });
			queryClient.invalidateQueries({ queryKey: ["health", "worker", "status"] });
		},
	});
};

// Manual Scan hooks
export const useScanStatus = (refetchInterval?: number) => {
	return useQuery({
		queryKey: ["scan", "status"],
		queryFn: () => apiClient.getScanStatus(),
		refetchInterval: refetchInterval,
	});
};

export const useStartManualScan = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: (path: string) => apiClient.startManualScan({ path }),
		onSuccess: () => {
			// Invalidate scan status to update immediately
			queryClient.invalidateQueries({ queryKey: ["scan", "status"] });
			// Invalidate queue to refresh when scan completes
			queryClient.invalidateQueries({ queryKey: ["queue"] });
		},
	});
};

export const useCancelScan = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: () => apiClient.cancelScan(),
		onSuccess: () => {
			// Invalidate scan status to update immediately
			queryClient.invalidateQueries({ queryKey: ["scan", "status"] });
		},
	});
};

export const useImportHistory = (limit?: number, refetchInterval?: number) => {
	return useQuery({
		queryKey: ["import", "history", limit],
		queryFn: () => apiClient.getImportHistory(limit),
		refetchInterval: refetchInterval,
	});
};

// NZBDav Import hooks
export const useNzbdavImportStatus = (refetchInterval?: number) => {
	return useQuery({
		queryKey: ["import", "nzbdav", "status"],
		queryFn: () => apiClient.getNzbdavImportStatus(),
		refetchInterval: refetchInterval,
	});
};

export const useResetNzbdavImportStatus = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: () => apiClient.resetNzbdavImportStatus(),
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["import", "nzbdav", "status"] });
		},
	});
};

export const useCancelNzbdavImport = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: () => apiClient.cancelNzbdavImport(),
		onSuccess: () => {
			// Invalidate scan status to update immediately
			queryClient.invalidateQueries({ queryKey: ["import", "nzbdav", "status"] });
		},
	});
};

export const useClearPendingNzbdavMigrations = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: () => apiClient.clearPendingNzbdavMigrations(),
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["import", "nzbdav", "status"] });
		},
	});
};

export const useClearAllNzbdavMigrations = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: () => apiClient.clearAllNzbdavMigrations(),
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["import", "nzbdav", "status"] });
		},
	});
};

export const useMigrateNzbdavSymlinks = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: (req: { libraryPath: string; sourceMountPath: string; dryRun: boolean }) =>
			apiClient.migrateNzbdavSymlinks({
				library_path: req.libraryPath,
				source_mount_path: req.sourceMountPath,
				dry_run: req.dryRun,
			}),
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["import", "nzbdav", "status"] });
		},
	});
};

// Native upload hook (using JWT authentication)
export const useUploadToQueue = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: ({
			file,
			category,
			priority,
			relativePath,
		}: {
			file: File;
			category?: string;
			priority?: number;
			relativePath?: string;
		}) => apiClient.uploadToQueue(file, category, priority, relativePath),
		onSuccess: () => {
			// Invalidate queue data to show newly uploaded files
			queryClient.invalidateQueries({ queryKey: ["queue"] });
			queryClient.invalidateQueries({ queryKey: ["queue", "stats"] });
		},
	});
};

export const useUploadNZBLnks = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: ({
			links,
			category,
			priority,
			relativePath,
		}: {
			links: string[];
			category?: string;
			priority?: number;
			relativePath?: string;
		}) => apiClient.uploadNZBLnks(links, category, priority, relativePath),
		onSuccess: () => {
			// Invalidate queue data to show newly added items
			queryClient.invalidateQueries({ queryKey: ["queue"] });
			queryClient.invalidateQueries({ queryKey: ["queue", "stats"] });
		},
	});
};

export const useSearchNZBByName = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: ({
			name,
			password,
			category,
			priority,
			relativePath,
		}: {
			name: string;
			password?: string;
			category?: string;
			priority?: number;
			relativePath?: string;
		}) => apiClient.searchNZBByName(name, password, category, priority, relativePath),
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["queue"] });
			queryClient.invalidateQueries({ queryKey: ["queue", "stats"] });
		},
	});
};

export const useAddTestQueueItem = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: (size: "100MB" | "1GB" | "10GB") => apiClient.addTestQueueItem(size),
		onSuccess: () => {
			// Invalidate queue data to show newly added test file
			queryClient.invalidateQueries({ queryKey: ["queue"] });
			queryClient.invalidateQueries({ queryKey: ["queue", "stats"] });
		},
	});
};

export const useSystemBrowse = (path?: string) => {
	return useQuery({
		queryKey: ["system", "browse", path],
		queryFn: () => apiClient.getSystemBrowse(path),
	});
};

export const useResetSystemStats = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: (params?: {
			duration?: string;
			reset_peak?: boolean;
			reset_totals?: boolean;
			reset_history?: boolean;
			reset_queue?: boolean;
			reset_provider_errors?: boolean;
		}) => apiClient.resetSystemStats(params),
		onSuccess: () => {
			// Invalidate all relevant metrics and history to show reset values
			queryClient.invalidateQueries({ queryKey: ["system", "pool", "metrics"] });
			queryClient.invalidateQueries({ queryKey: ["queue", "stats"] });
			queryClient.invalidateQueries({ queryKey: ["queue", "history"] });
			queryClient.invalidateQueries({ queryKey: ["import", "history"] });
		},
	});
};

// ARR Webhook Registration hook
export const useRegisterArrsWebhooks = () => {
	return useMutation({
		mutationFn: () => apiClient.registerArrsWebhooks(),
	});
};

// ARR Download Client Registration hook
export const useRegisterArrsDownloadClients = () => {
	return useMutation({
		mutationFn: () => apiClient.registerArrsDownloadClients(),
	});
};

// ARR Download Client Test hook
export const useTestArrsDownloadClients = () => {
	return useMutation({
		mutationFn: () => apiClient.testArrsDownloadClients(),
	});
};

// Indexer health hooks
export const useIndexerStats = () => {
	return useQuery({
		queryKey: ["system", "indexer-stats"],
		queryFn: () => apiClient.getIndexerStats(),
		refetchInterval: 10 * 1000, // Refetch every 10 seconds
	});
};

export const useCleanupIndexerStats = () => {
	const queryClient = useQueryClient();

	return useMutation({
		mutationFn: (params: { hours?: number; indexer?: string }) =>
			apiClient.cleanupIndexerStats(params),
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["system", "indexer-stats"] });
		},
	});
};
