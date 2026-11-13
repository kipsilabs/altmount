import type {
	ActiveStream,
	APIResponse,
	AuthResponse,
	ChangeOwnPasswordRequest,
	CorruptedMetadataStats,
	FileHealth,
	FileMetadata,
	FuseStatus,
	HealthCheckRequest,
	HealthCleanupRequest,
	HealthCleanupResponse,
	HealthPriority,
	HealthStats,
	HealthWorkerStatus,
	ImportHistoryItem,
	ImportStatusResponse,
	LibraryAudit,
	LibraryAuditCleanupRequest,
	LibraryAuditCleanupResult,
	LibrarySyncStatus,
	ManualScanRequest,
	MetadataMigrationResult,
	MetadataMigrationStatus,
	NzbdavMigrateSymlinksRequest,
	NzbdavMigrateSymlinksResponse,
	Par2RepairJob,
	PoolMetrics,
	ProviderHistoricalStatsResponse,
	ProviderSpeedTestHistoryResponse,
	QueueHistoricalStatsResponse,
	QueueItem,
	QueueStats,
	ScanStatusResponse,
	SystemBrowseResponse,
	UploadNZBLnkResponse,
	User,
} from "../types/api";

import type {
	ConfigResponse,
	ConfigSection,
	ConfigUpdateRequest,
	EvaluateReleaseRequest,
	EvaluateReleaseResponse,
	InspectSearchRequest,
	InspectSearchResponse,
	PipelineTuneResponse,
	ProviderBackbone,
	ProviderConfig,
	ProviderCreateRequest,
	ProviderReorderRequest,
	ProviderTestRequest,
	ProviderTestResponse,
	ProviderUpdateRequest,
	StremioDisplayConfig,
	StremioDisplayPresetsResponse,
	StremioDisplayPreview,
} from "../types/config";

import type {
	PeernetCreateInviteRequest,
	PeernetCreateNetworkRequest,
	PeernetInvite,
	PeernetInviteLink,
	PeernetJoinNetworkRequest,
	PeernetMember,
	PeernetNetworkStatus,
	PeernetPeer,
	PeernetStatus,
	PeernetUpdateNetworkRequest,
} from "../types/peernet";
import type { UpdateChannel, UpdateStatusResponse } from "../types/update";

export interface LogEntry {
	time: string;
	level: string;
	msg: string;
	attrs?: Record<string, unknown>;
	[key: string]: unknown;
}

export class APIError extends Error {
	public status: number;
	public details: string;

	constructor(status: number, message: string, details: string) {
		super(message);
		this.status = status;
		this.name = "APIError";
		this.details = details;
	}
}

/**
 * Shape of the JSON error envelope returned by the server. The `error` field
 * may be a structured object ({ message, details }) or a plain string, with
 * top-level `message`/`details` used as fallbacks.
 */
interface ApiErrorEnvelope {
	error?: { message?: string; details?: string } | string;
	message?: string;
	details?: string;
}

/**
 * Builds an APIError from a parsed error response body, applying the standard
 * fallback chain: nested error.message → top-level message → HTTP status text;
 * details similarly (nested error.details → top-level details → "").
 */
function extractApiError(status: number, statusText: string, body: ApiErrorEnvelope): APIError {
	const nestedError = typeof body.error === "object" ? body.error : undefined;
	const errorMessage =
		(typeof body.error === "object" ? nestedError?.message : body.error) ||
		body.message ||
		`HTTP ${status}: ${statusText}`;
	const errorDetails = nestedError?.details || body.details || "";

	return new APIError(status, errorMessage, errorDetails);
}

class APIClient {
	private baseURL: string;

	constructor(baseURL = "/api") {
		this.baseURL = baseURL;
	}

	private async request<T>(endpoint: string, options: RequestInit = {}): Promise<T> {
		const url = `${this.baseURL}${endpoint}`;

		const config: RequestInit = {
			credentials: "include", // Include cookies for Safari compatibility
			cache: "no-store",
			headers: {
				"Content-Type": "application/json",
				...options.headers,
			},
			...options,
		};

		try {
			const response = await fetch(url, config);

			// Auth proxy (e.g. Authelia) issues a 302 to the login page. With
			// redirect: "manual" in the service worker the SW returns an opaque
			// redirect here instead of following it cross-origin (which would
			// CORS-fail). Reload so the browser navigates through Authelia.
			if (response.type === "opaqueredirect") {
				window.location.reload();
				throw new APIError(302, "Session expired, redirecting to login", "");
			}

			if (!response.ok) {
				if (response.status === 401) {
					window.dispatchEvent(new CustomEvent("api:unauthorized"));
				}
				let errorData: ApiErrorEnvelope = {};
				try {
					errorData = await response.json();
				} catch {
					// empty or non-JSON error body — fall through to status-based message
				}

				throw extractApiError(response.status, response.statusText, errorData);
			}

			if (response.status === 204) {
				return undefined as T;
			}

			const data: APIResponse<T> = await response.json();

			if (!data.success) {
				// Handle error in the success=false format
				const errorMessage =
					(typeof data.error === "object" ? data.error?.message : data.error) ||
					"API request failed";
				const errorDetails = typeof data.error === "object" ? data.error?.details || "" : "";
				throw new APIError(response.status, errorMessage, errorDetails);
			}

			return data.data as T;
		} catch (error) {
			if (error instanceof APIError) {
				throw error;
			}
			throw new APIError(0, error instanceof Error ? error.message : "Network error", "");
		}
	}

	private async requestWithMeta<T>(
		endpoint: string,
		options: RequestInit = {},
	): Promise<APIResponse<T>> {
		const url = `${this.baseURL}${endpoint}`;

		const config: RequestInit = {
			credentials: "include", // Include cookies for Safari compatibility
			cache: "no-store",
			headers: {
				"Content-Type": "application/json",
				...options.headers,
			},
			...options,
		};

		try {
			const response = await fetch(url, config);

			// Auth proxy (e.g. Authelia) issues a 302 to the login page. With
			// redirect: "manual" in the service worker the SW returns an opaque
			// redirect here instead of following it cross-origin (which would
			// CORS-fail). Reload so the browser navigates through Authelia.
			if (response.type === "opaqueredirect") {
				window.location.reload();
				throw new APIError(302, "Session expired, redirecting to login", "");
			}

			if (!response.ok) {
				if (response.status === 401) {
					window.dispatchEvent(new CustomEvent("api:unauthorized"));
				}
				// Try to parse error response
				try {
					const errorData: ApiErrorEnvelope = await response.json();
					throw extractApiError(response.status, response.statusText, errorData);
				} catch (e) {
					if (e instanceof APIError) {
						throw e;
					}
					// If parsing fails, use generic error
					throw new APIError(
						response.status,
						`HTTP ${response.status}: ${response.statusText}`,
						"",
					);
				}
			}

			const data: APIResponse<T> = await response.json();

			if (!data.success) {
				// Handle error in the success=false format
				const errorMessage =
					(typeof data.error === "object" ? data.error?.message : data.error) ||
					"API request failed";
				const errorDetails = typeof data.error === "object" ? data.error?.details || "" : "";
				throw new APIError(response.status, errorMessage, errorDetails);
			}

			return data;
		} catch (error) {
			if (error instanceof APIError) {
				throw error;
			}
			throw new APIError(0, error instanceof Error ? error.message : "Network error", "");
		}
	}

	// Queue endpoints
	async getQueue(params?: {
		limit?: number;
		offset?: number;
		status?: string;
		since?: string;
		search?: string;
		sort_by?: string;
		sort_order?: "asc" | "desc";
	}) {
		const searchParams = new URLSearchParams();
		if (params?.limit) searchParams.set("limit", params.limit.toString());
		if (params?.offset !== undefined) searchParams.set("offset", params.offset.toString());
		if (params?.status) searchParams.set("status", params.status);
		if (params?.since) searchParams.set("since", params.since);
		if (params?.search) searchParams.set("search", params.search);
		if (params?.sort_by) searchParams.set("sort_by", params.sort_by);
		if (params?.sort_order) searchParams.set("sort_order", params.sort_order);

		const query = searchParams.toString();
		return this.requestWithMeta<QueueItem[]>(`/queue${query ? `?${query}` : ""}`);
	}

	async deleteQueueItem(id: number) {
		return this.request<QueueItem>(`/queue/${id}`, { method: "DELETE" });
	}

	async deleteBulkQueueItems(ids: number[]) {
		return this.request<{ deleted_count: number; message: string }>("/queue/bulk", {
			method: "DELETE",
			headers: {
				"Content-Type": "application/json",
			},
			body: JSON.stringify({ ids }),
		});
	}

	async restartBulkQueueItems(ids: number[]) {
		return this.request<{ restarted_count: number; message: string }>("/queue/bulk/restart", {
			method: "POST",
			headers: {
				"Content-Type": "application/json",
			},
			body: JSON.stringify({ ids }),
		});
	}

	async retryQueueItem(id: number) {
		return this.request<QueueItem>(`/queue/${id}/retry`, {
			method: "POST",
		});
	}

	async cancelQueueItem(id: number) {
		return this.request<{ message: string; id: number }>(`/queue/${id}/cancel`, {
			method: "POST",
		});
	}

	async updateQueueItemPriority(id: number, priority: 1 | 2 | 3) {
		return this.request<QueueItem>(`/queue/${id}/priority`, {
			method: "PATCH",
			body: JSON.stringify({ priority }),
		});
	}

	async bulkUpdateQueueItemPriority(ids: number[], priority: 1 | 2 | 3) {
		return this.request<{ updated_count: number; skipped_count: number; message: string }>(
			"/queue/bulk/priority",
			{
				method: "PATCH",
				body: JSON.stringify({ ids, priority }),
			},
		);
	}

	async cancelBulkQueueItems(ids: number[]) {
		return this.request<{
			cancelled_count: number;
			not_processing_count: number;
			not_found_count: number;
			results: Record<string, string>;
			message: string;
		}>("/queue/bulk/cancel", {
			method: "POST",
			body: JSON.stringify({ ids }),
		});
	}

	async getQueueStats() {
		return this.request<QueueStats>("/queue/stats");
	}

	async getQueueHistory(days?: number) {
		const searchParams = new URLSearchParams();
		if (days) searchParams.set("days", days.toString());

		const query = searchParams.toString();
		return this.request<QueueHistoricalStatsResponse>(
			`/queue/stats/history${query ? `?${query}` : ""}`,
		);
	}

	async clearCompletedQueue(olderThan?: string) {
		const searchParams = new URLSearchParams();
		if (olderThan) searchParams.set("older_than", olderThan);

		const query = searchParams.toString();
		return this.request<QueueStats>(`/queue/completed${query ? `?${query}` : ""}`, {
			method: "DELETE",
		});
	}

	async clearFailedQueue(olderThan?: string) {
		const searchParams = new URLSearchParams();
		if (olderThan) searchParams.set("older_than", olderThan);

		const query = searchParams.toString();
		return this.request<QueueStats>(`/queue/failed${query ? `?${query}` : ""}`, {
			method: "DELETE",
		});
	}

	async clearPendingQueue(olderThan?: string) {
		const searchParams = new URLSearchParams();
		if (olderThan) searchParams.set("older_than", olderThan);

		const query = searchParams.toString();
		return this.request<QueueStats>(`/queue/pending${query ? `?${query}` : ""}`, {
			method: "DELETE",
		});
	}

	// Health endpoints
	async getHealth(params?: {
		limit?: number;
		offset?: number;
		status?: string;
		since?: string;
		search?: string;
		sort_by?: string;
		sort_order?: "asc" | "desc";
	}) {
		const searchParams = new URLSearchParams();
		if (params?.limit) searchParams.set("limit", params.limit.toString());
		if (params?.offset !== undefined) searchParams.set("offset", params.offset.toString());
		if (params?.status) searchParams.set("status", params.status);
		if (params?.since) searchParams.set("since", params.since);
		if (params?.search) searchParams.set("search", params.search);
		if (params?.sort_by) searchParams.set("sort_by", params.sort_by);
		if (params?.sort_order) searchParams.set("sort_order", params.sort_order);

		const query = searchParams.toString();
		return this.requestWithMeta<FileHealth[]>(`/health${query ? `?${query}` : ""}`);
	}

	async deleteHealthItem(id: number, options?: { deleteMeta?: boolean; deleteSymlink?: boolean }) {
		const searchParams = new URLSearchParams();
		if (options?.deleteMeta) searchParams.set("delete_meta", "true");
		if (options?.deleteSymlink) searchParams.set("delete_symlink", "true");
		const query = searchParams.toString();
		return this.request<FileHealth>(`/health/${id}${query ? `?${query}` : ""}`, {
			method: "DELETE",
		});
	}

	async deleteBulkHealthItems(
		filePaths: string[],
		options?: { deleteMeta?: boolean; deleteSymlink?: boolean },
	) {
		return this.request<{
			message: string;
			deleted_count: number;
			file_paths: string[];
			deleted_at: string;
			meta_deleted_count?: number;
			symlink_deleted_count?: number;
		}>("/health/bulk/delete", {
			method: "POST",
			body: JSON.stringify({
				file_paths: filePaths,
				delete_meta: options?.deleteMeta ?? false,
				delete_symlink: options?.deleteSymlink ?? false,
			}),
		});
	}

	async restartBulkHealthItems(filePaths: string[]) {
		return this.request<{
			message: string;
			restarted_count: number;
			file_paths: string[];
			restarted_at: string;
		}>("/health/bulk/restart", {
			method: "POST",
			body: JSON.stringify({ file_paths: filePaths }),
		});
	}

	async repairBulkHealthItems(filePaths: string[]) {
		return this.request<{
			message: string;
			success_count: number;
			failed_count: number;
			errors: Record<string, string>;
		}>("/health/bulk/repair", {
			method: "POST",
			body: JSON.stringify({ file_paths: filePaths }),
		});
	}

	async repairHealthItem(id: number, resetRepairRetryCount?: boolean) {
		return this.request<FileHealth>(`/health/${id}/repair`, {
			method: "POST",
			body: JSON.stringify({ reset_repair_retry_count: resetRepairRetryCount }),
		});
	}

	async getHealthStats() {
		return this.request<HealthStats>("/health/stats");
	}

	// PAR2 repair endpoints
	async getPar2RepairJobs(limit?: number) {
		const query = limit ? `?limit=${limit}` : "";
		return this.request<Par2RepairJob[]>(`/par2repair${query}`);
	}

	async triggerPar2Repair(filePath: string) {
		return this.request<{ message: string }>("/par2repair", {
			method: "POST",
			body: JSON.stringify({ file_path: filePath }),
		});
	}

	async cancelPar2Repair(id: number) {
		return this.request<{ message: string }>(`/par2repair/${id}`, {
			method: "DELETE",
		});
	}

	async cancelAllPar2Repairs() {
		return this.request<{ cancelled: number }>("/par2repair", {
			method: "DELETE",
		});
	}

	async resetAllHealthChecks() {
		return this.request<{
			message: string;
			restarted_count: number;
			restarted_at: string;
		}>("/health/reset-all", {
			method: "POST",
		});
	}

	async regenerateSymlinks(filePaths?: string[], useImportPath?: boolean) {
		return this.request<{
			message: string;
			files_processed: number;
			success_count: number;
			errors: string[];
			error_count: number;
			warning?: string;
			completed_at: string;
		}>("/health/regenerate-symlinks", {
			method: "POST",
			body: JSON.stringify({ file_paths: filePaths, use_import_path: useImportPath ?? false }),
		});
	}

	async cleanupHealth(params?: HealthCleanupRequest) {
		return this.request<HealthCleanupResponse>("/health/cleanup", {
			method: "DELETE",
			body: JSON.stringify(params),
		});
	}

	async addHealthCheck(data: HealthCheckRequest) {
		return this.request<{ message: string }>("/health/check", {
			method: "POST",
			body: JSON.stringify(data),
		});
	}

	async getHealthWorkerStatus() {
		return this.request<HealthWorkerStatus>("/health/worker/status");
	}

	async scanLibraryAudit(graceHours?: number) {
		return this.request<LibraryAudit>("/health/library-audit/scan", {
			method: "POST",
			body: JSON.stringify(graceHours === undefined ? {} : { grace_hours: graceHours }),
		});
	}

	async cleanupLibraryAudit(data: LibraryAuditCleanupRequest) {
		return this.request<LibraryAuditCleanupResult>("/health/library-audit/cleanup", {
			method: "POST",
			body: JSON.stringify(data),
		});
	}

	async getLibrarySyncStatus() {
		return this.request<LibrarySyncStatus>("/health/library-sync/status");
	}

	async startLibrarySync() {
		return this.request<{ message: string }>("/health/library-sync/start", {
			method: "POST",
		});
	}

	async cancelLibrarySync() {
		return this.request<{ message: string }>("/health/library-sync/cancel", {
			method: "POST",
		});
	}

	async getCorruptedMetadataStats() {
		return this.request<CorruptedMetadataStats>("/metadata/corrupted");
	}

	async purgeCorruptedMetadata() {
		return this.request<void>("/metadata/corrupted", { method: "DELETE" });
	}

	async getMetadataMigrationStatus() {
		return this.request<MetadataMigrationStatus>("/metadata/migration/status");
	}

	async dryRunMetadataMigration() {
		return this.request<MetadataMigrationResult>("/metadata/migration/dry-run", {
			method: "POST",
		});
	}

	async startMetadataMigration() {
		return this.request<{ message: string }>("/metadata/migration/start", {
			method: "POST",
		});
	}

	async cancelMetadataMigration() {
		return this.request<{ message: string }>("/metadata/migration/cancel", {
			method: "POST",
		});
	}

	async getPoolMetrics() {
		return this.request<PoolMetrics>("/system/pool/metrics");
	}

	async getProviderHistoricalStats(days = 30, interval = "daily") {
		return this.request<ProviderHistoricalStatsResponse>(
			`/system/provider-stats?days=${days}&interval=${interval}`,
		);
	}

	async getProviderSpeedHistory(days = 30) {
		return this.request<ProviderSpeedTestHistoryResponse>(
			`/system/provider-speed-history?days=${days}`,
		);
	}

	async directHealthCheck(id: number, verifyContent?: boolean) {
		return this.request<{
			message: string;
			id: number;
			file_path: string;
			old_status: string;
			new_status: string;
			checked_at: string;
			health_data: FileHealth;
		}>(`/health/${id}/check-now`, {
			method: "POST",
			...(verifyContent === undefined
				? {}
				: { body: JSON.stringify({ verify_content: verifyContent }) }),
		});
	}

	async setHealthPriority(id: number, priority: HealthPriority) {
		return this.request<{
			success: boolean;
			message: string;
			priority: HealthPriority;
		}>(`/health/${id}/priority`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ priority }),
		});
	}

	async unmaskHealthItem(id: number) {
		return this.request<{
			message: string;
			id: number;
			file_path: string;
			updated_at: string;
			health_data: FileHealth;
		}>(`/health/${id}/unmask`, {
			method: "POST",
		});
	}

	async cancelHealthCheck(id: number) {
		return this.request<{
			message: string;
			id: number;
			file_path: string;
			old_status: string;
			new_status: string;
			cancelled_at: string;
			health_data: FileHealth;
		}>(`/health/${id}/cancel`, {
			method: "POST",
		});
	}

	// File metadata endpoints
	async getFileMetadata(path: string) {
		return this.request<FileMetadata>(`/files/info?path=${encodeURIComponent(path)}`);
	}

	async getActiveStreams() {
		return this.request<ActiveStream[]>("/files/active-streams");
	}

	async exportMetadataToNZB(path: string): Promise<Blob> {
		const url = `${this.baseURL}/files/export-nzb?path=${encodeURIComponent(path)}`;

		const response = await fetch(url, {
			credentials: "include",
			headers: {
				Accept: "application/x-nzb",
			},
		});

		if (!response.ok) {
			const errorData = await response.json();
			throw new APIError(
				response.status,
				errorData.message || `HTTP ${response.status}: ${response.statusText}`,
				errorData.details || "",
			);
		}

		return response.blob();
	}

	async batchExportNZBs(rootPath: string): Promise<Blob> {
		const url = `${this.baseURL}/files/export-batch`;

		const response = await fetch(url, {
			method: "POST",
			credentials: "include",
			headers: {
				"Content-Type": "application/json",
				Accept: "application/zip",
			},
			body: JSON.stringify({
				root_path: rootPath,
			}),
		});

		if (!response.ok) {
			const errorData = await response.json();
			throw new APIError(
				response.status,
				errorData.message || `HTTP ${response.status}: ${response.statusText}`,
				errorData.details || "",
			);
		}

		return response.blob();
	}

	// Authentication endpoints
	async getCurrentUser() {
		return this.request<User>("/user");
	}

	async refreshToken() {
		return this.request<AuthResponse>("/user/refresh", {
			method: "POST",
		});
	}

	async logout() {
		return this.request<AuthResponse>("/user/logout", {
			method: "POST",
		});
	}

	async regenerateAPIKey() {
		return this.request<{ api_key: string; message: string }>("/user/api-key/regenerate", {
			method: "POST",
		});
	}

	async changeOwnPassword(data: ChangeOwnPasswordRequest) {
		return this.request<AuthResponse>("/user/password", {
			method: "PUT",
			body: JSON.stringify(data),
		});
	}

	async registerArrsWebhooks() {
		return this.request<{ message: string }>("/arrs/webhook/register", {
			method: "POST",
		});
	}

	async registerArrsDownloadClients() {
		return this.request<{ message: string }>("/arrs/download-client/register", {
			method: "POST",
		});
	}

	async testArrsDownloadClients() {
		return this.request<Record<string, string>>("/arrs/download-client/test", {
			method: "POST",
		});
	}

	// Direct authentication methods
	async login(username: string, password: string) {
		return this.request<AuthResponse>("/auth/login", {
			method: "POST",
			body: JSON.stringify({ username, password }),
		});
	}

	async register(username: string, email: string | undefined, password: string) {
		return this.request<AuthResponse>("/auth/register", {
			method: "POST",
			body: JSON.stringify({
				username,
				email: email || undefined,
				password,
			}),
		});
	}

	async checkRegistrationStatus() {
		return this.request<{ registration_enabled: boolean; user_count: number }>(
			"/auth/registration-status",
		);
	}

	async getAuthConfig() {
		return this.request<{ login_required: boolean }>("/auth/config");
	}

	async resetAdminPassword(username: string, newPassword: string) {
		return this.request<{ message: string }>("/auth/reset-admin-password", {
			method: "POST",
			body: JSON.stringify({ username, new_password: newPassword }),
		});
	}

	// Configuration endpoints
	async getConfig() {
		return this.request<ConfigResponse>("/config");
	}

	async updateConfigSection(section: ConfigSection, config: ConfigUpdateRequest) {
		return this.request<ConfigResponse>(`/config/${section}`, {
			method: "PATCH",
			body: JSON.stringify(config),
		});
	}

	// Peer network
	async getPeernetStatus() {
		return this.request<PeernetStatus>("/peernet/status");
	}

	async getPeernetPeers(networkId: string) {
		return this.request<PeernetPeer[]>(`/peernet/networks/${encodeURIComponent(networkId)}/peers`);
	}

	async confirmPeernetPeer(networkId: string, address: string) {
		return this.request<void>(`/peernet/networks/${encodeURIComponent(networkId)}/peers/confirm`, {
			method: "POST",
			body: JSON.stringify({ address }),
		});
	}

	async createPeernetNetwork(body: PeernetCreateNetworkRequest) {
		return this.request<PeernetNetworkStatus>("/peernet/networks", {
			method: "POST",
			body: JSON.stringify(body),
		});
	}

	async joinPeernetNetwork(body: PeernetJoinNetworkRequest) {
		return this.request<PeernetNetworkStatus>("/peernet/networks/join", {
			method: "POST",
			body: JSON.stringify(body),
		});
	}

	async updatePeernetNetwork(networkId: string, body: PeernetUpdateNetworkRequest) {
		return this.request<PeernetNetworkStatus>(
			`/peernet/networks/${encodeURIComponent(networkId)}`,
			{
				method: "PATCH",
				body: JSON.stringify(body),
			},
		);
	}

	async deletePeernetNetwork(networkId: string) {
		return this.request<void>(`/peernet/networks/${encodeURIComponent(networkId)}`, {
			method: "DELETE",
		});
	}

	async getPeernetInvites(networkId: string) {
		return this.request<PeernetInvite[]>(
			`/peernet/networks/${encodeURIComponent(networkId)}/invites`,
		);
	}

	async createPeernetInvite(networkId: string, body: PeernetCreateInviteRequest) {
		return this.request<PeernetInviteLink>(
			`/peernet/networks/${encodeURIComponent(networkId)}/invites`,
			{ method: "POST", body: JSON.stringify(body) },
		);
	}

	async deletePeernetInvite(networkId: string, inviteId: string) {
		return this.request<void>(
			`/peernet/networks/${encodeURIComponent(networkId)}/invites/${encodeURIComponent(inviteId)}`,
			{ method: "DELETE" },
		);
	}

	async getPeernetMembers(networkId: string) {
		return this.request<PeernetMember[]>(
			`/peernet/networks/${encodeURIComponent(networkId)}/members`,
		);
	}

	async deletePeernetMember(networkId: string, nodeId: string) {
		return this.request<void>(
			`/peernet/networks/${encodeURIComponent(networkId)}/members/${encodeURIComponent(nodeId)}`,
			{ method: "DELETE" },
		);
	}

	async reloadConfig() {
		return this.request<ConfigResponse>("/config/reload", {
			method: "POST",
		});
	}

	// System endpoints
	async restartServer(force = false) {
		return this.request<{ message: string; timestamp: string }>("/system/restart", {
			method: "POST",
			body: JSON.stringify({ force }),
		});
	}

	async getSystemBrowse(path?: string) {
		const searchParams = new URLSearchParams();
		if (path) searchParams.set("path", path);

		const query = searchParams.toString();
		return this.request<SystemBrowseResponse>(`/system/browse${query ? `?${query}` : ""}`);
	}

	async resetSystemStats(params?: {
		duration?: string;
		reset_peak?: boolean;
		reset_totals?: boolean;
		reset_history?: boolean;
		reset_queue?: boolean;
		reset_provider_errors?: boolean;
	}) {
		const searchParams = new URLSearchParams();
		if (params?.duration) searchParams.set("duration", params.duration);
		if (params?.reset_peak) searchParams.set("reset_peak", "true");
		if (params?.reset_totals) searchParams.set("reset_totals", "true");
		if (params?.reset_history) searchParams.set("reset_history", "true");
		if (params?.reset_queue) searchParams.set("reset_queue", "true");
		if (params?.reset_provider_errors) searchParams.set("reset_provider_errors", "true");

		const query = searchParams.toString();
		return this.request<{ message: string }>(`/system/stats/reset${query ? `?${query}` : ""}`, {
			method: "POST",
		});
	}

	async getIndexerStats() {
		return this.request<
			{
				indexer: string;
				total_imports: number;
				success_count: number;
				failed_count: number;
				last_24h_count: number;
				success_rate: number;
				last_seen_at: string;
			}[]
		>("/system/indexer-stats");
	}

	async cleanupIndexerStats(params: { hours?: number; indexer?: string }) {
		const queryParams = new URLSearchParams();
		if (params.hours !== undefined) queryParams.append("hours", params.hours.toString());
		if (params.indexer !== undefined) queryParams.append("indexer", params.indexer);

		return this.request<{ pruned_rows: number; hours?: number }>(
			`/system/indexer-stats/cleanup?${queryParams.toString()}`,
			{
				method: "DELETE",
			},
		);
	}

	// Provider endpoints
	async testProvider(data: ProviderTestRequest) {
		return this.request<ProviderTestResponse>("/providers/test", {
			method: "POST",
			body: JSON.stringify(data),
		});
	}

	async testProviderSpeed(id: string) {
		return this.request<{ speed_mbps: number; duration_seconds: number }>(
			`/providers/${id}/speedtest`,
			{
				method: "POST",
			},
		);
	}

	async tuneProviderPipeline(id: string) {
		return this.request<PipelineTuneResponse>(`/providers/${id}/tune-pipeline`, {
			method: "POST",
		});
	}

	async getProviderBackbones() {
		return this.request<ProviderBackbone[]>("/providers/backbones");
	}

	async createProvider(data: ProviderCreateRequest) {
		return this.request<ProviderConfig>("/providers", {
			method: "POST",
			body: JSON.stringify(data),
		});
	}

	async updateProvider(id: string, data: Partial<ProviderUpdateRequest>) {
		return this.request<ProviderConfig>(`/providers/${id}`, {
			method: "PUT",
			body: JSON.stringify(data),
		});
	}

	async deleteProvider(id: string) {
		return this.request<{ message: string }>(`/providers/${id}`, {
			method: "DELETE",
		});
	}

	async resetProviderQuota(id: string) {
		return this.request<{ message: string }>(`/providers/${id}/reset-quota`, {
			method: "POST",
		});
	}

	async reorderProviders(data: ProviderReorderRequest) {
		return this.request<ProviderConfig[]>("/providers/reorder", {
			method: "PUT",
			body: JSON.stringify(data),
		});
	}

	// Manual Scan endpoints
	async startManualScan(data: ManualScanRequest) {
		return this.request<ScanStatusResponse>("/import/scan", {
			method: "POST",
			body: JSON.stringify(data),
		});
	}

	async getScanStatus() {
		return this.request<ScanStatusResponse>("/import/scan/status");
	}

	async getImportHistory(limit?: number) {
		const searchParams = new URLSearchParams();
		if (limit) searchParams.set("limit", limit.toString());

		const query = searchParams.toString();
		return this.request<ImportHistoryItem[]>(`/import/history${query ? `?${query}` : ""}`);
	}

	async cancelScan() {
		return this.request<ScanStatusResponse>("/import/scan", {
			method: "DELETE",
		});
	}

	// NZBDav Import endpoints
	async getNzbdavImportStatus() {
		return this.request<ImportStatusResponse>("/import/nzbdav/status");
	}

	async resetNzbdavImportStatus() {
		return this.request<{ message: string }>("/import/nzbdav/reset", {
			method: "POST",
		});
	}

	async cancelNzbdavImport() {
		return this.request<{ message: string }>("/import/nzbdav", {
			method: "DELETE",
		});
	}

	async clearPendingNzbdavMigrations() {
		return this.request<{ message: string; data: { deleted: number } }>(
			"/import/nzbdav/pending-migrations",
			{ method: "DELETE" },
		);
	}

	async clearAllNzbdavMigrations() {
		return this.request<{ message: string; data: { deleted: number } }>(
			"/import/nzbdav/migrations",
			{ method: "DELETE" },
		);
	}

	async migrateNzbdavSymlinks(req: NzbdavMigrateSymlinksRequest) {
		return this.request<NzbdavMigrateSymlinksResponse>("/import/nzbdav/migrate-symlinks", {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify(req),
		});
	}

	// Native upload endpoint using JWT authentication
	async uploadToQueue(
		file: File,
		category?: string,
		priority?: number,
		relativePath?: string,
	): Promise<APIResponse<QueueItem>> {
		const formData = new FormData();
		formData.append("file", file);
		if (category) {
			formData.append("category", category);
		}
		if (priority !== undefined) {
			formData.append("priority", priority.toString());
		}
		if (relativePath) {
			formData.append("relative_path", relativePath);
		}

		return this.request<APIResponse<QueueItem>>("/queue/upload", {
			method: "POST",
			body: formData,
			// Don't set Content-Type header - let browser set it with boundary for multipart/form-data
			headers: {},
		});
	}

	async uploadNZBLnks(
		links: string[],
		category?: string,
		priority?: number,
		relativePath?: string,
	): Promise<UploadNZBLnkResponse> {
		return this.request<UploadNZBLnkResponse>("/queue/upload-nzblnk", {
			method: "POST",
			body: JSON.stringify({
				links,
				category: category || undefined,
				priority: priority ?? undefined,
				relative_path: relativePath || undefined,
			}),
		});
	}

	async searchNZBByName(
		name: string,
		password?: string,
		category?: string,
		priority?: number,
		relativePath?: string,
	): Promise<{ queue_id: number; title: string; indexer: string }> {
		return this.request("/queue/upload-by-name", {
			method: "POST",
			body: JSON.stringify({
				name,
				password: password || undefined,
				category: category || undefined,
				priority: priority ?? undefined,
				relative_path: relativePath || undefined,
			}),
		});
	}

	async addTestQueueItem(size: "100MB" | "1GB" | "10GB") {
		return this.request<APIResponse<QueueItem>>("/queue/test", {
			method: "POST",
			body: JSON.stringify({ size }),
		});
	}

	// FUSE endpoints
	async getFuseStatus() {
		return this.request<FuseStatus>("/fuse/status");
	}

	async startFuseMount(path: string) {
		return this.request<{ message: string }>("/fuse/start", {
			method: "POST",
			body: JSON.stringify({ path }),
		});
	}

	async stopFuseMount() {
		return this.request<{ message: string }>("/fuse/stop", {
			method: "POST",
			body: JSON.stringify({}),
		});
	}

	async forceStopFuseMount() {
		return this.request<{ message: string }>("/fuse/force-stop", {
			method: "POST",
			body: JSON.stringify({}),
		});
	}

	// Update endpoints
	async checkUpdateStatus(channel: UpdateChannel): Promise<UpdateStatusResponse> {
		return this.request<UpdateStatusResponse>(`/system/update/status?channel=${channel}`);
	}

	async applyUpdate(channel: UpdateChannel, force = false): Promise<{ message: string }> {
		return this.request<{ message: string }>("/system/update/apply", {
			method: "POST",
			body: JSON.stringify({ channel, force }),
		});
	}

	async getLogs(params?: { level?: string; limit?: number }): Promise<LogEntry[]> {
		const searchParams = new URLSearchParams();
		if (params?.level) searchParams.set("level", params.level);
		if (params?.limit) searchParams.set("limit", params.limit.toString());
		const query = searchParams.toString();
		return this.request<LogEntry[]>(`/logs${query ? `?${query}` : ""}`);
	}

	// Stremio Search & Ranking Inspector
	async getStremioDisplayPresets(): Promise<StremioDisplayPresetsResponse> {
		return this.request<StremioDisplayPresetsResponse>("/stremio/display/presets");
	}

	async previewStremioDisplay(
		display: StremioDisplayConfig,
		showCachedIndicator?: boolean,
	): Promise<StremioDisplayPreview[]> {
		return this.request<StremioDisplayPreview[]>("/stremio/display/preview", {
			method: "POST",
			body: JSON.stringify({ display, show_cached_indicator: showCachedIndicator }),
		});
	}

	async evaluateStremioRelease(payload: EvaluateReleaseRequest): Promise<EvaluateReleaseResponse> {
		return this.request<EvaluateReleaseResponse>("/stremio/scoring/evaluate", {
			method: "POST",
			body: JSON.stringify(payload),
		});
	}

	async inspectStremioSearch(payload: InspectSearchRequest): Promise<InspectSearchResponse> {
		return this.request<InspectSearchResponse>("/stremio/search/inspect", {
			method: "POST",
			body: JSON.stringify(payload),
		});
	}
}

// Export a default instance
export const apiClient = new APIClient();
