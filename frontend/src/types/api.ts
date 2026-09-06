// Base API Response types
export interface APIResponse<T = unknown> {
	success: boolean;
	data?: T;
	error?:
		| string
		| {
				code: string;
				message: string;
				details: string;
		  };
	meta?: {
		count: number;
		limit: number;
		offset: number;
		total?: number;
	};
}

// Queue types
export const QueueStatus = {
	PENDING: "pending",
	PROCESSING: "processing",
	COMPLETED: "completed",
	FAILED: "failed",
} as const;

export type QueueStatus = (typeof QueueStatus)[keyof typeof QueueStatus];

export interface QueueItem {
	id: number;
	nzb_path: string;
	nzb_display_name: string;
	target_path: string;
	category?: string;
	relative_path?: string;
	priority: number;
	status: QueueStatus;
	created_at: string;
	updated_at: string;
	started_at?: string;
	completed_at?: string;
	retry_count: number;
	max_retries: number;
	error_message?: string;
	batch_id?: string;
	metadata?: string;
	file_size?: number;
	percentage?: number; // Progress percentage (0-100), only present for items being processed
	stage?: string; // Human-readable stage label (e.g. "Validating segments"), injected client-side from live progress
	storage_path?: string; // Internal FUSE mount path (populated after completion)
	indexer?: string;
}

export interface ProgressUpdate {
	id: number;
	percentage: number;
}

export interface ImportHistoryItem {
	id: number;
	nzb_name: string;
	file_name: string;
	virtual_path: string;
	library_path?: string;
	category?: string;
	metadata?: string;
	file_size: number;
	completed_at: string;
	indexer?: string;
}

export interface QueueStats {
	total_queued: number;
	total_processing: number;
	total_completed: number;
	total_failed: number;
	avg_processing_time_ms: number;
	last_updated: string;
}

export interface QueueHistoryRange {
	completed: number;
	failed: number;
	percentage: number;
}

export interface DailyStat {
	day: string;
	completed: number;
	failed: number;
}

export interface QueueHistoricalStatsResponse {
	last_24_hours: QueueHistoryRange;
	last_7_days: QueueHistoryRange;
	last_30_days: QueueHistoryRange;
	last_365_days: QueueHistoryRange;
	daily: DailyStat[];
}

// NZBLNK upload types
export interface NZBLnkResult {
	link: string;
	success: boolean;
	queue_id?: number;
	title?: string;
	error_message?: string;
}

export interface UploadNZBLnkResponse {
	results: NZBLnkResult[];
	success_count: number;
	failed_count: number;
}

// Manual Scan types
export const ScanStatus = {
	IDLE: "idle",
	SCANNING: "scanning",
	CANCELING: "canceling",
} as const;

export type ScanStatus = (typeof ScanStatus)[keyof typeof ScanStatus];

export interface ManualScanRequest {
	path: string;
}

export interface ScanStatusResponse {
	status: ScanStatus;
	path?: string;
	start_time?: string;
	files_found: number;
	files_added: number;
	current_file?: string;
	last_error?: string;
}

// Import Job types
export interface MigrationStats {
	pending: number;
	imported: number;
	failed: number;
	symlinks_migrated: number;
	total: number;
}

export interface ImportStatusResponse {
	status: "idle" | "running" | "canceling" | "completed";
	total: number;
	added: number;
	failed: number;
	skipped?: number;
	last_error?: string;
	migration_stats?: MigrationStats;
}

export interface NzbdavMigrateSymlinksRequest {
	library_path: string;
	source_mount_path: string;
	dry_run: boolean;
}

export interface NzbdavMigrateSymlinksResponse {
	scanned: number;
	matched: number;
	rewritten: number;
	skipped_wrong_prefix?: number;
	unmatched: string[];
	errors: string[];
	dry_run: boolean;
}

// Health types
export const HealthStatus = {
	PENDING: "pending",
	CHECKING: "checking",
	HEALTHY: "healthy",
	CORRUPTED: "corrupted",
	REPAIR_TRIGGERED: "repair_triggered",
} as const;

export type HealthStatus = (typeof HealthStatus)[keyof typeof HealthStatus];

export const HealthPriority = {
	Normal: 0,
	High: 1,
	Next: 2,
} as const;

export type HealthPriority = (typeof HealthPriority)[keyof typeof HealthPriority];

export interface FileHealth {
	id: number;
	file_path: string;
	status: HealthStatus;
	last_checked: string;
	last_error?: string;
	retry_count: number;
	max_retries: number;
	source_nzb_path?: string;
	library_path?: string;
	error_details?: string;
	repair_retry_count: number;
	max_repair_retries: number;
	created_at: string;
	updated_at: string;
	scheduled_check_at?: string;
	priority: HealthPriority;
	// Failure masking fields
	streaming_failure_count: number;
	is_masked: boolean;
	indexer?: string;
}

export interface HealthStats {
	total: number;
	pending: number;
	healthy: number;
	corrupted: number;
	repair_triggered: number;
	checking: number;
}

export interface HealthCleanupRequest {
	older_than?: string;
	status?: HealthStatus;
	delete_files?: boolean;
}

export interface HealthCleanupResponse {
	records_deleted: number;
	files_deleted?: number;
	older_than: string;
	status_filter?: HealthStatus;
	file_deletion_errors?: string[];
	warning?: string;
}

// File metadata types
export interface SegmentInfo {
	message_id: string;
	segment_size: number;
	start_offset: number;
	end_offset: number;
	available: boolean;
}

export interface NestedSegmentInfo {
	message_id: string;
	segment_size: number;
	start_offset: number;
	end_offset: number;
}

export interface NestedSourceInfo {
	volume_index: number;
	inner_length: number;
	inner_volume_size: number;
	encrypted: boolean;
	segment_count: number;
	segments: NestedSegmentInfo[];
}

export interface FileMetadata {
	file_size: number;
	source_nzb_path: string;
	status: "healthy" | "corrupted" | "unspecified";
	segment_count: number;
	available_segments?: number;
	encryption: "none" | "rclone";
	created_at: string;
	modified_at: string;
	password_protected: boolean;
	segments: SegmentInfo[];
	nested_sources?: NestedSourceInfo[];
}

// Authentication types
export interface User {
	id: string;
	email?: string;
	name: string;
	avatar_url?: string;
	provider: string;
	api_key?: string;
	is_admin: boolean;
	last_login?: string;
}

export interface AuthResponse {
	user?: User;
	redirect_url?: string;
	message?: string;
}

export interface ChangeOwnPasswordRequest {
	current_password: string;
	new_password: string;
}

// Health Worker types
export interface HealthCheckRequest {
	file_path: string;
	source_nzb_path: string;
	priority?: HealthPriority;
}

export interface HealthWorkerStatus {
	status: string;
	total_runs_completed: number;
	total_files_checked: number;
	total_files_recovered: number;
	total_files_corrupted: number;
	current_run_files_checked: number;
	pending_manual_checks: number;
	error_count: number;
	current_run_start_time?: string;
	last_run_time?: string;
	next_run_time?: string;
	last_error?: string;
}

// Library Sync types
export interface LibrarySyncProgress {
	total_files: number;
	processed_files: number;
	start_time: string;
}

export interface LibrarySyncResult {
	files_added: number;
	files_deleted: number;
	duration: number;
	completed_at: string;
}

export interface LibrarySyncStatus {
	is_running: boolean;
	progress?: LibrarySyncProgress;
	last_sync_result?: LibrarySyncResult;
}

// Pool Metrics types
export interface ProviderStatus {
	id: string;
	host: string;
	used_connections: number;
	max_connections: number;
	state: string;
	error_count: number;
	last_connection_attempt: string;
	last_successful_connect: string;
	failure_reason: string;
	current_speed_bytes_per_sec: number;
	ping_ms: number;
	last_speed_test_mbps: number;
	last_speed_test_time?: string;
	missing_count: number;
	missing_rate_per_minute: number;
	missing_warning: boolean;
	byte_count: number;
	byte_count_24h: number;
	started_at: string;
	quota_bytes?: number;
	quota_used?: number;
	quota_reset_at?: string;
	quota_exceeded?: boolean;
}

export interface ActiveStream {
	id: string;
	file_path: string;
	started_at: string;
	source: string;
	user_name?: string;
	client_ip?: string;
	user_agent?: string;
	bytes_sent: number;
	bytes_downloaded: number;
	current_offset: number;
	bytes_per_second: number;
	download_speed: number;
	speed_avg: number;
	total_size: number;
	eta: number;
	status: string;
	total_connections: number;
	buffered_offset: number;
}

export interface PoolMetrics {
	bytes_downloaded: number;
	bytes_downloaded_24h: number;
	bytes_uploaded: number;
	articles_downloaded: number;
	articles_posted: number;
	total_errors: number;
	provider_errors: Record<string, number>;
	provider_bytes: Record<string, number>;
	download_speed_bytes_per_sec: number;
	max_download_speed_bytes_per_sec: number;
	upload_speed_bytes_per_sec: number;
	timestamp: string;
	started_at: string;
	providers: ProviderStatus[];
}

// SABnzbd API response types
export interface SABnzbdAddResponse {
	status: boolean;
	nzo_ids: string[];
}

// System Browse types
export interface FileEntry {
	name: string;
	path: string;
	is_dir: boolean;
	size: number;
	mod_time: string;
}

export interface SystemBrowseResponse {
	current_path: string;
	parent_path: string;
	files: FileEntry[];
}

// FUSE types
export interface FuseStatus {
	status: "stopped" | "starting" | "running" | "error";
	path: string;
	healthy?: boolean;
	health_error?: string;
}

export interface ProviderHistoricalStat {
	timestamp: string;
	provider_id: string;
	bytes_downloaded: number;
}

export interface ProviderHistoricalStatsResponse {
	stats: ProviderHistoricalStat[];
}

export interface ProviderSpeedTestHistoryStat {
	id: number;
	provider_id: string;
	speed_mbps: number;
	created_at: string;
}

export interface ProviderSpeedTestHistoryResponse {
	history: ProviderSpeedTestHistoryStat[];
}
