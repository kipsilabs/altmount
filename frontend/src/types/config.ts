// Configuration types that match the backend API structure

// Mount type for unified mount configuration
export type MountType = "none" | "rclone" | "fuse" | "rclone_external";

// Base configuration response from API
export interface ConfigResponse {
	webdav: WebDAVConfig;
	api: APIConfig;
	auth: AuthConfig;
	database: DatabaseConfig;
	metadata: MetadataConfig;
	streaming: StreamingConfig;
	health: HealthConfig;
	rclone: RCloneConfig;
	fuse: FuseConfig;
	segment_cache: SegmentCacheConfig;
	import: ImportConfig;
	log: LogConfig;
	sabnzbd: SABnzbdConfig;
	arrs: ArrsConfig;
	stremio: StremioConfig;
	providers: ProviderConfig[];
	user_agent?: string;
	network: NetworkConfig;
	par2_repair: Par2RepairConfig;
	mount_path: string;
	mount_type: MountType;
	memory_limit_mb?: number | null; // Go soft memory limit; 0/unset auto, -1 off
	api_key?: string;
	download_key?: string;
	profiler_enabled: boolean;
}

// Background PAR2 repair of missing usenet articles
export interface Par2RepairConfig {
	enabled?: boolean;
	max_repair_ratio?: number; // fraction of a file's bytes repairable; PAR2 redundancy is the hard ceiling
	max_memory_mb?: number; // in-heap solver budget per job; larger repairs spill to disk
	max_concurrent_jobs?: number;
	max_connections?: number; // NNTP connections repair fetches may use (shared across jobs); 0 = default 10
	min_release_size_mb?: number; // releases smaller than this are not repaired; 0 = no minimum
	max_release_size_mb?: number; // releases larger than this are not repaired; 0 = no maximum
	max_patch_store_mb?: number; // total patch-store size cap; 0 = unlimited
	patch_dir?: string; // where patches + solver scratch live; empty = <metadata_root>/patches
	arr_first?: boolean; // corrupted files: ARR repair first, PAR2 as fallback when the ARRs come up empty (default true)
	repair_on_import?: boolean; // queue a repair as soon as a damaged file imports
}

// WebDAV server configuration
export interface WebDAVConfig {
	port: number;
	user: string;
	password: string;
	host?: string;
	debug?: boolean;
}

// API server configuration
export interface APIConfig {
	prefix: string;
}

// Authentication configuration
export interface AuthConfig {
	login_required: boolean;
}

// Network proxy configuration for outbound HTTP (indexers, arrs, SABnzbd, NZBLNK).
// Empty strings disable proxying for that scheme. Mirrors standard
// HTTP_PROXY/HTTPS_PROXY/NO_PROXY env-var semantics. Internal endpoints
// (RC server, self-loopback) are not affected.
export interface NetworkConfig {
	http_proxy: string;
	https_proxy: string;
	no_proxy: string;
}

// Database configuration
export interface DatabaseConfig {
	type: string;
	path: string;
	dsn: string;
}

// Metadata configuration
export interface MetadataConfig {
	root_path: string;
	backup: MetadataBackupConfig;
	migration?: MetadataMigrationConfig;
}

export interface MetadataMigrationConfig {
	default_group: string;
}

export interface MetadataBackupConfig {
	enabled?: boolean;
	schedule: string; // cron expression (UTC)
	keep_backups: number;
	path: string;
}

// Failure masking configuration
export interface FailureMaskingConfig {
	enabled: boolean;
	threshold: number;
}

// Streaming configuration
export interface StreamingConfig {
	max_prefetch: number;
	read_timeout_seconds: number;
	failure_masking: FailureMaskingConfig;
}

// Segment cache configuration
export interface SegmentCacheConfig {
	enabled: boolean | null;
	cache_path: string;
	max_size_gb: number;
	expiry_hours: number;
	memory_mb: number; // in-memory tier of decoded articles; 0 disables
}

// Health configuration
export interface HealthConfig {
	enabled?: boolean;
	library_dir?: string;
	cleanup_orphaned_metadata?: boolean;
	check_interval_seconds?: number;
	/** Max in-flight STAT checks per sweep. 0 or absent = adapt to pool + stream activity. */
	max_concurrent_segment_checks?: number;
	/** @deprecated renamed to max_concurrent_segment_checks; migrated automatically. */
	max_connections_for_health_checks?: number;
	check_batch_size?: number; // Files fetched and swept together per health-check cycle
	max_concurrent_jobs?: number; // Max concurrent health check jobs
	segment_sample_percentage?: number; // Percentage of segments to check (1-100)
	max_retries?: number; // Max health check retries
	library_sync_interval_minutes?: number; // Library sync interval in minutes (optional)
	library_sync_concurrency?: number;
	check_all_segments?: boolean; // Whether to check all segments or use sampling
	resolve_repair_on_import?: boolean; // Automatically resolve pending repairs in the same directory when a new file is imported
	verify_data?: boolean; // Verify 1 byte of data for each segment
	read_timeout_seconds?: number; // Timeout for data verification
	acceptable_missing_segments_percentage?: number;
	// SABnzbd category names whose files are never registered for health checking
	// by the library-sync discovery pass. Matching is by the category's directory.
	excluded_categories?: string[];
	repair: RepairConfig;
	// What happens when the health checker or a streaming read confirms real
	// (non-degraded) corruption: "repair" (default) triggers an Arr rescan;
	// "delete" removes the file and cleans up now-empty parent directories instead.
	corruption_action?: "repair" | "delete";
	verify_content?: boolean; // Probe each media file's header for a valid container signature during health checks
	verify_content_timeout_seconds?: number; // Per-file content probe timeout (default 15s)
	// Days a corrupted_metadata safety copy is kept before the health cycle prunes it.
	// 0 or absent means keep forever.
	corrupted_retention_days?: number;
}

export interface RepairConfig {
	enabled: boolean;
	interval_minutes: number;
	max_cooldown_hours: number;
	max_repair_retries: number; // Max repair notification retries
	exponential_backoff: boolean;
	auto_search_wait_seconds: number; // Wait for the ARR's own redownload search before deleting the file record; 0 disables
	file_delete_confirm_seconds: number; // Wait for the ARR to report the deleted file record as unlinked; 0 disables
}

// Dry run result for library sync
export interface DryRunSyncResult {
	orphaned_metadata_count: number; // Number of orphaned metadata files
	orphaned_library_files: number; // Number of orphaned library files (symlinks/STRM)
	database_records_to_clean: number; // Number of database records to clean
	would_cleanup: boolean; // Whether cleanup would occur based on config
}

// RClone configuration (sanitized)
export interface RCloneConfig {
	// Encryption
	password_set: boolean;
	salt_set: boolean;

	// RC (Remote Control) Configuration
	rc_enabled: boolean;
	rc_url: string;
	vfs_name: string;
	rc_port: number;
	rc_user: string;
	rc_pass_set: boolean;
	rc_options: Record<string, string>;

	// Mount Configuration
	mount_enabled: boolean;
	mount_options: Record<string, string>;

	// Mount-Specific Settings
	allow_other: boolean;
	allow_non_empty: boolean;
	read_only: boolean;
	timeout: string;
	syslog: boolean;

	// How long the rcd may stay unresponsive to liveness probes before it is
	// killed and restarted. Empty means the built-in default (90s).
	rcd_restart_after: string;

	// System and filesystem options
	log_level: string;
	uid: number;
	gid: number;
	umask: string;
	buffer_size: string;
	attr_timeout: string;
	transfers: number;

	// VFS Cache Settings
	cache_dir: string;
	vfs_cache_mode: string;
	vfs_cache_poll_interval: string;
	vfs_read_chunk_size: string;
	vfs_read_chunk_size_limit: string;
	vfs_cache_max_size: string;
	vfs_cache_max_age: string;
	vfs_read_ahead: string;
	dir_cache_time: string;
	vfs_cache_min_free_space: string;
	vfs_disk_space_total: string;
	vfs_read_chunk_streams: number;

	// Advanced Settings
	no_mod_time: boolean;
	no_checksum: boolean;
	async_read: boolean;
	vfs_fast_fingerprint: boolean;
	use_mmap: boolean;
	links: boolean;
}

// Fuse configuration
export interface FuseConfig {
	mount_path: string;
	enabled: boolean;
	allow_other: boolean;
	debug: boolean;
	attr_timeout_seconds: number;
	entry_timeout_seconds: number;
	max_cache_size_mb: number;
	max_read_ahead_mb: number;
	async_buffer_size_mb: number;
	async_buffer_max_total_mb: number;
	no_mod_time: boolean;
}

// Import strategy type
export type ImportStrategy = "NONE" | "SYMLINK" | "STRM";

// Import configuration
export interface ImportConfig {
	max_processor_workers: number;
	/**
	 * Connections held back from import per active stream. `null`/absent
	 * derives the reservation from pool size; an explicit `0` disables it.
	 */
	stream_headroom_connections?: number | null;
	queue_processing_interval_seconds: number; // Interval in seconds for queue processing
	allowed_file_extensions: string[];
	max_download_prefetch: number;
	segment_sample_percentage: number; // Percentage of segments to check (1-100)
	read_timeout_seconds: number;
	import_strategy: ImportStrategy;
	import_dir?: string | null;
	watch_dir?: string | null;
	watch_interval_seconds?: number | null;
	allow_nested_rar_extraction?: boolean;
	rename_to_nzb_name?: boolean;
	filter_sample_files?: boolean;
	failed_item_retention_hours?: number | null;
	history_retention_days?: number | null;
	verify_content?: boolean; // Probe each media file's header for a valid container signature before reporting import success
	verify_content_timeout_seconds?: number; // Per-file content probe timeout (default 15s)
}

// Log configuration
export interface LogConfig {
	file: string;
	level: string;
	max_size: number;
	max_age: number;
	max_backups: number;
	compress: boolean;
}

// NNTP Provider configuration (sanitized)
export interface ProviderConfig {
	id: string;
	name?: string;
	host: string;
	port: number;
	username: string;
	max_connections: number;
	min_connections_alive?: number;
	inflight_requests: number;
	stat_inflight_requests: number;
	tls: boolean;
	insecure_tls: boolean;
	proxy_url?: string;
	password_set: boolean;
	enabled: boolean;
	is_backup_provider: boolean;
	storage_group?: string;
	skip_ping?: boolean;
	keepalive_interval_seconds?: number;
	keepalive_command?: string;
	user_agent?: string;
	quota_bytes?: number;
	quota_period_hours?: number;
	max_article_age_days?: number;
	strict_max_article_age?: boolean;
	last_rtt_ms?: number;
	last_speed_test_mbps?: number;
	last_speed_test_time?: string;
	account_expiration_date?: string;
}

// Pipeline auto-tune result for a single provider
export interface PipelineDepthSample {
	depth: number;
	mbps: number;
}

export interface PipelineTuneResponse {
	recommended_inflight: number;
	recommended_stat_inflight: number;
	baseline_mbps: number;
	best_mbps: number;
	improvement_pct: number;
	enabled: boolean;
	test_connections: number;
	tested: PipelineDepthSample[];
	warning?: string;
}

// SABnzbd configuration
export interface SABnzbdConfig {
	enabled: boolean;
	complete_dir: string;
	download_client_base_url?: string;
	categories: SABnzbdCategory[];
	history_retention_minutes: number;
	fallback_host?: string;
	fallback_api_key?: string; // Obfuscated when returned from API
	fallback_api_key_set?: boolean; // For display purposes only
}

// SABnzbd category configuration
export interface SABnzbdCategory {
	name: string;
	order: number;
	priority: number;
	dir: string;
}

// Configuration update request types
export interface ConfigUpdateRequest {
	webdav?: WebDAVUpdateRequest;
	api?: APIUpdateRequest;
	auth?: AuthUpdateRequest;
	database?: DatabaseUpdateRequest;
	metadata?: MetadataUpdateRequest;
	streaming?: StreamingUpdateRequest;
	segment_cache?: Partial<SegmentCacheConfig>;
	health?: HealthUpdateRequest;
	rclone?: RCloneUpdateRequest;
	fuse?: Partial<FuseConfig>;
	import?: ImportUpdateRequest;
	log?: LogUpdateRequest;
	sabnzbd?: SABnzbdUpdateRequest;
	arrs?: ArrsConfig;
	stremio?: Partial<StremioConfig>;
	providers?: ProviderUpdateRequest[];
	user_agent?: string;
	network?: NetworkConfig;
	par2_repair?: Par2RepairConfig;
	mount_path?: string;
	mount_type?: MountType;
	profiler_enabled?: boolean;
}

// WebDAV update request
export interface WebDAVUpdateRequest {
	port?: number;
	user?: string;
	password?: string;
	host?: string;
	debug?: boolean;
}

// API update request
export interface APIUpdateRequest {
	prefix?: string;
}

// Auth update request
export interface AuthUpdateRequest {
	login_required?: boolean;
}

// Database update request
export interface DatabaseUpdateRequest {
	type?: string;
	path?: string;
	dsn?: string;
}

// Metadata update request
export interface MetadataUpdateRequest {
	root_path?: string;
	backup?: MetadataBackupConfig;
}

// Streaming update request
export interface StreamingUpdateRequest {
	max_prefetch?: number;
	read_timeout_seconds?: number;
	failure_masking?: Partial<FailureMaskingConfig>;
}

// Health update request
export interface HealthUpdateRequest {
	enabled?: boolean;
	library_dir?: string;
	cleanup_orphaned_metadata?: boolean;
	check_interval_seconds?: number; // Interval in seconds (optional)
	/** Max in-flight STAT checks per sweep. 0 or absent = adapt to pool + stream activity. */
	max_concurrent_segment_checks?: number;
	/** @deprecated renamed to max_concurrent_segment_checks; migrated automatically. */
	max_connections_for_health_checks?: number;
	check_batch_size?: number; // Files fetched and swept together per health-check cycle
	max_concurrent_jobs?: number; // Max concurrent health check jobs
	segment_sample_percentage?: number; // Percentage of segments to check (1-100)
	max_retries?: number;
	read_timeout_seconds?: number;
	library_sync_interval_minutes?: number; // Library sync interval in minutes (optional)
	library_sync_concurrency?: number;
	check_all_segments?: boolean; // Whether to check all segments or use sampling
	resolve_repair_on_import?: boolean;
	verify_data?: boolean;
	acceptable_missing_segments_percentage?: number;
	repair?: Partial<RepairConfig>;
	corruption_action?: "repair" | "delete";
}

// RClone update request
export interface RCloneUpdateRequest {
	password?: string;
	salt?: string;
	rc_enabled?: boolean;
	rc_url?: string;
	vfs_name?: string;
	rc_port?: number;
	rc_user?: string;
	rc_pass?: string;
	rc_options?: Record<string, string>;
	mount_enabled?: boolean;
	mount_options?: Record<string, string>;

	// Mount-Specific Settings
	allow_other?: boolean;
	allow_non_empty?: boolean;
	read_only?: boolean;
	timeout?: string;
	syslog?: boolean;
	rcd_restart_after?: string;

	// System and filesystem options
	log_level?: string;
	uid?: number;
	gid?: number;
	umask?: string;
	buffer_size?: string;
	attr_timeout?: string;
	transfers?: number;

	// VFS Cache Settings
	cache_dir?: string;
	vfs_cache_mode?: string;
	vfs_cache_poll_interval?: string;
	vfs_cache_max_size?: string;
	vfs_cache_max_age?: string;
	vfs_read_chunk_size?: string;
	vfs_read_chunk_size_limit?: string;
	vfs_read_ahead?: string;
	dir_cache_time?: string;
	vfs_cache_min_free_space?: string;
	vfs_disk_space_total?: string;
	vfs_read_chunk_streams?: number;

	// Advanced Settings
	no_mod_time?: boolean;
	no_checksum?: boolean;
	async_read?: boolean;
	vfs_fast_fingerprint?: boolean;
	use_mmap?: boolean;
	links?: boolean;
}

// Import update request
export interface ImportUpdateRequest {
	max_processor_workers?: number;
	queue_processing_interval_seconds?: number; // Interval in seconds for queue processing
	allowed_file_extensions?: string[];
	import_strategy?: ImportStrategy;
	import_dir?: string | null;
	watch_dir?: string | null;
	watch_interval_seconds?: number | null;
	allow_nested_rar_extraction?: boolean;
	rename_to_nzb_name?: boolean;
	filter_sample_files?: boolean;
	history_retention_days?: number | null;
}

// Log update request
export interface LogUpdateRequest {
	file?: string;
	level?: string;
	max_size?: number;
	max_age?: number;
	max_backups?: number;
	compress?: boolean;
}

// Provider update request
export interface ProviderUpdateRequest {
	name?: string;
	host?: string;
	port?: number;
	username?: string;
	password?: string;
	max_connections?: number;
	min_connections_alive?: number;
	inflight_requests?: number;
	stat_inflight_requests?: number;
	tls?: boolean;
	insecure_tls?: boolean;
	proxy_url?: string;
	enabled?: boolean;
	is_backup_provider?: boolean;
	storage_group?: string;
	skip_ping?: boolean;
	keepalive_interval_seconds?: number;
	keepalive_command?: string;
	user_agent?: string;
	quota_bytes?: number;
	quota_period_hours?: number;
	max_article_age_days?: number;
	strict_max_article_age?: boolean;
	account_expiration_date?: string;
}

// SABnzbd update request
export interface SABnzbdUpdateRequest {
	enabled?: boolean;
	complete_dir?: string;
	categories?: SABnzbdCategory[];
	history_retention_minutes?: number;
	fallback_host?: string;
	fallback_api_key?: string;
}

// Configuration section names for PATCH requests
export type ConfigSection =
	| "webdav"
	| "auth"
	| "metadata"
	| "streaming"
	| "segment_cache"
	| "health"
	| "import"
	| "providers"
	| "mount"
	| "rclone"
	| "fuse"
	| "sabnzbd"
	| "arrs"
	| "stremio"
	| "network"
	| "par2_repair"
	| "system";

// Form data interfaces for UI components
export interface RCloneMountFormData {
	mount_enabled: boolean;
	mount_options: Record<string, string>;

	// Mount-Specific Settings
	allow_other: boolean;
	allow_non_empty: boolean;
	read_only: boolean;
	timeout: string;
	syslog: boolean;
	rcd_restart_after: string;

	// System and filesystem options
	log_level: string;
	uid: number;
	gid: number;
	umask: string;
	buffer_size: string;
	attr_timeout: string;
	transfers: number;

	// VFS Cache Settings
	cache_dir: string;
	vfs_cache_mode: string;
	vfs_cache_poll_interval: string;
	vfs_read_chunk_size: string;
	vfs_read_chunk_size_limit: string;
	vfs_cache_max_size: string;
	vfs_cache_max_age: string;
	vfs_read_ahead: string;
	dir_cache_time: string;
	vfs_cache_min_free_space: string;
	vfs_disk_space_total: string;
	vfs_read_chunk_streams: number;

	// Advanced Settings
	no_mod_time: boolean;
	no_checksum: boolean;
	async_read: boolean;
	vfs_fast_fingerprint: boolean;
	use_mmap: boolean;
	links: boolean;
}

export interface MountStatus {
	mounted: boolean;
	mount_point: string;
	error?: string;
	started_at?: string;
}

export interface ProviderFormData {
	name: string;
	host: string;
	port: number;
	username: string;
	password: string;
	max_connections: number;
	min_connections_alive: number;
	inflight_requests: number;
	stat_inflight_requests: number;
	tls: boolean;
	insecure_tls: boolean;
	proxy_url: string;
	enabled: boolean;
	is_backup_provider: boolean;
	storage_group: string;
	skip_ping: boolean;
	keepalive_interval_seconds: number;
	keepalive_command: string;
	user_agent: string;
	quota_bytes: number;
	quota_period_hours: number;
	max_article_age_days: number;
	strict_max_article_age: boolean;
	account_expiration_date: string;
}

export interface LogFormData {
	file: string;
	level: string;
	max_size: number;
	max_age: number;
	max_backups: number;
	compress: boolean;
}

// Arrs configuration types
export type ArrsType = "radarr" | "sonarr" | "lidarr" | "readarr" | "whisparr" | "sportarr";

export interface ArrsInstanceConfig {
	name: string;
	url: string;
	api_key: string;
	category?: string;
	enabled: boolean;
	sync_interval_hours: number;
}

export interface ArrsConfig {
	enabled: boolean;
	max_workers: number;
	webhook_base_url?: string;
	radarr_instances: ArrsInstanceConfig[];
	sonarr_instances: ArrsInstanceConfig[];
	lidarr_instances: ArrsInstanceConfig[];
	readarr_instances: ArrsInstanceConfig[];
	whisparr_instances: ArrsInstanceConfig[];
	sportarr_instances: ArrsInstanceConfig[];
	queue_cleanup_enabled?: boolean;
	queue_cleanup_interval_seconds?: number;
	queue_cleanup_grace_period_minutes?: number;
	queue_cleanup_max_failures?: number;
	queue_cleanup_rules?: StuckCleanupRule[];
}

export type StuckCleanupAction = "remove" | "blocklist" | "blocklist_search";

export interface StuckCleanupRule {
	message: string;
	enabled: boolean;
	action: StuckCleanupAction;
}

// Prowlarr indexer configuration (nested inside StremioConfig)
export interface ProwlarrConfig {
	enabled: boolean;
	host: string;
	api_key: string;
	api_key_set?: boolean;
	categories: number[];
	indexers?: number[];
	preferred_indexers?: number[];
	preferred_indexer_names?: string[];
	languages: string[];
	preferred_languages?: string[];
	qualities: string[];
	exclude_keywords?: string[];
	custom_scores?: Record<string, number>;
}

// A single Prowlarr indexer returned by POST /api/prowlarr/indexers
export interface ProwlarrIndexer {
	id: number;
	name: string;
	enable: boolean;
	protocol: string;
}

// TRaSH Custom Format definition
export interface TrashCustomFormat {
	id: string;
	name: string;
	category: "source" | "hdr" | "audio" | "release_group" | "resolution" | "custom";
	pattern: string;
	patternType: "regex" | "token";
	score: number;
	enabled: boolean;
	isCustom: boolean;
	pattern_type?: "regex" | "token";
	is_custom?: boolean;
	invert?: boolean;
}

export type ScoringPreset = "trash_recommended" | "remux_enthusiast" | "compatibility" | "custom";

export interface StreamScoringConfig {
	preset: ScoringPreset;
	custom_formats: TrashCustomFormat[];
	exclude_keywords: string[];
	exclude_regex?: string;
	preferred_languages: string[];
	require_preferred_language: boolean;
	size_limits?: {
		resolution_4k?: { min_gb: number; max_gb: number };
		resolution_1080p?: { min_gb: number; max_gb: number };
	};
}

export interface NewsnabIndexerConfig {
	id: string;
	name: string;
	url: string;
	api_key: string;
	api_key_set?: boolean;
	categories?: number[];
	weight: number;
	timeout_seconds: number;
	enabled: boolean;
}

export interface StremioIndexersConfig {
	provider: "prowlarr" | "newsnab" | "both";
	user_agent_mode?: "auto" | "custom";
	custom_user_agent?: string;
	prowlarr: ProwlarrConfig;
	newsnab?: NewsnabIndexerConfig[];
}

export interface ScoredReleaseItem {
	title: string;
	download_url: string;
	size: number;
	publish_date: string;
	indexer: string;
	indexer_id: string;
	source: string;
	guid: string;
	score: number;
	matched_formats: string[];
	matched_languages: string[];
	excluded: boolean;
	exclude_reason?: string;
}

export interface InspectSearchRequest {
	query: string;
	type?: "movie" | "series";
	imdb_id?: string;
	tvdb_id?: string;
	season?: number;
	episode?: number;
	timeout_ms?: number;
	scoring?: StreamScoringConfig;
}

export interface InspectSearchResponse {
	total_results: number;
	active_results: number;
	discarded_results: number;
	releases: ScoredReleaseItem[];
}

export interface UserAgentInfo {
	tv_user_agent: string;
	movie_user_agent: string;
	sonarr_version: string;
	radarr_version: string;
	last_updated: string;
	source: string;
}

// Stremio integration configuration
export interface StremioConfig {
	enabled: boolean;
	addon_name?: string;
	addon_description?: string;
	base_url?: string;
	direct_stream?: boolean;
	show_cached_indicator?: boolean;
	fallback_timeout_ms?: number;
	max_retries?: number;
	stream_ttl_seconds?: number;
	indexers?: StremioIndexersConfig;
	scoring?: StreamScoringConfig;
	nzb_ttl_hours: number;
	failed_release_ttl_hours: number;
	max_fallback_releases: number;
	include_library_streams?: boolean;
	fast_fail_header_only?: boolean;
	prowlarr: ProwlarrConfig;
}

// Helper type for configuration sections
interface ConfigSectionInfo {
	title: string;
	description: string;
	icon: string;
	canEdit: boolean;
	hidden?: boolean;
}

// Configuration sections metadata
// Provider management types
export interface ProviderTestRequest {
	provider_id?: string;
	host: string;
	port: number;
	username: string;
	password: string;
	tls: boolean;
	insecure_tls: boolean;
	proxy_url?: string;
	skip_ping?: boolean;
}

export interface ProviderTestResponse {
	success: boolean;
	error_message?: string;
	rtt_ms?: number;
}

export interface ProviderCreateRequest {
	name?: string;
	host: string;
	port: number;
	username: string;
	password: string;
	max_connections: number;
	min_connections_alive?: number;
	inflight_requests?: number;
	stat_inflight_requests?: number;
	tls: boolean;
	insecure_tls: boolean;
	proxy_url?: string;
	enabled: boolean;
	is_backup_provider: boolean;
	storage_group?: string;
	skip_ping?: boolean;
	keepalive_interval_seconds?: number;
	keepalive_command?: string;
	user_agent?: string;
	quota_bytes?: number;
	quota_period_hours?: number;
	account_expiration_date?: string;
}

// A single hostname -> backbone (storage group) mapping used to autofill
// a provider's storage_group. Sourced from the public rexum provider tree.
export interface ProviderBackbone {
	host: string;
	backbone: string;
	provider: string;
}

export interface ProviderReorderRequest {
	provider_ids: string[];
}

export const CONFIG_SECTIONS: Record<ConfigSection | "system", ConfigSectionInfo> = {
	webdav: {
		title: "WebDAV Server",
		description: "Expose your virtual library over the network via WebDAV protocol.",
		icon: "Globe",
		canEdit: true,
	},
	auth: {
		title: "Authentication",
		description: "User authentication and login settings",
		icon: "Shield",
		canEdit: true,
	},
	mount: {
		title: "Mount",
		description: "Configure filesystem mount (RClone or native FUSE)",
		icon: "HardDrive",
		canEdit: true,
	},
	metadata: {
		title: "Metadata",
		description: "Configure how AltMount stores and manages virtual file metadata.",
		icon: "Folder",
		canEdit: true,
	},
	streaming: {
		title: "Streaming & Downloads",
		description: "Segment prefetch and on-disk segment cache for smoother media playback.",
		icon: "Download",
		canEdit: true,
	},
	segment_cache: {
		title: "Segment Cache",
		description: "Segment-aligned disk cache shared by FUSE and WebDAV for faster media playback",
		icon: "HardDrive",
		canEdit: true,
		hidden: true,
	},
	health: {
		title: "Health Monitoring",
		description: "File health monitoring and automatic repair settings",
		icon: "Shield",
		canEdit: true,
	},
	import: {
		title: "Import Processing",
		description: "Configure how workers handle new imports and validation.",
		icon: "Cog",
		canEdit: true,
	},
	providers: {
		title: "NNTP Providers",
		description: "Usenet provider configuration for downloads",
		icon: "Radio",
		canEdit: true,
	},
	rclone: {
		title: "RClone",
		description: "RClone configuration",
		icon: "HardDrive",
		canEdit: true,
		hidden: true,
	},
	fuse: {
		title: "FUSE",
		description: "Native FUSE configuration",
		icon: "HardDrive",
		canEdit: true,
		hidden: true,
	},
	sabnzbd: {
		title: "SABnzbd API",
		description: "Emulate a SABnzbd server to allow ARR applications to send NZBs to AltMount.",
		icon: "Download",
		canEdit: true,
	},
	arrs: {
		title: "ARR Management",
		description:
			"Configure Radarr, Sonarr, Lidarr, Readarr, and Whisparr instances for media file synchronization and automatic repair.",
		icon: "Cog",
		canEdit: true,
	},
	stremio: {
		title: "Stremio Integration",
		description:
			"Upload an NZB for instant stream URLs, or enable the addon to automatically search Prowlarr by IMDB ID and stream results directly from Stremio.",
		icon: "Tv",
		canEdit: true,
	},
	network: {
		title: "Network & User Agent",
		description:
			"HTTP/HTTPS proxy and indexer User-Agent for outbound indexer, Arrs, NZB grab, and SABnzbd fallback traffic",
		icon: "Globe",
		canEdit: true,
	},
	par2_repair: {
		title: "PAR2 Repair",
		description: "Background reconstruction of missing usenet articles from PAR2 recovery data",
		icon: "Wrench",
		canEdit: true,
	},
	system: {
		title: "System",
		description: "System settings",
		icon: "HardDrive",
		canEdit: true,
	},
};
