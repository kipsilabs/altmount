import {
	Activity,
	AlertTriangle,
	Cog,
	Download,
	Folder,
	Globe,
	HardDrive,
	Link,
	Network as NetworkIcon,
	Radio,
	RefreshCw,
	Server,
	Settings,
	Shield,
	ShieldAlert,
	Tv,
	Wrench,
} from "lucide-react";
import { useEffect, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { ArrsConfigSection } from "../components/config/ArrsConfigSection";
import { AuthConfigSection } from "../components/config/AuthConfigSection";
import { ComingSoonSection } from "../components/config/ComingSoonSection";
import { HealthConfigSection } from "../components/config/HealthConfigSection";
import { MetadataConfigSection } from "../components/config/MetadataConfigSection";
import { MountConfigSection } from "../components/config/MountConfigSection";
import { NetworkConfigSection } from "../components/config/NetworkConfigSection";
import { Par2RepairConfigSection } from "../components/config/Par2RepairConfigSection";
import { ProvidersConfigSection } from "../components/config/ProvidersConfigSection";
import { PeernetConfigSection } from "../components/config/peernet/PeernetConfigSection";
import { SABnzbdConfigSection } from "../components/config/SABnzbdConfigSection";
import { StreamingConfigSection } from "../components/config/StreamingConfigSection";
import { StremioConfigSection } from "../components/config/StremioConfigSection";
import { SystemConfigSection } from "../components/config/SystemConfigSection";
import { WebDAVConfigSection } from "../components/config/WebDAVConfigSection";
import { ImportConfigSection } from "../components/config/WorkersConfigSection";
import { ErrorAlert } from "../components/ui/ErrorAlert";
import { LoadingSpinner } from "../components/ui/LoadingSpinner";
import { RestartRequiredBanner } from "../components/ui/RestartRequiredBanner";
import { useConfirm } from "../contexts/ModalContext";
import { useAuth } from "../hooks/useAuth";
import {
	useConfig,
	useLibrarySyncNeeded,
	useReloadConfig,
	useRestartServer,
	useTriggerLibrarySync,
	useUpdateConfigSection,
} from "../hooks/useConfig";
import type {
	ArrsConfig,
	AuthConfig,
	ConfigSection,
	HealthConfig,
	ImportConfig,
	LogFormData,
	MetadataConfig,
	NetworkConfig,
	Par2RepairConfig,
	ProviderConfig,
	SABnzbdConfig,
	SegmentCacheConfig,
	StreamingConfig,
	StremioConfig,
	WebDAVConfig,
} from "../types/config";
import { CONFIG_SECTIONS } from "../types/config";
import type { PeernetConfig } from "../types/peernet";

// Helper function to get icon component
const getIconComponent = (iconName: string) => {
	const iconMap = {
		Globe,
		Folder,
		Download,
		Shield,
		Cog,
		Radio,
		HardDrive,
		ShieldAlert,
		Activity,
		Wrench,
		Server,
		Tv,
		Link,
		Network: NetworkIcon,
	};
	return iconMap[iconName as keyof typeof iconMap] || Settings;
};

// Define section groups for modern organization
const SECTION_GROUPS = [
	{
		title: "Core Services",
		sections: ["webdav", "mount", "providers"],
	},
	{
		title: "Media Management",
		sections: ["metadata", "streaming"],
	},
	{
		title: "Automation",
		sections: ["sabnzbd", "arrs", "health", "par2_repair", "stremio", "import"],
	},
	{
		title: "System",
		sections: ["auth", "network", "peernet", "system"],
	},
];

export function ConfigurationPage() {
	const { data: config, isLoading, error, refetch } = useConfig();
	const reloadConfig = useReloadConfig();
	const restartServer = useRestartServer();
	const updateConfigSection = useUpdateConfigSection();
	const { recheckAuth } = useAuth();
	const { data: syncNeeded } = useLibrarySyncNeeded();
	const triggerLibrarySync = useTriggerLibrarySync();
	const { confirmAction } = useConfirm();
	const navigate = useNavigate();
	const { section } = useParams<{ section: string }>();

	// Get active section from URL parameter, default to webdav
	const activeSection = (() => {
		if (!section) return "webdav";
		// Redirect legacy rclone/fuse paths to mount
		if (section === "rclone" || section === "fuse") return "mount" as ConfigSection;
		// NZBLNK settings now live inside the Network section
		if (section === "nzblnk") return "network" as ConfigSection;
		const validSections = Object.keys(CONFIG_SECTIONS) as (ConfigSection | "system")[];
		return validSections.includes(section as ConfigSection | "system")
			? (section as ConfigSection | "system")
			: "webdav";
	})();

	// Redirect to default section if no section is specified, or legacy paths
	useEffect(() => {
		if (!section) {
			navigate("/config/webdav", { replace: true });
		} else if (section === "rclone" || section === "fuse") {
			navigate("/config/mount", { replace: true });
		} else if (section === "nzblnk") {
			navigate("/config/network", { replace: true });
		}
	}, [section, navigate]);

	const [restartRequiredConfigs, setRestartRequiredConfigs] = useState<string[]>([]);
	const [isRestartBannerDismissed, setIsRestartBannerDismissed] = useState(() => {
		// Initialize from session storage on component mount
		return sessionStorage.getItem("restartBannerDismissed") === "true";
	});

	// Helper functions for restart required state
	const addRestartRequiredConfig = (configName: string) => {
		setRestartRequiredConfigs((prev) => (prev.includes(configName) ? prev : [...prev, configName]));
		setIsRestartBannerDismissed(false);
	};

	const handleDismissRestartBanner = () => {
		setIsRestartBannerDismissed(true);
		sessionStorage.setItem("restartBannerDismissed", "true");
	};

	// Clear restart state on config reload (indicates server restart)
	const handleReloadConfig = async () => {
		try {
			await reloadConfig.mutateAsync();
			setRestartRequiredConfigs([]);
			setIsRestartBannerDismissed(false);
			sessionStorage.removeItem("restartBannerDismissed");
		} catch (error) {
			console.error("Failed to reload configuration:", error);
		}
	};

	// Handle server restart
	const handleRestartServer = async () => {
		const confirmed = await confirmAction(
			"Restart Server",
			"This will restart the entire server. All active connections will be lost. Continue?",
			{
				type: "error",
				confirmText: "Restart Server",
				confirmButtonClass: "btn-error",
			},
		);
		if (!confirmed) {
			return;
		}

		try {
			await restartServer.mutateAsync(false);
			// Clear local state since server is restarting
			setRestartRequiredConfigs([]);
			setIsRestartBannerDismissed(false);
			sessionStorage.removeItem("restartBannerDismissed");

			// Wait a bit for the server to restart, then reload the page
			setTimeout(() => {
				window.location.reload();
			}, 3000);
		} catch (error) {
			console.error("Failed to restart server:", error);
		}
	};

	// Handle configuration updates
	// biome-ignore lint/suspicious/noExplicitAny: accepts various config types
	const handleConfigUpdate = async (section: string, data: any) => {
		try {
			if (section === "webdav" && config) {
				const webdavData = data as unknown as WebDAVConfig;
				const portChanged = webdavData.port !== config.webdav.port;
				await updateConfigSection.mutateAsync({
					section: "webdav",
					config: { webdav: webdavData },
				});
				if (portChanged) addRestartRequiredConfig("WebDAV Port");
			} else if (section === "auth") {
				await updateConfigSection.mutateAsync({
					section: "auth",
					config: { auth: data as unknown as AuthConfig },
				});
				// Re-evaluate auth state so loginRequired reflects the new config immediately
				await recheckAuth();
			} else if (section === "streaming") {
				await updateConfigSection.mutateAsync({
					section: "streaming",
					config: { streaming: data as unknown as StreamingConfig },
				});
			} else if (section === "segment_cache") {
				await updateConfigSection.mutateAsync({
					section: "segment_cache",
					config: { segment_cache: data as unknown as SegmentCacheConfig },
				});
			} else if (section === "import") {
				await updateConfigSection.mutateAsync({
					section: "import",
					config: { import: data as unknown as ImportConfig },
				});
			} else if (section === "metadata" && config) {
				const metadataData = data as unknown as MetadataConfig;
				const rootPathChanged = metadataData.root_path !== config.metadata.root_path;
				await updateConfigSection.mutateAsync({
					section: "metadata",
					config: { metadata: metadataData },
				});
				if (rootPathChanged) addRestartRequiredConfig("Metadata Root Path");
			} else if (section === "mount") {
				await updateConfigSection.mutateAsync({
					section: "mount",
					config: data,
				});
			} else if (section === "sabnzbd") {
				await updateConfigSection.mutateAsync({
					section: "sabnzbd",
					config: { sabnzbd: data as unknown as SABnzbdConfig },
				});
			} else if (section === "arrs") {
				await updateConfigSection.mutateAsync({
					section: "arrs",
					config: { arrs: data as unknown as ArrsConfig },
				});
			} else if (section === "health") {
				await updateConfigSection.mutateAsync({
					section: "health",
					config: { health: data as unknown as HealthConfig },
				});
			} else if (section === "stremio") {
				await updateConfigSection.mutateAsync({
					section: "stremio",
					config: { stremio: data as unknown as StremioConfig },
				});
			} else if (section === "providers") {
				await updateConfigSection.mutateAsync({
					section: "providers",
					config: { providers: data as unknown as ProviderConfig[] },
				});
			} else if (section === "user_agent") {
				await updateConfigSection.mutateAsync({
					section: "network",
					config: { user_agent: data as unknown as string },
				});
			} else if (section === "network") {
				await updateConfigSection.mutateAsync({
					section: "network",
					config: { network: data as unknown as NetworkConfig },
				});
			} else if (section === "par2_repair") {
				await updateConfigSection.mutateAsync({
					section: "par2_repair",
					config: { par2_repair: data as unknown as Par2RepairConfig },
				});
			} else if (section === "peernet") {
				await updateConfigSection.mutateAsync({
					section: "peernet",
					config: { peernet: data as unknown as PeernetConfig },
				});
			} else if (section === "log") {
				const logData = data as unknown as LogFormData & { profiler_enabled?: boolean };
				const { profiler_enabled, ...logConfig } = logData;
				await updateConfigSection.mutateAsync({
					section: "system",
					config: {
						log: logConfig,
						profiler_enabled: profiler_enabled,
					},
				});
			}
		} catch (error) {
			console.error("Failed to update configuration:", error);
			throw error;
		}
	};

	if (isLoading) {
		return (
			<div className="flex h-[60vh] w-full items-center justify-center">
				<LoadingSpinner size="lg" />
			</div>
		);
	}

	if (error) {
		return (
			<div className="space-y-4">
				<h1 className="font-bold text-3xl">Configuration</h1>
				<ErrorAlert error={error as Error} onRetry={() => refetch()} />
			</div>
		);
	}

	if (!config) {
		return (
			<div className="space-y-4">
				<h1 className="font-bold text-3xl">Configuration</h1>
				<div className="alert alert-warning">
					<AlertTriangle className="h-6 w-6" />
					<div>
						<div className="font-bold">Configuration Not Available</div>
						<div className="text-sm">Unable to load configuration.</div>
					</div>
				</div>
			</div>
		);
	}

	return (
		<div className="space-y-6">
			{/* Header */}
			<div className="flex flex-col justify-between gap-4 sm:flex-row sm:items-center">
				<div className="flex items-center space-x-3">
					<div className="shrink-0 rounded-xl bg-primary/10 p-2 shadow-inner">
						<Settings className="h-8 w-8 text-primary" />
					</div>
					<div className="min-w-0">
						<h1 className="truncate font-bold text-2xl tracking-tight sm:text-3xl">
							Configuration
						</h1>
						<p className="text-base-content/60 text-xs sm:text-sm">
							System settings and preferences.
						</p>
					</div>
				</div>

				<div className="flex flex-wrap items-center gap-2 sm:flex-nowrap">
					<button
						type="button"
						className="btn btn-ghost btn-sm border-base-300 bg-base-100 hover:bg-base-200"
						onClick={handleReloadConfig}
						disabled={reloadConfig.isPending}
					>
						{reloadConfig.isPending ? (
							<LoadingSpinner size="sm" />
						) : (
							<RefreshCw className="h-4 w-4" />
						)}
						Reload
					</button>

					<button
						type="button"
						className="btn btn-error btn-outline btn-sm"
						onClick={handleRestartServer}
						disabled={restartServer.isPending}
					>
						{restartServer.isPending ? <LoadingSpinner size="sm" /> : <Radio className="h-4 w-4" />}
						Restart
					</button>
				</div>
			</div>

			<RestartRequiredBanner
				restartRequiredConfigs={restartRequiredConfigs}
				onDismiss={handleDismissRestartBanner}
				isDismissed={isRestartBannerDismissed}
			/>

			{syncNeeded?.needs_sync && (
				<div className="alert alert-warning rounded-2xl border border-warning/20 bg-warning/5 shadow-sm">
					<AlertTriangle className="h-6 w-6" />
					<div className="flex-1">
						<div className="font-bold">Library Sync Required</div>
						<div className="text-sm opacity-80">Mount path has been updated. Update symlinks?</div>
					</div>
					<button
						type="button"
						className="btn btn-primary btn-sm px-6"
						onClick={() => triggerLibrarySync.mutate()}
						disabled={triggerLibrarySync.isPending}
					>
						{triggerLibrarySync.isPending ? <LoadingSpinner size="sm" /> : "Run Now"}
					</button>
				</div>
			)}

			<div className="grid grid-cols-1 gap-6 lg:grid-cols-12">
				{/* Modern Sidebar (Stacks on mobile, exactly like Import) */}
				<div className="lg:col-span-3 xl:col-span-2">
					{" "}
					<div className="card border border-base-200 bg-base-100/50 shadow-sm backdrop-blur-md sm:sticky sm:top-24">
						<div className="card-body p-2 sm:p-4">
							{SECTION_GROUPS.map((group) => (
								<div key={group.title} className="mb-4 last:mb-0">
									<h3 className="mb-2 px-4 font-bold text-base-content/40 text-xs uppercase tracking-widest">
										{group.title}
									</h3>
									<ul className="menu menu-md gap-1 p-0">
										{group.sections.map((key) => {
											const s = CONFIG_SECTIONS[key as ConfigSection | "system"];
											if (s.hidden) return null;
											const Icon = getIconComponent(s.icon);
											const isActive = activeSection === key;
											return (
												<li key={key}>
													<button
														type="button"
														aria-current={isActive ? "page" : undefined}
														className={`flex items-center gap-3 rounded-xl py-3 pr-4 pl-6 transition-all ${
															isActive
																? "scale-[1.02] bg-primary font-bold text-primary-content shadow-lg shadow-primary/20"
																: "text-base-content/70 hover:bg-base-200"
														}`}
														onClick={() => navigate(`/config/${key}`)}
													>
														<Icon className={`h-5 w-5 ${isActive ? "" : "text-base-content/40"}`} />
														<div className="min-w-0 flex-1 text-left">
															<div className="text-sm">{s.title}</div>
														</div>
														{!s.canEdit && (
															<span className="badge badge-ghost badge-xs text-base-content/70">
																🔒
															</span>
														)}
													</button>
												</li>
											);
										})}
									</ul>
								</div>
							))}
						</div>
					</div>
				</div>

				{/* Modern Content Card — min-w-0 lets grid column shrink so long inputs don’t overflow on narrow viewports */}
				<div className="min-w-0 lg:col-span-9 xl:col-span-10">
					{" "}
					<div className="card min-h-[600px] overflow-hidden rounded-2xl border-2 border-base-300/50 bg-base-100 shadow-md">
						<div className="card-body p-4 sm:p-10">
							{/* Modern Section Header */}
							<div className="mb-10 border-base-200 border-b pb-8">
								<div className="mb-2 flex items-start space-x-5 sm:items-center">
									<div className="shrink-0 rounded-2xl bg-primary/10 p-4 shadow-inner">
										{(() => {
											const Icon = getIconComponent(CONFIG_SECTIONS[activeSection].icon);
											return <Icon className="h-8 w-8 text-primary" />;
										})()}
									</div>
									<div className="min-w-0 flex-1">
										<h2 className="break-words font-bold text-3xl text-base-content tracking-tight">
											{CONFIG_SECTIONS[activeSection].title}
										</h2>
										<p className="mt-1 max-w-2xl break-words text-base-content/60 text-sm leading-relaxed">
											{CONFIG_SECTIONS[activeSection].description}
										</p>
									</div>
								</div>
							</div>

							<div
								className={`mx-auto w-full min-w-0 ${activeSection === "providers" ? "max-w-none" : "max-w-4xl"}`}
							>
								{activeSection === "webdav" && (
									<WebDAVConfigSection
										config={config}
										onUpdate={handleConfigUpdate}
										isUpdating={updateConfigSection.isPending}
									/>
								)}
								{activeSection === "auth" && (
									<AuthConfigSection
										config={config}
										onUpdate={handleConfigUpdate}
										isUpdating={updateConfigSection.isPending}
									/>
								)}
								{activeSection === "import" && (
									<ImportConfigSection
										config={config}
										onUpdate={handleConfigUpdate}
										isUpdating={updateConfigSection.isPending}
									/>
								)}
								{activeSection === "metadata" && (
									<MetadataConfigSection
										config={config}
										onUpdate={handleConfigUpdate}
										isUpdating={updateConfigSection.isPending}
									/>
								)}
								{activeSection === "streaming" && (
									<StreamingConfigSection
										config={config}
										onUpdate={handleConfigUpdate}
										isUpdating={updateConfigSection.isPending}
									/>
								)}
								{activeSection === "system" && (
									<SystemConfigSection
										config={config}
										onUpdate={handleConfigUpdate}
										onRefresh={async () => {
											await refetch();
										}}
										isUpdating={updateConfigSection.isPending}
									/>
								)}
								{activeSection === "providers" && (
									<ProvidersConfigSection
										config={config}
										onUpdate={handleConfigUpdate}
										isUpdating={updateConfigSection.isPending}
									/>
								)}
								{activeSection === "mount" && (
									<MountConfigSection
										config={config}
										onUpdate={handleConfigUpdate}
										isUpdating={updateConfigSection.isPending}
									/>
								)}
								{activeSection === "sabnzbd" && (
									<SABnzbdConfigSection
										config={config}
										onUpdate={handleConfigUpdate}
										isUpdating={updateConfigSection.isPending}
									/>
								)}
								{activeSection === "arrs" && (
									<ArrsConfigSection
										config={config}
										onUpdate={handleConfigUpdate}
										isUpdating={updateConfigSection.isPending}
									/>
								)}
								{activeSection === "health" && (
									<HealthConfigSection
										config={config}
										onUpdate={handleConfigUpdate}
										isUpdating={updateConfigSection.isPending}
									/>
								)}
								{activeSection === "stremio" && (
									<StremioConfigSection
										config={config}
										onUpdate={handleConfigUpdate}
										isUpdating={updateConfigSection.isPending}
									/>
								)}
								{activeSection === "network" && (
									<NetworkConfigSection
										config={config}
										onUpdate={handleConfigUpdate}
										isUpdating={updateConfigSection.isPending}
									/>
								)}
								{activeSection === "par2_repair" && (
									<Par2RepairConfigSection
										config={config}
										onUpdate={handleConfigUpdate}
										isUpdating={updateConfigSection.isPending}
									/>
								)}
								{activeSection === "peernet" && (
									<PeernetConfigSection
										config={config}
										onUpdate={handleConfigUpdate}
										isUpdating={updateConfigSection.isPending}
									/>
								)}
								{![
									"webdav",
									"auth",
									"import",
									"metadata",
									"streaming",
									"system",
									"providers",
									"mount",
									"sabnzbd",
									"arrs",
									"health",
									"stremio",
									"network",
									"par2_repair",
									"peernet",
								].includes(activeSection) && (
									<ComingSoonSection
										sectionName={CONFIG_SECTIONS[activeSection]?.title || activeSection}
									/>
								)}
							</div>
						</div>
					</div>
				</div>
			</div>
		</div>
	);
}
