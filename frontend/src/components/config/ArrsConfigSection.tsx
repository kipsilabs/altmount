import { AlertTriangle, Plus, Save, Trash2, Webhook } from "lucide-react";
import { useEffect, useState } from "react";
import { useRegisterArrsWebhooks } from "../../hooks/useApi";
import type {
	ArrsConfig,
	ArrsInstanceConfig,
	ArrsType,
	ConfigResponse,
	StuckCleanupAction,
	StuckCleanupRule,
} from "../../types/config";
import { LoadingSpinner } from "../ui/LoadingSpinner";
import { ArrsInstanceCard } from "./ArrsInstanceCard";

interface ArrsConfigSectionProps {
	config: ConfigResponse;
	onUpdate?: (section: string, data: ArrsConfig) => Promise<void>;
	isReadOnly?: boolean;
	isUpdating?: boolean;
}

interface NewInstanceForm {
	api_version: "auto" | "v3" | "v5";
	name: string;
	type: ArrsType;
	url: string;
	api_key: string;
	category: string;
	enabled: boolean;
}

const DEFAULT_NEW_INSTANCE: NewInstanceForm = {
	api_version: "auto",
	name: "",
	type: "radarr",
	url: "",
	api_key: "",
	category: "movies",
	enabled: true,
};

const ARR_TYPES: { type: ArrsType; label: string; color: string; defaultCategory: string }[] = [
	// Distinct accents per type; minimal themes collapse primary/secondary/accent/info to one blue.
	{ type: "radarr", label: "Radarr", color: "bg-warning", defaultCategory: "movies" },
	{ type: "sonarr", label: "Sonarr", color: "bg-info", defaultCategory: "tv" },
	{ type: "lidarr", label: "Lidarr", color: "bg-success", defaultCategory: "music" },
	{ type: "readarr", label: "Readarr", color: "bg-error", defaultCategory: "books" },
	{ type: "whisparr", label: "Whisparr", color: "bg-secondary", defaultCategory: "movies" },
	{ type: "sportarr", label: "Sportarr", color: "bg-accent", defaultCategory: "sports" },
];

export function ArrsConfigSection({
	config,
	onUpdate,
	isReadOnly = false,
	isUpdating = false,
}: ArrsConfigSectionProps) {
	const [formData, setFormData] = useState<ArrsConfig>(config.arrs);
	const [hasChanges, setHasChanges] = useState(false);
	const [showAddInstance, setShowAddInstance] = useState(false);
	const [newInstance, setNewInstance] = useState<NewInstanceForm>(DEFAULT_NEW_INSTANCE);
	const [validationErrors, setValidationErrors] = useState<string[]>([]);
	const [showApiKeys, setShowApiKeys] = useState<Record<string, boolean>>({});
	const [webhookSuccess, setWebhookSuccess] = useState<string | null>(null);
	const [webhookError, setWebhookError] = useState<string | null>(null);
	const [saveError, setSaveError] = useState<string | null>(null);
	const [newStuckPattern, setNewStuckPattern] = useState("");

	const registerWebhooks = useRegisterArrsWebhooks();
	const defaultWebhookUrl = `http://${config.webdav.host || "altmount"}:${config.webdav.port}`;

	// Sync form data when config changes from external sources (reload)
	useEffect(() => {
		setFormData(config.arrs);
		setHasChanges(false);
		setValidationErrors([]);
		setSaveError(null);
	}, [config.arrs]);

	const handleRegisterWebhooks = async () => {
		setWebhookSuccess(null);
		setWebhookError(null);
		try {
			await registerWebhooks.mutateAsync();
			setWebhookSuccess("Webhook registration triggered successfully.");
			// Hide success message after 5 seconds
			setTimeout(() => setWebhookSuccess(null), 5000);
		} catch (error) {
			setWebhookError(error instanceof Error ? error.message : "Failed to register webhooks.");
		}
	};

	const validateForm = (data: ArrsConfig): string[] => {
		const errors: string[] = [];
		if (data.enabled) {
			if (!config.mount_path) {
				errors.push(
					"Mount Path must be configured in General/System settings before enabling Arrs service",
				);
			}

			const allInstances: { instance: ArrsInstanceConfig; typeLabel: string }[] = [];
			for (const { type, label } of ARR_TYPES) {
				const instances = data[`${type}_instances` as keyof ArrsConfig] as ArrsInstanceConfig[];
				if (instances) {
					allInstances.push(...instances.map((i) => ({ instance: i, typeLabel: label })));
				}
			}

			const nameCount: Record<string, number> = {};
			allInstances.forEach(({ instance }) => {
				nameCount[instance.name] = (nameCount[instance.name] || 0) + 1;
			});
			Object.entries(nameCount).forEach(([name, count]) => {
				if (count > 1) errors.push(`Instance name "${name}" is used multiple times`);
			});

			allInstances.forEach(({ instance, typeLabel }, index) => {
				if (!instance.name.trim())
					errors.push(`${typeLabel} instance #${index + 1}: Name is required`);
				if (!instance.url.trim()) {
					errors.push(`${typeLabel} instance "${instance.name}": URL is required`);
				} else {
					try {
						new URL(instance.url);
					} catch {
						errors.push(`${typeLabel} instance "${instance.name}": Invalid URL format`);
					}
				}
				if (!instance.api_key.trim())
					errors.push(`${typeLabel} instance "${instance.name}": API key is required`);
			});
		}
		return errors;
	};

	const handleFormChange = (field: keyof ArrsConfig, value: ArrsConfig[keyof ArrsConfig]) => {
		const newFormData = { ...formData, [field]: value };
		setFormData(newFormData);
		setHasChanges(true);
		setValidationErrors(validateForm(newFormData));
	};

	const handleInstanceChange = (
		type: ArrsType,
		index: number,
		field: keyof ArrsInstanceConfig,
		value: ArrsInstanceConfig[keyof ArrsInstanceConfig],
	) => {
		const instancesKey = `${type}_instances` as keyof ArrsConfig;
		const instances = [...(formData[instancesKey] as ArrsInstanceConfig[])];
		instances[index] = { ...instances[index], [field]: value };
		const newFormData = { ...formData, [instancesKey]: instances };
		setFormData(newFormData);
		setHasChanges(true);
		setValidationErrors(validateForm(newFormData));
	};

	const removeInstance = (type: ArrsType, index: number) => {
		const instancesKey = `${type}_instances` as keyof ArrsConfig;
		const instances = [...(formData[instancesKey] as ArrsInstanceConfig[])];
		instances.splice(index, 1);
		const newFormData = { ...formData, [instancesKey]: instances };
		setFormData(newFormData);
		setHasChanges(true);
		setValidationErrors(validateForm(newFormData));
	};

	const addInstance = () => {
		if (!newInstance.name.trim() || !newInstance.url.trim() || !newInstance.api_key.trim()) return;
		const instancesKey = `${newInstance.type}_instances` as keyof ArrsConfig;
		let category = newInstance.category.trim();
		if (!category) {
			const arrTypeMeta = ARR_TYPES.find((t) => t.type === newInstance.type);
			category = arrTypeMeta?.defaultCategory || "movies";
		}
		const instances = [
			...((formData[instancesKey] as ArrsInstanceConfig[]) || []),
			{
				name: newInstance.name,
				url: newInstance.url,
				api_key: newInstance.api_key,
				...(newInstance.type === "sonarr" ? { api_version: newInstance.api_version } : {}),
				category: category,
				enabled: newInstance.enabled,
				sync_interval_hours: 1,
			},
		];
		const newFormData = { ...formData, [instancesKey]: instances };
		setFormData(newFormData);
		setHasChanges(true);
		setValidationErrors(validateForm(newFormData));
		setNewInstance(DEFAULT_NEW_INSTANCE);
		setShowAddInstance(false);
	};

	const handleAddStuckPattern = () => {
		if (!newStuckPattern.trim()) return;
		const currentList = formData.queue_cleanup_rules || [];
		if (currentList.some((m) => m.message === newStuckPattern.trim())) {
			setNewStuckPattern("");
			return;
		}
		const newList: StuckCleanupRule[] = [
			...currentList,
			{ message: newStuckPattern.trim(), enabled: true, action: "blocklist_search" },
		];
		handleFormChange("queue_cleanup_rules", newList);
		setNewStuckPattern("");
	};

	const handleRemoveStuckPattern = (index: number) => {
		const newList = [...(formData.queue_cleanup_rules || [])];
		newList.splice(index, 1);
		handleFormChange("queue_cleanup_rules", newList);
	};

	const handleToggleStuckPattern = (index: number) => {
		const newList = [...(formData.queue_cleanup_rules || [])];
		newList[index] = { ...newList[index], enabled: !newList[index].enabled };
		handleFormChange("queue_cleanup_rules", newList);
	};

	const handleSetStuckRuleAction = (index: number, action: StuckCleanupAction) => {
		const newList = [...(formData.queue_cleanup_rules || [])];
		newList[index] = { ...newList[index], action };
		handleFormChange("queue_cleanup_rules", newList);
	};

	const handleSave = async () => {
		if (!onUpdate || validationErrors.length > 0) return;
		setSaveError(null);
		try {
			await onUpdate("arrs", formData);
			setHasChanges(false);
		} catch (error) {
			console.error("Failed to save arrs configuration:", error);
			setSaveError(error instanceof Error ? error.message : "Failed to save configuration");
		}
	};

	const toggleApiKeyVisibility = (instanceId: string) => {
		setShowApiKeys((prev) => ({ ...prev, [instanceId]: !prev[instanceId] }));
	};

	const renderInstance = (instance: ArrsInstanceConfig, type: ArrsType, index: number) => {
		const instanceId = `${type}-${index}`;
		const isApiKeyVisible = showApiKeys[instanceId] || false;
		return (
			<ArrsInstanceCard
				key={instanceId}
				instance={instance}
				type={type}
				index={index}
				isReadOnly={isReadOnly}
				isApiKeyVisible={isApiKeyVisible}
				categories={config.sabnzbd?.categories}
				onToggleApiKey={() => toggleApiKeyVisibility(instanceId)}
				onRemove={() => removeInstance(type, index)}
				onInstanceChange={(field, value) => handleInstanceChange(type, index, field, value)}
			/>
		);
	};

	return (
		<div className="space-y-10">
			<div className="space-y-8">
				{/* Enable/Disable Arrs */}
				<div className="rounded-2xl border-2 border-base-300/80 bg-base-200/60 p-6">
					<div className="flex items-start justify-between gap-4">
						<div className="min-w-0 flex-1">
							<h4 className="break-words font-bold text-base-content text-sm">Service Engine</h4>
							<p className="mt-1 break-words text-[11px] text-base-content/50 leading-relaxed">
								Allows AltMount to talk to Radarr/Sonarr for repairs and updates.
							</p>
						</div>
						<input
							type="checkbox"
							className="toggle toggle-primary mt-1 shrink-0"
							checked={formData.enabled}
							onChange={(e) => handleFormChange("enabled", e.target.checked)}
							disabled={isReadOnly}
						/>
					</div>
				</div>

				{/* Webhooks Auto-Registration */}
				{formData.enabled && (
					<div className="fade-in slide-in-from-top-2 animate-in space-y-6 rounded-2xl border-2 border-base-300/80 bg-base-200/60 p-6">
						<div className="flex items-center gap-2">
							<h4 className="font-bold text-base-content/40 text-xs uppercase tracking-widest">
								Automation
							</h4>
							<div className="h-px flex-1 bg-base-300/50" />
						</div>

						<div className="space-y-6">
							<div>
								<h5 className="font-bold text-sm">AltMount Webhooks</h5>
								<p className="mt-1 break-words text-[11px] text-base-content/50 leading-relaxed">
									Automatically configure hooks in ARR applications to notify AltMount of upgrades
									and renames.
								</p>
							</div>

							<div className="flex flex-col gap-4 sm:flex-row sm:items-end">
								<fieldset className="fieldset flex-1">
									<legend className="fieldset-legend font-semibold">AltMount Callback URL</legend>
									<input
										type="url"
										className="input input-bordered w-full bg-base-100 font-mono text-sm"
										value={formData.webhook_base_url ?? defaultWebhookUrl}
										onChange={(e) => handleFormChange("webhook_base_url", e.target.value)}
										placeholder={defaultWebhookUrl}
										disabled={isReadOnly}
									/>
								</fieldset>

								<button
									type="button"
									className="btn btn-primary shrink-0 px-6 shadow-lg shadow-primary/20"
									onClick={handleRegisterWebhooks}
									disabled={isReadOnly || registerWebhooks.isPending || hasChanges}
								>
									{registerWebhooks.isPending ? (
										<LoadingSpinner size="sm" />
									) : (
										<Webhook className="h-4 w-4" />
									)}
									{registerWebhooks.isPending ? "Connecting..." : "Setup Webhooks"}
								</button>
							</div>

							{hasChanges && (
								<p className="font-bold text-warning text-xs">
									Save changes before configuring webhooks.
								</p>
							)}
							{webhookSuccess && (
								<div className="alert alert-success rounded-xl py-2 text-xs">{webhookSuccess}</div>
							)}
							{webhookError && (
								<div className="alert alert-error rounded-xl py-2 text-xs">{webhookError}</div>
							)}
						</div>
					</div>
				)}

				{/* Queue Cleanup Settings */}
				{formData.enabled && (
					<div className="fade-in slide-in-from-top-4 animate-in space-y-6 rounded-2xl border-2 border-base-300/80 bg-base-200/60 p-6">
						<div className="flex items-center gap-2">
							<h4 className="font-bold text-base-content/40 text-xs uppercase tracking-widest">
								Maintenance
							</h4>
							<div className="h-px flex-1 bg-base-300/50" />
						</div>

						<div className="flex items-start justify-between gap-4">
							<div className="min-w-0 flex-1">
								<h5 className="break-words font-bold text-base-content text-sm">
									Queue Auto-Cleanup
								</h5>
								<p className="mt-1 break-words text-[11px] text-base-content/50 leading-relaxed">
									Automatically clear stuck and failed imports from your *arr queues.
								</p>
							</div>
							<input
								type="checkbox"
								className="checkbox checkbox-primary checkbox-sm mt-1 shrink-0"
								checked={formData.queue_cleanup_enabled ?? true}
								onChange={(e) => handleFormChange("queue_cleanup_enabled", e.target.checked)}
								disabled={isReadOnly}
							/>
						</div>

						{(formData.queue_cleanup_enabled ?? true) && (
							<div className="fade-in zoom-in-95 animate-in space-y-6">
								<div className="grid grid-cols-1 gap-6 sm:grid-cols-2 lg:grid-cols-3">
									<fieldset className="fieldset">
										<legend className="fieldset-legend font-semibold">Cleanup Interval</legend>
										<div className="join w-full">
											<input
												type="number"
												className="input input-bordered join-item w-full bg-base-100 font-mono text-sm"
												value={formData.queue_cleanup_interval_seconds ?? 10}
												onChange={(e) =>
													handleFormChange(
														"queue_cleanup_interval_seconds",
														Number.parseInt(e.target.value, 10) || 10,
													)
												}
												min={1}
												max={3600}
												disabled={isReadOnly}
											/>
											<span className="btn btn-ghost join-item pointer-events-none border-base-300 text-xs">
												sec
											</span>
										</div>
										<div className="mt-2 whitespace-normal text-base-content/70 text-xs">
											How often *arr queues are checked.
										</div>
									</fieldset>

									<fieldset className="fieldset">
										<legend className="fieldset-legend whitespace-normal font-semibold md:whitespace-nowrap">
											Cleanup Grace Period
										</legend>
										<div className="join w-full">
											<input
												type="number"
												className="input input-bordered join-item w-full bg-base-100 font-mono text-sm"
												value={formData.queue_cleanup_grace_period_minutes ?? 5}
												onChange={(e) =>
													handleFormChange(
														"queue_cleanup_grace_period_minutes",
														Number.parseInt(e.target.value, 10) || 5,
													)
												}
												min={0}
												disabled={isReadOnly}
											/>
											<span className="btn btn-ghost join-item pointer-events-none border-base-300 text-xs">
												min
											</span>
										</div>
										<div className="mt-2 whitespace-normal text-base-content/70 text-xs">
											How long an import must stay stuck before cleanup acts. Brief errors are
											ignored.
										</div>
									</fieldset>

									<fieldset className="fieldset">
										<legend className="fieldset-legend whitespace-normal font-semibold md:whitespace-nowrap">
											Failure Limit
										</legend>
										<div className="join w-full">
											<input
												type="number"
												className="input input-bordered join-item w-full bg-base-100 font-mono text-sm"
												value={formData.queue_cleanup_max_failures ?? 0}
												onChange={(e) =>
													handleFormChange(
														"queue_cleanup_max_failures",
														Math.max(0, Number.parseInt(e.target.value, 10) || 0),
													)
												}
												min={0}
												disabled={isReadOnly}
											/>
											<span className="btn btn-ghost join-item pointer-events-none border-base-300 text-xs">
												tries
											</span>
										</div>
										<div className="mt-2 whitespace-normal text-base-content/70 text-xs">
											After this many cleanups on the same item, give up: blocklist and unmonitor
											it. 0 disables.
										</div>
									</fieldset>
								</div>

								<div className="space-y-4">
									<h5 className="font-bold text-base-content/60 text-xs uppercase">Error Rules</h5>
									<p className="whitespace-normal text-base-content/70 text-xs">
										Match a stuck import's error to an action: remove, blocklist, or blocklist +
										search.
									</p>
									<div className="custom-scrollbar max-h-72 space-y-2 overflow-y-auto pr-2">
										{(formData.queue_cleanup_rules || []).map((rule, index) => (
											<div
												key={index}
												className="flex items-center gap-2 rounded-xl border border-base-300/50 bg-base-100/50 p-2 pl-3"
											>
												<input
													type="checkbox"
													className="checkbox checkbox-sm checkbox-primary shrink-0"
													checked={rule.enabled}
													onChange={() => handleToggleStuckPattern(index)}
													disabled={isReadOnly}
												/>
												<span
													className={`min-w-0 flex-1 truncate font-mono text-xs ${!rule.enabled ? "text-base-content/50 line-through" : ""}`}
													title={rule.message}
												>
													{rule.message}
												</span>
												<select
													className="select select-bordered select-xs w-44 shrink-0 bg-base-100 text-xs"
													value={rule.action}
													onChange={(e) =>
														handleSetStuckRuleAction(index, e.target.value as StuckCleanupAction)
													}
													disabled={isReadOnly || !rule.enabled}
												>
													<option value="remove">Remove only</option>
													<option value="blocklist">Blocklist</option>
													<option value="blocklist_search">Blocklist + search</option>
												</select>
												<button
													type="button"
													className="btn btn-ghost btn-sm shrink-0 text-error hover:bg-error/10"
													onClick={() => handleRemoveStuckPattern(index)}
													disabled={isReadOnly}
												>
													<Trash2 className="h-3 w-3" />
												</button>
											</div>
										))}
									</div>

									{!isReadOnly && (
										<div className="join w-full shadow-sm">
											<input
												type="text"
												className="input input-bordered join-item flex-1 bg-base-100 text-xs"
												placeholder="Add an *arr error message... (defaults to blocklist + search)"
												value={newStuckPattern}
												onChange={(e) => setNewStuckPattern(e.target.value)}
												onKeyDown={(e) => e.key === "Enter" && handleAddStuckPattern()}
											/>
											<button
												type="button"
												className="btn btn-primary join-item px-4"
												onClick={handleAddStuckPattern}
												disabled={!newStuckPattern.trim()}
											>
												<Plus className="h-4 w-4" />
											</button>
										</div>
									)}
								</div>
							</div>
						)}
					</div>
				)}

				{/* Instances Lists */}
				{formData.enabled && (
					<div className="fade-in slide-in-from-top-6 animate-in space-y-10">
						{ARR_TYPES.map(({ type, label, color, defaultCategory }) => {
							const instancesKey = `${type}_instances` as keyof ArrsConfig;
							const instances = (formData[instancesKey] || []) as ArrsInstanceConfig[];

							return (
								<div key={type} className="space-y-6">
									<div className="flex items-center justify-between gap-4">
										<h4 className="flex items-center gap-2 font-bold text-sm">
											<div className={`h-2 w-2 rounded-full ${color}`} /> {label} Instances
										</h4>
										<button
											type="button"
											className="btn btn-sm btn-primary px-4"
											onClick={() => {
												setNewInstance({
													...DEFAULT_NEW_INSTANCE,
													type,
													category: defaultCategory,
												});
												setShowAddInstance(true);
											}}
											disabled={isReadOnly}
										>
											<Plus className="h-3 w-3" /> Add
										</button>
									</div>
									<div className="grid grid-cols-1 gap-4">
										{instances.map((instance, index) => renderInstance(instance, type, index))}
										{instances.length === 0 && (
											<div className="rounded-2xl border-2 border-base-300 border-dashed p-8 text-center font-bold text-base-content/60 text-xs">
												No {label} configured
											</div>
										)}
									</div>
								</div>
							);
						})}
					</div>
				)}
			</div>

			{/* Modal for adding instance */}
			{showAddInstance && (
				<div className="modal modal-open backdrop-blur-sm">
					<div className="modal-box rounded-2xl border border-base-300 shadow-2xl">
						<h3 className="mb-6 font-black text-xl uppercase tracking-tighter">
							Add {ARR_TYPES.find((t) => t.type === newInstance.type)?.label || "ARR"}
						</h3>
						<div className="space-y-5">
							<fieldset className="fieldset">
								<legend className="fieldset-legend font-bold">Friendly Name</legend>
								<input
									type="text"
									className="input input-bordered w-full"
									value={newInstance.name}
									onChange={(e) => setNewInstance((prev) => ({ ...prev, name: e.target.value }))}
									placeholder="My ARR Server"
								/>
							</fieldset>
							<fieldset className="fieldset">
								<legend className="fieldset-legend font-bold">Server URL</legend>
								<input
									type="url"
									className="input input-bordered w-full font-mono text-sm"
									value={newInstance.url}
									onChange={(e) => setNewInstance((prev) => ({ ...prev, url: e.target.value }))}
									placeholder="http://192.168.1.10:7878"
								/>
							</fieldset>
							<fieldset className="fieldset">
								<legend className="fieldset-legend font-bold">API Key</legend>
								<input
									type="password"
									title="API Key"
									className="input input-bordered w-full font-mono text-sm"
									value={newInstance.api_key}
									onChange={(e) => setNewInstance((prev) => ({ ...prev, api_key: e.target.value }))}
								/>
							</fieldset>
							{newInstance.type === "sonarr" && (
								<fieldset className="fieldset">
									<legend className="fieldset-legend font-bold">Sonarr API</legend>
									<select
										className="select select-bordered w-full"
										value={newInstance.api_version}
										onChange={(e) =>
											setNewInstance((prev) => ({
												...prev,
												api_version: e.target.value as NewInstanceForm["api_version"],
											}))
										}
									>
										<option value="auto">Auto (default)</option>
										<option value="v3">Legacy v3</option>
										<option value="v5">Native v5</option>
									</select>
								</fieldset>
							)}
							<fieldset className="fieldset">
								<legend className="fieldset-legend font-bold">Category Mapping</legend>
								<select
									className="select select-bordered w-full"
									value={newInstance.category}
									onChange={(e) =>
										setNewInstance((prev) => ({ ...prev, category: e.target.value }))
									}
								>
									<option value="">(Auto Detect)</option>
									{config.sabnzbd?.categories?.map((cat) => (
										<option key={cat.name} value={cat.name}>
											{cat.name}
										</option>
									))}
								</select>
							</fieldset>
						</div>
						<div className="modal-action gap-3">
							<button
								type="button"
								className="btn btn-ghost"
								onClick={() => {
									setShowAddInstance(false);
									setNewInstance(DEFAULT_NEW_INSTANCE);
								}}
							>
								Cancel
							</button>
							<button
								type="button"
								className="btn btn-primary px-8 shadow-lg shadow-primary/20"
								onClick={addInstance}
								disabled={
									!newInstance.name.trim() || !newInstance.url.trim() || !newInstance.api_key.trim()
								}
							>
								Add Server
							</button>
						</div>
					</div>
				</div>
			)}

			{/* Validation & Save */}
			<div className="space-y-4 border-base-200 border-t pt-6">
				{validationErrors.map((error, idx) => (
					<div
						key={idx}
						className="alert alert-warning rounded-xl border border-warning/20 bg-warning/5 py-2 text-xs"
					>
						<AlertTriangle className="h-4 w-4 shrink-0" />
						<span className="break-words">{error}</span>
					</div>
				))}
				{saveError && <div className="alert alert-error rounded-xl py-2 text-xs">{saveError}</div>}

				{hasChanges && (
					<div className="flex justify-end">
						<button
							type="button"
							className={`btn btn-primary px-10 shadow-lg shadow-primary/20 ${isUpdating ? "loading" : ""}`}
							onClick={handleSave}
							disabled={isUpdating || validationErrors.length > 0}
						>
							{!isUpdating && <Save className="h-4 w-4" />}
							{isUpdating ? "Saving..." : "Save Changes"}
						</button>
					</div>
				)}
			</div>
		</div>
	);
}
