import { AlertTriangle, Check, Loader, Save, Wifi } from "lucide-react";
import { useEffect, useState } from "react";
import { useToast } from "../../contexts/ToastContext";
import { useProviderBackbones, useProviders } from "../../hooks/useProviders";
import type { ProviderBackbone, ProviderConfig, ProviderFormData } from "../../types/config";

// registrableDomain returns the last two labels of a hostname (e.g. "eweka.nl"),
// used as a fallback when an exact host match isn't found in the backbone list.
function registrableDomain(host: string): string {
	const parts = host.split(".").filter(Boolean);
	if (parts.length < 2) return "";
	return parts.slice(-2).join(".");
}

// lookupBackbone finds the backbone for a hostname: an exact (case-insensitive)
// match first, then a same-registrable-domain match, but only when every such
// entry agrees on a single backbone (avoids guessing when ambiguous).
function lookupBackbone(host: string, backbones: ProviderBackbone[]): ProviderBackbone | undefined {
	const h = host.trim().toLowerCase();
	if (!h || backbones.length === 0) return undefined;

	const exact = backbones.find((b) => b.host === h);
	if (exact) return exact;

	const domain = registrableDomain(h);
	if (!domain) return undefined;

	const matches = backbones.filter((b) => registrableDomain(b.host) === domain);
	if (matches.length === 0) return undefined;
	const distinct = new Set(matches.map((m) => m.backbone));
	return distinct.size === 1 ? matches[0] : undefined;
}

interface ProviderModalProps {
	mode: "create" | "edit";
	provider?: ProviderConfig | null;
	/** Seeds the "Backup Only" toggle when creating a new provider. */
	defaultBackup?: boolean;
	/** Global provider user-agent, applied to newly created providers. */
	defaultUserAgent?: string;
	onSuccess: () => void;
	onCancel: () => void;
}

const BYTES_PER_GB = 1_000_000_000;

const defaultFormData: ProviderFormData = {
	name: "",
	host: "",
	port: 119,
	username: "",
	password: "",
	max_connections: 10,
	min_connections_alive: 0,
	inflight_requests: 10,
	stat_inflight_requests: 100,
	tls: false,
	insecure_tls: false,
	proxy_url: "",
	enabled: true,
	is_backup_provider: false,
	storage_group: "",
	skip_ping: false,
	keepalive_interval_seconds: 0,
	keepalive_command: "",
	user_agent: "",
	quota_bytes: 0,
	quota_period_hours: 0,
	max_article_age_days: 0,
	strict_max_article_age: false,
	account_expiration_date: "",
};

export function ProviderModal({
	mode,
	provider,
	defaultBackup = false,
	defaultUserAgent = "",
	onSuccess,
	onCancel,
}: ProviderModalProps) {
	const [formData, setFormData] = useState<ProviderFormData>(defaultFormData);
	const [isTestingConnection, setIsTestingConnection] = useState(false);
	const [connectionTestResult, setConnectionTestResult] = useState<{
		success: boolean;
		message?: string;
		rttMs?: number;
	} | null>(null);
	const [canSave, setCanSave] = useState(false);
	const [quotaEnabled, setQuotaEnabled] = useState(false);
	const [retentionEnabled, setRetentionEnabled] = useState(false);
	const [quotaGbInput, setQuotaGbInput] = useState("");
	const [backboneHint, setBackboneHint] = useState<string | null>(null);

	const { testProvider, createProvider, updateProvider } = useProviders();
	const { data: backbones } = useProviderBackbones();
	const { showToast } = useToast();

	// Suggest a storage group from the entered host's backbone. Only fills an
	// empty field and never overwrites a value the user typed.
	const handleHostBlur = () => {
		if (!backbones || formData.storage_group.trim() !== "") return;
		const match = lookupBackbone(formData.host, backbones);
		if (match) {
			setFormData((prev) => ({ ...prev, storage_group: match.backbone }));
			setBackboneHint(`Detected backbone: ${match.backbone} (${match.provider})`);
		}
	};

	// Initialize form data when provider changes
	useEffect(() => {
		if (mode === "edit" && provider) {
			setFormData({
				name: provider.name ?? "",
				host: provider.host,
				port: provider.port,
				username: provider.username,
				password: "", // Always start with empty password for security
				max_connections: provider.max_connections,
				min_connections_alive: provider.min_connections_alive ?? 0,
				inflight_requests: provider.inflight_requests || 10,
				stat_inflight_requests: provider.stat_inflight_requests || 100,
				tls: provider.tls,
				insecure_tls: provider.insecure_tls,
				proxy_url: provider.proxy_url || "",
				enabled: provider.enabled,
				is_backup_provider: provider.is_backup_provider,
				storage_group: provider.storage_group ?? "",
				skip_ping: provider.skip_ping ?? false,
				keepalive_interval_seconds: provider.keepalive_interval_seconds ?? 0,
				keepalive_command: provider.keepalive_command ?? "",
				user_agent: provider.user_agent ?? "",
				quota_bytes: provider.quota_bytes ?? 0,
				quota_period_hours: provider.quota_period_hours ?? 0,
				max_article_age_days: provider.max_article_age_days ?? 0,
				strict_max_article_age: provider.strict_max_article_age ?? false,
				account_expiration_date: provider.account_expiration_date ?? "",
			});
			const qb = provider.quota_bytes ?? 0;
			setQuotaEnabled(qb > 0);
			setRetentionEnabled((provider.max_article_age_days ?? 0) > 0);
			setQuotaGbInput(qb > 0 ? String(Math.round((qb / BYTES_PER_GB) * 100) / 100) : "1");
			// For edit mode, allow saving without testing if only non-connection fields change
			setCanSave(true);
		} else {
			setFormData({
				...defaultFormData,
				is_backup_provider: defaultBackup,
				user_agent: defaultUserAgent,
			});
			setQuotaEnabled(false);
			setRetentionEnabled(false);
			setQuotaGbInput("1");
			setCanSave(false);
		}
		setConnectionTestResult(null);
	}, [mode, provider, defaultBackup, defaultUserAgent]);

	const handleInputChange = (field: keyof ProviderFormData, value: string | number | boolean) => {
		setFormData((prev) => ({ ...prev, [field]: value }));

		// Reset connection test if connection-related fields change
		if (
			["host", "port", "username", "password", "tls", "insecure_tls", "proxy_url"].includes(field)
		) {
			setConnectionTestResult(null);
			setCanSave(false);
		}
	};

	const handleTestConnection = async () => {
		if (!formData.host || !formData.username || !formData.password) {
			showToast({
				type: "warning",
				title: "Missing Required Fields",
				message: "Please fill in all required fields before testing connection",
			});
			return;
		}

		setIsTestingConnection(true);
		setConnectionTestResult(null);

		try {
			const result = await testProvider.mutateAsync({
				provider_id: provider?.id,
				host: formData.host,
				port: formData.port,
				username: formData.username,
				password: formData.password,
				tls: formData.tls,
				insecure_tls: formData.insecure_tls,
				proxy_url: formData.proxy_url || undefined,
				skip_ping: formData.skip_ping,
			});

			setConnectionTestResult({
				success: result.success,
				message: result.error_message,
				rttMs: result.rtt_ms,
			});

			setCanSave(result.success);
		} catch (error) {
			setConnectionTestResult({
				success: false,
				message: error instanceof Error ? error.message : "Connection test failed",
			});
			setCanSave(false);
		} finally {
			setIsTestingConnection(false);
		}
	};

	const handleSave = async () => {
		if (mode === "create" && !canSave && !formData.skip_ping) {
			showToast({
				type: "warning",
				title: "Connection Test Required",
				message: "Please test the connection successfully before saving a new provider",
			});
			return;
		}

		try {
			if (mode === "create") {
				await createProvider.mutateAsync({
					...formData,
					proxy_url: formData.proxy_url || undefined,
				});
			} else if (mode === "edit" && provider) {
				// Only send changed fields for update
				const updateData: Partial<ProviderFormData> = {};

				if (formData.host !== provider.host) updateData.host = formData.host;
				if (formData.port !== provider.port) updateData.port = formData.port;
				if (formData.username !== provider.username) updateData.username = formData.username;
				if (formData.password) updateData.password = formData.password; // Only include if not empty
				if (formData.max_connections !== provider.max_connections)
					updateData.max_connections = formData.max_connections;
				if (formData.min_connections_alive !== (provider.min_connections_alive ?? 0))
					updateData.min_connections_alive = formData.min_connections_alive;
				if (formData.inflight_requests !== provider.inflight_requests)
					updateData.inflight_requests = formData.inflight_requests;
				if (formData.stat_inflight_requests !== provider.stat_inflight_requests)
					updateData.stat_inflight_requests = formData.stat_inflight_requests;
				if (formData.tls !== provider.tls) updateData.tls = formData.tls;
				if (formData.insecure_tls !== provider.insecure_tls)
					updateData.insecure_tls = formData.insecure_tls;
				if (formData.proxy_url !== (provider.proxy_url || ""))
					updateData.proxy_url = formData.proxy_url;
				if (formData.enabled !== provider.enabled) updateData.enabled = formData.enabled;
				if (formData.is_backup_provider !== provider.is_backup_provider)
					updateData.is_backup_provider = formData.is_backup_provider;
				if (formData.storage_group !== (provider.storage_group ?? ""))
					updateData.storage_group = formData.storage_group;
				if (formData.skip_ping !== (provider.skip_ping ?? false))
					updateData.skip_ping = formData.skip_ping;
				if (formData.keepalive_interval_seconds !== (provider.keepalive_interval_seconds ?? 0))
					updateData.keepalive_interval_seconds = formData.keepalive_interval_seconds;
				if (formData.keepalive_command !== (provider.keepalive_command ?? ""))
					updateData.keepalive_command = formData.keepalive_command;
				if (formData.user_agent !== (provider.user_agent ?? ""))
					updateData.user_agent = formData.user_agent;
				if (formData.account_expiration_date !== (provider.account_expiration_date ?? ""))
					updateData.account_expiration_date = formData.account_expiration_date;
				if (formData.name !== (provider.name ?? "")) updateData.name = formData.name;
				if (formData.quota_bytes !== (provider.quota_bytes ?? 0))
					updateData.quota_bytes = formData.quota_bytes;
				if (formData.quota_period_hours !== (provider.quota_period_hours ?? 0))
					updateData.quota_period_hours = formData.quota_period_hours;
				if (formData.max_article_age_days !== (provider.max_article_age_days ?? 0))
					updateData.max_article_age_days = formData.max_article_age_days;
				if (formData.strict_max_article_age !== (provider.strict_max_article_age ?? false))
					updateData.strict_max_article_age = formData.strict_max_article_age;

				await updateProvider.mutateAsync({
					id: provider.id,
					data: updateData,
				});
			}

			onSuccess();
		} catch (error) {
			console.error("Failed to save provider:", error);
			showToast({
				type: "error",
				title: "Save Failed",
				message: "Failed to save provider. Please try again.",
			});
		}
	};

	const isFormValid = formData.host && formData.username && (mode === "edit" || formData.password);
	const isSaving = createProvider.isPending || updateProvider.isPending;

	return (
		<div className="modal modal-open backdrop-blur-sm">
			<div className="modal-box w-full min-w-0 max-w-none rounded-none border border-base-300 shadow-2xl sm:max-w-2xl sm:rounded-2xl">
				<h3 className="mb-6 font-black text-xl uppercase tracking-tighter">
					{mode === "create" ? "Add New Provider" : "Edit Provider"}
				</h3>

				<form className="min-w-0 space-y-6" onSubmit={(e) => e.preventDefault()}>
					{/* Identity + connection — two equal halves, uniform spacing */}
					<div className="grid grid-cols-1 gap-x-6 gap-y-6 sm:grid-cols-2">
						<fieldset className="fieldset sm:col-span-2">
							<legend className="fieldset-legend font-bold">Nickname</legend>
							<input
								id="name"
								type="text"
								className="input input-bordered w-full font-mono text-sm"
								value={formData.name}
								onChange={(e) => handleInputChange("name", e.target.value)}
								placeholder="e.g. Main Provider"
							/>
						</fieldset>

						<fieldset className="fieldset">
							<legend className="fieldset-legend font-bold">NNTP Host *</legend>
							<input
								id="host"
								type="text"
								className="input input-bordered w-full font-mono text-sm"
								value={formData.host}
								onChange={(e) => handleInputChange("host", e.target.value)}
								onBlur={handleHostBlur}
								placeholder="news.example.com"
								required
							/>
						</fieldset>

						<fieldset className="fieldset">
							<legend className="fieldset-legend font-bold">Port</legend>
							<input
								id="port"
								type="number"
								className="input input-bordered w-full font-mono text-sm"
								value={formData.port}
								onChange={(e) =>
									handleInputChange("port", Number.parseInt(e.target.value, 10) || 119)
								}
								min={1}
								max={65535}
							/>
						</fieldset>

						<fieldset className="fieldset">
							<legend className="fieldset-legend font-bold">Max Connections</legend>
							<input
								id="max_connections"
								type="number"
								className="input input-bordered w-full font-mono text-sm"
								value={formData.max_connections}
								onChange={(e) =>
									handleInputChange("max_connections", Number.parseInt(e.target.value, 10) || 1)
								}
								min={1}
								max={100}
							/>
						</fieldset>

						<fieldset className="fieldset">
							<legend className="fieldset-legend font-bold">Min Connections Alive</legend>
							<input
								id="min_connections_alive"
								type="number"
								className="input input-bordered w-full font-mono text-sm"
								value={formData.min_connections_alive}
								onChange={(e) =>
									handleInputChange(
										"min_connections_alive",
										Number.parseInt(e.target.value, 10) || 0,
									)
								}
								min={0}
								max={formData.max_connections}
							/>
							<p className="label mt-1 text-base-content/70 text-xs">
								0 = disabled. Keeps this many connections dialed and open at all times, instead of
								connecting cold on first traffic after being idle.
							</p>
						</fieldset>

						<fieldset className="fieldset">
							<legend className="fieldset-legend font-bold">Pipeline (Inflight)</legend>
							<input
								id="inflight_requests"
								type="number"
								className="input input-bordered w-full font-mono text-sm"
								value={formData.inflight_requests}
								onChange={(e) =>
									handleInputChange("inflight_requests", Number.parseInt(e.target.value, 10) || 1)
								}
								min={1}
								max={100}
							/>
						</fieldset>

						<fieldset className="fieldset sm:col-span-2">
							<legend className="fieldset-legend font-bold">Stat Pipeline (Inflight)</legend>
							<input
								id="stat_inflight_requests"
								type="number"
								className="input input-bordered w-full max-w-[10rem] font-mono text-sm"
								value={formData.stat_inflight_requests}
								onChange={(e) =>
									handleInputChange(
										"stat_inflight_requests",
										Number.parseInt(e.target.value, 10) || 1,
									)
								}
								min={1}
								max={1000}
							/>
							<p className="label mt-1 block text-balance break-words text-base-content/70 text-xs">
								Pipeline depth for bodyless STAT commands (existence checks). Since STAT carries no
								payload, this can run much deeper than the BODY pipeline above without inflating
								memory use.
							</p>
						</fieldset>

						<fieldset className="fieldset">
							<legend className="fieldset-legend font-bold">Username *</legend>
							<input
								id="username"
								type="text"
								className="input input-bordered w-full font-mono text-sm"
								value={formData.username}
								onChange={(e) => handleInputChange("username", e.target.value)}
								required
							/>
						</fieldset>

						<fieldset className="fieldset">
							<legend className="fieldset-legend font-bold">
								Password {mode === "create" ? "*" : ""}
							</legend>
							<input
								id="password"
								type="password"
								className="input input-bordered w-full font-mono text-sm"
								value={formData.password}
								onChange={(e) => handleInputChange("password", e.target.value)}
								placeholder={mode === "edit" ? "••••••••••••••••" : ""}
								required={mode === "create"}
							/>
							{mode === "edit" && (
								<p className="label text-base-content/70 text-xs">Leave empty to keep current.</p>
							)}
						</fieldset>
					</div>

					{/* Security Settings */}
					<div className="min-w-0 space-y-4 rounded-2xl border-2 border-base-300/80 bg-base-200/60 p-5">
						<h4 className="font-bold text-base-content/60 text-xs uppercase tracking-widest">
							Options & Security
						</h4>

						<div className="flex min-w-0 flex-col gap-4">
							<label
								htmlFor="tls"
								className="label w-full min-w-0 cursor-pointer items-start justify-start gap-3"
							>
								<input
									id="tls"
									type="checkbox"
									className="checkbox checkbox-primary checkbox-sm mt-0.5 shrink-0"
									checked={formData.tls}
									onChange={(e) => handleInputChange("tls", e.target.checked)}
								/>
								<div className="min-w-0 flex-1">
									<span className="label-text font-bold text-xs">Use SSL/TLS</span>
									<span className="block break-words text-base-content/70 text-xs">
										Highly recommended for privacy.
									</span>
								</div>
							</label>

							{formData.tls && (
								<label
									htmlFor="insecure_tls"
									className="label ml-0 w-full min-w-0 cursor-pointer items-start justify-start gap-3 border-base-300 border-l-2 pl-4 sm:ml-7 sm:border-l-0 sm:pl-0"
								>
									<input
										id="insecure_tls"
										type="checkbox"
										className="checkbox checkbox-warning checkbox-sm mt-0.5 shrink-0"
										checked={formData.insecure_tls}
										onChange={(e) => handleInputChange("insecure_tls", e.target.checked)}
									/>
									<div className="min-w-0 flex-1">
										<span className="label-text font-bold text-xs">
											Insecure (Skip Verification)
										</span>
										<span className="block break-words text-base-content/70 text-xs">
											Only use for self-signed certs.
										</span>
									</div>
								</label>
							)}

							<label
								htmlFor="is_backup_provider"
								className="label w-full min-w-0 cursor-pointer items-start justify-start gap-3"
							>
								<input
									id="is_backup_provider"
									type="checkbox"
									className="checkbox checkbox-primary checkbox-sm mt-0.5 shrink-0"
									checked={formData.is_backup_provider}
									onChange={(e) => handleInputChange("is_backup_provider", e.target.checked)}
								/>
								<div className="min-w-0 flex-1">
									<span className="label-text font-bold text-xs">Backup Only</span>
									<span className="block break-words text-base-content/70 text-xs">
										Only use when primary providers fail.
									</span>
								</div>
							</label>

							<label
								htmlFor="skip_ping"
								className="label w-full min-w-0 cursor-pointer items-start justify-start gap-3"
							>
								<input
									id="skip_ping"
									type="checkbox"
									className="checkbox checkbox-primary checkbox-sm mt-0.5 shrink-0"
									checked={formData.skip_ping}
									onChange={(e) => handleInputChange("skip_ping", e.target.checked)}
								/>
								<div className="min-w-0 flex-1">
									<span className="label-text font-bold text-xs">Skip server ping</span>
									<span className="block text-balance break-words text-base-content/70 text-xs">
										Enable if the server doesn't support the DATE command and you get a date/ping
										error when connecting.
									</span>
								</div>
							</label>
						</div>

						{/* Proxy + Account Expiration */}
						<div className="grid grid-cols-1 gap-4 border-base-300/60 border-t pt-4 sm:grid-cols-2">
							<fieldset className="fieldset">
								<legend className="fieldset-legend font-bold">SOCKS5 Proxy (Optional)</legend>
								<input
									id="proxy_url"
									type="text"
									className="input input-bordered w-full font-mono text-sm"
									value={formData.proxy_url}
									onChange={(e) => handleInputChange("proxy_url", e.target.value)}
									placeholder="socks5://user:pass@host:port"
								/>
							</fieldset>

							<fieldset className="fieldset">
								<legend className="fieldset-legend font-bold">Storage Group (Optional)</legend>
								<input
									id="storage_group"
									type="text"
									className="input input-bordered w-full font-mono text-sm"
									value={formData.storage_group}
									onChange={(e) => {
										handleInputChange("storage_group", e.target.value);
										setBackboneHint(null);
									}}
									placeholder="e.g. omicron"
								/>
								<p className="label mt-1 text-base-content/70 text-xs">
									{backboneHint ??
										"Providers sharing a backbone: give them the same group so a missing-article response skips the rest."}
								</p>
							</fieldset>

							<fieldset className="fieldset">
								<legend className="fieldset-legend font-bold">Account Expiration Date</legend>
								<input
									id="account_expiration_date"
									type="date"
									className="input input-bordered w-full font-mono text-sm"
									value={formData.account_expiration_date}
									onChange={(e) => handleInputChange("account_expiration_date", e.target.value)}
								/>
								<p className="label mt-1 text-base-content/70 text-xs">
									Optional. When this account's subscription ends.
								</p>
							</fieldset>
						</div>
					</div>

					{/* Keep-Alive */}
					<div className="space-y-4 rounded-2xl border-2 border-base-300/80 bg-base-200/60 p-5">
						<h4 className="font-bold text-base-content/60 text-xs uppercase tracking-widest">
							Keep-Alive
						</h4>
						<div className="space-y-4">
							<fieldset className="fieldset">
								<legend className="fieldset-legend font-bold">Interval (seconds)</legend>
								<input
									id="keepalive_interval_seconds"
									type="number"
									className="input input-bordered w-full font-mono text-sm"
									value={formData.keepalive_interval_seconds}
									onChange={(e) =>
										handleInputChange(
											"keepalive_interval_seconds",
											Number.parseInt(e.target.value, 10) || 0,
										)
									}
									min={0}
								/>
								<p className="label mt-1 text-base-content/70 text-xs">
									0 = disabled. Seconds between idle keepalive pings.
								</p>
							</fieldset>

							<fieldset className="fieldset">
								<legend className="fieldset-legend font-bold">Command</legend>
								<input
									id="keepalive_command"
									type="text"
									className="input input-bordered w-full font-mono text-sm"
									value={formData.keepalive_command}
									onChange={(e) => handleInputChange("keepalive_command", e.target.value)}
									placeholder="DATE"
									disabled={formData.keepalive_interval_seconds === 0}
								/>
								<p className="label mt-1 text-base-content/70 text-xs">
									NNTP command for the probe. Defaults to DATE.
								</p>
							</fieldset>
						</div>
					</div>

					{/* Download Quota */}
					<div className="space-y-4 rounded-2xl border-2 border-base-300/80 bg-base-200/60 p-5">
						<label
							htmlFor="quota_enabled"
							className="label w-full min-w-0 cursor-pointer items-start justify-start gap-3"
						>
							<input
								id="quota_enabled"
								type="checkbox"
								className="checkbox checkbox-primary checkbox-sm mt-0.5 shrink-0"
								checked={quotaEnabled}
								onChange={(e) => {
									setQuotaEnabled(e.target.checked);
									if (!e.target.checked) {
										handleInputChange("quota_bytes", 0);
										handleInputChange("quota_period_hours", 0);
									} else {
										const gb = Number.parseFloat(quotaGbInput) || 1;
										setQuotaGbInput(String(gb));
										handleInputChange("quota_bytes", Math.round(gb * BYTES_PER_GB));
									}
								}}
							/>
							<div className="min-w-0 flex-1">
								<span className="label-text font-bold text-xs">Download Quota</span>
								<span className="block break-words text-base-content/70 text-xs">
									Limit how much this provider can download per period.
								</span>
							</div>
						</label>

						{quotaEnabled && (
							<div className="grid grid-cols-1 gap-4 md:grid-cols-2">
								<fieldset className="fieldset">
									<legend className="fieldset-legend font-bold">Quota Limit (GB)</legend>
									<input
										id="quota_bytes"
										type="number"
										className="input input-bordered w-full font-mono text-sm"
										value={quotaGbInput}
										onChange={(e) => {
											setQuotaGbInput(e.target.value);
											const gb = Number.parseFloat(e.target.value);
											if (!Number.isNaN(gb) && gb > 0) {
												handleInputChange("quota_bytes", Math.round(gb * BYTES_PER_GB));
											}
										}}
										onBlur={() => {
											const gb = Number.parseFloat(quotaGbInput);
											if (Number.isNaN(gb) || gb <= 0) {
												setQuotaGbInput("1");
												handleInputChange("quota_bytes", BYTES_PER_GB);
											}
										}}
										min={0.01}
										step={1}
									/>
								</fieldset>

								<fieldset className="fieldset">
									<legend className="fieldset-legend font-bold">Reset Period</legend>
									<select
										id="quota_period_hours"
										className="select select-bordered w-full font-mono text-sm"
										value={formData.quota_period_hours}
										onChange={(e) =>
											handleInputChange("quota_period_hours", Number.parseInt(e.target.value, 10))
										}
									>
										<option value={0}>Lifetime (never resets)</option>
										<option value={24}>Daily (24h)</option>
										<option value={168}>Weekly (7d)</option>
										<option value={720}>Monthly (30d)</option>
									</select>
								</fieldset>
							</div>
						)}
					</div>

					{/* Article Retention */}
					<div className="space-y-4 rounded-2xl border-2 border-base-300/80 bg-base-200/60 p-5">
						<label
							htmlFor="retention_enabled"
							className="label w-full min-w-0 cursor-pointer items-start justify-start gap-3"
						>
							<input
								id="retention_enabled"
								type="checkbox"
								className="checkbox checkbox-primary checkbox-sm mt-0.5 shrink-0"
								checked={retentionEnabled}
								onChange={(e) => {
									setRetentionEnabled(e.target.checked);
									if (e.target.checked) {
										handleInputChange("max_article_age_days", 1000);
									} else {
										handleInputChange("max_article_age_days", 0);
										handleInputChange("strict_max_article_age", false);
									}
								}}
							/>
							<div className="min-w-0 flex-1">
								<span className="label-text font-bold text-xs">Limited Retention</span>
								<span className="block break-words text-base-content/70 text-xs">
									Set this when the provider only keeps recent posts. Newer articles are fetched
									from it first, leaving providers with deeper retention free for older ones.
								</span>
							</div>
						</label>

						{retentionEnabled && (
							<div className="grid grid-cols-1 gap-4 md:grid-cols-2">
								<fieldset className="fieldset">
									<legend className="fieldset-legend font-bold">Retention (days)</legend>
									<input
										id="max_article_age_days"
										type="number"
										className="input input-bordered w-full font-mono text-sm"
										value={formData.max_article_age_days}
										onChange={(e) =>
											handleInputChange(
												"max_article_age_days",
												Number.parseInt(e.target.value, 10) || 0,
											)
										}
										min={1}
										step={1}
									/>
									<p className="label">How far back this provider's retention reaches.</p>
								</fieldset>

								<fieldset className="fieldset">
									<legend className="fieldset-legend font-bold">Older Articles</legend>
									<select
										id="strict_max_article_age"
										className="select select-bordered w-full font-mono text-sm"
										value={formData.strict_max_article_age ? "skip" : "last"}
										onChange={(e) =>
											handleInputChange("strict_max_article_age", e.target.value === "skip")
										}
									>
										<option value="last">Try this provider last</option>
										<option value="skip">Never ask this provider</option>
									</select>
									<p className="label">
										"Try last" still reaches the provider if others miss, so a wrong date cannot
										make a file unplayable.
									</p>
								</fieldset>
							</div>
						)}
					</div>

					{/* Connection Test */}
					<div className="min-w-0 space-y-4 border-base-300/50 border-t pt-4">
						<div className="flex min-w-0 flex-wrap items-center justify-between gap-2">
							<h4 className="min-w-0 font-bold text-base-content/60 text-xs uppercase tracking-widest">
								Connectivity Check
							</h4>
							<button
								type="button"
								className="btn btn-sm btn-outline px-4"
								onClick={handleTestConnection}
								disabled={!isFormValid || isTestingConnection}
							>
								{isTestingConnection ? (
									<Loader className="h-3 w-3 animate-spin" />
								) : (
									<Wifi className="h-3 w-3" />
								)}
								Test Link
							</button>
						</div>

						{connectionTestResult && (
							<div
								className={`alert rounded-xl py-2 text-xs ${
									connectionTestResult.success
										? "alert-success border-success/20 bg-success/10 text-success"
										: "alert-error border-error/20 bg-error/10 text-error"
								}`}
							>
								{connectionTestResult.success ? (
									<Check className="h-4 w-4" />
								) : (
									<AlertTriangle className="h-4 w-4" />
								)}
								<div className="min-w-0">
									<div className="font-black text-xs uppercase tracking-widest">
										{connectionTestResult.success
											? `Success${connectionTestResult.rttMs !== undefined ? ` • ${connectionTestResult.rttMs}ms` : ""}`
											: "Failed"}
									</div>
									{connectionTestResult.message && (
										<div className="mt-0.5 break-words font-medium">
											{connectionTestResult.message}
										</div>
									)}
								</div>
							</div>
						)}
					</div>
				</form>

				<div className="modal-action gap-3">
					<button type="button" className="btn btn-ghost" onClick={onCancel}>
						Cancel
					</button>
					<button
						type="button"
						className="btn btn-primary px-8 shadow-lg shadow-primary/20"
						onClick={handleSave}
						disabled={isSaving || (mode === "create" && !canSave && !formData.skip_ping)}
					>
						{isSaving ? <Loader className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}
						{mode === "create" ? "Create Provider" : "Save Changes"}
					</button>
				</div>
			</div>
		</div>
	);
}
