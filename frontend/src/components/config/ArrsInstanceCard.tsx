import { Activity, Eye, EyeOff, Globe, Key, Tag, Trash2 } from "lucide-react";
import { useCallback, useState } from "react";
import type { ArrsInstanceConfig, ArrsType, SABnzbdCategory } from "../../types/config";
import { LoadingSpinner } from "../ui/LoadingSpinner";

interface ArrsInstanceCardProps {
	instance: ArrsInstanceConfig;
	type: ArrsType;
	index: number;
	isReadOnly: boolean;
	isApiKeyVisible: boolean;
	categories?: SABnzbdCategory[];
	onToggleApiKey: () => void;
	onRemove: () => void;
	onInstanceChange: (
		field: keyof ArrsInstanceConfig,
		value: ArrsInstanceConfig[keyof ArrsInstanceConfig],
	) => void;
}

export function ArrsInstanceCard({
	instance,
	type,
	index,
	isReadOnly,
	isApiKeyVisible,
	categories = [],
	onToggleApiKey,
	onRemove,
	onInstanceChange,
}: ArrsInstanceCardProps) {
	const instanceKey = `${type}-${index}`;
	const [testResult, setTestResult] = useState<{
		type: "success" | "error" | null;
		message: string;
	}>({ type: null, message: "" });
	const [isTestingConnection, setIsTestingConnection] = useState(false);

	const testConnection = useCallback(async () => {
		if (!instance.url) {
			setTestResult({ type: "error", message: "Required info missing" });
			return;
		}
		setIsTestingConnection(true);
		setTestResult({ type: null, message: "" });
		try {
			const response = await fetch("/api/arrs/instances/test", {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify({
					type,
					name: instance.name,
					url: instance.url,
					api_key: instance.api_key,
					...(type === "sonarr" ? { api_version: instance.api_version || "auto" } : {}),
				}),
			});
			const data = await response.json();
			if (data.success) {
				setTestResult({ type: "success", message: data.message || "Link Active" });
			} else {
				setTestResult({ type: "error", message: data.error || data.message || "Link Failed" });
			}
		} catch (_error) {
			setTestResult({ type: "error", message: "Network Error" });
		} finally {
			setIsTestingConnection(false);
			setTimeout(() => {
				setTestResult({ type: null, message: "" });
			}, 5000);
		}
	}, [instance.name, instance.url, instance.api_key, instance.api_version, type]);

	const handleInstanceChange = useCallback(
		(field: keyof ArrsInstanceConfig, value: ArrsInstanceConfig[keyof ArrsInstanceConfig]) => {
			if (field === "url" || field === "api_key" || field === "api_version")
				setTestResult({ type: null, message: "" });
			onInstanceChange(field, value);
		},
		[onInstanceChange],
	);

	// Mirror ARR_TYPES in ArrsConfigSection: distinct accents per type.
	const getBorderColor = () => {
		switch (type) {
			case "radarr":
				return "bg-warning";
			case "sonarr":
				return "bg-info";
			case "lidarr":
				return "bg-success";
			case "readarr":
				return "bg-error";
			case "whisparr":
				return "bg-secondary";
			case "sportarr":
				return "bg-accent";
			default:
				return "bg-base-300";
		}
	};

	return (
		<div
			key={instanceKey}
			className="group relative overflow-hidden rounded-2xl border border-base-300 bg-base-100/50 transition-all hover:shadow-md"
		>
			<div className={`absolute top-0 bottom-0 left-0 w-1.5 ${getBorderColor()}`} />

			<div className="space-y-6 p-5 pl-7">
				{/* Header */}
				<div className="flex items-center justify-between gap-4">
					<div className="min-w-0">
						<div className="flex items-center gap-2">
							<span className="font-black text-base-content/50 text-xs uppercase tracking-tighter">
								{type}
							</span>
							<h4 className="break-all font-bold text-base tracking-tight">
								{instance.name || "Unnamed Instance"}
							</h4>
						</div>
					</div>
					<button
						type="button"
						className="btn btn-ghost btn-sm text-error opacity-0 transition-opacity group-hover:opacity-100"
						onClick={onRemove}
						disabled={isReadOnly}
					>
						<Trash2 className="h-3.5 w-3.5" />
					</button>
				</div>

				<div className="grid grid-cols-1 gap-6 sm:grid-cols-2">
					<fieldset className="fieldset">
						<legend className="fieldset-legend flex items-center gap-2 font-semibold">
							<Globe className="h-3 w-3 text-base-content/60" /> URL
						</legend>
						<input
							type="url"
							className="input input-sm input-bordered w-full bg-base-100 font-mono text-xs"
							value={instance.url}
							onChange={(e) => handleInstanceChange("url", e.target.value)}
							placeholder="http://localhost:7878"
							disabled={isReadOnly}
						/>
					</fieldset>

					<fieldset className="fieldset">
						<legend className="fieldset-legend flex items-center gap-2 font-semibold">
							<Key className="h-3 w-3 text-base-content/60" /> API Key
						</legend>
						<div className="join w-full shadow-sm">
							<input
								type={isApiKeyVisible ? "text" : "password"}
								className="input input-sm input-bordered join-item flex-1 bg-base-100 font-mono text-xs"
								value={instance.api_key}
								onChange={(e) => handleInstanceChange("api_key", e.target.value)}
								placeholder="Paste key..."
								disabled={isReadOnly}
							/>
							<button
								type="button"
								className="btn btn-sm btn-ghost join-item border-base-300 px-2"
								onClick={onToggleApiKey}
								disabled={isReadOnly}
							>
								{isApiKeyVisible ? (
									<EyeOff className="h-3.5 w-3.5 text-base-content/70" />
								) : (
									<Eye className="h-3.5 w-3.5 text-base-content/70" />
								)}
							</button>
						</div>
					</fieldset>

					<fieldset className="fieldset">
						<legend className="fieldset-legend flex items-center gap-2 font-semibold">
							<Tag className="h-3 w-3 text-base-content/60" /> Category
						</legend>
						<select
							className="select select-sm select-bordered w-full bg-base-100 font-bold text-xs"
							value={instance.category || ""}
							onChange={(e) => handleInstanceChange("category", e.target.value)}
							disabled={isReadOnly}
						>
							<option value="">(Automatic)</option>
							{categories.map((cat) => (
								<option key={cat.name} value={cat.name}>
									{cat.name}
								</option>
							))}
						</select>
					</fieldset>

					{type === "sonarr" && (
						<fieldset className="fieldset">
							<legend className="fieldset-legend font-semibold">Sonarr API</legend>
							<select
								className="select select-sm select-bordered w-full bg-base-100 text-xs"
								value={instance.api_version || "auto"}
								onChange={(e) =>
									handleInstanceChange(
										"api_version",
										e.target.value as ArrsInstanceConfig["api_version"],
									)
								}
								disabled={isReadOnly}
							>
								<option value="auto">Auto (default)</option>
								<option value="v3">Legacy v3</option>
								<option value="v5">Native v5</option>
							</select>
						</fieldset>
					)}
					<div className="flex flex-col justify-center pt-2">
						<label className="label cursor-pointer justify-start gap-3">
							<input
								type="checkbox"
								className="checkbox checkbox-sm checkbox-primary"
								checked={instance.enabled}
								onChange={(e) => handleInstanceChange("enabled", e.target.checked)}
								disabled={isReadOnly}
							/>
							<span className="label-text font-bold text-xs uppercase tracking-wider">Active</span>
						</label>
					</div>
				</div>

				{/* Quick Actions & Status */}
				<div className="flex items-center justify-between gap-4 border-base-300/50 border-t pt-4">
					<div className="flex-1">
						{testResult.type && (
							<div
								className={`fade-in slide-in-from-left-2 flex animate-in items-center gap-2 font-black text-xs uppercase tracking-widest ${testResult.type === "success" ? "text-success" : "text-error"}`}
							>
								<Activity className="h-3 w-3" /> {testResult.message}
							</div>
						)}
					</div>
					<button
						type="button"
						className={`btn btn-sm px-4 shadow-sm ${
							isTestingConnection
								? "btn-disabled"
								: testResult.type === "success"
									? "btn-success"
									: testResult.type === "error"
										? "btn-error"
										: "btn-outline border-base-300"
						}`}
						onClick={testConnection}
						disabled={isReadOnly || isTestingConnection || !instance.url}
					>
						{isTestingConnection ? <LoadingSpinner size="sm" /> : "Test Link"}
					</button>
				</div>
			</div>
		</div>
	);
}
