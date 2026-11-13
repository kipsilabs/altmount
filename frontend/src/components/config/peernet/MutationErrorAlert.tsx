import { AlertCircle } from "lucide-react";

interface MutationErrorAlertProps {
	error: Error | null;
}

export function MutationErrorAlert({ error }: MutationErrorAlertProps) {
	if (!error) return null;
	const details = "details" in error && typeof error.details === "string" ? error.details : "";
	return (
		<div className="alert alert-error alert-soft text-sm" role="alert">
			<AlertCircle className="h-5 w-5 shrink-0" aria-hidden="true" />
			<div className="min-w-0 break-words">
				<div>{error.message}</div>
				{details && details !== error.message && <div className="opacity-70">{details}</div>}
			</div>
		</div>
	);
}
