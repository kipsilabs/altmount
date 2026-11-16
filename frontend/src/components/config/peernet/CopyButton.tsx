import { Check, Copy } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { copyToClipboard } from "../../../lib/utils";

interface CopyButtonProps {
	value: string;
	label: string;
	withText?: boolean;
}

export function CopyButton({ value, label, withText = false }: CopyButtonProps) {
	const [copied, setCopied] = useState(false);
	const timer = useRef<ReturnType<typeof setTimeout> | null>(null);

	useEffect(
		() => () => {
			if (timer.current) clearTimeout(timer.current);
		},
		[],
	);

	const handleCopy = async () => {
		const ok = await copyToClipboard(value);
		if (ok) {
			setCopied(true);
			if (timer.current) clearTimeout(timer.current);
			timer.current = setTimeout(() => setCopied(false), 2000);
		}
	};

	return (
		<button
			type="button"
			className="btn btn-ghost btn-xs"
			onClick={handleCopy}
			aria-label={copied ? "Copied" : label}
			title={copied ? "Copied" : label}
		>
			{copied ? (
				<Check className="h-4 w-4 text-success" aria-hidden="true" />
			) : (
				<Copy className="h-4 w-4" aria-hidden="true" />
			)}
			{withText && (copied ? "Copied" : "Copy")}
		</button>
	);
}
