interface PlainTextProps {
	value: string;
	maxWidthClass?: string;
	mono?: boolean;
}

// Renders text received from other machines. React escapes it; it is never
// used as a link target, class name or style. Long values are cut visually and
// the full text stays available in the tooltip.
export function PlainText({ value, maxWidthClass = "max-w-48", mono = false }: PlainTextProps) {
	return (
		<span
			className={`inline-block truncate align-bottom ${maxWidthClass} ${mono ? "font-mono text-xs" : ""}`}
			title={value}
		>
			{value === "" ? "-" : value}
		</span>
	);
}
