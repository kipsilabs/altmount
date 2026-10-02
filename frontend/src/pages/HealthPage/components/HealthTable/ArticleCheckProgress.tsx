import type { HealthCheckProgress } from "../../../../types/api";

interface ArticleCheckProgressProps {
	progress: HealthCheckProgress;
}

export function ArticleCheckProgress({ progress }: ArticleCheckProgressProps) {
	const { total_articles: total, articles_to_check: planned, articles_checked: checked } = progress;
	const percent = planned > 0 ? Math.min(100, Math.round((checked / planned) * 100)) : 0;
	return (
		<div className="mt-1 min-w-40 text-base-content/70 text-xs">
			<div>{total.toLocaleString()} articles</div>
			<div>
				{checked.toLocaleString()} / {planned.toLocaleString()} checked ({percent}%)
			</div>
			{planned > 0 && (
				<progress
					className="progress progress-warning h-1 w-full"
					value={checked}
					max={planned}
					aria-label="Article check progress"
				/>
			)}
		</div>
	);
}
