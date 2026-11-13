import type { PeernetSharingConfig } from "../../../types/peernet";
import { TagListEditor } from "./TagListEditor";

interface SharingExclusionsCardProps {
	sharing: PeernetSharingConfig;
	onChange: (sharing: PeernetSharingConfig) => void;
	disabled?: boolean;
}

export function SharingExclusionsCard({
	sharing,
	onChange,
	disabled = false,
}: SharingExclusionsCardProps) {
	return (
		<div className="card bg-base-100 shadow-lg">
			<div className="card-body space-y-2">
				<h3 className="card-title text-lg">Sharing exclusions</h3>
				<p className="text-base-content/70 text-sm">
					A release in an excluded category or from an excluded indexer is never published, on any
					network. While any indexer is excluded, releases whose indexer is unknown are also
					withheld. Matching ignores case.
				</p>
				<TagListEditor
					label="Excluded categories"
					placeholder="Category name"
					tags={sharing.excluded_categories}
					disabled={disabled}
					onChange={(excluded_categories) => onChange({ ...sharing, excluded_categories })}
				/>
				<TagListEditor
					label="Excluded indexers"
					placeholder="Indexer name"
					tags={sharing.excluded_indexers}
					disabled={disabled}
					onChange={(excluded_indexers) => onChange({ ...sharing, excluded_indexers })}
				/>
			</div>
		</div>
	);
}
