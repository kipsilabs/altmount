import type { PeernetStatus } from "../../../types/peernet";

/**
 * - unsaved: the feature is not enabled in the saved settings yet
 * - loading: enabled, first status response not received
 * - unavailable: enabled, status could not be loaded
 * - stopped: enabled, but the node did not start (no node ID)
 * - running: the node is up
 */
export type NodeState = "unsaved" | "loading" | "unavailable" | "stopped" | "running";

export function deriveNodeState(
	savedEnabled: boolean,
	status: PeernetStatus | undefined,
	isLoading: boolean,
): NodeState {
	if (!savedEnabled) return "unsaved";
	if (status) return status.node_id === "" ? "stopped" : "running";
	return isLoading ? "loading" : "unavailable";
}
