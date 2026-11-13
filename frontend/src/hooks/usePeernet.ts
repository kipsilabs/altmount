import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { apiClient } from "../api/client";
import type {
	PeernetCreateInviteRequest,
	PeernetCreateNetworkRequest,
	PeernetInvite,
	PeernetJoinNetworkRequest,
	PeernetMember,
	PeernetPeer,
	PeernetStatus,
	PeernetUpdateNetworkRequest,
} from "../types/peernet";

const STATUS_REFETCH_MS = 10_000;
const LIST_REFETCH_MS = 15_000;

export const peernetKeys = {
	all: ["peernet"] as const,
	status: () => [...peernetKeys.all, "status"] as const,
	peers: (networkId: string) => [...peernetKeys.all, "peers", networkId] as const,
	invites: (networkId: string) => [...peernetKeys.all, "invites", networkId] as const,
	members: (networkId: string) => [...peernetKeys.all, "members", networkId] as const,
};

export function usePeernetStatus(enabled: boolean) {
	return useQuery<PeernetStatus>({
		queryKey: peernetKeys.status(),
		queryFn: () => apiClient.getPeernetStatus(),
		enabled,
		refetchInterval: STATUS_REFETCH_MS,
		retry: 1,
	});
}

export function usePeernetPeers(networkId: string, enabled = true) {
	return useQuery<PeernetPeer[]>({
		queryKey: peernetKeys.peers(networkId),
		queryFn: () => apiClient.getPeernetPeers(networkId),
		enabled,
		refetchInterval: LIST_REFETCH_MS,
		retry: 1,
	});
}

export function usePeernetInvites(networkId: string, enabled = true) {
	return useQuery<PeernetInvite[]>({
		queryKey: peernetKeys.invites(networkId),
		queryFn: () => apiClient.getPeernetInvites(networkId),
		enabled,
		retry: 1,
	});
}

export function usePeernetMembers(networkId: string, enabled = true) {
	return useQuery<PeernetMember[]>({
		queryKey: peernetKeys.members(networkId),
		queryFn: () => apiClient.getPeernetMembers(networkId),
		enabled,
		retry: 1,
	});
}

export function useConfirmPeernetPeer(networkId: string) {
	const queryClient = useQueryClient();
	return useMutation({
		mutationFn: (address: string) => apiClient.confirmPeernetPeer(networkId, address),
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: peernetKeys.peers(networkId) });
			queryClient.invalidateQueries({ queryKey: peernetKeys.status() });
		},
	});
}

export function useCreatePeernetNetwork() {
	const queryClient = useQueryClient();
	return useMutation({
		mutationFn: (body: PeernetCreateNetworkRequest) => apiClient.createPeernetNetwork(body),
		onSuccess: () => queryClient.invalidateQueries({ queryKey: peernetKeys.status() }),
	});
}

export function useJoinPeernetNetwork() {
	const queryClient = useQueryClient();
	return useMutation({
		mutationFn: (body: PeernetJoinNetworkRequest) => apiClient.joinPeernetNetwork(body),
		// The pasted invite link is a secret: do not keep it in the mutation cache.
		gcTime: 0,
		onSuccess: () => queryClient.invalidateQueries({ queryKey: peernetKeys.status() }),
	});
}

export function useUpdatePeernetNetwork(networkId: string) {
	const queryClient = useQueryClient();
	return useMutation({
		mutationFn: (body: PeernetUpdateNetworkRequest) =>
			apiClient.updatePeernetNetwork(networkId, body),
		onSuccess: () => queryClient.invalidateQueries({ queryKey: peernetKeys.status() }),
	});
}

export function useDeletePeernetNetwork() {
	const queryClient = useQueryClient();
	return useMutation({
		mutationFn: (networkId: string) => apiClient.deletePeernetNetwork(networkId),
		// Refresh the status first so the network's panels unmount, then drop their
		// per-network queries so they are never refetched for a network that is gone.
		onSuccess: async (_data, networkId) => {
			await queryClient.invalidateQueries({ queryKey: peernetKeys.status() });
			queryClient.removeQueries({
				predicate: (query) =>
					query.queryKey[0] === peernetKeys.all[0] &&
					query.queryKey[1] !== "status" &&
					query.queryKey[2] === networkId,
			});
		},
	});
}

// The invite link is a one-time secret. The mutation is not given a cache key
// and the caller must hold the returned link only in transient component state.
export function useCreatePeernetInvite(networkId: string) {
	const queryClient = useQueryClient();
	return useMutation({
		mutationFn: (body: PeernetCreateInviteRequest) =>
			apiClient.createPeernetInvite(networkId, body),
		gcTime: 0,
		onSuccess: () => queryClient.invalidateQueries({ queryKey: peernetKeys.invites(networkId) }),
	});
}

export function useDeletePeernetInvite(networkId: string) {
	const queryClient = useQueryClient();
	return useMutation({
		mutationFn: (inviteId: string) => apiClient.deletePeernetInvite(networkId, inviteId),
		onSuccess: () => queryClient.invalidateQueries({ queryKey: peernetKeys.invites(networkId) }),
	});
}

export function useDeletePeernetMember(networkId: string) {
	const queryClient = useQueryClient();
	return useMutation({
		mutationFn: (nodeId: string) => apiClient.deletePeernetMember(networkId, nodeId),
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: peernetKeys.members(networkId) });
			queryClient.invalidateQueries({ queryKey: peernetKeys.status() });
		},
	});
}
