# Sonarr v5 and season-pack repair

Status: approved by the user and implemented on `feat/sonarr-v5-support`.

## Goal

Use Sonarr's native v5 API when available, preserve v3 for Sonarr v4 and explicit compatibility use, and repair failed packs without discarding healthy episodes or issuing duplicate actions.

## Behavior

- Each Sonarr instance independently selects an API. Existing configurations default to `auto`: v5 for a confirmed Sonarr v5 instance, v3 for Sonarr v4. Optional `api_version: v3` and `api_version: v5` overrides support rollback and explicit deployments. An unsupported forced version reports a connection error.
- Discover via the unversioned `/api` resource and validate the selected version's system status. If discovery is absent or unusable, probe read-only versioned status endpoints. Treat authentication errors, timeouts, and server errors as failures rather than evidence that the instance needs an older API. Never retry a mutation using another API version.
- Respect URL bases, API keys, existing proxy/timeout settings and request cancellation. Cache selection per effective instance configuration; invalidate on URL/key/version changes. Recheck after a bounded cache lifetime and after a confirmed unsupported endpoint on a read, allowing an upgraded instance to move to v5 without restarting AltMount. In-flight operations retain one selected adapter.
- Both API adapters return download groups containing episode targets. V5 supplies grouped `episodeIds`; v3 records are grouped by instance and normalized download ID across every queue page. Never combine different instances or conflicting series/download-client identities. For absent download IDs, retain independent queue records rather than guessing from titles or paths.
- A group may contain multiple queue IDs, episodes, seasons, and episode files. Episode IDs and file IDs are deduplicated. Explicit pack metadata is optional: unknown is distinct from false. Do not infer a full season from a release title.
- Remove or blocklist a failed download once per group, while counting failures and applying monitoring decisions per affected episode. Preserve the existing configured cleanup rules, grace period, disabled-breaker behavior, and ownership restriction to AltMount downloads.
- For a partially imported pack, refresh episode/file state before acting. Keep healthy imported episodes and their files. Only unresolved episodes consume failure counts or become search targets. An imported file known to be unhealthy remains a repair target; `hasFile` alone does not prove health. Aggregate `episodesWithFilesCount` is a hint, never evidence identifying which episodes are healthy.
- Wait for a matching Sonarr automatic replacement search as today. Then evaluate every target: healthy, covered by an active replacement download, unresolved, or unknown. A replacement covering one episode must not suppress repair of its unresolved siblings. Unknown state defers destructive action until a later bounded retry, without incrementing breakers.
- Match automatic commands by their actual target: episode intersection for `EpisodeSearch`, and both series and season for `SeasonSearch`. Wait for all relevant active commands within the configured budget. A search for another season must not suppress this repair.
- Search the remaining monitored, unresolved episodes in one `EpisodeSearch` command per repair operation. Do not introduce unconditional `SeasonSearch`: it can reacquire episodes already satisfied. A future whole-season policy can be added independently.
- If only some episodes hit the failure limit, unmonitor those episodes before a replacement can be triggered. Use blocklist-without-search for that group, then explicitly search only eligible siblings. If the required unmonitor fails, stop that replacement sequence and retry later. Preserve files for healthy siblings.
- Revalidate original unhealthy file IDs immediately before deletion; never delete a newly linked replacement. Treat an already absent original file or cascade-removed sibling queue record as state to reconcile, not a reason to skip remaining episode monitoring decisions.
- Coalesce concurrent repair attempts within an instance/download. Repeated observations are not new failures. Re-read state after ambiguous mutation outcomes instead of replaying deletion, blocklisting, or searches blindly. No new persistent recovery store is proposed; restart behavior remains bounded by existing in-memory trackers and refreshed Sonarr state.
- Whisparr continues using its existing v3 adapter; Radarr/Lidarr/Readarr/Sportarr keep their existing behavior.

Example: a ten-episode pack imported eight healthy episodes, episode 9 has an active replacement, and episode 10 is missing. Preserve episodes 1–8, avoid another search for episode 9, and search episode 10 once. If episode 10 has reached the configured failure limit, unmonitor it and issue no search for it.

## Architecture

Introduce an AltMount-owned Sonarr interface with separate v3 and v5 implementations. Initially reuse starr's existing DTOs for unchanged library/registration operations at the adapter boundary, but own the queue-group, removal-options, and repair-state models. Business logic must not rely on starr's singular `EpisodeID` queue shape or concrete `*sonarr.Sonarr` assertions.

Use the existing starr client inside the v3 adapter. Implement explicit v5 request/response mapping for the operations AltMount uses; a transport-level `/v3` to `/v5` string replacement is insufficient. The native queue deletion contract uses `removalMethod` and `blocklistMethod` enums instead of the v3 boolean options. Keep per-adapter queue identities separate.

## Scope and validation

Migrate library discovery, instance detection/testing, health reads, webhook and download-client registration/update, queue/history reads, monitoring, episode-file deletion, command submission and command polling. No new search ranking, episode ordering UI, SABnzbd protocol changes, or download-engine behavior.

Verify equivalent outcomes with v3 and v5 fixtures, plus opt-in smoke tests against disposable instances of actual Sonarr v4 and v5. A live v5 smoke test is required before claiming runtime validation; fixture-only evidence must remain labeled as such.

## Sources

- [Official v5 API schema](https://github.com/Sonarr/Sonarr/blob/a6171f0ca61b2a76ec529da3e9aadb11435344f9/src/Sonarr.Api.V5/openapi.json)
- [Native grouped queue resource](https://github.com/Sonarr/Sonarr/blob/a6171f0ca61b2a76ec529da3e9aadb11435344f9/src/Sonarr.Api.V5/Queue/QueueResource.cs)
- [Native queue deletion and retrieval](https://github.com/Sonarr/Sonarr/blob/a6171f0ca61b2a76ec529da3e9aadb11435344f9/src/Sonarr.Api.V5/Queue/QueueController.cs)
- [Retained v3 queue compatibility controller](https://github.com/Sonarr/Sonarr/blob/a6171f0ca61b2a76ec529da3e9aadb11435344f9/src/Sonarr.Api.V3/Queue/QueueController.cs)

Implementation must pin source revisions when recording fixtures because the v5 API is evolving.
