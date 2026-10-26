# Sonarr v5 and season-pack repair implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task by task. Steps use checkbox syntax for tracking.

**Goal:** Support native Sonarr v5 and legacy v3 instances together, with download-level pack actions and episode-level repair decisions.

**Architecture:** Put API differences behind an AltMount-owned Sonarr client interface. Normalize both queues into download groups, then use shared pack planning and execution in the scanner and worker. Preserve other ARR integrations and existing policies.

**Tech stack:** Go, existing HTTP/proxy infrastructure, starr for the legacy adapter, React/TypeScript for a Sonarr-only API override.

**Spec:** `docs/superpowers/specs/2026-10-26-sonarr-v5-season-packs-design.md`

**Status:** approved and implemented for PR #1033. Fixture regression coverage, live read-only v4/v5 smoke checks, affected backend tests, frontend type checking and lint completed. Live populated-library mutations remain untested and are explicitly labeled below.

## Global constraints

- Sonarr v4 remains on `/api/v3`; native Sonarr v5 uses `/api/v5`; instances can coexist.
- Empty `api_version` means `auto`; allowed explicit values are `auto`, `v3`, `v5`.
- Never fall back to another API after a mutation, authentication failure, timeout, or server error.
- Preserve healthy files, per-episode failure thresholds, current cleanup policy, and AltMount-only ownership.
- Keep Whisparr on the existing v3 path and preserve all other ARR behavior.
- Do not add unconditional season searches, a new database, or a new persistence layer.
- Use Conventional Commits for every published commit and PR title; branch names use `<type>/<description>`.

## Review focus

1. V3 sibling records split across pages must become one download without losing episode targets.
2. A replacement for one pack episode must not suppress searches for unresolved siblings.
3. Repeated webhook/cleanup/health observations must not multiply counts, queue deletes or searches.
4. Mixed imported, corrupt, missing, unmonitored and unknown episodes must receive distinct decisions.
5. Changed API credentials, URL bases, upgrades and ambiguous mutation failures must not replay writes or reuse stale identities.

## Task 1: Introduce download groups and a legacy Sonarr adapter

**Files:** Create `internal/arrs/clients/sonarr.go`, `sonarr_v3.go`, `sonarr_v3_test.go`; create `internal/arrs/model/sonarr.go`; modify `clients/manager.go`, `scanner/{manager,redownload,discovery,breaker}.go`, `data/manager.go`, `registrar/manager.go`, `worker/{stuck_cleanup,import_failure}.go`, `service.go`, and affected tests.

**Interfaces:** Define `clients.SonarrClient` for the Sonarr operations currently used by those consumers. Include named methods for health and commands instead of leaking `GetInto`/`PostInto`. Retain existing starr return DTOs for unchanged operations during this migration. Add `QueueGroups(ctx context.Context) ([]model.SonarrDownloadGroup, error)` and `RemoveDownload(ctx context.Context, group model.SonarrDownloadGroup, opts model.SonarrRemovalOptions) error`.

`SonarrDownloadGroup` carries instance-local download identity, source queue IDs, series ID, deduplicated episode IDs, download-client identity, status/messages, output path, and optional season/full-season/import-count hints. Removal options explicitly represent remove-from-client, change-category, ignore, blocklist-only and blocklist-and-search semantics. Reject invalid combinations before writing.

- [x] Write failing normalization tests: multi-page siblings, multi-episode files, unknown download IDs, conflicting series/client identities, mixed instance isolation, queue removal mapping, and preserved legacy registration/repair behavior.
- [x] Run targeted tests and verify the missing abstraction/group behavior fails.
- [x] Implement the v3 adapter and group aggregation after pagination completes; migrate concrete Sonarr assertions/call sites. Wrap Whisparr with fixed legacy behavior wherever shared helpers require the interface.
- [x] Pass `go test ./internal/arrs/...`; commit `refactor(sonarr): introduce version-neutral client and download groups`.

**Deliverable:** Existing v3 behavior works through the new boundary; no native-v5 selection yet.

## Task 2: Implement and select the native v5 adapter

**Files:** Create `clients/sonarr_v5.go`, `sonarr_v5_test.go`, `sonarr_version.go`, `sonarr_version_test.go`, `clients/testdata/sonarr/`; modify `clients/manager.go`, `instances/manager.go`, relevant tests, `internal/config/manager.go`, `internal/arrs/model/types.go`, `frontend/src/types/config.ts`, `frontend/src/components/config/ArrsConfigSection.tsx`, and API config conversions where needed.

**Interfaces:** Add `APIVersion() string` to the selected client. Add `api_version` to instance configuration and carry it into `model.ConfigInstance`; validate and display it only for Sonarr. Empty means auto. Cache clients by effective configuration and selected API, with synchronized bounded capability refresh. Keep network discovery outside the manager's global lock.

- [x] Capture small fixtures with pinned upstream source revisions for every used native endpoint. Verify actual differences in episode/history filters, subresource parameters, notifications, download clients, and command bodies before implementing mappings.
- [x] Write failing tests for auto-v4, auto-v5, forced-v3 on v5, forced-v5 on v4, mixed instances, missing/HTML discovery, unsupported status endpoints, 401/403, 5xx, timeout/cancellation, URL bases, configured proxy/client reuse, configuration invalidation and upgrade refresh.
- [x] Write native contract tests for library/episode/file/history reads, grouped queue pagination, health/status, notification/download-client create/update/test, monitoring, file deletion, command submission/list/polling and every removal action.
- [x] Implement read-only capability detection and explicit v5 request mapping. Map legacy blocklist/remove intentions to the official `removalMethod` and `blocklistMethod` enums. Never reuse v3 queue IDs after a version switch. Surface selection in connection-test feedback/logs without exposing keys.
- [x] Add the Sonarr-only selector: Auto (default), Legacy v3, Native v5. Test old config loading and config round-tripping; run frontend checks and ARR tests.
- [x] Commit `feat(sonarr): select native v5 with legacy v3 compatibility`.

**Deliverable:** All existing Sonarr workflows work through either real API contract; v5 is selected automatically when confirmed.

## Task 3: Plan partial-pack repair per episode

**Files:** Create `scanner/sonarr_pack.go`, `scanner/sonarr_pack_test.go`; modify `scanner/redownload.go`, `scanner/manager.go`, `scanner/discovery.go`, `model/sonarr.go`, and existing redownload tests.

**Interfaces:** Add a pure `planSonarrPackRepair(snapshot model.SonarrRepairSnapshot) model.SonarrRepairPlan`. Snapshot contains the intended episode targets, refreshed episode/file identities, health evidence, current monitoring state, active replacement coverage and per-target failure eligibility. Plan partitions targets into satisfied, replacement-covered, unresolved/searchable, exhausted and unknown, with deduplicated unhealthy file IDs. The executor obtains one adapter and retains it for the whole operation.

- [x] Write failing tests for ten-episode/partial packs; multiple seasons; one replacement covering only a subset; corrupt imported files; shared episode-file IDs; missing health evidence; unmonitored episodes; a replacement linked between snapshot and deletion; and a failed read that must defer rather than act.
- [x] Replace whole-target short-circuits based on `queueHasEpisode`/`anyEpisodeFileReplaced` with per-target coverage. Keep existing automatic-search wait and file-deletion confirmation safeguards.
- [x] Match automatic `SeasonSearch` commands by series and season; match episode searches by target intersection. Test another season's search, multiple relevant running commands, timeout, command deduplication, and cancellation before subsequent writes.
- [x] Execute at most one explicit `EpisodeSearch` containing only unresolved eligible IDs; preserve healthy files. Treat grouped native metadata as hints and resolve episode identities explicitly.
- [x] Test equivalent outcomes with v3 and v5; run scanner tests; commit `fix(sonarr): repair only unresolved season-pack episodes`.

**Deliverable:** Partial success no longer hides unresolved siblings or triggers unnecessary searches for healthy episodes.

## Task 4: Coalesce cleanup, failures and replacement actions per download

**Files:** Modify `worker/stuck_cleanup.go`, `worker/import_failure.go`, `scanner/redownload.go`, `failures/tracker.go`; create focused `worker/sonarr_pack_test.go` and `failures/tracker_test.go` additions; place shared coordination in a new focused `failures/sonarr_actions.go` if needed.

**Interfaces:** Coordinate work by instance/download identity and generation, while retaining `failures.EpisodeKey` for breaker counts. Count each episode once for one failure-driven action across scanner and worker. Track pending and confirmed outcomes separately; release reservations after unsuccessful preconditions without losing the need to retry.

- [x] Write failing tests for duplicate queue rows, repeated observations, concurrent repair paths, grace-period continuity, partial imports, mixed per-episode thresholds, disabled breaker, non-AltMount ownership and a new replacement download for the same episode.
- [x] Select one removal per group. Bump only unresolved affected episodes once; unmonitor exhausted targets before allowing a replacement. On mixed thresholds, remove/blocklist without auto-search and explicitly search eligible siblings. An unmonitor failure halts that replacement sequence.
- [x] Test duplicate-free deletion/search, unchanged queue rules, unknown targets, empty IDs, successful-action resets, failed deletions, ambiguous timeouts, command coalescing and state refresh before retries. Cover cascade sibling deletion/404 and assert all exhausted episodes still receive their monitoring decision. Explicitly verify no cross-version mutation retry.
- [x] Run worker/scanner tests and focused concurrency tests with `-race`; commit `fix(sonarr): coordinate pack cleanup and failure tracking`.

**Deliverable:** One failed pack produces one download action, while every relevant episode retains its independent failure policy.

## Task 5: Validate actual versions and document rollout

**Files:** Extend `clients/sonarr_test.go` with end-to-end adapter scenarios; add opt-in live smoke coverage under `internal/arrs/clients/`; update `docs/docs/3. Configuration/integration.md`, `config.sample.yaml`, and generated API documentation if the config contract changes.

- [x] Run the complete ARR suite and race checks appropriate to the new shared coordinator, backend lint for changed packages, frontend type checks, and config/API schema checks required by the repository.
- [ ] Against disposable v4 and v5 instances, verify API selection, status/library/queue/history reads, webhook/download-client registration and command status; verify forced v3 on v5. Use controlled fixture downloads for destructive cleanup checks, never a user's library.
- [ ] Exercise partial pack + active sibling replacement + mixed failure thresholds; record observed deletes/searches and confirm healthy episode files survive. Label any live scenario that could not be run instead of claiming live validation.
- [x] Document automatic selection, overrides/rollback, mixed-instance setup, group cleanup and per-episode limits. Update PR #1033's title/body to describe the final implementation and meaningful evidence.
- [x] Review the final diff and commit `docs(sonarr): document native v5 and season-pack recovery`.

## Acceptance scenario

Run v4 and v5 together. Both use the expected API and honor URL bases/proxies. For a ten-episode pack with eight healthy imports, one active replacement and one missing episode, the repair preserves all eight files, avoids a duplicate replacement, removes/blocklists the failed release once when appropriate, and searches only the remaining eligible episode. Repeating the event does not add another breaker count or search. Force v3 on v5 and observe equivalent repair outcomes.

## Size and primary risks

This is a substantial backend change with a small configuration UI change, best reviewed as the five dependent steps above. Highest risks are native-v5 contract drift, destructive cleanup affecting healthy siblings, and races between Sonarr's own redownload handling and AltMount's scanner/worker. Native v5 must not be enabled by default until endpoint parity and partial-pack tests pass.

## Execution notes

- Read-only smoke checks passed on disposable Sonarr 4.0.15.2941 (auto v3), Sonarr 5.0.0.2800 (auto v5), and v5 forced to v3. Libraries were empty; registration writes and destructive pack repair were verified with HTTP fixtures, not populated live libraries. The two unchecked live mutation steps above record that limit.
- A pinned native OpenAPI contract and provenance live under `internal/arrs/clients/testdata/sonarr/`.
- The version-neutral interface temporarily retains starr's generic API methods for shared command helpers. Native adapters explicitly allow/map their used command and health endpoints and reject unsupported legacy paths.
- Client/consumer interface migrations were integrated together to keep the implementation commit buildable. Both v3/v5 pack execution and failure recovery were tested before publication.
- Recovery stages retain target identities independently of queue presence and monitoring state, serialize scanner/worker actions by download, and reconcile uncertain commands/history failures. In-memory completion/stage retention is bounded to one day (completed callbacks/file stages: at most 10,000 entries); restart does not persist that recovery state.
