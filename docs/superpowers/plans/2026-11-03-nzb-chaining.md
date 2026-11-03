# NZB Chaining Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans to implement task-by-task.

**Goal:** Resolve direct and gzip NZB payloads and queue multiple nested NZBs independently.
**Architecture:** Extend the existing bounded resolver. A typed expansion result carries validated child NZBs and chain history to the import service. Stage child files before atomically switching the original job to the first release and inserting its siblings. Continue processing the first release in the same worker to preserve ARR/Stremio tracking.
**Tech Stack:** Go, existing ZIP/RAR and NZB libraries, SQLite/PostgreSQL queue repositories.
**Spec:** Issue #1040 and the user's approval in this conversation: support all cases including multiple NZBs.

## Global Constraints

- Preserve single-job identity; inherit category, priority, indexer and operational flags for children, with independent download IDs for additional jobs; the first release keeps the original identity.
- Preserve four wrapper levels, 32 MiB per level, 8 MiB per NZB; cap fan-out at 64 jobs.
- Do not treat media posts with NZB sidecars as wrappers.
- Never extract untrusted archive paths to disk.
- Publish only when the user explicitly requests it.

## Review Focus

Transport failures, atomic rollback, duplicate expansion on retries, branch-spanning cycles/depth, and premature wrapper media notifications.

### Task 1: Extend bounded resolution

**Files:** internal/importer/nested_nzb.go, internal/importer/nested_nzb_test.go, internal/importer/processor.go.
**Interfaces:** Keep resolveNestedNzb's single-byte-result interface; return a typed nestedNzbJobsError for multiple validated named payloads. Carry chain state through metadata.
- [x] Add direct/gzip integration cases and multiple-payload assertions; run them and observe failure.
- [x] Add direct candidates and validate every candidate before returning the expansion result. Apply size, count, depth and cycle checks.
- [x] Run nested NZB tests; expected PASS.

### Task 2: Atomically expand queue jobs

**Files:** internal/importer/nested_nzb_queue.go, internal/importer/nested_nzb_queue_test.go, internal/importer/service.go, internal/database/queue_repository.go, internal/database/nested_nzb_test.go.
**Interfaces:** QueueRepository.ExpandNzbWrapper(ctx, parent, children) updates the original job to the first child and inserts siblings in one transaction, once. Service handles typed expansion and continues processing the first release with its original job and download IDs. Comparing the original NZB path prevents duplicate expansion.
- [x] Add real database tests for child inheritance, independent identities, retry idempotence, transaction rollback, depth and cycles; observe failures.
- [x] Stage named child payloads using safe generated paths, inherit queue settings and sanitized metadata, and atomically expand the parent.
- [x] Verify importer and repository tests; expected PASS.

### Task 3: Documentation and verification

**Files:** docs/docs/7. FAQ/index.md.
- [x] Describe direct/gzip and multi-job behavior and limits.
- [x] Run formatting, targeted race tests and lint; inspect final diff.

## Execution notes

- RED→GREEN: direct/gzip imports, multiple-ZIP resolution, database fan-out interface, service fan-out, and original-job identity regression.
- Review found that retiring the wrapper before its children finish would falsely report failure to ARR/Stremio. The first child now continues in the original job.
- Final validation passed: `go test -race ./internal/importer/... ./internal/database ./internal/api` and `go vet ./internal/importer/... ./internal/database ./internal/api`.
- Independent review identified the original-job completion issue; its regression test failed before the correction and passed afterward. No deferred code-review findings.
- golangci-lint cannot decode Go 1.27 export data (installed tool supports an older export format).
- The user subsequently requested a PR; publish the implementation on `feat/nzb-chaining`.
