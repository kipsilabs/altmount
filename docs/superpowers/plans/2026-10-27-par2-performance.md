# PAR2 Performance Implementation Plan

**Goal:** Improve the existing streaming repair path while preserving checksums, playback priority, and bounded buffering.
**Architecture:** Batch at most four source slices within 32 MiB of input buffering; fold through persistent workers and the existing SIMD backend. Preserve eight default margin rows, expose an optional lower margin, retain hidden-damage insurance, and report backend and phase timings.
**Tech Stack:** Go, gopar-turbo gf16, existing NNTP background lane.
**Spec:** User approved all optimizations described in the preceding response.

- [x] Add batch round-trip tests with partial batches, late missing slices, and live worker limits; run them red. Capture existing fold benchmark.
- [x] Implement synchronous FoldPresentBatch, reusable prepared buffers, coefficient tables, persistent workers, and a bounded batch in sweep. Run repair tests and compare benchmarks.
- [x] Test configurable recovery margins (nil default, explicit zero, verify-only safety, hidden damage). Wire config through service and planner without changing defaults.
- [x] Add structured backend, memory, and phase timing logs. Document concurrency, memory, scratch placement, profiling, and benchmark usage.
- [x] Run repair tests with CGO and pure Go, race tests, full Go tests, and review the diff.

Review focus: partial final batches; corruption discovered while earlier slices wait for folding; changing playback state; closing lazy workers; memory overhead and spill behavior. Do not adjust deployment settings or raise default resource consumption without workload evidence.

## Validation record

- Full `go test ./...`: passed with loopback access for local HTTP/NNTP test servers.
- `go test -race ./internal/par2repair ./internal/config -count=1`: passed.
- `CGO_ENABLED=0 go test internal/par2repair/solver.go internal/par2repair/solver_test.go -count=1`: passed. The full package cannot build without CGO because the existing NNTP dependency references unavailable rapidyenc types.
- Frontend TypeScript build check, focused Biome check, and all six Bun tests: passed.
- Independent code review: no important findings; checked buffered slice ownership, late damage, playback limits, and worker cleanup before arena unmapping.
- Apple M4, 10 Go workers, 4 MiB slices, eight recovery rows: original single-slice median 4842 MB/s; four-slice batch median 9003 MB/s (~1.86x arithmetic throughput). This is a synthetic fold benchmark, not an end-to-end NNTP repair measurement.
- CPU profile captured at `/tmp/altmount-par2-fold.cpu`; most sampled computation remains in the CGO arithmetic backend. Download bottlenecks require a real release/provider workload.

Implementation decisions: retain deployment defaults and existing hidden-damage insurance; expose baseline margins in YAML/API/UI; use one input slice for spill jobs to limit added resident memory; close each persistent worker pool before retrying or unmapping its arena. The memory budget remains a soft accumulator threshold, not an RSS cap.

Verification limitation: the repository-pinned golangci-lint could not decode Go 1.27 export data (version 4 versus supported version 2). This prevents Go lint validation with the installed toolchain; compilation, the full test suite, race checks and frontend checks passed.
