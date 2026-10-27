# PAR2 Repair

AltMount can repair releases with missing (taken-down) usenet articles using the PAR2 recovery files posted alongside them. When a stream hits a dead article, playback continues instantly (zero-fill for plain video), and a background job reconstructs the missing bytes with Reed–Solomon from the release's PAR2 data. The repaired article payloads are stored locally and served on every later read — byte-exact, permanently, surviving restarts.

## How it works

1. **Trigger** — a repair job is queued when: a stream hits a confirmed-missing article, the health checker classifies a file as degraded, an import completes with missing segments (opt-in, see `repair_on_import`), or you `POST /api/par2repair`.
2. **Plan** — the job parses the PAR2 index recorded at import time, maps the dead articles to recovery-set slices, and checks the damage against the caps below and against the redundancy the release actually carries.
3. **Sweep** — the entire recovery set (all RAR volumes) is streamed once from your providers over the import connection lane, so active streams keep priority. Nothing release-sized is stored; memory stays at damage-size.
4. **Verify & persist** — recovered slices are verified against the PAR2 checksums (MD5 + CRC32) before anything is written. Patches are written atomically: a concurrent read sees zero-fill or the finished bytes, never garbage.

The cost of one repair is roughly one full download of the release — that is inherent to how PAR2's Reed–Solomon math works (every recovery slice is a combination of *all* input slices). It happens once per file, ever.

## Configuration

```yaml
par2_repair:
  enabled: true            # default: true
  max_repair_ratio: 0.02   # max fraction of a file's bytes a repair may reconstruct
  max_memory_mb: 256       # accumulator heap budget; larger jobs use scratch files
  max_concurrent_jobs: 1   # simultaneous repair jobs
  max_connections: 10     # shared repair download connections
  recovery_margin: 8      # baseline spare recovery slices
  patch_dir: ""           # patches and scratch; defaults to <metadata_root>/patches
  max_patch_store_mb: 0    # total patch-store size cap; 0 = unlimited
  repair_on_import: false  # queue a repair as soon as a damaged file imports
```

- **`max_repair_ratio`** — defaults to the same 2% byte-ratio the streaming zero-fill policy tolerates, so everything watchable becomes byte-exact. Raise it (e.g. `0.10`) to save more heavily damaged files instead of letting ARR replacement handle them. The release's PAR2 redundancy is always the hard ceiling: damage beyond the posted recovery slices is unrepairable no matter the setting.
- **`max_memory_mb`** — a repair holds one slice-sized buffer per missing slice. A plan exceeding this budget uses memory-mapped scratch files. This is an accumulator budget, not a hard process RSS limit: recovery payloads, recovered slices, fetch buffers and solver scratch also need memory.

A failed PAR2 repair does not make a degraded file unplayable. AltMount records the failure reason on its health record and keeps the file degraded and visible, with its existing health-check schedule. Files already classified as corrupted and releases that have not imported yet keep their existing handling.

- **`repair_on_import`** — when an import finds confirmed missing segments, queue the repair immediately rather than waiting for someone to press play. This also covers **archive sets** (RAR/7z), which were previously dropped outright: a damaged set is parked as `waiting_repair` in the queue while PAR2 rebuilds its volumes, then imports automatically once the repair lands (or fails with the repair's reason if the damage proves unrepairable). Because repaired bytes exist only locally, the import availability sweep and segment reads consult the patch store, so a repaired release imports normally. The reason this matters is article lifetime: a release's PAR2 volumes are most likely to still be retrievable close to its post date, so a file imported today with three dead articles is repairable today but may not be six months from now. Off by default, because each repair downloads the full release — importing a large damaged backlog with this on is expensive.
- **`max_patch_store_mb`** — caps the total size of stored patches; when exceeded, the oldest patches are evicted first. Safe because patches are regenerable — a later stream simply re-triggers the repair.

The Health page lists recent repair jobs with live sweep progress, an estimated time remaining, and how long each job ran — reported for failed jobs too, since a repair has to stream the release before it can tell whether the damage is beyond its recovery data. When a repair cannot run, the reason is shown in plain language along with what to do about it.

Repaired payloads live under `<metadata_root>/patches/`. They can be deleted at any time. After a successful repair the file's health status flips back to healthy.

## API

List recent repair jobs:

```bash
curl http://localhost:8080/api/par2repair
```

Queue a repair manually:

```bash
curl -X POST http://localhost:8080/api/par2repair \
  -H "Content-Type: application/json" \
  -d '{"file_path": "/movies/Some.Movie.2024/movie.mkv"}'
```

## Notes

- Works for plain files and for RAR/AES/nested releases (their streams fail on new damage as before, but repaired articles serve transparently).
- Files without PAR2 files in the original NZB, or whose PAR2 articles are themselves gone, cannot be repaired. Degraded files stay visible and their health records explain why repair failed.
- Job state persists in the database: pending repairs survive restarts, transient NNTP failures retry with exponential backoff.

## Performance tuning

Repair normally reads the full recovery set once. Measure one repair with playback
idle before changing settings: playback deliberately limits folding to one worker,
and the NNTP background lane keeps streams and imports ahead of repair downloads.

- If logs show most sweep time in `fetch_wait`, try raising `max_connections` from
  10 to 20, then 32, while watching provider limits and throughput. Changes apply
  live. Each job's prefetch is capped at 64; connections are shared across jobs.
  Start with `max_concurrent_jobs: 1` to avoid competing for bandwidth and memory.
- If `fold_time` dominates, check the `PAR2 solver backend` log. `accelerated=true`
  confirms the CGO SIMD backend; `method` identifies the selected kernel. Custom
  builds should use `CGO_ENABLED=1` on a supported platform. Idle worker width also
  depends on Go's CPU/container limits.
- Each recovery row is folded across every present slice. `recovery_margin` keeps
  eight baseline spare rows by default. Lower it to 2, or explicitly 0, for less
  arithmetic when damage is well known. This can cost another full download if
  more damage appears or the matrix is singular. Estimated hidden-damage rows are
  still added, and verification-only repairs retain at least eight baseline rows.
  Negative values use the default. Changes apply to newly planned jobs.
- Large repairs log `spill_to_disk=true`. If paging is slowing them down, increase
  `max_memory_mb` only when RAM is available, or place `patch_dir` on fast local
  storage. It also contains persistent patches; changing it requires a restart
  and does not migrate existing patches. Memory mapping allows OS paging rather
  than imposing a strict resident-memory cap.

The sweep batches up to four slices with at most 32 MiB of raw batch buffers,
plus prepared SIMD buffers of comparable size. A single larger slice still needs
its full buffer. Spill jobs use one slice at a time to avoid extra batch memory.
Workers and prepared buffers are reused across folds, and playback priority is
rechecked for each batch. Checksums are still verified before folding and before
patches are persisted.

`PAR2 planning performance` reports liveness checking and planning time.
`PAR2 sweep performance` reports downloaded bytes and time spent waiting for
articles, checking CRC32, and folding. `PAR2 job performance` reports elapsed,
recovery download, solve, recovered-slice verification, and persistence times,
including failed attempts. These durations describe the consumer pipeline:
background downloads overlap arithmetic, so `fetch_wait` is stalled consumer time,
not total network time. Compare runs with the same release, damage and CPU limits.

Benchmark the arithmetic on an idle machine:

```bash
go test ./internal/par2repair -run '^$' -bench BenchmarkFoldPresent -benchmem -count=5
```

Capture an arithmetic CPU profile (this benchmark uses synthetic data, no providers):

```bash
go test ./internal/par2repair -run '^$' -bench BenchmarkFoldPresentBatch -benchtime=5s -cpuprofile=/tmp/par2-fold.cpu
go tool pprof /tmp/par2-fold.cpu
```

Fold benchmark throughput excludes NNTP downloads and is not an end-to-end speedup.
