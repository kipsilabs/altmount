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
  max_memory_mb: 256       # accumulator memory budget per job
  max_concurrent_jobs: 1   # simultaneous repair jobs
  max_patch_store_mb: 0    # total patch-store size cap; 0 = unlimited
  repair_on_import: false  # queue a repair as soon as a damaged file imports
```

- **`max_repair_ratio`** — defaults to the same 2% byte-ratio the streaming zero-fill policy tolerates, so everything watchable becomes byte-exact. Raise it (e.g. `0.10`) to save more heavily damaged files instead of letting ARR replacement handle them. The release's PAR2 redundancy is always the hard ceiling: damage beyond the posted recovery slices is unrepairable no matter the setting.
- **`max_memory_mb`** — a repair holds one slice-sized buffer per missing slice. A plan exceeding this budget is marked unrepairable.

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
