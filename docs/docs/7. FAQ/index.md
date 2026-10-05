---
title: Frequently Asked Questions
description: Answers to common questions about AltMount setup, Usenet providers, streaming, ARR integration, and more.
keywords: [altmount, faq, questions, usenet, streaming, arr, setup, providers]
---

# Frequently Asked Questions

Answers to the most common questions from the AltMount community.

## General

### What's the difference between AltMount and similar tools?

AltMount is an all-in-one solution that combines NZB downloading, WebDAV serving, and media streaming. Compared to other tools, AltMount adds:

- **Built-in rclone mounting** (FUSE) — no separate rclone setup needed
- **Web UI** for configuration, queue management, and health monitoring
- **Corruption detection during playback** — files are checked as they're streamed
- **Internal Fuse Mount** — no need to mount a separate FUSE filesystem

### Which import strategy should I use?

It depends on your setup:

- **SYMLINK** (recommended for most users): Best when using Sonarr/Radarr. Creates symlinks that ARR apps can import from.
- **NONE** (Direct): Simplest setup. Access files directly via WebDAV/FUSE mount.
- **STRM**: Creates `.strm` files with WebDAV URLs. Useful for remote players or setups without ARR apps.

See the [Import Strategy Comparison](../3.%20Configuration/integration.md#import-strategy-comparison) for a detailed breakdown.

## Troubleshooting

### Can an NZB point to a compressed archive containing another NZB?

Yes. AltMount automatically resolves RAR or ZIP wrapper posts containing one `.nzb` or `.nzb.gz`, then imports the release described by that NZB. The wrapper is read and decompressed during import; the final release uses normal streaming. The original queue item, category, and download ID are retained for Sonarr/Radarr.

Wrapper posts may include `.nfo`, `.txt`, `.sfv`, `.srr`, or `.par2` sidecars. Archives containing media or other payloads use the normal import path. RAR passwords are read from the NZB's existing password metadata or filename conventions; encrypted ZIPs are unsupported.

Automatic resolution is limited to 32 MiB of archive volumes per level, an 8 MiB extracted NZB, and four wrapper levels. Multiple embedded NZBs, invalid NZBs, and cycles fail the import. This feature does not enable streaming compressed media files.

### Why are my imports stuck in purple?

Imports stuck in purple/processing state are usually caused by:

1. **A hanging ffprobe process** during media analysis
2. **Memory exhaustion** on the host system
3. **A degraded container** state

**Quick fix**: Restart the container (`docker restart altmount`). If that doesn't help, check for stuck ffprobe processes and review system memory.

See [Stuck Imports Troubleshooting](../5.%20Troubleshooting/common-issues.md#imports-stuck-in-processing-state-purple) for full details.

### Why does my library size change after a health repair?

When AltMount detects a corrupted file and triggers a repair through Sonarr/Radarr, the ARR app searches for a replacement. The new release may be a different encode with a slightly different file size. This is normal — the content is the same, but the specific release may differ.

### My playback keeps freezing. What should I do?

Playback freezing is most commonly caused by rclone VFS settings (if using rclone mount) or insufficient prefetch. Check the [Playback Freezing Guide](../5.%20Troubleshooting/performance.md#playback-freezing) and the [Rclone VFS Settings](../3.%20Configuration/streaming.md#rclone-vfs-recommended-settings) for recommended configurations.

## Configuration

### What health check percentage should I use?

The default **5%** works well for most libraries. It catches the majority of corruption since affected files usually have many missing segments, not just one or two.

- Use **10-20%** if you want more confidence with moderate resource usage.
- Use **100%** temporarily after switching Usenet providers or if you suspect widespread issues.

See [Choosing the Right Percentage](../3. Configuration/health-monitoring.md#choosing-the-right-segment_sample_percentage) for guidelines.

### How do I configure symlink paths with Docker?

The key requirement is that both AltMount and your ARR containers must see the same paths. Mount `import_dir` and `mount_path` as shared Docker volumes:

```yaml
services:
  altmount:
    volumes:
      - /mnt:/mnt:rshared
  sonarr:
    volumes:
      - /mnt:/mnt
```

See [Symlink Paths with Docker](../5.%20Troubleshooting/common-issues.md#symlink-paths-not-working-with-docker) for detailed troubleshooting.

### Why are my providers showing offline?

Provider connectivity issues can be caused by:

1. Running an outdated AltMount image — always pull the latest version
2. Incorrect or expired credentials — verify on your provider's website
3. Exceeding connection limits — reduce `max_connections` if running other Usenet clients
4. Network issues — try alternative provider endpoints

See [Providers Showing Offline](../5.%20Troubleshooting/common-issues.md#providers-showing-offline) for step-by-step troubleshooting.
