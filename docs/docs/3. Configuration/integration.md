---
title: ARR Integration
description: Integrate AltMount as a SABnzbd-compatible download client with Sonarr, Radarr, and other ARR applications.
keywords: [altmount, sonarr, radarr, sabnzbd, arr, integration, download client, usenet]
---

# ARR Integration

AltMount acts as a SABnzbd-compatible download client for Sonarr and Radarr. This guide walks through the full setup process for seamless integration with your media management applications.

## Sonarr Version Compatibility

Sonarr v4 and v5 use the same AltMount integration. You can configure instances of both versions at the same time; no version selector or configuration migration is required. Enter the instance URL (including any URL base, such as `http://sonarr:8989/sonarr`), without an `/api/v3` or `/api/v5` suffix.

AltMount uses the v3 compatibility API retained by Sonarr v5 for library discovery, queue cleanup, repair searches, and download client and webhook registration. This preserves the per-episode queue records used by AltMount's failure tracking; Sonarr's native v5 API groups multiple episodes into one queue record.

## Prerequisites

Before configuring ARR integration, make sure the following are in place:

1. **SABnzbd compatibility enabled** -- set `sabnzbd.enabled: true` in your config
2. **ARRs service enabled** -- set `arrs.enabled: true`
3. **At least one NNTP provider configured** -- AltMount needs a working Usenet connection

## Step 1: Configure SABnzbd Categories

Set up categories in your AltMount config that match your ARR setup. Each category needs a `type` field indicating which ARR it belongs to:

```yaml
sabnzbd:
  enabled: true
  categories:
    - name: movies
      type: radarr
    - name: tv
      type: sonarr
```

The category names should match what you configure in Sonarr/Radarr as the download client category.

## Step 2: Add ARR Instances

Navigate to the ARR settings page in the AltMount web UI. Add each Sonarr and Radarr instance with:

![ARR integration settings showing Radarr/Sonarr instance configuration](/images/config-arrs.png)

- **Instance URL** -- the full URL of your Sonarr/Radarr instance (e.g., `http://sonarr:8989`)
- **API key** -- found in Sonarr/Radarr under Settings > General

## Step 3: Register as Download Client

AltMount can automatically register itself as a SABnzbd download client in your Sonarr/Radarr instances.

**Via the web UI:** Click the "Register Download Client" button on the ARR settings page.

**Via API:**

```bash
curl -X POST http://localhost:8080/api/arrs/download-client/register \
  -H "Authorization: Bearer <token>"
```

This tells Sonarr/Radarr to send NZBs to AltMount using the SABnzbd API protocol.

## Step 4: Register Webhooks

Webhooks enable instant import notifications. When a download completes, Sonarr/Radarr is notified immediately instead of waiting for the next polling cycle.

**Via the web UI:** Click "Register Webhooks" on the ARR settings page.

**Via API:**

```bash
curl -X POST http://localhost:8080/api/arrs/webhook/register \
  -H "Authorization: Bearer <token>"
```

## Step 5: Configure Import Strategy

AltMount supports three import strategies. Choose the one that best fits your setup:

### Import Strategy Comparison

| Feature           | **NONE** (Direct)                                      | **SYMLINK**                                                                                                   | **STRM**                                                                                                        |
| ----------------- | ------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| How it works      | Media accessed directly via WebDAV/FUSE mount          | Creates symlinks in an import directory                                                                       | Creates `.strm` files pointing to WebDAV URLs                                                                   |
| ARR integration   | ARR reads directly from mount                          | ARR imports from symlink directory                                                                            | ARR imports `.strm` files                                                                                       |
| Docker setup      | Simplest — only mount path needed                      | Requires shared volumes for import dir + mount                                                                | Requires shared volume for STRM output dir                                                                      |
| Best for          | Simple setups without no debrid program like dechyparr | Most ARR setups (recommended)                                                                                 | Emby/Jellyfin full stream speed                                                                                 |
| File management   | Files managed through AltMount only                    | ARR manages library, AltMount manages metadata                                                                | Player resolves `.strm` URL at playback time                                                                    |
| Health monitoring | Metadata-only sync (fast)                              | Full sync with library directory                                                                              | Full sync with library directory                                                                                |
| Limitations       | Slower arr imports due to the need of mount refreshing | Symlinks can become broken when the original file is replaced if health monitoring is not properly configured | STRM files can become broken when the original file is replaced if health monitoring is not properly configured |

### Recommended: SYMLINK Strategy

For ARR integration, the **SYMLINK** import strategy is recommended. This creates symlinks in an import directory that your ARR apps can access.

```yaml
# Root-level mount path (NOT inside import section)
mount_path: /mnt/remotes/altmount
import:
  import_strategy: SYMLINK
  import_dir: /mnt/symlinks/altmount
```

- `mount_path` -- **root-level config field** — the path where the WebDAV/FUSE content is mounted, as seen by the ARR apps
- `import_dir` -- the directory where AltMount creates symlinks for completed downloads. Must be accessible by Sonarr/Radarr

> **Note:** `mount_path` is a root-level configuration field, not nested inside `import`. Both AltMount and your ARR apps must be able to reach `import_dir` and `mount_path`. In Docker, this typically means shared volumes.

### NONE (Direct Access) Strategy

Use this when you don't need ARR integration or prefer to access files directly through the WebDAV/FUSE mount:

```yaml
import:
  import_strategy: NONE
```

No `import_dir` or `mount_path` is needed. Media is accessed directly via the mount point.

### STRM Strategy

Creates `.strm` files that contain WebDAV URLs. Useful when your media player can resolve HTTP URLs directly:

```yaml
# Replace with your actual AltMount hostname/port if changed
mount_path: http://altmount:8080

import:
  import_strategy: STRM
  import_dir: /mnt/strm/altmount
```

### Example Docker Volume Setup (SYMLINK)

```yaml
services:
  altmount:
    volumes:
      - /mnt/mnt:rshared

  sonarr:
    volumes:
      - /mnt/mnt
```

With this layout, both containers see the same paths, so `import_dir: /mnt/symlinks/altmount` and `mount_path: /mnt/remotes/altmount` will work correctly.

### Common Import Strategy Issues

- **Symlinks failing**: Ensure both `import_dir` and `mount_path` resolve to valid paths inside all containers. If symlinks point to paths that don't exist inside the ARR container, imports will fail silently.
- **Extra path segments** (e.g., `../complete/..` appearing in paths): This usually means `mount_path` doesn't match the actual mount point inside the container. Double-check your Docker volume mappings.
- **STRM files not playing**: The WebDAV URL in the `.strm` file must be reachable from the media player. If using Docker, ensure the player can reach AltMount's hostname and port.

## Step 6: Configure Queue Cleanup

AltMount can automatically monitor ARR queues and clean up stuck or failed imports to keep
things tidy. One pass covers all *arr types (Radarr, Sonarr, Whisparr, Lidarr, Readarr,
Sportarr): it removes ghost/empty-folder entries (already imported, or the source path is
gone) and applies your message rules to stuck imports. Only queue items owned by AltMount's
download client are ever touched.

```yaml
arrs:
  enabled: true
  webhook_base_url: "" # Base URL for webhook callbacks (defaults to http://<host>:<port>)
  queue_cleanup_enabled: true
  queue_cleanup_interval_seconds: 300
  queue_cleanup_grace_period_minutes: 5 # Wait before cleaning up (default: 5)
  # Message rules: when a stuck import's *arr status message matches a rule, run its
  # action (remove | blocklist | blocklist_search). Manage these in the web UI.
  queue_cleanup_rules:
    - message: "Sample"
      enabled: true
      action: blocklist_search
    - message: "Not a Custom Format upgrade"
      enabled: true
      action: remove
    - message: "automatic import is not possible"
      enabled: false
      action: remove
```

| Field                                | Description                                                                                                                                            |
| ------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `queue_cleanup_enabled`              | Enable or disable automatic queue cleanup                                                                                                             |
| `queue_cleanup_interval_seconds`     | How often to check ARR queues (in seconds)                                                                                                            |
| `queue_cleanup_grace_period_minutes` | Minimum age (in minutes) a stuck/failed item must persist before it is cleaned up (default: 5). Ghost/empty-folder removal is immediate.              |
| `webhook_base_url`                   | Base URL ARRs use to reach AltMount for webhooks (default: `http://<host>:<port>`)                                                                    |
| `queue_cleanup_rules`                | Message rules for stuck imports. Each rule has a `message` substring (case-insensitive), an `enabled` boolean, and an `action`: `remove`, `blocklist`, or `blocklist_search`. |

## Verifying the Setup

After completing the steps above:

1. Send a test NZB from Sonarr or Radarr to AltMount
2. Check the AltMount queue page to confirm the NZB was received
3. Once processed, verify that Sonarr/Radarr picks up the import via webhook

You can also check the integration health via API:

```bash
curl http://localhost:8080/api/arrs/health \
  -H "Authorization: Bearer <token>"
```

## Troubleshooting

### Download Client Test Fails

If the connection test in Sonarr/Radarr fails:

1. **Verify network connectivity** between ARR and AltMount containers
2. **Check SABnzbd API is enabled** in AltMount config
3. **Confirm the host and port** match your AltMount instance
4. **Check firewall rules** if running on separate hosts

### Imports Not Detected

If Sonarr/Radarr doesn't detect completed downloads:

1. **Verify webhooks are registered** -- check ARR Settings > Connect
2. **Check import paths** -- `import_dir` and `mount_path` must be accessible to ARR
3. **Review ARR logs** for import errors
4. **Test manual import** from ARR to verify path configuration

### Queue Cleanup Issues

If queue cleanup isn't working as expected:

1. **Verify ARR instances are configured** with valid API keys
2. **Check cleanup interval** isn't too frequent (recommended: 300+ seconds)
3. **Review allowlist** to ensure error messages match exactly
4. **Monitor ARR logs** for API communication issues

## Next Steps

With ARR integration configured:

1. **[Configure Health Monitoring](health-monitoring.md)** - Enable automatic repair for corrupted files
2. **[Troubleshooting](../5.%20Troubleshooting/common-issues.md)** - Resolve integration issues

---

ARR integration enables fully automated media management with AltMount acting as your primary download client. Combined with health monitoring, you can achieve a self-healing media collection that automatically repairs corruption and maintains high availability.


## Indexer cart RSS

In **Configuration → Import**, set **RSS feed URL** to your indexer's cart RSS URL
and choose a **Polling interval (seconds)**. The default is 300 seconds; accepted
intervals range from 1 to 86400 seconds. Clear the URL to disable polling. Changes
apply automatically without restarting AltMount.

```yaml
import:
  rss_feed_url: "https://indexer.example/rss?apikey=YOUR_KEY"
  rss_poll_interval_seconds: 300
```

AltMount reads RSS 2.0 items, downloads NZBs from an NZB enclosure or the item's
link, and sends them through the normal import queue. Relative URLs are supported.
The saved feed URL is hidden in configuration responses because it can contain an
API key. Keep the masked value when changing other settings, replace it to change
the feed, or clear it to disable the feature.

Successfully queued entries are remembered in the database even after queue or
history cleanup and restarts. Failed downloads retry on the next poll, and a failed
entry does not block other entries. Changing the feed URL creates a new feed identity.
The feature supports one RSS feed and XML NZBs; Atom feeds and compressed NZB
attachments are not supported. Network proxy settings and the configured User-Agent
apply to both feed and NZB requests. Feed responses are limited to 8 MiB and NZBs
to 64 MiB. A local configured feed is supported, but a feed cannot direct downloads
to other literal private addresses.
