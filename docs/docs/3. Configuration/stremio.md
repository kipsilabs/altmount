---
title: Stremio Integration
description: Stream Usenet content directly in Stremio using AltMount's built-in addon with automatic Prowlarr search.
keywords: [altmount, stremio, streaming, usenet, prowlarr, addon, nzb]
---

# Stremio Integration

AltMount provides two complementary ways to stream Usenet content directly through [Stremio](https://www.stremio.com/):

1. **Stremio Addon** — install a personalised addon in Stremio that automatically searches Prowlarr by IMDB ID, downloads the best NZB, and returns stream URLs without any manual steps.
2. **Manual NZB upload** — POST an NZB file to `POST /api/nzb/streams` from your own Stremio add-on; AltMount queues it and returns stream URLs once the content is ready.

## Overview

| Mode | How it works | Best for |
|------|-------------|----------|
| Stremio Addon | Stremio requests streams → AltMount searches Prowlarr → downloads best NZB → returns streams | Fully automatic playback |
| Manual endpoint | Your add-on POSTs an NZB → AltMount queues it → returns streams | Custom workflows, hand-picked NZBs |

## Prerequisites

- Stremio integration enabled in your configuration (`stremio.enabled: true`).
- At least one NNTP provider configured and online.
- Your AltMount API key (visible in **Settings > API Key**).
- *(For automatic search)* Prowlarr running and accessible from AltMount.

## Web UI Configuration

![Stremio configuration](/images/config-stremio.png)
_AltMount web interface showing the Stremio Integration settings_

The Stremio settings are split into four cards:

- **Endpoint** — toggle the integration on/off and set the Public Base URL.
- **Addon Install URL** — appears automatically once the integration is enabled. Shows the manifest URL that Stremio needs. Use **Copy** to copy it to your clipboard or **Install** to open Stremio directly.
- **Cache** — NZB File Cache TTL controls how long AltMount keeps downloaded NZBs on disk.
- **Prowlarr Indexer** — optional automatic search by IMDB ID (see below).

## Configuration (YAML)

Add the following block to your `config.yaml`:

```yaml
stremio:
  enabled: true
  nzb_ttl_hours: 24   # 0 = keep cached streams forever
  nzb_grab_cache_hours: 24 # reuse NZBs already grabbed from the same nzb_url; 0 = no cache
  skip_search_when_cached: false # Skip indexer search when matching cached streams exist
  base_url: ""        # optional — set if auto-detection gives the wrong origin
  prowlarr:
    enabled: true
    host: "http://localhost:9696"
    api_key: "YOUR_PROWLARR_API_KEY"
    categories: [2000, 2010, 2030, 2040, 2045, 2060, 5000, 5010, 5030, 5040]
    languages: []   # e.g. ["English", "DUAL"] — empty = all
    qualities: []   # e.g. ["1080p", "4K"] — empty = all
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `false` | Enable the Stremio integration |
| `nzb_ttl_hours` | int | `24` | Hours before a cached NZB result expires. `0` means never expire. |
| `nzb_grab_cache_hours` | int | `24` | Hours an `nzb_url` sent to `/api/nzb/streams` (e.g. by AIOStreams) reuses its earlier import instead of grabbing the NZB again, sparing strict indexers. `0` disables it. |
| `skip_search_when_cached` | bool | `false` | Return matching cached imports and healthy library streams without searching indexers. Searches normally when no cached streams match. Respects NZB cache expiry; library streams follow the Reuse Library Releases setting. |
| `base_url` | string | `""` | Public base URL used when building stream and manifest links (e.g. `https://altmount.example.com`). When empty, AltMount auto-detects the origin from the incoming request. Set this when running behind a reverse proxy or when the detected origin is wrong. |

When `nzb_ttl_hours` is greater than zero, submitting the same NZB filename within the TTL window returns the cached stream URLs immediately without re-queueing or re-downloading.

### Prowlarr Automatic Search

When `prowlarr.enabled` is `true`, the Stremio addon endpoint searches Prowlarr by IMDB ID before returning streams. AltMount queries `/api/v1/search` on your Prowlarr instance, picks the best NZB, queues it, and returns stream URLs — all within a single Stremio request.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `false` | Enable automatic Prowlarr search |
| `host` | string | `http://localhost:9696` | Prowlarr base URL |
| `api_key` | string | `""` | Prowlarr API key |
| `categories` | int[] | `[2000,2010,…]` | Newznab category IDs to search |
| `languages` | string[] | `[]` | Keyword filter — only show releases whose title contains at least one keyword. Empty = all. |
| `qualities` | string[] | `[]` | Keyword filter — only show releases matching a quality keyword. Empty = all. |

> **Tip:** The web UI renders categories, languages, and qualities as badge chips. Press Enter or comma to add a keyword; click × to remove.

### Scoring & Keyword Filters

Release scoring (`exclude_keywords`, `custom_formats`, language preferences) is configured from the web UI's Stremio settings card and can be previewed live with the built-in **Release Scoring & Media Search Inspector**.

Keyword filters use **token-boundary matching**, not plain substring matching:

- A keyword like `TS` matches `Movie.2024.TS.1080p` but deliberately does **not** match titles where it only appears inside a larger word, such as `DTS-HD.MA` or `Knights`.
- Multi-word phrases match across release-name delimiters: `Korean Dub` matches `Korean.Dub`, `Korean_Dub`, and `Korean-Dub`.
- Advanced users can write explicit regular expressions, either bare (`\b(cam|ts)\b`) or slash-delimited (`/(cam|ts)/i`). Invalid patterns are ignored rather than treated as literal text.

:::note Upgrading
Earlier versions matched keywords as plain substrings. After upgrading, review your exclude-keyword lists: entries that relied on partial-word hits (for example `ip` intended to block `WEBRip`) will no longer match and should be replaced with a regex such as `\bip\b|webrip` or a full token like `webrip`.
:::

## Authentication — the `download_key`

The streams endpoint and addon routes do **not** accept your raw API key. Instead they expect a `download_key`, which is the lowercase hex SHA-256 hash of your API key:

```
download_key = sha256(api_key)   # lowercase hex
```

This is safe to embed in Stremio stream URLs and the addon manifest URL: it cannot be reversed to recover your API key, and it has no other privileges in AltMount.

The `download_key` is shown in the **Settings** page. You can also compute it manually:

```bash
# Linux / macOS
echo -n "YOUR_API_KEY" | sha256sum | awk '{print $1}'

# macOS (if sha256sum is not available)
echo -n "YOUR_API_KEY" | shasum -a 256 | awk '{print $1}'
```

## Stremio Addon Install

Once the integration is enabled and the configuration is saved, AltMount generates a personalised manifest URL:

```
<base_url>/stremio/<download_key>/manifest.json
```

This URL is shown in the **Addon Install URL** card in the web UI. You can:

- **Copy** the URL and paste it into Stremio → Add-ons → Install from URL.
- **Install** to open Stremio directly via the `stremio://` URI scheme.

The `download_key` embedded in the URL is the SHA-256 hash of your API key — safe to share with the Stremio app (see [Authentication](#authentication--the-download_key)).

## Third-Party Addon Integration

If you are building your own Stremio addon you can integrate with AltMount in two ways,
depending on whether you want AltMount or your addon to handle NZB resolution.

### Pattern A — AltMount resolves the NZB (Prowlarr required)

Your addon proxies AltMount's stream endpoint. AltMount searches Prowlarr by IMDB ID and
returns ready-to-use stream options; your addon just passes them to Stremio.

```
GET <base_url>/stremio/<download_key>/stream/<type>/<id>.json
```

| Parameter | Description |
|-----------|-------------|
| `download_key` | SHA-256 of your AltMount API key |
| `type` | `movie` or `series` |
| `id` | IMDB ID (`tt1234567`) or `tt1234567:season:episode` for series |

**Response:**

```json
{
  "streams": [
    {
      "name":  "AltMount 🇬🇧 1080p - My Movie (2024) [1080p][Eng]",
      "title": "My.Movie.2024.1080p.BluRay.x264\n💾 8.50 GB 🌐 NZBgeek",
      "url":   "<base_url>/stremio/<download_key>/play?url=<prowlarr_nzb_url>&title=..."
    }
  ]
}
```

When Stremio follows a `url`, AltMount downloads the NZB, queues it at high priority, waits
for the download, and 302-redirects to the WebDAV media file. Your addon has no further work.

**JavaScript example:**

```javascript
const { addonBuilder } = require("stremio-addon-sdk");

const ALTMOUNT = "http://altmount.example.com";
const KEY      = "YOUR_DOWNLOAD_KEY"; // sha256(api_key)

const builder = new addonBuilder({
  id: "com.example.my-addon", version: "1.0.0", name: "My Addon",
  resources: ["stream"], types: ["movie", "series"], catalogs: [], idPrefixes: ["tt"],
});

builder.defineStreamHandler(async ({ type, id }) => {
  const res  = await fetch(`${ALTMOUNT}/stremio/${KEY}/stream/${type}/${id}.json`);
  const data = await res.json();
  return { streams: data.streams ?? [] };
});

module.exports = builder.getInterface();
```

---

### Pattern B — Your addon resolves the NZB

Your addon finds the NZB file itself (from any indexer or source) and hands it to AltMount.
AltMount queues the download, waits for it to complete (long-poll), and returns stream URLs
your addon passes directly to `callback({ streams })`.

```
POST <base_url>/api/nzb/streams
Content-Type: multipart/form-data
```

| Field | Required | Description |
|-------|----------|-------------|
| `download_key` | Yes | SHA-256 of your AltMount API key (alternatively, send the raw key in the `X-Api-Key` header) |
| `file` | Conditional | The `.nzb` file (max 100 MB). Provide this **or** `nzb_url` |
| `nzb_url` | Conditional | URL AltMount downloads the NZB from. Provide this **or** `file` |
| `category` | No | Download category (e.g. `movies`, `tv`) |
| `timeout` | No | Seconds to wait before returning 408 (default: `300`) |
| `season` | No | Season number — see [Selecting one episode from a season pack](#selecting-one-episode-from-a-season-pack) |
| `episode` | No | Episode number (pair with `season`) |
| `id` | No | Stremio content id (e.g. `tt1234567:1:5`) as an alternative to `season`/`episode` |

**Success response (`200 OK`):**

```json
{
  "streams": [
    {
      "url":   "http://altmount.example.com/webdav/movies/My.Movie.2024.mkv",
      "title": "My.Movie.2024.mkv",
      "name":  "AltMount"
    }
  ],
  "_queue_item_id": 42,
  "_queue_status":  "completed"
}
```

**408 Timeout** — download did not finish within `timeout` seconds. Use `queue_item_id` from
the error details to poll `GET /api/queue/:id` and retry.

**JavaScript example:**

```javascript
builder.defineStreamHandler(async ({ type, id }) => {
  // Your addon resolves the NZB however it likes
  const nzbBuffer = await myIndexer.fetchNzb(id);

  const form = new FormData();
  form.append("download_key", KEY);
  form.append("file", new Blob([nzbBuffer], { type: "application/x-nzb" }), `${id}.nzb`);
  form.append("category", type === "movie" ? "movies" : "tv");
  form.append("timeout", "300");

  const res  = await fetch(`${ALTMOUNT}/api/nzb/streams`, { method: "POST", body: form });
  if (!res.ok) return { streams: [] };

  const data = await res.json();
  return { streams: data.streams ?? [] };
});
```

> **Caching:** AltMount deduplicates by NZB filename within the configured TTL
> (`nzb_ttl_hours`). Submitting the same filename a second time returns cached stream URLs
> immediately without re-downloading.

### Selecting one episode from a season pack

When a series request resolves to a **season pack** (a single NZB containing every
episode), AltMount needs to know which episode to serve — otherwise it cannot tell the
requested episode from the others in the pack.

Provide the episode context in any one of these forms (checked in this order):

1. explicit `season` **and** `episode` fields;
2. a combined Stremio content id via `id` (or `stremio_id`), e.g. `tt1234567:1:5`;
3. `season`/`episode` query parameters embedded in the `nzb_url` value.

AltMount then returns only the matching episode's stream. If a multi-episode pack is
submitted **without** any episode context, AltMount responds with `400 Bad Request`
(`"Episode not specified"`) rather than silently returning the first episode — so make
sure your addon forwards the season/episode (or `id`) from the Stremio request.

Single-file releases (movies, single-episode releases) do not need this and are unaffected.

**AIOStreams / proxy example** — append the Stremio id when handing the NZB to AltMount:

```
POST <base_url>/api/nzb/streams
  download_key=<sha256>
  nzb_url=<season-pack nzb url>
  id=tt1234567:1:5
```

## Endpoint Reference

### `POST /api/nzb/streams`

Submit an NZB manually and receive stream URLs. The request blocks (long-polls) until the content is ready or the timeout is reached, so the add-on can hand the result straight to `callback({ streams })` — no polling required.

**Content-Type**: `multipart/form-data`

| Field | Required | Description |
|-------|----------|-------------|
| `download_key` | Yes | SHA-256 of your API key (lowercase hex). Alternatively send the raw key in the `X-Api-Key` header |
| `file` | Conditional | The `.nzb` file to process (max 100 MB). Provide this **or** `nzb_url` |
| `nzb_url` | Conditional | URL to download the NZB from. Provide this **or** `file` |
| `category` | No | Download category (e.g. `movies`, `tv`) |
| `timeout` | No | Seconds to wait before returning a 408 (default: `300`) |
| `season` | No | Season number for selecting one episode from a season pack |
| `episode` | No | Episode number (pair with `season`) |
| `id` | No | Stremio content id (e.g. `tt1234567:1:5`) as an alternative to `season`/`episode` |

> Fields may be sent as `multipart/form-data` or as URL query parameters. See
> [Selecting one episode from a season pack](#selecting-one-episode-from-a-season-pack).

### Response

**200 OK** — streams are ready:

```json
{
  "streams": [
    {
      "url":   "http://192.168.1.10:8080/webdav/movies/Movie.Name.2024.mkv",
      "title": "Movie.Name.2024.mkv",
      "name":  "AltMount"
    }
  ],
  "_queue_item_id": 42,
  "_queue_status":  "completed"
}
```

| Field | Description |
|-------|-------------|
| `streams[].url` | Direct HTTP URL to the media file, playable by Stremio |
| `streams[].title` | Filename shown in Stremio |
| `streams[].name` | Source label shown in Stremio (`"AltMount"`) |
| `_queue_item_id` | Internal queue ID (useful for debugging or manual follow-up) |
| `_queue_status` | Final queue status at the time of the response |

**408 Request Timeout** — the download did not complete within the timeout:

```json
{
  "success": false,
  "error": {
    "code":    "REQUEST_TIMEOUT",
    "message": "Download did not complete within the timeout period",
    "details": "queue_item_id: 42"
  }
}
```

Use `_queue_item_id` / `queue_item_id` from the error details to check progress via the queue API or retry later.

**400 Bad Request** — a multi-episode season pack was submitted without episode context:

```json
{
  "success": false,
  "error": {
    "code":    "BAD_REQUEST",
    "message": "Episode not specified",
    "details": "This release is a multi-episode pack; provide season and episode (or a Stremio id like tt1234567:1:5)."
  }
}
```

## Caching

To avoid re-downloading the same release, AltMount caches the stream URLs keyed by the NZB filename. If a second request arrives with the same filename within the TTL window, the cached streams are returned immediately.

Set `nzb_ttl_hours: 0` to cache forever (useful if your library is stable and disk space is not a concern).

## Example

```bash
# 1. Compute your download_key
DOWNLOAD_KEY=$(echo -n "YOUR_API_KEY" | sha256sum | awk '{print $1}')

# 2. Submit the NZB and get stream URLs
curl -s -X POST "http://localhost:8080/api/nzb/streams" \
  -F "download_key=${DOWNLOAD_KEY}" \
  -F "file=@/path/to/release.nzb" \
  -F "category=movies" \
  -F "timeout=300" | jq .
```

Example output:

```json
{
  "streams": [
    {
      "url":   "http://localhost:8080/webdav/movies/My.Movie.2024.mkv",
      "title": "My.Movie.2024.mkv",
      "name":  "AltMount"
    }
  ],
  "_queue_item_id": 7,
  "_queue_status":  "completed"
}
```

## Limitations

- **Synchronous long-poll**: The request blocks for up to `timeout` seconds (default 300 s). Stremio add-ons should set an appropriate HTTP timeout on their end.
- **408 on timeout**: If the download is still in progress when the timeout fires, AltMount returns 408. You can use the returned `queue_item_id` to poll the queue API and retry the streams request once the item completes.
- **Single endpoint**: There is no separate "check status" endpoint for the streams workflow; use the standard queue endpoints (`GET /api/queue/:id`) for manual follow-up.
