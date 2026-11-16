---
title: Peer Network
description: Optionally share release health and finished imports with other AltMount instances.
keywords: [altmount, peer network, peernet, sharing, privacy]
---

# Peer Network

The peer network lets AltMount instances help each other. An instance that has imported a release and found it healthy can tell others, so they can pick healthy releases, shorten their own health checks, or reuse a finished import. Media content is never transferred; it always comes from your own Usenet providers. Everything received from a peer is advisory and verified locally before use.

The feature is **off by default**. While it is off nothing listens, nothing connects and no key is generated. All settings below apply live; no restart is needed.

## Configuration

```yaml
peernet:
  enabled: false
  accept_incoming: false
  listen_port: 8470
  advertise_address: ''
  open_network:
    level: 0
    peers: []
  sharing:
    excluded_categories: []
    excluded_indexers: []
  min_corroboration: 3
```

- **`enabled`** — master switch.
- **`accept_incoming`** — run the peer listener on `listen_port` so others can connect to you. It is not needed at any level, because a node can publish and serve over connections it opened itself. It is needed to host a private network and helps the open network.
- **`listen_port`** — 1024-65535. A reverse proxy in front of this port must pass TCP through rather than terminate TLS.
- **`advertise_address`** — optional `host:port` that other peers should use to reach you. A private or LAN address is only announced on private networks that allow private addresses; the open network never uses one, and a loopback address is never usable. The settings screen warns when the address cannot be used.
- **`open_network.level`** — exposure level on the open network (see below).
- **`open_network.peers`** — peer addresses you enter, as `host:port` with no scheme, userinfo or path (at most 64). Loopback addresses (`127.0.0.1`, `localhost`) are rejected because they can never work; private (LAN) addresses are saved but never dialled on the open network, and the list marks them with a warning. The open network has no shipped peers; a fresh install knows nobody until you add one.
- **`sharing.excluded_categories` / `sharing.excluded_indexers`** — releases in these categories or from these indexers are never published. Matching ignores case and surrounding spaces.
- **`min_corroboration`** — how many distinct nodes must attest a release before it is shown as healthy on the open network (default 3, minimum 1). Nodes in the same network range count as one (see below). An explicit value you saved earlier is kept.

Private networks (hosted or joined), their invites, certificates and their per-network levels are managed in the UI and stored in the database, not in this file.

## Private networks

A private network is invite-only. The host creates it, gets a host address (new networks start with the advertise address, when it is usable on the network; change it any time) and creates invite links; each link works once. Members join by pasting the link.

- **Private addresses.** The per-network "Allow private addresses" switch lets peers on LAN (private) addresses connect. **Loopback is never allowed**, even with this switch on, so a host address such as `127.0.0.1:8470` is rejected and an invite that names one cannot be redeemed.
- **Removing a member.** The host's removal takes effect immediately: the node is refused and no longer listed as a peer. A removed member learns of it as soon as the host refuses one of its requests (or at the latest at its next revocation-list refresh, about every 15 minutes); the network then shows "Removed by the host", stops all peer traffic and can only be left.

### Testing on one machine or a LAN

Because loopback is refused, two instances on one machine cannot reach each other through `127.0.0.1`. Use the machine's LAN IP instead: give each instance a different listen port, set the host's advertise address (or host address) to `<LAN-IP>:<port>`, and turn on "Allow private addresses" on the private network on **both** nodes (the member sets it when joining).

## Exposure levels

| Level | Name | Receives | Publishes | Serves |
|---|---|---|---|---|
| 0 | Off | nothing | nothing | nothing |
| 1 | Receive only | release records, health attestations, bundles | nothing | nothing |
| 2 | Share metadata | same as 1 | release records and health attestations for your own healthy imports | nothing |
| 3 | Share bundles | same as 1 | same as 2 | bundles, on request |

Only imports that completed and passed a local health check are published, and exclusions are applied before anything is signed.

## What levels 1 and 2 do

Levels 1 and 2 exchange small signed records only. No article IDs, no bundles and no media content leave your instance at these levels.

### What is received (levels 1 and 2)

- **Release records**: a title, size, file count, post date and, when the publisher knew them, media IDs for a release.
- **Health attestations**: "this file of this release was checked healthy (or not) at this time, against these provider backbones".

Records are stored locally with an expiry (30 days unless announced again) and bounded on an open network where anyone can create identities for free: at most 50,000 records per network, at most 5,000 from one publisher and at most 20,000 delivered by one peer. A peer that fills its share only replaces its own oldest records, and when the network is full the peer holding the most records gives up its oldest first. Your own records are exempt from the publisher and peer quotas and are never evicted to make room for received ones. All peers together are limited to about 200 new records per second, and no single peer may use more than a quarter of that rate, so a few noisy peers cannot starve the others. A peer that sends invalid records or keeps restarting its sequence is backed off for a while; records it already delivered are kept, because they may be honest publishers' records it relayed. A peer that merely re-sends records you already hold is not punished, however long that takes. A pusher's address is only backed off after several different node IDs behind it misbehaved, and never for private, loopback or link-local addresses. A record is only ever a hint: it is never treated as an import, and nothing in it can write a health status, mark a file corrupted, trigger a repair or delete anything.

### What is published (level 2)

For each of your own imports that has passed a local health check, a release record and an attestation for the checked file. Publishing waits until the check has run, applies your category and indexer exclusions before anything is signed, and skips files that have known holes or were repaired from PAR2 data. An attestation is only published when at least one of your providers has a recognised backbone name. The backbone is the provider's storage group when you set one, and otherwise it is taken automatically from the provider's host by the same known-hosts list the provider form uses (an exact host, or a regional host of a provider whose listed hosts all share one backbone); only the resulting backbone name is used, no host is sent anywhere. The Activity card says so when checks were skipped for lack of a backbone; the release record is announced again before it expires, but only while the file is still in your library and its current local health status is healthy. A later failed check or a deleted file stops the re-announcement.

Release names are cleaned before they are published: any password written into the NZB name (`Name{{password}}.nzb`, `/password:...`, `password=...`), the NZB file extension and the local queue prefix are removed, and a release whose name still contains `{{`, `}}` or the import's own password is not published (skip reason "Title contained a password").

Exclusions also apply to what you have already published. When you add an excluded category or indexer, the next upkeep pass (about every ten minutes) deletes your own records and publish state for matching releases. Matching uses the category and indexer remembered privately beside the publish state (never sent to anyone), so releases published by an older version, which did not remember them, are not matched. Copies other nodes already received cannot be recalled; they expire on their own.

### What is never shared

Provider hosts, usernames and passwords (only a recognised backbone name such as a well-known provider family), rclone settings, ARR or indexer names, local paths, download IDs, API keys, accounts, your health history, known holes, and anything about your local activity beyond the check time.

### What a peer can learn about this node

At level 2 a peer sees which releases you have imported and found healthy, when you checked them, which provider backbones you use, and your node ID and IP address. A node that only receives (level 1) publishes nothing and reveals nothing about its library.

### How network evidence affects health checks

Evidence from the network can only make a *routine* check of a file that is already healthy cheaper or later. It never decides a verdict, and a file's status is only ever set by your own checks.

- An attestation counts only when it is recent (under 7 days old), is for exactly the same file (matching is by identifiers derived from the article IDs, never by title) and comes from a node whose providers' backbones are all among yours.
- A healthy attestation also has to say how much of the file was checked. It counts only when that is at least 10 segments and at least 5% of the file's segments, which is what a normal scheduled check covers. Attestations that do not say (older ones) or that cover less never count as healthy evidence.
- **"Known" nodes.** On the open network a node counts only when your instance has actually connected to it and verified its identity at least 24 hours ago, and has reached it again within the last 7 days. Nodes learned only from other peers' lists are not known until you have connected to them yourself. A node also needs an address you verified it at: on the open network at most one node per network range (/24 for IPv4, /48 for IPv6) of that address is counted, so many identities behind one range still count as one.
- **Open network**: when enough distinct nodes agree (see `min_corroboration`), and each has been known to your instance for at least 24 hours, a routine check samples fewer segments (never fewer than ten, and never fewer than the first and last segments the checker always probes). The check still runs, and the next check is scheduled as usual. A check shortened this way is weaker than a normal one, so it is never published as evidence itself, and at most one shortened check happens between two normal checks of the same file.
- **Private network**: the host vouches for who is a member, not for what members say. One member's recent healthy attestation may postpone one routine check, to no later than the time the file would have been checked next anyway, and never twice in a row without a real check in between.
- **Never affected**: first checks of a new import, files with known holes or PAR2 patches, files that are degraded, corrupted or in repair, manual rechecks, repair re-validation, and installations with "check all segments" turned on.
- **Unhealthy evidence** can move a healthy file's next check earlier, but on the open network it needs the same number of distinct network ranges as healthy corroboration; a single unhealthy node neither removes the "healthy on the network" label nor blocks a shortened check that enough healthy nodes support, while enough unhealthy nodes do resolve it conservatively. On a private network one member is enough, as before. Moving a check earlier means a problem is noticed sooner; it never changes a status by itself, and such early checks come out of one shared budget of a few per hour, however many nodes or identities report. (This version does not publish unhealthy attestations, so this path only matters once other nodes do.)
- Every lookup is bounded by a short timeout and fails quietly: if the peer data cannot be read, the check simply runs as it would without the feature.

### `min_corroboration`

On the open network a release is shown as healthy on the network, and a check is shortened, only when at least this many distinct nodes, counted once per network range, attest it (default 3, minimum 1). Raising it makes the network's word count for less. It damps casual junk; it does not stop a determined attacker with many identities, which is why a shortened check still samples the file's start, end and body locally.

### Stremio labels and media-ID discovery

In Stremio, a stream served from your own library is labelled "Healthy on the peer network" when the exact release it belongs to is attested by enough nodes. The label text is fixed; nothing a peer sent is shown. Search results from your indexers are not labelled: they are NZB files that have not been downloaded yet, so their identifiers are not known, and AltMount does not guess by title.

Release records carry media IDs only when the publisher imported the release through Stremio, so discovery of releases by IMDb ID only covers Stremio-originated imports. In this version a release record carries no bundle, so it cannot be played or imported from the record alone and is not offered as a stream.

### Which library files can be shared

A file can be shared when your instance knows the whole release it came from, which means the release's NZB file list was kept at import time. Files imported before that was kept, or whose list was rebuilt from the surviving files by a metadata migration, still get a File ID but no Release ID, are skipped as "Release could not be identified", and are never guessed from partial data. The Activity card shows "N of M library files can be shared; the rest were imported before sharing data was kept", and `GET /peernet/status` reports it as `library` with `files`, `with_release_id`, `file_id_only` and `not_indexable`. Re-importing a release makes its files shareable.

A release without import history is titled from its NZB name, without a trailing media extension such as `.mkv`.

### Why old library files rarely have a live attestation

Files older than 30 days are checked about every 90 days, and an attestation is only useful for 7 days. Most of an old library therefore has no fresh evidence at any moment, and the main saving is on recently added files.

## Privacy

At levels 2 and 3 the network can see which releases this node has, tied to its node ID and IP address. At level 3, article IDs are shared too. At level 1 only the bundles you ask for are revealed, to the peer you ask. Peers see each other's IP address; use the outbound proxy settings if that matters to you.
