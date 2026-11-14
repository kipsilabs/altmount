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
    fast_path: false
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
- **`open_network.fast_path`** — default `false`. Use layouts offered by open-network peers to skip archive analysis when you import an NZB. The result is verified by sampling, not by full analysis, so it is off unless you turn it on, and even then it needs at least two publishers in different network ranges announcing the identical bundle. Layouts from your private networks need no switch. Playing a peer candidate in Stremio is unaffected by this switch.
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

- An attestation counts only when it is recent (under 7 days old), is for exactly the same file (matching is by the file's identifier derived from its article IDs, never by title, and whatever release the publisher filed the file under) and comes from a node whose providers' backbones are all among yours.
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

In Stremio, a stream served from your own library is labelled "Healthy on the peer network" when every video file of the release it belongs to is attested healthy by enough nodes (matched by file, so it works for older imports too). The label text is fixed; nothing a peer sent is shown. Search results from your indexers are not labelled: they are NZB files that have not been downloaded yet, so their identifiers are not known, and AltMount does not guess by title.

Release records carry media IDs only when the publisher imported the release through Stremio, so discovery of releases by IMDb ID only covers Stremio-originated imports. A release record only becomes playable when its publisher serves a bundle; see "Stremio peer candidates" below.

### Which library files can be shared

Every library file that was imported with the release's NZB kept has a Release ID taken from that NZB. Files imported before that (the older metadata format keeps the segments inside each file) or whose list was rebuilt by a metadata migration have a File ID but not the release's full segment list, and your NZB files are **not** read again to get one. Instead, AltMount identifies such a release **by the set of files it contains**: the files that came from one import (found through the source NZB name stored in each file's metadata, never by folder, because a folder can hold several imports and one import can span folders) are given one Release ID that is the hash of their File IDs. The step runs in the background from the index AltMount already keeps, so it costs no extra reads of your files, and it runs again shortly after files are added, removed or renamed.

What that means for you:

- Older imports are now shared like any other: a release record and, after a healthy check, an attestation. Records say which kind of Release ID they carry, so a receiver never mistakes one for the other.
- If a file is added to or removed from such a release later, its Release ID changes. The records published under the old ID are withdrawn from your own store and the new ID is announced after the next healthy check.
- A file that has an NZB-based Release ID never also gets a derived one.
- Two nodes that imported the same NZB and kept the same files derive the same Release ID. If they kept **different subsets** of the files, their Release IDs differ. Health evidence is still shared per file: an attestation is matched by File ID whatever Release ID it was published under, so both nodes benefit from each other's checks, but the two nodes do not share the release's identity. A release is also labelled "healthy on the peer network" only when every one of its video files has qualifying evidence.
- A file whose metadata names no source NZB cannot be grouped and stays unshared; so does an import with more than 2,000 files (it gets no release ID and is counted as too large).
- The same NZB imported twice into two folders is one release: duplicate files count once.

The Activity card shows, in plain language, how many library files can be shared, how many of those are older imports identified by their files, and why the others cannot be. `GET /peernet/status` reports this as `library` with `files`, `with_release_id`, `with_nzb_release_id`, `with_derived_release_id`, `file_id_only`, `not_indexable`, `no_source_nzb`, `group_too_large` and `pending`.

A release without import history is titled from its NZB name, without a trailing media extension such as `.mkv`.

### Why old library files rarely have a live attestation

Files older than 30 days are checked about every 90 days, and an attestation is only useful for 7 days. Most of an old library therefore has no fresh evidence at any moment, and the main saving is on recently added files.

## What level 3 does

At level 3 this node serves **finished imports on request**. When a release is published, AltMount builds a *bundle* from the import it already did: the segment list (the article IDs), the file layout, sizes, offsets, nested archive sources and the original file names. Another node that has your signed release record can ask you for that bundle. **Article IDs leave your node at this level.** Level 2 shares none.

A bundle is small (about 34 KB for a 1 GB release, a few MB for a very large one), compressed, and its hash is carried in your signed release record, so nobody can swap in a different one. Your node builds it again whenever it is asked (and keeps the last few in memory for ten minutes), and only serves bundles it announced itself and only to authenticated peers, within per-peer and node-wide rate and byte limits.

### What can and cannot be shared as a bundle

A release is shared as a bundle only when AltMount can prove that nothing private or unreproducible goes with it. A release that cannot be shared still publishes its normal record (without a bundle), and the status page counts the reasons. Not shareable:

- **rclone-encrypted** files and any other encryption type (the rclone password and salt are never shared);
- releases where a **PAR2 repair** patched any article locally (the patched bytes exist only on your node), and releases whose patch status is not known;
- releases with **known holes**, unresolved segments or inconsistent local metadata;
- **keyed releases** (encrypted RAR or 7-Zip archives) whose password did not come with the NZB's own content. AltMount records, for **new imports only**, whether an archive password came from a `<meta type="password">` that was already inside the NZB as it was received. A password typed into AltMount (the link `&p=` or the password field), spelled in the NZB's file name or in a subject line, found inside another release's archive, or supplied before this was recorded does not count, and the release is then not shared. When it does count, the bundle carries the archive key and IV (a receiver needs only those to play the release) and not the password text, which AltMount never stores.

### What receiving a bundle does

At level 1 and above, importing an NZB first asks the network for a bundle of exactly that release (the Release ID of the NZB's segment list), with a short time limit, and falls back to a normal import on any problem. The Stremio flow below imports a release the same way. A received bundle is untrusted. AltMount:

1. checks its hash against the signed record and recomputes every identifier from the segment lists;
2. validates the layout (ranges, offsets, sizes, names) and applies local limits;
3. checks that the articles exist on **your own providers** (a sample probe);
4. reads the start of the main media file through the same reader that will serve it (a wrong key or wrong segments fail here);
5. only then writes the files, marked as coming from the peer network. They are **playable immediately**.

A **full check** of every segment is then scheduled at high priority. Until it passes, the import is **not announced to the ARRs and is not published** to the network (no record, attestation or bundle).

**What Sonarr and Radarr see while it is pending.** An item they grabbed through the SABnzbd-compatible API stays in the *queue* as work in progress (status `Downloading`, 99 percent) and is not in the *history*, so they never import content that is not verified yet. When the full check passes, the item moves to the history as completed and the ARR imports it as usual. When it fails, or when the 7-day limit below is reached, it appears in the history as **failed** with a short message ("Peer-supplied release failed its full health check and was removed"), so the ARR blocklists the release and searches again. An item you hid from the ARRs yourself (skip ARR notification) stays hidden in every state. The Queue page of AltMount shows the item as completed from the start, because the files are playable at once.

**When a full check fails.** In every case the same single path runs and no ARR repair is ever attempted for the file: the queue item turns into a failed one, and every file of that import is removed (metadata, content index, library copy, health record and peer state), so a corrupt file does not stay in your library and the rest of the release cannot complete the import later. This holds for an item an ARR grabbed, for a release started from Stremio (which no ARR knows) and for an import hidden from the ARRs. Only files of a peer import that is in the pending state are ever removed; nothing else is touched. The publisher is **not trusted for bundle fetches for a while**: 30 minutes, doubling per repeated failure up to 6 hours, forgiven after a day or as soon as a later bundle from the same publisher passes its full check. This never affects record sync. It is mild on purpose: a failure can also mean your own providers lack articles.

**Stale imports.** A pending import is held for at most 7 days: after that it is failed exactly as above (the publisher is not blamed, since no check ran), and the status page shows how many are pending and how many waited too long. Turning the peer network off does not strand pending imports: the full check keeps running for them until they finish.

**Deleting and restarting.** Deleting a file removes its pending state; deleting the queue item of a pending import is allowed and the verification carries on without announcing it; if every file of an import is deleted the hold is lifted. After a restart AltMount reconciles the state: a write that a crash interrupted (a release only partly written) is rolled back completely, so it can never be announced, published or served; rows whose files no longer exist are dropped (a release with no files left is never announced); a hold nothing can release is lifted. A queue row of a release started from Stremio has no NZB: retrying, restarting or downloading the NZB of such an item is refused with a clear message, and a restart never tries to import it again. Renaming a file or folder keeps the pending state.

The release identity the bundle proved is kept for the imported files, so they can be matched to network evidence and re-shared like local imports. If you later delete a file of a release identified by its file set, its identity changes, exactly as for a local import.

Before anything is written the layout is also checked against the articles themselves and against the NZB:

- **No gaps.** A bundle file may not contain a hole; a release with known holes is not shareable and a received bundle with one is refused.
- **Coverage.** In a bundle for an NZB, every file (and every archive volume or PAR2 file) must be exactly one NZB file entry's segments, in NZB order; no segment is used twice and none is left out.
- **Sizes against your NZB.** When you import an NZB, each declared decoded segment size must fit the NZB's (encoded, a few percent larger) size.
- **Sampled article headers.** For each file and archive volume the first and last article and a few random ones (chosen locally, up to 24 in all) are read from your providers, and their yEnc headers must agree with the layout: decoded size, start offset, and total file size. A mismatch refuses the bundle and counts against the publisher. The cost is at most 24 article downloads (roughly 18 MB) per import.
- **Media structure.** Besides the start of each media file, its container index is read at several positions including the end: Matroska/WebM (EBML header, Segment size, and every SeekHead entry such as the Cues), MP4/MOV (the whole top-level box chain must end exactly at the end of the file, or, for fragmented files with thousands of boxes, a fragment box must end exactly there), MPEG-TS/M2TS (sync bytes on the 188, 192 or 204 byte grid at the start, quarters and end). Files muxed for streaming that have no Matroska SeekHead, or an unknown-size Segment, are accepted when a level-1 element opens the Segment, a known Segment size lands on the end of the file and the quarters and the end can be read; trailing free, skip or uuid boxes, a zero-sized last box, 64-bit box sizes and up to 64 KiB of zero padding in MP4 are accepted; other containers get full-length reads at the quarters and end. Every written media file passes this, not only the largest.
- **File types.** Only media files and small subtitle (`.srt .ass .sub .idx .sup`) and `.nfo` files are written (at most 40 media files and 50 sidecars, subtitles up to 16 MB, `.nfo` up to 1 MB). Files your *allowed file extensions* exclude are skipped like in a normal import. Playlists, links, queue files, scripts and executables (`.strm .nzb .nzbz .meta .url .lnk .m3u .m3u8 .pls .desktop .exe .sh ...`) make the whole bundle refused.
- **Folder name.** The release folder comes from the title with the media extension removed first; names that are empty, hidden (start with a dot), reserved by AltMount or equal to a category folder are refused, so a title can never point outside the category folder.
- **Offers.** Of several offers, the bundle announced identically by publishers in the most distinct network ranges is preferred; a publisher's own attestation never raises the rank of its own offer.

- **Plain layouts only from the open network.** A bundle offered through the open network is accepted only when every file is exactly one plain run of articles: archive volumes (nested layouts), encryption keys and multi-clip files are refused with `layout_not_allowed_open`, and media whose container has no structural check (anything but Matroska/WebM, MP4/MOV and MPEG-TS/M2TS) with `container_not_verifiable`. This is a stated limitation, not a bug: those layouts cannot be bound to the articles without reading archive headers, which is planned. Private networks still accept them.
- **Sampling.** Every run of articles is sampled: its first and last article and at least one random one in between (one per 500 for a media file, at least three) have their yEnc headers (part size, offset, total) compared with the layout, up to 600 articles and 512 MiB; a release with more archive volumes than that is refused (`too_many_tables`). A single movie file reads about 5 articles (4 MB), a 50-volume release 150 (115 MB), a 200-volume release 600 (461 MB). With your NZB at hand the first article's yEnc file name must also match the NZB entry it belongs to. The forced full check reads a further random sample of headers (1 percent of the articles, 20 to 500) and fails the import on a contradiction. It samples; it does not prove.
- **Media structure.** Besides the end of the file, the container is read at random positions inside the file (Matroska: three random 1 MiB windows must contain cluster data; MPEG-TS: eight random packet positions must be in sync; fragmented MP4 must end in a real `moof`+`mdat` pair or `mfra`). What remains unprovable: bytes inside MP4 `mdat`, inside a Matroska frame or between sampled positions.
- **Degraded means failed.** A peer release whose full check finds the media playable but with missing segments is failed and removed, never announced. When you supplied the NZB the queue item is not marked failed: it is imported normally from your NZB, once, so you get what a normal import would have produced. A release imported directly (Stremio peer play) is failed. A missing article alone does not count against the publisher (it can be propagation just after posting); only a contradiction between the layout and the articles' own headers does, and that layout is then ignored whoever offers it, for 7 days.
- **Corroboration** counts distinct /24 (IPv4) or /48 (IPv6) ranges of the address this node actually connected to when it last verified the publisher, never host names; with the open-network switch off, open-network publishers are not even contacted.

What this proves: the articles exist on your providers, sampled articles match the layout, and the media container is structurally sound at the places it was read. It does not prove that the content between the sampled points is the titled media; that is the same class of risk as a mislabelled NZB from an indexer.

### Stremio peer candidates

When the peer network is enabled and a network is at level 1 or above, the Stremio stream list for a movie (or an episode) can include releases other nodes announced for that IMDb ID with a bundle you can fetch from a peer you have verified, and that you do not already have. They are marked "🌐 Peer network • verified on play", with "Healthy on the peer network" when enough nodes attest it, and come after your library streams. At most 10 are listed, healthy first, from one short lookup that is cached for a few seconds and gives nothing on any error. The lookup runs while the indexer search runs and never delays the list by more than a fraction of a second after the search finished.

Playing one asks your node (not the player) to fetch the bundle, verify it as described above and write the files into the same folder a normal Stremio import of that category would use, then the player is redirected to the file; this typically takes a few seconds. The play link carries only the release ID. Only releases for which your node holds a signed record offering a fetchable bundle can be requested, the address dialled always comes from your verified peer table, concurrent plays of a release share one import, and plays are rate limited per key and overall (a few per minute). The IMDb ID, season and episode of the play request are kept for your own listing but marked unverified: they are never put into your own published records, because they come from the requesting player, not from anything proven. Stremio's play link can only be requested with your Stremio key.

### Limits

- Bundles: 8 MB compressed, 16 MB decompressed (about 340,000 segments of a typical release, roughly 250 GB), 500,000 segments and 10,000 files at most; a receiving node also refuses releases over 2 TiB or 2,000 files.
- Fetching: on demand only, never speculative; at most 3 attempts of 60 seconds per request, 60 attempts per hour overall, 12 per publisher, 2 downloads at once.
- The health check must be enabled to receive bundles, because the full check must be able to run.
- A bundle is for the exact release: two NZBs of the same movie with different article sets are different releases.

## Privacy

At levels 2 and 3 the network can see which releases this node has, tied to its node ID and IP address. At level 3, article IDs are shared too. At level 1 only the bundles you ask for are revealed, to the peer you ask. Peers see each other's IP address; use the outbound proxy settings if that matters to you.
