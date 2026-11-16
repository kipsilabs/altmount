# Peer network design

Date: 2026-11-12
Status: draft, awaiting approval
Working name: "peer network" (config key `peernet`; the existing `network` key is already used for proxy settings)

## Intent

Let AltMount instances help each other. An instance that has already imported a
release and found it healthy can tell other instances, so they can:

- know which releases are healthy before picking one,
- shorten or skip their own health checks when someone already checked the same
  articles,
- import a release near-instantly by reusing the finished import (segment list
  plus archive layout) instead of redoing the expensive analysis.

The network is open: anyone running AltMount can join. It must also support
private networks where one AltMount is the host and decides who is a member.

The owner's overriding concern is security: joining the network must never give
other people access to anything else in the instance, and nothing received from
a peer may be able to inject code or damage the library.

## Goals

1. Feature is **off by default**. Nothing listens, nothing connects, no key is
   generated until the user enables it.
2. Three exposure levels the user picks per network (see "Exposure levels").
3. An open public network and host-run private networks, using one protocol.
4. Peer data is advisory. Everything is verified locally before it is used.
5. Shared bundles are small (compressed) and tamper-evident.
6. A new node catches up on history; it does not only see new announcements.
7. A node can take part without opening an inbound port.

## Non-goals

- Transferring media content. Content always comes from the user's own Usenet
  providers.
- Anonymity. Peers see each other's IP address. Users who need to hide it use
  the existing outbound proxy settings.
- Solving fake identities in the open network. They are damped, not prevented.
- Replacing indexers as a product promise. The network may make an indexer
  unnecessary for a given release, but that is a side effect.

## Anti-goals (these make the feature a failure even if it works)

- A peer, or a bug reachable by a peer, can read provider credentials, ARR API
  keys, user accounts, the rclone password, local paths, or any API route.
- Data from a peer can, on its own, mark a file corrupted, trigger a repair or
  re-download, blocklist a release, or delete anything.
- A received file can cause code execution, path traversal, or unbounded memory
  or disk use.
- Enabling the feature silently starts sharing. Sharing always requires the user
  to choose level 2 or 3.

## Exposure levels

Chosen per network. Level 0 is the default.

| Level | Name | Receives | Publishes | Serves |
|---|---|---|---|---|
| 0 | Off | nothing | nothing | nothing |
| 1 | Receive only | release records, health attestations, bundles | nothing | nothing |
| 2 | Share metadata | same as 1 | release records and health attestations for its own healthy imports | nothing |
| 3 | Share bundles | same as 1 | same as 2 | bundles, on request |

"Accept incoming connections" is a separate switch, default off. It is not
needed at any level, because a node can publish and serve over connections it
opened itself. It is needed to host a private network and it helps the open
network. (Design call, not something the owner specified.)

At level 2 no article IDs leave the instance. At level 3 they do.

## What is shared

Three things. All are plain data in protobuf, never executable, never used as a
path.

### Identifiers (new, needed by everything else)

- **Release ID**: SHA-256 over the release's real segment message-IDs in
  canonical order (file order as in the NZB, segments by number; gap
  placeholders excluded).
- **File ID**: SHA-256 over one file's ordered real segment message-IDs,
  including nested and outer archive sources.

Both are computed once at import and stored. No such identifier exists today.

### Release record (small, gossiped)

Release ID, bundle ID (hash of the bundle, if the publisher serves one), title,
total size, file count, post date, optional media IDs (IMDb, TMDB, TVDB, season,
episode), publisher node ID, timestamp, signature. Hard cap 2 KB.

### Health attestation (small, gossiped)

Release ID, File ID, checked-at time, total segments, segments checked, result
(healthy, or missing count), whether the media header was verified, the set of
provider backbones the check ran against, signature.

- Backbones come from the existing per-provider `storage_group` field,
  normalised (trimmed, lower-cased) and accepted only when they match the known
  backbone list AltMount already fetches for the provider settings. Free-text
  labels outside that list are not used. A provider with no recognised backbone
  contributes nothing, and an instance with none does not publish attestations.
- A peer can lie about its backbones. That is acceptable because of the trust
  rules below.
- Files made healthy by local PAR2 patches are never attested.
- Attestations older than 7 days are ignored. (Design call.)

### Bundle (fetched on demand, level 3 only)

A sanitised finished import for one release:

- the segment list (what the `.nzbz` store holds),
- per-file layout: sizes, offsets, segment references, nested archive sources,
  clip boundaries, original filename, post date, PAR2 file references,
- the NZB password, and archive AES key and IV, only when they derive from a
  password that is already in the NZB. This is a rule the publisher applies; the
  receiver cannot check it and does not rely on it.

The bundle has its own schema with a version field, independent of the on-disk
metadata format. It is converted to and from the local format. A bundle with an
unknown major version is rejected.

Importing a bundle never runs an archive parser or a key derivation. The layout
is already resolved, so the key and IV are used only to decrypt the stream; a
wrong key produces garbage and fails the media header probe. The password is an
opaque, length-capped string that is stored and not used at bundle import. It
is only used if the user later reprocesses the release, which is the normal
import path.

Canonical protobuf, zstd-compressed at a high level. The Bundle ID is the
SHA-256 of the canonical uncompressed bytes and is carried in the signed release
record, so a peer cannot substitute a different bundle.

### Never shared

rclone password and salt, anything from an rclone-encrypted file, provider hosts
and credentials (only backbone labels), ARR instance names, indexer names, local
paths and store paths, download IDs, API keys, users, health status history,
known holes, timestamps of local activity beyond the check time.

### What is eligible to publish

Only imports that completed and passed a local health check. Imports that came
from a peer bundle are published only after their full local check passes (see
trust rule 2). The user can exclude categories and exclude indexers ("never
share releases from this indexer"). Exclusions are applied before anything is
signed, so an excluded release never produces a record or attestation.

Stated consequence, shown to the user when they pick a level: at levels 2 and 3
the network can see which releases this node has, tied to its node ID and IP
address. Level 1 reveals only the bundles it asks for, to the peer it asks.

Matching is exact. Two NZBs of what a person would call the same release, with
different article sets, are different releases. Candidates are found by media
ID; health results and bundles apply only to the exact Release and File IDs. (Design call: the indexer exclusion exists because redistributing an
indexer's NZBs can break its terms.)

## Trust rules for received data

These are the core of the security model.

1. **Release records are hints.** They add candidates and affect ordering. A
   record is never treated as an import.
2. **A bundle is verified before it is accepted**, in every network, private
   ones included:
   - size limits, hash equals the announced Bundle ID, segment list hashes to
     the Release ID;
   - structural validation of the layout: references in range, offsets inside
     their segments, extents contiguous and summing to the declared file size,
     and a segment count plausible for the declared total size;
   - the existing local release probe (STAT sample against the user's own
     providers);
   - the existing media header probe through the layout.

   It is then written as a local import in pending state, marked as coming from
   the peer network, and may be played. A **full** check of every segment is
   scheduled at high priority. Until that passes, the import is not announced
   to the ARRs and is not published to the network.

   What this proves: the articles exist on the user's providers and the start
   of the file is the claimed kind of media. It does not prove the whole file
   is the titled content. That is the same risk as a mislabelled NZB from an
   indexer, and the user chose the release by its title either way.
3. **Healthy attestations** count only when fresh and when the attester's
   backbones are a subset of the receiver's.
   - Open network: they reduce the local sample to its floor. They never skip a
     check and never lengthen the re-check interval.
   - Private network: they may skip one scheduled check, never for longer than
     the attestation's 7-day validity. The host vouches for who is a member,
     not for what members say.
4. **Unhealthy attestations** affect candidate ranking and may move a local
   check forward. They never write a status. Peer-triggered re-checks come out
   of one **global** hourly budget, not a per-publisher one, so creating many
   identities does not raise it.
5. **Corroboration** in the open network: a release is shown as "healthy on the
   network" only when at least two distinct nodes attest it (configurable), and
   a node counts only once the local instance has known it for 24 hours. This
   damps casual junk; it does not stop a determined attacker with many
   identities. Rule 2 is the real protection.
6. **Signatures cover everything**: a type tag, the network ID, every field,
   the publisher and the creation time. A record is rejected if its network ID
   is not the one it arrived in, if its creation time is more than 5 minutes in
   the future, or if it is past its expiry. An attestation's checked-at time
   may not be later than its creation time.

## Isolation rules

- The peer protocol has its **own listener on its own port**, started only when
  "accept incoming connections" is on. It serves only peer routes. It is not the
  main web server and accepts none of its credentials.
- The peer code lives in its own package and receives **narrow interfaces**
  only: list publishable releases, read a bundle, verify a bundle, deliver a
  health hint. It is not handed the config manager, user or provider data, or
  ARR clients.
- Peer strings (titles, filenames, subjects) are length-capped, stripped of
  control characters and path separators, and pass through the existing
  filename sanitisers before any use. Virtual paths are always derived locally.
- No peer input is parsed as XML, used in a template, used to build a command,
  or interpolated into SQL. NZB XML is only generated outward, with escaping.
- The UI renders peer strings as text only.
- **Hard limits**: record 2 KB; bundle 8 MB compressed and 64 MB decompressed,
  enforced while streaming; 500k segments and 10k files per bundle; per-peer
  request rates; a storage cap for the index with oldest-first eviction; a quota
  per publisher; records expire after 30 days unless re-announced. (Numbers are
  design calls; the builder may tune them.)
- **Listener limits**: caps on total and per-IP connections, timeouts on
  handshake, read and write, capped header and body sizes, a cap on records per
  push, and a concurrency cap per peer.
- **Outbound connections** to peer addresses:
  - refuse loopback, private, link-local and the instance's own addresses. The
    check is made on the address actually being connected to, inside the
    dialler, so a DNS answer cannot change between check and connect;
  - never follow redirects;
  - send no request until the other side has completed the mutual TLS handshake
    as the expected node ID, so a non-peer service only ever sees a TLS hello;
  - allow private addresses only as a per-network setting the user turns on for
    a private network, applied only to addresses learned inside that network;
    loopback is never allowed, whatever the setting;
  - go through the existing proxy settings. Through a proxy the resolved
    address cannot be checked locally, so literal private addresses are refused
    and the node-ID check is the remaining protection.
- Decoders get fuzz tests.

## Identity and transport

- Each node has an Ed25519 key, generated when the feature is first enabled and
  stored in its own file with owner-only permissions. Node ID is the SHA-256 of
  the public key. The key is never returned by the API.
- Connections are TLS 1.3 with mutual authentication, using certificates derived
  from the node keys and verified by pinning the key to the node ID. This uses
  the Go standard library; there is no hand-written handshake.
- On top of that, plain HTTP request and response. Every interaction is started
  by the client:
  - pull records since a cursor (this is how history is caught up). The cursor
    is a sequence number local to the peer being asked, kept per peer by the
    puller. It is never a timestamp, so backdated records cannot slip behind it,
  - push own signed records,
  - fetch a bundle by ID,
  - exchange peer addresses.
- Records carry their publisher's signature, so they can be relayed by nodes
  that did not create them.

Design calls here, with the alternatives considered:

- **Pull over HTTP rather than a push gossip mesh over WebSockets.** It needs no
  new networking dependency, the puller controls its own rate, outbound-only
  nodes work naturally, and catch-up is built in. The cost is delay equal to the
  poll interval.
- **Standard-library mutual TLS rather than a full peer-to-peer library.** A
  large library would bring hardened gossip and NAT traversal, but also a big
  dependency tree and much more exposed surface than this feature needs.
- Consequence: a reverse proxy in front of the peer port must pass TCP through
  rather than terminate TLS.

## Networks

A network is identified by a root public key. Everything stored is keyed by
network, and records never cross networks. A node may join more than one
network, each with its own exposure level.

- **Open network**: any node is admitted. It has no root key and no owner; its
  network ID is a well-known constant. Peers are found two ways only: a list of
  addresses the user enters, and peer exchange once connected. The shipped list
  is empty, so a fresh install knows nobody until the user adds one address.
  Known peers are saved so restarts do not start from zero. (Owner's decision:
  no public DHT and no shipped bootstrap nodes. Discovery sources are kept
  behind one interface so another source can be added later.)
- **Private network**: one AltMount is the host and holds the root key.
  - The host creates invite codes: 128 bits of randomness, single use,
    expiring. The invite link also carries the host's address and key, so the
    joiner knows exactly which host it is talking to.
  - A joiner presents an invite to the host and receives a membership
    certificate bound to the joiner's node ID: node ID, network, role, expiry,
    signed by the host. Redeeming is rate-limited per address and overall, and
    codes are compared in constant time.
  - Certificates last 48 hours and are renewed automatically by the host.
    Removing a member means the host stops renewing and adds the node to a
    signed revocation list, which members fetch whenever they contact the host.
    A member that was offline learns of a removal at the latest when the
    removed node's certificate expires.
  - Members check each other's certificates on every connection.
  - Members always connect to the host. They may also connect to each other if
    they accept incoming connections. If the host is offline, members with
    valid certificates keep working among themselves until expiry.
  - The network ID is derived from the host's key, so a network name cannot be
    taken over by someone else.

## Where it plugs into AltMount

- Release and File IDs: computed in the import processor where the segment list
  is in hand, stored with the import; a backfill for existing imports.
- Publishing: from the health worker's healthy result path.
- Health hints: consulted in the health checker's prepare step, before sampling.
- Bundle import: a fast path at the start of NZB processing, before the
  first-segment warm-up and release probe.
- Release candidates: the Stremio search and cached-release paths, keyed by
  IMDb ID plus season and episode.
- Storage: new tables via the existing migration mechanism (both SQLite and
  Postgres).
- Config: a new `peernet` section following the existing pattern.
- Lifecycle: a background service with Start and Stop like the existing workers,
  reacting to config changes.
- Frontend: a settings section to enable the feature, pick levels (with the
  stated privacy consequence of each), join or host a private network, create
  invites, and see peers and statistics. From phase 2, Stremio stream labels
  mark network-healthy candidates. From phase 3, imports that came from the
  peer network carry a badge in the queue and health views until their full
  check passes.

## Phases

Each phase gets its own implementation plan and is useful alone.

1. **Foundation**: Release and File IDs with backfill, config and exposure
   levels, node identity, transport and listener, peer discovery, private
   networks with invites and certificates, isolation rules, settings UI and
   status. No release data flows yet.
2. **Metadata sharing** (levels 1 and 2): release records and health
   attestations, pull and push, catch-up, limits and quotas, health-check
   shortening, network-healthy candidates in Stremio.
3. **Bundles** (level 3): build, serve, fetch, verify, and the instant-import
   path.

## Left to the builder

Port number, poll intervals, exact limits, table layout, protobuf field layout,
zstd level and whether a dictionary is worth it, UI layout, how the backfill is
scheduled.

## Related, out of scope

The post-import health check re-tests segments the import probe passed seconds
earlier. Suppressing that duplicate is a local improvement independent of this
feature.

## Open questions for the owner

None. The bootstrap question is settled: user-entered addresses plus peer
exchange, no shipped nodes, no DHT.

## Changes made during phase 2

Decisions taken while building phase 2 that differ from, or add to, the design
above.

- **Unhealthy attestations are not published.** Only healthy checks are
  attested. The consumer still understands unhealthy attestations (they may
  move a check forward from a shared budget), but nothing publishes them yet.
- **Reduce-to-floor needs corroboration and minimum coverage.** On the open
  network a check is cut to its floor only with enough distinct known
  publishers (default 3, see "Later changes"), and a healthy attestation counts only when it
  reports its coverage: at least max(10 segments, 5% of the file's segments).
  Unknown coverage (0/0, rows stored before the columns existed) never counts.
  The floor itself is max(10 segments, 2%), so a normal 5% check qualifies and
  a floor-level one does not.
- **One reduced check between normal checks.** A file whose check was
  shortened is not shortened again until a normal check has run.
- **Reduced checks are not attested.** A check shortened by network evidence
  is never published as evidence, so nodes cannot cite each other's floors.
- **"Known" publisher** means: connected to and identity-verified by this node
  at least 24 hours ago, and reached again within the last 7 days. Peers
  learned only through peer exchange are never known. The standing is looked up
  for all candidate publishers in one query, so fresh identities cannot use up
  the per-call lookup bound ahead of known ones.
- **Storage and rate limits.** A per-relayer cap (20,000 records delivered by
  one peer) next to the per-publisher cap (5,000); a node-wide ingest rate
  (about 200 new records per second); a network cap of 50,000 records. Own
  records are exempt from the quotas and are never evicted for received ones.
- **Small-order keys are rejected** when verifying peer and record keys.
- **Level 3 is disabled in the settings UI** until phase 3 delivers bundles.
- **Stremio labelling** applies only to library or cached streams matched by
  their exact Release ID. Peer release records are not offered as candidates
  until bundles exist, because a record alone cannot be played or imported.
- **Media IDs only exist for Stremio-originated imports**, so discovery by
  IMDb ID covers those imports only.
- **Passwords in names.** Passwords embedded in NZB names are stripped before
  publishing, and a release whose title still contains its password is not
  published (skip reason `title_contains_password`). The password itself is
  only compared against titles; the release-information code sees a narrowed
  four-field view of file metadata, never the full record.

### Later changes (review round)

- **No purge on hostility.** A peer that sends hostile records is backed off,
  and nothing it delivered is deleted: records are attributed to the relayer
  that delivered them first, so a purge would let a hostile relayer delete
  honest publishers' records (which honest relayers never re-send once cursors
  have moved on). The per-relayer cap and fair eviction bound a hostile
  relayer's footprint. A superseding record keeps the original relayer's
  attribution; only an unattributed row changes hands, and then the new
  relayer's cap applies.
- **Stale pages are not a strike.** A page that stores nothing new but moves
  the cursor is an honest peer whose records we hold; the round budget and the
  backlog flag bound the work. A true replay loop does not advance the cursor
  and is still backed off.
- **Fair ingest.** Each relayer may use at most a quarter of the node-wide
  ingest rate (and half its burst), with the node-wide bucket on top.
- **IP back-off needs distinct nodes.** An address is backed off only after
  hostile strikes from at least three distinct node IDs inside ten minutes, and
  never for private, loopback or link-local addresses. Concurrent pushes per
  address default to 4.
- **Corroboration per network prefix.** On the open network at most one
  corroborating publisher per address prefix (/24 IPv4, /48 IPv6) of the
  address this node verified it at counts; a known publisher without a
  verified address counts as nothing. The default minimum rises from 2 to 3
  (a value saved explicitly is kept).
- **Unhealthy evidence needs the same threshold.** On the open network, the
  "healthy on the network" label, the shortened check and the early recheck
  all treat unhealthy evidence as evidence only when it reaches the same
  number of distinct prefixes. Private networks are unchanged (one member).
- **Patch status can be unknown.** `CheckStats.PatchKnown` says the PAR2 patch
  summary was computed; without it a healthy notice publishes the release
  record but no attestation (skip reason `patch_status_unknown`). The patch
  summary is switched on before the observer is installed. A check shortened
  by peer advice, or one with unknown patch status, reaches the publisher as an
  explicit "release record only" input (`CheckResult.AttestationSkip`) instead
  of posing as a patched file.
- **Origin lookup follows what the importer writes.** Multi-file and archive
  imports record the release folder as the history path, single-file imports
  the file. The lookup tries the file path and its ancestor folders (nearest
  first, newest row wins, bounded) in one indexed `IN` query.
- **Release IDs need the whole release.** Metadata from before the NZB store
  existed (inline segments) and stores rebuilt by the metadata migration do not
  hold the release's full file list; those files keep a File ID, get no Release
  ID and are never guessed. Status reports `library.files`,
  `library.with_release_id`, `library.file_id_only` and `library.not_indexable`,
  and the Activity card shows one line about it.
- **Backbone from the provider host.** With no `storage_group` the backbone is
  taken from the provider's host by the same host mapping the provider form
  uses (exact host, or a registrable domain whose listed hosts all agree). An
  explicit group wins. The Activity card hints at the setting when attestations
  were skipped for lack of a backbone.
- **Host address default.** Creating a private network sets its host address to
  the advertise address when the network can use it (not loopback; private only
  when the network allows private addresses).
- **Smaller fixes.** Retracting an excluded release deletes all its own rows,
  in batches; an exclusion entry named "unknown" matches no indexer, like the
  publisher; the fallback title drops a trailing media extension; the
  consumers' node-ID waiter makes a final check under its lock.
