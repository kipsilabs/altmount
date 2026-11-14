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
- **Hard limits**: record 2 KB; bundle 8 MB compressed and 16 MB decompressed (lowered from 64 MB in the second hardening round),
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

## Phase 3: bundle format

Implemented by `internal/peernet/bundle`: a pure package (no network, database
or filesystem). Schema version 1; an unknown major version is rejected.

### Encoding

Canonical protobuf wire format written by hand. Fields appear in ascending
number, repeated fields are consecutive, optional fields are omitted when
empty, varints are minimal. The decoder is strict (no unknown or duplicate
fields, no out-of-order fields) and finally re-encodes what it decoded: any
difference from the received bytes is a rejection, so a bundle has exactly one
byte representation. **Bundle ID** = SHA-256 of the canonical uncompressed
bytes. The wire form is zstd at its highest level (single frame, 8 MB window,
CRC). No dictionary: on synthetic data with realistic random message-IDs a
trained 84 KB dictionary saved 0.1 to 0.3 percent. Sizes, offsets and
references are run-length coded in the schema instead, so the layout costs a
few bytes per file and the message-IDs are essentially all of the payload.

Typical size (random 20 to 31 character IDs, 716,800-byte segments, one file):
1 GB is about 1,500 segments, 64 KB canonical, 34 KB on the wire; 50 GB is about
75,000 segments, 3.2 MB canonical, 1.7 MB on the wire. The 8 MB wire cap is
reached at roughly 350,000 segments (about 2.5 TB of 716,800-byte articles),
below the 500,000 segment cap.

### Schema (all integers are unsigned varints unless noted)

| Message | Field | Content |
|---|---|---|
| Bundle | 1 version | must be 1 |
| | 2 scheme | 1 = nzb, 2 = files |
| | 3 release_id | 32 bytes |
| | 4 title | sanitised, at most 256 bytes |
| | 5 posted | Unix seconds of the release post |
| | 6 total_size | sum of the file sizes |
| | 7 password | optional opaque string from the NZB, at most 256 bytes |
| | 8 pool | repeated canonical message-IDs (no angle brackets) |
| | 9 release_files | nzb scheme only: packed counts, one per NZB file in NZB order |
| | 10 files | repeated File, sorted by File ID ascending |
| File | 1 name | sanitised base name |
| | 2 size | bytes, at least 1 |
| | 3 date | optional release date |
| | 4 segments | segment table (absent for nested files) |
| | 5, 6 aes_key, aes_iv | together, directly encrypted file |
| | 7 outer | repeated OuterSource |
| | 8 extents | packed triples (outer index, inner offset, inner length) |
| | 9 clips | packed pairs (byte length, zigzag 90 kHz delta) |
| | 10 par2 | repeated Par2File |
| | 11 file_id | 32 bytes, recomputed by Verify |
| OuterSource | 1 segments, 2 aes_key, 3 aes_iv, 4 volume_size | one outer archive volume; the key triple only when encrypted |
| Par2File | 1 name, 2 size, 3 segments | no gaps allowed |
| Segment table | 1 count | number of positions |
| | 2 refs | packed (base, count) runs; base is pool index + 1, 0 means gap |
| | 3 sizes | packed (decoded size, count) runs |
| | 4 partials | packed (index delta, start, end) for segments not used in full |

A gap is a synthetic placeholder for an article the NZB never listed. It has no
message-ID in the bundle; the receiver derives a local placeholder id.

### Identity: two ID schemes, both covered by the Bundle ID

- **nzb**: Release ID is the existing Release ID over the full ordered segment
  list. The pool holds that list in NZB order (including NZB files that were not
  imported) and `release_files` keeps the per-file boundaries the hash needs.
  Files refer to pool entries, so message-IDs are stored once.
- **files**: Release ID is `contentid.ReleaseIDFromFiles` over the bundle's File
  IDs (domain `altmount-peernet/release-files/v1`, IDs sorted ascending
  bytewise). The pool holds each message-ID once, in first-use order over the
  sorted files (main segments, outer sources, PAR2).

In both schemes every file's File ID is recomputed with the existing File ID
function from the segments the bundle describes (main segments plus each
extent's outer segments). `Verify(wire, bundleID, releaseID)` runs: size limits,
bounded decompression, hash equals the announced Bundle ID, strict decode and
structural validation, File IDs, then the Release ID for the declared scheme
must equal both the embedded and the announced one. A bundle presented with a
Release ID of the other scheme, or with a trimmed segment list, fails.

### Limits (exported constants)

8 MB compressed, 16 MB decompressed (was 64 MB) (enforced while streaming, 8 MB zstd
window, single-threaded decoder, no goroutines left behind), 500,000 segments
(pool, and positions over all tables), 10,000 files, 50,000 NZB file entries,
message-ID 248 bytes, file name 255 bytes, title 256 bytes, password 256 bytes,
10,000 outer sources per file, 100,000 extents per bundle, 100,000 clips per
file, 1,000 PAR2 files per file, segment 8 MiB, file and bundle 2^50 bytes,
nesting depth 1 (the schema cannot express a cycle or deeper nesting).

### Reader assumptions and the checks that uphold them (summary)

The full table with reader file and line references is in the comment at the top
of `validate.go`.

- A segment's usable range is `[start, end]` with `0 <= start <= end < size`,
  size at most 8 MiB (readers drop empty ranges and allocate `end + 1` bytes for
  gaps).
- Placeholder and gap handling is explicit; a pool ID with the placeholder prefix
  is refused. Message-IDs reach NNTP commands, so they are printable ASCII
  without spaces or brackets.
- File size is at least 1. An unencrypted file's segments cover exactly its
  size; an AES file's cover the padded size (at most 15 bytes more).
- AES keys are 16, 24 or 32 bytes and IVs 16 bytes, both present or both absent.
- Nested files have no inline segments or file-level key; extents lie inside the
  encrypted volume size (or the plain outer coverage), do not overlap, are at
  least 1 byte, and add up to the file size; every outer source is used; an
  encrypted outer volume is covered by its padded size.
- Clip lengths are positive multiples of 192, add up to the file size, and
  deltas are bounded.
- Names are single path components that are fixed points of the sanitiser,
  unique ignoring case. Sums use checked arithmetic.

### What is shared, dropped, or makes a release not shareable

Shared: file name (sanitised base name), size, segments (message-ID, decoded
size, usable range), nested outer sources and extents, clip boundaries, PAR2
file references, per-file release date, post date, title, optional NZB password,
and AES key and IV only when the publisher sets `PasswordFromNZB` for the file.

Dropped: source and store paths, store reference, download ID, rclone password
and salt, known holes and the provider fingerprint, status, created and modified
times, indexer, category and ARR names, provider data.

Not shareable (the builder returns a typed error for the whole release): any
rclone-encrypted file, any other encryption type, a file carrying key material
whose password is not from the NZB, a file healed by local PAR2 patches, a file
whose segments are not resolved or whose local metadata is inconsistent.

### Conversion

`FromLocal` builds a bundle from resolved per-file metadata; `ToLocal` returns
per-file metadata messages in the inline (v1) layout using shared outer sources.
Readers need no `.nzbz` store entry for such files. Names are sanitised base
names; the caller derives virtual paths locally. Placeholder ids and timestamps
are local. The nzb scheme also returns the per-file segment list so a later step
can compute the Release ID of the local import; it lacks subjects and groups, so
it cannot regenerate an NZB.

## Library-derived release IDs and file-level evidence

Added 2026-11-13. This section supersedes the "Release IDs need the whole
release" bullet of "Later changes (review round)": files imported in the older
on-disk metadata format (segments kept inline per file, no NZB store reference)
and files from stores rebuilt by the metadata migration are no longer left
without a Release ID. Found on a real library: 2,317 of 2,358 files had a File
ID and no Release ID, so they published nothing and received no network
evidence.

### Decision

Make older content shareable **without re-parsing NZBs**, at the cost of one
background read of metadata that the indexer already performs. The release's
full segment list is not available for such files, so the release is identified
by the set of its files instead.

### The "files" Release ID (byte layout)

```
files release ID = SHA-256( u32be(len(L)) || L || fileID(1) || fileID(2) || ... || fileID(n) )
L = "altmount-peernet/release-files/v1"            (ASCII, 33 bytes, so the prefix is 00 00 00 21)
fileID(i) = the 32-byte File ID, unchanged; the list is the DISTINCT File IDs sorted ascending
            by plain bytewise comparison; no count, no separators.
n >= 1 and n <= 10,000 (distinct File IDs). A zero File ID is invalid.
```

The domain label is framed exactly like the existing ones (u32 big-endian
length, then the label), and differs from the "nzb" Release ID label, so the two
ID spaces cannot collide. Version 1 must never change; a new layout needs a new
label. Implemented by `contentid.ReleaseIDFromFiles`; the bundle package
verifies the same rule by calling it. Duplicates are removed because one NZB
imported into two folders yields two rows with the same File ID, and a node
holding the duplicate must derive the same ID as one that does not (this
happens in the real library: 4 of 64 groups).

### Which files make up "the release" (grouping rule)

The files of one **import**, found through the metadata's `SourceNzbPath`: the
stored copy of the NZB the importer recorded in each file's metadata. It is
unique per import (it contains the queue item's own folder), so the key is a
hash of it (`group_key`, local only, never transmitted, never part of an ID).
Only the File IDs of the group reach the ID.

Candidates and why they lost:

- Release folder (parent directory): wrong both ways on the real library. A
  folder can hold several imports (a season folder with one NZB per episode;
  2 of 78 folders did) and one import can span folders (nested release folders
  and sample folders; 8 of 63 releases spanned more than one directory). A single
  file sits directly in a category folder next to unrelated files, so a folder
  rule would merge unrelated imports.
- `import_history` (nzb_id or folder-keyed virtual path): can be aged out. The
  real library had 3 history rows for 2,358 files.
- `SourceNzbPath`: present on every file of the real library (2,358 of 2,358),
  survives restarts and moves of the library (it is stored in the metadata, and
  a folder rename does not change it), needs no extra read because the indexer
  already reads the metadata to compute the File ID.

A file with an empty `SourceNzbPath` cannot be grouped and gets no derived ID
(counted as `no_source_nzb`). A group with more than 2,000 files gets no ID (the ID function itself accepts up to 10,000, the bundle bound, so a bundle can be verified)
(`group_too_large`). A file with a store-based ("nzb") ID is never part of a
derived group and never gets a derived one; the derived group is the other files
of that import. A store the metadata migration rebuilt (`.nzbs/_migrated`)
still does not produce an "nzb" ID: its files now get a "files" ID.

Stability: two nodes that imported the same NZB and kept the same file set
derive the same ID, whatever their paths or folder names. Nodes that kept
different subsets derive different Release IDs; File IDs match, so health
evidence is shared per file. This is accepted and documented for users.

Known limitation: if a node's NZB storage reused one path for two different
NZBs, their files would be grouped together. The importer's per-queue-item
folder makes this unlikely; no case was found in the real library.

### Storage

Migration 057 (SQLite and PostgreSQL):

- `peernet_content_index.release_scheme` (0 none, 1 nzb, 2 files; existing rows
  with a `release_id` become 1) and `group_key` (local grouping key, partial
  index on non-empty values).
- `peernet_records.release_scheme` (1 nzb, 2 files; default 1) and an index
  `(network_id, file_id, kind)` for file-level lookups.

A release ID with no scheme given to the repository is stored as scheme 1.

### Pipeline

- **Indexer.** `Entry` now also computes the group key from the metadata it
  already reads. `IndexPath` writes only when the row's identity changed (file,
  group, store-based ID), so metadata rewrites by health checks no longer touch
  the index and never make a derived ID flicker.
- **Reconcile** already reads each row's metadata, so it fills in group keys on
  rows written before the migration and treats a derived ID as part of the group,
  not of the file.
- **Derive** (`DeriveAll`, `DeriveDirty`) reads only the index table. `DeriveAll`
  is a keyset walk over groups (500 per page, at most 5,000 member rows per read,
  one short transaction per write batch, cancellable between batches, resumable
  and idempotent: a second pass over an unchanged index writes nothing).
  Complexity: O(files) rows read, O(groups) queries, memory bounded by one
  batch. Measured on a synthetic SQLite database of 200,000 rows in 40,000
  groups: first pass (every row written) 4.7 s, unchanged pass 1.8 s on a
  developer laptop; a 500,000-row library extrapolates to about 12 s and 4.5 s.
  It runs once per enable after backfill and reconcile.
- **Incremental.** The metadata observer marks the groups a write, delete or
  rename touches; a debounced pass (3 s quiet, at most 30 s) derives only those.
  Too many dirty groups, or a directory removal too large to track, falls back
  to a full pass. Dirty state is in memory; a restart runs the full pass anyway.
- **Withdrawal.** When a group's derived ID changes or the group disappears (a
  file added, removed, a file gaining a store, the last file deleted), the old
  ID is withdrawn if no indexed file still carries it: this node's own records
  for it are deleted in every network and its publish state forgotten, as
  exclusions already do. Copies other nodes hold cannot be recalled and expire.
  A rename of a folder changes no File ID and withdraws nothing.

### Records

Both record types carry an optional header field 9, `release_scheme`. Absent
means "nzb"; present must be exactly 2 ("files"); 0, 1 and any other value are
rejected as non-canonical. It sits between the common header (1-8) and the
type-specific fields (10+), is part of the signed body, and is covered by the
decode-then-re-encode identity rule. A record without it is byte-identical to the
encoding that predates it, so already-released records still verify and
re-encode identically. Nodes that predate the field reject a record that carries
it (unknown field); that is the safe direction. The store keeps the scheme
beside each record and `CandidatesByMedia` returns it.

### Health evidence by file

An old-format import and a fresh import of the same NZB have different Release
IDs and identical File IDs, so the consumer matches attestations by
`(network, file_id)` whatever Release ID they were published under:

- `AttestationsForFiles(network, fileIDs, notBefore)`, bounded to 1,024 files,
  returns at most one row per (file, publisher, verdict) and at most 5,000 rows,
  unhealthy first, so truncation can only drop healthy evidence. The index
  `(network_id, file_id, kind)` serves it (the query defeats SQLite's habit of
  preferring the `(network, expires_at)` index when it has no statistics).
- `HealthAdvice` and the recheck pass use it. The release ID is optional in
  `HealthAdvice`, so a file with only a File ID now gets advice. Every rule is
  unchanged: freshness, coverage, backbone subset, own records excluded, known
  publishers, distinct network prefixes, the private-network and open-network
  thresholds, conservative mixed evidence. Recheck evidence is paid for once per
  file (the budget key no longer includes the release ID), so the same attestation
  reached through two release IDs cannot be paid for twice.
- **Label ("healthy on the network").** A release is labelled when **every one
  of its video files** (known extensions, at most 16 per release) has qualifying
  evidence under the existing per-release rule applied per file (enough
  eligible publishers, distinct prefixes on the open network, no unhealthy
  evidence at the threshold). Reasoning: a release is as available as its worst
  file, and a publisher that attested only some of its files has not vouched for
  the release; this is stricter than the previous per-release tally, where one
  attested file counted for the whole release. A release with no video file, or
  more than 16, is not labelled. One lookup per receiving network serves a whole
  page (at most 200 releases and 1,024 files), with one batched publisher-standing
  lookup per network.

Security reasoning. File IDs are derivable by anyone holding the NZB, and so
were Release IDs, so file-level matching hands an attacker nothing they could
not already do: they could already attest any release/file pair. What changed and
how it is handled:

- An attacker no longer needs the right Release ID; the File ID is the
  same knowledge. The claim ("these articles are healthy") is about the file.
- One publisher could in principle fill a file's lookup with records under many
  invented Release IDs (5,000 per publisher are allowed). The query keeps one
  row per (file, publisher, verdict), so a publisher counts once per file and
  cannot crowd out others.
- Corroboration still needs distinct known publishers in distinct network
  prefixes; an own or relayed-own record never counts; nothing here writes a
  status. The label became stricter, not looser.

### Publishing and status

Old-format files publish like any other: release record and attestation, with
`release_scheme` 2. All exclusion, password, title and skip rules are unchanged.
The title comes from the same sources (history name, queue name, the metadata's
source NZB name with a media extension stripped); on the real library no derived
group had an empty title. Total size and file count come from the grouped files
(distinct File IDs). A derived release of up to 2,000 library files is described;
larger ones get an ID but publish nothing (`no_release_info`).

`GET /peernet/status` `library` now reports `with_nzb_release_id`,
`with_derived_release_id` and the reasons `no_source_nzb`, `group_too_large`,
`pending` next to the existing fields; the Activity card says so in plain
language.

### Real-library result (read-only check on a copy)

2,358 indexed files, 41 with an "nzb" ID. After the pass: 41 "nzb", 2,317
"files", 0 unshared. 64 derived groups (sizes: 36 single files, 10 pairs, 2 of
3-5, 4 of 6-10, 8 of 11-50, 2 of 51-500, one of 1,570); the largest five are
1,570, 382, 148, 39 and 23 files. 5 groups span more than one release folder:
four are one import kept under several folders (identical File IDs, now counted
once) and one is a two-file NZB whose files sit in two folders. None merges
unrelated imports. A folder rule would have merged unrelated imports in 2 of 78
folders.

## Phase 3: building, serving and fetching bundles

Implemented by `internal/peernet/bundlex` and the service. Level 3 only serves.

- **Build.** At publish time, when a release record is about to be signed for a
  network at level 3, the bundle is built from local data (one build at a time,
  off the health worker's goroutine) so the signed record carries the Bundle ID.
  The builder reads full per-file metadata only through its own interface
  (`bundlex.BuildSource`), which nothing else in the peernet packages receives.
  Not shareable, counted by reason: rclone encryption, other encryption, key
  material whose password is not proven to be inside the NZB, a PAR2 patch
  over any article (or patch status unknown), known holes, no NZB segment list
  for an "nzb" release (it never falls back to "files"), a rebuilt Release ID
  that differs from the indexed one, limits exceeded. Such a release still
  publishes its record, without a Bundle ID.
- **Password origin.** The metadata keeps an archive's AES key and IV but not the
  password or where it came from (the importer holds it for the import only and
  the stored NZB has no meta head). The origin is therefore never known, so key
  material is never shared and encrypted-archive releases are not shareable until
  the importer records the origin.
- **No stored bundles.** A rebuild of a 50 GB, 75,000-segment release takes about
  0.11 s of CPU (including a self-check with the receiver's verifier) plus the
  metadata read, and builds are deterministic, so only the Bundle ID is stored
  (publish state) and the last bundles stay in a 32 MB memory cache for 10
  minutes. A rebuild whose ID differs from the announced one is never served; the
  record is marked to be announced again.
- **Level changes.** The publish state remembers the level a record was published
  at and its Bundle ID. A network moving to level 3 (or below it) supersedes its
  records at the next upkeep pass.
- **Serve.** `GET /peer/v1/bundle?n=<network>&b=<bundle id>` on the peer listener.
  Level below 3, unknown network, denied caller, a Bundle ID this node did not
  announce in that network, or a release excluded since all get the same
  undecorated 403. Per-peer 30 per hour (burst 5), per public address 60 per hour
  (burst 10; private ranges are not limited per address), 128 MB per minute
  node-wide (burst 32 MB), 4 at once (1 per peer, 2 per address); no ranges, no
  compression; peers record exchange has backed off are refused.
- **Fetch.** On demand only. Offers are stored release records with a Bundle ID,
  ordered known publisher, healthy attestation, newest; only publishers with a
  verified address are dialled. At most 3 attempts, 60 s each, 8 MB cap while
  streaming, `bundle.Verify` against the IDs and scheme of the signed record.
  Failed verification or an oversized body backs the publisher off (10 minutes,
  doubling, capped at 2 hours) for bundle fetches only; record sync is unaffected.
  60 attempts per hour node-wide, 12 per publisher, 2 downloads at once.

## Phase 3: wiring decisions

Added when the publishing and the receiving halves of phase 3 were connected.

- **Fetcher and reporter.** The service is the importer's bundle fetcher (the
  fetch source becomes the importer's origin) and its bad-bundle reporter. The
  importer's fast path is installed only while the feature is enabled and a
  fetcher is connected, and acts only while a network is at level 1 or above
  (checked on every import). With the feature off none of the hooks exists.
- **Bad-bundle back-off (fetch domain only).** A bundle that passed hash
  verification but whose release then failed the local full check backs its
  publisher off for bundle fetches: 30 minutes, doubling per report, capped at 6
  hours, per (network, publisher), with the doubling forgotten after 24 hours
  without a report, cleared at once by a later release from that publisher whose
  every file passed its full check, bounded in memory like the other fetch
  tables, and counted (`full_check_reports`, `full_check_backed_off`). It is kept
  apart from the hostile-bytes strikes and never touches records, record sync or
  stored data. Why mild: a full-check failure is also what a receiver whose own
  providers lack articles sees, so the same honest publisher must not be shut
  out for long, while repeated failures still escalate. Abuse by a publisher is
  already bounded by hash and identity verification: a publisher cannot make a
  bundle that passes those and also reliably fails a full check on every
  provider.
- **Publish eligibility.** A file that came from a peer bundle is not published
  (record, attestation, bundle, re-announcement) until the importer's persistent
  state says its full check passed. The publishing pipeline asks a narrow
  interface (`PeerBundleStatus`) for each healthy notice and for every file of a
  release before a re-announcement; an error means not eligible (skip reason
  `peer_status_unknown`), a pending file is skipped with
  `peer_bundle_not_fully_checked` through the publisher's existing
  `FromPeerBundle`/`FullCheckPassed` inputs (which nothing set before). The truth
  is the persistent state, not the size of the one check that triggered the
  notice, so later sampled re-checks of a verified file publish like any local
  import. The health worker now tells the gate before it tells the publishing
  observer, so a notice never finds the pass unrecorded.
- **Patch index.** The PAR2 patch store is handed to the builder, so bundle
  sharing is not blocked as `patch_status_unknown`.
- **Release identity of bundle-imported files.** Their metadata has no source
  NZB path and no store, so nothing in it can reproduce the Release ID the
  receiver verified. Accept records the verified Release ID and scheme directly
  in the content index (before the metadata is written, so the observer finds
  the row) with a local group key derived from that ID. No schema change: a row
  whose group key is the key of its own Release ID is a verified bundle row, and
  a row with a local group key whose metadata names no source NZB and no store
  is never replaced by an entry computed from the metadata (index, reconcile,
  backfill). The derive pass treats such rows like any group: for the "files"
  scheme an intact set derives the same ID (nothing is written) and a deleted
  file changes the group's ID and withdraws the old one exactly as for a local
  import; an "nzb" scheme ID names the NZB and survives deleting a file.
  Consequence: a bundle-imported "nzb" release has no NZB store, so it cannot be
  rebuilt as a bundle (`no_nzb_list`); its record and attestations can be
  published.
- **Password origin.** The decision: record it for new imports, never share keys
  of unknown origin.
  - *Where it is stored.* A side table `peernet_password_origin` (migration 061)
    keyed by File ID, one row per file whose archive password came with the NZB's
    own content; no row means "not from the NZB", which is what every earlier
    import is. A File ID key survives renames and moves (the path changes, the
    segments do not) and does not depend on the asynchronous content indexer. The
    proto was not touched: the committed generated file does not match what the
    Makefile's protoc invocation produces, so regenerating risks changing symbol
    names for a one-bit fact.
  - *Which origins set it.* Only a `<meta type="password">` (the parsed meta head,
    or a meta element elsewhere in the NZB's first 64 KB) that is the password in
    effect, in an NZB the API did not alter, and not a nested NZB read from inside
    another release's archive. Not set: a password spelled in the NZB file name
    (`{{pw}}`), in a file subject, typed by the user (`&p=` of a link or the
    password field of a form, which the API embeds as a meta element before
    enqueueing), supplied by an ARR through the SABnzbd API (only the name form
    exists there; an NZB the ARR delivers with its own meta counts, as it is NZB
    content), or found for a file with no key material. The API marks an
    injected password in the queue item's metadata (`_altmount_pw_injected`, never
    the password); because the parser lets the NZB's own meta win over an embedded
    one, an injected password on an NZB that also carries its own meta password is
    conservatively treated as injected.
  - *Written when.* After a successful import, for the written files that carry
    key material, by a hook the importer has only while the peer network is
    enabled (so imports made while it is off carry no flag).
  - *What the builder answers.* "From the NZB" with no password text, because the
    password is never stored: the bundle carries the key and IV, which is all a
    receiver needs to play it. The password is only used if the user later
    reprocesses the release.
  - Bundle-imported files carry no flag: their key's origin is the publisher's
    claim, which a receiver cannot check, so they are not re-shared with keys.
- **Stremio flow.** `GET /stremio/:key/stream/...` appends peer candidates after
  the library streams: records for the IMDb ID (and season and episode) from the
  receiving networks, not own, not already in the content index, with a fetchable
  bundle offer (publisher is a verified peer with an address), healthy-labelled
  first, at most 10, one bounded lookup (3 s timeout, 20 offer checks, a 15 s
  cache), failing open. Titles are length-capped and stripped of control
  characters and only ever sit inside JSON text; the marker is a fixed local
  string. The stream URL is `GET /stremio/:key/peer/:release` (64 lower-case hex
  release ID, content type and IMDb ID in the query), in the same key-checked
  group. It validates the ID, rate limits (per key 6 per minute, burst 3, and 20
  per minute overall, burst 6), requires a stored signed record offering a
  fetchable bundle (a local read, no fetch otherwise), runs the import once for
  concurrent requests (detached from any one client, 2 minute limit), writing into
  the folder a normal import of the category (Movies or TV) would use
  (`PeerImportDir`), with the queue metadata and a `stremio:` download ID a
  Stremio-originated import carries (IMDb ID, type, season and episode; no
  TMDB/TVDB lookup, which would be an external call per play), so this node's own
  republished record carries the media IDs and the Stremio cleanup treats the
  import like any other. It then redirects to the library stream of the main
  media file (largest non-sample file; for an episode, the matching one). Any
  failure answers 503 with a fixed message. No configuration switch was added:
  the feature follows the peer network being enabled with a receiving network and
  a connected fetcher; a `peernet.stremio_candidates` key would need config
  patching and UI work for little gain.
- **Title of a bundle import** is the bundle's sanitised title.

## Phase 3: hardening after review

A security and a correctness review of the receiving half of phase 3 produced these rules. A bundle is a peer's description of how to read a release; every byte of it is hostile.

- **No gaps in bundles.** A shareable file has no known holes. `Validate` rejects any gap reference (files, outer sources, PAR2); the builder refuses a placeholder segment (`not shareable`). The wire format can still spell a gap, but no decoder result containing one validates.
- **Coverage rule (nzb scheme).** The pool is the NZB's segment list in NZB order, ids unique. Every segment table (inline file, outer volume, PAR2 file) must be exactly one NZB file entry's segments, in order. An entry read inline belongs to one file and is read once; an outer volume or PAR2 file may be shared by several files (a multi-file archive) but only once per file and never also inline.
- **Decode budgets.** Clips (200,000), outer sources (50,000) and PAR2 files (10,000) have bundle-wide budgets next to segments and extents; packed fields are counted before allocation and each file is validated as soon as it is decoded. A hostile 64 KB input is bounded by a test (allocations under 128 MB).
- **Sizes against the user's NZB.** `<segment bytes>` is the encoded size (a few percent above decoded). A declared decoded size must be at most the encoded one and not below 90 percent of it minus 512 bytes per segment, per segment and per table.
- **Sampled yEnc verification.** Before writing, on every accept path, the first and last article of each table and two random ones (crypto/rand, at most 24 in all) are fetched and their yEnc headers (part size, 0-based begin offset, total) must equal the layout (declared size, cumulative offset, sum of the table). The pool cannot stop after the headers, so a sample costs a whole article. A mismatch is attributable to the publisher and is reported (fetch-domain back-off). No header reader or a missing article rejects the bundle.
- **Structural media verification.** Every written media file passes the head signature check and then a container check through the staged layout: Matroska/WebM EBML header, Segment size and every SeekHead target (this reaches the Cues at the tail); MP4/MOV top-level box chain ending exactly at the file end with moov and mdat; MPEG-TS/M2TS sync at the start, quarters and the last packet; other containers get full-length reads at quarters and the tail. There is no deeper media parser in the repository.
- **Fast-path policy.** The importer's fast path takes private-network offers without a switch. Open-network offers need `peernet.open_network.fast_path` (default false) and at least two corroborating publishers (distinct /24 or /48 ranges announcing the identical Bundle ID). Fetch (5 s) and accept (15 s) are separately bounded; a layout that cannot be verified in time is declined and the NZB imports normally. Direct imports (Stremio peer play) do not use the switch.
- **Corroborated offers.** `Offer.Corroborators` counts distinct network ranges of known verified publishers announcing the same Bundle ID. Order: known, attested healthy by someone else, corroborators, newest. A publisher's own attestation never counts for its own offer.
- **Extension policy.** The importer's allowed-extensions list is applied (disallowed files are skipped, as the importer does). A fixed forbidden list (`.strm .nzb .nzbz .meta .url .lnk .m3u .m3u8 .pls .desktop`, executables and scripts) refuses the whole bundle. Written files are verified media (at most 40) plus subtitles and `.nfo` under size caps (at most 50); everything else is not written.
- **Folder naming.** `filesystem.SafeFolderName` strips the media extension first, rejects empty, dot-leading, reserved (`.nzbs corrupted_metadata patches _migrated .peernet-staging`) and category-equal names, caps length, and the importer's `PeerImportDir` joins it directly (never through `NzbFolderPath`). `cleanTarget` requires at least two components and no reserved component.
- **Write order.** Pending state rows, then metadata files, then the content index, under a per-target lock, with an undo that runs on error, panic or cancellation and removes only what the call created. A second accept of the same release waits and finds it already imported. A file with the same File ID at the path counts as the same import only when its recorded Release ID matches. Names are compared NFC and case-insensitively.
- **Pending lifetime.** A file for which the health system will never create a record (`postprocessor.WillScheduleHealth`) is marked passed at once. Pending files older than 7 days are failed. The health gate stays installed while any file is pending even when the feature is off. Delete, rename and folder rename keep the state consistent through a secondary metadata observer; a rename keeps the pending state.
- **Media IDs of peer plays.** The request's IMDb ID, type, season and episode are stored under `unverified_*` keys in the queue metadata; the publisher's media-ID reader ignores them.
- **Serve side.** Serve builds have their own slot and a node-wide rate (3 burst, 360 per hour), remember an unbuildable release for a minute, and never wait behind or block the publishing builds. An item larger than the cache cap is not cached. Full strike tables evict the entry that expires first.
- **ARR visibility while verifying.** The hold is its own persisted mark, `import_queue.peer_hold` (migration 062), not the user's `skip_arr_notification`, which keeps meaning only what the user asked. A held item is a completed queue row. The SABnzbd-compatible queue lists held items (the active list widened by `peer_hold <> 0`, no extra query) as `Downloading`, 99 percent, a status this API already emits for work in progress and every ARR client treats as active, never as terminal; the history excludes them (`completed` arm: `peer_hold = 0`; by-id lookups skip them). A pass clears the mark and the item reaches the history as completed through the normal path, the ARR notification following. A failure (full check failed, or the 7-day stale escalation) sets the item `failed` with a short message and removes its provisional history rows, so the history shows it as failed and the ARR blocklists and searches again. A user's skip keeps the item out of queue and history in every state. An import no ARR is told about (caller-suppressed or direct) ends its hold when the last file passes. A queue item the user removed is not announced.
- **Failure path per origin.** One path for every origin: `FullCheckFailed` marks the file failed, then `failImport` fails the held queue item, removes every file of the import (metadata through `RemoveFailedFile`, which is the health worker's delete-on-corruption removal plus the health record, content index, state rows) and never touches anything that is not a row of that import. The health worker stops before any ARR repair for a corrupted verdict on a pending peer import (`IsPendingPeerImport`, fail-open) and lets the gate end the import after the verdict is written, so the same item is never acted on twice. An ARR-originated item: the ARR learns from the failed history entry. A direct import (Stremio peer play): there is no ARR and no special Stremio failure behaviour exists in the health path (`stremio.skip_repair` only governs PAR2 repair during import), so it follows the same removal; the queue row shows failed. Stale: identical to a failed check except that the publisher is not reported.
- **Terminal-safe queue rows.** The queue row of a direct import is created `completed` with a marker path (`peernet-bundle/...`, `IsPeerImport`), held at once and rolled back when a later step fails, so a restart never retries it into a failure; retry, bulk restart and NZB download of such a row are refused with a clear message (409). Deleting the queue item of a pending import is allowed; the state rows keep verifying and announce nothing.
- **Startup reconcile.** Rows are written uncommitted (`peernet_bundle_imports.committed`, migration 062) before the first file and committed as the last step of `accept.write`; an uncommitted row blocks announcement and publishing. At start `Reconcile` rolls every uncommitted import back whole (files whose ID matches the row, index entries, rows), drops rows whose files no longer exist and lifts holds nothing refers to. Tests abandon `write` after every step (`rows`, each file, `files`, `index`).
- **Stremio listing.** Peer candidates are looked up concurrently with the indexer search and collected afterwards with at most a 250 ms grace; a slow, failing or panicking lookup yields no peer streams. The merge (library, peer, indexer entries) is one function and byte-identical to the former output when there are no peer streams.
- **Cheap no-offer import.** The importer asks `MayHaveOffers` first: a cached answer, recomputed only when a bundle-announcing record was stored (`BundleOfferGeneration`), so with no offer nothing is built or hashed. When offers exist the store built for the lookup is reused by the import's parse unless gap placeholders changed the NZB.
- **Structure check variants.** See the matrix in the user documentation: SeekHead-less and unknown-size Matroska, fragmented MP4, trailing boxes and zero padding, 64-bit and zero-sized boxes, and 188, 192 and 204 byte transport streams are accepted; a layout correct only at the head is still rejected, because the end of the file is always read (SeekHead target, box chain, fragment box ending at EOF, sync on the last packet). The weakest case is a SeekHead-less Matroska, where the end is checked by the Segment size and readable tail only.

## Phase 3: second hardening round

A second review with working PoCs found that the identities bind less than the first round assumed: the File ID and the Release ID hash only message-IDs, so sizes, usable ranges, extents, keys and names are bound by nothing but local checks, and the forced full check is STAT-only, so a size or offset lie that survives acceptance is permanent. The rules below close those gaps as far as sampling can.

**Layout binding.**

- A pool entry is referenced at most once within a segment table, and by one table only: two tables may share an entry only when they are the identical table (every reference, size and usable range equal, as for a shared outer volume or a PAR2 file listed by several files). Tables are keyed everywhere (validation, sampling) by `bundle.KeyOf`, a hash of the whole table, never by their first message-id. PoCs P1 (one article referenced 1,999 times as a 1.4 GB file) and P4 (two files sharing a first article, the second never sampled) are regression tests.
- Interior segments of every table are used whole (`Start == 0`, `End == Size-1`); only the first may start inside its article and only the last may end inside it (`ErrPartialRange`). Real archive slicing in this importer is expressed by extents into outer volumes, never by interior partial ranges of an inline table, so no other layout kind was needed. PoC P2 (100 articles of 700 KB each used for 10 bytes) is a regression test. In the files scheme a declared segment size is bounded by 4 MiB (`MaxFilesSchemeSegment`): posters use 0.25 to 1 MB articles (the importer's own estimates assume about 768 KB) and nothing in the importer or the readers expects more; the nzb scheme keeps the 8 MiB wire cap and is additionally bounded by the user's NZB.
- Sampling. Every table gets its first and last article and at least one uniformly random interior article (crypto/rand); tables of media files get one per 500 segments, at least three. The cap is 600 articles and 512 MiB (`MaxYencSamples`, `MaxYencBytes`) within 90 seconds (`YencTimeout`). A layout whose tables do not fit at the minimum rate is rejected (`too_many_tables`), never left unsampled. Measured at 768 KB articles: a single 1.1 GB file reads 5 articles (4 MB); a 50-volume release 150 articles (115 MB); a 200-volume release 600 articles (461 MB); 201 volumes are rejected. When the user's NZB is present the first article's yEnc `name=` must equal the subject file name of the NZB entry the table reads (obfuscated names bind too); a disagreement with the local NZB rejects the bundle but is not reported against the publisher.
- Forced full check. A pending peer import additionally gets a header sample proportional to its size (1 percent of its articles, at least 20, at most 500, bounded bytes) through an optional `health.PeerHeaderVerifier` installed together with the peer gate (nil: no change; `internal/health` imports nothing from the peer network). A mismatch fails the import and is the only failure attributable to the publisher. This is sampling, not proof.
- Open network and nested layouts. Archive layouts (outer volumes, extents, AES keys, clips) cannot be bound without reading archive headers. In the open network only plain layouts are accepted (`layout_not_allowed_open`); private networks keep nested layouts, and what a node may serve is unchanged. Open-network bundles for archive releases need archive-header spot checks (planned).
- Structure check. A fragmented MP4 must end in a `moof`+`mdat` pair whose sizes chain to the end of the file or an `mfra` with a valid `mfro` (optionally followed by `free`/`skip`); a non-fragmented MP4 walks up to 256 top-level box headers instead of a 64-read cap; Matroska reads three random interior positions of 1 MiB (three tries each) and requires two consecutive Cluster-interior elements (SimpleBlock, BlockGroup, Timecode, Cluster) in each; MPEG-TS checks eight random positions on the packet grid. The open network accepts only containers with a real check (Matroska/WebM, MP4/MOV, MPEG-TS/M2TS); other media is refused there (`container_not_verifiable`). Still unprovable: bytes inside `mdat`, inside a Matroska frame or between sampled TS packets, and everything between sampled positions. A forger who puts real structure everywhere the check can look, and garbage elsewhere, is not detected by structure alone.

**Trust and fairness.** Corroboration (bundle offers and phase 2 hints, `KnownPrefixes`) keys on the remote IP actually connected to when the peer was last verified (`peernet_peers.connected_ip`, migration 063; /24 for IPv4, /48 for IPv6); a peer with none counts for nothing, so two DNS names for one server are one range. Attesters that rank offers must themselves be known. The fast path hands its requirements (open allowed, corroboration floor) to the fetcher, which filters offers before dialing anyone, so with the switch off no open-network peer is contacted. The serve-side build budget is spent only after the build slot is held and the build starts, and a requester (node ID, public address) can use at most a sixth of it with a burst of one; cache hits and refused requests cost nothing. Only one bundle is verified at a time node-wide (`bundle.VerifyContext`); `MaxDecompressedSize` is 16 MiB (a 50 GB release is about 3.2 MB canonical, so roughly 340,000 segments, about 250 GB at 768 KB per article, still fits comfortably). Decode peak heap fell from 6.5 times the canonical size to about 3.3 times (single exact read buffer, streaming canonical comparison instead of a second full re-encode).

**State and failure.** Every action on a row's path (failure, hold, forced check, stale escalation) first compares the File ID at the path with the row; a foreign row is dropped and causes no delete, hold or forced check. A delete followed by a rewrite of the same File ID at the same path ends the peer import cleanly instead of re-attaching to a normal import. A degraded verdict fails a pending peer import at once (never announced, removed, no PAR2 repair queued); on the fast path the queue item is put back for a normal import with the peer fast path skipped once (`RequeueNormalImport`), a direct import is failed. A check that failed outright and ended corrupted counts like a corrupted verdict. The publisher is reported only for a header contradiction, not for missing articles (which can be propagation just after posting), and then the layout goes on a negative cache keyed by Bundle ID (`peernet_failed_bundles`, 7 days, persisted because failures surface up to a week later) that the fetcher consults before dialing. Housekeeping releases holds of finished imports owing no ARR (`SettledHolds`) in addition to orphaned ones, and `Reconcile` takes the write lock exclusively so it can never roll back a write in flight. Deleting a held queue item by download ID or path removes its provisional history. Idempotent `SafeFolderName`, the dead `Options.FilesReleaseID` hook, and the prebuilt NZB store reused after `TrimArticles` were fixed.

**Residual risk.** Content between sampled points is not proven to be the titled media. That is the same class of risk as a mislabelled NZB from an indexer, now bounded by random sampling per table, header samples during the full check, structural checks at random interior positions, plain layouts only in the open network, and a fallback to a normal import when a peer layout does not hold up.
