# v0.2.1: pre-V3 security maintenance release

This release addresses [#838](https://github.com/kipsilabs/altmount/issues/838)
for installations using legacy inline-segment metadata. It is based on
`0b202e0023abf299d268627562ea57d47fd39d15`, with focused security backports.
It does not include the V3 shared NZB store or the metadata migration from main.

## Changes

- Backport [#837](https://github.com/kipsilabs/altmount/pull/837): redact ARR
  webhook userinfo, query values, and fragments in registrar and startup logs.
  Authenticated webhook registration still uses the original URL.
- Backport [#840](https://github.com/kipsilabs/altmount/pull/840): use stable
  provider IDs instead of authentication usernames in NNTP names, propagated
  errors, logs, metrics, and provider status. Same-host accounts remain distinct.
  Includes nntppool v4.23.0, focused regression tests, and the legacy UI adaptation.
- Publish maintenance images without moving the `latest`, `v0`, or `0` aliases.
  GitHub's latest release also remains on the current release line.

## Upgrade and rollback

1. Stop AltMount and back up configuration, database, and metadata together.
2. Pin Docker to `ghcr.io/kipsilabs/altmount:v0.2.1` or
   `laris11/altmount:v0.2.1`. CLI users should download the matching platform
   archive and verify it against `checksums-cli.txt` from the v0.2.1 release.
3. Start AltMount with the same configuration and storage paths. There is no
   metadata conversion or database schema change relative to `0b202e0`.

Provider IDs must be unique, non-empty public identifiers without credentials
or non-graphic characters. Missing IDs receive deterministic `provider_N` IDs
on configuration load. Existing IDs are retained. Legacy provider quota keys
are moved atomically to ID-based keys; ambiguous keys are retained instead of
being credited to multiple accounts. Provider status and the system UI show IDs
instead of authentication usernames.

To roll back, stop AltMount and restore the pre-upgrade binary/image and the
configuration/database/metadata backup together. Older binaries use the legacy
quota keys and cannot read the quota state under the new keys.

This release is for pre-V3 stores. Do not point it at a store already converted
to V3. Moving to main or a V3 release is a separate upgrade requiring its
metadata migration procedure and a fresh backup. Keep the version pinned and
avoid the updater's `latest` or `dev` channels while remaining on this line.

## Maintenance scope

`fix/pre-v3-security-release` is the upstream maintenance branch for this
backport. Its scope is targeted security fixes for the `0b202e0` baseline;
feature development and the V3 migration remain on main. No support end date
is set by this release.

## Validation

- Full `go test ./...` and `go vet ./...` pass.
- Race tests pass for ARR registrar, config, pool, database, and API packages.
- Frontend production build and embedded CLI build pass.
- Metadata code and database migration files match `0b202e0`.
