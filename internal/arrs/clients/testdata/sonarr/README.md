These are fixture contract tests, not a recording from a running Sonarr instance.

`native-contract.json` extracts the request parameters and response schema references
for every native endpoint used by AltMount from Sonarr's v5 OpenAPI document at
commit `a6171f0ca61b2a76ec529da3e9aadb11435344f9`:
https://github.com/Sonarr/Sonarr/blob/a6171f0ca61b2a76ec529da3e9aadb11435344f9/src/Sonarr.Api.V5/openapi.json

Retrieved 2026-10-26 from the immutable revision. The previous `v5-develop` branch
was unavailable; the schema on `develop` at this revision matched the design's
v5 schema. Test responses are deliberately small, handwritten examples. The
endpoint parity test verifies its requests exist in this pinned contract.

Native differences covered include grouped `episodeIds`, optional pack metadata,
`connection` resources, `skipTesting` on download-client writes, repeated episode
file IDs, history `seriesIds`, episode `includeSubresources`, and removal enums.
