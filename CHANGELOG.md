# Changelog

All notable changes to Sluice are recorded here, following [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and [Semantic Versioning](https://semver.org/). Earlier history is in nirn-proxy's
[releases](https://github.com/germanoeich/nirn-proxy/releases) and in this repository's commits.

## Unreleased

The first release as Sluice, continuing nirn-proxy 1.3.3 on Melonly-Moderation's engine rewrite.
[MIGRATING.md](MIGRATING.md) covers upgrading.

### Added

- Webhook fail-fast: calls to a webhook Discord reported as unknown (10015) or invalid (50027) are answered by Sluice
  for an hour.
- Cloudflare-block detection, which pauses outbound traffic during a block (`CLOUDFLARE_BAN_DETECTION`), and the
  `/sluice/health/upstream` endpoint.
- `DISCORD_API_URL`, `CLUSTER_ADVERTISE_ADDR`, `METRICS_NAMESPACE` and `LOG_FORMAT` settings.
- Metrics for queue wait, invalid responses, Cloudflare blocks and webhook short-circuits.
- Paths without the `/api` prefix are forwarded under it.
- `sluice --version` and `sluice --help`.
- Release binaries, a multi-arch container image on GHCR and npm packages, all with build provenance.

### Changed

- Renamed from nirn-proxy. Metrics default to the `sluice_` prefix, and `/sluice/healthz` joins `/nirn/healthz`.
- Logging uses Go's `log/slog`, redacts credentials from every field, and can write JSON.
- An upstream timeout answers `408`, as nirn-proxy documented.
- A bot token is judged only by routes it authenticates: webhook-token and interaction calls neither mark it valid or
  invalid nor are refused because of it.
- The `clientId` metric label holds a bot's user ID once Discord accepts its token.
- Every response Sluice generates carries `Via: 1.1 sluice` besides `generated-by-proxy: true`, except the
  Cloudflare-pause 429.

### Fixed

Compared with nirn-proxy 1.3.3:

- A swept bucket queue no longer leaves requests waiting forever.
- Channel creation no longer shares one queue across every guild, nor `/channels/:id` across every channel.
- Reaction paths with encoded characters such as `#` reach Discord intact.
- The global limit is taken when a request is sent, not when it arrives.
- `Forwarded` and `X-Forwarded-*` headers no longer reach Discord, and paths with dot segments, encoded separators or an
  encoded `?` are rejected.
- Repeated slashes, which a base URL ending in `/` produces, are collapsed, so those requests keep their channel, guild
  and webhook limits.
- Non-numeric identifiers in routes such as templates and activity instances no longer create a bucket and a metric
  label each.
