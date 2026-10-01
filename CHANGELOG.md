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
- `CLIENT_AUTH_SECRET`, which makes clients present a shared secret in `X-Sluice-Auth`.
- `STATE_FILE`, which keeps the invalid-request history, a Cloudflare block and the deleted webhooks across restarts.
- `BOT_WIDE_ROUTES`, for routes Discord limits per bot across guilds although their headers do not say so.
- Graceful shutdown: on `SIGTERM`, liveness fails, the node leaves its cluster, and requests in flight get up to 15
  seconds to finish.
- Metrics for queue wait, invalid responses, Cloudflare blocks, edge refusals, webhook short-circuits and deprecated
  API versions, and a warning the first time a client uses a deprecated API version or none.
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
- Sluice's own errors are JSON in Discord's error shape, and its own 429s carry Discord's `X-RateLimit-*` headers, so
  discord.js reports and waits them out as it does Discord's.
- `QUEUE_TIMEOUT` defaults to 10 seconds and bounds only the wait for a turn, so requests are answered within
  discord.js's default timeout. A request that runs out of it, or finds its queue full, gets a `429` with
  `Retry-After` instead of a `408` or `503`.
- A 429 or 403 without `Via` pauses all traffic only once a request of Sluice's own confirms the edge blocks this IP;
  otherwise only the refused client's route backs off.
- Sluice puts its own `User-Agent` in front of one that does not start with `DiscordBot`, and asks Discord for gzip
  itself.
- An `Authorization` scheme Discord never accepts gets a 401 from Sluice instead of spending the invalid-request
  budget.
- Interaction follow-ups are also recognised by their application's ID, not only by the interaction token's format.

### Deprecated

- `/nirn/healthz`, `BUFFER_SIZE` and `DISABLE_GLOBAL_RATELIMIT_DETECTION` will be removed in Sluice 2.0. Sluice logs a
  warning when a client calls `/nirn/healthz`.

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
- A shared-scope 429 no longer closes its whole bucket; nirn-proxy exempted only reactions.
- `REQUEST_TIMEOUT` no longer counts the upload, so a large file is not cut short, and cooldowns use the precise
  `retry_after` of Discord's body instead of the whole seconds of `Retry-After`.
- A deleted webhook is recognised even when the client asked for a compressed response.
- A Discord 5xx or an edge refusal no longer vouches for a bot token.
- A token that only imitates an interaction token's prefix can no longer put its own text into bucket names and metric
  labels.
