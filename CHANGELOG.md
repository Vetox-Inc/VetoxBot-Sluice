# Changelog

All notable changes to Sluice are recorded here, following [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and [Semantic Versioning](https://semver.org/).

## Unreleased

## 1.0.1 - 2026-10-04

Documentation and wording only: requests are handled exactly as in 1.0.0.

### Added

- A benchmark of traffic through Sluice against the same traffic sent directly, reported in
  [BENCHMARKS.md](BENCHMARKS.md) and reproducible with `bench/run.sh`.

### Changed

- The documentation is rewritten, and the npm package's description and README with it.
- `sluice --help` lists the settings in the groups [CONFIG.md](CONFIG.md) now uses.
- The startup warning for `BUFFER_SIZE` names `MAX_QUEUE_DEPTH` as what bounds a queue.
- A node that is not part of a cluster logs "Running as a single node" at startup, in place of "Running in
  stand-alone mode".

## 1.0.0 - 2026-10-03

The first release.

### Added

Queueing:

- One queue for each of Discord's buckets, learned from `X-RateLimit-Bucket` and kept apart per channel, guild and
  webhook. A queue sends one request at a time, first in first out.
- Pacing of each bot's global limit, taken when a request is sent. `BOT_RATELIMIT_OVERRIDES` sets a raised limit, and
  `BOT_WIDE_ROUTES` names routes Discord limits per bot across guilds although their headers do not say so.
- Bounded waits. A request waits up to `QUEUE_TIMEOUT`, 10 seconds by default, for its turn, and then gets a `429` with
  `Retry-After`, as does one that finds its queue full. An attempt that goes `REQUEST_TIMEOUT` without progress
  answers `408`, and an upload that keeps moving is never cut short.
- Retries of Discord's 429s whenever the request body can be sent again. A shared-scope 429 holds up only its own
  request, and cooldowns follow the exact `retry_after` of Discord's body.

Protecting the IP:

- A budget of 9,500 invalid responses per 10 minutes, after which Sluice answers `503` instead of sending.
- A bot token Discord rejected is refused by Sluice afterwards. A token is judged only by routes it authenticates:
  webhook-token and interaction calls neither mark it invalid nor are refused because of it.
- An `Authorization` scheme Discord never accepts gets its `401` from Sluice.
- Calls to a webhook Discord reported as unknown (10015) or invalid (50027) are answered by Sluice for an hour.
- Cloudflare-block detection (`CLOUDFLARE_BAN_DETECTION`): a 429 or 403 without `Via` backs off the refused route, and
  pauses all traffic once a request of Sluice's own confirms that the edge blocks this IP.
- `STATE_FILE`, which keeps the invalid-response history, a Cloudflare block and the deleted webhooks across restarts.
- `CLIENT_AUTH_SECRET`, which makes clients present a shared secret in `X-Sluice-Auth`.

Requests and responses:

- Requests reach Discord as sent, with their path encoding intact. A path without the `/api` prefix is forwarded under
  it, repeated slashes are collapsed, and paths with dot segments, encoded separators or an encoded `?` are refused.
- `Forwarded` and `X-Forwarded-*` headers are dropped. Sluice supplies a `User-Agent` when there is none, puts its own
  in front of one that does not start with `DiscordBot`, and asks Discord for gzip itself.
- Every response Sluice generates carries `generated-by-proxy: true` and `Via: 1.1 sluice`, except the 429 it answers
  during a Cloudflare block.
- Sluice's own errors are JSON in Discord's error shape, and its own 429s carry Discord's `X-RateLimit-*` headers, so
  discord.js reports and waits them out as it does Discord's.
- Bucket names and metric labels are built only from identifiers Sluice has checked, so neither a crafted token nor
  an unusual path can add text to them or multiply them.
- Interaction follow-ups are recognised by their application's ID as well as by the interaction token's format.

Operations:

- Prometheus metrics under the `sluice_` prefix, which `METRICS_NAMESPACE` changes: Discord's responses, queue wait,
  failures, invalid responses, Cloudflare blocks, edge refusals, answers for deleted webhooks and deprecated API
  versions. The `clientId` label holds a bot's user ID once Discord accepts its token.
- `/sluice/healthz` for liveness and `/sluice/health/upstream` for the state of this IP at Discord.
- Graceful shutdown: on `SIGTERM`, liveness fails, the node leaves its cluster, and requests in flight get up to 15
  seconds to finish.
- Logging through Go's `log/slog`, with tokens redacted from every field, as text or as JSON (`LOG_FORMAT`). A warning
  the first time a client uses a deprecated API version or none.
- Clusters of nodes that gossip under a shared secret and pass requests to each other over mutual TLS, with
  `CLUSTER_ADVERTISE_ADDR` for nodes behind Docker or NAT.
- `DISCORD_API_URL`, to point Sluice at a mock or a staging server.
- `sluice --version` and `sluice --help`.
- Release binaries, a multi-arch container image on GHCR and npm packages, all with build provenance.

### Deprecated

- `/nirn/healthz`, `BUFFER_SIZE` and `DISABLE_GLOBAL_RATELIMIT_DETECTION` are still accepted and will be removed in
  Sluice 2.0. Sluice logs a warning when a client calls `/nirn/healthz`.

Sluice derives from nirn-proxy 1.3.3. [MIGRATING.md](MIGRATING.md) lists what differs for those moving from it.
