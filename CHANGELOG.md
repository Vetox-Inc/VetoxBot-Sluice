# Changelog

All notable changes to Sluice are recorded here, following [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and [Semantic Versioning](https://semver.org/). Earlier history is in nirn-proxy's
[releases](https://github.com/germanoeich/nirn-proxy/releases) and in this repository's commits.

## Unreleased

### Changed

- A shared-scope 429 reaches its client at once instead of being retried by Sluice, so the client's own backoff decides
  when the resource is asked again.
- A Cloudflare block pauses outbound traffic for the edge's whole `Retry-After`, up to 25 hours instead of one, because
  the edge can block an IP for a day. A refused route that is not an IP block still backs off for at most an hour.

### Added

- Shared-limit storm protection: after 20 shared-scope 429s for one resource within 10 seconds, Sluice answers that
  resource's requests with a shared-scope 429 itself for Discord's cooldown, at least 10 seconds and at most a minute.
  `sluice_failures_total` counts both with the reasons `shared_rate_limit` and `shared_rate_limit_breaker`.
- `CLOUDFLARE_PROBE_INTERVAL`: while a block pauses traffic, Sluice probes the edge every 15 minutes by default,
  ending the pause early once Discord answers and re-arming it while the edge still refuses.
- `MAX_REFUSALS_PER_SECOND`, 20 by default: a ceiling on the 401, 403 and 429 responses, shared-scope ones included,
  this IP collects per second; a cluster node gets its share, but at least 10. Past half of it, requests go out only
  while refusals plus requests in flight stay below it; at it, Sluice answers every request with a `429` itself for 2
  seconds. The invalid-request budget bounded only the 10-minute total, which a flood could spend in a second. New
  metrics: `sluice_upstream_refusals`, `sluice_refusal_guard_blocked`, `sluice_refusal_guard_trips_total`,
  `sluice_edge_provisional_pauses_total` and `sluice_interaction_short_circuits_total`, and the `sluice_failures_total`
  reasons `refusal_rate`, `upstream_rate_limit`, `invalid_token` and `shared_rate_limit_reclosed`.
- A `200` from `/sluice/health/upstream` carries `Sluice-Edge-Clear-For`: the whole seconds since the last edge pause
  ended, or since the node started. A client waiting out a ban can tell a lift that holds from a leak without probing
  Discord, and need not wait out its own calm period after every restart.

### Fixed

- A pause in force ends only after two probes in a row get Discord's own success, the second a minute after the
  first: during a ban the edge lets the odd request through, and one such probe resumed every client. After an early
  end, the next refusal can re-arm the pause at once. An answer that is not a refusal but not Discord's success
  either, such as a 5xx or an error page without `Via`, no longer counts as the block lifting.
- Sluice no longer retries a 429 on an interaction or webhook-token route, and retries any other 429 at most once,
  only after a `Retry-After` of at most 5 seconds. Retries of 429s on interaction routes, which skip the global limit,
  multiplied a client's follow-ups into hundreds of refusals a second.
- A probe that cannot tell whether the edge blocks this IP, by a 5xx, an error page or no answer, pauses traffic
  provisionally for a minute instead of letting every refusal start another probe. Probes on suspicion run at most once
  every 30 seconds whatever they find, and ten edge refusals within those 30 seconds pause traffic provisionally even
  after a clear probe, since the edge lets the odd request through during a ban.
- When a shared-limit closure ends, one trial request goes first: a shared-scope 429 closes the resource again, and
  only another answer reopens it. Every request waiting on the resource went through at once.
- A credential's 401 lock is checked before every attempt, so requests already queued when the first 401 arrived no
  longer reach Discord.
- An interaction token Discord answered 401, such as an expired one, is answered by Sluice for 15 minutes instead of
  reaching Discord on every follow-up.
- `STATE_FILE` is saved at once when a Cloudflare pause starts or changes, after every 100th invalid response and when
  the refusal guard trips, besides every 5 seconds: a crash during a flood could forget seconds of invalid responses.
  Its format stays readable by 1.0.0. Each save also syncs the file's directory, and startup removes temporary files a
  crash left behind; a save that keeps failing is logged once, not on every try.
- A panic in an upstream attempt can no longer hold a refusal-guard slot, an invalid-request reservation or a
  shared-limit trial for good.

## 1.0.0 - 2026-10-03

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
