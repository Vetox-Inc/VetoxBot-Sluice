# Changelog

All notable changes to Sluice are recorded here, following [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and [Semantic Versioning](https://semver.org/).

## Unreleased

### Added

- `sluice --check`, which reads the settings as a start would and exits without opening a port or joining a
  cluster: with status 0 when they are valid, and otherwise with the error a start would stop on.
- A warning at startup for a setting that is accepted and probably a mistake: a timeout under 100 milliseconds,
  `MAX_RETRY_CAPTURE_BYTES` below `MAX_RETRY_BODY_BYTES`, and a state file that is off because the process has no
  cache directory.
- A reason in `sluice_failures_total` for every error Sluice answers with on its own and for a client that left
  before its answer: 24 reasons where there were 9, listed in [CONFIG.md](CONFIG.md#enable_metrics). The nine keep
  their meaning. Every reason is exported from the start, at 0, so that an alert sees its first failure.
- A warning in the log for the failures an operator has to act on, such as a Discord that cannot be reached, a
  rejected token or a `MAX_` limit that is reached. It names the method and the route, and comes at most once every 30
  seconds for a reason, with a count of the failures in between.
- Metrics: `sluice_warnings_total`; `sluice_resource_usage` and `sluice_resource_limit`, which show how close the
  limits on clients, bearer tokens, buckets, requests in flight and retry copies are; `sluice_global_limit`, the
  requests a second each bot is paced at; `sluice_invalid_requests_limit`, the count at which a node stops sending;
  and `sluice_build_info`, which carries the version.
- Alert rules for Prometheus in [`prometheus/alerts.yml`](prometheus/alerts.yml), with tests of every rule, and a row
  for the new metrics in the Grafana dashboard.
- A systemd unit in [`systemd/sluice.service`](systemd/sluice.service) that gives Sluice the network and its state
  directory, and nothing else of the host.
- The release archives carry the unit, the alert rules and the dashboard beside the binary.
- A hangup (`SIGHUP`) stops Sluice the way `SIGTERM` does, letting the requests in flight finish, unless Sluice was
  started to ignore it, as `nohup` does. The npm launcher passes a hangup on either way.
- The startup log line names the upstream and counts the entries of `BOT_RATELIMIT_OVERRIDES` and `BOT_WIDE_ROUTES`.

### Changed

- At `MAX_BUCKET_STATES`, Sluice first drops the buckets that nothing uses, waits for or has to wait out, and refuses
  a request only when that leaves no room. It used to answer `503` until the next periodic clean-up.
- `sluice_queue_wait_seconds` includes the wait of a request that Sluice answered itself after it had queued.
- With `DISABLE_401_LOCK=true`, a token Discord answered `401` is sent one request at a time until Discord accepts
  one, as a token Sluice has not seen before is. Its requests used to go out side by side.
- The image is published from the `release` environment, as the npm packages are, and a run by hand of either
  workflow refuses a tag that is not on `master`.
- A release is published only after the whole of CI has passed on its commit, and only from a tag on `master`.
- CI runs discord.js against the built binary, the alert-rule tests and a build of the image for its three
  platforms, and it also runs once a week.

### Fixed

- A stop signal that arrived twice within a second ended Sluice at once and cut off the requests in flight. It does
  when a process manager or a terminal signals a whole process group and a launcher, such as the npm one, passes the
  signal on. The two now count as one stop.
- On Windows, the npm launcher ended Sluice at once on Ctrl+C. It now leaves the stop to Sluice, which finishes the
  requests in flight.
- With `DISABLE_401_LOCK=true`, a `401` from Discord counted as proof that the token was real, so the ID inside a
  rejected token could become a `clientId` in metrics. Such a token now stays `Unverified`.
- A webhook or interaction token sent where the ID belongs, by a client that swapped the two, was kept in the route
  label of the metrics and the log. Anything but an ID in that place is now `!`.
- A bot token whose first part decodes to more than 20 digits no longer has those digits kept as a bot ID.
- A request path that is not valid UTF-8 was logged with every such request. It is logged once.
- In a cluster, a request for another node whose client left was counted as `peer_error`. It is `client_closed`.
- A `Retry-After` too large to be a duration, on a refusal by Discord's edge, could pause traffic for one second
  instead of the longest pause.
- A state file that cannot be saved is reported once, and once more when saving works again, instead of every 5
  seconds.
- The warning for an edge check that reached no verdict says why.
- A route in a log line reads as its metric label does: `/webhooks/!/!` used to come out as `/webhooks/!/:token`.

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
