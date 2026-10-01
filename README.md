# Sluice

[![CI](https://github.com/Vetox-Inc/VetoxBot-Sluice/actions/workflows/ci.yml/badge.svg)](https://github.com/Vetox-Inc/VetoxBot-Sluice/actions/workflows/ci.yml)

Sluice is a transparent HTTP reverse proxy for Discord's REST API. It paces every bot's requests against Discord's
per-route and global rate limits, keeps each bucket's requests in order, retries the 429s it can safely replay,
protects your IP from Cloudflare bans, and exports Prometheus metrics. Any number of bots and processes can share one
Sluice, or one cluster of them.

Sluice continues [nirn-proxy](https://github.com/germanoeich/nirn-proxy), which its author has archived. It reads the
same settings and serves the same ports, so a single nirn-proxy node can be swapped for Sluice directly;
[MIGRATING.md](MIGRATING.md) lists what changes. [Lineage and credits](#lineage-and-credits) names everyone whose work
it builds on.

## Quick start

Run the container image, publishing the proxy port on loopback only:

```sh
docker run --rm -p 127.0.0.1:8080:8080 ghcr.io/vetox-inc/sluice:1
```

Or run the prebuilt binary from npm, which needs no Go toolchain:

```sh
npx @vetox-bot/sluice
```

Every [release](https://github.com/Vetox-Inc/VetoxBot-Sluice/releases) also has binaries for Linux, macOS and Windows.
To build from source, clone the repository and run `go build -o sluice .` with Go 1.26 or later.

Then send your REST traffic to Sluice instead of Discord:

```text
https://discord.com/api/v10/gateway
        becomes
http://127.0.0.1:8080/api/v10/gateway
```

In discord.js, that is `new Client({ intents, rest: { api: 'http://127.0.0.1:8080/api' } })`. Most libraries have a
similar base-URL option; otherwise, remap the host. With its defaults, Sluice answers within discord.js's 15-second
request timeout. Its own errors use Discord's JSON error shape, and its own 429s carry Discord's rate-limit headers, so
discord.js reports and waits them out as it would Discord's.

Sluice forwards the method, path, query, headers and body as sent. Like any reverse proxy, it rewrites `Host` and
drops hop-by-hop, `Forwarded` and `X-Forwarded-*` headers. It supplies a `User-Agent` when the client sends none, and
puts its own in front of one that does not start with `DiscordBot`, the format Discord requires. It asks Discord for
gzip itself, so clients receive responses uncompressed. A path without the `/api` prefix is forwarded under it, and
repeated slashes are collapsed, as Discord does.

## Security

`BIND_IP` defaults to `0.0.0.0`, and the metrics and pprof listeners have no authentication. Anyone who can reach the
proxy port can send requests to Discord from your IP, spending its invalid-request budget, unless you set
`CLIENT_AUTH_SECRET`; clients then send it in the `X-Sluice-Auth` header. Your clients' tokens cross that hop in plain
HTTP, so keep the listeners on loopback or a private network, or restrict them with a firewall or network policy. Only
the cluster-peer listener has built-in mutual TLS.

Sluice redacts tokens from every log field, including webhook and interaction tokens in paths. Report vulnerabilities
privately, as [SECURITY.md](SECURITY.md) describes.

## How Sluice schedules requests

Discord documents these REST [rate limits](https://docs.discord.com/developers/topics/rate-limits):

- **Global:** 50 requests per second per bot unless Discord has raised it, and 50 per second per IP without
  authentication. Sluice paces bearer tokens the same way. Interaction endpoints are exempt.
- **Per route:** limits Discord reports in each response, grouped into buckets.
- **Invalid requests:** 10,000 responses with status 401, 403 or 429 per 10 minutes per IP, not counting shared-scope
  429s. Past that, Cloudflare blocks the IP.

Per-route limits are never hardcoded, so new Discord endpoints need no update. Sluice starts from a conservative route
grouping and learns each bucket from `X-RateLimit-Bucket`, `X-RateLimit-Remaining` and `X-RateLimit-Reset-After`, and
from `Retry-After` on 429s. Buckets stay separate per channel, guild and webhook. Each bucket sends one request at a
time, first in first out, and moves on as soon as Discord's response headers arrive, so a slow client never holds up
the queue. The global limit is taken when a request is sent, never while it waits.

Waits are bounded. `MAX_QUEUE_DEPTH` caps the requests waiting on one bucket, and `MAX_IN_FLIGHT_REQUESTS` caps
everything admitted. `QUEUE_TIMEOUT` bounds how long a request waits for its turn. A request that cannot get it in
time, or finds its queue full, gets a `429` with `Retry-After`: it never reached Discord, so clients retry it safely.
`REQUEST_TIMEOUT` bounds how long an attempt may go without progress, so an upload that keeps moving is not cut
short; no attempt lasts more than 10 minutes. A request whose client disconnects leaves its queue at once.

### 429 retries

When Discord answers 429, Sluice retries the request itself if it can replay the body: when there is none, or when it
captured the whole body while sending it, up to `MAX_RETRY_BODY_BYTES`. Retries go back through the scheduler and must
start within `QUEUE_TIMEOUT`. If the known cooldown outlasts `QUEUE_TIMEOUT`, Sluice returns the 429 straight away,
with `Retry-After`. A body it cannot replay gets Discord's original 429, never a partial
or altered retry. No other status is retried.

A shared-scope 429 limits one resource, such as one message's reactions, while its bucket keeps capacity. Sluice keeps
the bucket open for everything else and makes only that request wait, or returns the 429 at once when the wait is too
long. Cooldowns are timed by the `retry_after` in Discord's body, which is more precise than the whole seconds of
`Retry-After`.

Sluice prevents avoidable requests and absorbs the 429s it can, but it cannot promise zero. Discord can change limits
and omit headers, and requests made outside Sluice with the same token are invisible to it. Some routes are limited
per bot across guilds although their headers say otherwise; list those in `BOT_WIDE_ROUTES`. Clients should still
handle 429s.

### Protecting your IP

- **Invalid-request budget.** Sluice counts 401, 403 and non-shared 429 responses over a rolling 10 minutes and stops
  sending new requests at 9,500, answering `503` until old responses age out. In a cluster, each node gets an equal
  share: 9,500 divided by `CLUSTER_MAX_NODES`.
- **Invalid tokens.** After a 401 on a route the token authenticates, later requests on such routes get a 401 from
  Sluice without reaching Discord. Webhook-token and interaction calls, which Discord authenticates by the token in
  their path, neither mark a token invalid nor are refused because of one. `DISABLE_401_LOCK=true` turns this off. An
  `Authorization` scheme Discord never accepts, anything but `Bot`, `Bearer` and `Basic`, gets a 401 from Sluice.
- **Deleted webhooks.** Once Discord reports a webhook as unknown (code 10015) or its token as invalid (code 50027),
  Sluice answers further calls to that webhook itself for an hour. Other 404s pass through.
- **Cloudflare blocks.** Discord's own responses carry `Via: 1.1 google`, so a 429 or 403 without it came from Discord's
  edge. Sluice returns it without a retry, backs that route off, and checks with a request of its own whether the edge
  blocks this IP or only that client. For a block, Sluice pauses all outbound traffic for the response's `Retry-After`
  and answers `429` meanwhile. Set `CLOUDFLARE_BAN_DETECTION=false` if something between Sluice and Discord strips
  `Via`.
- **Restarts.** The invalid-request history, a Cloudflare block and the deleted webhooks survive a restart in
  `STATE_FILE`, which defaults to a file in the user cache directory.

## Clustering

Set `CLUSTER_MEMBERS` or `CLUSTER_DNS` to run several nodes as one proxy. Each bot or bearer token is assigned to one
node by rendezvous hashing, and that node owns its buckets and global pacer. Unauthenticated traffic is assigned by
egress affinity, so its per-IP limit is coordinated too. Nodes gossip over HashiCorp memberlist with a shared
`CLUSTER_SECRET`, and forward requests to each other over a dedicated TLS 1.3 listener that verifies certificates in
both directions.

The cluster favours availability, so its coordination is protection, not a guarantee:

- During a network partition, both sides can serve the same token with separate state.
- Membership changes can move a token while its requests are in flight.
- Several tokens that Discord counts as one user, or unauthenticated traffic sharing an egress IP, can exceed limits
  no single node sees.
- A shared bucket cannot be coordinated until Discord names it in a response.

Startup fails if no seed can be joined, rather than starting an isolated node. `CLUSTER_MAX_NODES` (default 32) caps
the cluster size. Open the gossip port (TCP and UDP) and the peer port only between nodes, and set
`CLUSTER_ADVERTISE_ADDR` when nodes reach each other through Docker or NAT. [CONFIG.md](CONFIG.md#clustering) has the
details.

## Responses

Discord's response passes through unchanged, after any retry. Responses Sluice generates itself carry
`generated-by-proxy: true` and `Via: 1.1 sluice`. The exception is the Cloudflare-pause 429, which omits `Via` so
clients treat it like the block it reports. Sluice's own errors (`400`, `403`, `408`, `502`, `503`, and `404` on a
reserved path) also carry `X-Sluice-Proxy-Error: true`, and their body is Discord's error shape,
`{"message": "...", "code": 0}`, which discord.js reports as `DiscordAPIError[0]` with Sluice's message.

| Status | When |
| --- | --- |
| `429` | The request could not get its turn within `QUEUE_TIMEOUT` or found its queue full, a known cooldown outlasts that deadline, or a Cloudflare block has paused traffic. Carries `Retry-After` and Discord's rate-limit headers. |
| `401` | Discord already rejected the token (see `DISABLE_401_LOCK`), a webhook's token is known to be invalid, or the `Authorization` scheme is one Discord never accepts. |
| `403` | `CLIENT_AUTH_SECRET` is set and the request did not carry it. |
| `404` | The webhook is known to be deleted, or the path is under the reserved `/sluice/` or `/nirn/` prefix. |
| `408` | A Discord attempt went `REQUEST_TIMEOUT` without progress. Discord may have acted on it, so it is not retried. |
| `502` | Discord could not be reached or returned an unusable response. |
| `503` | Sluice could not safely take the request: exhausted capacity or invalid-request budget, an unavailable peer, or shutdown. Carries `Retry-After: 1`. |
| `400` | `CONNECT`, protocol upgrades, and paths containing dot segments, encoded separators or an encoded `?`. |

If a deadline expires after Discord's response headers were forwarded, Sluice aborts the stream, because the status
can no longer change.

## Configuration

Sluice reads environment variables, and a `.env` file in the working directory when present. Every setting is optional
for a single node, and `sluice --help` lists them all. The ones most deployments set:

| Variable | Default | Purpose |
| --- | --- | --- |
| `BIND_IP` | `0.0.0.0` | Address every listener binds to |
| `PORT` | `8080` | Proxy port |
| `METRICS_PORT` | `9000` | Prometheus `/metrics` port |
| `CLIENT_AUTH_SECRET` | empty | Secret clients must send in `X-Sluice-Auth` |
| `BOT_RATELIMIT_OVERRIDES` | empty | Raised global limits, as `bot_id:requests_per_second` |
| `REQUEST_TIMEOUT` | `5000` | Milliseconds a Discord attempt may go without progress |
| `QUEUE_TIMEOUT` | `10000` | Milliseconds a request may wait for its turn |
| `STATE_FILE` | a file in the user cache directory | Where protection state survives restarts |
| `LOG_LEVEL`, `LOG_FORMAT` | `info`, `text` | Log verbosity; `json` for structured logs |

[CONFIG.md](CONFIG.md) documents every setting with its range and default.

## Metrics and health

Metrics are served without authentication at `/metrics` on `METRICS_PORT` while `ENABLE_METRICS` is on, as it is by
default. Their names start with `sluice_`; set `METRICS_NAMESPACE=nirn_proxy` to keep nirn-proxy's names, dashboards
and alerts.

| Metric | Labels | Meaning |
| --- | --- | --- |
| `sluice_requests` | `method`, `status`, `route`, `clientId` | Histogram of Discord responses, one per attempt |
| `sluice_queue_wait_seconds` | `method`, `route` | Wait for the bucket and global limit before the first attempt |
| `sluice_open_connections` | `method`, `route` | Requests being handled now, not TCP sockets |
| `sluice_failures_total` | `reason` | Requests Sluice failed itself |
| `sluice_error` | none | Errors logged |
| `sluice_invalid_requests` | none | Invalid responses in the rolling 10 minutes |
| `sluice_cloudflare_blocked` | none | 1 while a Cloudflare block pauses traffic |
| `sluice_cloudflare_blocks_total` | none | Cloudflare blocks confirmed |
| `sluice_edge_refusals_total` | none | Responses from Discord's edge rather than Discord |
| `sluice_webhook_short_circuits_total` | none | Calls to deleted webhooks answered by Sluice |
| `sluice_deprecated_api_requests_total` | `version` | Requests naming a deprecated API version, or none |
| `sluice_requests_routed_sent`, `_received`, `_error` | none | Requests forwarded between cluster nodes |

`clientId` is a bot's user ID once Discord has accepted its token, and `Unverified` before that. Bearer and
unauthenticated traffic show as `Bearer` and `NoAuth`, and once the label has 1,024 values, further bots share `Other`. Route labels are
normalised and capped, so cardinality stays bounded. [grafana/sluice-dashboard.json](grafana/sluice-dashboard.json) is
an importable Grafana dashboard for either metric prefix.

The proxy port also serves two health endpoints:

- `/sluice/healthz` is liveness: `200`, or `503` while draining or shutting down, or when the cluster exceeds
  `CLUSTER_MAX_NODES`. nirn-proxy's `/nirn/healthz` still answers, but it is deprecated and goes in Sluice 2.0.
- `/sluice/health/upstream` answers `503` while a Cloudflare block is active or at least 80% of the invalid-request
  budget is used. Alert on it; do not restart on it.

On `SIGTERM`, Sluice drains: liveness fails, the node leaves its cluster, and requests already admitted get up to 15
seconds to finish; a second signal stops it at once. Give your process manager at least 20 seconds to stop it.

With `ENABLE_PPROF=true`, profiles are served at `/debug/pprof/` on `PPROF_PORT`. Never expose that port publicly.

## Migrating

Coming from nirn-proxy or from Melonly-Moderation's fork? [MIGRATING.md](MIGRATING.md) lists every difference.

## Contributing

Bug reports and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) and the
[Code of Conduct](CODE_OF_CONDUCT.md).

## Lineage and credits

Sluice exists because of other people's work, and their commits remain in this repository's history.

- **[nirn-proxy](https://github.com/germanoeich/nirn-proxy)** by [Germano Eichenberg](https://github.com/germanoeich).
  It was born out of a need within [Dyno](https://dyno.gg), proved useful to many other bot developers, and is now
  archived by its author. Sluice carries it on.
- **[Melonly-Moderation's rewrite](https://github.com/Melonly-Moderation/nirn-proxy)** of nirn-proxy's engine: learned
  buckets, the cancellation-safe scheduler, bounded state and authenticated clustering. Sluice's engine is that
  rewrite.
- **Community forks** of nirn-proxy by bsian03, PluralKit, DraftBot, TicketsBot, LorittaBot, WelcomerTeam and davfsa,
  whose ideas Sluice uses.
- **[weir](https://github.com/Xavinlol/weir)** by Xavin, whose Cloudflare-block detection, `Via` marking and upstream
  health check inspired Sluice's own.

nirn-proxy's acknowledgements, as its author wrote them:

- [Eris](https://github.com/abalabahaha/eris) - used as reference throughout this project
- [Twilight](https://github.com/twilight-rs) - used as inspiration and reference
- [@bsian](https://github.com/bsian03) & [@bean](https://github.com/beanjo55) - for listening to my rants and providing assistance

## License

Sluice is free software under the [GNU General Public License v3.0](LICENSE), the license of nirn-proxy. It is a
modified version of nirn-proxy: Vetox has changed it since 2026-09-30, and the commit history records every change.
Copyright in nirn-proxy and in Melonly-Moderation's work stays with their authors.
