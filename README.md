# Sluice

[![CI](https://github.com/Vetox-Inc/VetoxBot-Sluice/actions/workflows/ci.yml/badge.svg)](https://github.com/Vetox-Inc/VetoxBot-Sluice/actions/workflows/ci.yml)

Sluice is a reverse proxy for Discord's REST API that keeps bots inside Discord's rate limits. Every process that
talks to Discord sends its requests to Sluice instead. Sluice queues them per bucket, paces each bot against its
global limit, retries the 429s that are safe to retry, and keeps the IP they share off Cloudflare's block list. Nothing
else about a bot changes: it keeps its library and its code, and points them at a different base URL.

One node serves any number of bots and processes. It is a single static binary with no dependencies, it exports
Prometheus metrics, and several nodes can run as one cluster.

## Why a proxy

A Discord library counts rate limits for the process it runs in, which is all a bot in one process needs. Split the
bot into shards or clusters, or put a dashboard or a worker beside it, and each process counts alone against limits
that Discord applies to all of them together. They overrun those limits without any one of them being able to tell.
Discord answers with 429s, and after enough of those it blocks the IP.

With Sluice in between, the counting happens in one place, for every process at once. In the
[benchmark](BENCHMARKS.md), eight processes sharing one bot's token sent 3,200 requests. Sent directly, Discord
refused 11,337 attempts along the way, more in a minute than it tolerates from an IP in ten. Sent through Sluice, it
refused none.

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

## How requests flow

Discord documents these REST [rate limits](https://docs.discord.com/developers/topics/rate-limits):

- **Global:** 50 requests per second per bot unless Discord has raised it, and 50 per second per IP without
  authentication. Sluice paces bearer tokens the same way. Interaction endpoints are exempt.
- **Per route:** limits Discord reports in each response, grouped into buckets.
- **Invalid requests:** 10,000 responses with status 401, 403 or 429 per 10 minutes per IP, not counting shared-scope
  429s. Past that, Cloudflare blocks the IP.

Per-route limits are never hardcoded, so new Discord endpoints need no update. Sluice starts from a conservative route
grouping and learns each bucket from `X-RateLimit-Bucket`, `X-RateLimit-Remaining` and `X-RateLimit-Reset-After`, and
from `Retry-After` on 429s. Buckets stay separate per channel, guild and webhook. Each bucket has a queue that sends
one request at a time, first in first out, and moves on as soon as Discord's response headers arrive, so a slow client
never holds up the queue. The global limit is taken when a request is sent, never while it waits, and requests are
spaced evenly rather than released in bursts.

Waits are bounded. `MAX_QUEUE_DEPTH` caps the requests waiting in one queue, and `MAX_IN_FLIGHT_REQUESTS` caps
everything Sluice has accepted. `QUEUE_TIMEOUT` bounds how long a request waits for its turn. A request that cannot
get it in time, or finds its queue full, gets a `429` with `Retry-After`: it never reached Discord, so clients retry it
safely. `REQUEST_TIMEOUT` bounds how long an attempt may go without progress, so an upload that keeps moving is not cut
short; no attempt lasts more than 10 minutes. A request without a body leaves its queue at once when its client
disconnects. One with a body is found out only when its turn comes, and may still reach Discord.

### Retries

When Discord answers 429, Sluice retries the request itself if it can send the body again: when there is none, or when
it kept a copy of the whole body while sending it, up to `MAX_RETRY_BODY_BYTES`. A retry goes to the back of its
queue, behind the requests already waiting there, and must start within `QUEUE_TIMEOUT`. If the cooldown outlasts
`QUEUE_TIMEOUT`, Sluice returns the 429 straight away,
with `Retry-After`. A body it cannot send again gets Discord's original 429, never a partial or altered retry. No other
status is retried.

A shared-scope 429 limits one resource, such as one message's reactions, while its bucket keeps capacity. Sluice keeps
the bucket open for everything else and makes only that request wait, or returns the 429 at once when the wait is too
long. Cooldowns are timed by the `retry_after` in Discord's body, which is more precise than the whole seconds of
`Retry-After`.

Sluice prevents avoidable 429s and absorbs the ones it can, but it cannot promise zero. Discord can change limits
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

## Responses

Discord's response passes through unchanged, after any retry. Responses Sluice generates itself carry
`generated-by-proxy: true` and `Via: 1.1 sluice`. The exception is the 429 it answers during a Cloudflare block, which
omits `Via` so clients treat it like the block it reports. Sluice's own errors (`400`, `403`, `408`, `502`, `503`, and
`404` on a reserved path) also carry `X-Sluice-Proxy-Error: true`, and their body is Discord's error shape,
`{"message": "...", "code": 0}`, which discord.js reports as `DiscordAPIError[0]` with Sluice's message.

| Status | When |
| --- | --- |
| `429` | The request could not get its turn within `QUEUE_TIMEOUT` or found its queue full, a cooldown outlasts that deadline, or a Cloudflare block has paused traffic. Carries `Retry-After` and Discord's rate-limit headers. |
| `401` | Discord already rejected the token (see `DISABLE_401_LOCK`), a webhook's token is known to be invalid, or the `Authorization` scheme is one Discord never accepts. |
| `403` | `CLIENT_AUTH_SECRET` is set and the request did not carry it. |
| `404` | The webhook is known to be deleted, or the path is under the reserved `/sluice/` prefix. |
| `408` | A Discord attempt went `REQUEST_TIMEOUT` without progress. Discord may have acted on it, so it is not retried. |
| `502` | Discord could not be reached or returned an unusable response. |
| `503` | Sluice could not safely take the request: exhausted capacity or invalid-request budget, an unavailable node, or shutdown. Carries `Retry-After: 1`. |
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

[CONFIG.md](CONFIG.md) documents every setting with its range and default. `sluice --check` reads the settings as a
start would, and exits without opening a port: with 0 when they are valid, and with the reason when they are not. Run
it before a restart takes the running proxy down.

## Metrics

Metrics are served without authentication at `/metrics` on `METRICS_PORT` while `ENABLE_METRICS` is on, as it is by
default. Their names start with `sluice_`, or with whatever `METRICS_NAMESPACE` is set to.

| Metric | Labels | Meaning |
| --- | --- | --- |
| `sluice_requests` | `method`, `status`, `route`, `clientId` | Histogram of Discord responses, one per attempt |
| `sluice_queue_wait_seconds` | `method`, `route` | Wait for the bucket and global limit, until a request is sent or Sluice answers it |
| `sluice_open_connections` | `method`, `route` | Requests being handled now, not TCP sockets |
| `sluice_failures_total` | `reason` | Requests Sluice answered with an error of its own, or whose client left |
| `sluice_error` | none | Errors logged, which a failed request is not |
| `sluice_warnings_total` | none | Warnings logged |
| `sluice_resource_usage`, `sluice_resource_limit` | `resource` | Clients, bearer tokens, buckets, requests in flight and retry copies: in use, and their `MAX_` limit |
| `sluice_global_limit` | `clientId` | Requests a second a client is paced at |
| `sluice_build_info` | `version`, `goversion` | 1, labelled with the running version |
| `sluice_invalid_requests` | none | Invalid responses in the rolling 10 minutes |
| `sluice_invalid_requests_limit` | none | The count at which this node stops sending |
| `sluice_cloudflare_blocked` | none | 1 while a Cloudflare block pauses traffic |
| `sluice_cloudflare_blocks_total` | none | Cloudflare blocks confirmed |
| `sluice_edge_refusals_total` | none | Responses from Discord's edge rather than Discord |
| `sluice_webhook_short_circuits_total` | none | Calls to deleted webhooks answered by Sluice |
| `sluice_deprecated_api_requests_total` | `version` | Requests naming a deprecated API version, or none |
| `sluice_requests_routed_sent`, `_received`, `_error` | none | Requests passed between cluster nodes |

`clientId` is a bot's user ID once Discord has accepted its token, and `Unverified` before that. Bearer and
unauthenticated traffic show as `Bearer` and `NoAuth`, and once the label has 1,024 values, further bots share `Other`.
Route labels are normalised and capped, so cardinality stays bounded.
[grafana/sluice-dashboard.json](grafana/sluice-dashboard.json) is an importable Grafana dashboard; its Metric prefix
box takes the value of `METRICS_NAMESPACE`.

Sluice does not log a request it fails: it counts it in `sluice_failures_total`, and warns at most once every 30
seconds about the reasons that need someone's attention. [CONFIG.md](CONFIG.md#enable_metrics) lists the reasons.
[prometheus/alerts.yml](prometheus/alerts.yml) holds alert rules for Prometheus, each tested against the failure it is
for.

With `ENABLE_PPROF=true`, profiles are served at `/debug/pprof/` on `PPROF_PORT`. Never expose that port publicly.

## Health and shutdown

The proxy port serves two health endpoints:

- `/sluice/healthz` is liveness: `200`, or `503` while draining or shutting down, or when the cluster exceeds
  `CLUSTER_MAX_NODES`.
- `/sluice/health/upstream` answers `503` while a Cloudflare block is active or at least 80% of the invalid-request
  budget is used. Alert on it; do not restart on it.

On `SIGTERM`, an interrupt or a hangup, Sluice drains: liveness fails, the node leaves its cluster, and requests
already accepted get up to 15 seconds to finish. A second signal a second or more after the first stops it at once;
one that follows sooner is taken for the same stop arriving twice, as it does when a process manager signals a whole
process group. Give your process manager at least 20 seconds to stop it.
[systemd/sluice.service](systemd/sluice.service) is a unit that runs Sluice on a Linux host with nothing but the
network and its state directory.

## Clusters

Several nodes run as one proxy once `CLUSTER_MEMBERS` or `CLUSTER_DNS` tells them where to find each other. Each bot
or bearer token is assigned to one node by rendezvous hashing, and that node keeps its queues and its global pacing.
Traffic without a token is assigned to one node as well, apart from interaction calls, so its per-IP limit has one
keeper too. Nodes gossip over HashiCorp memberlist with a shared `CLUSTER_SECRET`, and pass requests to each other
over a dedicated TLS 1.3 listener that verifies certificates in both directions.

A cluster favours availability, so its coordination is protection, not a guarantee:

- During a network partition, both sides can serve the same token with separate state.
- Membership changes can move a token while its requests are in flight.
- Several tokens that Discord counts as one user, or unauthenticated traffic sharing an outgoing IP, can exceed limits
  no single node sees.
- A shared bucket cannot be coordinated until Discord names it in a response.

Startup fails if no seed can be joined, rather than starting an isolated node. `CLUSTER_MAX_NODES` (default 32) caps
the cluster size. Open the gossip port (TCP and UDP) and the peer port only between nodes, and set
`CLUSTER_ADVERTISE_ADDR` when nodes reach each other through Docker or NAT. [CONFIG.md](CONFIG.md#cluster) has the
details.

## Security

Out of the box Sluice listens on every interface (`BIND_IP` is `0.0.0.0`), and its metrics and pprof ports ask for no
credentials. Anyone who can reach the proxy port can send requests to Discord from your IP and spend its
invalid-request budget, unless you set `CLIENT_AUTH_SECRET`; clients then send it in the `X-Sluice-Auth` header. Your
clients' tokens cross that hop in plain HTTP, so keep all three ports on loopback or a private network, or put a
firewall in front of them. Only the port between cluster nodes has mutual TLS built in.

Sluice redacts tokens from every log field, including webhook and interaction tokens in paths. Report vulnerabilities
privately, as [SECURITY.md](SECURITY.md) describes.

## Performance

[BENCHMARKS.md](BENCHMARKS.md) sends the same workloads straight to a mock Discord and through Sluice 1.0.0 on a
single CPU core, and compares them:

| Measured | Result |
| --- | --- |
| Time the hop adds to a request, median | 0.2 to 0.4 ms |
| Time the hop adds to a request, 99th percentile | 3 to 7 ms |
| Requests a second on one core | About 6,000 |
| Footprint at 40 requests a second | 2% of a core, 17 MiB |
| Discord's 429s for 3,200 requests from eight processes | 11,337 sent directly, none through Sluice |
| Discord's 429s in 20 minutes of mixed traffic at 80% of a bot's limit | 3,292 sent directly, none through Sluice |
| Median request in that traffic | 26 ms directly, 64 ms through Sluice, which spaces requests out |
| Memory after those 20 minutes | 19 MiB |
| Requests that failed through Sluice | None of 1.9 million |

That page has the method, every run, and what the benchmark does not cover.

## Contributing

Bug reports and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) and the
[Code of Conduct](CODE_OF_CONDUCT.md).

## Origins and license

Sluice is an improved and modernized version derived from [nirn-proxy](https://github.com/germanoeich/nirn-proxy) by
[Germano Eichenberg](https://github.com/germanoeich). That project was born out of a need within
[Dyno](https://dyno.gg) and is now archived by its author. The commits of everyone named here remain in this
repository's history, and [MIGRATING.md](MIGRATING.md) covers moving over from it.

Sluice also builds on:

- **[Melonly-Moderation's rewrite](https://github.com/Melonly-Moderation/nirn-proxy)** of the engine: learned buckets,
  the cancellation-safe scheduler, bounded state and authenticated clustering. Sluice's engine is that rewrite.
- **Community forks** by bsian03, PluralKit, DraftBot, TicketsBot, LorittaBot, WelcomerTeam and davfsa, whose ideas
  Sluice uses.
- **[weir](https://github.com/Xavinlol/weir)** by Xavin, whose Cloudflare-block detection, `Via` marking and upstream
  health check inspired Sluice's own.

The original project's acknowledgements, as its author wrote them:

- [Eris](https://github.com/abalabahaha/eris) - used as reference throughout this project
- [Twilight](https://github.com/twilight-rs) - used as inspiration and reference
- [@bsian](https://github.com/bsian03) & [@bean](https://github.com/beanjo55) - for listening to my rants and providing assistance

Sluice is free software under the [GNU General Public License v3.0](LICENSE), the license of the project it derives
from. It is a modified version of that project: Vetox has changed it since 2026-09-30, and the commit history records
every change. Copyright in the original and in Melonly-Moderation's work stays with their authors.
