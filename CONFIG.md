# Configuration

Sluice takes its settings from environment variables, and from a `.env` file in its working directory when there is
one. A single node runs with nothing set. A cluster needs its secret and certificates, described under
[Cluster](#cluster).

A value Sluice cannot use stops it at startup with an error that names the setting, and so does a port it cannot
bind: it never starts half-configured. Times are whole milliseconds, sizes are bytes, and switches are `true` or
`false`. `sluice --help` prints every name on this page.

- [Listening](#listening)
- [Reaching Discord](#reaching-discord)
- [Queues and limits](#queues-and-limits)
- [Retries](#retries)
- [Protecting your IP](#protecting-your-ip)
- [Memory bounds](#memory-bounds)
- [Logs, metrics and profiling](#logs-metrics-and-profiling)
- [Cluster](#cluster)
- [Retired settings](#retired-settings)

## Listening

| Setting | Default | Accepts |
| --- | --- | --- |
| `BIND_IP` | `0.0.0.0` | An IP address |
| `PORT` | `8080` | 1 to 65535 |
| `CLIENT_AUTH_SECRET` | empty | At least 32 characters |

### `BIND_IP`

The address all of Sluice's listeners bind: the proxy, metrics and pprof, and in a cluster the gossip and peer ports
too. The default listens on every interface, and neither metrics nor pprof asks who is calling, so bind `127.0.0.1` or
a private interface, or keep the ports behind a firewall, unless they are meant to be public.

In a cluster it has to be a literal IP address. A hostname is refused there, because resolving it could leave gossip
listening on every interface.

### `PORT`

The port clients send their Discord requests to.

### `CLIENT_AUTH_SECRET`

A shared secret that clients must send in the `X-Sluice-Auth` header. With it set, a request that lacks it gets `403`
and `X-Sluice-Proxy-Error: true`. Left empty, Sluice serves anyone who can reach the port.

Set it whenever another host can reach the proxy port. Whoever can send requests through Sluice can use up your IP's
[invalid-request budget](#protecting-your-ip). The secret travels in plain HTTP, so it belongs on a network you trust.
In discord.js it goes with the other REST options:

```js
new Client({ intents, rest: { api: 'http://sluice:8080/api', headers: { 'X-Sluice-Auth': secret } } })
```

Sluice strips the header before it forwards a request. The health endpoints stay open without it, and cluster nodes
identify each other by certificate instead. The answer to a missing secret is `403`, not `401`, because discord.js
throws its bot token away when it sees a `401`.

## Reaching Discord

| Setting | Default | Accepts |
| --- | --- | --- |
| `DISCORD_API_URL` | `https://discord.com` | A URL without a path |
| `OUTBOUND_IP` | empty | A local IP address |
| `DISABLE_HTTP_2` | `true` | `true` or `false` |
| `REQUEST_TIMEOUT` | `5000` | 1 to 86,400,000 ms |

### `DISCORD_API_URL`

Where requests are forwarded. Change it only to test against a mock or a staging server. It must be `https`, except
for a loopback host, and must not carry a path. Do not use `discordapp.com`, which refuses API v10.

A mock has two things to get right, or Sluice will take it for a Cloudflare block: it must put a `Via` header on its
429 and 403 responses, and it must answer `GET /api/v10/gateway`. The alternative is
`CLOUDFLARE_BAN_DETECTION=false`. [bench/mock.go](bench/mock.go) is a mock that does both.

### `OUTBOUND_IP`

The local address Sluice connects to Discord from, for hosts with more than one. Empty leaves the choice to the
operating system. Traffic between cluster nodes ignores it, as it ignores `HTTP_PROXY`.

### `DISABLE_HTTP_2`

Keeps Sluice's connections to Discord on HTTP/1.1. That is the default because HTTP/2 carries many requests on one
connection, and when that connection stalls, every request on it waits. Clients' connections to Sluice are not
affected.

### `REQUEST_TIMEOUT`

How long one attempt at Discord may make no progress. Each of these steps gets that long: connecting and sending the
request's headers, every 64 KiB of an upload, the wait for Discord's response headers, and the rest of the response.
A large upload that keeps moving is therefore never cut off, while a stalled one is. No attempt lasts longer than 10
minutes in total.

An attempt that runs out answers `408` with `X-Sluice-Proxy-Error: true`. Sluice does not send it again, and neither
does discord.js: Discord may already have carried the request out. If the time runs out after Sluice has started
passing Discord's response on, it breaks the connection off, since the status the client received cannot be taken
back.

## Queues and limits

| Setting | Default | Accepts |
| --- | --- | --- |
| `QUEUE_TIMEOUT` | `10000` | 1 to 86,400,000 ms |
| `MAX_QUEUE_DEPTH` | `1000` | 1 to 1,000,000 |
| `MAX_IN_FLIGHT_REQUESTS` | `4096` | 1 to 1,000,000 |
| `BOT_RATELIMIT_OVERRIDES` | empty | `id:limit` pairs |
| `BOT_WIDE_ROUTES` | empty | `METHOD /route` entries |

Sluice never needs to be told a route's limit: it reads each bucket's limit from Discord's responses. What it cannot
read is a bot's global limit, which is Discord's documented 50 requests a second unless `BOT_RATELIMIT_OVERRIDES` says
otherwise. Requests without a token share 50 a second per outgoing IP, and each bearer token is paced at 50 as well.
Interaction endpoints are exempt, as they are at Discord.

### `QUEUE_TIMEOUT`

How long a request may wait for its turn, counted from the moment it arrives. The wait covers its bucket's queue, a
cooldown already in force, the global limit and the pause before a retry. It is not started again for a retry, and it
never interrupts an attempt already on its way to Discord, which `REQUEST_TIMEOUT` bounds.

A request that would wait longer gets a `429` from Sluice with `Retry-After`. Because that request never reached
Discord, sending it again is always safe, and discord.js does so on its own. The `429` carries the headers Discord
uses for the same situation: `X-RateLimit-Remaining: 0` and `X-RateLimit-Reset-After` for a bucket, or
`X-RateLimit-Global: true` for the global limit. When Discord has already announced a cooldown longer than the time
left, Sluice answers with that `429` at once instead of holding the connection open.

With both timeouts at their defaults, every request is answered within 15 seconds, which is discord.js's own request
timeout. Keep `QUEUE_TIMEOUT` plus `REQUEST_TIMEOUT` at or under your library's timeout. A library that gives up first
may send again a request Sluice is still working on.

### `MAX_QUEUE_DEPTH`

How many requests may wait in one queue, not counting the one whose turn it is. A request that finds the queue full
gets the same `429` as one that waited too long, straight away.

### `MAX_IN_FLIGHT_REQUESTS`

How many requests Sluice handles at once, across all tokens and both the public and the cluster listener. A request
counts from the moment it is accepted until its response has been passed on, whether it is waiting, being sent or
streaming back. One over the limit gets `503` with `Retry-After: 1` and `X-Sluice-Proxy-Error: true`. The health
endpoints are not counted, so they keep answering when Sluice is full.

### `BOT_RATELIMIT_OVERRIDES`

The global limit of bots Discord has granted more than 50 requests a second, as a comma-separated list. A bot is named
by its user ID or by the SHA-256 of its token:

```text
BOT_RATELIMIT_OVERRIDES=392827169497284619:100,sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef:75
```

The hash is taken over the token alone, without `Bot` or `Bearer`, and written as 64 lowercase hex digits. Limits are
whole numbers above zero. When both forms match a bot, the user ID wins. Never put a token itself here.

### `BOT_WIDE_ROUTES`

Routes that Discord limits once for the whole bot, although their headers describe a limit per channel, guild or
webhook. Sluice gives each listed route a single queue per token, so a 429 in one guild holds the others back instead
of being repeated in each of them.

Write an entry as the method and the route, with every ID replaced by `!`, exactly as the `route` label of
`sluice_requests` shows it:

```text
BOT_WIDE_ROUTES=GET /guilds/!/vanity-url
```

Separate several with commas. List a route only when its 429s show that guilds share one limit, because every request
on a listed route then waits for the one before it.

## Retries

| Setting | Default | Accepts |
| --- | --- | --- |
| `MAX_RETRY_BODY_BYTES` | `26214400` (25 MiB) | 0 to 1,073,741,824 |
| `MAX_RETRY_CAPTURE_BYTES` | `268435456` (256 MiB) | 0 to 1,099,511,627,776 |

When Discord answers 429, Sluice waits and sends the request again itself, provided it can send the body again. A
request without a body always qualifies. For one with a body, Sluice keeps a copy while it sends the first attempt.
These two settings bound those copies. Keeping them costs throughput on large uploads, which
[BENCHMARKS.md](BENCHMARKS.md#large-uploads) measures.

### `MAX_RETRY_BODY_BYTES`

The largest body Sluice keeps a copy of. A body over the limit is still forwarded, but if Discord answers it with a
429, the client receives that 429 rather than a retry.

A copy of up to 1 MiB stays in memory. A larger one is written to a file in the system's temporary directory, which
Sluice must be able to write to. The file is deleted when the request no longer needs it, which is as soon as
Discord's response headers arrive, unless they are a 429.

### `MAX_RETRY_CAPTURE_BYTES`

The room for all copies held at one time. A request that declares a length for which there is no room gets `503`
before it reaches Discord. A request of unknown length is forwarded without a complete copy, and so without a retry.

`0` turns the copies off. Bodies then pass straight through, and only requests without a body are retried.

## Protecting your IP

| Setting | Default | Accepts |
| --- | --- | --- |
| `DISABLE_401_LOCK` | `false` | `true` or `false` |
| `CLOUDFLARE_BAN_DETECTION` | `true` | `true` or `false` |
| `STATE_FILE` | a file in the user cache directory | A path, or empty |

Discord temporarily blocks an IP at its edge once that IP has received 10,000 responses with status 401, 403 or 429
inside 10 minutes. A 429 whose scope is `shared` is the one exception. Three of Sluice's protections have no setting:

- **The invalid-request budget.** Sluice counts those responses over a rolling 10 minutes and stops sending at 9,500,
  short of Discord's threshold. Until old responses age out, requests get `503`. In a cluster the budget is divided
  beforehand: each node may use 9,500 divided by `CLUSTER_MAX_NODES`, rounded down and never less than 1, which is 296
  with the default of 32 nodes. That division protects an IP the nodes share only if everything sending from that IP
  belongs to the cluster and every node has the same `CLUSTER_MAX_NODES`.
- **Deleted webhooks.** When Discord reports a webhook as unknown (code 10015) or its token as invalid (code 50027),
  Sluice answers calls to that webhook itself for the next hour, because Discord penalises an IP that keeps calling
  one. No other 404 is remembered.
- **Authorization schemes Discord rejects.** A request whose `Authorization` scheme is not `Bot`, `Bearer` or `Basic`
  gets its `401` from Sluice, on routes where the token is what authenticates, instead of collecting one from Discord.

No proxy can promise that Discord never answers 429. Discord changes limits, leaves headers out of some responses and
reports the emoji routes' quota inaccurately, as its
[rate-limit documentation](https://docs.discord.com/developers/topics/rate-limits) says. Sluice prevents the 429s it
can see coming and counts the rest against the budget.

### `DISABLE_401_LOCK`

By default, the first `401` a token receives on a route it authenticates marks the token as invalid, and Sluice answers
every later request with that token itself, with a `401` in Discord's shape. Calls authenticated by a webhook token or
an interaction token in their path are left out in both directions: a `401` there says nothing about the bot's token,
and a bot token already marked invalid does not block them.

Turn the lock off, with `true`, only when a token Discord has rejected can start working again as it is. Sluice
forgets what it knew about a token once the token has been idle, with no cooldown in force, for more than 10 minutes.

### `CLOUDFLARE_BAN_DETECTION`

Discord's own responses carry a `Via` header. A 429 or 403 without one therefore came from the edge in front of
Discord. Sluice passes such a response to the client without retrying it and stops sending that client's requests on
that route for the response's `Retry-After`, or for 60 seconds when there is none, and never for more than an hour.
Then it finds out what was refused by requesting `/api/v10/gateway` itself, with its own `User-Agent` and no token,
while other traffic carries on:

- **The edge refuses that request too.** The IP is blocked. Sluice sends nothing for the `Retry-After`, answers every
  request in the meantime with a 429 that has no `Via`, so that clients react as they would to the block itself, and
  reports `503` on `/sluice/health/upstream`.
- **Discord answers it.** Only that client's requests were refused, for instance because of their `User-Agent`.
  Nothing else stops, and Sluice does not check again for 30 seconds.
- **No answer arrives.** That settles nothing. The route stays backed off, and the next refusal triggers a new check.

Set it to `false` when something between Sluice and Discord removes `Via`, such as an outbound proxy.

### `STATE_FILE`

Where Sluice keeps what protects your IP across a restart: the invalid responses of the last 10 minutes, a Cloudflare
block still in force, and the webhooks found deleted or invalid in the last hour. Discord goes on counting all three
while Sluice is down, so a restart without this file starts from a count that is too low.

Sluice reads the file when it starts, replaces it atomically every 5 seconds while its contents change, and writes it
once more when it stops. The file holds no token, and webhooks appear only as hashes. A file Sluice cannot read is
renamed with `.invalid` at the end rather than overwritten. A saved block is not restored while
`CLOUDFLARE_BAN_DETECTION` is `false`.

The default is `sluice/state-<PORT>.json` in the user's cache directory, which is `~/.cache` on Linux and
`%LocalAppData%` on Windows. An empty value keeps the state in memory only. Two instances that use the same port on
different addresses of one host need a file each. A container loses the default file when it is recreated, so mount a
volume and point `STATE_FILE` at it, for example `/state/sluice.json` on a volume that user 65532 can write.

## Memory bounds

| Setting | Default | Accepts |
| --- | --- | --- |
| `MAX_CLIENT_STATES` | `4096` | 1 to 1,000,000 |
| `MAX_BEARER_COUNT` | `1024` | 1 to 1,000,000 |
| `MAX_BUCKET_STATES` | `65536` | 1 to 10,000,000 |

Everything Sluice remembers has a ceiling, so traffic cannot make it grow without limit. At a ceiling Sluice makes
room by dropping what is idle, and when nothing is idle it answers `503` instead of taking on more.

### `MAX_CLIENT_STATES`

How many tokens Sluice tracks at once, bot and bearer together. A token's entry can be dropped only while it is idle
and under no cooldown. When every entry is in use, a request with a new token gets `503`.

### `MAX_BEARER_COUNT`

How many of those entries may be bearer tokens. At the limit Sluice drops the bearer token that has gone unused the
longest, if that is more than 10 minutes and it has no request in progress and no cooldown in force. Otherwise the new
token gets `503`.

### `MAX_BUCKET_STATES`

How many buckets Sluice tracks, counting both the provisional bucket a route starts in and the names Discord then
gives them. Idle buckets are cleared out periodically. At the limit, a request that needs a new bucket gets `503`.

If the limit is reached just as Discord names a bucket, Sluice keeps the route in its provisional bucket, cooldown
included. Until there is room again, two routes that Discord counts as one bucket are queued apart.

## Logs, metrics and profiling

| Setting | Default | Accepts |
| --- | --- | --- |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `LOG_FORMAT` | `text` | `text` or `json` |
| `ENABLE_METRICS` | `true` | `true` or `false` |
| `METRICS_PORT` | `9000` | 1 to 65535 |
| `METRICS_NAMESPACE` | `sluice` | A metric name prefix |
| `ENABLE_PPROF` | `false` | `true` or `false` |
| `PPROF_PORT` | `7654` | 1 to 65535 |

### `LOG_LEVEL`

The least severe level that is logged. Three more names are accepted from older configurations: `trace` logs as
`debug`, and `fatal` and `panic` log as `error`.

### `LOG_FORMAT`

`text` writes a line of `key=value` pairs per record, and `json` writes a JSON object per line. In both, Sluice blanks
out bot, webhook and interaction tokens wherever they appear, in the message and in every field.

### `ENABLE_METRICS`

Serves Prometheus metrics at `/metrics` on `METRICS_PORT`. Turned off, the listener is not opened, and Sluice skips
the request histogram, the count of requests in progress and the cluster-routing counters. The error counter is still
kept in memory.

What the labels of `sluice_requests` hold:

- **`status`** is the HTTP status as text, such as `200 OK`. Three values are Sluice's own: `429 Shared` for a 429 with
  shared scope, and `429 Edge` and `403 Edge` for a refusal by Discord's edge.
- **`clientId`** is a bot's user ID once Discord has accepted its token and `Unverified` before that. Bearer tokens show
  as `Bearer` and requests without a token as `NoAuth`. After 1,024 different values, further bots share `Other`.
- **`method`** is the request's method, or `OTHER` for one HTTP does not define.
- **`route`** is the path with every ID replaced by `!`. A route longer than 512 bytes, and any new route once 1,024
  are known, is labelled `/unknown`.

The histogram counts Discord's responses, one for every attempt that got as far as response headers. An attempt that
ended in a 429 and was retried is counted too, and an attempt that failed before any headers is not. It is therefore
not a count of the requests clients sent.

`sluice_failures_total` counts the requests Sluice answered with an error of its own, by `reason`: `queue_timeout`,
`queue_full`, `rate_limit_deadline`, `upstream_timeout`, `upstream_error`, `deadline`, `peer_error`, `client_auth` and
`unsupported_authorization`. `sluice_deprecated_api_requests_total` counts requests by the API `version` they name when
that version is `v8` or older, and as `none` when they name no version, which Discord serves as v6. Sluice also logs
each such version the first time it sees it. The README's [metrics table](README.md#metrics) lists the rest.

### `METRICS_PORT`

The port `/metrics` is served on. Like pprof, it binds `BIND_IP` and has no authentication.

### `METRICS_NAMESPACE`

The prefix of every Sluice metric, which gives `sluice_requests`, `sluice_error` and so on by default. It has to match
`[a-zA-Z_][a-zA-Z0-9_]*`. The Go runtime's and the process's own metrics keep their standard names, `go_*` and
`process_*`.

### `ENABLE_PPROF`

Serves Go's profiling endpoints under `/debug/pprof/` on `PPROF_PORT`. Profiles reveal a good deal about a running
process, so never let that port be reached from outside.

### `PPROF_PORT`

The port pprof is served on when `ENABLE_PPROF` is `true`.

### Health and shutdown

The proxy port answers two health checks, which do not count against `MAX_IN_FLIGHT_REQUESTS` and need no
`CLIENT_AUTH_SECRET`:

- **`/sluice/healthz`** says whether this node should receive traffic. It answers `200`, and `503` while Sluice is
  draining or stopping, or while its cluster has more members than `CLUSTER_MAX_NODES`.
- **`/sluice/health/upstream`** says whether Discord is accepting this IP. It answers `503` during a Cloudflare block
  and once 80% of the invalid-request budget is used. Alert on it. Do not restart on it: a restart cures neither.

On `SIGTERM` or `SIGINT` Sluice drains. `/sluice/healthz` turns to `503`, the node leaves its cluster, which may take
up to 5 seconds, the listeners close, and the requests already accepted get up to 15 seconds in all to finish.
Whatever is left then gets `503`, and a second signal stops Sluice immediately. Allow your process manager at least 20
seconds, for instance `--stop-timeout 20` in Docker or `kill_timeout: 20000` in PM2.

## Cluster

| Setting | Default | Accepts |
| --- | --- | --- |
| `CLUSTER_MEMBERS` | empty | `host[:port]` entries |
| `CLUSTER_DNS` | empty | A DNS name |
| `CLUSTER_SECRET` | none | At least 32 characters |
| `CLUSTER_CA_FILE` | none | A PEM file |
| `CLUSTER_CERT_FILE` | none | A PEM file |
| `CLUSTER_KEY_FILE` | none | A PEM file |
| `CLUSTER_PORT` | `7946` | 1 to 65535 |
| `CLUSTER_PEER_PORT` | `8443` | 1 to 65535 |
| `CLUSTER_ADVERTISE_ADDR` | empty | An IP address |
| `CLUSTER_MAX_NODES` | `32` | 1 to 9,500 |
| `NODE_NAME` | generated | A unique name |

Sluice forms a cluster when `CLUSTER_MEMBERS` or `CLUSTER_DNS` is set, and then the secret and the three certificate
files are required. Without either, it is a single node and reads none of the other settings in this section.

In a cluster, every token belongs to exactly one node, which keeps that token's queues and its global pacing. A node
that receives a request for another node's token passes it on. Requests without a token all belong to one node, so
the limit they share per IP has a single keeper too. Interaction calls without a token, which that limit does not
cover, are spread over the nodes instead.

A cluster stays available when nodes cannot see each other, at the price of exactness. Two sides of a network split
can each serve the same token for a while, and a token's requests can be in flight when a membership change moves it
to another node. Requests sent with the same token from outside the cluster are invisible to it altogether.

Every node of a cluster has to run the same Sluice version, so upgrade them together. Open `CLUSTER_PORT` and
`CLUSTER_PEER_PORT` between the nodes and to nobody else.

### `CLUSTER_MEMBERS`

The nodes to join through, as a comma-separated list of `host` or `host:port`. One reachable node is enough, since the
rest are learned from it. Spaces and empty entries are ignored. When this is set, `CLUSTER_DNS` is not used.

```text
CLUSTER_MEMBERS=10.0.0.2,10.0.0.3:7946
```

Sluice refuses to start when it can reach none of them. It never falls back to running alone, where it would count
the same tokens' limits apart from the cluster it was meant to join.

### `CLUSTER_DNS`

A name that resolves to the nodes to join through, each contacted on `CLUSTER_PORT`. A headless Kubernetes service
fits.

### `CLUSTER_SECRET`

The secret that encrypts and authenticates gossip, the same on every node. Generate it randomly. Its SHA-256 serves
as the 32-byte gossip key.

### `CLUSTER_CA_FILE`

The certificate authorities whose certificates nodes accept from each other, in PEM.

### `CLUSTER_CERT_FILE`

This node's certificate chain, in PEM. One certificate serves both ways, to accept other nodes' connections and to
connect to them, so it needs both the server and the client usage, and a subject alternative name for the address the
node advertises. Sluice checks the chain, the dates, the usages and that name before it joins.

### `CLUSTER_KEY_FILE`

The private key of that certificate, in PEM. Let only the Sluice process read it.

Nodes speak TLS 1.3 to each other and verify the other's certificate in both directions. No setting turns that off.

### `CLUSTER_PORT`

The gossip port, on both TCP and UDP.

### `CLUSTER_PEER_PORT`

The HTTPS port on which nodes pass requests to each other. It binds `BIND_IP` and accepts only clients with a
certificate from `CLUSTER_CA_FILE`. Each node announces its port to the others through gossip.

### `CLUSTER_ADVERTISE_ADDR`

The IP address at which the other nodes can reach this one, for when that is not the address it binds, as with Docker
or NAT. Empty lets Sluice detect it. The certificate needs a subject alternative name for this address. Only the
address may be translated: both cluster ports must keep their numbers on the way.

### `CLUSTER_MAX_NODES`

The largest cluster this node will be part of, and the same on every node. Sluice will not join a larger cluster. If
the cluster grows past the number while Sluice is running, the health check and all traffic answer `503` until it
shrinks again.

The number is a commitment, not an estimate. It decides each node's share of the
[invalid-request budget](#protecting-your-ip), whatever the number of nodes actually running.

### `NODE_NAME`

This node's name in the cluster, which has to be unique. By default it is derived from the hostname.

## Retired settings

Sluice still accepts these two settings from older configurations. It ignores them and logs a warning at startup, and
it will stop accepting them in Sluice 2.0.

### `BUFFER_SIZE`

Sized a queue that no longer exists. `MAX_QUEUE_DEPTH` bounds each queue now.

### `DISABLE_GLOBAL_RATELIMIT_DETECTION`

Switched off a guess at each bot's global limit. Sluice makes no such guess: a bot gets Discord's documented 50
requests a second, or what `BOT_RATELIMIT_OVERRIDES` gives it.
