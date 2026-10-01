# Configuration

All variables are optional in stand-alone mode. Enabling clustering requires the secret and TLS files described below. Sluice also loads a local `.env` file when present. Invalid values and listener bind failures stop startup instead of silently degrading the service.

Durations are integer milliseconds. Boolean settings should be `true` or `false`.

## Server and transport

### `LOG_LEVEL`

One of nirn-proxy's level names: `trace`, `debug`, `info`, `warn`, `error`, `fatal` or `panic`. `trace` logs as `debug`; `fatal` and `panic` log as `error`. Default: `info`.

### `LOG_FORMAT`

`text` writes one `key=value` line per record and `json` one JSON object per line. Default: `text`. Webhook, interaction and bot tokens are redacted from the message and every field in both formats.

### `BIND_IP`

Address used by the proxy, memberlist gossip, cluster-peer, metrics, and pprof listeners. Default: `0.0.0.0`.

The metrics and pprof listeners have no built-in authentication, and the proxy listener has only `CLIENT_AUTH_SECRET`. Use `127.0.0.1`, a private interface, and firewall or network-policy restrictions when they should not be public.

Cluster mode requires `BIND_IP` to be a literal IP address; hostnames are rejected so memberlist cannot accidentally fall back to a wildcard bind.

### `PORT`

Proxy HTTP port, from 1 through 65535. Default: `8080`.

### `CLIENT_AUTH_SECRET`

When set, clients must send it in the `X-Sluice-Auth` header; any other request gets `403` with `X-Sluice-Proxy-Error: true`. It must contain at least 32 characters. The health endpoints stay open, and cluster peers authenticate with mutual TLS instead. Sluice removes the header before a request reaches Discord or a peer. Default: empty, which accepts every client that can reach the port.

Set it whenever other hosts can reach the proxy port: anyone who can send requests through Sluice can spend your IP's invalid-request budget. The secret travels over plain HTTP, so keep that traffic on a trusted network. In discord.js, pass it with the other REST options:

```js
new Client({ intents, rest: { api: 'http://sluice:8080/api', headers: { 'X-Sluice-Auth': secret } } })
```

A 403 rather than a 401 answers a missing secret, because discord.js discards its bot token on a 401.

### `OUTBOUND_IP`

Optional local source IP for outbound Discord connections. Default: empty, which lets the operating system choose. Cluster-peer traffic uses its own direct transport and never uses this address or `HTTP_PROXY`.

### `REQUEST_TIMEOUT`

How long one Discord attempt may go without progress: connecting and sending the request headers, each 64 KiB of an upload, the wait from the end of the upload to Discord's response headers, and from those to the end of the response. An upload that keeps moving is not cut short, however large, while one that stalls is; no attempt lasts more than 10 minutes. Valid range: 1 through 86,400,000 milliseconds. Default: `5000`.

An attempt that exceeds it answers `408 Request Timeout` with `X-Sluice-Proxy-Error: true`, as nirn-proxy did. Discord may already have acted on that request, and clients such as discord.js do not retry a 408. If the timeout passes after Discord's response headers were forwarded, Sluice aborts the response stream, because its status can no longer change.

### `DISABLE_HTTP_2`

Disables HTTP/2 on outbound connections when `true`. It does not affect the inbound server. Default: `true` because a stalled multiplexed connection can delay unrelated Discord requests.

### `DISCORD_API_URL`

Base URL requests are forwarded to. Default: `https://discord.com`. Point it at a mock or staging server for testing. It must use `https` unless the host is loopback, and must not include a path. Never use `discordapp.com`, which rejects API v10. A mock must send `Via` on its 429 and 403 responses and answer `GET /api/v10/gateway`, or set `CLOUDFLARE_BAN_DETECTION=false`, because Sluice reads a 429 or 403 without `Via` as a refusal by Discord's edge and checks whether this IP is blocked.

## Scheduling and retries

### `QUEUE_TIMEOUT`

How long a request may wait for its turn: its bucket's queue, a known cooldown, the global limit and the pause before a retry. It counts from the request's arrival and is not renewed per retry. It never cuts short an attempt already sent to Discord, which `REQUEST_TIMEOUT` bounds. Valid range: 1 through 86,400,000 milliseconds. Default: `10000`.

With the defaults, Sluice answers every request within 15 seconds, discord.js's default request timeout. Keep `QUEUE_TIMEOUT` plus `REQUEST_TIMEOUT` at or below your library's timeout: a library that gives up first may retry a request Sluice is still holding.

A request whose wait would outlast this deadline gets a `429` from Sluice with `Retry-After` and Discord's headers for an exhausted bucket (`X-RateLimit-Remaining: 0`, `X-RateLimit-Reset-After`), or `X-RateLimit-Global: true` when the global limit held it. That request never reached Discord, so clients retry it safely, and discord.js waits and retries on its own. When a cooldown Discord already announced outlasts this deadline, Sluice returns the current Discord `429`, or that cached `429`, at once instead of holding the connection.

### `BOT_WIDE_ROUTES`

Comma-separated routes that Discord limits per bot although their headers suggest a limit per channel, guild or webhook. Sluice queues each listed route in one bucket per credential instead of one per channel, guild or webhook, so a 429 on one guild holds back the others instead of being repeated on each. Default: empty.

Write each entry as a method and the route as the `route` label of `sluice_requests` shows it, identifiers as `!`:

```text
BOT_WIDE_ROUTES=GET /guilds/!/vanity-url
```

List a route only when its 429s show that Discord shares one limit across guilds: every request on a listed route waits for the one before it.

### `MAX_QUEUE_DEPTH`

Maximum number of waiting requests on each FIFO gate. The request currently holding the gate is not included. Valid range: 1 through 1,000,000. Default: `1000`.

A request that finds its queue full gets a `429` at once, as described under `QUEUE_TIMEOUT`.

### `MAX_IN_FLIGHT_REQUESTS`

Process-wide maximum number of non-health requests admitted through the proxy handler, including requests received on the public and cluster-peer listeners. Queued requests, active upstream attempts, and streamed responses all occupy a slot. Valid range: 1 through 1,000,000. Default: `4096`.

The health endpoints (`/sluice/healthz`, its nirn-proxy alias `/nirn/healthz`, and `/sluice/health/upstream`) bypass this limit so they remain observable during saturation. Excess requests fail immediately with `503 Service Unavailable`, `Retry-After: 1`, and `X-Sluice-Proxy-Error: true`.

### `MAX_RETRY_BODY_BYTES`

Maximum request-body size Sluice captures while sending the first attempt when no replay function is available. Valid range: 0 through 1,073,741,824 bytes. Default: `26214400` (25 MiB). Captures larger than 1 MiB spill to the system temporary directory, which must be writable. Spill files are removed when retry handling finishes—immediately after response headers when no retry is needed.

Sluice retries a Discord 429 only if the body is replayable. Requests without bodies and requests that supply a replay function do not depend on this capture limit. If capture is incomplete or exceeds the limit, the original 429 is returned.

### `MAX_RETRY_CAPTURE_BYTES`

Process-wide capacity for request bodies currently being captured for possible retry. Valid range: 0 through 1,099,511,627,776 bytes. Default: `268435456` (256 MiB). When capacity is nonzero, known-length requests that do not fit fail before contacting Discord; unknown-length captures become non-replayable when capacity is exhausted. Set to `0` to pass bodies without a replay function through without capturing or retrying them.

### `MAX_BEARER_COUNT`

Maximum number of bearer-token client states retained at once, additionally bounded by `MAX_CLIENT_STATES`. Valid range: 1 through 1,000,000. Default: `1024`.

When the limit is reached, Sluice can evict only the oldest state that has been untouched for more than 10 minutes, has no active request, and has no live global or route block. Otherwise a new bearer client receives `503 Service Unavailable`.

### `MAX_CLIENT_STATES`

Maximum combined number of bot and bearer credential states. Valid range: 1 through 1,000,000. Default: `4096`. Only idle, unblocked states can be evicted; admission fails with 503 when no safe eviction candidate exists.

### `MAX_BUCKET_STATES`

Process-wide capacity for learned rate-limit buckets and aliases. Valid range: 1 through 10,000,000. Default: `65536`. Capacity is reclaimed when idle state is swept; allocating a new optimistic bucket fails with 503 rather than growing memory without bound. If learning an alias would exceed the cap, Sluice retains that route's blocked optimistic bucket instead of discarding its rate state, but coordination with another route in the same Discord bucket remains degraded until capacity is available.

## Global and invalid-request protection

Discord's documented default global capacity is 50 requests per second for each authenticated bot and 50 requests per second per egress IP without authentication. Sluice applies the same pace conservatively to bearer credentials. Interaction endpoints bypass this global pacer. Per-route capacities are learned from Discord response headers and are never configured here.

Discord can change limits, omit headers, and return inaccurate emoji-control quota headers, so zero 429s cannot be guaranteed; see the official [rate-limit documentation](https://docs.discord.com/developers/topics/rate-limits).

In stand-alone mode, Sluice stops new upstream attempts after recording 9,500 invalid responses in a rolling 10-minute window, leaving headroom below Discord's documented 10,000-response Cloudflare threshold. Statuses 401, 403, and non-shared 429 count toward this process-wide egress budget; it resets continuously as old responses expire.

In cluster mode, each node receives `max(1, floor(9500 / CLUSTER_MAX_NODES))` slots. With the default maximum of 32 nodes, that is 296 per node and at most 9,472 across the cluster. This static partition protects a shared-NAT budget only when every process using that egress is in this cluster, every node uses the same maximum, and the actual membership never exceeds it. Each node keeps its own rolling history across restarts in `STATE_FILE`.

Sluice answers requests to a webhook that Discord reported as deleted (code 10015), or whose token it rejected (code 50027), itself for an hour, because Discord restricts IPs that keep calling such webhooks. Other 404s, such as a missing message, are never cached.

An `Authorization` header with a scheme other than `Bot`, `Bearer` or `Basic` gets a Discord-shaped `401` from Sluice on routes the credential authenticates, because Discord rejects every other scheme with a 401 that counts against the budget.

### `BOT_RATELIMIT_OVERRIDES`

Comma-separated explicit global capacities for credentials with Discord-approved elevated limits. Default: empty.

Keys may be either:

- A numeric bot user ID: `<bot_id>:<requests_per_second>`
- A token fingerprint: `sha256:<64 lowercase hex characters>:<requests_per_second>`

The fingerprint is SHA-256 of the credential itself, without the `Bot` or `Bearer` scheme. Examples:

```text
BOT_RATELIMIT_OVERRIDES=392827169497284619:100,sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef:75
```

Limits must be positive integers. A bot-ID override takes precedence over a fingerprint override. Never place a raw token in this setting.

### `DISABLE_401_LOCK`

When `false`, the first ordinary authenticated 401 marks that credential invalid and later requests fail locally with Discord-shaped 401 responses. Interaction endpoints and webhook-token routes are excluded both ways: their 401s judge the token in the path rather than the credential, so they never mark it invalid and are never refused because of it. Default: `false`.

Set this to `true` only if credentials can become valid again without changing their token. Cached validity is reclaimed after the credential has been inactive and unblocked for more than 10 minutes.

### `CLOUDFLARE_BAN_DETECTION`

When `true`, a 429 or 403 without Discord's `Via` header means Discord's edge answered rather than Discord. Sluice passes that response to the client without retrying it, backs that client's route off for the response's `Retry-After` (60 seconds when it is absent, at most an hour), and requests `/api/v10/gateway` itself, with its own User-Agent and no credential, while other traffic continues:

- If the edge refuses that request too, this IP is blocked. Every outbound request from this node pauses for the `Retry-After`; requests during the pause get a synthetic 429 without `Via`, so clients back off as they would for the real block; and `/sluice/health/upstream` reports 503.
- If Discord answers it, the edge refused only that client's requests, for example over its User-Agent, and nothing else pauses. Sluice checks again no sooner than 30 seconds later.
- If it gets no answer at all, which proves nothing, only the refused route backs off, and the next refusal checks again.

Set `false` if an egress proxy between Sluice and Discord strips `Via`. Default: `true`.

### `STATE_FILE`

Where Sluice keeps, across restarts, the memory that protects this IP: the invalid responses of the last 10 minutes, a Cloudflare block in progress, and the deleted or invalid webhooks of the last hour. Without it, a restart forgets what Discord still counts. Sluice reads the file at startup, rewrites it atomically every 5 seconds while something changes and again at shutdown. It holds no tokens: webhooks appear only as hashes. A file it cannot read is renamed with `.invalid` appended and never overwritten, and a saved block is not restored while `CLOUDFLARE_BAN_DETECTION` is `false`.

Default: `sluice/state-<PORT>.json` in the user cache directory, such as `~/.cache` on Linux and `%LocalAppData%` on Windows. An empty value keeps the state in memory only. Instances on one host that share a port on different addresses need a `STATE_FILE` each. A container keeps the default file across restarts but not when it is recreated, so point `STATE_FILE` at a volume, such as `/state/sluice.json` with a volume at `/state` that user 65532 can write.

## Observability

### `ENABLE_METRICS`

Enables Prometheus metrics and serves them on `/metrics`. When `false`, Sluice disables that listener and skips the request histogram, active-request gauge, and cluster-routing observations. Error-level logs may still increment the process-local error counter. Default: `true`.

`sluice_requests` measures Discord responses, one for each outbound attempt that returns response headers, including absorbed 429 attempts. It excludes transport failures before headers and is not a count of logical inbound requests. Its `status` label is the HTTP status, with `429 Shared` for a shared-scope 429 and `429 Edge` or `403 Edge` for a refusal by Discord's edge. Its `clientId` label is the bot's user ID once Discord has accepted that token, `Unverified` until then, and `Bearer` or `NoAuth` for other traffic; once the label has 1,024 values, further bots share `Other`. Unknown methods use `OTHER`; excessive or oversized route labels collapse to `/unknown`.

`sluice_failures_total{reason}` counts bounded proxy failure reasons: `queue_timeout`, `queue_full`, `rate_limit_deadline`, `upstream_timeout`, `upstream_error`, `deadline`, `peer_error`, `client_auth` and `unsupported_authorization`.

`sluice_queue_wait_seconds{method,route}` measures how long requests waited for their bucket and the global limit before their first Discord attempt. `sluice_invalid_requests` is the current rolling 10-minute invalid-response count, `sluice_webhook_short_circuits_total` counts requests answered from the webhook fail-fast cache, `sluice_cloudflare_blocked` and `sluice_cloudflare_blocks_total` track confirmed Cloudflare blocks, and `sluice_edge_refusals_total` counts every response from Discord's edge rather than Discord. `sluice_deprecated_api_requests_total{version}` counts requests that name a deprecated or discontinued API version (`v8` or earlier) or none (`none`), which Discord serves as its deprecated default, v6; Sluice also logs each version the first time it sees it.

### Health endpoints

`/sluice/healthz` is liveness: 200 unless the proxy is draining for shutdown, shutting down, or the cluster exceeds `CLUSTER_MAX_NODES`. nirn-proxy's `/nirn/healthz` still answers the same way, but it is deprecated and will be removed in Sluice 2.0; Sluice logs a warning the first time it is used. `/sluice/health/upstream` answers 503 while a Cloudflare block is active or at least 80% of the invalid-request budget is used; use it for alerting, not for restarts.

On `SIGTERM` or `SIGINT`, Sluice drains: liveness answers 503, the node leaves its cluster, the listeners close, and requests already admitted get up to 15 seconds to finish (in a cluster, including up to 5 to leave it) before the rest are cancelled with `503`. A second signal stops Sluice at once. Give your process manager at least 20 seconds to stop Sluice, for example `--stop-timeout 20` for Docker or `kill_timeout: 20000` for PM2.

### `METRICS_PORT`

Metrics HTTP port, from 1 through 65535. Default: `9000`.

### `METRICS_NAMESPACE`

Prefix of every Sluice metric. Default: `sluice`, which gives `sluice_requests`, `sluice_error` and so on. Set `nirn_proxy` to keep nirn-proxy's exact metric names, so existing dashboards and alerts keep working. Must match `[a-zA-Z_][a-zA-Z0-9_]*`. Go runtime and process metrics (`go_*`, `process_*`) are never prefixed.

### `ENABLE_PPROF`

Serves Go pprof handlers under `/debug/pprof/`. Default: `false`.

### `PPROF_PORT`

pprof HTTP port, from 1 through 65535. Default: `7654`.

Metrics and pprof both bind to `BIND_IP` without application authentication. Profiling and operational data can be sensitive; do not expose either listener publicly.

## Clustering

Clustering is disabled when both `CLUSTER_MEMBERS` and `CLUSTER_DNS` are empty. Stand-alone mode does not load or require any cluster secret or TLS file.

When clustering is enabled, startup stops if no configured seed can be joined. A node never silently becomes a separate singleton when none of its seeds can be reached because of a network, DNS, or secret mismatch.

### `CLUSTER_PORT`

memberlist gossip port, from 1 through 65535. Default: `7946`.

Memberlist uses this port for both TCP and UDP. Gossip is encrypted and authenticated with the key derived from `CLUSTER_SECRET`.

### `CLUSTER_PEER_PORT`

Dedicated HTTPS port for proxy traffic between members, from 1 through 65535. Default: `8443`. It binds to `BIND_IP`, requires a valid client certificate, and is advertised through memberlist metadata. Peer traffic uses a direct transport and never honors `HTTP_PROXY`.

### `CLUSTER_ADVERTISE_ADDR`

IP address other members use to reach this node, for Docker or NAT where the bind address is not reachable. Default: empty, which advertises the address memberlist detects. The peer certificate needs a SAN for the advertised address, and `CLUSTER_PORT` and `CLUSTER_PEER_PORT` must keep the same numbers across the translation.

### `CLUSTER_MAX_NODES`

Maximum permitted membership, from 1 through 9,500. Default: `32`. Startup is rejected if the joined membership is larger. If membership later exceeds the cap, health checks and Discord traffic fail with 503 until it falls back within the cap. Configure the same value on every node.

This value also statically partitions the invalid-request safety budget between nodes; it is a capacity commitment, not merely the expected replica count.

### `CLUSTER_SECRET`

Shared memberlist secret, required only when clustering is enabled. It must contain at least 32 characters. Sluice supplies SHA-256 of the secret as memberlist's 32-byte secret key. Use a randomly generated value and configure the same value on every node.

### `CLUSTER_CA_FILE`

PEM file containing the CA certificates trusted for peer mTLS. Required only when clustering is enabled.

### `CLUSTER_CERT_FILE`

PEM certificate chain presented for both the TLS server and client roles. Required only when clustering is enabled. Before joining, Sluice verifies the chain, validity period, both authentication usages, and a SAN for the memberlist-advertised IP address because peers connect to that address.

### `CLUSTER_KEY_FILE`

PEM private key for `CLUSTER_CERT_FILE`. Required only when clustering is enabled. Restrict its filesystem permissions to the Sluice process.

Peer TLS requires TLS 1.3. Certificate verification cannot be disabled: servers require and validate client certificates, and clients validate peer server names against the configured CA.

### `CLUSTER_MEMBERS`

Comma-separated seed addresses in `host[:port]` form. Empty entries and surrounding whitespace are ignored. This setting takes precedence over `CLUSTER_DNS`. Default: empty.

Example:

```text
CLUSTER_MEMBERS=10.0.0.2,10.0.0.3:7946
```

### `CLUSTER_DNS`

DNS name resolved to seed-node IPs. Each result uses `CLUSTER_PORT`. A headless Kubernetes service is a typical choice. Default: empty.

### `NODE_NAME`

Optional unique memberlist node name. Default: memberlist's generated host-based name.

Authenticated traffic is assigned by token affinity, while unauthenticated non-interaction traffic is assigned by shared egress affinity. The cluster is AP: partitions and membership changes can temporarily duplicate rate-limit state, and traffic using the same Discord identity outside the cluster is not visible.

Memberlist's advertised IP (see `CLUSTER_ADVERTISE_ADDR`) and `CLUSTER_PEER_PORT` must be reachable by every peer. Port translation is unsupported.

Cluster affinity and the peer protocol differ from nirn-proxy's. Upgrade every node as one coordinated deployment; clusters that mix Sluice with nirn-proxy or with other Sluice versions are unsupported. Open `CLUSTER_PORT` (TCP and UDP) and `CLUSTER_PEER_PORT` (TCP) only between trusted nodes. nirn-proxy's `/nirn/global` endpoint no longer exists.

## Compatibility variables

These nirn-proxy settings are accepted, with a warning at startup, until Sluice 2.0.

### `BUFFER_SIZE`

Ignored if present. Use `MAX_QUEUE_DEPTH`; the channel-buffer scheduler no longer exists.

### `DISABLE_GLOBAL_RATELIMIT_DETECTION`

Ignored if present. Sluice never infers REST limits from `/gateway/bot`; it uses Discord's documented default of 50 requests per second plus `BOT_RATELIMIT_OVERRIDES`.
