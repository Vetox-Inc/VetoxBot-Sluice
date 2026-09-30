# Nirn Proxy Reborn

Nirn Proxy is a transparent HTTP reverse proxy for Discord's REST API. It coordinates per-route and global rate limits, preserves FIFO dispatch within a bucket, retries safe 429 responses, and exports Prometheus metrics.

Clients send the same method, path, query, application headers, and body they would send to `discord.com`, but use the proxy address instead. As with any reverse proxy, Nirn rewrites `Host`, removes hop-by-hop and internal `X-Nirn-Hop` headers, and supplies a fallback `User-Agent` when one is absent.

```text
https://discord.com/api/v10/gateway
        becomes
http://nirn-proxy:8080/api/v10/gateway
```

The public proxy, metrics, and pprof listeners have no built-in application authentication, and `BIND_IP` defaults to `0.0.0.0`. Restrict them with a firewall or network policy. Discord credentials cross the client-to-proxy hop, so terminate TLS and authenticate callers before that hop leaves a trusted private network. Only the separate cluster-peer listener has built-in mutual TLS.

## Rate-limit model

Discord currently documents these HTTP API limits:

- Authenticated bots: 50 requests per second globally by default. Nirn conservatively applies the same pacing to bearer credentials.
- Unauthenticated requests: 50 requests per second per egress IP.
- Interaction endpoints: exempt from the global limit.
- Invalid requests: 10,000 responses with status 401, 403, or 429 per 10 minutes per egress IP; shared-scope 429s do not count.

See Discord's official [HTTP rate-limit documentation](https://docs.discord.com/developers/topics/rate-limits). Gateway session-start limits are a separate system and are not used to infer REST capacity; see the [Gateway documentation](https://docs.discord.com/developers/events/gateway).

Per-route limits are never hardcoded. Nirn begins with a conservative route grouping, then learns Discord's bucket identity and state from `X-RateLimit-Bucket`, `X-RateLimit-Remaining`, and `X-RateLimit-Reset-After`, using `Retry-After` for 429 responses when available. Buckets remain distinct across Discord's major parameters such as channel, guild, and webhook identity.

Each bucket has a bounded, cancellation-safe FIFO queue. `MAX_QUEUE_DEPTH` bounds waiting requests per gate, while `MAX_IN_FLIGHT_REQUESTS` bounds all admitted non-health requests across the process. `QUEUE_TIMEOUT` is the overall scheduling deadline, including waits, attempts, retries, and the final Discord response stream. The bucket is released after Discord's response headers are processed, so a slow downstream reader cannot block later requests in the same bucket. Shared-bucket FIFO coordination begins after Discord's first response reveals the bucket identity; before then Nirn uses its conservative route grouping.

The proxy also reserves a process-local safety budget for invalid responses. Responses with status 401, 403, or non-shared 429 consume it; Nirn stops its own new upstream attempts at 9,500 retained events per rolling 10 minutes. Cluster nodes statically divide those slots by `CLUSTER_MAX_NODES` to retain shared-NAT headroom. Restarts and traffic outside Nirn are not represented in this history.

## Affinity and clustering

All requests using the same bot or bearer credential are assigned to the same node with rendezvous hashing. That node owns the credential's per-route buckets and global pacer, eliminating the old internal global-limit RPC. Unauthenticated non-interaction traffic is assigned by shared egress affinity so its IP-scoped global limit is coordinated. Interaction traffic bypasses the global pacer but still observes per-route limits.

Clustering uses HashiCorp memberlist/SWIM and is intentionally AP. Gossip requires a shared secret and peer proxying uses a dedicated TLS 1.3 listener with certificate verification in both directions. The peer client transport is direct: it is separate from Discord's transport and never honors `HTTP_PROXY`.

Rate-limit coordination is therefore practical protection, not a mathematical guarantee:

- During a network partition, each side can temporarily process the same credential with independent state.
- Membership changes can move an identity while requests are still in flight.
- Requests made with the same Discord identity outside this cluster are invisible to Nirn.
- Multiple credentials that Discord accounts to one user, or unauthenticated traffic sharing the same external egress IP, can exceed limits that one Nirn state cannot observe.
- Discord may omit rate-limit headers, and it specifically warns that emoji-control quota headers can be inaccurate.
- A previously unseen shared bucket cannot be coordinated until Discord identifies it in a response.
- A non-replayable request body or an exhausted retry deadline exposes Discord's original 429 or a proxy timeout.

Nirn prevents avoidable dispatches and absorbs replay-safe 429s; it cannot guarantee zero 429s. Configure clients and infrastructure to tolerate them.

> **Upgrade note:** token affinity, rendezvous routing, and the peer-hop wire behavior changed. Upgrade the whole cluster as one coordinated deployment; do not run legacy and new nodes together.

Set `CLUSTER_MEMBERS` or `CLUSTER_DNS` to enable clustering. Cluster mode also requires `CLUSTER_SECRET`, `CLUSTER_CA_FILE`, `CLUSTER_CERT_FILE`, and `CLUSTER_KEY_FILE`. Startup verifies the certificate chain, lifetime, client/server usages, and a SAN for the memberlist-advertised IP. Expose the gossip port (TCP and UDP) and peer HTTPS port only between cluster nodes. The advertised IP and peer port must be directly reachable; NAT or peer-port translation is unsupported.

If every configured seed join fails, startup fails rather than silently creating an isolated singleton. Ensure a cold-start discovery set includes the node itself or another reachable seed.

`CLUSTER_MAX_NODES` defaults to 32. Joining a larger cluster fails startup; exceeding the cap after startup makes health checks and Discord requests fail with 503. This cap also partitions the shared invalid-request budget, so every node must use the same value. Traffic outside the cluster that shares the egress IP remains invisible to Nirn.

The only reserved `/nirn/` endpoint on the proxy and peer listeners is `/nirn/healthz`; `/nirn/global` no longer exists. Metrics and pprof use separate listeners.

## 429 retries

Nirn transparently retries a Discord 429 only when the request body can be replayed safely:

- Requests without a body are replayable.
- Bodies with a replay function are replayable.
- Other bodies are captured while first sent, up to `MAX_RETRY_BODY_BYTES`, and are replayable only if capture completes without exceeding that limit.

Retries re-enter the same bounded scheduler and must complete within `QUEUE_TIMEOUT`. If the known cooldown cannot leave a full `REQUEST_TIMEOUT` attempt, Nirn immediately returns Discord's current 429 or a cached 429 with `Retry-After`. If the body cannot be replayed, Nirn returns Discord's original 429 instead of risking a partial or changed request. Other Discord statuses are not retried.

## Proxy responses

| Status | Meaning |
|---|---|
| Discord status | Discord's response after any safe 429 retry is passed through. |
| `429 Too Many Requests` | Discord's current response, or a cached Discord cooldown that cannot fit before the scheduler deadline. |
| `408 Request Timeout` | Nirn's scheduler queue/retry deadline expired. |
| `504 Gateway Timeout` | An outbound Discord attempt exceeded `REQUEST_TIMEOUT`. |
| `502 Bad Gateway` | Discord could not be reached or returned an unusable transport response. |
| `503 Service Unavailable` | Nirn could not safely process the request, such as a full queue, unavailable peer, client-capacity exhaustion, or shutdown. |
| `400 Bad Request` | `CONNECT` and protocol upgrades are unsupported. |

Proxy-generated 408, 502, 503, and 504 responses include `X-Nirn-Proxy-Error: true`.
If a deadline expires after Discord response headers were forwarded, Nirn aborts the response stream because its status can no longer be changed.

Nirn does not disguise an internal failure as a Discord 429.

Nirn caches an invalid credential after its first ordinary 401. Idle state is eventually reclaimed. Set `DISABLE_401_LOCK=true` only when credential caching is undesirable.

## Configuration

All settings are optional in stand-alone mode, and a local `.env` file is loaded when present. Malformed or unreadable `.env` files fail startup; cluster mode requires its secret and TLS files. See [CONFIG.md](CONFIG.md) for the complete environment-variable reference, defaults, validation ranges, and override syntax.

## Metrics and health

With metrics enabled, unauthenticated `/metrics` is served on `BIND_IP:METRICS_PORT`. `nirn_proxy_requests` observes Discord responses—one for each outbound attempt that returns response headers—so a transparent retry adds another observation. It excludes transport failures before headers and is not a count of logical inbound requests. `nirn_proxy_failures_total{reason}` separates scheduler, upstream, peer, and known-rate-limit deadline failures. The legacy `clientId` label contains only `Bot`, `Bearer`, or `NoAuth`, never an ID or token. Unknown methods become `OTHER`, numeric route components are normalized, and excessive or oversized route labels collapse to `/unknown`.

The legacy `nirn_proxy_open_connections` name is retained for dashboard compatibility, but the gauge counts active handler requests rather than TCP sockets.

An importable dashboard is included at [grafana/nirn-proxy-dashboard.json](grafana/nirn-proxy-dashboard.json).

| Metric | Labels |
|---|---|
| `nirn_proxy_error` | none |
| `nirn_proxy_failures_total` | `reason` |
| `nirn_proxy_requests` | `method`, `status`, `route`, `clientId` |
| `nirn_proxy_open_connections` | `method`, `route` |
| `nirn_proxy_requests_routed_sent` | none |
| `nirn_proxy_requests_routed_received` | none |
| `nirn_proxy_requests_routed_error` | none |

`GET /nirn/healthz` returns 200 while the proxy is available and 503 during shutdown or cluster over-capacity. When pprof is enabled, its endpoints are available under `http://BIND_IP:PPROF_PORT/debug/pprof/`; do not expose them publicly. Enabled listeners are bound before startup completes, so a port conflict fails startup.

## Running

Build and run locally with a supported Go toolchain:

```sh
go run .
```

Release binaries and container images are available from the repository's [releases](https://github.com/Melonly-Moderation/nirn-proxy/releases) and [packages](https://github.com/Melonly-Moderation/nirn-proxy/pkgs/container/nirn-proxy).

Nirn Proxy is licensed under the terms in [LICENSE](LICENSE).
