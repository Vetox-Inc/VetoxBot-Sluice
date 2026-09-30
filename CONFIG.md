# Configuration

All variables are optional in stand-alone mode. Enabling clustering requires the secret and TLS files described below. Nirn also loads a local `.env` file when present. Invalid values and listener bind failures stop startup instead of silently degrading the service.

Durations are integer milliseconds. Boolean settings should be `true` or `false`.

## Server and transport

### `LOG_LEVEL`

Logrus level: `panic`, `fatal`, `error`, `warn`, `info`, `debug`, or `trace`. Default: `info`.

### `BIND_IP`

Address used by the proxy, memberlist gossip, cluster-peer, metrics, and pprof listeners. Default: `0.0.0.0`.

The public proxy, metrics, and pprof listeners have no built-in authentication. Use `127.0.0.1`, a private interface, and firewall or network-policy restrictions when they should not be public.

Cluster mode requires `BIND_IP` to be a literal IP address; hostnames are rejected so memberlist cannot accidentally fall back to a wildcard bind.

### `PORT`

Proxy HTTP port, from 1 through 65535. Default: `8080`.

### `OUTBOUND_IP`

Optional local source IP for outbound Discord connections. Default: empty, which lets the operating system choose. Cluster-peer traffic uses its own direct transport and never uses this address or `HTTP_PROXY`.

### `REQUEST_TIMEOUT`

Per-attempt deadline for sending the Discord request body through consuming its response body. Valid range: 1 through 86,400,000 milliseconds. Default: `5000`.

### `DISABLE_HTTP_2`

Disables HTTP/2 on outbound connections when `true`. It does not affect the inbound server. Default: `true` because a stalled multiplexed connection can delay unrelated Discord requests.

## Scheduling and retries

### `QUEUE_TIMEOUT`

Overall deadline once scheduling begins, including FIFO and global waits, every outbound attempt, retry delays, and final Discord response streaming. It is not renewed per retry. Valid range: 1 through 86,400,000 milliseconds. Default: `60000`.

Before Discord response headers arrive, an expired scheduler queue returns `408 Request Timeout`, while a Discord attempt that exceeds `REQUEST_TIMEOUT` returns `504 Gateway Timeout`. Both responses include `X-Nirn-Proxy-Error: true`. If a deadline expires after response headers were forwarded, Nirn aborts the response stream because its status can no longer be changed.

When Discord has already supplied a cooldown that cannot leave a full `REQUEST_TIMEOUT` attempt before this deadline, Nirn returns the current Discord `429` or a cached `429` with `Retry-After` immediately instead of holding the connection until it times out.

### `MAX_QUEUE_DEPTH`

Maximum number of waiting requests on each FIFO gate. The request currently holding the gate is not included. Valid range: 1 through 1,000,000. Default: `1000`.

A full queue fails fast with `503 Service Unavailable`.

### `MAX_IN_FLIGHT_REQUESTS`

Process-wide maximum number of non-health requests admitted through the proxy handler, including requests received on the public and cluster-peer listeners. Queued requests, active upstream attempts, and streamed responses all occupy a slot. Valid range: 1 through 1,000,000. Default: `4096`.

`/nirn/healthz` bypasses this limit so readiness remains observable during saturation. Excess requests fail immediately with `503 Service Unavailable`, `Retry-After: 1`, and `X-Nirn-Proxy-Error: true`.

### `MAX_RETRY_BODY_BYTES`

Maximum request-body size Nirn captures while sending the first attempt when no replay function is available. Valid range: 0 through 1,073,741,824 bytes. Default: `26214400` (25 MiB). Captures larger than 1 MiB spill to the system temporary directory, which must be writable. Spill files are removed when retry handling finishes—immediately after response headers when no retry is needed.

Nirn retries a Discord 429 only if the body is replayable. Requests without bodies and requests that supply a replay function do not depend on this capture limit. If capture is incomplete or exceeds the limit, the original 429 is returned.

### `MAX_RETRY_CAPTURE_BYTES`

Process-wide capacity for request bodies currently being captured for possible retry. Valid range: 0 through 1,099,511,627,776 bytes. Default: `268435456` (256 MiB). When capacity is nonzero, known-length requests that do not fit fail before contacting Discord; unknown-length captures become non-replayable when capacity is exhausted. Set to `0` to pass bodies without a replay function through without capturing or retrying them.

### `MAX_BEARER_COUNT`

Maximum number of bearer-token client states retained at once, additionally bounded by `MAX_CLIENT_STATES`. Valid range: 1 through 1,000,000. Default: `1024`.

When the limit is reached, Nirn can evict only the oldest state that has been untouched for more than 10 minutes, has no active request, and has no live global or route block. Otherwise a new bearer client receives `503 Service Unavailable`.

### `MAX_CLIENT_STATES`

Maximum combined number of bot and bearer credential states. Valid range: 1 through 1,000,000. Default: `4096`. Only idle, unblocked states can be evicted; admission fails with 503 when no safe eviction candidate exists.

### `MAX_BUCKET_STATES`

Process-wide capacity for learned rate-limit buckets and aliases. Valid range: 1 through 10,000,000. Default: `65536`. Capacity is reclaimed when idle state is swept; allocating a new optimistic bucket fails with 503 rather than growing memory without bound. If learning an alias would exceed the cap, Nirn retains that route's blocked optimistic bucket instead of discarding its rate state, but coordination with another route in the same Discord bucket remains degraded until capacity is available.

## Global and invalid-request protection

Discord's documented default global capacity is 50 requests per second for each authenticated bot and 50 requests per second per egress IP without authentication. Nirn applies the same pace conservatively to bearer credentials. Interaction endpoints bypass this global pacer. Per-route capacities are learned from Discord response headers and are never configured here.

Discord can change limits, omit headers, and return inaccurate emoji-control quota headers, so zero 429s cannot be guaranteed; see the official [rate-limit documentation](https://docs.discord.com/developers/topics/rate-limits).

In stand-alone mode, Nirn stops new upstream attempts after recording 9,500 invalid responses in a rolling 10-minute window, leaving headroom below Discord's documented 10,000-response Cloudflare threshold. Statuses 401, 403, and non-shared 429 count toward this process-wide egress budget; it resets continuously as old responses expire.

In cluster mode, each node receives `max(1, floor(9500 / CLUSTER_MAX_NODES))` slots. With the default maximum of 32 nodes, that is 296 per node and at most 9,472 across the cluster. This static partition protects a shared-NAT budget only when every process using that egress is in this cluster, every node uses the same maximum, and the actual membership never exceeds it. A process restart resets that node's local rolling history, so deployment churn still consumes the shared headroom.

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

When `false`, the first ordinary authenticated 401 marks that credential invalid and later requests fail locally with Discord-shaped 401 responses. Interaction endpoints are excluded. Default: `false`.

Set this to `true` only if credentials can become valid again without changing their token. Cached validity is reclaimed after the credential has been inactive and unblocked for more than 10 minutes.

## Observability

### `ENABLE_METRICS`

Enables Prometheus metrics and serves them on `/metrics`. When `false`, Nirn disables that listener and skips the request histogram, active-request gauge, and cluster-routing observations. Error-level logs may still increment the process-local error counter. Default: `true`.

`nirn_proxy_requests` measures Discord responses, one for each outbound attempt that returns response headers, including absorbed 429 attempts. It excludes transport failures before headers and is not a count of logical inbound requests. Its legacy `clientId` label is only `Bot`, `Bearer`, or `NoAuth`. Unknown methods use `OTHER`; excessive or oversized route labels collapse to `/unknown`.

`nirn_proxy_failures_total{reason}` counts bounded proxy failure reasons, including `queue_timeout`, `upstream_timeout`, `upstream_error`, `peer_error`, and `rate_limit_deadline`.

### `METRICS_PORT`

Metrics HTTP port, from 1 through 65535. Default: `9000`.

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

### `CLUSTER_MAX_NODES`

Maximum permitted membership, from 1 through 9,500. Default: `32`. Startup is rejected if the joined membership is larger. If membership later exceeds the cap, health checks and Discord traffic fail with 503 until it falls back within the cap. Configure the same value on every node.

This value also statically partitions the invalid-request safety budget between nodes; it is a capacity commitment, not merely the expected replica count.

### `CLUSTER_SECRET`

Shared memberlist secret, required only when clustering is enabled. It must contain at least 32 characters. Nirn supplies SHA-256 of the secret as memberlist's 32-byte secret key. Use a randomly generated value and configure the same value on every node.

### `CLUSTER_CA_FILE`

PEM file containing the CA certificates trusted for peer mTLS. Required only when clustering is enabled.

### `CLUSTER_CERT_FILE`

PEM certificate chain presented for both the TLS server and client roles. Required only when clustering is enabled. Before joining, Nirn verifies the chain, validity period, both authentication usages, and a SAN for the memberlist-advertised IP address because peers connect to that address.

### `CLUSTER_KEY_FILE`

PEM private key for `CLUSTER_CERT_FILE`. Required only when clustering is enabled. Restrict its filesystem permissions to the Nirn process.

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

Memberlist's advertised IP and `CLUSTER_PEER_PORT` must be directly reachable by every peer. NAT or peer-port translation is unsupported.

The affinity and peer wire behavior changed from legacy releases. Upgrade every node as one coordinated deployment; mixed-version clusters are unsupported. Open `CLUSTER_PORT` (TCP and UDP) and `CLUSTER_PEER_PORT` (TCP) only between trusted nodes. `/nirn/global` was removed.

## Compatibility variables

### `BUFFER_SIZE`

Ignored if present. Use `MAX_QUEUE_DEPTH`; the channel-buffer scheduler no longer exists.

### `DISABLE_GLOBAL_RATELIMIT_DETECTION`

Ignored if present. Nirn no longer infers REST limits from `/gateway/bot`; it uses Discord's documented default plus `BOT_RATELIMIT_OVERRIDES`.
