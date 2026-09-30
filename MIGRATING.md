# Migrating to Sluice

## From nirn-proxy 1.3

A single nirn-proxy node can be replaced in place: Sluice reads the same environment variables and listens on the same
default ports. Stop nirn-proxy, start Sluice with the same settings, and keep the old binary until you are satisfied.
Then review what changes.

### Metrics

- Names start with `sluice_`. Set `METRICS_NAMESPACE=nirn_proxy` to keep `nirn_proxy_*`, so existing dashboards and
  alerts keep working.
- The `clientId` label holds a bot's user ID once Discord has accepted its token, and `Unverified` until then, instead
  of the bot's `username#discriminator`.
- `open_connections` counts requests being handled, not TCP connections.
- New metrics: `queue_wait_seconds`, `failures_total`, `invalid_requests`, `cloudflare_blocked`,
  `cloudflare_blocks_total` and `webhook_short_circuits_total`.

### Global rate limit

nirn-proxy inferred raised global limits from `/gateway/bot`. Discord documents no such rule, so Sluice uses Discord's
default of 50 requests per second unless `BOT_RATELIMIT_OVERRIDES` sets a bot's raised limit. **Set it for every bot
whose limit Discord has raised**, or Sluice paces that bot at 50 per second. `DISABLE_GLOBAL_RATELIMIT_DETECTION` is
ignored, with a warning at startup.

### Scheduling

- Buckets follow Discord's `X-RateLimit-Bucket` headers instead of path guesses. Routes nirn-proxy grouped wrongly
  (channel creation across guilds, `/channels/:id` across channels, `/users/:id` per user) now queue the way Discord
  limits them.
- `BUFFER_SIZE` is ignored, with a warning; `MAX_QUEUE_DEPTH` bounds each queue instead.
- Sluice retries a 429 itself when it can replay the body.

### Health endpoints

- `/nirn/healthz` still answers; `/sluice/healthz` is the new name.
- `/sluice/health/upstream` is new: `503` while this IP is blocked or close to Discord's invalid-request limit.
- Any other path under `/nirn/` or `/sluice/` returns `404`, including nirn-proxy's cluster-internal `/nirn/global`.

### Protecting your IP

These behaviours are new:

- A bot token is judged only by routes it authenticates: webhook-token and interaction calls neither mark it invalid nor
  are refused because of it.
- Calls to a webhook Discord reported as unknown (code 10015) or invalid (code 50027) are answered by Sluice for an
  hour.
- A 429 or 403 without Discord's `Via` header pauses all outbound traffic for its `Retry-After`. Set
  `CLOUDFLARE_BAN_DETECTION=false` if something between Sluice and Discord strips `Via`.
- Sluice stops sending at 9,500 invalid responses per 10 minutes and answers `503` until they age out.

### Responses

- `generated-by-proxy: true` still marks the responses Sluice generates, which now also carry `Via: 1.1 sluice`.
- Timeouts answer `408`, as before.
- Requests reach Discord without `Forwarded` and `X-Forwarded-*` headers, and with Sluice's `User-Agent` when the
  client sends none.

### Shutdown

On shutdown, Sluice cancels queued and in-flight requests, answering `503` with `Retry-After: 1` wherever the response
has not started, instead of draining them. Clients should retry.

### Clustering

- The peer protocol changed. Every node needs `CLUSTER_SECRET` and mutual-TLS certificates (`CLUSTER_CA_FILE`,
  `CLUSTER_CERT_FILE`, `CLUSTER_KEY_FILE`), and nodes forward requests over a new port, `CLUSTER_PEER_PORT` (default
  8443).
- A request routed to a failed node gets `503` with `Retry-After: 1`, where nirn-proxy returned a mocked 429.
- Upgrade every node together: a cluster cannot mix Sluice with nirn-proxy.
- `CLUSTER_ADVERTISE_ADDR` covers nodes behind Docker or NAT.

### Logs

Logs come from Go's `log/slog`: `key=value` text by default, or JSON with `LOG_FORMAT=json`. `LOG_LEVEL` accepts
nirn-proxy's level names. Update any log parser that expects nirn-proxy's format.

## From Melonly-Moderation/nirn-proxy

Sluice's engine is Melonly's rewrite, so its scheduling, settings, clustering and shutdown carry over. What differs:

- An upstream timeout answers `408` again, as nirn-proxy documented, instead of `504`.
- `X-Nirn-Proxy-Error` is now `X-Sluice-Proxy-Error`, and the internal `X-Nirn-Hop` header is now `X-Sluice-Hop`, so
  upgrade a cluster in one go.
- Metrics default to the `sluice_` prefix; `METRICS_NAMESPACE=nirn_proxy` restores yours. `clientId` holds bot user IDs
  instead of `Bot`.
- `Forwarded` and `X-Forwarded-*` headers are no longer sent to Discord.
- Repeated slashes in a path are collapsed as Discord does, and paths with an encoded `?` are rejected.
- New: the 401 scope rule, webhook fail-fast, Cloudflare-block detection, `/sluice/health/upstream`, `DISCORD_API_URL`,
  `CLUSTER_ADVERTISE_ADDR`, `LOG_FORMAT`, `METRICS_NAMESPACE`, and the metrics listed above.
