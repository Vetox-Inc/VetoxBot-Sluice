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
  `cloudflare_blocks_total`, `edge_refusals_total`, `webhook_short_circuits_total` and
  `deprecated_api_requests_total`.

### Global rate limit

nirn-proxy inferred raised global limits from `/gateway/bot`. Discord documents no such rule, so Sluice uses Discord's
default of 50 requests per second unless `BOT_RATELIMIT_OVERRIDES` sets a bot's raised limit. **Set it for every bot
whose limit Discord has raised**, or Sluice paces that bot at 50 per second. `DISABLE_GLOBAL_RATELIMIT_DETECTION` is
ignored, with a warning at startup, until Sluice 2.0.

### Scheduling

- Buckets follow Discord's `X-RateLimit-Bucket` headers instead of path guesses. Routes nirn-proxy grouped wrongly
  (channel creation across guilds, `/channels/:id` across channels, `/users/:id` per user) now queue the way Discord
  limits them.
- `BUFFER_SIZE` is ignored, with a warning, until Sluice 2.0; `MAX_QUEUE_DEPTH` bounds each queue instead.
- Sluice retries a 429 itself when it can replay the body.
- nirn-proxy held a request until its bucket freed, however long that took. Sluice waits up to `QUEUE_TIMEOUT` (10
  seconds), then answers `429` with `Retry-After`, so the client retries instead of timing out; a full queue answers
  the same way.
- A shared-scope 429, such as a busy message's reactions, no longer holds up the rest of its bucket.

### Health endpoints

- `/nirn/healthz` still answers, but it is deprecated and goes in Sluice 2.0; `/sluice/healthz` is the new name.
- `/sluice/health/upstream` is new: `503` while this IP is blocked or close to Discord's invalid-request limit.
- Any other path under `/nirn/` or `/sluice/` returns `404`, including nirn-proxy's cluster-internal `/nirn/global`.

### Protecting your IP

These behaviours are new:

- A bot token is judged only by routes it authenticates: webhook-token and interaction calls neither mark it invalid nor
  are refused because of it. An `Authorization` scheme Discord never accepts is refused by Sluice.
- Calls to a webhook Discord reported as unknown (code 10015) or invalid (code 50027) are answered by Sluice for an
  hour.
- A 429 or 403 without Discord's `Via` header came from Discord's edge. When a request of Sluice's own confirms the edge
  blocks this IP, all outbound traffic pauses for its `Retry-After`; otherwise only that route backs off. Set
  `CLOUDFLARE_BAN_DETECTION=false` if something between Sluice and Discord strips `Via`.
- Sluice stops sending at 9,500 invalid responses per 10 minutes and answers `503` until they age out.
- `STATE_FILE` keeps the invalid-request history, a Cloudflare block and the deleted webhooks across restarts. It
  defaults to a file in the user cache directory.
- `CLIENT_AUTH_SECRET` makes clients present a secret in `X-Sluice-Auth`. Set it when other hosts can reach the proxy
  port.

### Responses

- `generated-by-proxy: true` still marks the responses Sluice generates, which now also carry `Via: 1.1 sluice`.
- An attempt that goes `REQUEST_TIMEOUT` without progress answers `408`, as before. The timeout no longer counts the
  upload, so large files are not cut short.
- Sluice's errors are JSON in Discord's error shape, not plain text.
- Requests reach Discord without `Forwarded` and `X-Forwarded-*` headers, and with Sluice's `User-Agent` when the
  client sends none or one that does not start with `DiscordBot`.

### Shutdown

On `SIGTERM`, Sluice drains: liveness fails, the node leaves its cluster, the listeners close, and requests already
admitted get up to 15 seconds to finish. Whatever is left is cancelled with `503` and `Retry-After: 1`, and a second
signal stops Sluice at once. Give your process manager at least 20 seconds to stop it.

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

Sluice's engine is Melonly's rewrite, so its scheduling, settings and clustering carry over. What differs:

- An upstream timeout answers `408` again, as nirn-proxy documented, instead of `504`. `REQUEST_TIMEOUT` now bounds an
  attempt without progress, so an upload that keeps moving is not cut short, and `QUEUE_TIMEOUT` bounds only the wait
  for a turn.
- `QUEUE_TIMEOUT` defaults to 10 seconds instead of 60, so requests are answered within discord.js's 15-second
  timeout. A request that runs out of it, or finds its queue full, gets a `429` with `Retry-After` instead of a `408`
  or `503`.
- Shutdown drains requests in flight for up to 15 seconds instead of cancelling them at once.
- Errors are Discord-shaped JSON instead of plain text.
- `X-Nirn-Proxy-Error` is now `X-Sluice-Proxy-Error`, and the internal `X-Nirn-Hop` header is now `X-Sluice-Hop`, so
  upgrade a cluster in one go.
- Metrics default to the `sluice_` prefix; `METRICS_NAMESPACE=nirn_proxy` restores yours. `clientId` holds bot user IDs
  instead of `Bot`.
- `Forwarded` and `X-Forwarded-*` headers are no longer sent to Discord.
- Repeated slashes in a path are collapsed as Discord does, and paths with an encoded `?` are rejected.
- New: the 401 scope rule, webhook fail-fast, Cloudflare-block detection, `/sluice/health/upstream`, `DISCORD_API_URL`,
  `CLUSTER_ADVERTISE_ADDR`, `LOG_FORMAT`, `METRICS_NAMESPACE`, `CLIENT_AUTH_SECRET`, `STATE_FILE`, `BOT_WIDE_ROUTES`,
  and the metrics listed above.
