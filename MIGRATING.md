# Migrating to Sluice

Sluice can take the place of a single nirn-proxy 1.3 node without a change to its settings: it reads the same
environment variables and listens on the same ports. This page lists what behaves differently afterwards. A cluster,
and Melonly-Moderation's fork, have sections of their own further down.

## Swapping it in

1. Stop the old proxy and start Sluice with the same environment.
2. Give every bot whose global limit Discord has raised an entry in `BOT_RATELIMIT_OVERRIDES`. The old proxy worked a
   raised limit out from `/gateway/bot`, a rule Discord does not document. Sluice does not, and paces a bot without an
   entry at 50 requests a second.
3. Keep the old binary until you are satisfied. Going back is the same swap in reverse.

`BUFFER_SIZE` and `DISABLE_GLOBAL_RATELIMIT_DETECTION` no longer do anything. Sluice accepts both, with a warning at
startup, until 2.0, so they can stay where they are for now.

## What changes

### Metrics and dashboards

- Metric names start with `sluice_`. Set `METRICS_NAMESPACE=nirn_proxy` to keep the old names, and with them your
  dashboards and alerts.
- The `clientId` label is the bot's user ID once Discord has accepted its token, and `Unverified` until then. It used
  to be the bot's `username#discriminator`.
- `open_connections` counts requests in progress, not TCP connections.
- `error` counts the errors Sluice logs, and a failed request is no longer one of them. Requests Sluice fails are
  counted in `failures_total`, by reason, so move an alert on `error` there.
- New metrics: `queue_wait_seconds`, `failures_total`, `invalid_requests`, `cloudflare_blocked`,
  `cloudflare_blocks_total`, `edge_refusals_total`, `webhook_short_circuits_total` and
  `deprecated_api_requests_total`.

### Health checks

- Liveness is `/sluice/healthz`. The old path, `/nirn/healthz`, still answers until Sluice 2.0, and Sluice logs a
  warning the first time it is called.
- `/sluice/health/upstream` is new. It answers `503` while this IP is blocked or close to Discord's invalid-request
  limit.
- Every other path under `/sluice/` or `/nirn/` answers `404`. That includes `/nirn/global`, which cluster nodes used
  to call on each other.

### Queueing

- Buckets follow Discord's `X-RateLimit-Bucket` header rather than a guess from the path. Three groupings change as a
  result: channel creation is queued per guild instead of once for all guilds, `/channels/:id` per channel instead of
  once for all channels, and `/users/:id` per user.
- A request waits at most `QUEUE_TIMEOUT`, 10 seconds by default, for its turn. Then it gets a `429` with
  `Retry-After`, and so does a request that finds its queue full. Before, a request was held until its bucket opened,
  however long that took, and clients timed out instead of retrying. `MAX_QUEUE_DEPTH` bounds each queue.
- After a 429 from Discord, Sluice sends the request again itself when it can replay the body.
- A shared-scope 429, such as one for a busy message's reactions, holds up only its own request. Before, it closed
  the whole bucket for everything but reactions.
- The global limit is counted when a request is sent, not when it arrives.

### Responses

- `generated-by-proxy: true` still marks the responses the proxy generates itself. They now also carry
  `Via: 1.1 sluice`.
- Sluice's own errors are JSON in Discord's error shape instead of plain text.
- An attempt that goes `REQUEST_TIMEOUT` without progress still answers `408`. The timeout no longer runs while an
  upload is moving, so large files are not cut short.
- Requests reach Discord without `Forwarded` and `X-Forwarded-*` headers. Sluice adds its own `User-Agent` when the
  client sends none, and puts it in front of one that does not start with `DiscordBot`.
- A path with dot segments, encoded separators or an encoded `?` is refused with `400`. Repeated slashes are collapsed,
  so a base URL that ends in `/` no longer costs a request its channel, guild or webhook limit.

### Protection that is new

- A bot token is judged only by the routes it authenticates. Calls that carry a webhook token or an interaction token
  in their path neither mark it invalid nor are refused because of it.
- An `Authorization` scheme Discord never accepts gets its `401` from Sluice.
- Calls to a webhook Discord reported as unknown (code 10015) or invalid (code 50027) are answered by Sluice for an
  hour.
- A 429 or 403 without Discord's `Via` header came from Discord's edge. When a request of Sluice's own confirms that
  the edge blocks this IP, all outbound traffic pauses for the `Retry-After`. Otherwise only that route backs off. Set
  `CLOUDFLARE_BAN_DETECTION=false` if something between Sluice and Discord removes `Via`.
- Sluice stops sending at 9,500 invalid responses in 10 minutes and answers `503` until they age out.
- `STATE_FILE` carries the invalid-response count, a block in force and the deleted webhooks across a restart. It
  defaults to a file in the user cache directory.
- `CLIENT_AUTH_SECRET` makes clients present a secret in `X-Sluice-Auth`. Set it when other hosts can reach the proxy
  port.

### Shutdown

On `SIGTERM`, Sluice drains: liveness fails, the node leaves its cluster, the listeners close, and requests already
accepted get up to 15 seconds to finish. Whatever is left then gets `503` with `Retry-After: 1`, and a second signal
stops Sluice at once. Allow your process manager at least 20 seconds.

### Logs

Logs come from Go's `log/slog`: one line of `key=value` pairs per record, or JSON with `LOG_FORMAT=json`. `LOG_LEVEL`
still accepts every level name it used to. Update any parser that expects the old format.

### Faults that are gone

- A queue cleared out while requests were waiting in it left them waiting forever.
- Reaction paths with encoded characters such as `#` did not reach Discord intact.
- Routes with a non-numeric identifier, such as templates and activity instances, created a bucket and a metric label
  for each identifier.
- A cooldown was timed by the whole seconds of `Retry-After`. Sluice uses the exact `retry_after` of Discord's body.

## Clusters

- The protocol between nodes is new. Every node needs `CLUSTER_SECRET` and certificates (`CLUSTER_CA_FILE`,
  `CLUSTER_CERT_FILE`, `CLUSTER_KEY_FILE`), and nodes pass requests to each other on a port of its own,
  `CLUSTER_PEER_PORT`, 8443 by default.
- A cluster cannot mix the two proxies, so replace every node together.
- A request routed to a node that has failed gets `503` with `Retry-After: 1`. It used to get a made-up 429.
- `CLUSTER_ADVERTISE_ADDR` covers nodes behind Docker or NAT.

## From Melonly-Moderation's fork

Sluice's engine is that fork's rewrite, so its queueing, settings and clustering carry over. What differs:

- An upstream timeout answers `408` again instead of `504`. `REQUEST_TIMEOUT` bounds an attempt that makes no
  progress, so an upload that keeps moving is not cut short, and `QUEUE_TIMEOUT` bounds only the wait for a turn.
- `QUEUE_TIMEOUT` defaults to 10 seconds instead of 60, so requests are answered within discord.js's 15-second
  timeout. A request that runs out of it, or finds its queue full, gets a `429` with `Retry-After` instead of a `408`
  or `503`.
- Shutdown drains the requests in flight for up to 15 seconds instead of cancelling them at once.
- Errors are JSON in Discord's shape instead of plain text.
- `X-Nirn-Proxy-Error` is now `X-Sluice-Proxy-Error`, and the internal `X-Nirn-Hop` header is now `X-Sluice-Hop`, so
  replace a cluster's nodes together.
- Metrics default to the `sluice_` prefix, which `METRICS_NAMESPACE` changes back. `clientId` holds bot user IDs
  instead of `Bot`.
- `Forwarded` and `X-Forwarded-*` headers are no longer sent to Discord.
- Repeated slashes in a path are collapsed, and a path with an encoded `?` is refused.
- New: judging a token only by the routes it authenticates, the answers for deleted webhooks, Cloudflare-block
  detection, `/sluice/health/upstream`, `DISCORD_API_URL`, `CLUSTER_ADVERTISE_ADDR`, `LOG_FORMAT`,
  `METRICS_NAMESPACE`, `CLIENT_AUTH_SECRET`, `STATE_FILE`, `BOT_WIDE_ROUTES`, and the metrics listed above.
