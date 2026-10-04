// Runs discord.js against Sluice and a mock Discord: the requests it sends, the limits it waits
// out, and how it reads the answers Sluice gives itself.
//
//   go build -o compat/discordjs/sluice . && cd compat/discordjs && npm ci && node harness.mjs
//
// SLUICE_BIN names another binary to test. A run takes about a minute.
import { spawn } from 'node:child_process';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { gzipSync } from 'node:zlib';
import { REST, Routes, DiscordAPIError, RateLimitError, Client } from 'discord.js';

const BOT_ID = '123456789012345678';
// Built at runtime, so that no token-shaped literal sits in the repository.
const TOKEN = ['MTIzNDU2Nzg5MDEyMzQ1Njc4', 'GAbCdE', 'x'.repeat(38)].join('.');
const BAD_TOKEN = ['MjIzNDU2Nzg5MDEyMzQ1Njc4', 'GAbCdE', 'y'.repeat(38)].join('.');
// The shape of an interaction token: "interaction:<id>:<random>" in base64url.
const INTERACTION_TOKEN = Buffer.from(['interaction', '500000000000000001', 'synthetic'].join(':')).toString('base64url');
const SECRET = 's'.repeat(40);
const UPLOAD_BYTES = 25 * 1024 * 1024;
// Longer than the default REQUEST_TIMEOUT of five seconds.
const UPLOAD_MS = 6500;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// ---------- mock Discord ----------
const hits = new Map();
const seen = [];
const count = (key) => hits.get(key) ?? 0;
const respond = (res, status, body, headers = {}) => {
  // Like Discord, a response without a body carries no JSON content type.
  res.writeHead(status, { Via: '1.1 google', ...(body === undefined ? {} : { 'Content-Type': 'application/json' }), ...headers });
  res.end(body === undefined ? undefined : JSON.stringify(body));
};
const discord = createServer((req, res) => handle(req, res).catch((error) => seen.push({ key: 'aborted', headers: {}, error: String(error) })));
async function handle(req, res) {
  // Discord decodes the path; discord.js sends @me as %40me.
  const path = decodeURIComponent(req.url.split('?')[0]);
  const key = `${req.method} ${path}`;
  hits.set(key, count(key) + 1);
  seen.push({ key, headers: req.headers });
  const n = count(key);

  let bytes = 0;
  if (path.endsWith('/channels/100000000000000009/messages')) {
    // Reads the upload at a steady pace that takes UPLOAD_MS however the body is chunked.
    const started = Date.now();
    for await (const chunk of req) {
      bytes += chunk.length;
      const wait = started + (bytes / UPLOAD_BYTES) * UPLOAD_MS - Date.now();
      if (wait > 0) await sleep(wait);
    }
    return respond(res, 200, { id: '1', bytes });
  }
  for await (const chunk of req) bytes += chunk.length;

  switch (true) {
    case path === '/api/v10/gateway' || path === '/api/v6/gateway':
      return respond(res, 200, { url: 'wss://gateway.discord.gg' });
    case path === '/api/v10/gateway/bot':
      return respond(res, 200, { url: 'wss://gateway.discord.gg', shards: 1, session_start_limit: { total: 1000, remaining: 999, reset_after: 0, max_concurrency: 1 } });
    case path === `/api/v10/users/${BOT_ID}`:
      return respond(res, 200, { id: BOT_ID, username: 'sluice-test', discriminator: '0', global_name: null, avatar: null });
    case path === '/api/v10/users/@me':
      if (req.headers.authorization === `Bot ${BAD_TOKEN}`) return respond(res, 401, { message: '401: Unauthorized', code: 0 });
      return respond(res, 200, { id: BOT_ID, username: 'sluice-test', discriminator: '0', bot: true });
    // Bucket exhausted by Discord's own headers.
    case path === '/api/v10/channels/100000000000000001/messages':
      return respond(res, 200, [], { 'X-RateLimit-Limit': '1', 'X-RateLimit-Remaining': '0', 'X-RateLimit-Reset-After': '2', 'X-RateLimit-Bucket': 'b-1' });
    case path === '/api/v10/channels/100000000000000002/messages':
      return respond(res, 200, [], { 'X-RateLimit-Limit': '1', 'X-RateLimit-Remaining': '0', 'X-RateLimit-Reset-After': n === 1 ? '12' : '1', 'X-RateLimit-Bucket': 'b-2' });
    // Shared-scope 429s on reactions.
    case /\/channels\/100000000000000003\/messages\/2000+1\/reactions\//.test(path):
      if (n === 1) return respond(res, 429, { message: 'The resource is being rate limited.', retry_after: 1.6, global: false }, { 'Retry-After': '2', 'X-RateLimit-Limit': '10', 'X-RateLimit-Remaining': '9', 'X-RateLimit-Reset-After': '60', 'X-RateLimit-Bucket': 'r-3', 'X-RateLimit-Scope': 'shared' });
      return respond(res, 204);
    case /\/channels\/100000000000000003\/messages\/2000+3\/reactions\//.test(path):
      return respond(res, 429, { message: 'The resource is being rate limited.', retry_after: 60, global: false }, { 'Retry-After': '60', 'X-RateLimit-Limit': '10', 'X-RateLimit-Remaining': '8', 'X-RateLimit-Reset-After': '60', 'X-RateLimit-Bucket': 'r-3', 'X-RateLimit-Scope': 'shared' });
    case /\/channels\/100000000000000003\/messages\/\d+\/reactions\//.test(path):
      return respond(res, 204);
    // Discord slower than REQUEST_TIMEOUT.
    case path === '/api/v10/channels/100000000000000004/messages':
      await sleep(7000);
      return respond(res, 200, []);
    // Global 429 once.
    case path === '/api/v10/channels/100000000000000007/messages':
      if (n === 1) return respond(res, 429, { message: 'You are being rate limited.', retry_after: 0.4, global: true }, { 'Retry-After': '1', 'X-RateLimit-Global': 'true', 'X-RateLimit-Scope': 'global' });
      return respond(res, 200, []);
    // One request per second on one bucket.
    case path === '/api/v10/channels/100000000000000008/messages':
      await sleep(1000);
      return respond(res, 200, []);
    case path === '/api/v10/channels/100000000000000010/messages/300000000000000001':
      return respond(res, 204);
    case path.startsWith('/api/v10/interactions/'):
      return respond(res, 204);
    case path.startsWith(`/api/v10/webhooks/${BOT_ID}/`):
      return respond(res, 200, { id: '4', content: 'follow-up' });
    case path === '/api/v10/webhooks/600000000000000001/dead-token':
      return respond(res, 404, { message: 'Unknown Webhook', code: 10015 });
    case path === '/api/v10/webhooks/600000000000000002/gzip-token': {
      const body = JSON.stringify({ message: 'Unknown Webhook', code: 10015 });
      if (/gzip/.test(req.headers['accept-encoding'] ?? '')) {
        res.writeHead(404, { Via: '1.1 google', 'Content-Type': 'application/json', 'Content-Encoding': 'gzip' });
        return res.end(gzipSync(body));
      }
      return respond(res, 404, JSON.parse(body));
    }
    default:
      return respond(res, 404, { message: `mock has no ${key}`, code: 0 });
  }
}

// ---------- Sluice ----------
const listen = async (server) => {
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  return server.address().port;
};
const freePort = async () => {
  const server = createServer();
  const port = await listen(server);
  server.close();
  return port;
};
const logs = [];
const binary = process.env.SLUICE_BIN ?? (process.platform === 'win32' ? './sluice.exe' : './sluice');
const startSluice = async (extra = {}) => {
  const port = await freePort();
  const metricsPort = await freePort();
  const child = spawn(binary, [], {
    env: { ...process.env, BIND_IP: '127.0.0.1', PORT: String(port), METRICS_PORT: String(metricsPort), DISCORD_API_URL: `http://127.0.0.1:${discordPort}`, STATE_FILE: '', ...extra },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  child.stderr.on('data', (data) => logs.push(String(data)));
  const base = `http://127.0.0.1:${port}`;
  for (let attempt = 0; ; attempt++) {
    try {
      if ((await fetch(`${base}/sluice/healthz`)).ok) break;
    } catch {}
    if (attempt === 100) throw new Error(`Sluice never became healthy:\n${logs.join('')}`);
    await sleep(100);
  }
  return { child, api: `${base}/api`, metrics: `http://127.0.0.1:${metricsPort}/metrics` };
};

const discordPort = await listen(discord);
const sluice = await startSluice();
const guarded = await startSluice({ CLIENT_AUTH_SECRET: SECRET });
const rest = (options = {}) => new REST({ api: sluice.api, ...options }).setToken(TOKEN);

// ---------- scenarios ----------
const results = [];
const scenario = async (name, fn) => {
  const started = Date.now();
  try {
    const detail = await fn();
    results.push({ name, ok: true, ms: Date.now() - started, detail: detail ?? '' });
  } catch (error) {
    results.push({ name, ok: false, ms: Date.now() - started, detail: error?.stack ?? String(error) });
  }
};
const assert = (condition, message) => {
  if (!condition) throw new Error(message);
};
const thrown = async (promise) => {
  try {
    await promise;
  } catch (error) {
    return error;
  }
  throw new Error('expected the request to fail');
};

await scenario('JSON request, headers and User-Agent reach Discord intact', async () => {
  const gateway = await rest().get(Routes.gateway());
  assert(gateway.url === 'wss://gateway.discord.gg', `gateway = ${JSON.stringify(gateway)}`);
  const request = seen.findLast((entry) => entry.key === 'GET /api/v10/gateway');
  assert(/^DiscordBot \(https:\/\/discord\.js\.org, \d+\.\d+\.\d+\)/.test(request.headers['user-agent']), `UA ${request.headers['user-agent']}`);
  assert(request.headers.authorization === `Bot ${TOKEN}`, 'Authorization changed');
  const reason = 'closed by a moderator ✓';
  await rest().delete(Routes.channelMessage('100000000000000010', '300000000000000001'), { reason });
  const deleted = seen.findLast((entry) => entry.key.startsWith('DELETE'));
  assert(deleted.headers['x-audit-log-reason'] === encodeURIComponent(reason), `audit reason ${deleted.headers['x-audit-log-reason']}`);
  return `UA "${request.headers['user-agent']}"`;
});

await scenario('discord.js Client fetches a user and /gateway/bot through Sluice', async () => {
  const client = new Client({ intents: [], rest: { api: sluice.api } });
  client.rest.setToken(TOKEN);
  const user = await client.users.fetch(BOT_ID);
  const gatewayBot = await client.rest.get(Routes.gatewayBot());
  await client.destroy();
  assert(user.username === 'sluice-test' && gatewayBot.shards === 1, 'unexpected data');
  return `User ${user.id}, ${gatewayBot.shards} shard`;
});

await scenario('25 MiB upload slower than REQUEST_TIMEOUT completes', async () => {
  const started = Date.now();
  const message = await rest().post(Routes.channelMessages('100000000000000009'), {
    body: { content: 'upload' },
    files: [{ name: 'big.bin', data: Buffer.alloc(UPLOAD_BYTES, 7), contentType: 'application/octet-stream' }],
  });
  const seconds = (Date.now() - started) / 1000;
  assert(message.bytes > UPLOAD_BYTES, `Discord received ${message.bytes} bytes`);
  assert(seconds > 5, `upload took only ${seconds}s, not longer than REQUEST_TIMEOUT`);
  return `${message.bytes} bytes in ${seconds.toFixed(1)}s`;
});

await scenario("Discord's bucket headers make discord.js wait", async () => {
  const client = rest();
  await client.get(Routes.channelMessages('100000000000000001'));
  const started = Date.now();
  await client.get(Routes.channelMessages('100000000000000001'));
  const waited = Date.now() - started;
  assert(waited >= 1900 && waited < 8000, `second request after ${waited}ms`);
  assert(count('GET /api/v10/channels/100000000000000001/messages') === 2, 'extra requests reached Discord');
  return `waited ${waited}ms`;
});

await scenario("Sluice's own 429 for an exhausted bucket is waited out like Discord's", async () => {
  await rest().get(Routes.channelMessages('100000000000000002'));
  const other = rest();
  const debug = [];
  other.on('restDebug', (line) => debug.push(line));
  const started = Date.now();
  await other.get(Routes.channelMessages('100000000000000002'));
  const waited = Date.now() - started;
  const unexpected = debug.find((line) => line.includes('Encountered unexpected 429'));
  assert(unexpected, 'discord.js never saw the 429');
  assert(unexpected.includes('Sublimit       : None'), `treated as a sublimit:\n${unexpected}`);
  assert(waited >= 11000, `resolved after ${waited}ms, before the bucket reset`);
  assert(count('GET /api/v10/channels/100000000000000002/messages') === 2, `Discord saw ${count('GET /api/v10/channels/100000000000000002/messages')} requests`);
  return `waited ${waited}ms as a bucket wait; 2 requests reached Discord`;
});

await scenario('Shared-scope 429: one reaction waits, the bucket stays open', async () => {
  const route = (message) => Routes.channelMessageOwnReaction('100000000000000003', message, '%F0%9F%8E%89');
  const started = Date.now();
  const limited = rest().put(route('200000000000000001')).then(() => Date.now() - started, (error) => error);
  await sleep(100);
  const otherStarted = Date.now();
  await rest().put(route('200000000000000002'));
  const other = Date.now() - otherStarted;
  const waited = await limited;
  assert(typeof waited === 'number', `limited reaction failed: ${waited}`);
  assert(other < 1200, `another message's reaction took ${other}ms`);
  assert(waited >= 1500 && waited < 6000, `limited reaction resolved after ${waited}ms`);
  const error = await thrown(rest({ rejectOnRateLimit: () => true }).put(route('200000000000000003')));
  assert(error instanceof RateLimitError && error.scope === 'shared', `long shared limit gave ${error}`);
  const afterStarted = Date.now();
  await rest().put(route('200000000000000004'));
  assert(Date.now() - afterStarted < 1200, 'a long shared limit closed the bucket');
  return `limited reaction ${waited}ms, other ${other}ms, long limit surfaced as RateLimitError(scope=shared)`;
});

await scenario("Sluice's errors are readable DiscordAPIErrors", async () => {
  const error = await thrown(rest().get('/channels/a%2Fb/messages'));
  assert(error instanceof DiscordAPIError && error.status === 400 && error.code === 0 && /encoded separators/.test(error.message), `got ${error}`);
  return `${error.name}: ${error.message}`;
});

await scenario('An attempt past REQUEST_TIMEOUT answers 408 once, unretried', async () => {
  const error = await thrown(rest().get(Routes.channelMessages('100000000000000004')));
  assert(error instanceof DiscordAPIError && error.status === 408 && /deadline/.test(error.message), `got ${error}`);
  assert(count('GET /api/v10/channels/100000000000000004/messages') === 1, 'discord.js retried');
  return `${error.name}: ${error.message}`;
});

await scenario('Interaction callback and follow-up without auth', async () => {
  await rest().post(Routes.interactionCallback('500000000000000001', INTERACTION_TOKEN), { auth: false, body: { type: 4, data: { content: 'hi' } } });
  const followUp = await rest().patch(Routes.webhookMessage(BOT_ID, INTERACTION_TOKEN, '@original'), { auth: false, body: { content: 'edited' } });
  const calls = seen.filter((entry) => entry.key.includes('/interactions/') || entry.key.includes(`/webhooks/${BOT_ID}/`));
  assert(calls.length === 2 && calls.every((entry) => !entry.headers.authorization), 'auth leaked or calls missing');
  return `follow-up ${followUp.content}`;
});

await scenario('Deleted webhook is answered by Sluice after the first 10015', async () => {
  for (let index = 0; index < 2; index++) {
    const error = await thrown(rest().post(Routes.webhook('600000000000000001', 'dead-token'), { auth: false, body: { content: 'x' } }));
    assert(error instanceof DiscordAPIError && error.code === 10015, `call ${index + 1}: ${error}`);
  }
  assert(count('POST /api/v10/webhooks/600000000000000001/dead-token') === 1, 'second call reached Discord');
  return 'DiscordAPIError[10015] twice, Discord called once';
});

await scenario('Compressed 10015 with the fetch strategy still fails fast', async () => {
  const client = rest({ makeRequest: fetch });
  for (let index = 0; index < 2; index++) {
    const error = await thrown(client.post(Routes.webhook('600000000000000002', 'gzip-token'), { auth: false, body: { content: 'x' } }));
    assert(error instanceof DiscordAPIError && error.code === 10015, `call ${index + 1}: ${error}`);
  }
  const request = seen.find((entry) => entry.key === 'POST /api/v10/webhooks/600000000000000002/gzip-token');
  assert(count('POST /api/v10/webhooks/600000000000000002/gzip-token') === 1, 'second call reached Discord');
  return `Discord was asked for "${request.headers['accept-encoding']}"`;
});

await scenario('Invalid token: first 401 from Discord, then locally', async () => {
  const error = await thrown(new REST({ api: sluice.api }).setToken(BAD_TOKEN).get(Routes.user('@me')));
  assert(error instanceof DiscordAPIError && error.status === 401, `first: ${error}`);
  const again = await thrown(new REST({ api: sluice.api }).setToken(BAD_TOKEN).get(Routes.user('@me')));
  assert(again instanceof DiscordAPIError && again.status === 401, `second: ${again}`);
  const reached = seen.filter((entry) => entry.headers.authorization === `Bot ${BAD_TOKEN}`).length;
  assert(reached === 1, `Discord saw the bad token ${reached} times`);
  return 'Discord saw the bad token once';
});

await scenario('Unsupported Authorization scheme is refused locally', async () => {
  const before = seen.length;
  const error = await thrown(new REST({ api: sluice.api, authPrefix: 'Token' }).setToken('abc').get(Routes.user('@me')));
  assert(error instanceof DiscordAPIError && error.status === 401 && seen.length === before, `got ${error}`);
  return `${error.name}: ${error.message}`;
});

await scenario("Discord's global 429 is absorbed by Sluice", async () => {
  const started = Date.now();
  await rest().get(Routes.channelMessages('100000000000000007'));
  const waited = Date.now() - started;
  assert(count('GET /api/v10/channels/100000000000000007/messages') === 2 && waited >= 350, `waited ${waited}ms`);
  return `absorbed in ${waited}ms`;
});

await scenario('Congested bucket: queue deadline turns into a retryable 429, all succeed', async () => {
  const clients = Array.from({ length: 15 }, () => rest());
  let rateLimited = 0;
  for (const client of clients) client.on('restDebug', (line) => line.includes('Encountered unexpected 429') && rateLimited++);
  const started = Date.now();
  const outcomes = await Promise.allSettled(clients.map((client) => client.get(Routes.channelMessages('100000000000000008'))));
  const failed = outcomes.filter((outcome) => outcome.status === 'rejected');
  assert(failed.length === 0, `failed: ${failed.map((outcome) => outcome.reason).join('; ')}`);
  assert(rateLimited > 0, 'no request ran out of QUEUE_TIMEOUT; the scenario did not congest');
  assert(count('GET /api/v10/channels/100000000000000008/messages') === 15, `Discord saw ${count('GET /api/v10/channels/100000000000000008/messages')} requests`);
  return `15/15 succeeded in ${Date.now() - started}ms; ${rateLimited} got Sluice's 429 and retried`;
});

await scenario('Deprecated API version is counted', async () => {
  await new REST({ api: sluice.api, version: '6' }).setToken(TOKEN).get(Routes.gateway());
  const metrics = await (await fetch(sluice.metrics)).text();
  assert(/sluice_deprecated_api_requests_total\{version="v6"\} 1/.test(metrics), 'v6 not counted');
  assert(logs.join('').includes('deprecated or discontinued'), 'no warning logged');
  return 'counted and warned';
});

await scenario('CLIENT_AUTH_SECRET: 403 without it, and the bot token survives', async () => {
  const client = new REST({ api: guarded.api }).setToken(TOKEN);
  const error = await thrown(client.get(Routes.gateway()));
  assert(error instanceof DiscordAPIError && error.status === 403 && /client authentication/.test(error.message), `got ${error}`);
  client.options.headers['X-Sluice-Auth'] = SECRET;
  const gateway = await client.get(Routes.gateway());
  assert(gateway.url, 'no gateway');
  assert(seen.every((entry) => !entry.headers['x-sluice-auth']), 'X-Sluice-Auth reached Discord');
  return `${error.name}: ${error.message}; the same REST then succeeded`;
});

await scenario('Every answer of its own is in sluice_failures_total', async () => {
  const metrics = await (await fetch(sluice.metrics)).text();
  const counted = (reason) => Number(new RegExp(`sluice_failures_total\\{reason="${reason}"\\} (\\d+)`).exec(metrics)?.[1] ?? -1);
  // What the scenarios above made Sluice answer itself, and one reason nothing triggered.
  for (const reason of ['bad_request', 'upstream_timeout', 'invalid_token', 'unsupported_authorization', 'queue_timeout', 'rate_limit_deadline']) {
    assert(counted(reason) >= 1, `${reason} = ${counted(reason)}, want at least 1`);
  }
  assert(counted('upstream_error') === 0, `upstream_error = ${counted('upstream_error')}, want it exported at 0`);
  return 'six reasons counted, an untouched one exported at 0';
});

// ---------- report ----------
sluice.child.kill();
guarded.child.kill();
discord.close();
for (const result of results) {
  console.log(`${result.ok ? 'PASS' : 'FAIL'}  ${result.name}  (${result.ms}ms)\n      ${result.detail.split('\n').join('\n      ')}`);
}
const failed = results.filter((result) => !result.ok).length;
console.log(`\n${results.length - failed}/${results.length} passed`);
process.exit(failed ? 1 : 0);
