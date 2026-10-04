// The artifact gate: packs the npm packages exactly as they would be published, installs the
// tarballs for this platform into a scratch project, and runs the installed binary against a
// mock Discord. Needs dist/ from a GoReleaser build, such as `goreleaser release --snapshot`.
import assert from 'node:assert/strict';
import { execFileSync, execSync, spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtempSync, rmSync } from 'node:fs';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { buildPackages } from './npm-packages.mjs';

const { out } = buildPackages();
const scratch = mkdtempSync(join(tmpdir(), 'sluice-pack-'));
// npm is npm.cmd on Windows, which needs a shell; every interpolated value is a path we created.
const npm = (args, cwd) => execSync(`npm ${args}`, { cwd, encoding: 'utf8', stdio: ['ignore', 'pipe', 'inherit'] });
const pack = (directory) => join(directory, npm('pack --silent', directory).trim().split('\n').pop());

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

let proxy;
const posix = process.platform !== 'win32';
const discord = createServer((request, response) => {
  response.setHeader('Via', '1.1 google');
  response.setHeader('Content-Type', 'application/json');
  // One answer is slow, so that a request is in flight when the proxy is told to stop.
  setTimeout(() => response.end(JSON.stringify({ path: request.url })), request.url.endsWith('/slow') ? 1500 : 0);
});
try {
  const tarballs = [pack(join(out, 'sluice')), pack(join(out, `sluice-${process.platform}-${process.arch}`))];
  npm('init -y', scratch);
  npm(`install --no-audit --no-fund ${tarballs.map((file) => JSON.stringify(file)).join(' ')}`, scratch);
  const launcher = join(scratch, 'node_modules', '@vetox-bot', 'sluice', 'bin', 'sluice.js');

  const version = execFileSync(process.execPath, [launcher, '--version'], { encoding: 'utf8' }).trim();
  assert.match(version, /^sluice \S+$/);

  const discordPort = await listen(discord);
  const start = async () => {
    const port = await freePort();
    proxy = spawn(process.execPath, [launcher], {
      stdio: ['ignore', 'inherit', 'inherit'],
      // Its own process group, so that the group can be stopped the way a process manager stops one.
      detached: posix,
      env: {
        ...process.env,
        BIND_IP: '127.0.0.1',
        PORT: String(port),
        METRICS_PORT: String(await freePort()),
        DISCORD_API_URL: `http://127.0.0.1:${discordPort}`,
        STATE_FILE: '',
      },
    });
    const base = `http://127.0.0.1:${port}`;
    for (let attempt = 0; ; attempt++) {
      try {
        if ((await fetch(`${base}/sluice/healthz`)).ok) break;
      } catch {}
      if (attempt === 100) throw new Error('the installed proxy never became healthy');
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    return base;
  };
  // Stops the proxy while it holds a request, and says how that request and the launcher ended.
  const stopMidRequest = async (stop) => {
    const base = await start();
    const proxied = await (await fetch(`${base}/v10/gateway`)).json();
    assert.equal(proxied.path, '/api/v10/gateway');
    const inFlight = fetch(`${base}/v10/slow`).then((response) => response.status, (error) => error);
    await new Promise((resolve) => setTimeout(resolve, 300));
    stop(proxy);
    // A launcher that keeps the signal to itself never exits.
    const [code] = await once(proxy, 'exit', { signal: AbortSignal.timeout(30_000) });
    proxy = undefined;
    return { status: await inFlight, code };
  };

  // Only the launcher is told to stop, so it has to pass the signal on to the binary.
  const passedOn = await stopMidRequest((launched) => launched.kill('SIGTERM'));
  // Windows has no graceful SIGTERM; the launcher's exit is enough there.
  if (posix) {
    assert.equal(passedOn.status, 200, 'the request in flight when the launcher was stopped');
    assert.equal(passedOn.code, 0);
    // A process manager signals the whole group, so the binary is told to stop twice: by the
    // group and by the launcher passing it on. The request in flight has to finish all the same.
    const group = await stopMidRequest((launched) => process.kill(-launched.pid, 'SIGTERM'));
    assert.equal(group.status, 200, 'the request in flight when the group was stopped');
    assert.equal(group.code, 0);
  }
  console.log(`Pack test passed: ${version} from npm tarballs on ${process.platform}-${process.arch}`);
} finally {
  if (proxy && posix) {
    try {
      process.kill(-proxy.pid, 'SIGKILL');
    } catch {}
  }
  proxy?.kill('SIGKILL');
  discord.close();
  rmSync(scratch, { recursive: true, force: true });
}
