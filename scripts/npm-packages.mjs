// Builds the npm packages from GoReleaser's dist/: one per platform binary, plus
// @vetox-bot/sluice, whose optionalDependencies pin them to the same version.
import { chmodSync, copyFileSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('..', import.meta.url));
const operatingSystems = { linux: 'linux', darwin: 'darwin', windows: 'win32' };
const cpus = { amd64: 'x64', arm64: 'arm64' };
const readJSON = (file) => JSON.parse(readFileSync(file, 'utf8'));
const writeJSON = (file, value) => writeFileSync(file, JSON.stringify(value, null, 2) + '\n');

export function buildPackages({ dist = join(root, 'dist') } = {}) {
  const out = join(dist, 'npm');
  const version = readJSON(join(dist, 'metadata.json')).version;
  const main = readJSON(join(root, 'npm/sluice/package.json'));
  rmSync(out, { recursive: true, force: true });

  const optionalDependencies = {};
  for (const artifact of readJSON(join(dist, 'artifacts.json'))) {
    const os = operatingSystems[artifact.goos];
    const cpu = cpus[artifact.goarch];
    if (artifact.type !== 'Binary' || !os || !cpu) continue;
    const name = `${main.name}-${os}-${cpu}`;
    const directory = join(out, `sluice-${os}-${cpu}`);
    const executable = join(directory, 'bin', os === 'win32' ? 'sluice.exe' : 'sluice');
    mkdirSync(join(directory, 'bin'), { recursive: true });
    copyFileSync(join(root, artifact.path), executable);
    chmodSync(executable, 0o755);
    copyFileSync(join(root, 'LICENSE'), join(directory, 'LICENSE'));
    writeJSON(join(directory, 'package.json'), {
      name,
      version,
      description: `The ${os}-${cpu} binary of ${main.name}`,
      license: main.license,
      homepage: main.homepage,
      repository: main.repository,
      os: [os],
      cpu: [cpu],
      files: ['bin'],
      preferUnplugged: true,
      publishConfig: main.publishConfig,
    });
    optionalDependencies[name] = version;
  }
  if (Object.keys(optionalDependencies).length === 0) throw new Error('dist/artifacts.json lists no npm platform binaries');

  const mainDirectory = join(out, 'sluice');
  mkdirSync(join(mainDirectory, 'bin'), { recursive: true });
  for (const file of ['index.js', 'index.d.ts', 'README.md', 'bin/sluice.js']) {
    copyFileSync(join(root, 'npm/sluice', file), join(mainDirectory, file));
  }
  chmodSync(join(mainDirectory, 'bin/sluice.js'), 0o755);
  copyFileSync(join(root, 'LICENSE'), join(mainDirectory, 'LICENSE'));
  const sorted = Object.fromEntries(Object.entries(optionalDependencies).sort(([a], [b]) => a.localeCompare(b)));
  writeJSON(join(mainDirectory, 'package.json'), { ...main, version, optionalDependencies: sorted });
  return { out, version, platforms: Object.keys(sorted) };
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const { out, version, platforms } = buildPackages();
  console.log(`Built @vetox-bot/sluice ${version} with ${platforms.length} platform packages in ${out}`);
}
