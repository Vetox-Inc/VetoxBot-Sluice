// Prints the CHANGELOG.md section for a release tag, as the GitHub release's notes. A pre-release without a section of
// its own takes Unreleased. Relative links are pinned to the tag, because a release page resolves them against itself.
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('..', import.meta.url));
const { repository } = JSON.parse(readFileSync(join(root, 'npm/sluice/package.json'), 'utf8'));
const repositoryURL = repository.url.replace(/^git\+/, '').replace(/\.git$/, '');

export function releaseNotes(tag, changelog = readFileSync(join(root, 'CHANGELOG.md'), 'utf8')) {
  const version = tag.replace(/^v/, '');
  const sections = new Map();
  let section;
  for (const line of changelog.split(/\r?\n/)) {
    const heading = /^## \[?v?([^\]\s]+)/.exec(line);
    if (heading) sections.set(heading[1], (section = []));
    else section?.push(line);
  }
  const notes = (sections.get(version) ?? (version.includes('-') ? sections.get('Unreleased') : undefined))
    ?.join('\n')
    .trim();
  if (!notes) throw new Error(`CHANGELOG.md has no entries for ${version}`);
  return notes.replace(/\]\((?![a-z][a-z\d+.-]*:|[#/])([^)\s]+)\)/gi, `](${repositoryURL}/blob/${tag}/$1)`) + '\n';
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  if (!process.argv[2]) throw new Error('Usage: node scripts/release-notes.mjs <tag>');
  process.stdout.write(releaseNotes(process.argv[2]));
}
