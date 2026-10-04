# Contributing

Thanks for helping. Bug reports, fixes and improvements are all welcome, and everyone taking part follows the
[Code of Conduct](CODE_OF_CONDUCT.md). For a larger change, open an issue first so the approach can be agreed before
you build it.

## Setup

```sh
git clone https://github.com/Vetox-Inc/VetoxBot-Sluice.git
cd VetoxBot-Sluice
go build -o sluice .
```

Go 1.26 or later; CI tests 1.26 and 1.27. The packaging checks also need [GoReleaser](https://goreleaser.com) v2 and
Node.js 20 or later.

## Before you open a pull request

```sh
gofmt -l .           # must print nothing
go vet ./...
go test -race ./...  # Discord is faked in-process: no network or token needed
golangci-lint run    # v2
```

All four must pass, and `go test` also runs every fuzz seed. A change to path handling should also get some fuzzing,
which CI runs for 30 seconds a target:

```sh
go test -run '^$' -fuzz '^FuzzPathClassificationIsStable$' -fuzztime 30s ./internal/proxy
```

Without a local Go toolchain, run them in the official image. Use `golang`, not an `-alpine` tag, because `-race`
needs cgo:

```sh
docker run --rm -v "$PWD:/src" -w /src golang:1.27 go test -race ./...
```

A change to what Sluice answers its clients must also pass discord.js, which CI runs against the built binary and a
mock Discord:

```sh
go build -o compat/discordjs/sluice .
cd compat/discordjs && npm ci && node harness.mjs
```

A change to a metric or to `prometheus/alerts.yml` must keep the tests of the alert rules passing. `promtool` comes
with Prometheus:

```sh
promtool test rules prometheus/alerts.test.yml
```

A change to packaging, the Dockerfile or the npm launcher must also pass:

```sh
goreleaser release --snapshot --clean  # builds every target into dist/
node scripts/pack-test.mjs             # installs the npm packages from dist/ and runs Sluice against a mock Discord
docker build .
```

A change that could move latency or throughput should come with a run of the benchmark, which needs only Docker.
[BENCHMARKS.md](BENCHMARKS.md) explains what it measures and how to read it:

```sh
bench/run.sh
```

A behavior change needs a test that fails without it, and a user-visible change needs a `CHANGELOG.md` entry under
`Unreleased`. A new or changed setting goes in `settingGroups` in `config.go` and in `CONFIG.md`; a test fails when
they disagree. The same holds for a reason of `sluice_failures_total`: `failureReasons` in
`internal/proxy/failures.go`, the calls that count it and the tables in `CONFIG.md` must agree.

## Design invariants

A change must not break these. If one has to bend, say so in the pull request.

- **One request per bucket at a time, first in first out.** Discord's per-route limits and message order depend on it.
- **The global limit is taken when a request is sent**, at Discord's documented rate or `BOT_RATELIMIT_OVERRIDES`,
  never inferred. Interaction callbacks and follow-ups are exempt, as Discord documents.
- **Every wait ends.** A request leaves its queue when its client disconnects, its deadline passes or the proxy shuts
  down, and an attempt ends once it goes `REQUEST_TIMEOUT` without progress, or after 10 minutes.
- **All state is bounded.** Clients, buckets, queues, retained bodies, fail-fast entries, known applications and metric
  labels each have a cap.
- **The invalid-request budget is never spent carelessly.** Sluice stops at 9,500 invalid responses per 10 minutes,
  pauses entirely while Discord's edge blocks its IP, and remembers both across restarts.
- **Requests reach Discord as sent:** path encoding, query, headers and body, apart from the documented rewrites.
- **Responses Sluice generates are marked** with `generated-by-proxy: true` and `Via: 1.1 sluice`. The one exception is
  the Cloudflare-pause 429, which omits `Via` on purpose.
- **Responses Sluice generates use Discord's shapes:** errors as `{"message", "code"}` JSON, and 429s with Discord's
  `X-RateLimit-*` headers, so Discord libraries handle them unchanged. A request that never reached Discord gets a 429,
  which clients retry safely; one that may have reached it never does.
- **Credentials never reach logs or metric labels.** Every log goes through the redacting logger, and `clientId` holds
  only validated bot user IDs.
- **A setting keeps its name and meaning** from one release to the next, so existing deployments keep working. One
  that no longer has an effect is still accepted, with a warning, until the next major release.

## Releasing

The version lives only in the git tag, which is stamped into the binaries, the image and the npm packages. To release,
maintainers move the `Unreleased` entries in `CHANGELOG.md` under a `## X.Y.Z - YYYY-MM-DD` heading, then push a
`vX.Y.Z` tag on `master`. The release workflow runs the whole of CI on the tagged commit and waits for approval in
the `release` environment, then publishes, all with build provenance:

- the GitHub release, with that changelog section as its notes, and binaries, checksums and SBOMs
- the multi-arch image `ghcr.io/vetox-inc/sluice`, tagged `X.Y.Z`, `X.Y`, `X` and `latest`
- the npm packages, `@vetox-bot/sluice` and one package per platform, built from that release's archives. They are
  staged: each goes live once a maintainer approves it on npmjs.com under Staged Packages, the platform packages
  first and `@vetox-bot/sluice` last, so it never points at a platform package that is not live yet

The workflow stops before publishing anything when a CI job fails, the tag is not on `master`, the changelog has no
section for the version or npm rejects the token. A tag with a pre-release suffix, such as `v1.0.0-rc.1`, can leave
its entries under `Unreleased` instead. It publishes a GitHub pre-release, the npm `next` tag and only its exact image
tag, so the pipeline can be rehearsed without moving `latest`. The exception is a package's first version, which npm
always makes `latest`.

npm and the image are published by jobs of their own once the GitHub release exists, so one failing leaves the other
alone. If a job fails, re-run the failed jobs: an image that is already published is left alone, npm skips the
packages that are already published, and the GitHub release's assets are replaced only if its own job runs again. A
package that is staged but not approved yet is skipped only when the job can list staged packages, which trusted
publishing cannot. Otherwise npm refuses that version as already staged: approve it, then re-run. The npm and Image
workflows can also be run by hand for a tag on `master`, to finish a release whose run can no longer be re-run.

The approval is asked for where a secret or a registry is at stake: before the checks that read the npm token, and
before npm and the image are published. The job that builds the GitHub release runs after the first of those and
holds no secret of the environment, so re-running that job alone publishes the commit the run was approved for
without asking again.

The setup behind this, done once:

- A `release` environment with required reviewers, limited to `v*` tags and, for runs by hand, `master`.
- An organization that allows public container packages, and the `sluice` package on GHCR made public after its first
  push.
- npm trusted publishing on each package: `release.yml` and `npm.yml` as trusted publishers, both with the `release`
  environment. Staging is the only action they need, and there is no secret to renew.
- For a package that does not exist yet, an `NPM_TOKEN` secret in that environment: a granular token that can stage
  under `@vetox-bot`, with no 2FA bypass. A trusted publisher can be added only to a package that exists, so the token
  stages its first version. Delete the secret once every package has its trusted publishers.

## Reporting bugs

Open an issue with the Sluice version (`sluice --version`), your configuration without secrets, what you expected and
what happened. Report security issues privately, as described in [SECURITY.md](SECURITY.md).
