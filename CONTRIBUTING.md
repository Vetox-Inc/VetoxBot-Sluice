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

A change to packaging, the Dockerfile or the npm launcher must also pass:

```sh
goreleaser release --snapshot --clean  # builds every target into dist/
node scripts/pack-test.mjs             # installs the npm packages from dist/ and runs Sluice against a mock Discord
docker build .
```

A behavior change needs a test that fails without it, and a user-visible change needs a `CHANGELOG.md` entry under
`Unreleased`. A new or changed setting goes in `settingGroups` in `config.go` and in `CONFIG.md`; a test fails when
they disagree.

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
- **nirn-proxy's settings keep their meaning**, so existing deployments keep working.

## Releasing

The version lives only in the git tag: GoReleaser stamps it into the binaries, the image and the npm packages. To
release, maintainers move the `Unreleased` entries in `CHANGELOG.md` under a `## X.Y.Z - YYYY-MM-DD` heading, then
push a `vX.Y.Z` tag on `master`. The release workflow waits for approval in the `release` environment, then publishes,
all with build provenance:

- the GitHub release, with that changelog section as its notes, and binaries, checksums and SBOMs
- the multi-arch image `ghcr.io/vetox-inc/sluice`, tagged `X.Y.Z`, `X.Y` and `X`
- the npm packages: `@vetox-bot/sluice` and one package per platform

The workflow stops before publishing anything when the changelog has no section for the version or npm rejects the
token. A tag with a pre-release suffix, such as `v1.0.0-rc.1`, can leave its entries under `Unreleased` instead. It
publishes a GitHub pre-release, the npm `next` tag and only its exact image tag, so the pipeline can be rehearsed
without moving `latest`. The exception is a package's first version, which npm always makes `latest`. If a release
fails partway, re-run the workflow: npm skips the packages it already has, the GitHub release's assets are replaced,
and the image, which is pushed last, is built once.

The first release needs one-time setup:

- A `release` environment with required reviewers.
- An `NPM_TOKEN` secret in that environment, from an npm account allowed to publish under `@vetox-bot`.
- After the first image push, make the `sluice` package on GHCR public.
- After the first npm publish, configure npm trusted publishing for `release.yml` on each package, then delete
  `NPM_TOKEN`.

## Reporting bugs

Open an issue with the Sluice version (`sluice --version`), your configuration without secrets, what you expected and
what happened. Report security issues privately, as described in [SECURITY.md](SECURITY.md).
