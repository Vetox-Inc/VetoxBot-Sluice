# VetoxBot-Sluice

Sluice: Vetox's public Discord REST rate-limit proxy, in Go, shipped as GitHub release binaries, the
`ghcr.io/vetox-inc/sluice` image and the npm package `@vetox-bot/sluice`. Vetox's Bot sends its REST traffic through
it; so can anyone else. It is a standalone service with no Kafka, Redis, MongoDB or i18n surface.

- **Invariants that must never regress:** [CONTRIBUTING.md → Design invariants](CONTRIBUTING.md#design-invariants).
- **Releasing:** [CONTRIBUTING.md → Releasing](CONTRIBUTING.md#releasing). The version lives only in the git tag.
- **Lineage stays intact.** Sluice is a GPL-3.0 fork of nirn-proxy through Melonly-Moderation's rewrite. The upstream
  commits stay in the history, and README.md → Origins and license keeps the credits, nirn-proxy's acknowledgements
  verbatim and the GPL §5(a) modification notice. Code from another project must be GPL-3.0-compatible and keep its
  notice.
- **The docs speak for Sluice.** The owner wants nirn-proxy named only where it is a fact a reader needs: README.md's
  final section, MIGRATING.md, and the closing line of the changelog's 1.0.0 entry and of the npm README. Nowhere else,
  and never as the frame for describing Sluice. The identifiers kept for compatibility (`/nirn/healthz`, the
  `nirn_proxy` metrics prefix) are documented in MIGRATING.md only.
- **Public repository:** no Vetox production figures, hostnames, tokens or Doppler names anywhere: code, docs, tests
  or commits.

## Constraints

- Go 1.26+ (`go.mod`); CI tests 1.26 and 1.27. `CGO_ENABLED=0`, so every binary and the distroless image are static.
- Settings: `settingGroups` in `config.go`, the variables the code reads, and `CONFIG.md` must agree.
  `TestConfigurationReferenceMatchesCodeAndDocs` enforces it.
- npm: a launcher plus one exactly pinned optional dependency per platform, built from GoReleaser's `dist/` by
  `scripts/npm-packages.mjs`, with no postinstall script. A release rebuilds that `dist/` from its published archives,
  so npm ships the released binaries.
- `STATE_FILE` defaults to a file in the user cache directory, so anything that runs the binary for a test or a tool
  sets `STATE_FILE=` (empty) to keep it from writing there.
- Responses Sluice generates keep Discord's shapes: JSON errors, and 429s with `X-RateLimit-*` headers. discord.js reads
  both, and a request that may have reached Discord never gets a retryable 429.

## Verification

`gofmt -l .` prints nothing; `go vet ./...`, `go test -race ./...` and `golangci-lint run` pass. Path-handling changes
also get the fuzz targets in `internal/proxy/routes_fuzz_test.go`, which CI runs for 30 s each; keep a failing input the
fuzzer writes to `testdata/fuzz/` as a regression seed. Packaging changes also pass
`goreleaser release --snapshot --clean` and `node scripts/pack-test.mjs`. Without a local Go toolchain, run
them in `golang:1.27` (not `-alpine`, which cannot run `-race`) and `golangci/golangci-lint:v2.14.0`, with
`MSYS_NO_PATHCONV=1` under Git Bash.

`bench/` is a stand-alone command that the proxy never imports. `bench/run.sh` reproduces BENCHMARKS.md against a
released image, or against `SLUICE_IMAGE`; a full run takes about 90 minutes and wants a machine doing nothing else,
so never run the gates above beside it. Its results are checked against the mock's own counts, and a mismatch fails
the run.
