# VetoxBot-Sluice

Sluice: Vetox's public Discord REST rate-limit proxy, in Go, shipped as GitHub release binaries, the
`ghcr.io/vetox-inc/sluice` image and the npm package `@vetox-bot/sluice`. Vetox's Bot sends its REST traffic through
it; so can anyone else. It is a standalone service with no Kafka, Redis, MongoDB or i18n surface.

- **Invariants that must never regress:** [CONTRIBUTING.md → Design invariants](CONTRIBUTING.md#design-invariants).
- **Releasing:** [CONTRIBUTING.md → Releasing](CONTRIBUTING.md#releasing). The version lives only in the git tag.
- **Lineage stays intact.** Sluice is a GPL-3.0 fork of nirn-proxy through Melonly-Moderation's rewrite. The owner
  requires every credit kept: the upstream commits, README.md → Lineage and credits, and nirn-proxy's acknowledgements
  verbatim. README.md → License is also the GPL §5(a) modification notice. Code from another project must be
  GPL-3.0-compatible and keep its notice.
- **Public repository:** no Vetox production figures, hostnames, tokens or Doppler names anywhere: code, docs, tests
  or commits.

## Constraints

- Go 1.26+ (`go.mod`); CI tests 1.26 and 1.27. `CGO_ENABLED=0`, so every binary and the distroless image are static.
- Settings: `settingGroups` in `config.go`, the variables the code reads, and `CONFIG.md` must agree.
  `TestConfigurationReferenceMatchesCodeAndDocs` enforces it.
- npm: a launcher plus one exactly pinned optional dependency per platform, built from GoReleaser's `dist/` by
  `scripts/npm-packages.mjs`, with no postinstall script.

## Verification

`gofmt -l .` prints nothing; `go vet ./...`, `go test -race ./...` and `golangci-lint run` pass. Packaging changes
also pass `goreleaser release --snapshot --clean` and `node scripts/pack-test.mjs`. Without a local Go toolchain, run
them in `golang:1.27` (not `-alpine`, which cannot run `-race`) and `golangci/golangci-lint:v2.14.0`, with
`MSYS_NO_PATHCONV=1` under Git Bash.
