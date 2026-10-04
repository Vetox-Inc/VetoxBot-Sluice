#!/usr/bin/env bash
# Runs the benchmark BENCHMARKS.md reports: each scenario straight to a mock Discord and through Sluice.
#
#   bench/run.sh                 every suite, then the report
#   bench/run.sh limits report   only these, in this order
#
# Suites: latency, ceiling, upload, limits, traffic, soak. It needs Docker and bash, and nothing else.
# A run that stops part-way can be started again: results already written are kept.
set -euo pipefail

SLUICE_IMAGE="${SLUICE_IMAGE:-ghcr.io/vetox-inc/sluice:1.0.0}"
GO_IMAGE="${GO_IMAGE:-golang:1.27}"
RUN_IMAGE="${RUN_IMAGE:-gcr.io/distroless/static-debian12:nonroot}"
GO_CACHE="${GO_CACHE:-sluice-bench-gobuild}"
REPS="${REPS:-3}"
SOAK="${SOAK:-20m}"
# One CPU each, so no part of a run competes with another for its core.
CPU_MOCK="${CPU_MOCK:-0}"
CPU_SLUICE="${CPU_SLUICE:-1}"
CPU_LOAD="${CPU_LOAD:-2}"
CPU_SPARE="${CPU_SPARE:-3}"

root="$(cd "$(dirname "$0")/.." && pwd)"
# Git Bash on Windows: Docker needs the Windows form of the path, unconverted.
if windows="$(cd "$root" && pwd -W 2>/dev/null)"; then root="$windows"; fi
export MSYS_NO_PATHCONV=1
out="$root/bench/.out"
results="${RESULTS:-$root/bench/results/${SLUICE_IMAGE##*:}}"

bot=900000000000000001
unlimited=(-route-limit 1000000 -route-window 1s -global-limit 1000000)
# Discord's limit on new messages in a channel and its default per bot, answered in 25 ms.
discord=(-route-limit 5 -route-window 5s -global-limit 50 -latency 25ms)
raised=(-e "BOT_RATELIMIT_OVERRIDES=$bot:1000000")
mock_flags=()
sluice_env=()

say() { echo "$(date -u +%H:%M:%S) $*"; }

cleanup() {
  docker rm -f sluice-bench-load sluice-bench-load-direct sluice-bench-sut sluice-bench-mock \
    sluice-bench-mock-direct > /dev/null 2>&1 || true
}
trap cleanup EXIT

build() {
  mkdir -p "$out" "$results"
  say "building the bench binary"
  docker run --rm -v "$root:/src" -v "$GO_CACHE:/root/.cache/go-build" -w /src -e CGO_ENABLED=0 \
    -e GOFLAGS=-buildvcs=false "$GO_IMAGE" go build -trimpath -o /src/bench/.out/bench ./bench
  docker image inspect "$SLUICE_IMAGE" > /dev/null 2>&1 || docker pull "$SLUICE_IMAGE"
  sut="$SLUICE_IMAGE"
  digest="$(docker image inspect -f '{{if .RepoDigests}}{{index .RepoDigests 0}}{{end}}' "$SLUICE_IMAGE")"
  if [ -n "$digest" ]; then sut="$SLUICE_IMAGE (${digest##*@})"; fi
}

start_mock() { # name cpu flag...
  docker run -d --name "$1" --cpuset-cpus "$2" -v "$out:/bench:ro" "$RUN_IMAGE" \
    /bench/bench mock -addr 127.0.0.1:9100 "${@:3}" > /dev/null
}

# Sluice shares the mock's network namespace, where the mock is on loopback: DISCORD_API_URL
# accepts plain HTTP only there.
start_sluice() { # name mock cpu env...
  docker run -d --name "$1" --network "container:$2" --cpuset-cpus "$3" \
    -e DISCORD_API_URL=http://127.0.0.1:9100 -e STATE_FILE= "${@:4}" "$SLUICE_IMAGE" > /dev/null
}

load() { # mode name mock cpu scenario via rep flag...
  local target=(-target http://127.0.0.1:9100)
  if [ "$6" = sluice ]; then target=(-target http://127.0.0.1:8080 -metrics http://127.0.0.1:9000/metrics); fi
  docker run "$1" --name "$2" --network "container:$3" --cpuset-cpus "$4" -v "$out:/bench:ro" \
    -v "$results:/results" --user "$(id -u):$(id -g)" "$RUN_IMAGE" /bench/bench load -scenario "$5" -via "$6" \
    -rep "$7" -sut "$sut" -out "/results/$5.$6.$7.json" "${target[@]}" "${@:8}"
}

# pair runs one scenario over both paths, alternating them so that drift on the host falls on both
# alike. Each run gets a fresh mock and a fresh Sluice.
pair() { # scenario flag...
  local scenario="$1" rep via
  shift
  for rep in $(seq "$REPS"); do
    for via in direct sluice; do
      if [ -s "$results/$scenario.$via.$rep.json" ]; then
        say "$scenario via $via #$rep: already done"
        continue
      fi
      cleanup
      start_mock sluice-bench-mock "$CPU_MOCK" "${mock_flags[@]}"
      if [ "$via" = sluice ]; then
        start_sluice sluice-bench-sut sluice-bench-mock "$CPU_SLUICE" ${sluice_env[@]+"${sluice_env[@]}"}
      fi
      if ! load --rm sluice-bench-load sluice-bench-mock "$CPU_LOAD" "$scenario" "$via" "$rep" "$@"; then
        docker logs sluice-bench-sut 2>&1 | tail -20 || true
        exit 1
      fi
    done
  done
  cleanup
}

# What Sluice adds to a request that no limit holds back.
latency() {
  mock_flags=(-route-limit 5 -route-window 5s -global-limit 50) sluice_env=()
  pair rate-0040-default -kind rate -rate 40 -duration 60s -warmup 10s -workers 64 -channels 1000
  mock_flags=("${unlimited[@]}") sluice_env=("${raised[@]}")
  pair rate-0500 -kind rate -rate 500 -duration 60s -warmup 10s -workers 64 -channels 1000
  pair rate-2000 -kind rate -rate 2000 -duration 60s -warmup 10s -workers 256 -channels 1000
}

# How much one core of Sluice carries.
ceiling() {
  mock_flags=("${unlimited[@]}") sluice_env=("${raised[@]}")
  pair closed-064 -kind closed -workers 64 -duration 30s -warmup 5s
  pair closed-256 -kind closed -workers 256 -duration 30s -warmup 5s
}

# Large bodies, which Sluice also keeps a copy of so it can send them again after a 429, and the
# same with that copy turned off.
upload() {
  mock_flags=("${unlimited[@]}") sluice_env=("${raised[@]}")
  pair upload-8mib -kind closed -workers 4 -duration 20s -warmup 3s -body-bytes 8388608
  sluice_env=("${raised[@]}" -e MAX_RETRY_CAPTURE_BYTES=0)
  pair upload-8mib-no-copy -kind closed -workers 4 -duration 20s -warmup 3s -body-bytes 8388608
}

# Eight processes share one bot token and meet Discord's limits.
limits() {
  mock_flags=("${discord[@]}") sluice_env=()
  pair limits-global -kind batch -processes 8 -per-process 400 -concurrency 32
  pair limits-shared-channel -kind batch -processes 8 -per-process 8 -concurrency 1 -shared
}

# mixed sends the same traffic, from eight processes, over both paths at the same time, each against
# a mock of its own. It is light enough for the two not to disturb each other.
mixed() { # scenario rate-per-process duration reps
  local scenario="$1" rep name failed
  for rep in $(seq "$4"); do
    if [ -s "$results/$scenario.direct.$rep.json" ] && [ -s "$results/$scenario.sluice.$rep.json" ]; then
      say "$scenario #$rep: already done"
      continue
    fi
    local flags=(-kind mixed -processes 8 -rate-per-process "$2" -channels 500 -duration "$3" -warmup 30s -seed "$rep")
    cleanup
    start_mock sluice-bench-mock-direct "$CPU_SPARE" "${discord[@]}"
    start_mock sluice-bench-mock "$CPU_MOCK" "${discord[@]}"
    start_sluice sluice-bench-sut sluice-bench-mock "$CPU_SLUICE"
    say "$scenario #$rep: both paths for $3"
    load -d sluice-bench-load-direct sluice-bench-mock-direct "$CPU_SPARE" "$scenario" direct "$rep" "${flags[@]}" > /dev/null
    load -d sluice-bench-load sluice-bench-mock "$CPU_LOAD" "$scenario" sluice "$rep" "${flags[@]}" > /dev/null
    failed=0
    for name in sluice-bench-load-direct sluice-bench-load; do
      [ "$(docker wait "$name")" = 0 ] || failed=1
      docker logs "$name" 2>&1 | tail -3
    done
    cleanup
    [ "$failed" = 0 ] || exit 1
  done
}

# A bot's traffic at 40% of its global limit.
traffic() { mixed mixed-20rps 2.5 5m "$REPS"; }

# The same at 80%, for long enough to show whether Sluice's footprint grows.
soak() { mixed soak-40rps 5 "$SOAK" 1; }

report() {
  docker run --rm -v "$out:/bench:ro" -v "$results:/results:ro" "$RUN_IMAGE" /bench/bench report /results
}

build
for suite in "${@:-latency ceiling upload limits traffic soak report}"; do
  for step in $suite; do
    case "$step" in
      latency | ceiling | upload | limits | traffic | soak | report) "$step" ;;
      *) echo "unknown suite: $step" >&2 && exit 2 ;;
    esac
  done
done
