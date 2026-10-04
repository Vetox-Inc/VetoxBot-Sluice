# Benchmarks

What does it cost to put Sluice between a bot and Discord, and what does it buy? This page answers with measurements:
the same workloads sent straight to a mock Discord and sent through Sluice 1.0.0, on one machine.
[bench/](bench) holds everything that produced them, and [Running it yourself](#running-it-yourself) repeats them.

## At a glance

- **The extra hop costs a few tenths of a millisecond.** At 40, 500 and 2,000 requests a second, the median request
  took 0.23 to 0.37 ms longer through Sluice, and the slowest 1% took 3 to 7 ms longer.
- **One core of Sluice carries about 6,000 requests a second.** That is over a hundred times the 50 a second Discord
  allows a bot by default. At 40 requests a second it used 2% of a core and 17 MiB of memory.
- **Eight processes sharing a bot's token drew 11,337 rate-limit rejections from Discord in a minute when they sent
  directly, and none through Sluice.** The work was 3,200 requests. Sent directly, it caused more 429s in one minute
  than Discord tolerates from an IP in ten. Through Sluice it finished 3.6 seconds later.
- **Sluice makes waits even, not short.** Under those limits the median request waited longer through Sluice (5.3 s
  against 3.0 s), while the slowest waited far less (5.5 s against 35 s).
- **Pacing costs latency as a bot nears its limit.** In mixed traffic at 40% of a bot's global limit, the median
  request took 27 ms through Sluice against 26 ms directly. At 80% it took 64 ms against 26 ms. In return Discord
  refused nothing, against 3,292 refusals in twenty minutes sent directly.
- **Twenty minutes of traffic left Sluice where it started.** Its memory settled at 19 MiB within ten minutes and
  stayed there.
- **Large uploads pay for the copy Sluice keeps.** An 8 MiB upload ran at 40 a second through one core of Sluice
  (320 MiB/s), and at 210 a second with the copy turned off.
- **Nothing failed.** Sluice answered 1,861,251 requests across 31 runs, every one successfully.

## Setup

### Two paths

```text
direct:   load generator ──────────────────────────► mock Discord
sluice:   load generator ──► Sluice 1.0.0 ──────────► mock Discord
```

Every scenario runs over both paths with the same generator, the same mock and the same settings. Each run starts a
fresh mock and a fresh Sluice. All three share one network namespace and talk over loopback, so no network stands
between them, and Sluice has a CPU core to itself.

The short scenarios run three times over each path, alternating direct and Sluice so that whatever else the machine
is doing falls on both alike, with the mock, Sluice and the generator each pinned to a core of its own. The
mixed-traffic scenarios put both paths to work at the same time, each against a mock of its own, so that both live
through exactly the same minutes. There the direct path's mock and generator share the fourth core.

Sluice is the released image, `ghcr.io/vetox-inc/sluice:1.0.0`
(`sha256:979073b66fc916cfc09bd9237af5392183a72dbfd4b83a01065dd566f2a3d735`), with its default settings except where a
scenario says otherwise, and with `STATE_FILE` empty so that no run inherits state from another.

### The mock Discord

[bench/mock.go](bench/mock.go) stands in for Discord's REST API as far as rate limits go:

- **A limit per route** and channel, guild or webhook. A window opens with its first request and admits a fixed number
  until it closes, which is how Discord describes its buckets. Every response reports it in `X-RateLimit-Bucket`,
  `-Limit`, `-Remaining`, `-Reset` and `-Reset-After`.
- **A limit per bot** across all routes, counted per second in the same way.
- **429s shaped like Discord's:** whole seconds in `Retry-After`, the exact wait in the body's `retry_after`, and the
  `user` or `global` scope.
- **Responses like Discord's:** a 741-byte message object, gzipped when the caller accepts gzip, and `Via` on
  everything.

The mock counts what reaches it. Those counts are the ground truth for "429s from Discord" below, and every run is
checked against them: the requests a run says succeeded must equal the requests the mock accepted, and on the direct
path the requests sent and the 429s received must equal what the mock received and sent. The runner rejects a run
that fails the check. None did.

### The clients

Two kinds of load are sent ([bench/load.go](bench/load.go)):

- **Plain requests**, for the scenarios that measure cost. They are `POST /channels/:id/messages` with a 256-byte
  JSON body, spread over channels so that no limit comes into play.
- **A library's requests**, for the scenarios that meet limits. [bench/client.go](bench/client.go) models what a
  Discord library does inside one process, following discord.js: one request at a time on each route, counting down
  the limit each response reports, at most 50 requests a second overall, and waiting out a 429 before trying again.
  Eight of these run side by side, each with its own connections and its own count, all using one bot token. That is
  eight shards, clusters or services of one bot.

The second kind is the best case for direct traffic. Each client keeps perfectly within the limits as far as it can
know them. What it cannot know is what the other seven are sending.

### The machine

| | |
| --- | --- |
| CPU | AMD Ryzen 5 5600GT, 6 cores and 12 threads |
| Memory | 14 GB |
| System | Windows 11, Docker Desktop with engine 29.3.1 |
| Docker's Linux VM | 4 CPUs, 5.8 GiB, kernel 6.6.87.2-microsoft-standard-WSL2 |
| Sluice | 1.0.0, one CPU core |
| Harness | built with Go 1.27 |

It is a developer's workstation, and it was in use during the runs, which were made in two sittings on one day. The
last table on this page gives the VM's load average before and after each run, and the repetitions show how much the
results moved.

### How the numbers are taken

- **Latency** at a fixed rate is counted from the moment a request was due to be sent, not from when it was sent, so a
  slow system cannot hide a delay by holding the generator back. The generator's own timer is part of it: at the 99th
  percentile it sent a request between 1.1 and 1.3 ms late in every run but one, on both paths alike. With every
  connection kept busy, latency is counted from sending to the end of the response. Under limits and in mixed traffic,
  it is counted from when a request came up until Discord accepted it, waits and retries included.
- **Percentiles** are nearest-rank over every request of the measured part of a run. Warm-up is left out: 10 seconds at
  a fixed rate, 5 with every connection busy, 3 for uploads and 30 in mixed traffic.
- **Each figure in the results** is the median of three runs, except the twenty-minute run, which was made once. The
  table at the end has every run.
- **Sluice's CPU and memory** come from its own `/metrics`: `process_cpu_seconds_total` and
  `process_resident_memory_bytes`, read once a second, or every five seconds in mixed traffic. Reading them that often
  is itself work for the one core Sluice has.
- **"Added by Sluice"** is the Sluice figure minus the direct figure.

## Results

### The cost of the extra hop

Requests at a fixed rate for 60 seconds, with no limit in the way. The first row is Sluice as it ships, with the mock
enforcing Discord's limits and the rate below them. For the other two, the mock's limits are lifted and the bot's
global limit is raised with `BOT_RATELIMIT_OVERRIDES`, since a bot at Discord's default cannot send that fast.

| Requests a second | Path | Median | 90% | 99% | 99.9% | Failed | Sluice CPU | Sluice memory |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 40 | direct | 0.95 ms | 1.38 ms | 2.70 ms | 6.03 ms | 0 | | |
| 40 | Sluice | 1.32 ms | 1.82 ms | 5.76 ms | 10.73 ms | 0 | 0.02 cores | 17.2 MiB |
| 40 | added by Sluice | 0.37 ms | 0.44 ms | 3.06 ms | 4.70 ms | | | |
| 500 | direct | 0.78 ms | 1.24 ms | 2.12 ms | 4.96 ms | 0 | | |
| 500 | Sluice | 1.08 ms | 1.61 ms | 6.36 ms | 14.82 ms | 0 | 0.18 cores | 19.0 MiB |
| 500 | added by Sluice | 0.30 ms | 0.37 ms | 4.24 ms | 9.86 ms | | | |
| 2,000 | direct | 0.66 ms | 1.23 ms | 2.39 ms | 15.16 ms | 0 | | |
| 2,000 | Sluice | 0.88 ms | 1.81 ms | 9.21 ms | 33.16 ms | 0 | 0.48 cores | 39.3 MiB |
| 2,000 | added by Sluice | 0.23 ms | 0.58 ms | 6.82 ms | 18.00 ms | | | |

Nine in ten requests pay well under a millisecond for the hop. The tail pays more: a few milliseconds at the 99th
percentile. Sluice had a single core here, shared between forwarding, garbage collection and the metrics read every
second, so the tail is what one core's pauses look like. One of the three runs at 2,000 a second had a 99th percentile
of 110 ms where the other two had 7 and 9 ms. The figure above is the middle of the three.

A request to the real Discord spends tens of milliseconds on the network and in Discord itself. Against that, the hop
is small. On loopback, where the direct path takes under a millisecond, it stands out.

### One core's capacity

Every connection kept busy for 30 seconds: each sends its next request as soon as the last is answered, on a channel
of its own. Limits lifted as above.

| Connections | Path | Requests a second | Median | 99% | Failed | Sluice CPU | Sluice memory |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 64 | direct | 23,073 | 2.76 ms | 6.13 ms | 0 | | |
| 64 | Sluice | 6,378 | 9.66 ms | 17.43 ms | 0 | 1.00 cores | 33.4 MiB |
| 256 | direct | 20,175 | 12.41 ms | 24.68 ms | 0 | | |
| 256 | Sluice | 5,405 | 45.98 ms | 73.38 ms | 0 | 1.00 cores | 68.7 MiB |

Sluice used all of its core in both, so these are what one core carries: about 6,400 requests a second, or 0.16 ms of
CPU for each. The direct figure is the ceiling of the generator and the mock, a core each, and says nothing about
Discord. More connections than a core can serve bring no more throughput, only longer waits and more memory: four
times the connections took twice the memory and delivered 15% less.

A proxy has to do more per request than either end. It parses the request, finds its queue, paces it, sends it on,
reads and unzips the answer, and records it. The comparison that matters is with the limit: 6,400 a second is 128
bots at Discord's default limit, each sending flat out, on one core.

### Large uploads

Four connections sending 8 MiB bodies for 20 seconds. Sluice keeps a copy of a request's body while it sends it, so
that it can send the request again if Discord answers 429. A copy over 1 MiB goes to a temporary file. The last row
turns the copy off with `MAX_RETRY_CAPTURE_BYTES=0`.

| Path | Uploads a second | Throughput | Median | 99% | Failed | Sluice CPU | Sluice memory |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| direct | 322 | 2.5 GiB/s | 12.17 ms | 21.92 ms | 0 | | |
| Sluice | 40 | 320 MiB/s | 98.56 ms | 158.34 ms | 0 | 1.00 cores | 22.6 MiB |
| Sluice, no copy | 210 | 1.6 GiB/s | 18.65 ms | 32.71 ms | 0 | 0.99 cores | 17.9 MiB |

This is where the hop costs most. Writing each body to a file takes most of the core: without the copy, the same core
moves five times as much. Memory stays flat either way, since a body is streamed, never held whole.

For nearly every bot the default is the right trade. 320 MiB a second is about 2.7 gigabits, more than most links to
Discord carry, and the copy is what lets Sluice absorb a 429 on an upload instead of handing it back. A service that
mostly relays large files, and handles a 429 itself, can turn the copy off.

### Eight processes, one bot: the global limit

Eight clients share one bot token. Each has 400 messages to send, every message to a different channel, and works on
32 at a time. The mock enforces Discord's default of 50 requests a second per bot and answers in 25 ms. The 3,200
messages cannot take less than 63 seconds.

| Path | Completed | Failed | Time to finish | 429s from Discord | Requests that reached Discord | Median | 99% | Slowest |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| direct | 3,200 | 0 | 63.3 s | 11,337 | 14,537 | 3.01 s | 20.06 s | 35.11 s |
| Sluice | 3,200 | 0 | 66.9 s | 0 | 3,200 | 5.34 s | 5.36 s | 5.46 s |

Sent directly, each client allows itself 50 requests a second, as a library does, so together they offer 400 against
a limit of 50. Discord refuses the rest. Every client then waits as the 429 tells it to and tries again, at the same
moment as the others. To deliver 3,200 messages, 14,537 requests reached Discord, and 11,337 of them were refused: in
the three runs, 9,752, 12,206 and 11,337.

Discord blocks an IP that collects 10,000 such responses in 10 minutes. Two of the three runs passed that inside a
minute, and the third came within 250 of it. Against the real Discord, a bot doing this is blocked before long.

Through Sluice, Discord received exactly the 3,200 requests and refused none, in all three runs. Sluice paces a bot
slightly under its limit, which came to 48 requests a second here, and that is why it finished 3.6 seconds later. It
used 3% of a core and 40 MiB.

The waits differ in shape. Sluice serves requests in the order they arrive, so every one waited about the same: 5.3
seconds, the time for the 256 requests ahead of it at 48 a second. Sent directly, the lucky half got through in 3
seconds and the unlucky kept losing the race: 1 in 100 took over 20 seconds, and the slowest 35.

### Eight processes, one channel

Eight clients each post 8 messages to the same channel, one after another. The mock enforces Discord's limit for new
messages in a channel, 5 per 5 seconds. The 64 messages cannot take less than 60 seconds.

| Path | Completed | Failed | Time to finish | 429s from Discord | Requests that reached Discord | Median | 99% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| direct | 64 | 0 | 60.7 s | 76 | 140 | 5.05 s | 25.26 s |
| Sluice | 64 | 0 | 60.5 s | 0 | 64 | 10.01 s | 10.04 s |

The same picture on a single route. Each client believes the channel's five messages are its own, so more than half
of what the eight send is refused: 76 refusals for 64 messages. Through Sluice there is one queue for the channel and
no refusal.

Again Sluice trades a shorter median for a bounded wait. With eight clients waiting on a channel that admits five per
window, a message waits one window or two, never more. Sent directly, a message that keeps colliding waited five
windows.

The two-window wait is just inside `QUEUE_TIMEOUT`, which is 10 seconds by default. Sluice does not hold a request
longer than that. With a few more clients on this channel, it would have answered some of them 429 itself, with the
time to wait. Such a 429 never reaches Discord and costs the IP nothing.

### A bot's ordinary traffic

The scenarios above push one thing at a time. This one is closer to a day in a bot's life. The same eight clients
each generate requests at random moments, 2.5 a second on average, for five minutes: 20 a second in all, 40% of the
bot's global limit. Seven in ten are new messages and the rest read one message. They are spread over 500 channels,
with half of the new messages going to 20 busy ones, each of which gets about a third of what its limit allows. Both
paths receive the same requests at the same moments.

| Path | Completed | Failed | 429s from Discord | Median | 90% | 99% | 99.9% | Sluice CPU | Sluice memory |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| direct | 6,106 | 0 | 37 | 26.31 ms | 26.84 ms | 28.00 ms | 1.97 s | | |
| Sluice | 6,106 | 0 | 0 | 27.23 ms | 51.83 ms | 98.82 ms | 1.97 s | 0.01 cores | 18.8 MiB |

The mock takes 25 ms to answer, so 26 ms is a request that waited for nothing. Sent directly, almost every request is
one of those. A few are not: the busy channels have several writers, who collide now and then, and Discord refused 37
requests in five minutes.

Through Sluice the median request waits less than a millisecond more. One in ten waits about 25 ms more, and one in
a hundred about 70 ms. That is the price of pacing. Sluice sends a bot's requests at least 20 ms apart, so that no
second can hold more than Discord allows. Requests that arrive in a cluster are spread out, where the direct
path lets them through together and usually gets away with it. In return, Discord refused nothing.

The slowest one in a thousand took two seconds on both paths. Those are messages to a busy channel that had used up
its five, and they wait for the channel whichever way they travel.

### Twenty minutes near the limit

The same traffic at twice the rate, 40 requests a second, which is 80% of the bot's global limit and puts the busy
channels at 70% of theirs. One run of twenty minutes.

| Path | Completed | Failed | 429s from Discord | Requests that reached Discord | Median | 90% | 99% | Slowest |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| direct | 47,875 | 0 | 3,292 | 51,168 | 26.48 ms | 27.38 ms | 3.29 s | 20.45 s |
| Sluice | 47,875 | 0 | 0 | 47,876 | 63.99 ms | 202.57 ms | 3.47 s | 16.42 s |

Close to the limit, pacing costs more. Traffic that averages 40 a second arrives in bursts well above that, and
Sluice let through 48 a second at most, so a burst took a while to drain. The median request took 64 ms instead of
26, and one in ten took over 200 ms. The slowest 1% took three seconds on both paths, waiting for a busy channel.

Sent directly, nothing waited to be paced, and Discord refused 3,292 requests: 707 at the global limit and 2,585 at a
channel's. That is 1,646 every ten minutes, a sixth of what gets an IP blocked, from one bot running at 80% of its
limit. Through Sluice, every request reached Discord once and none was refused.

Sluice's footprint over the twenty minutes, in four-minute stretches:

| Minutes | Memory | Goroutines | CPU |
| --- | ---: | ---: | ---: |
| 0 to 4 | 18.2 MiB | 60 | 0.025 cores |
| 4 to 8 | 18.6 MiB | 59 | 0.025 cores |
| 8 to 12 | 19.2 MiB | 61 | 0.027 cores |
| 12 to 16 | 19.0 MiB | 59 | 0.026 cores |
| 16 to 20 | 19.3 MiB | 58 | 0.028 cores |

Memory rose by a megabyte in the first ten minutes, while Sluice met the thousand routes in use and learned their
buckets, and then held between 19.0 and 19.3 MiB. It never exceeded 21.2 MiB. The number of goroutines and the CPU
used did not move.

### Footprint

| Sluice was | CPU | Memory |
| --- | ---: | ---: |
| Idle, just started | | 11.6 MiB |
| Forwarding 40 requests a second | 0.02 cores | 17.2 MiB |
| Forwarding 500 requests a second | 0.18 cores | 19.0 MiB |
| Forwarding 2,000 requests a second | 0.48 cores | 39.3 MiB |
| Saturated, 64 connections | 1.00 cores | 33.4 MiB |
| Saturated, 256 connections | 1.00 cores | 68.7 MiB |
| Queueing for 8 clients at the global limit | 0.03 cores | 40.2 MiB |
| Mixed traffic, 20 requests a second | 0.01 cores | 18.8 MiB |
| Mixed traffic, 40 requests a second for 20 minutes | 0.03 cores | 21.2 MiB |

Memory is the resident set at its peak during a run. It follows the number of requests in flight, not the number
forwarded: 256 held connections cost more than 2,000 requests a second that come and go. The release archive for
Linux is 5 MiB, and Docker reports the image at 24.6 MB.

### Errors

| | Direct | Through Sluice |
| --- | ---: | ---: |
| Runs | 31 | 31 |
| Requests sent | 5,113,998 | 1,861,251 |
| Answered 429 by Discord | 36,964 | 0 |
| Answered 429 by Sluice | | 0 |
| Other errors | 0 | 0 |
| Connections that failed | 0 | 0 |

Sluice's own failure counter, `sluice_failures_total`, stayed at zero in every run, and no run lost its Sluice
process.

## Conclusions

1. **For one process that stays inside its limits, Sluice is a cost and no gain.** The cost is a third of a
   millisecond at the median and a few percent of a core when traffic is light, and tens of milliseconds when a bot
   runs near its global limit, because Sluice spaces its requests. A bot that runs as a single process and never sees
   a 429 does not need a proxy.
2. **For several processes on one token, Sluice is the difference between working and being blocked.** The clients
   here were each as careful as a client can be, and together they still drew thousands of 429s a minute under load,
   and a steady trickle in ordinary traffic. Nothing a process does alone fixes that, because the limit is shared and
   the knowledge is not.
3. **Sluice does not make limited work faster.** Discord's limits set the pace on both paths, and Sluice finishes a
   little later because it stays a little under them. What changes is who waits and for how long: everyone about the
   same, instead of most a little and some very long.
4. **Capacity is not a concern.** One core carries over a hundred bots at Discord's default limit. A deployment will
   meet Discord's limits long before it meets Sluice's.
5. **Uploads are the one place to tune.** The default suits a bot. A service that relays large files at volume should
   read [`MAX_RETRY_CAPTURE_BYTES`](CONFIG.md#max_retry_capture_bytes).

## What this does not show

- **Discord itself.** The mock follows Discord's documentation, and Discord does not always. It changes limits, omits
  headers on some responses and limits some routes more widely than their headers say.
- **A real network.** Everything ran on loopback. Sluice also reached the mock over plain HTTP, where it reaches
  Discord over TLS, which costs CPU that is not counted here.
- **A real library.** The clients model one, as closely as a few hundred lines can.
- **Days of uptime.** Twenty minutes is the longest run here. It shows no growth after the first ten, and it cannot
  rule out growth too slow to see in that time.
- **Clusters.** Every run used a single node.
- **Other machines.** The absolute figures belong to this one, and it was busy. The comparison between the two paths
  is what carries over.

## Running it yourself

It needs Docker and bash, and takes about 100 minutes on a machine with four CPUs to spare.

```sh
bench/run.sh                 # every suite, then the tables
bench/run.sh limits report   # one suite, then the tables
```

The suites are `latency`, `ceiling`, `upload`, `limits`, `traffic` and `soak`. Results go to
`bench/results/<version>/`, one JSON file a run, and a run that is interrupted carries on from the files it finds
there. These variables change what is measured:

| Variable | Default | Meaning |
| --- | --- | --- |
| `SLUICE_IMAGE` | `ghcr.io/vetox-inc/sluice:1.0.0` | The image to measure. Build one with `docker build -t sluice:dev .` to measure a change. |
| `REPS` | `3` | Runs of each scenario over each path |
| `RESULTS` | `bench/results/<image tag>` | Where results are written |
| `CPU_MOCK`, `CPU_SLUICE`, `CPU_LOAD`, `CPU_SPARE` | `0`, `1`, `2`, `3` | The CPU each part is pinned to |
| `SOAK` | `20m` | Length of the long run |

Measure on a machine doing nothing else, and never beside a build or a test run: the figures are only as quiet as
the machine.

## Every run

The files are in [bench/results/1.0.0](bench/results/1.0.0). Scenario names are the runner's: `rate-` is a fixed
rate, `closed-` is every connection kept busy, `limits-` is the eight clients with work to get through, and `mixed-`
and `soak-` are their ordinary traffic. "Check" is the comparison with the mock's counts.

| Scenario | Path | Run | Started (UTC) | Requests | Failed | Requests/s | p50 | p99 | Max | 429s from Discord | 429s from Sluice | Reached Discord | Sluice CPU | Sluice memory | Load before, after | Check |
| --- | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| closed-064 | direct | 1 | 08:22:47 | 637948 | 0 | 21264.2 | 2.94 ms | 7.73 ms | 17.48 ms | 0 | 0 | 637948 | - | - | 1.85, 2.11 | ok |
| closed-064 | direct | 2 | 08:24:01 | 700326 | 0 | 23343.3 | 2.73 ms | 6.01 ms | 15.04 ms | 0 | 0 | 700326 | - | - | 2.31, 2.39 | ok |
| closed-064 | direct | 3 | 08:25:15 | 692223 | 0 | 23073.3 | 2.76 ms | 6.13 ms | 15.77 ms | 0 | 0 | 692224 | - | - | 2.84, 2.77 | ok |
| closed-064 | sluice | 1 | 08:23:25 | 190292 | 0 | 6340.1 | 9.69 ms | 17.45 ms | 28.28 ms | 0 | 0 | 190266 | 1.00 cores | 33.4 MiB | 2.02, 2.31 | ok |
| closed-064 | sluice | 2 | 08:24:38 | 191401 | 0 | 6377.5 | 9.66 ms | 17.43 ms | 40.21 ms | 0 | 0 | 191390 | 1.00 cores | 33.1 MiB | 2.19, 2.84 | ok |
| closed-064 | sluice | 3 | 08:25:52 | 192769 | 0 | 6424.5 | 9.58 ms | 17.19 ms | 27.38 ms | 0 | 0 | 192762 | 1.00 cores | 33.6 MiB | 2.77, 2.79 | ok |
| closed-256 | direct | 1 | 08:26:29 | 605335 | 0 | 20174.8 | 12.41 ms | 24.68 ms | 41.94 ms | 0 | 0 | 605337 | - | - | 2.96, 2.58 | ok |
| closed-256 | direct | 2 | 08:27:41 | 607771 | 0 | 20255.8 | 12.39 ms | 24.19 ms | 40.11 ms | 0 | 0 | 607771 | - | - | 2.63, 2.79 | ok |
| closed-256 | direct | 3 | 08:28:56 | 602941 | 0 | 20094.7 | 12.45 ms | 24.68 ms | 36.43 ms | 0 | 0 | 602941 | - | - | 2.86, 2.65 | ok |
| closed-256 | sluice | 1 | 08:27:06 | 162417 | 0 | 5406.1 | 45.95 ms | 73.38 ms | 98.44 ms | 0 | 0 | 162239 | 1.00 cores | 68.7 MiB | 2.58, 2.63 | ok |
| closed-256 | sluice | 2 | 08:28:18 | 162282 | 0 | 5404.6 | 45.98 ms | 73.25 ms | 117.46 ms | 0 | 0 | 162137 | 1.00 cores | 68.2 MiB | 2.89, 2.86 | ok |
| closed-256 | sluice | 3 | 08:29:34 | 159864 | 0 | 5322.3 | 46.12 ms | 74.92 ms | 406.69 ms | 0 | 0 | 159759 | 0.99 cores | 69.3 MiB | 2.76, 3.18 | ok |
| limits-global | direct | 1 | 08:35:10 | 3200 | 0 | 50.6 | 2.01 s | 21.06 s | 35.08 s | 9752 | 0 | 12952 | - | - | 2.96, 1.76 | ok |
| limits-global | direct | 2 | 08:37:24 | 3200 | 0 | 50.6 | 3.01 s | 20.06 s | 43.12 s | 12206 | 0 | 15406 | - | - | 0.54, 0.41 | ok |
| limits-global | direct | 3 | 08:39:38 | 3200 | 0 | 50.5 | 3.01 s | 19.06 s | 35.11 s | 11337 | 0 | 14537 | - | - | 0.64, 0.56 | ok |
| limits-global | sluice | 1 | 08:36:16 | 3200 | 0 | 47.8 | 5.34 s | 5.36 s | 5.75 s | 0 | 0 | 3200 | 0.03 cores | 40.2 MiB | 1.76, 0.54 | ok |
| limits-global | sluice | 2 | 08:38:29 | 3200 | 0 | 47.9 | 5.34 s | 5.35 s | 5.42 s | 0 | 0 | 3200 | 0.03 cores | 40.3 MiB | 0.41, 0.52 | ok |
| limits-global | sluice | 3 | 08:40:43 | 3200 | 0 | 47.8 | 5.34 s | 5.37 s | 5.46 s | 0 | 0 | 3200 | 0.03 cores | 40.2 MiB | 0.68, 0.31 | ok |
| limits-shared-channel | direct | 1 | 08:41:52 | 64 | 0 | 1.1 | 5.05 s | 25.26 s | 25.26 s | 71 | 0 | 135 | - | - | 0.29, 0.25 | ok |
| limits-shared-channel | direct | 2 | 08:43:56 | 64 | 0 | 1.1 | 5.05 s | 25.25 s | 25.25 s | 76 | 0 | 140 | - | - | 0.25, 0.11 | ok |
| limits-shared-channel | direct | 3 | 08:46:01 | 64 | 0 | 1.1 | 5.05 s | 25.26 s | 25.26 s | 76 | 0 | 140 | - | - | 0.18, 0.22 | ok |
| limits-shared-channel | sluice | 1 | 08:42:54 | 64 | 0 | 1.1 | 10.01 s | 10.04 s | 10.04 s | 0 | 0 | 64 | 0.00 cores | 17.1 MiB | 0.25, 0.25 | ok |
| limits-shared-channel | sluice | 2 | 08:44:59 | 64 | 0 | 1.1 | 10.01 s | 10.04 s | 10.04 s | 0 | 0 | 64 | 0.00 cores | 17.0 MiB | 0.11, 0.18 | ok |
| limits-shared-channel | sluice | 3 | 08:47:06 | 64 | 0 | 1.1 | 10.01 s | 10.04 s | 10.04 s | 0 | 0 | 64 | 0.00 cores | 17.1 MiB | 0.22, 0.29 | ok |
| mixed-20rps | direct | 1 | 09:30:42 | 6095 | 0 | 20.3 | 26.31 ms | 27.52 ms | 3.16 s | 25 | 0 | 6120 | - | - | 0.40, 0.46 | ok |
| mixed-20rps | direct | 2 | 09:36:31 | 6160 | 0 | 20.5 | 26.29 ms | 28.00 ms | 3.16 s | 37 | 0 | 6197 | - | - | 0.71, 0.34 | ok |
| mixed-20rps | direct | 3 | 09:42:19 | 6106 | 0 | 20.4 | 26.32 ms | 29.35 ms | 2.92 s | 43 | 0 | 6149 | - | - | 0.63, 0.19 | ok |
| mixed-20rps | sluice | 1 | 09:30:43 | 6095 | 0 | 20.3 | 27.23 ms | 98.81 ms | 3.17 s | 0 | 0 | 6095 | 0.01 cores | 18.8 MiB | 0.40, 0.46 | ok |
| mixed-20rps | sluice | 2 | 09:36:32 | 6160 | 0 | 20.5 | 27.23 ms | 103.98 ms | 3.16 s | 0 | 0 | 6160 | 0.01 cores | 18.5 MiB | 0.71, 0.34 | ok |
| mixed-20rps | sluice | 3 | 09:42:20 | 6106 | 0 | 20.4 | 27.18 ms | 98.82 ms | 2.92 s | 0 | 0 | 6106 | 0.01 cores | 18.9 MiB | 0.58, 0.17 | ok |
| rate-0040-default | direct | 1 | 08:00:42 | 2400 | 0 | 40.0 | 947 µs | 2.50 ms | 9.36 ms | 0 | 0 | 2400 | - | - | 0.80, 0.57 | ok |
| rate-0040-default | direct | 2 | 08:03:08 | 2400 | 0 | 40.0 | 990 µs | 6.69 ms | 69.20 ms | 0 | 0 | 2400 | - | - | 0.77, 0.76 | ok |
| rate-0040-default | direct | 3 | 08:05:40 | 2400 | 0 | 40.0 | 921 µs | 2.70 ms | 9.26 ms | 0 | 0 | 2400 | - | - | 0.94, 0.36 | ok |
| rate-0040-default | sluice | 1 | 08:01:56 | 2400 | 0 | 40.0 | 1.33 ms | 5.76 ms | 10.68 ms | 0 | 0 | 2399 | 0.02 cores | 17.2 MiB | 0.57, 0.49 | ok |
| rate-0040-default | sluice | 2 | 08:04:28 | 2400 | 0 | 40.0 | 1.32 ms | 5.77 ms | 13.43 ms | 0 | 0 | 2399 | 0.02 cores | 17.4 MiB | 0.65, 0.94 | ok |
| rate-0040-default | sluice | 3 | 08:06:53 | 2400 | 0 | 40.0 | 1.25 ms | 4.37 ms | 15.56 ms | 0 | 0 | 2400 | 0.02 cores | 17.2 MiB | 0.49, 0.40 | ok |
| rate-0500 | direct | 1 | 08:08:05 | 30000 | 0 | 500.0 | 736 µs | 1.75 ms | 14.50 ms | 0 | 0 | 30000 | - | - | 0.40, 0.28 | ok |
| rate-0500 | direct | 2 | 08:10:30 | 30000 | 0 | 500.0 | 780 µs | 2.14 ms | 17.15 ms | 0 | 0 | 30000 | - | - | 1.28, 0.94 | ok |
| rate-0500 | direct | 3 | 08:12:59 | 30000 | 0 | 500.0 | 805 µs | 2.12 ms | 13.10 ms | 0 | 0 | 30000 | - | - | 1.35, 1.34 | ok |
| rate-0500 | sluice | 1 | 08:09:17 | 30000 | 0 | 500.0 | 1.06 ms | 4.89 ms | 41.47 ms | 0 | 0 | 29999 | 0.17 cores | 18.7 MiB | 0.28, 1.28 | ok |
| rate-0500 | sluice | 2 | 08:11:44 | 30000 | 0 | 500.0 | 1.08 ms | 6.36 ms | 74.17 ms | 0 | 0 | 30000 | 0.18 cores | 19.0 MiB | 1.03, 1.30 | ok |
| rate-0500 | sluice | 3 | 08:14:12 | 30000 | 0 | 500.0 | 1.14 ms | 6.54 ms | 25.89 ms | 0 | 0 | 29999 | 0.20 cores | 19.8 MiB | 1.34, 1.34 | ok |
| rate-2000 | direct | 1 | 08:15:28 | 120000 | 0 | 2000.0 | 664 µs | 3.83 ms | 168.94 ms | 0 | 0 | 120001 | - | - | 1.13, 1.16 | ok |
| rate-2000 | direct | 2 | 08:17:53 | 120000 | 0 | 2000.0 | 655 µs | 1.78 ms | 8.67 ms | 0 | 0 | 120001 | - | - | 1.38, 1.20 | ok |
| rate-2000 | direct | 3 | 08:20:20 | 120000 | 0 | 2000.0 | 655 µs | 2.39 ms | 47.67 ms | 0 | 0 | 120000 | - | - | 2.00, 0.95 | ok |
| rate-2000 | sluice | 1 | 08:16:41 | 120000 | 0 | 2000.0 | 871 µs | 6.76 ms | 36.16 ms | 0 | 0 | 119998 | 0.47 cores | 39.3 MiB | 1.16, 1.41 | ok |
| rate-2000 | sluice | 2 | 08:19:07 | 120000 | 0 | 2000.0 | 881 µs | 9.21 ms | 52.95 ms | 0 | 0 | 119996 | 0.48 cores | 33.2 MiB | 1.20, 2.00 | ok |
| rate-2000 | sluice | 3 | 08:21:35 | 120000 | 0 | 2000.0 | 948 µs | 110.36 ms | 211.34 ms | 0 | 0 | 120000 | 0.52 cores | 50.4 MiB | 0.87, 1.85 | ok |
| soak-40rps | direct | 1 | 09:48:13 | 47875 | 0 | 39.9 | 26.48 ms | 3.29 s | 20.45 s | 3292 | 0 | 51168 | - | - | 0.43, 0.92 | ok |
| soak-40rps | sluice | 1 | 09:48:14 | 47875 | 0 | 39.9 | 63.99 ms | 3.47 s | 16.42 s | 0 | 0 | 47876 | 0.03 cores | 21.2 MiB | 0.43, 0.92 | ok |
| upload-8mib | direct | 1 | 08:30:11 | 6477 | 0 | 323.8 | 12.08 ms | 21.85 ms | 29.68 ms | 0 | 0 | 6477 | - | - | 2.93, 2.57 | ok |
| upload-8mib | direct | 2 | 08:31:02 | 6408 | 0 | 320.4 | 12.24 ms | 21.98 ms | 39.42 ms | 0 | 0 | 6408 | - | - | 2.99, 3.01 | ok |
| upload-8mib | direct | 3 | 08:31:52 | 6438 | 0 | 321.9 | 12.17 ms | 21.92 ms | 34.88 ms | 0 | 0 | 6438 | - | - | 3.26, 2.96 | ok |
| upload-8mib | sluice | 1 | 08:30:38 | 786 | 0 | 39.3 | 98.56 ms | 170.16 ms | 614.34 ms | 0 | 0 | 788 | 0.98 cores | 22.1 MiB | 2.53, 2.81 | ok |
| upload-8mib | sluice | 2 | 08:31:27 | 801 | 0 | 40.0 | 98.96 ms | 154.85 ms | 176.82 ms | 0 | 0 | 803 | 1.00 cores | 22.6 MiB | 2.77, 3.54 | ok |
| upload-8mib | sluice | 3 | 08:32:16 | 809 | 0 | 40.3 | 97.11 ms | 158.34 ms | 199.86 ms | 0 | 0 | 811 | 1.00 cores | 22.9 MiB | 2.72, 3.47 | ok |
| upload-8mib-no-copy | direct | 1 | 08:32:42 | 6499 | 0 | 324.9 | 12.05 ms | 21.73 ms | 33.16 ms | 0 | 0 | 6499 | - | - | 3.27, 2.77 | ok |
| upload-8mib-no-copy | direct | 2 | 08:33:31 | 6410 | 0 | 320.5 | 12.22 ms | 21.94 ms | 35.08 ms | 0 | 0 | 6409 | - | - | 2.78, 2.94 | ok |
| upload-8mib-no-copy | direct | 3 | 08:34:20 | 6432 | 0 | 321.5 | 12.19 ms | 21.85 ms | 30.22 ms | 0 | 0 | 6432 | - | - | 3.19, 2.58 | ok |
| upload-8mib-no-copy | sluice | 1 | 08:33:06 | 4188 | 0 | 209.3 | 18.66 ms | 32.10 ms | 61.87 ms | 0 | 0 | 4186 | 0.99 cores | 17.8 MiB | 2.79, 2.78 | ok |
| upload-8mib-no-copy | sluice | 2 | 08:33:56 | 4199 | 0 | 209.9 | 18.56 ms | 33.19 ms | 48.13 ms | 0 | 0 | 4198 | 0.99 cores | 17.9 MiB | 2.94, 3.19 | ok |
| upload-8mib-no-copy | sluice | 3 | 08:34:45 | 4203 | 0 | 210.0 | 18.65 ms | 32.71 ms | 42.06 ms | 0 | 0 | 4202 | 0.99 cores | 17.9 MiB | 2.58, 2.96 | ok |
