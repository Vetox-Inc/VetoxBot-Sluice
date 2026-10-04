package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	benchBot     = "900000000000000001"
	firstChannel = int64(1200000000000000001)
	firstMessage = int64(1300000000000000001)
	apiPrefix    = "/api/v10"
	// busyChannels take half of a mixed run's new messages, so their limits come into play.
	busyChannels = 20
	// limitedBySluiceLabel is how the response counts name a 429 that Sluice answered itself.
	limitedBySluiceLabel = "429 sluice"
)

type loadOptions struct {
	scenario, kind, via, target, mock, metrics, out, sut string
	rep                                                  int

	duration, warmup, wait, drain time.Duration

	rate                         float64
	workers, channels, bodyBytes int

	processes, perProcess, concurrency int
	shared                             bool
	ratePerProcess                     float64
	seed                               uint64
}

// collector gathers the measured requests of a run, across goroutines.
type collector struct {
	mu        sync.Mutex
	latencies []time.Duration
	lags      []time.Duration
	failed    int
}

func (c *collector) add(latency, lag time.Duration, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !ok {
		c.failed++
		return
	}
	c.latencies = append(c.latencies, latency)
	c.lags = append(c.lags, lag)
}

func messagesPath(channel int) string {
	return apiPrefix + "/channels/" + strconv.FormatInt(firstChannel+int64(channel), 10) + "/messages"
}

func pathFor(method string, channel int) string {
	if method == http.MethodGet {
		return messagesPath(channel) + "/" + strconv.FormatInt(firstMessage+int64(channel), 10)
	}
	return messagesPath(channel)
}

// routeKey is what a library queues a request under before Discord names its bucket.
func routeKey(method string, channel int) string {
	return method + " " + strconv.Itoa(channel)
}

func messageBody(size int) []byte {
	const frame = `{"content":""}`
	return []byte(`{"content":"` + strings.Repeat("a", max(size-len(frame), 1)) + `"}`)
}

// marker notes the moment a run's measured part begins: Sluice's footprint and the mock's counts
// are both taken from there.
type marker struct {
	probe    *sampler
	snapshot func()
}

func (m marker) now() {
	if m.probe != nil {
		m.probe.mark()
	}
	m.snapshot()
}

func (m marker) after(wait time.Duration) {
	time.AfterFunc(wait, m.now)
}

// runRate sends requests at a fixed rate whatever the answers do, and times each from the moment
// it was due, so a slow system cannot hide its delay by slowing the generator down.
func runRate(ctx context.Context, o loadOptions, client *http.Client, counts *tally, mark marker) (*collector, time.Duration) {
	raw := &sender{http: client, base: o.target, authorization: botAuthorization(benchBot)}
	body := messageBody(o.bodyBytes)
	total := int(o.rate * (o.warmup + o.duration).Seconds())
	warm := int(o.rate * o.warmup.Seconds())
	type job struct {
		index int
		due   time.Time
	}
	jobs := make(chan job, total)
	start := time.Now().Add(200 * time.Millisecond)
	go func() {
		for i := range total {
			due := start.Add(seconds(float64(i) / o.rate))
			time.Sleep(time.Until(due))
			jobs <- job{i, due}
		}
		close(jobs)
	}()
	mark.after(time.Until(start.Add(o.warmup)))

	measured := &collector{}
	var workers sync.WaitGroup
	for range o.workers {
		workers.Go(func() {
			for next := range jobs {
				sent := time.Now()
				reply, err := raw.send(ctx, http.MethodPost, messagesPath(next.index%o.channels), body)
				ok := counts.record(reply, err)
				if next.index >= warm {
					measured.add(time.Since(next.due), sent.Sub(next.due), ok)
				}
			}
		})
	}
	workers.Wait()
	return measured, time.Since(start.Add(o.warmup))
}

// runClosed keeps every connection busy: each worker sends its next request as soon as the last
// one is answered, on a channel of its own.
func runClosed(ctx context.Context, o loadOptions, client *http.Client, counts *tally, mark marker) (*collector, time.Duration) {
	raw := &sender{http: client, base: o.target, authorization: botAuthorization(benchBot)}
	body := messageBody(o.bodyBytes)
	measureFrom := time.Now().Add(o.warmup)
	deadline := measureFrom.Add(o.duration)
	mark.after(o.warmup)

	measured := &collector{}
	var workers sync.WaitGroup
	for worker := range o.workers {
		workers.Go(func() {
			path := messagesPath(worker)
			for {
				begin := time.Now()
				if !begin.Before(deadline) {
					return
				}
				reply, err := raw.send(ctx, http.MethodPost, path, body)
				ok := counts.record(reply, err)
				if !begin.Before(measureFrom) {
					measured.add(time.Since(begin), 0, ok)
				}
			}
		})
	}
	workers.Wait()
	return measured, time.Since(measureFrom)
}

// libraryClients are the independent processes of a run: each has its own connections and its
// own idea of the rate limits, and all of them use one bot token.
func libraryClients(o loadOptions, counts *tally) []*libraryClient {
	clients := make([]*libraryClient, o.processes)
	for i := range clients {
		transport := &http.Transport{MaxIdleConnsPerHost: 256, IdleConnTimeout: 90 * time.Second}
		clients[i] = newLibraryClient(&sender{
			http:          &http.Client{Transport: transport, Timeout: 2 * time.Minute},
			base:          o.target,
			authorization: botAuthorization(benchBot),
		}, counts)
	}
	return clients
}

// runBatch gives every process a fixed amount of work and times how long all of it takes.
func runBatch(ctx context.Context, o loadOptions, counts *tally, mark marker) (*collector, time.Duration) {
	body := messageBody(o.bodyBytes)
	clients := libraryClients(o, counts)
	mark.now()

	measured := &collector{}
	start := time.Now()
	var processes sync.WaitGroup
	for process, client := range clients {
		processes.Go(func() {
			slots := make(chan struct{}, o.concurrency)
			var requests sync.WaitGroup
			for i := range o.perProcess {
				channel := process*o.perProcess + i
				if o.shared {
					channel = 0
				}
				slots <- struct{}{}
				requests.Go(func() {
					defer func() { <-slots }()
					begin := time.Now()
					err := client.do(ctx, http.MethodPost, messagesPath(channel), routeKey(http.MethodPost, channel), body)
					measured.add(time.Since(begin), 0, err == nil)
				})
			}
			requests.Wait()
		})
	}
	processes.Wait()
	return measured, time.Since(start)
}

// pick chooses what a mixed run's next request is: mostly new messages, half of them to a few
// busy channels, and the rest reads spread over every channel.
func pick(random *rand.Rand, channels int) (string, int) {
	if random.Float64() < 0.7 {
		if random.Float64() < 0.5 {
			return http.MethodPost, random.IntN(busyChannels)
		}
		return http.MethodPost, busyChannels + random.IntN(channels-busyChannels)
	}
	return http.MethodGet, random.IntN(channels)
}

// runMixed has every process generate requests at random moments for a set time, the way a bot's
// traffic arrives, from a seed so that both paths get the same requests at the same moments.
func runMixed(ctx context.Context, o loadOptions, counts *tally, mark marker) (*collector, time.Duration, int) {
	body := messageBody(o.bodyBytes)
	clients := libraryClients(o, counts)
	start := time.Now()
	measureFrom := start.Add(o.warmup)
	deadline := measureFrom.Add(o.duration)
	mark.after(o.warmup)
	requestCtx, cancel := context.WithDeadline(ctx, deadline.Add(o.drain))
	defer cancel()

	measured := &collector{}
	var unfinished atomic.Int64
	var processes sync.WaitGroup
	for process, client := range clients {
		processes.Go(func() {
			random := rand.New(rand.NewPCG(o.seed, uint64(process)+1))
			var requests sync.WaitGroup
			for arrival := start; ; {
				arrival = arrival.Add(seconds(random.ExpFloat64() / o.ratePerProcess))
				method, channel := pick(random, o.channels)
				if !arrival.Before(deadline) {
					break
				}
				time.Sleep(time.Until(arrival))
				requests.Go(func() {
					var payload []byte
					if method == http.MethodPost {
						payload = body
					}
					err := client.do(requestCtx, method, pathFor(method, channel), routeKey(method, channel), payload)
					if arrival.Before(measureFrom) {
						return
					}
					if errors.Is(err, context.DeadlineExceeded) {
						unfinished.Add(1)
					}
					measured.add(time.Since(arrival), 0, err == nil)
				})
			}
			requests.Wait()
		})
	}
	processes.Wait()
	return measured, o.duration, int(unfinished.Load())
}

func host() hostInfo {
	info := hostInfo{CPUs: runtime.NumCPU(), LoadBefore: loadAverage()}
	if release, err := os.ReadFile("/proc/sys/kernel/osrelease"); err == nil {
		info.Kernel = strings.TrimSpace(string(release))
	}
	if cpuinfo, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		for line := range strings.SplitSeq(string(cpuinfo), "\n") {
			if name, value, found := strings.Cut(line, ":"); found && strings.TrimSpace(name) == "model name" {
				info.CPUModel = strings.TrimSpace(value)
				break
			}
		}
	}
	return info
}

func loadAverage() string {
	data, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return ""
	}
	fields := strings.Fields(string(data))
	if len(fields) < 3 {
		return ""
	}
	return strings.Join(fields[:3], " ")
}

func waitReady(client *http.Client, o loadOptions) error {
	answers := func(url string) bool {
		response, err := client.Get(url)
		if err != nil {
			return false
		}
		_ = response.Body.Close()
		return response.StatusCode == http.StatusOK
	}
	for deadline := time.Now().Add(o.wait); ; time.Sleep(100 * time.Millisecond) {
		if answers(o.mock+"/__bench/stats") && (o.via != "sluice" || answers(o.target+"/sluice/healthz")) {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("the mock or Sluice did not come up")
		}
	}
}

func fetchMockStats(client *http.Client, base string) (mockStats, error) {
	response, err := client.Get(base + "/__bench/stats")
	if err != nil {
		return mockStats{}, err
	}
	defer func() { _ = response.Body.Close() }()
	var stats mockStats
	return stats, json.NewDecoder(response.Body).Decode(&stats)
}

// reconcile checks a run's own counts against what the mock saw, so a result that lost or
// invented requests is never reported.
func reconcile(run *result) string {
	limited := run.Mock.LimitedGlobal + run.Mock.LimitedRoute
	switch {
	case run.Unfinished > 0 || run.TransportErrors > 0:
		return "not checked: some requests had no answer"
	case int64(run.Succeeded) != run.Mock.Succeeded:
		return fmt.Sprintf("MISMATCH: %d requests succeeded but the mock accepted %d", run.Succeeded, run.Mock.Succeeded)
	case run.Via != "direct":
		return "ok"
	case int64(run.Attempts) != run.Mock.Received:
		return fmt.Sprintf("MISMATCH: %d requests were sent but the mock received %d", run.Attempts, run.Mock.Received)
	case int64(run.Responses["429 discord"]) != limited:
		return fmt.Sprintf("MISMATCH: %d 429s were received but the mock sent %d", run.Responses["429 discord"], limited)
	}
	return "ok"
}

func runLoad(args []string) error {
	var o loadOptions
	flags := flag.NewFlagSet("load", flag.ExitOnError)
	flags.StringVar(&o.scenario, "scenario", "", "name the result is filed under")
	flags.StringVar(&o.kind, "kind", "", "rate, closed, batch or mixed")
	flags.StringVar(&o.via, "via", "direct", "direct or sluice: the path the requests take")
	flags.StringVar(&o.target, "target", "http://127.0.0.1:9100", "where requests are sent")
	flags.StringVar(&o.mock, "mock", "http://127.0.0.1:9100", "the mock, for its own counts")
	flags.StringVar(&o.metrics, "metrics", "", "Sluice's /metrics URL, sampled during the run")
	flags.StringVar(&o.out, "out", "", "file the JSON result is written to; standard output when empty")
	flags.StringVar(&o.sut, "sut", "", "what was measured, for the record")
	flags.IntVar(&o.rep, "rep", 1, "repetition number")
	flags.DurationVar(&o.duration, "duration", 30*time.Second, "rate, closed and mixed: measured time")
	flags.DurationVar(&o.warmup, "warmup", 5*time.Second, "rate, closed and mixed: time before measuring starts")
	flags.DurationVar(&o.wait, "wait", 30*time.Second, "how long to wait for the mock and Sluice to come up")
	flags.DurationVar(&o.drain, "drain", time.Minute, "mixed: how long to wait for requests still in flight at the end")
	flags.Float64Var(&o.rate, "rate", 100, "rate: requests per second")
	flags.IntVar(&o.workers, "workers", 64, "rate and closed: concurrent connections")
	flags.IntVar(&o.channels, "channels", 1000, "rate and mixed: channels the requests are spread over")
	flags.IntVar(&o.bodyBytes, "body-bytes", 256, "request body size")
	flags.IntVar(&o.processes, "processes", 8, "batch and mixed: independent clients sharing the bot's token")
	flags.IntVar(&o.perProcess, "per-process", 100, "batch: requests each client has to send")
	flags.IntVar(&o.concurrency, "concurrency", 32, "batch: requests each client works on at once")
	flags.BoolVar(&o.shared, "shared", false, "batch: every client posts to the same channel")
	flags.Float64Var(&o.ratePerProcess, "rate-per-process", 5, "mixed: requests per second each client generates")
	flags.Uint64Var(&o.seed, "seed", 1, "mixed: seed of the arrival times and channel choices")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if o.kind == "mixed" && o.channels <= busyChannels {
		return fmt.Errorf("a mixed run needs more than %d channels", busyChannels)
	}

	ctx := context.Background()
	transport := &http.Transport{MaxIdleConnsPerHost: 4096, IdleConnTimeout: 90 * time.Second}
	client := &http.Client{Transport: transport, Timeout: 2 * time.Minute}
	if err := waitReady(client, o); err != nil {
		return err
	}

	run := result{
		Scenario:  o.scenario,
		Kind:      o.kind,
		Via:       o.via,
		Rep:       o.rep,
		SUT:       o.sut,
		StartedAt: time.Now().UTC().Truncate(time.Second),
		Host:      host(),
	}
	before, err := fetchMockStats(client, o.mock)
	if err != nil {
		return err
	}
	var probe *sampler
	if o.metrics != "" {
		interval := time.Second
		if o.kind == "mixed" {
			interval = 5 * time.Second
		}
		if probe, err = startSampler(o.metrics, interval); err != nil {
			return err
		}
	}

	counts := newTally()
	var measuredFrom atomic.Pointer[mockStats]
	var limitedBefore atomic.Int64
	mark := marker{probe: probe, snapshot: func() {
		if snapshot, err := fetchMockStats(client, o.mock); err == nil {
			measuredFrom.Store(&snapshot)
		}
		limitedBefore.Store(int64(counts.count(limitedBySluiceLabel)))
	}}
	var measured *collector
	var elapsed time.Duration
	switch o.kind {
	case "rate":
		measured, elapsed = runRate(ctx, o, client, counts, mark)
		run.Parameters = map[string]string{
			"rate": strconv.FormatFloat(o.rate, 'f', -1, 64), "workers": strconv.Itoa(o.workers),
			"channels": strconv.Itoa(o.channels), "bodyBytes": strconv.Itoa(o.bodyBytes),
			"duration": o.duration.String(), "warmup": o.warmup.String(),
		}
	case "closed":
		measured, elapsed = runClosed(ctx, o, client, counts, mark)
		run.Parameters = map[string]string{
			"workers": strconv.Itoa(o.workers), "bodyBytes": strconv.Itoa(o.bodyBytes),
			"duration": o.duration.String(), "warmup": o.warmup.String(),
		}
	case "batch":
		measured, elapsed = runBatch(ctx, o, counts, mark)
		run.Parameters = map[string]string{
			"processes": strconv.Itoa(o.processes), "perProcess": strconv.Itoa(o.perProcess),
			"concurrency": strconv.Itoa(o.concurrency), "shared": strconv.FormatBool(o.shared),
			"bodyBytes": strconv.Itoa(o.bodyBytes),
		}
	case "mixed":
		measured, elapsed, run.Unfinished = runMixed(ctx, o, counts, mark)
		run.Parameters = map[string]string{
			"processes": strconv.Itoa(o.processes), "ratePerProcess": strconv.FormatFloat(o.ratePerProcess, 'f', -1, 64),
			"channels": strconv.Itoa(o.channels), "busyChannels": strconv.Itoa(busyChannels),
			"seed": strconv.FormatUint(o.seed, 10), "bodyBytes": strconv.Itoa(o.bodyBytes),
			"duration": o.duration.String(), "warmup": o.warmup.String(),
		}
	default:
		return fmt.Errorf("unknown kind %q", o.kind)
	}

	if probe != nil {
		run.Sluice = probe.stop()
	}
	after, err := fetchMockStats(client, o.mock)
	if err != nil {
		return err
	}
	run.Mock = after.since(before)
	if from := measuredFrom.Load(); from != nil {
		sinceMark := after.since(*from)
		run.MockMeasured = &sinceMark
	}
	run.Host.LoadAfter = loadAverage()

	run.MeasuredSeconds = elapsed.Seconds()
	run.Requests = len(measured.latencies)
	run.Failed = measured.failed
	run.Throughput = float64(run.Requests) / elapsed.Seconds()
	if o.kind == "rate" {
		run.GeneratorLagP99 = summarize(measured.lags).P99
	}
	run.Latency = summarize(measured.latencies)
	run.Attempts, run.Succeeded = counts.attempts, counts.succeeded
	run.Responses, run.TransportErrors = counts.responses, counts.transport
	run.LimitedBySluice = counts.responses[limitedBySluiceLabel] - int(limitedBefore.Load())
	run.Reconcile = reconcile(&run)

	encoded, err := json.MarshalIndent(run, "", "  ")
	if err != nil {
		return err
	}
	if o.out == "" {
		fmt.Println(string(encoded))
	} else if err := os.WriteFile(o.out, append(encoded, '\n'), 0o644); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "%s via %s #%d: %d requests in %.1fs, %d failed, p50 %s, p99 %s, %s\n",
		run.Scenario, run.Via, run.Rep, run.Requests, run.MeasuredSeconds, run.Failed,
		formatMicros(run.Latency.P50), formatMicros(run.Latency.P99), run.Reconcile)
	if strings.HasPrefix(run.Reconcile, "MISMATCH") {
		return errors.New(run.Reconcile)
	}
	return nil
}
