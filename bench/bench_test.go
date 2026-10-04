package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPercentileIsNearestRank(t *testing.T) {
	samples := make([]time.Duration, 100)
	for i := range samples {
		samples[i] = time.Duration(100-i) * time.Millisecond
	}
	summary := summarize(samples)
	for name, pair := range map[string][2]float64{
		"min": {summary.Min, 1000}, "p50": {summary.P50, 50_000}, "p90": {summary.P90, 90_000},
		"p99": {summary.P99, 99_000}, "p99.9": {summary.P999, 100_000}, "max": {summary.Max, 100_000},
		"mean": {summary.Mean, 50_500},
	} {
		if pair[0] != pair[1] {
			t.Errorf("%s = %v µs, want %v", name, pair[0], pair[1])
		}
	}
	if single := summarize([]time.Duration{time.Second}); single.P50 != 1e6 || single.P999 != 1e6 {
		t.Errorf("one sample: p50 %v, p99.9 %v, want 1e6 for both", single.P50, single.P999)
	}
	if empty := summarize(nil); empty.Count != 0 {
		t.Errorf("no samples: count %d, want 0", empty.Count)
	}
}

func TestMedian(t *testing.T) {
	for _, test := range []struct {
		values []float64
		want   float64
	}{{nil, 0}, {[]float64{7}, 7}, {[]float64{9, 1, 5}, 5}, {[]float64{4, 1, 3, 2}, 2.5}} {
		if got := median(test.values); got != test.want {
			t.Errorf("median(%v) = %v, want %v", test.values, got, test.want)
		}
	}
}

func TestWindowOpensWithItsFirstRequest(t *testing.T) {
	var limit window
	start := time.Unix(1_700_000_000, 0)
	for i, want := range []int{1, 0} {
		remaining, resetAfter, allowed := limit.take(start.Add(time.Duration(i)*time.Second), 2, 5*time.Second)
		if !allowed || remaining != want || resetAfter != time.Duration(5-i)*time.Second {
			t.Fatalf("request %d: allowed %v, remaining %d, reset after %v", i+1, allowed, remaining, resetAfter)
		}
	}
	if _, resetAfter, allowed := limit.take(start.Add(4*time.Second), 2, 5*time.Second); allowed || resetAfter != time.Second {
		t.Fatalf("third request in the window: allowed %v, reset after %v, want refused with 1s left", allowed, resetAfter)
	}
	if remaining, _, allowed := limit.take(start.Add(5*time.Second), 2, 5*time.Second); !allowed || remaining != 1 {
		t.Fatalf("first request of the next window: allowed %v, remaining %d", allowed, remaining)
	}
}

func TestRouteOfBlanksIdentifiersAndKeepsTheScope(t *testing.T) {
	for _, test := range []struct{ method, path, route, scope string }{
		{"POST", "/api/v10/channels/1200000000000000001/messages", "POST/channels/!/messages", "1200000000000000001"},
		{"GET", "/api/v10/channels/12/messages/34", "GET/channels/!/messages/!", "12"},
		{"GET", "/api/v10/guilds/55/members/66", "GET/guilds/!/members/!", "55"},
		{"GET", "/api/v10/users/@me", "GET/users/@me", ""},
		{"GET", "/api/users/77", "GET/users/!", ""},
	} {
		if route, scope := routeOf(test.method, test.path); route != test.route || scope != test.scope {
			t.Errorf("routeOf(%s %s) = %q, %q; want %q, %q", test.method, test.path, route, scope, test.route, test.scope)
		}
	}
}

func TestSampleMessageIsJSON(t *testing.T) {
	var message map[string]any
	if err := json.Unmarshal([]byte(sampleMessage), &message); err != nil {
		t.Fatal(err)
	}
}

func startMock(t *testing.T, routeLimit int, routeWindow time.Duration, globalLimit int) (*mockDiscord, *httptest.Server) {
	t.Helper()
	discord, err := newMockDiscord(routeLimit, routeWindow, globalLimit, 0)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(discord)
	t.Cleanup(server.Close)
	return discord, server
}

func TestMockRejectsLikeDiscord(t *testing.T) {
	_, server := startMock(t, 1, time.Minute, 1000)
	post := func() *http.Response {
		request, _ := http.NewRequest(http.MethodPost, server.URL+messagesPath(0), strings.NewReader(`{}`))
		request.Header.Set("Authorization", botAuthorization(benchBot))
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	first := post()
	_ = first.Body.Close()
	if first.StatusCode != http.StatusOK || first.Header.Get("X-RateLimit-Remaining") != "0" || first.Header.Get("Via") == "" {
		t.Fatalf("first request: status %d, headers %v", first.StatusCode, first.Header)
	}
	second := post()
	body, _ := io.ReadAll(second.Body)
	_ = second.Body.Close()
	reply := answer{status: second.StatusCode, header: second.Header, payload: body}
	wait, global := reply.retryAfter()
	if reply.label() != "429 discord" || global || wait <= 59*time.Second || wait > time.Minute ||
		second.Header.Get("Retry-After") != "60" || second.Header.Get("X-RateLimit-Scope") != "user" {
		t.Fatalf("second request: %s, wait %v, global %v, headers %v, body %s", reply.label(), wait, global, second.Header, body)
	}
}

func TestMockGlobalLimitIsPerBot(t *testing.T) {
	discord, server := startMock(t, 1000, time.Minute, 2)
	raw := &sender{http: server.Client(), base: server.URL, authorization: botAuthorization(benchBot)}
	other := &sender{http: server.Client(), base: server.URL, authorization: botAuthorization("900000000000000002")}
	for i := range 3 {
		reply, err := raw.send(context.Background(), http.MethodPost, messagesPath(i), []byte(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		if _, global := reply.retryAfter(); (i < 2) != reply.ok() || (i == 2 && !global) {
			t.Fatalf("request %d: status %d, headers %v", i+1, reply.status, reply.header)
		}
	}
	if reply, err := other.send(context.Background(), http.MethodPost, messagesPath(0), []byte(`{}`)); err != nil || !reply.ok() {
		t.Fatalf("another bot within the same second: %v, %v", reply.status, err)
	}
	if stats := discord.stats(); stats.Received != 4 || stats.Succeeded != 3 || stats.LimitedGlobal != 1 {
		t.Fatalf("stats = %+v", stats)
	}
}

// One process stays inside a route's limit on its own. Two that share a token cannot: each
// counts the limit as if it were alone. This is the difference the benchmark measures.
func TestLibraryClientsCollideOnlyAcrossProcesses(t *testing.T) {
	const each = 6
	body := []byte(`{}`)
	send := func(clients []*libraryClient) {
		var requests sync.WaitGroup
		for _, client := range clients {
			for range each {
				requests.Go(func() {
					if err := client.do(context.Background(), http.MethodPost, messagesPath(0), routeKey(http.MethodPost, 0), body); err != nil {
						t.Error(err)
					}
				})
			}
		}
		requests.Wait()
	}
	client := func(server *httptest.Server, counts *tally) *libraryClient {
		return newLibraryClient(&sender{http: server.Client(), base: server.URL, authorization: botAuthorization(benchBot)}, counts)
	}

	discord, server := startMock(t, 2, 300*time.Millisecond, 1000)
	alone := newTally()
	send([]*libraryClient{client(server, alone)})
	if stats := discord.stats(); stats.Succeeded != each || stats.LimitedRoute != 0 || alone.responses["429 discord"] != 0 {
		t.Fatalf("one process: mock %+v, client %v", stats, alone.responses)
	}

	discord, server = startMock(t, 2, 300*time.Millisecond, 1000)
	shared := newTally()
	send([]*libraryClient{client(server, shared), client(server, shared)})
	stats := discord.stats()
	if stats.Succeeded != 2*each || stats.LimitedRoute == 0 || int64(shared.responses["429 discord"]) != stats.LimitedRoute {
		t.Fatalf("two processes: mock %+v, client %v", stats, shared.responses)
	}
	run := result{Via: "direct", Attempts: shared.attempts, Succeeded: shared.succeeded, Responses: shared.responses, Mock: stats}
	if check := reconcile(&run); check != "ok" {
		t.Fatalf("reconcile = %q", check)
	}
}

func TestReconcileCatchesLostRequests(t *testing.T) {
	run := result{Via: "direct", Attempts: 10, Succeeded: 9, Responses: map[string]int{"429 discord": 1},
		Mock: mockStats{Received: 10, Succeeded: 9, LimitedRoute: 1}}
	if check := reconcile(&run); check != "ok" {
		t.Fatalf("matching counts: %q", check)
	}
	for name, change := range map[string]func(*result){
		"a success the mock never saw": func(r *result) { r.Succeeded++ },
		"a request that went missing":  func(r *result) { r.Mock.Received-- },
		"a 429 nobody received":        func(r *result) { r.Mock.LimitedGlobal++ },
	} {
		broken := run
		change(&broken)
		if check := reconcile(&broken); !strings.HasPrefix(check, "MISMATCH") {
			t.Errorf("%s: %q", name, check)
		}
	}
	// Through Sluice the mock also receives Sluice's own retries, so only successes must match.
	proxied := run
	proxied.Via, proxied.Mock.Received = "sluice", 14
	if check := reconcile(&proxied); check != "ok" {
		t.Errorf("through Sluice: %q", check)
	}
}

// The reading taken when a run stops comes a few milliseconds after the last one. Its values count,
// but a CPU rate over that interval would be noise: here, 10 cores for a process using half of one.
func TestUsageSkipsIntervalsTooShortToMeasure(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	at := func(milliseconds int, cpuSeconds, rssBytes, goroutines float64) reading {
		return reading{
			at:         start.Add(time.Duration(milliseconds) * time.Millisecond),
			cpuSeconds: cpuSeconds, rssBytes: rssBytes, goroutines: goroutines,
			failures: map[string]float64{"queue_timeout": 0, "queue_full": 2},
		}
	}
	measured := []reading{at(0, 1, 10, 5), at(1000, 1.5, 12, 6), at(2000, 2, 30, 9), at(2004, 2.04, 11, 4)}
	usage := usageOf(reading{rssBytes: 8}, measured, time.Second)

	if len(usage.Series) != 2 || usage.Series[0].CPUCores != 0.5 || usage.Series[1].CPUCores != 0.5 || usage.Series[1].Seconds != 2 {
		t.Fatalf("series = %+v, want the two whole intervals at half a core", usage.Series)
	}
	if mean := 1.04 / 2.004; usage.CPUCoresMean < mean-1e-9 || usage.CPUCoresMean > mean+1e-9 {
		t.Errorf("mean = %v cores, want %v over the whole run", usage.CPUCoresMean, mean)
	}
	if usage.IdleRSSBytes != 8 || usage.RSSPeakBytes != 30 || usage.RSSEndBytes != 11 || usage.GoroutinesPeak != 9 || usage.GoroutinesEnd != 4 {
		t.Errorf("usage = %+v", usage)
	}
	if len(usage.Failures) != 1 || usage.Failures["queue_full"] != 2 {
		t.Errorf("failures = %v, want only the reason that occurred", usage.Failures)
	}
	if empty := usageOf(reading{rssBytes: 8}, nil, time.Second); empty.IdleRSSBytes != 8 || len(empty.Series) != 0 {
		t.Errorf("no readings: %+v", empty)
	}
}

func TestScrapeReadsSluiceMetrics(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "# HELP go_goroutines Number of goroutines.\n# TYPE go_goroutines gauge\ngo_goroutines 17\n"+
			"process_cpu_seconds_total 1.25\nprocess_resident_memory_bytes 2.4117248e+07\n"+
			"sluice_failures_total{reason=\"queue_timeout\"} 3\nsluice_failures_total{reason=\"queue_full\"} 0\n"+
			"sluice_requests_count{method=\"POST\",route=\"/channels/!/messages\",status=\"200 OK\"} 99\n")
	}))
	defer server.Close()
	current, err := scrape(server.Client(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if current.goroutines != 17 || current.cpuSeconds != 1.25 || current.rssBytes != 24117248 ||
		current.failures["queue_timeout"] != 3 || len(current.failures) != 2 {
		t.Fatalf("reading = %+v", current)
	}
}
