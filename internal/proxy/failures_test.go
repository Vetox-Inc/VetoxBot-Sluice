package proxy

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// lockedBuffer is a log sink that a test can read while the proxy still writes to it.
type lockedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(data)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

func captureLog(t *testing.T) *lockedBuffer {
	t.Helper()
	output := &lockedBuffer{}
	previous := logger
	SetLogger(NewLogger(output, slog.LevelInfo, "text"))
	t.Cleanup(func() { logger = previous })
	return output
}

func waitForLog(t *testing.T, output *lockedBuffer, want string) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if logged := output.String(); strings.Contains(logged, want) {
			return logged
		}
		if time.Now().After(deadline) {
			t.Fatalf("log never held %q:\n%s", want, output.String())
		}
		time.Sleep(time.Millisecond)
	}
}

func failureCount(reason string) float64 {
	return testutil.ToFloat64(ProxyFailures.WithLabelValues(reason))
}

// holdUpstream returns a transport that reports each request on arrived and then holds it until
// release is closed or the request ends.
func holdUpstream(arrived chan<- struct{}, release <-chan struct{}) roundTripFunc {
	return func(request *http.Request) (*http.Response, error) {
		arrived <- struct{}{}
		select {
		case <-release:
			return testResponse(request, http.StatusOK, nil, nil), nil
		case <-request.Context().Done():
			return nil, request.Context().Err()
		}
	}
}

func awaitSignal(t *testing.T, signal <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func TestEveryAnswerOfSluicesOwnIsCounted(t *testing.T) {
	okay := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return jsonResponse(request, http.StatusOK, `{}`), nil
	})
	bot := "Bot " + fakeBotToken
	otherBot := "Bot " + strings.Join([]string{
		base64.RawStdEncoding.EncodeToString([]byte("987654321098765432")), "GAbCdE", strings.Repeat("y", 38),
	}, ".")

	// held starts a request that stays at the upstream until the returned func releases it.
	held := func(t *testing.T, proxy *Proxy, arrived chan struct{}, release chan struct{}, path, authorization string) func() {
		t.Helper()
		done := make(chan struct{})
		go func() {
			defer close(done)
			serve(proxy, http.MethodGet, path, authorization, nil)
		}()
		awaitSignal(t, arrived, "the held request to reach the upstream")
		return func() {
			close(release)
			awaitSignal(t, done, "the held request to finish")
		}
	}

	cases := []struct {
		reason string
		status int
		run    func(t *testing.T) *httptest.ResponseRecorder
	}{
		{"unknown_endpoint", http.StatusNotFound, func(t *testing.T) *httptest.ResponseRecorder {
			return serve(newTestProxy(t, testConfig(okay)), http.MethodGet, "/sluice/nothing-here", "", nil)
		}},
		{"bad_request", http.StatusBadRequest, func(t *testing.T) *httptest.ResponseRecorder {
			return serve(newTestProxy(t, testConfig(okay)), http.MethodGet, "/api/v10/channels/%2e%2e/gateway", "", nil)
		}},
		{"bad_request", http.StatusBadRequest, func(t *testing.T) *httptest.ResponseRecorder {
			request := httptest.NewRequest(http.MethodGet, "/api/v10/gateway", nil)
			request.Header.Set("Upgrade", "websocket")
			response := httptest.NewRecorder()
			newTestProxy(t, testConfig(okay)).ServeHTTP(response, request)
			return response
		}},
		{"in_flight_limit", http.StatusServiceUnavailable, func(t *testing.T) *httptest.ResponseRecorder {
			arrived, release := make(chan struct{}, 1), make(chan struct{})
			config := testConfig(holdUpstream(arrived, release))
			config.MaxInFlightRequests = 1
			proxy := newTestProxy(t, config)
			defer held(t, proxy, arrived, release, "/api/v10/gateway", "")()
			return serve(proxy, http.MethodGet, "/api/v10/gateway", "", nil)
		}},
		{"client_limit", http.StatusServiceUnavailable, func(t *testing.T) *httptest.ResponseRecorder {
			arrived, release := make(chan struct{}, 1), make(chan struct{})
			config := testConfig(holdUpstream(arrived, release))
			config.MaxClientStates = 1
			proxy := newTestProxy(t, config)
			defer held(t, proxy, arrived, release, "/api/v10/users/@me", bot)()
			return serve(proxy, http.MethodGet, "/api/v10/users/@me", otherBot, nil)
		}},
		{"bucket_limit", http.StatusServiceUnavailable, func(t *testing.T) *httptest.ResponseRecorder {
			arrived, release := make(chan struct{}, 1), make(chan struct{})
			config := testConfig(holdUpstream(arrived, release))
			config.MaxBucketStates = 1
			proxy := newTestProxy(t, config)
			defer held(t, proxy, arrived, release, "/api/v10/channels/111111111111111111/messages", "")()
			return serve(proxy, http.MethodGet, "/api/v10/channels/222222222222222222/messages", "", nil)
		}},
		{"invalid_request_budget", http.StatusServiceUnavailable, func(t *testing.T) *httptest.ResponseRecorder {
			config := testConfig(okay)
			config.InvalidRequestLimit = 1
			proxy := newTestProxy(t, config)
			now := time.Now()
			proxy.invalidRequests.reserve(now)
			proxy.invalidRequests.complete(now, true)
			return serve(proxy, http.MethodGet, "/api/v10/gateway", "", nil)
		}},
		{"retry_capture_limit", http.StatusServiceUnavailable, func(t *testing.T) *httptest.ResponseRecorder {
			config := testConfig(okay)
			config.MaxRetryCaptureBytes = 3
			response := httptest.NewRecorder()
			newTestProxy(t, config).ServeHTTP(response, captureRequest("1234", 4))
			return response
		}},
		{"shutting_down", http.StatusServiceUnavailable, func(t *testing.T) *httptest.ResponseRecorder {
			proxy := newTestProxy(t, testConfig(okay))
			if err := proxy.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			return serve(proxy, http.MethodGet, "/api/v10/gateway", "", nil)
		}},
		{"invalid_token", http.StatusUnauthorized, func(t *testing.T) *httptest.ResponseRecorder {
			proxy := newTestProxy(t, testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
				return jsonResponse(request, http.StatusUnauthorized, `{"message": "401: Unauthorized", "code": 0}`), nil
			})))
			if first := serve(proxy, http.MethodGet, "/api/v10/users/@me", bot, nil); first.Header().Get("Generated-By-Proxy") != "" {
				t.Fatal("the first 401 has to come from Discord")
			}
			return serve(proxy, http.MethodGet, "/api/v10/users/@me", bot, nil)
		}},
		{"cloudflare_pause", http.StatusTooManyRequests, func(t *testing.T) *httptest.ResponseRecorder {
			proxy := newTestProxy(t, testConfig(okay))
			proxy.cloudflare.settle(time.Now(), edgeBlocked, time.Minute)
			return serve(proxy, http.MethodGet, "/api/v10/gateway", "", nil)
		}},
		{"upstream_error", http.StatusBadGateway, func(t *testing.T) *httptest.ResponseRecorder {
			return serve(newTestProxy(t, testConfig(roundTripFunc(func(*http.Request) (*http.Response, error) {
				return nil, errors.New("connection refused")
			}))), http.MethodGet, "/api/v10/gateway", "", nil)
		}},
	}
	for _, test := range cases {
		t.Run(test.reason, func(t *testing.T) {
			before := failureCount(test.reason)
			response := test.run(t)
			if response.Code != test.status {
				t.Fatalf("status = %d, want %d", response.Code, test.status)
			}
			if got := failureCount(test.reason) - before; got != 1 {
				t.Fatalf("%s counted %v times, want once", test.reason, got)
			}
		})
	}
}

func TestClientThatLeavesIsCountedAndNotAnswered(t *testing.T) {
	arrived, release := make(chan struct{}, 1), make(chan struct{})
	defer close(release)
	proxy := newTestProxy(t, testConfig(holdUpstream(arrived, release)))
	ctx, cancel := context.WithCancel(context.Background())
	response := httptest.NewRecorder()
	done := make(chan struct{})
	before := failureCount("client_closed")
	go func() {
		defer close(done)
		proxy.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v10/gateway", nil).WithContext(ctx))
	}()
	awaitSignal(t, arrived, "the request to reach the upstream")
	cancel()
	awaitSignal(t, done, "the abandoned request to end")
	if got := failureCount("client_closed") - before; got != 1 {
		t.Fatalf("client_closed counted %v times, want once", got)
	}
	if response.Body.Len() != 0 {
		t.Fatalf("a client that left was answered: %q", response.Body.String())
	}
}

func TestClientThatLeavesMidUploadIsNotAnUpstreamFailure(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()
	for name, announced := range map[string]int{"kept for a retry": 512 << 10, "streamed": 4 << 20} {
		t.Run(name, func(t *testing.T) {
			output := captureLog(t)
			config := testConfig(nil)
			config.DiscordURL = upstream.URL
			server := httptest.NewServer(newTestProxy(t, config))
			defer server.Close()

			closed, failed := failureCount("client_closed"), failureCount("upstream_error")
			connection, err := net.Dial("tcp", server.Listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			head := "POST /api/v10/channels/111111111111111111/messages HTTP/1.1\r\nHost: sluice\r\n" +
				"Content-Type: application/octet-stream\r\nContent-Length: " + strconv.Itoa(announced) + "\r\n\r\n"
			if _, err := connection.Write(append([]byte(head), make([]byte, 64<<10)...)); err != nil {
				t.Fatal(err)
			}
			// Long enough for the part that was sent to be on its way to the upstream.
			time.Sleep(100 * time.Millisecond)
			if err := connection.Close(); err != nil {
				t.Fatal(err)
			}

			deadline := time.Now().Add(2 * time.Second)
			for failureCount("client_closed") == closed && failureCount("upstream_error") == failed && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if got := failureCount("upstream_error") - failed; got != 0 {
				t.Errorf("a client that left mid-upload was counted as %v upstream errors", got)
			}
			if got := failureCount("client_closed") - closed; got != 1 {
				t.Errorf("client_closed counted %v times, want once", got)
			}
			if logged := output.String(); strings.Contains(logged, "Could not reach Discord") {
				t.Errorf("a client that left was logged as Discord being unreachable:\n%s", logged)
			}
		})
	}
}

func TestFailuresAnOperatorMustActOnAreLogged(t *testing.T) {
	output := captureLog(t)
	config := testConfig(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("dial tcp 203.0.113.7:443: connect: connection refused")
	}))
	proxy := newTestProxy(t, config)
	for range 5 {
		serve(proxy, http.MethodPost, "/api/v10/channels/111111111111111111/messages", "", strings.NewReader("{}"))
	}
	logged := waitForLog(t, output, "Could not reach Discord")
	for _, want := range []string{"level=WARN", "reason=upstream_error", "method=POST", "route=/channels/!/messages", "connection refused"} {
		if !strings.Contains(logged, want) {
			t.Errorf("warning lacks %q:\n%s", want, logged)
		}
	}
	if warnings := strings.Count(logged, "Could not reach Discord"); warnings != 1 {
		t.Errorf("five failures in a row logged %d warnings, want 1:\n%s", warnings, logged)
	}
	if strings.Contains(logged, "111111111111111111") {
		t.Errorf("the warning names a channel:\n%s", logged)
	}
}

func TestFailureWarningNeverQuotesAWebhookToken(t *testing.T) {
	const webhookToken = "hook-token-in-the-path"
	output := captureLog(t)
	config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return nil, errors.New("Post \"" + request.URL.String() + "\": connection reset by peer")
	}))
	proxy := newTestProxy(t, config)
	serve(proxy, http.MethodPost, "/api/v10/webhooks/222222222222222222/"+webhookToken, "", strings.NewReader("{}"))
	logged := waitForLog(t, output, "Could not reach Discord")
	if strings.Contains(logged, webhookToken) {
		t.Fatalf("the warning quotes the webhook's token:\n%s", logged)
	}
	for _, want := range []string{"route=/webhooks/!/!", "/webhooks/222222222222222222/:token", "connection reset by peer"} {
		if !strings.Contains(logged, want) {
			t.Errorf("warning lacks %q:\n%s", want, logged)
		}
	}
}

func TestFailureLogSpacesWarningsAndCountsTheRest(t *testing.T) {
	log := newFailureLog()
	request := httptest.NewRequest(http.MethodGet, "/api/v10/gateway", nil)
	start := time.Now()
	log.report(start, "upstream_error", request, nil)
	log.report(start.Add(time.Second), "upstream_error", request, nil)
	log.report(start.Add(2*time.Second), "upstream_error", request, nil)
	log.report(start.Add(3*time.Second), "upstream_timeout", request, nil)
	log.report(start.Add(failureWarningInterval), "upstream_error", request, nil)

	var got []failureWarning
	for len(log.warnings) > 0 {
		got = append(got, <-log.warnings)
	}
	if len(got) != 3 {
		t.Fatalf("queued %d warnings, want 3: %+v", len(got), got)
	}
	if got[0].reason != "upstream_error" || got[0].suppressed != 0 {
		t.Errorf("first warning = %+v, want upstream_error with nothing suppressed", got[0])
	}
	if got[1].reason != "upstream_timeout" {
		t.Errorf("another reason was held back by the first: %+v", got[1])
	}
	if got[2].reason != "upstream_error" || got[2].suppressed != 2 {
		t.Errorf("warning after the interval = %+v, want upstream_error with 2 suppressed", got[2])
	}
}

func TestFullFailureLogNeverBlocksARequest(t *testing.T) {
	log := newFailureLog()
	request := httptest.NewRequest(http.MethodGet, "/api/v10/gateway", nil)
	start := time.Now()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for step := range 4 * cap(log.warnings) {
			log.report(start.Add(time.Duration(step)*failureWarningInterval), "upstream_error", request, nil)
		}
	}()
	awaitSignal(t, done, "reports into a log nobody reads")
	if len(log.warnings) != cap(log.warnings) {
		t.Fatalf("queued %d warnings, want the queue's capacity %d", len(log.warnings), cap(log.warnings))
	}
}

func TestFailureReasonsMatchTheCodeAndTheDocs(t *testing.T) {
	listed := make(map[string]bool, len(failureReasons))
	for _, reason := range failureReasons {
		if listed[reason] {
			t.Errorf("%s is listed twice", reason)
		}
		listed[reason] = true
	}
	for reason := range failureWarnings {
		if !listed[reason] {
			t.Errorf("failureWarnings has %s, which failureReasons does not list", reason)
		}
	}

	used := make(map[string]bool)
	sources, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	counted := regexp.MustCompile(`\.fail\("([a-z_]+)"|reason :?= "([a-z_]+)"`)
	for _, name := range sources {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		source, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if name != "failures.go" && strings.Contains(string(source), "ProxyFailures.WithLabelValues(") {
			t.Errorf("%s counts a failure without Proxy.fail", name)
		}
		for _, match := range counted.FindAllStringSubmatch(string(source), -1) {
			used[match[1]+match[2]] = true
		}
	}
	for reason := range used {
		if !listed[reason] {
			t.Errorf("the code counts %s, which failureReasons does not list", reason)
		}
	}

	reference, err := os.ReadFile(filepath.Join("..", "..", "CONFIG.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, reason := range failureReasons {
		if !used[reason] {
			t.Errorf("failureReasons lists %s, which the code never counts", reason)
		}
		if !strings.Contains(string(reference), "| `"+reason+"` |") {
			t.Errorf("CONFIG.md has no row for the failure reason %s", reason)
		}
	}
}

func TestEveryFailureReasonIsExportedFromTheStart(t *testing.T) {
	families, err := newMetricSet("sluice").registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	exported := make(map[string]float64)
	for _, family := range families {
		if family.GetName() != "sluice_failures_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			exported[metric.GetLabel()[0].GetValue()] = metric.GetCounter().GetValue()
		}
	}
	for _, reason := range failureReasons {
		if value, ok := exported[reason]; !ok || value != 0 {
			t.Errorf("%s at start = %v (exported: %t), want 0", reason, value, ok)
		}
	}
}

func TestResponseCutAfterItsHeadersIsCounted(t *testing.T) {
	output := captureLog(t)
	proxy := newTestProxy(t, testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		body := io.MultiReader(strings.NewReader(`{"half":`), iotest.ErrReader(errors.New("connection reset by peer")))
		return testResponse(request, http.StatusOK, discordVia.Clone(), io.NopCloser(body)), nil
	})))
	server := httptest.NewServer(proxy)
	defer server.Close()

	before := failureCount("response_aborted")
	response, err := http.Get(server.URL + "/api/v10/gateway")
	if err == nil {
		_, err = io.ReadAll(response.Body)
		_ = response.Body.Close()
	}
	if err == nil {
		t.Fatal("the client received a whole response")
	}
	if got := failureCount("response_aborted") - before; got != 1 {
		t.Fatalf("response_aborted counted %v times, want once", got)
	}
	if logged := waitForLog(t, output, "ended before its body did"); !strings.Contains(logged, "connection reset by peer") {
		t.Errorf("the warning lacks the cause:\n%s", logged)
	}
}

func TestBucketLimitFreesUnusedBucketsBeforeRefusing(t *testing.T) {
	config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return jsonResponse(request, http.StatusOK, `{}`), nil
	}))
	config.MaxBucketStates = 2
	proxy := newTestProxy(t, config)
	before := failureCount("bucket_limit")
	for _, channel := range []string{"111111111111111111", "222222222222222222", "333333333333333333"} {
		if response := serve(proxy, http.MethodGet, "/api/v10/channels/"+channel+"/messages", "", nil); response.Code != http.StatusOK {
			t.Fatalf("channel %s got %d, want 200: nothing was using the first two buckets", channel, response.Code)
		}
	}
	if got := failureCount("bucket_limit") - before; got != 0 {
		t.Fatalf("bucket_limit counted %v times, want none", got)
	}
	if used := proxy.bucketSlots.used.Load(); used < 1 || used > 2 {
		t.Fatalf("%d bucket slots in use, want 1 or 2", used)
	}
}

func TestBucketReclaimRunsAtMostOnceASecond(t *testing.T) {
	config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return jsonResponse(request, http.StatusOK, `{}`), nil
	}))
	config.MaxBucketStates = 1
	proxy := newTestProxy(t, config)
	status := func(channel string) int {
		return serve(proxy, http.MethodGet, "/api/v10/channels/"+channel+"/messages", "", nil).Code
	}
	if first, second := status("111111111111111111"), status("222222222222222222"); first != http.StatusOK || second != http.StatusOK {
		t.Fatalf("got %d and %d, want 200 twice: the second frees the bucket the first left idle", first, second)
	}
	before := failureCount("bucket_limit")
	if got := status("333333333333333333"); got != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want 503: the table was swept a moment ago", got)
	}
	if got := failureCount("bucket_limit") - before; got != 1 {
		t.Fatalf("bucket_limit counted %v times, want once", got)
	}
	proxy.reclaimMu.Lock()
	proxy.reclaimedAt = time.Now().Add(-2 * bucketReclaimInterval)
	proxy.reclaimMu.Unlock()
	if got := status("333333333333333333"); got != http.StatusOK {
		t.Fatalf("got %d, want 200 once the interval has passed", got)
	}
}

func TestPeerRequestWhoseClientLeftIsNotAPeerError(t *testing.T) {
	output := captureLog(t)
	proxy := newTestProxy(t, testConfig(nil))
	request := httptest.NewRequest(http.MethodGet, "/api/v10/gateway", nil)
	closed, failed := failureCount("client_closed"), failureCount("peer_error")

	response := httptest.NewRecorder()
	proxy.peerProxy.ErrorHandler(response, request, context.Canceled)
	if left, peer := failureCount("client_closed")-closed, failureCount("peer_error")-failed; left != 1 || peer != 0 {
		t.Fatalf("a client that left counted as %v client_closed and %v peer_error, want 1 and 0", left, peer)
	}
	if response.Body.Len() != 0 {
		t.Fatalf("a client that left was answered: %q", response.Body.String())
	}

	response = httptest.NewRecorder()
	proxy.peerProxy.ErrorHandler(response, request, errors.New("dial tcp 203.0.113.9:8443: connect: connection refused"))
	if got := failureCount("peer_error") - failed; got != 1 || response.Code != http.StatusServiceUnavailable {
		t.Fatalf("an unreachable peer counted %v times and answered %d, want once and 503", got, response.Code)
	}
	if logged := waitForLog(t, output, "Could not reach a cluster peer"); strings.Count(logged, "level=WARN") != 1 {
		t.Errorf("want the one warning, for the peer that could not be reached:\n%s", logged)
	}
}

func TestBotIDLongerThanAUserIDIsNotKept(t *testing.T) {
	token := func(id string) string {
		return base64.RawStdEncoding.EncodeToString([]byte(id)) + ".GAbCdE." + strings.Repeat("x", 38)
	}
	for id, want := range map[string]string{
		"123456789012345678":     "123456789012345678",
		strings.Repeat("9", 20):  strings.Repeat("9", 20),
		strings.Repeat("9", 21):  "",
		strings.Repeat("9", 400): "",
	} {
		if got := botIDFromToken(token(id)); got != want {
			t.Errorf("botIDFromToken with a %d-digit ID = %q, want %q", len(id), got, want)
		}
	}
}

// promtool tests the alert rules against series written by hand, so only this ties the metrics
// and reasons they name to what the code exports.
func TestAlertRulesNameOnlyWhatSluiceExports(t *testing.T) {
	file, err := os.ReadFile(filepath.Join("..", "..", "prometheus", "alerts.yml"))
	if err != nil {
		t.Fatal(err)
	}
	rules := regexp.MustCompile(`(?m)^\s*#.*$`).ReplaceAllString(string(file), "")

	reasons := regexp.MustCompile(`reason(=~|=)"([^"]+)"`).FindAllStringSubmatch(rules, -1)
	if len(reasons) == 0 {
		t.Fatal("found no rule that names a failure reason")
	}
	for _, matcher := range reasons {
		for _, alternative := range strings.Split(matcher[2], "|") {
			pattern, err := regexp.Compile("^(?:" + alternative + ")$")
			if err != nil {
				t.Fatalf("reason matcher %q: %v", matcher[2], err)
			}
			if !slices.ContainsFunc(failureReasons, pattern.MatchString) {
				t.Errorf("an alert rule waits for the reason %q, which Sluice never counts", alternative)
			}
		}
	}

	exported := make(map[string]bool)
	for _, suffix := range metricSuffixes {
		exported[DefaultMetricsNamespace+"_"+suffix] = true
	}
	for _, histogram := range []string{"requests", "queue_wait_seconds"} {
		for _, series := range []string{"_bucket", "_count", "_sum"} {
			exported[DefaultMetricsNamespace+"_"+histogram+series] = true
		}
	}
	named := regexp.MustCompile(`\b`+DefaultMetricsNamespace+`_[a-z_]+`).FindAllString(rules, -1)
	if len(named) == 0 {
		t.Fatal("found no rule that names a metric")
	}
	for _, metric := range named {
		if !exported[metric] {
			t.Errorf("an alert rule reads %s, which Sluice does not export", metric)
		}
	}

	callback := MetricsPathFromBucket(GetOptimisticBucketPath("/api/v10/interactions/203039963636301824/token/callback", http.MethodPost))
	if !strings.Contains(rules, `route!="`+callback+`"`) {
		t.Errorf("no alert rule leaves out the interaction callback route as it is labelled, %q", callback)
	}
}

func TestBucketsInUseOrCoolingDownAreNeverReclaimed(t *testing.T) {
	proxy := newTestProxy(t, testConfig(nil))
	cooling := mustBucket(t, proxy.noAuth, 1)
	cooling.blockUntil(time.Now().Add(time.Minute))
	busy := mustBucket(t, proxy.noAuth, 2)
	if err := busy.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer busy.release()
	mustBucket(t, proxy.noAuth, 3)

	proxy.sweepBuckets(time.Now(), 0)
	if got := proxy.bucketSlots.used.Load(); got != 2 {
		t.Fatalf("%d buckets left, want the cooling one and the busy one", got)
	}
	if mustBucket(t, proxy.noAuth, 1) != cooling || mustBucket(t, proxy.noAuth, 2) != busy {
		t.Fatal("a bucket in use or cooling down was replaced")
	}
}

func TestQueueWaitCountsRequestsToldToComeBack(t *testing.T) {
	arrived, release := make(chan struct{}, 1), make(chan struct{})
	config := testConfig(holdUpstream(arrived, release))
	config.QueueTimeout = 60 * time.Millisecond
	proxy := newTestProxy(t, config)
	const path = "/api/v10/channels/444444444444444444/pins"
	waits := func() uint64 {
		families, err := metricsRegistry.Gather()
		if err != nil {
			t.Fatal(err)
		}
		for _, family := range families {
			if family.GetName() != "sluice_queue_wait_seconds" {
				continue
			}
			for _, metric := range family.GetMetric() {
				for _, label := range metric.GetLabel() {
					if label.GetName() == "route" && label.GetValue() == "/channels/!/pins" {
						return metric.GetHistogram().GetSampleCount()
					}
				}
			}
		}
		return 0
	}

	before := waits()
	done := make(chan struct{})
	go func() {
		defer close(done)
		serve(proxy, http.MethodGet, path, "", nil)
	}()
	awaitSignal(t, arrived, "the first request to hold the bucket")
	if response := serve(proxy, http.MethodGet, path, "", nil); response.Code != http.StatusTooManyRequests {
		t.Fatalf("the queued request got %d, want 429", response.Code)
	}
	close(release)
	awaitSignal(t, done, "the first request to finish")
	if got := waits() - before; got != 2 {
		t.Fatalf("queue_wait_seconds observed %d requests, want the one sent and the one told to come back", got)
	}
}

func TestGlobalLimitIsExportedForEachClient(t *testing.T) {
	config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return jsonResponse(request, http.StatusOK, `{}`), nil
	}))
	config.GlobalOverrides = "123456789012345678:120"
	proxy := newTestProxy(t, config)
	serve(proxy, http.MethodGet, "/api/v10/users/@me", "Bot "+fakeBotToken, nil)

	limits := gaugesByLabel(t, "sluice_global_limit", "clientId")
	if limits["123456789012345678"] != 120 || limits["NoAuth"] != discordGlobalLimit {
		t.Fatalf("global limits = %v, want 120 for the bot and %d for NoAuth", limits, discordGlobalLimit)
	}
}

func TestWarningsAreCounted(t *testing.T) {
	before := testutil.ToFloat64(WarningCounter)
	testLogger := NewLogger(&bytes.Buffer{}, slog.LevelInfo, "text")
	testLogger.Info("not a warning")
	testLogger.Warn("a warning")
	testLogger.Error("an error, counted by sluice_error")
	if got := testutil.ToFloat64(WarningCounter) - before; got != 1 {
		t.Fatalf("warnings counter increased by %v, want 1", got)
	}
}

func TestHugeCloudflarePauseIsCappedBeforeDurationConversion(t *testing.T) {
	for value, want := range map[string]time.Duration{
		"1e300": maxCloudflarePause,
		"9e9":   maxCloudflarePause,
		"7200":  maxCloudflarePause,
		"30":    30 * time.Second,
		"0.2":   time.Second,
		"-5":    defaultCloudflarePause,
		"soon":  defaultCloudflarePause,
	} {
		if got := cloudflarePause(testHeaders(map[string]string{"Retry-After": value})); got != want {
			t.Errorf("Retry-After %s pauses for %s, want %s", value, got, want)
		}
	}
}

func TestWithoutTheLockA401DoesNotVouchForAToken(t *testing.T) {
	var calls atomic.Int64
	config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls.Add(1)
		return jsonResponse(request, http.StatusUnauthorized, `{"message": "401: Unauthorized", "code": 0}`), nil
	}))
	config.Disable401Lock = true
	proxy := newTestProxy(t, config)
	for range 2 {
		serve(proxy, http.MethodGet, "/api/v10/users/@me", "Bot "+fakeBotToken, nil)
	}
	if calls.Load() != 2 {
		t.Fatalf("Discord saw %d requests, want both: the lock is off", calls.Load())
	}
	proxy.clientsMu.Lock()
	defer proxy.clientsMu.Unlock()
	for _, state := range proxy.bots {
		if label := state.metricsLabel(); label != "Unverified" {
			t.Fatalf("a token Discord answered 401 is labelled %q, want Unverified", label)
		}
	}
}
