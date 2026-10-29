package proxy

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestSharedScopeRateLimitLeavesItsBucketOpen(t *testing.T) {
	// Discord's documented shared-scope example: the resource is limited for 1337 seconds while
	// its bucket still has 9 of 10 requests left.
	var calls atomic.Int64
	proxy := newTestProxy(t, testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			return testResponse(request, http.StatusTooManyRequests, testHeaders(map[string]string{
				"Via": "1.1 google", "Retry-After": "1337", "X-RateLimit-Limit": "10", "X-RateLimit-Remaining": "9",
				"X-RateLimit-Reset-After": "64.57", "X-RateLimit-Bucket": "abcd1234", "X-RateLimit-Scope": "shared",
			}), io.NopCloser(strings.NewReader(`{"message": "The resource is being rate limited.", "retry_after": 1337, "global": false}`))), nil
		}
		return testResponse(request, http.StatusNoContent, discordVia.Clone(), nil), nil
	})))
	reaction := func(message string) string {
		return "/api/v10/channels/123456789012345678/messages/" + message + "/reactions/%F0%9F%8E%89/@me"
	}

	limited := serve(proxy, http.MethodPut, reaction("223456789012345678"), "", nil)
	if limited.Code != http.StatusTooManyRequests || limited.Header().Get("Retry-After") != "1337" || !strings.Contains(limited.Body.String(), "resource") {
		t.Fatalf("shared 429 reached the client as %d %v %q", limited.Code, limited.Header(), limited.Body.String())
	}
	started := time.Now()
	if other := serve(proxy, http.MethodPut, reaction("323456789012345678"), "", nil); other.Code != http.StatusNoContent || time.Since(started) > 500*time.Millisecond {
		t.Fatalf("another message's reaction got %d after %v; the shared limit closed the whole bucket", other.Code, time.Since(started))
	}
	if calls.Load() != 2 {
		t.Fatalf("Discord was called %d times, want 2", calls.Load())
	}
	if count := proxy.invalidRequests.count(time.Now()); count != 0 {
		t.Fatalf("a shared-scope 429 used %d invalid-request slots", count)
	}
}

func TestSharedScopeRateLimitReachesItsClientUnretried(t *testing.T) {
	// Even a cooldown that fits the queue deadline is the client's to wait out: retried here, the
	// request would add to the shared 429s Discord's edge sees while the client has no say.
	var calls atomic.Int64
	config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			return testResponse(request, http.StatusTooManyRequests, testHeaders(map[string]string{
				"Via": "1.1 google", "Retry-After": "1", "X-RateLimit-Remaining": "4", "X-RateLimit-Scope": "shared",
			}), io.NopCloser(strings.NewReader(`{"retry_after": 0.05, "global": false}`))), nil
		}
		return testResponse(request, http.StatusNoContent, discordVia.Clone(), nil), nil
	}))
	config.QueueTimeout = 3 * config.UpstreamTimeout
	proxy := newTestProxy(t, config)
	before := testutil.ToFloat64(ProxyFailures.WithLabelValues("shared_rate_limit"))
	path := "/api/v10/channels/1/messages/2/reactions/x/@me"

	limited := serve(proxy, http.MethodPut, path, "", nil)
	if limited.Code != http.StatusTooManyRequests || calls.Load() != 1 || limited.Header().Get("Retry-After") != "1" ||
		limited.Header().Get("X-RateLimit-Scope") != "shared" || limited.Header().Get("Generated-By-Proxy") != "" ||
		limited.Body.String() != `{"retry_after": 0.05, "global": false}` {
		t.Fatalf("got %d %v %q after %d calls, want Discord's own shared 429 after one", limited.Code, limited.Header(), limited.Body.String(), calls.Load())
	}
	if got := testutil.ToFloat64(ProxyFailures.WithLabelValues("shared_rate_limit")) - before; got != 1 {
		t.Fatalf("shared_rate_limit counted %v times, want 1", got)
	}
	if retried := serve(proxy, http.MethodPut, path, "", nil); retried.Code != http.StatusNoContent || calls.Load() != 2 {
		t.Fatalf("the client's own retry got %d after %d calls; the shared 429 closed the bucket", retried.Code, calls.Load())
	}
}

// interactionToken builds a token in the shape Discord gives an interaction: base64 of
// "interaction:<id>:<secret>", which is how Sluice tells a follow-up from a webhook call.
func interactionToken(id string) string {
	return strings.TrimRight(base64.StdEncoding.EncodeToString([]byte("interaction:"+id+":"+strings.Repeat("s", 64))), "=")
}

func followUpPath(application string, interaction int) string {
	return "/api/v10/webhooks/" + application + "/" + interactionToken(strconv.Itoa(1103049420930568322+interaction))
}

func sharedRateLimitResponse(request *http.Request) *http.Response {
	return testResponse(request, http.StatusTooManyRequests, testHeaders(map[string]string{
		"Via": "1.1 google", "Retry-After": "1", "X-RateLimit-Scope": "shared",
	}), io.NopCloser(strings.NewReader(`{"message": "The resource is being rate limited.", "retry_after": 0.5, "global": false}`)))
}

func TestSharedRateLimitStormClosesItsResource(t *testing.T) {
	var output bytes.Buffer
	previous := logger
	SetLogger(NewLogger(&output, slog.LevelInfo, "text"))
	t.Cleanup(func() { logger = previous })
	const application, otherApplication = "203039963636301824", "303039963636301824"
	var calls, otherCalls, channelCalls atomic.Int64
	config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch {
		case strings.Contains(request.URL.Path, "/webhooks/"+application+"/"):
			calls.Add(1)
			return sharedRateLimitResponse(request), nil
		case strings.Contains(request.URL.Path, "/webhooks/"+otherApplication+"/"):
			otherCalls.Add(1)
		default:
			channelCalls.Add(1)
		}
		return testResponse(request, http.StatusNoContent, discordVia.Clone(), nil), nil
	}))
	// The storm is faster than the refusal guard allows; this test is about the resource alone.
	config.MaxRefusalsPerSecond = 1000
	proxy := newTestProxy(t, config)
	revision := proxy.stateRevision()

	for interaction := range sharedLimitThreshold {
		if response := serve(proxy, http.MethodPost, followUpPath(application, interaction), "", nil); response.Code != http.StatusTooManyRequests ||
			response.Header().Get("Generated-By-Proxy") != "" {
			t.Fatalf("follow-up %d got %d %v, want Discord's shared 429", interaction, response.Code, response.Header())
		}
	}
	refused := testutil.ToFloat64(ProxyFailures.WithLabelValues("shared_rate_limit_breaker"))
	for interaction := sharedLimitThreshold; interaction < sharedLimitThreshold+50; interaction++ {
		response := serve(proxy, http.MethodPost, followUpPath(application, interaction), "", nil)
		retryAfter, _ := strconv.Atoi(response.Header().Get("Retry-After"))
		if response.Code != http.StatusTooManyRequests || response.Header().Get("Generated-By-Proxy") != "true" ||
			response.Header().Get("Via") != sluiceVia || response.Header().Get("X-RateLimit-Scope") != "shared" ||
			response.Header().Get("X-RateLimit-Remaining") != "" || retryAfter < 9 || retryAfter > int(minSharedLimitCooldown/time.Second) ||
			!strings.Contains(response.Body.String(), `"retry_after": `) {
			t.Fatalf("follow-up %d after the storm got %d %v %q, want a local shared 429", interaction, response.Code, response.Header(), response.Body.String())
		}
	}
	if calls.Load() != sharedLimitThreshold {
		t.Fatalf("Discord was called %d times for the storming application, want %d", calls.Load(), sharedLimitThreshold)
	}
	if got := testutil.ToFloat64(ProxyFailures.WithLabelValues("shared_rate_limit_breaker")) - refused; got != 50 {
		t.Fatalf("shared_rate_limit_breaker counted %v refusals, want 50", got)
	}
	if warnings := strings.Count(output.String(), "answering its requests locally"); warnings != 1 || strings.Contains(output.String(), "aW50") {
		t.Fatalf("logged %d closure warnings, want 1 and no token:\n%s", warnings, output.String())
	}

	if response := serve(proxy, http.MethodPost, followUpPath(otherApplication, 0), "", nil); response.Code != http.StatusNoContent || otherCalls.Load() != 1 {
		t.Fatalf("another application's follow-up got %d after %d calls; the closure spread", response.Code, otherCalls.Load())
	}
	if response := serve(proxy, http.MethodPost, "/api/v10/channels/1/messages", "", nil); response.Code != http.StatusNoContent || channelCalls.Load() != 1 {
		t.Fatalf("a channel message got %d after %d calls; the closure spread", response.Code, channelCalls.Load())
	}
	if proxy.stateRevision() != revision {
		t.Fatal("a shared-limit closure reached STATE_FILE; it must stay in memory")
	}
}

func TestSharedRateLimitStormReachesDiscordBoundedTimes(t *testing.T) {
	var calls atomic.Int64
	config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls.Add(1)
		time.Sleep(time.Millisecond)
		return sharedRateLimitResponse(request), nil
	}))
	config.MaxInFlightRequests = 256
	proxy := newTestProxy(t, config)
	// Clients that retry at once, as if each 429 were a new request: without the guard each of
	// these requests reaches Discord.
	const clients, requestsEach = 16, 50
	var wg sync.WaitGroup
	for client := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for request := range requestsEach {
				serve(proxy, http.MethodPost, followUpPath("203039963636301824", client*requestsEach+request), "", nil)
			}
		}()
	}
	wg.Wait()
	// Requests already in flight when the resource closed may still reach Discord, at most one per client.
	t.Logf("Discord was called %d times for %d requests", calls.Load(), clients*requestsEach)
	if got := calls.Load(); got > sharedLimitThreshold+clients {
		t.Fatalf("Discord was called %d times for %d requests, want at most %d", got, clients*requestsEach, sharedLimitThreshold+clients)
	}
}

func TestSharedLimitGuardClosesOnlyForABurst(t *testing.T) {
	guard := newSharedLimitGuard()
	const key = "POST /webhooks/!/! webhooks:1"
	start := time.Now()

	// Spread wider than the window, refusals never close the key.
	spacing := sharedLimitWindow/(sharedLimitThreshold-1) + time.Millisecond
	for refusal := range 3 * sharedLimitThreshold {
		if _, closed := guard.record(key, time.Second, start.Add(time.Duration(refusal)*spacing)); closed {
			t.Fatalf("refusal %d closed the key although no %d fell within %v", refusal, sharedLimitThreshold, sharedLimitWindow)
		}
	}

	now := start.Add(time.Hour)
	for refusal := 1; refusal < sharedLimitThreshold; refusal++ {
		if _, closed := guard.record(key, time.Second, now); closed {
			t.Fatalf("refusal %d of a burst closed the key early", refusal)
		}
	}
	closure, closed := guard.record(key, time.Second, now)
	if !closed || closure != minSharedLimitCooldown || guard.retryAfter(key, now) != minSharedLimitCooldown {
		t.Fatalf("the burst's last refusal closed %v for %v, want %v", closed, closure, minSharedLimitCooldown)
	}
	if _, closed := guard.record(key, time.Hour, now.Add(time.Second)); closed || guard.retryAfter(key, now) != minSharedLimitCooldown {
		t.Fatal("a refusal of a request in flight extended the closure")
	}
	if delay := guard.retryAfter(key, now.Add(minSharedLimitCooldown)); delay != 0 {
		t.Fatalf("the key was still closed for %v after its cooldown", delay)
	}

	// Its closure over, the key lets one trial through, and refusals of other requests prove
	// nothing until the trial does.
	now = now.Add(minSharedLimitCooldown)
	if delay, trial := guard.admit(key, now); delay != 0 || !trial {
		t.Fatalf("the first request after the closure got %v, trial %v; want the trial", delay, trial)
	}
	if delay, trial := guard.admit(key, now); delay != sharedTrialRetryDelay || trial {
		t.Fatalf("a second request during the trial got %v, trial %v; want %v", delay, trial, sharedTrialRetryDelay)
	}
	for refusal := range 2 * sharedLimitThreshold {
		if _, closed := guard.record(key, time.Second, now); closed {
			t.Fatalf("refusal %d during the trial closed the key", refusal)
		}
	}
	if _, closed := guard.settleTrial(key, now, &http.Response{StatusCode: http.StatusNoContent}, rateLimitInfo{}); closed || guard.retryAfter(key, now) != 0 {
		t.Fatal("a successful trial did not open the key")
	}

	// Reopened, the key needs a whole new burst, and a long cooldown is capped.
	for refusal := 1; refusal < sharedLimitThreshold; refusal++ {
		if _, closed := guard.record(key, 10*time.Minute, now); closed {
			t.Fatalf("refusal %d after reopening closed the key early", refusal)
		}
	}
	if closure, closed := guard.record(key, 10*time.Minute, now); !closed || closure != maxSharedLimitCooldown {
		t.Fatalf("a 10-minute cooldown closed %v for %v, want %v", closed, closure, maxSharedLimitCooldown)
	}

	if _, closed := guard.record("", time.Second, now); closed || guard.retryAfter("", now) != 0 {
		t.Fatal("a request without a key was tracked")
	}
}

func TestSharedLimitGuardForgetsTheOldestKeys(t *testing.T) {
	guard := newSharedLimitGuard()
	now := time.Now()
	for key := range maxSharedLimitKeys + 10 {
		guard.record(strconv.Itoa(key), time.Second, now)
	}
	if len(guard.entries) != maxSharedLimitKeys || guard.order.Len() != maxSharedLimitKeys {
		t.Fatalf("guard holds %d keys and %d list entries, want %d", len(guard.entries), guard.order.Len(), maxSharedLimitKeys)
	}
	if _, ok := guard.entries["0"]; ok {
		t.Fatal("the oldest key survived eviction")
	}
}

func TestRateLimitCooldownUsesDiscordsPreciseRetryAfter(t *testing.T) {
	for _, test := range []struct {
		name, body string
		want       time.Duration
	}{
		{"seconds body refines the header", `{"retry_after": 1.25, "global": false}`, 1250 * time.Millisecond},
		{"v6 milliseconds body is ignored", `{"retry_after": 1250, "global": false}`, 2 * time.Second},
		{"no body", ``, 2 * time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
				return testResponse(request, http.StatusTooManyRequests, testHeaders(map[string]string{
					"Via": "1.1 google", "Retry-After": "2", "X-RateLimit-Remaining": "0", "X-RateLimit-Scope": "user",
				}), io.NopCloser(strings.NewReader(test.body))), nil
			}))
			config.QueueTimeout = 100 * time.Millisecond
			config.UpstreamTimeout = 20 * time.Millisecond
			proxy := newTestProxy(t, config)
			path := "/api/v10/channels/1/messages"
			response, err := (&scheduledTransport{base: proxy.transport, proxy: proxy}).RoundTrip(
				scheduledRequest(t, context.Background(), proxy.noAuth, http.MethodGet, path, nil, true),
			)
			if err != nil {
				t.Fatal(err)
			}
			if body, _ := io.ReadAll(response.Body); string(body) != test.body {
				t.Fatalf("client received %q, want Discord's body %q", body, test.body)
			}
			_ = response.Body.Close()
			bucket := mustBucket(t, proxy.noAuth, routeHash(http.MethodGet, GetOptimisticBucketPath(path, http.MethodGet), majorParameter(path)))
			if got := bucket.retryAfter(time.Now()); got > test.want+rateLimitHeaderSlack || got < test.want-100*time.Millisecond {
				t.Fatalf("bucket cooldown = %v, want about %v", got, test.want)
			}
		})
	}
}

func TestSyntheticRateLimitsCarryDiscordsHeaders(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/api/v10/gateway", nil)
	bucket := rememberedRateLimitResponse(request, 1500*time.Millisecond, false)
	body, _ := io.ReadAll(bucket.Body)
	reset, err := strconv.ParseFloat(bucket.Header.Get("X-RateLimit-Reset"), 64)
	if err != nil || bucket.Header.Get("Retry-After") != "2" || bucket.Header.Get("X-RateLimit-Remaining") != "0" ||
		bucket.Header.Get("X-RateLimit-Reset-After") != "1.500" || bucket.Header.Get("X-RateLimit-Scope") != "user" ||
		math.Abs(reset-float64(time.Now().Add(1500*time.Millisecond).UnixMilli())/1000) > 1 ||
		string(body) != `{"message":"You are being rate limited.","retry_after":1.500,"global":false}` {
		t.Fatalf("bucket 429 = %v %s", bucket.Header, body)
	}
	global := rememberedRateLimitResponse(request, 300*time.Millisecond, true)
	if global.Header.Get("X-RateLimit-Global") != "true" || global.Header.Get("X-RateLimit-Scope") != "global" ||
		global.Header.Get("Retry-After") != "1" || global.Header.Get("X-RateLimit-Remaining") != "" {
		t.Fatalf("global 429 headers = %v", global.Header)
	}
}

// slowReader yields remaining bytes a chunk at a time, pausing before each chunk.
type slowReader struct {
	remaining int
	chunk     int
	pause     time.Duration
}

func (r *slowReader) Read(target []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	time.Sleep(r.pause)
	size := min(len(target), r.chunk, r.remaining)
	for index := range size {
		target[index] = 'x'
	}
	r.remaining -= size
	return size, nil
}

func (*slowReader) Close() error { return nil }

func TestUploadThatKeepsMovingOutlastsTheRequestTimeout(t *testing.T) {
	wrappers := map[string]func(*Proxy) http.RoundTripper{
		"discord": func(proxy *Proxy) http.RoundTripper {
			return &scheduledTransport{base: proxy.transport, proxy: proxy}
		},
		"cluster peer": func(proxy *Proxy) http.RoundTripper {
			return &timeoutTransport{base: proxy.transport, timeout: 100 * time.Millisecond}
		},
	}
	for name, wrap := range wrappers {
		t.Run(name, func(t *testing.T) {
			var received atomic.Int64
			config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
				read, err := io.Copy(io.Discard, request.Body)
				if err != nil {
					return nil, err
				}
				received.Store(read)
				return testResponse(request, http.StatusOK, discordVia.Clone(), nil), nil
			}))
			config.UpstreamTimeout = 100 * time.Millisecond
			proxy := newTestProxy(t, config)
			// 1 MiB at 64 KiB per 30 ms takes about half a second: five request timeouts.
			request := scheduledRequest(t, context.Background(), proxy.noAuth, http.MethodPost, "/api/v10/channels/1/messages", nil, true)
			request.Body = &slowReader{remaining: 1 << 20, chunk: 64 << 10, pause: 30 * time.Millisecond}
			request.ContentLength = 1 << 20
			response, err := wrap(proxy).RoundTrip(request)
			if err != nil {
				t.Fatalf("moving upload failed: %v", err)
			}
			_ = response.Body.Close()
			if received.Load() != 1<<20 {
				t.Fatalf("Discord received %d bytes, want %d", received.Load(), 1<<20)
			}
		})
	}
}

func TestStalledUploadTimesOut(t *testing.T) {
	config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		read := make(chan error, 1)
		go func() {
			_, err := io.Copy(io.Discard, request.Body)
			read <- err
		}()
		select {
		case err := <-read:
			return nil, fmt.Errorf("stalled body ended: %v", err)
		case <-request.Context().Done():
			return nil, request.Context().Err()
		}
	}))
	config.UpstreamTimeout = 50 * time.Millisecond
	config.QueueTimeout = 5 * time.Second
	proxy := newTestProxy(t, config)
	body, writer := io.Pipe()
	defer writer.Close()
	go func() { _, _ = writer.Write(bytes.Repeat([]byte("x"), 1024)) }()
	request := scheduledRequest(t, context.Background(), proxy.noAuth, http.MethodPost, "/api/v10/channels/1/messages", nil, true)
	request.Body, request.ContentLength = body, -1
	started := time.Now()
	_, err := (&scheduledTransport{base: proxy.transport, proxy: proxy}).RoundTrip(request)
	if !errors.Is(err, errUpstreamDeadline) || time.Since(started) > time.Second {
		t.Fatalf("stalled upload ended with %v after %v, want %v after about 50ms", err, time.Since(started), errUpstreamDeadline)
	}
}

func TestQueueDeadlineDoesNotCutAStartedAttempt(t *testing.T) {
	config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		select {
		case <-time.After(150 * time.Millisecond):
			return testResponse(request, http.StatusOK, discordVia.Clone(), nil), nil
		case <-request.Context().Done():
			return nil, request.Context().Err()
		}
	}))
	config.QueueTimeout = 50 * time.Millisecond
	config.UpstreamTimeout = time.Second
	proxy := newTestProxy(t, config)
	response, err := (&scheduledTransport{base: proxy.transport, proxy: proxy}).RoundTrip(
		scheduledRequest(t, context.Background(), proxy.noAuth, http.MethodGet, "/api/v10/gateway", nil, true),
	)
	if err != nil {
		t.Fatalf("an attempt already sent to Discord was cut by the queue deadline: %v", err)
	}
	_ = response.Body.Close()
}

func TestCooldownShorterThanTheQueueDeadlineIsWaitedOut(t *testing.T) {
	// REQUEST_TIMEOUT longer than QUEUE_TIMEOUT once disabled every wait: attempts no longer
	// inherit the queue deadline, so none of it has to be kept for them.
	var calls atomic.Int64
	config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			return testResponse(request, http.StatusTooManyRequests, testHeaders(map[string]string{
				"Via": "1.1 google", "Retry-After": "1", "X-RateLimit-Remaining": "0", "X-RateLimit-Scope": "user",
			}), io.NopCloser(strings.NewReader(`{"retry_after": 0.2, "global": false}`))), nil
		}
		return testResponse(request, http.StatusOK, discordVia.Clone(), nil), nil
	}))
	config.QueueTimeout = 500 * time.Millisecond
	config.UpstreamTimeout = time.Second
	proxy := newTestProxy(t, config)
	if response := serve(proxy, http.MethodGet, "/api/v10/channels/1/messages", "", nil); response.Code != http.StatusOK || calls.Load() != 2 {
		t.Fatalf("a 200ms cooldown within a 500ms queue deadline answered %d after %d calls, want 200 after 2", response.Code, calls.Load())
	}
}

func TestAttemptCeilingEndsAnExchangeThatKeepsProgressing(t *testing.T) {
	cause := errors.New("ceiling")
	ctx, stall, end := newStallTimer(context.Background(), time.Hour, 100*time.Millisecond, cause)
	defer end()
	started := time.Now()
	for ctx.Err() == nil && time.Since(started) < time.Second {
		stall.progress()
		time.Sleep(10 * time.Millisecond)
	}
	if !errors.Is(context.Cause(ctx), cause) || time.Since(started) > 500*time.Millisecond {
		t.Fatalf("progressing exchange ended with %v after %v, want the ceiling after about 100ms", context.Cause(ctx), time.Since(started))
	}
}

func TestBotWideRoutesShareOneBucketAcrossGuilds(t *testing.T) {
	for _, botWide := range []bool{false, true} {
		var calls atomic.Int64
		config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
			calls.Add(1)
			return testResponse(request, http.StatusTooManyRequests, testHeaders(map[string]string{
				"Via": "1.1 google", "Retry-After": "30", "X-RateLimit-Remaining": "0", "X-RateLimit-Scope": "user",
			}), nil), nil
		}))
		if botWide {
			config.BotWideRoutes = "GET /guilds/!/vanity-url, PATCH /guilds/123/mfa"
		}
		proxy := newTestProxy(t, config)
		for _, guild := range []string{"111111111111111111", "222222222222222222"} {
			if response := serve(proxy, http.MethodGet, "/api/v10/guilds/"+guild+"/vanity-url", "Bot "+fakeBotToken, nil); response.Code != http.StatusTooManyRequests {
				t.Fatalf("botWide=%v: guild %s got %d", botWide, guild, response.Code)
			}
		}
		if want := map[bool]int64{false: 2, true: 1}[botWide]; calls.Load() != want {
			t.Fatalf("botWide=%v: Discord was called %d times across two guilds, want %d", botWide, calls.Load(), want)
		}
	}
	if _, err := parseBotWideRoutes("/guilds/!/vanity-url"); err == nil {
		t.Fatal("a route without a method was accepted")
	}
}
