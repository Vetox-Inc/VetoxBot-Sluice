package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
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

func TestSharedScopeRateLimitRetriesOnlyItsRequest(t *testing.T) {
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
	started := time.Now()
	response := serve(proxy, http.MethodPut, "/api/v10/channels/1/messages/2/reactions/x/@me", "", nil)
	if elapsed := time.Since(started); response.Code != http.StatusNoContent || calls.Load() != 2 || elapsed < 50*time.Millisecond || elapsed > 900*time.Millisecond {
		t.Fatalf("status %d after %d calls in %v, want 204 after one retry in about 50ms", response.Code, calls.Load(), elapsed)
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
