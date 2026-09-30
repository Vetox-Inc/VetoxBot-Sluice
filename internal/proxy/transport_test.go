package proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

func TestBucketReleasesAfterUpstreamHeaders(t *testing.T) {
	blockedBody := newBlockingBody()
	var calls atomic.Int64
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			return testResponse(request, http.StatusOK, nil, blockedBody), nil
		}
		return testResponse(request, http.StatusOK, nil, nil), nil
	})
	proxy := newTestProxy(t, testConfig(transport))
	scheduled := &scheduledTransport{base: proxy.transport, proxy: proxy}
	path := "/api/v10/channels/123/messages"

	first, err := scheduled.RoundTrip(scheduledRequest(t, context.Background(), proxy.noAuth, http.MethodGet, path, nil, true))
	if err != nil {
		t.Fatal(err)
	}
	defer first.Body.Close()

	bucketPath := GetOptimisticBucketPath(path, http.MethodGet)
	bucket := mustBucket(t, proxy.noAuth, routeHash(http.MethodGet, bucketPath, majorParameter(path)))
	if active := bucket.active.Load(); active != 0 {
		t.Fatalf("bucket remained active after response headers: %d", active)
	}
	select {
	case <-blockedBody.release:
		t.Fatal("first response body was closed before the caller closed it")
	default:
	}

	second, err := scheduled.RoundTrip(scheduledRequest(t, context.Background(), proxy.noAuth, http.MethodGet, path, nil, true))
	if err != nil {
		t.Fatalf("second request remained coupled to first response body: %v", err)
	}
	_ = second.Body.Close()
	if got := calls.Load(); got != 2 {
		t.Fatalf("outbound calls = %d, want 2", got)
	}
}

func TestOutboundErrorReleasesBucket(t *testing.T) {
	sentinel := errors.New("upstream failed")
	var calls atomic.Int64
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			return nil, sentinel
		}
		return testResponse(request, http.StatusOK, nil, nil), nil
	})
	proxy := newTestProxy(t, testConfig(transport))
	scheduled := &scheduledTransport{base: proxy.transport, proxy: proxy}
	path := "/api/v10/channels/123/messages"

	_, err := scheduled.RoundTrip(scheduledRequest(t, context.Background(), proxy.noAuth, http.MethodGet, path, nil, true))
	if !errors.Is(err, sentinel) {
		t.Fatalf("first request error = %v, want %v", err, sentinel)
	}
	bucket := mustBucket(t, proxy.noAuth, routeHash(http.MethodGet, GetOptimisticBucketPath(path, http.MethodGet), majorParameter(path)))
	if active := bucket.active.Load(); active != 0 {
		t.Fatalf("failed request leaked bucket activity: %d", active)
	}

	response, err := scheduled.RoundTrip(scheduledRequest(t, context.Background(), proxy.noAuth, http.MethodGet, path, nil, true))
	if err != nil {
		t.Fatalf("request after upstream failure: %v", err)
	}
	_ = response.Body.Close()
	if got := calls.Load(); got != 2 || bucket.active.Load() != 0 {
		t.Fatalf("outbound calls=%d active=%d, want calls=2 active=0", got, bucket.active.Load())
	}
}

func TestQueueTimeoutAndCapacity(t *testing.T) {
	t.Run("timeout", func(t *testing.T) {
		var calls atomic.Int64
		transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
			calls.Add(1)
			return testResponse(request, http.StatusOK, nil, nil), nil
		})
		config := testConfig(transport)
		config.QueueTimeout = 20 * time.Millisecond
		config.MaxQueueDepth = 2
		proxy := newTestProxy(t, config)
		scheduled := &scheduledTransport{base: proxy.transport, proxy: proxy}
		path := "/api/v10/channels/123/messages"
		bucket := mustBucket(t, proxy.noAuth, routeHash(http.MethodGet, GetOptimisticBucketPath(path, http.MethodGet), majorParameter(path)))
		if err := bucket.acquire(context.Background()); err != nil {
			t.Fatal(err)
		}

		_, err := scheduled.RoundTrip(scheduledRequest(t, context.Background(), proxy.noAuth, http.MethodGet, path, nil, true))
		if !errors.Is(err, errQueueDeadline) {
			t.Fatalf("queued request error = %v, want %v", err, errQueueDeadline)
		}
		bucket.release()
		if calls.Load() != 0 || bucket.active.Load() != 0 {
			t.Fatalf("timed-out request leaked through: calls=%d active=%d", calls.Load(), bucket.active.Load())
		}
	})

	t.Run("capacity", func(t *testing.T) {
		var calls atomic.Int64
		transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
			calls.Add(1)
			return testResponse(request, http.StatusOK, nil, nil), nil
		})
		config := testConfig(transport)
		config.QueueTimeout = time.Second
		config.MaxQueueDepth = 1
		proxy := newTestProxy(t, config)
		scheduled := &scheduledTransport{base: proxy.transport, proxy: proxy}
		path := "/api/v10/channels/123/messages"
		bucket := mustBucket(t, proxy.noAuth, routeHash(http.MethodGet, GetOptimisticBucketPath(path, http.MethodGet), majorParameter(path)))
		if err := bucket.acquire(context.Background()); err != nil {
			t.Fatal(err)
		}
		holderReleased := false
		defer func() {
			if !holderReleased {
				bucket.release()
			}
		}()

		firstContext, cancelFirst := context.WithCancel(context.Background())
		defer cancelFirst()
		firstRequest := scheduledRequest(t, firstContext, proxy.noAuth, http.MethodGet, path, nil, true)
		firstResult := make(chan error, 1)
		go func() {
			_, err := scheduled.RoundTrip(firstRequest)
			firstResult <- err
		}()
		waitForGateWaiters(t, &bucket.gate, 1)

		_, err := scheduled.RoundTrip(scheduledRequest(t, context.Background(), proxy.noAuth, http.MethodGet, path, nil, true))
		if !errors.Is(err, errQueueFull) {
			t.Fatalf("over-capacity request error = %v, want errQueueFull", err)
		}
		cancelFirst()
		if err := <-firstResult; !errors.Is(err, context.Canceled) {
			t.Fatalf("first queued request error = %v, want context.Canceled", err)
		}
		bucket.release()
		holderReleased = true
		if calls.Load() != 0 || bucket.active.Load() != 0 {
			t.Fatalf("capacity test leaked through: calls=%d active=%d", calls.Load(), bucket.active.Load())
		}
	})
}

func TestUpstreamTimeoutIsDistinctFromQueueTimeout(t *testing.T) {
	config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		<-request.Context().Done()
		return nil, request.Context().Err()
	}))
	config.QueueTimeout = time.Second
	config.UpstreamTimeout = 20 * time.Millisecond
	proxy := newTestProxy(t, config)

	_, err := (&scheduledTransport{base: proxy.transport, proxy: proxy}).RoundTrip(
		scheduledRequest(t, context.Background(), proxy.noAuth, http.MethodGet, "/api/v10/gateway", nil, false),
	)
	if !errors.Is(err, errUpstreamDeadline) || errors.Is(err, errQueueDeadline) {
		t.Fatalf("upstream error = %v, want only %v", err, errUpstreamDeadline)
	}
}

func TestExhaustedInvalidRequestBudgetFailsBeforeSchedulerWait(t *testing.T) {
	var calls atomic.Int64
	config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls.Add(1)
		return testResponse(request, http.StatusOK, nil, nil), nil
	}))
	config.InvalidRequestLimit = 1
	config.QueueTimeout = 2 * time.Second
	proxy := newTestProxy(t, config)
	now := time.Now()
	if !proxy.invalidRequests.reserve(now) {
		t.Fatal("failed to reserve invalid-request test budget")
	}
	proxy.invalidRequests.complete(now, true)
	proxy.noAuth.global.blockFor(time.Hour)

	started := time.Now()
	_, err := (&scheduledTransport{base: proxy.transport, proxy: proxy}).RoundTrip(
		scheduledRequest(t, context.Background(), proxy.noAuth, http.MethodGet, "/api/v10/gateway", nil, false),
	)
	if !errors.Is(err, errInvalidRequestBudget) {
		t.Fatalf("request error = %v, want %v", err, errInvalidRequestBudget)
	}
	if elapsed := time.Since(started); elapsed >= config.QueueTimeout/2 {
		t.Fatalf("exhausted budget waited in scheduler for %s", elapsed)
	}
	if calls.Load() != 0 {
		t.Fatalf("exhausted budget made %d outbound calls", calls.Load())
	}
}

func TestRateLimitResponseIsRetriedWithPOSTBody(t *testing.T) {
	const payload = `{"content":"hello"}`
	var bodies []string
	var calls int
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
		bodies = append(bodies, string(body))
		calls++
		if calls == 1 {
			return testResponse(request, http.StatusTooManyRequests, http.Header{
				"Retry-After":           {"0.001"},
				"X-Ratelimit-Remaining": {"0"},
				"X-Ratelimit-Scope":     {"user"},
			}, io.NopCloser(strings.NewReader("limited"))), nil
		}
		return testResponse(request, http.StatusCreated, http.Header{
			"X-Ratelimit-Remaining": {"1"},
		}, io.NopCloser(strings.NewReader("created"))), nil
	})
	config := testConfig(transport)
	config.QueueTimeout = 2 * config.UpstreamTimeout
	proxy := newTestProxy(t, config)
	scheduled := &scheduledTransport{base: proxy.transport, proxy: proxy}
	request := scheduledRequest(t, context.Background(), proxy.noAuth, http.MethodPost, "/api/v10/channels/123/messages", strings.NewReader(payload), true)

	response, err := scheduled.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("final status = %d, want %d", response.StatusCode, http.StatusCreated)
	}
	if calls != 2 || len(bodies) != 2 || bodies[0] != payload || bodies[1] != payload {
		t.Fatalf("retry calls=%d bodies=%q, want two copies of %q", calls, bodies, payload)
	}
}

func TestRateLimitResponseIsRetriedWithoutBody(t *testing.T) {
	var calls atomic.Int64
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Body != nil {
			t.Fatal("bodyless retry gained a request body")
		}
		if calls.Add(1) == 1 {
			return testResponse(request, http.StatusTooManyRequests, http.Header{
				"Retry-After":           {"0.001"},
				"X-Ratelimit-Remaining": {"0"},
			}, nil), nil
		}
		return testResponse(request, http.StatusOK, nil, nil), nil
	})
	config := testConfig(transport)
	config.QueueTimeout = 2 * config.UpstreamTimeout
	proxy := newTestProxy(t, config)
	scheduled := &scheduledTransport{base: proxy.transport, proxy: proxy}
	request := scheduledRequest(t, context.Background(), proxy.noAuth, http.MethodGet, "/api/v10/gateway", nil, true)
	response, err := scheduled.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK || calls.Load() != 2 {
		t.Fatalf("final status=%d calls=%d, want 200 and two attempts", response.StatusCode, calls.Load())
	}
}

func TestRateLimitWaitThatCannotFitReturns429Immediately(t *testing.T) {
	t.Run("current Discord response", func(t *testing.T) {
		var calls atomic.Int64
		config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
			calls.Add(1)
			return testResponse(request, http.StatusTooManyRequests, testHeaders(map[string]string{
				"Retry-After":           "1",
				"X-RateLimit-Remaining": "0",
			}), nil), nil
		}))
		config.QueueTimeout = 100 * time.Millisecond
		config.UpstreamTimeout = 20 * time.Millisecond
		proxy := newTestProxy(t, config)
		response, err := (&scheduledTransport{base: proxy.transport, proxy: proxy}).RoundTrip(
			scheduledRequest(t, context.Background(), proxy.noAuth, http.MethodPost, "/api/v10/webhooks/123/token", nil, true),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusTooManyRequests || calls.Load() != 1 {
			t.Fatalf("status=%d calls=%d, want 429/1", response.StatusCode, calls.Load())
		}
	})

	t.Run("remembered bucket response", func(t *testing.T) {
		var calls atomic.Int64
		config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
			calls.Add(1)
			return testResponse(request, http.StatusNoContent, nil, nil), nil
		}))
		config.QueueTimeout = 100 * time.Millisecond
		config.UpstreamTimeout = 20 * time.Millisecond
		proxy := newTestProxy(t, config)
		path := "/api/v10/webhooks/123/token"
		bucket := mustBucket(t, proxy.noAuth, routeHash(http.MethodPost, GetOptimisticBucketPath(path, http.MethodPost), majorParameter(path)))
		bucket.blockUntil(time.Now().Add(time.Second))
		response, err := (&scheduledTransport{base: proxy.transport, proxy: proxy}).RoundTrip(
			scheduledRequest(t, context.Background(), proxy.noAuth, http.MethodPost, path, nil, true),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusTooManyRequests || response.Header.Get("Retry-After") == "" || calls.Load() != 0 {
			t.Fatalf("status=%d retry-after=%q calls=%d, want local 429/positive/0", response.StatusCode, response.Header.Get("Retry-After"), calls.Load())
		}
	})

	t.Run("longer global response wins", func(t *testing.T) {
		var calls atomic.Int64
		config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
			calls.Add(1)
			return testResponse(request, http.StatusNoContent, nil, nil), nil
		}))
		config.QueueTimeout = 100 * time.Millisecond
		config.UpstreamTimeout = 20 * time.Millisecond
		proxy := newTestProxy(t, config)
		path := "/api/v10/gateway"
		bucket := mustBucket(t, proxy.noAuth, routeHash(http.MethodGet, GetOptimisticBucketPath(path, http.MethodGet), majorParameter(path)))
		bucket.blockUntil(time.Now().Add(2 * time.Second))
		proxy.noAuth.global.blockFor(3 * time.Second)
		response, err := (&scheduledTransport{base: proxy.transport, proxy: proxy}).RoundTrip(
			scheduledRequest(t, context.Background(), proxy.noAuth, http.MethodGet, path, nil, false),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusTooManyRequests || response.Header.Get("X-RateLimit-Global") != "true" || response.Header.Get("Retry-After") != "3" || calls.Load() != 0 {
			t.Fatalf("status=%d global=%q retry-after=%q calls=%d, want 429/true/3/0", response.StatusCode, response.Header.Get("X-RateLimit-Global"), response.Header.Get("Retry-After"), calls.Load())
		}
	})
}

func TestInteractionGlobalRateLimitReturns429BeforeQueueDeadline(t *testing.T) {
	var calls atomic.Int64
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls.Add(1)
		return testResponse(request, http.StatusTooManyRequests, http.Header{
			"Retry-After":           {"1"},
			"X-Ratelimit-Global":    {"true"},
			"X-Ratelimit-Remaining": {"0"},
		}, nil), nil
	})
	config := testConfig(transport)
	config.QueueTimeout = 20 * time.Millisecond
	proxy := newTestProxy(t, config)
	scheduled := &scheduledTransport{base: proxy.transport, proxy: proxy}
	request := scheduledRequest(t, context.Background(), proxy.noAuth, http.MethodPost, "/api/v10/interactions/123/token/callback", nil, true)
	response, err := scheduled.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusTooManyRequests || calls.Load() != 1 {
		t.Fatalf("status=%d calls=%d, want 429/1", response.StatusCode, calls.Load())
	}
}

func TestLargeRetryBodySpillsToDiskAndIsRemoved(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), retryMemoryLimit+1)
	request := httptest.NewRequest(http.MethodPost, "/api/v10/channels/123/messages", bytes.NewReader(payload))
	budget := newResourceBudget(int64(len(payload)))
	replay, err := newBodyReplay(request, int64(len(payload)), budget)
	if err != nil {
		t.Fatal(err)
	}

	first, err := replay.body(0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, first); err != nil {
		t.Fatal(err)
	}
	_ = first.Close()
	spillWriter := replay.capture.file
	if spillWriter == nil {
		t.Fatal("large replay body did not spill to a temporary file")
	}
	if retryable, err := replay.prepareRetry(); err != nil || !retryable {
		t.Fatalf("prepare spilled retry: retryable=%v err=%v", retryable, err)
	}
	if replay.capture.file != nil {
		t.Fatal("spill writer remained open after retry preparation")
	}
	if _, err := spillWriter.Write([]byte("x")); err == nil {
		t.Fatal("sealed spill writer still accepted data")
	}
	if replay.capture.buffer.Cap() != 0 {
		t.Fatal("capture retained its memory buffer after retry preparation")
	}

	second, err := replay.body(1)
	if err != nil {
		t.Fatal(err)
	}
	fileName := replay.fileName
	if fileName == "" {
		t.Fatal("large replay body did not spill to a temporary file")
	}
	got, err := io.ReadAll(second)
	_ = second.Close()
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("replayed body changed: bytes=%d err=%v", len(got), err)
	}

	replay.close()
	if got := budget.used.Load(); got != 0 {
		t.Fatalf("retry budget after cleanup = %d, want 0", got)
	}
	if _, err := os.Stat(fileName); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary replay file still exists: %v", err)
	}
}

type contextAwareBody struct {
	ctx    context.Context
	reader *strings.Reader
}

func (b *contextAwareBody) Read(target []byte) (int, error) {
	select {
	case <-b.ctx.Done():
		return 0, b.ctx.Err()
	default:
		return b.reader.Read(target)
	}
}

func (*contextAwareBody) Close() error { return nil }

func TestReturnedResponseBodyOwnsSchedulerContexts(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusTooManyRequests} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			outboundContext := make(chan context.Context, 1)
			transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
				outboundContext <- request.Context()
				header := make(http.Header)
				if status == http.StatusTooManyRequests {
					header.Set("Retry-After", "0")
				}
				body := &contextAwareBody{ctx: request.Context(), reader: strings.NewReader("streamed")}
				return testResponse(request, status, header, body), nil
			})
			proxy := newTestProxy(t, testConfig(transport))
			scheduled := &scheduledTransport{base: proxy.transport, proxy: proxy}
			var requestBody io.Reader
			if status == http.StatusTooManyRequests {
				// An unread, unknown-length body makes this 429 deliberately non-replayable.
				requestBody = io.NopCloser(strings.NewReader(""))
			}
			request := scheduledRequest(t, context.Background(), proxy.noAuth, http.MethodPost, "/api/v10/channels/123/messages", requestBody, true)

			response, err := scheduled.RoundTrip(request)
			if err != nil {
				t.Fatal(err)
			}
			ctx := <-outboundContext
			select {
			case <-ctx.Done():
				t.Fatalf("response context was canceled at headers: %v", ctx.Err())
			default:
			}
			body, err := io.ReadAll(response.Body)
			if err != nil || string(body) != "streamed" {
				t.Fatalf("response body = %q, err = %v", body, err)
			}
			if err := response.Body.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
				t.Fatal("closing response body did not release scheduler contexts")
			}
		})
	}
}

func TestTransportsDefendAgainstInvalidRoundTripperResults(t *testing.T) {
	t.Run("scheduled nil response", func(t *testing.T) {
		var calls atomic.Int64
		transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
			if calls.Add(1) == 1 {
				return nil, nil
			}
			return testResponse(request, http.StatusOK, nil, nil), nil
		})
		config := testConfig(transport)
		config.InvalidRequestLimit = 1
		proxy := newTestProxy(t, config)
		scheduled := &scheduledTransport{base: proxy.transport, proxy: proxy}
		request := func() *http.Request {
			return scheduledRequest(t, context.Background(), proxy.noAuth, http.MethodGet, "/api/v10/gateway", nil, true)
		}

		if _, err := scheduled.RoundTrip(request()); err == nil {
			t.Fatal("nil upstream response was accepted")
		}
		response, err := scheduled.RoundTrip(request())
		if err != nil {
			t.Fatalf("request after nil response: %v", err)
		}
		_ = response.Body.Close()
		if calls.Load() != 2 {
			t.Fatalf("upstream calls = %d, want 2", calls.Load())
		}
	})

	t.Run("scheduled nil body", func(t *testing.T) {
		transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
			return &http.Response{
				Status:     "204 No Content",
				StatusCode: http.StatusNoContent,
				Header:     make(http.Header),
				Request:    request,
			}, nil
		})
		proxy := newTestProxy(t, testConfig(transport))
		scheduled := &scheduledTransport{base: proxy.transport, proxy: proxy}
		request := scheduledRequest(t, context.Background(), proxy.noAuth, http.MethodGet, "/api/v10/gateway", nil, true)
		response, err := scheduled.RoundTrip(request)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		if err != nil || len(body) != 0 {
			t.Fatalf("nil upstream body read %d bytes with error %v", len(body), err)
		}
		if err := response.Body.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("timeout response with error", func(t *testing.T) {
		body := newBlockingBody()
		sentinel := errors.New("transport failed")
		transport := &timeoutTransport{
			base: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				return testResponse(request, http.StatusBadGateway, nil, body), sentinel
			}),
			timeout: time.Second,
		}
		request := httptest.NewRequest(http.MethodGet, "https://cluster.invalid/", nil)
		if _, err := transport.RoundTrip(request); !errors.Is(err, sentinel) {
			t.Fatalf("timeout transport error = %v, want %v", err, sentinel)
		}
		select {
		case <-body.release:
		default:
			t.Fatal("response body accompanying transport error was not closed")
		}
	})

	t.Run("timeout nil response", func(t *testing.T) {
		transport := &timeoutTransport{
			base:    roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, nil }),
			timeout: time.Second,
		}
		request := httptest.NewRequest(http.MethodGet, "https://cluster.invalid/", nil)
		if _, err := transport.RoundTrip(request); err == nil {
			t.Fatal("nil cluster response was accepted")
		}
	})
}

func TestClusterPeerTimeoutLeavesServerResponseMargin(t *testing.T) {
	config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return testResponse(request, http.StatusOK, nil, nil), nil
	}))
	proxy := newTestProxy(t, config)
	transport, ok := proxy.peerProxy.Transport.(*timeoutTransport)
	if !ok {
		t.Fatalf("peer transport = %T, want *timeoutTransport", proxy.peerProxy.Transport)
	}
	if want := config.QueueTimeout + config.UpstreamTimeout; transport.timeout != want {
		t.Fatalf("peer timeout = %s, want %s", transport.timeout, want)
	}
}

type blockingLogWriter struct{ release <-chan struct{} }

func (w blockingLogWriter) Write(message []byte) (int, error) {
	<-w.release
	return len(message), nil
}

func TestRequestCompletionDoesNotDependOnDebugOutput(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	testLogger := logrus.New()
	testLogger.SetOutput(blockingLogWriter{release: release})
	testLogger.SetLevel(logrus.DebugLevel)
	previousLogger := logger
	SetLogger(testLogger)
	t.Cleanup(func() { logger = previousLogger })

	var calls atomic.Int64
	proxy := newTestProxy(t, testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			return testResponse(request, http.StatusTooManyRequests, testHeaders(map[string]string{
				"Retry-After":           "0",
				"X-RateLimit-Scope":     "shared",
				"X-RateLimit-Remaining": "0",
			}), nil), nil
		}
		return testResponse(request, http.StatusNoContent, nil, nil), nil
	})))
	scheduled := &scheduledTransport{base: proxy.transport, proxy: proxy}
	result := make(chan error, 1)
	go func() {
		response, err := scheduled.RoundTrip(scheduledRequest(t, context.Background(), proxy.noAuth, http.MethodPost, "/api/v10/webhooks/123/token", nil, false))
		if response != nil {
			_ = response.Body.Close()
		}
		result <- err
	}()

	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		unblock()
		t.Fatal("request blocked on debug output")
	}
}

func TestTimeoutResponseDoesNotDependOnLogOutput(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	testLogger := logrus.New()
	testLogger.SetOutput(blockingLogWriter{release: release})
	previousLogger := logger
	SetLogger(testLogger)
	t.Cleanup(func() { logger = previousLogger })

	config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		<-request.Context().Done()
		return nil, request.Context().Err()
	}))
	config.UpstreamTimeout = 20 * time.Millisecond
	proxy := newTestProxy(t, config)
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		proxy.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v10/gateway", nil))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout response blocked on log output")
	}
	if response.Code != http.StatusGatewayTimeout || response.Header().Get("X-Nirn-Proxy-Error") != "true" {
		t.Fatalf("status=%d proxy-error=%q, want 504/true", response.Code, response.Header().Get("X-Nirn-Proxy-Error"))
	}
}

func TestNestedWebhookNotFoundIsNotCachedAsUnknownWebhook(t *testing.T) {
	var calls atomic.Int64
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			return testResponse(request, http.StatusNotFound, nil, nil), nil
		}
		return testResponse(request, http.StatusOK, nil, nil), nil
	})
	proxy := newTestProxy(t, testConfig(transport))
	state := newClientState(identify("Bot webhook-404-token"), 1_000_000, 8, newResourceBudget(256))
	scheduled := &scheduledTransport{base: proxy.transport, proxy: proxy}
	const prefix = "/api/v10/webhooks/123456789012345678/webhook-token/messages/"
	for index, path := range []string{prefix + "223456789012345678", prefix + "323456789012345678"} {
		response, err := scheduled.RoundTrip(scheduledRequest(t, context.Background(), state, http.MethodGet, path, nil, false))
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		want := []int{http.StatusNotFound, http.StatusOK}[index]
		if response.StatusCode != want {
			t.Fatalf("request %d status = %d, want %d", index+1, response.StatusCode, want)
		}
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("nested webhook calls = %d, want 2", got)
	}
}

type signalingBody struct {
	started chan<- struct{}
	release <-chan struct{}
}

func (b *signalingBody) Read([]byte) (int, error) {
	select {
	case b.started <- struct{}{}:
	default:
	}
	<-b.release
	return 0, io.EOF
}

func (*signalingBody) Close() error { return nil }

func TestRateLimitRetryReentersLatestBucket(t *testing.T) {
	const (
		path          = "/api/v10/channels/123456789012345678/messages"
		seedPath      = "/api/v10/channels/123456789012345678/pins"
		discordBucket = "concurrently-remapped-bucket"
	)
	drainStarted := make(chan struct{}, 1)
	releaseDrain := make(chan struct{})
	drainReleased := false
	defer func() {
		if !drainReleased {
			close(releaseDrain)
		}
	}()
	var calls atomic.Int64
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			return testResponse(request, http.StatusTooManyRequests, testHeaders(map[string]string{
				"Retry-After": "0",
			}), &signalingBody{started: drainStarted, release: releaseDrain}), nil
		}
		return testResponse(request, http.StatusOK, nil, nil), nil
	})
	config := testConfig(transport)
	config.QueueTimeout = 3 * time.Second
	config.UpstreamTimeout = 2 * time.Second
	proxy := newTestProxy(t, config)
	state := proxy.noAuth
	major := majorParameter(path)
	route := routeHash(http.MethodPost, GetOptimisticBucketPath(path, http.MethodPost), major)
	seedRoute := routeHash(http.MethodPost, GetOptimisticBucketPath(seedPath, http.MethodPost), major)
	target := mustBucket(t, state, seedRoute)
	if got := state.learnBucket(seedRoute, discordBucket, major, target); got != target {
		t.Fatal("test setup changed seed target")
	}
	old := mustBucket(t, state, route)
	if old == target {
		t.Fatal("test setup did not create an obsolete bucket")
	}
	if err := target.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	targetHeld := true
	defer func() {
		if targetHeld {
			target.release()
		}
	}()

	type result struct {
		response *http.Response
		err      error
	}
	results := make(chan result, 1)
	request := scheduledRequest(t, context.Background(), state, http.MethodPost, path, nil, true)
	go func() {
		response, err := (&scheduledTransport{base: proxy.transport, proxy: proxy}).RoundTrip(request)
		results <- result{response: response, err: err}
	}()
	select {
	case <-drainStarted:
	case <-time.After(time.Second):
		t.Fatal("429 body drain did not start")
	}
	if got := state.learnBucket(route, discordBucket, major, old); got != target {
		t.Fatal("concurrent remap did not select the canonical target")
	}
	close(releaseDrain)
	drainReleased = true
	waitForGateWaiters(t, &target.gate, 1)
	if got := calls.Load(); got != 1 {
		t.Fatalf("retry bypassed the latest bucket: upstream calls = %d, want 1", got)
	}
	target.release()
	targetHeld = false

	select {
	case got := <-results:
		if got.err != nil {
			t.Fatal(got.err)
		}
		_ = got.response.Body.Close()
		if got.response.StatusCode != http.StatusOK || calls.Load() != 2 {
			t.Fatalf("final status=%d calls=%d, want 200/2", got.response.StatusCode, calls.Load())
		}
	case <-time.After(time.Second):
		t.Fatal("retry remained blocked after the latest bucket was released")
	}
}
