package proxy

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func captureRequest(body string, contentLength int64) *http.Request {
	request := httptest.NewRequest(http.MethodPost, "/api/v10/channels/1/messages", nil)
	request.Body = io.NopCloser(strings.NewReader(body))
	request.ContentLength = contentLength
	request.GetBody = nil
	return request
}

func TestRetryCaptureKnownLengthCapacityAndRelease(t *testing.T) {
	budget := newResourceBudget(4)
	first, err := newBodyReplay(captureRequest("1234", 4), 8, budget)
	if err != nil {
		t.Fatal(err)
	}
	if got := budget.used.Load(); got != 4 {
		t.Fatalf("reserved bytes = %d, want 4", got)
	}
	if _, err := newBodyReplay(captureRequest("x", 1), 8, budget); !errors.Is(err, errRetryCaptureBudget) {
		t.Fatalf("saturated admission error = %v, want %v", err, errRetryCaptureBudget)
	}

	first.close()
	first.close()
	if got := budget.used.Load(); got != 0 {
		t.Fatalf("bytes after repeated cleanup = %d, want 0", got)
	}
	second, err := newBodyReplay(captureRequest("x", 1), 8, budget)
	if err != nil {
		t.Fatalf("admission after release: %v", err)
	}
	second.close()
	if got := budget.used.Load(); got != 0 {
		t.Fatalf("bytes after final cleanup = %d, want 0", got)
	}
}

func TestRetryCaptureCapacityReturns503BeforeUpstream(t *testing.T) {
	var calls atomic.Int64
	config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls.Add(1)
		return testResponse(request, http.StatusOK, nil, nil), nil
	}))
	config.MaxRetryCaptureBytes = 3
	proxy := newTestProxy(t, config)

	request := captureRequest("1234", 4)
	response := httptest.NewRecorder()
	proxy.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", response.Code)
	}
	if calls.Load() != 0 {
		t.Fatalf("upstream calls = %d, want 0", calls.Load())
	}
}

func TestZeroRetryCaptureCapacityPassesBodyThrough(t *testing.T) {
	var calls atomic.Int64
	var upstreamBody string
	config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls.Add(1)
		body, err := io.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
		upstreamBody = string(body)
		return testResponse(request, http.StatusCreated, nil, nil), nil
	}))
	config.MaxRetryCaptureBytes = 0
	proxy := newTestProxy(t, config)

	response := httptest.NewRecorder()
	proxy.ServeHTTP(response, captureRequest("1234", 4))
	if response.Code != http.StatusCreated || calls.Load() != 1 || upstreamBody != "1234" {
		t.Fatalf("status=%d calls=%d body=%q, want 201/1/1234", response.Code, calls.Load(), upstreamBody)
	}
}

func TestUnknownRetryCaptureExhaustionReleasesReservation(t *testing.T) {
	budget := newResourceBudget(3)
	replay, err := newBodyReplay(captureRequest("1234", -1), 8, budget)
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
	if retryable, err := replay.prepareRetry(); err != nil || retryable {
		t.Fatalf("exhausted unknown body: retryable=%v err=%v", retryable, err)
	}
	if got := budget.used.Load(); got != 0 {
		t.Fatalf("bytes after capture exhaustion = %d, want 0", got)
	}
	replay.close()
}

func TestEmptyUnknownLengthBodyReplays(t *testing.T) {
	for name, contentLength := range map[string]int64{"negative": -1, "zero": 0} {
		t.Run(name, func(t *testing.T) {
			budget := newResourceBudget(1)
			replay, err := newBodyReplay(captureRequest("", contentLength), 1, budget)
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
			if retryable, err := replay.prepareRetry(); err != nil || !retryable {
				t.Fatalf("empty body: retryable=%v err=%v", retryable, err)
			}
			second, err := replay.body(1)
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(second)
			_ = second.Close()
			if err != nil || len(data) != 0 {
				t.Fatalf("empty replay bytes=%d err=%v", len(data), err)
			}
			replay.close()
		})
	}
}

func TestClientStateCapEvictsOnlyIdleUnblockedState(t *testing.T) {
	config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return testResponse(request, http.StatusOK, nil, nil), nil
	}))
	config.MaxBearerClients = 2
	config.MaxClientStates = 2
	proxy := newTestProxy(t, config)

	botIdentity := identify("Bot blocked-bot")
	bot, err := proxy.client(botIdentity)
	if err != nil {
		t.Fatal(err)
	}
	bot.end()
	bearerIdentity := identify("Bearer idle-bearer")
	bearer, err := proxy.client(bearerIdentity)
	if err != nil {
		t.Fatal(err)
	}
	bearer.end()
	old := time.Now().Add(-clientIdleTimeout - time.Minute).UnixNano()
	bot.lastUsed.Store(old)
	bearer.lastUsed.Store(old)
	mustBucket(t, bot, 1).blockUntil(time.Now().Add(time.Hour))

	replacementIdentity := identify("Bot replacement")
	replacement, err := proxy.client(replacementIdentity)
	if err != nil {
		t.Fatal(err)
	}
	replacement.end()
	proxy.clientsMu.Lock()
	blockedStillPresent := proxy.bots[botIdentity.key] == bot
	_, idleStillPresent := proxy.bearers[bearerIdentity.key]
	proxy.clientsMu.Unlock()
	if !blockedStillPresent || idleStillPresent {
		t.Fatalf("cap eviction blocked=%v idle=%v, want blocked=true idle=false", blockedStillPresent, idleStillPresent)
	}

	if admitted, err := proxy.client(identify("Bearer no-room")); !errors.Is(err, errTooManyClients) {
		if err == nil {
			admitted.end()
		}
		t.Fatalf("full non-idle cap error = %v, want %v", err, errTooManyClients)
	}
}

func TestBucketStateCapIsSharedAndReleasesOnSweep(t *testing.T) {
	config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return testResponse(request, http.StatusOK, nil, nil), nil
	}))
	config.MaxBucketStates = 1
	proxy := newTestProxy(t, config)
	first := mustBucket(t, proxy.noAuth, 1)

	client, err := proxy.client(identify("Bot second-client"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.end()
	if _, err := client.bucket(2); !errors.Is(err, errBucketStateLimit) {
		t.Fatalf("shared bucket cap error = %v, want %v", err, errBucketStateLimit)
	}

	old := time.Now().Add(-clientIdleTimeout - time.Minute)
	first.lastUsed.Store(old.UnixNano())
	proxy.noAuth.sweep(time.Now())
	if got := proxy.bucketSlots.used.Load(); got != 0 {
		t.Fatalf("bucket slots after sweep = %d, want 0", got)
	}
	if _, err := client.bucket(2); err != nil {
		t.Fatalf("bucket admission after sweep: %v", err)
	}
}

func TestBucketAliasCannotGrowPastStateBudget(t *testing.T) {
	slots := newResourceBudget(1)
	state := newClientState(identity{kind: authNone, label: "NoAuth"}, discordGlobalLimit, 8, slots)
	const routeHash = 1
	current := mustBucket(t, state, routeHash)
	if target := state.learnBucket(routeHash, "discord-bucket", "channels:1", current); target != current {
		t.Fatal("capacity-limited alias changed the active bucket")
	}
	state.mu.RLock()
	buckets, aliases := len(state.buckets), len(state.aliases)
	state.mu.RUnlock()
	if buckets != 1 || aliases != 0 || slots.used.Load() != 1 {
		t.Fatalf("bounded state buckets=%d aliases=%d slots=%d, want 1/0/1", buckets, aliases, slots.used.Load())
	}
}

func TestSweepReclaimsLargeBucketAndAliasSet(t *testing.T) {
	const entries = 4096
	now := time.Now()
	old := now.Add(-clientIdleTimeout - time.Minute).UnixNano()
	slots := newResourceBudget(2 * entries)
	state := newClientState(identity{kind: authNone, label: "NoAuth"}, discordGlobalLimit, 8, slots)
	if !slots.reserve(2 * entries) {
		t.Fatal("reserve test state")
	}
	state.mu.Lock()
	for index := range entries {
		hash := uint64(index + 1)
		bucket := newBucketState(8)
		bucket.lastUsed.Store(old)
		if index%2 == 0 {
			bucket.blockUntil(now.Add(time.Hour))
		}
		state.buckets[hash] = bucket
		state.aliases[hash+entries] = hash
	}
	state.mu.Unlock()

	state.sweep(now)
	state.mu.RLock()
	buckets, aliases := len(state.buckets), len(state.aliases)
	state.mu.RUnlock()
	if buckets != entries/2 || aliases != entries/2 {
		t.Fatalf("swept state buckets=%d aliases=%d, want %d/%d", buckets, aliases, entries/2, entries/2)
	}
	if got := slots.used.Load(); got != entries {
		t.Fatalf("remaining state slots = %d, want %d", got, entries)
	}
}

func TestClientAdmissionAndCloseAreSerialized(t *testing.T) {
	proxy, err := New(testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return testResponse(request, http.StatusOK, nil, nil), nil
	})))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := proxy.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := proxy.client(identify("Bot after-close")); !errors.Is(err, errProxyClosed) {
		t.Fatalf("post-close admission error = %v, want %v", err, errProxyClosed)
	}
}

func TestInFlightAdmissionRejectsExcessButBypassesHealth(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		close(started)
		select {
		case <-release:
			return testResponse(request, http.StatusOK, nil, nil), nil
		case <-request.Context().Done():
			return nil, request.Context().Err()
		}
	}))
	config.MaxInFlightRequests = 1
	proxy := newTestProxy(t, config)

	firstResponse := httptest.NewRecorder()
	firstDone := make(chan struct{})
	go func() {
		proxy.ServeHTTP(firstResponse, httptest.NewRequest(http.MethodGet, "/api/v10/gateway", nil))
		close(firstDone)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first request did not reach upstream")
	}

	healthResponse := httptest.NewRecorder()
	proxy.ServeHTTP(healthResponse, httptest.NewRequest(http.MethodGet, "/nirn/healthz", nil))
	if healthResponse.Code != http.StatusOK {
		t.Fatalf("health under saturation = %d, want 200", healthResponse.Code)
	}
	secondResponse := httptest.NewRecorder()
	proxy.ServeHTTP(secondResponse, httptest.NewRequest(http.MethodGet, "/api/v10/gateway", nil))
	if secondResponse.Code != http.StatusServiceUnavailable || secondResponse.Header().Get("X-Nirn-Proxy-Error") != "true" {
		t.Fatalf("saturated response = %d error=%q, want 503/true", secondResponse.Code, secondResponse.Header().Get("X-Nirn-Proxy-Error"))
	}

	close(release)
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("admitted request did not finish")
	}
	if firstResponse.Code != http.StatusOK || proxy.inFlight.used.Load() != 0 {
		t.Fatalf("first status=%d remaining admissions=%d, want 200/0", firstResponse.Code, proxy.inFlight.used.Load())
	}
}

func TestDiscordTransportCapsActiveConnections(t *testing.T) {
	transport, err := newHTTPTransport("", false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(transport.CloseIdleConnections)
	if transport.MaxConnsPerHost != 1024 {
		t.Fatalf("Discord connection cap = %d, want 1024", transport.MaxConnsPerHost)
	}
}

func TestDiscordTransportDisablesHTTP2(t *testing.T) {
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	}))
	server.EnableHTTP2 = true
	server.TLS = &tls.Config{NextProtos: []string{"h2", "http/1.1"}}
	server.StartTLS()
	t.Cleanup(server.Close)

	transport, err := newHTTPTransport("", true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(transport.CloseIdleConnections)
	transport.TLSClientConfig.InsecureSkipVerify = true // test server certificate

	response, err := (&http.Client{Transport: transport}).Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.ProtoMajor != 1 {
		t.Fatalf("Discord protocol = %s, want HTTP/1.1", response.Proto)
	}
}
