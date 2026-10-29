package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func userRateLimit(request *http.Request, retryAfter string) *http.Response {
	header := testHeaders(map[string]string{"Via": "1.1 google", "X-RateLimit-Scope": "user", "X-RateLimit-Remaining": "0"})
	if retryAfter != "" {
		header.Set("Retry-After", retryAfter)
	}
	return testResponse(request, http.StatusTooManyRequests, header, io.NopCloser(strings.NewReader(`{"message": "You are being rate limited.", "global": false}`)))
}

// stormFollowUp is a follow-up of its own application and interaction, so no bucket, breaker or
// pacer slows a storm of them: only the IP-wide guards can.
func stormFollowUp(client, request int) string {
	return followUpPath(strconv.Itoa(203039963636301824+client*1_000_000+request), request)
}

// drained reads a request's body as Discord would, so Sluice can replay it.
func drained(request *http.Request) {
	if request.Body != nil {
		_, _ = io.Copy(io.Discard, request.Body)
	}
}

func TestRateLimitsOnTokenRoutesAreNeverRetried(t *testing.T) {
	for name, path := range map[string]string{
		"follow-up":            followUpPath("203039963636301824", 1),
		"interaction callback": "/api/v10/interactions/1103049420930568322/" + interactionToken("1103049420930568322") + "/callback",
		"webhook":              "/api/v10/webhooks/123456789012345678/webhook-token",
	} {
		for _, scope := range []string{"user", "global"} {
			t.Run(name+" "+scope, func(t *testing.T) {
				var calls atomic.Int64
				config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
					calls.Add(1)
					drained(request)
					response := userRateLimit(request, "0")
					if scope == "global" {
						response.Header.Set("X-RateLimit-Global", "true")
						response.Header.Set("X-RateLimit-Scope", "global")
					}
					return response, nil
				}))
				config.QueueTimeout = 2 * time.Second
				proxy := newTestProxy(t, config)
				response := serve(proxy, http.MethodPost, path, "", strings.NewReader(`{}`))
				if response.Code != http.StatusTooManyRequests || response.Header().Get("Generated-By-Proxy") != "" || calls.Load() != 1 {
					t.Fatalf("got %d %v after %d upstream attempts, want Discord's own 429 after one", response.Code, response.Header(), calls.Load())
				}
			})
		}
	}
}

func TestRateLimitRetriesAreBounded(t *testing.T) {
	for _, test := range []struct {
		name       string
		retryAfter string
		want       int64
	}{
		{"short cooldown", "0", 2},
		{"no Retry-After", "", 1},
		{"long cooldown", "6", 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int64
			config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
				calls.Add(1)
				drained(request)
				return userRateLimit(request, test.retryAfter), nil
			}))
			config.QueueTimeout = 10 * time.Second
			proxy := newTestProxy(t, config)
			started := time.Now()
			response := serve(proxy, http.MethodPost, "/api/v10/channels/1/messages", "Bot "+fakeBotToken, strings.NewReader(`{}`))
			if response.Code != http.StatusTooManyRequests || calls.Load() != test.want {
				t.Fatalf("got %d after %d upstream attempts in %v, want 429 after %d", response.Code, calls.Load(), time.Since(started), test.want)
			}
		})
	}
}

func TestRefusalRateGuardCapsRefusedResponses(t *testing.T) {
	output := captureLog(t)
	for name, answer := range map[string]func(*http.Request) *http.Response{
		"403": func(request *http.Request) *http.Response {
			return jsonResponse(request, http.StatusForbidden, `{"message": "Missing Access", "code": 50001}`)
		},
		"shared 429": sharedRateLimitResponse,
	} {
		t.Run(name, func(t *testing.T) {
			var mu sync.Mutex
			var sent []time.Time
			config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
				mu.Lock()
				sent = append(sent, time.Now())
				mu.Unlock()
				time.Sleep(5 * time.Millisecond)
				return answer(request), nil
			}))
			config.MaxRefusalsPerSecond = 20
			config.MaxInFlightRequests = 256
			config.MaxBucketStates = 1 << 20
			proxy := newTestProxy(t, config)
			before := testutil.ToFloat64(ProxyFailures.WithLabelValues("refusal_rate"))
			const clients = 16
			stop := time.Now().Add(2500 * time.Millisecond)
			var wg sync.WaitGroup
			var local atomic.Int64
			for client := range clients {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for request := 0; time.Now().Before(stop); request++ {
						response := serve(proxy, http.MethodPost, stormFollowUp(client, request), "", nil)
						if response.Header().Get("Generated-By-Proxy") == "true" {
							local.Add(1)
							if response.Code != http.StatusTooManyRequests || response.Header().Get("Retry-After") == "" {
								t.Errorf("a locally answered request got %d %v, want a 429 with Retry-After", response.Code, response.Header())
								return
							}
						}
					}
				}()
			}
			wg.Wait()
			mu.Lock()
			defer mu.Unlock()
			// Requests already in flight when the first refusals arrive are refused too, so only
			// the seconds after the first are held to the ceiling.
			worst := 0
			for start := range sent {
				if sent[start].Sub(sent[0]) < time.Second {
					continue
				}
				end := start
				for end < len(sent) && sent[end].Sub(sent[start]) < time.Second {
					end++
				}
				worst = max(worst, end-start)
			}
			t.Logf("%d refused upstream in 2.5 s, worst later second %d, %d answered locally", len(sent), worst, local.Load())
			if limit := 3*config.MaxRefusalsPerSecond + clients; len(sent) > limit {
				t.Fatalf("Discord refused %d requests in 2.5 s, want at most %d", len(sent), limit)
			}
			if worst > config.MaxRefusalsPerSecond {
				t.Fatalf("after the first second, one second held %d refusals, want at most %d", worst, config.MaxRefusalsPerSecond)
			}
			if got := testutil.ToFloat64(ProxyFailures.WithLabelValues("refusal_rate")) - before; got == 0 || int64(got) > local.Load() {
				t.Fatalf("refusal_rate counted %v of %d local answers", got, local.Load())
			}
		})
	}
	if !strings.Contains(output.String(), "answering requests locally") {
		t.Fatalf("no warning when the refusal guard tripped:\n%s", output.String())
	}
}

func TestRefusalRateGuardIgnoresNotFound(t *testing.T) {
	var calls atomic.Int64
	config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls.Add(1)
		return jsonResponse(request, http.StatusNotFound, `{"message": "Unknown Message", "code": 10008}`), nil
	}))
	config.MaxRefusalsPerSecond = 5
	proxy := newTestProxy(t, config)
	for message := range 50 {
		response := serve(proxy, http.MethodGet, "/api/v10/channels/1/messages/"+strconv.Itoa(1000+message), "Bot "+fakeBotToken, nil)
		if response.Code != http.StatusNotFound {
			t.Fatalf("404 number %d reached the client as %d", message, response.Code)
		}
	}
	if calls.Load() != 50 {
		t.Fatalf("Discord was called %d times for 50 requests answered 404", calls.Load())
	}
}

func TestRefusalGuardWaitsForASlotUnderPressure(t *testing.T) {
	guard := newRefusalGuard(4)
	now := time.Now()
	// Two refusals put the guard under pressure; two slots remain for requests in flight.
	for range 2 {
		if delay, err := guard.admit(context.Background()); delay != 0 || err != nil {
			t.Fatalf("admit = %v, %v", delay, err)
		}
		guard.complete(now, true)
	}
	for range 2 {
		if delay, err := guard.admit(context.Background()); delay != 0 || err != nil {
			t.Fatalf("admit under pressure = %v, %v", delay, err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := guard.admit(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a third request in flight under pressure was admitted: %v", err)
	}
	admitted := make(chan error, 1)
	go func() {
		_, err := guard.admit(context.Background())
		admitted <- err
	}()
	time.Sleep(20 * time.Millisecond)
	guard.complete(time.Now(), false)
	select {
	case err := <-admitted:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("a finished request did not free its slot")
	}
	// Two more refusals reach the ceiling: the guard answers locally for its cooldown.
	guard.complete(time.Now(), true)
	guard.complete(time.Now(), true)
	if delay, err := guard.admit(context.Background()); err != nil || delay <= 0 || delay > refusalCooldown {
		t.Fatalf("admit after the ceiling = %v, %v; want a local answer for up to %v", delay, err, refusalCooldown)
	}
}

func TestQueuedRequestsStopOnceTheirTokenIsRejected(t *testing.T) {
	var revoked atomic.Bool
	var rejected atomic.Int64
	config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if !revoked.Load() {
			return jsonResponse(request, http.StatusOK, `{}`), nil
		}
		rejected.Add(1)
		return jsonResponse(request, http.StatusUnauthorized, `{"message": "401: Unauthorized", "code": 0}`), nil
	}))
	config.MaxQueueDepth = 64
	proxy := newTestProxy(t, config)
	bot := "Bot " + fakeBotToken
	if response := serve(proxy, http.MethodGet, "/api/v10/channels/1/messages", bot, nil); response.Code != http.StatusOK {
		t.Fatalf("setup request got %d", response.Code)
	}
	revoked.Store(true)
	// Requests for different channels wait for the global limit, not for one another's buckets.
	var wg sync.WaitGroup
	for channel := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if response := serve(proxy, http.MethodGet, "/api/v10/channels/"+strconv.Itoa(100+channel)+"/messages", bot, nil); response.Code != http.StatusUnauthorized {
				t.Errorf("a request for a revoked token got %d", response.Code)
			}
		}()
	}
	wg.Wait()
	if rejected.Load() != 1 {
		t.Fatalf("Discord rejected the revoked token %d times, want 1", rejected.Load())
	}
}

func TestExpiredInteractionTokenIsAnsweredLocally(t *testing.T) {
	var calls sync.Map
	config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		count, _ := calls.LoadOrStore(request.URL.Path, new(atomic.Int64))
		count.(*atomic.Int64).Add(1)
		return jsonResponse(request, http.StatusUnauthorized, `{"message": "Invalid Webhook Token", "code": 50027}`), nil
	}))
	proxy := newTestProxy(t, config)
	expired, other := followUpPath("203039963636301824", 1), followUpPath("203039963636301824", 2)
	before := testutil.ToFloat64(InteractionShortCircuits)
	for attempt := range 5 {
		response := serve(proxy, http.MethodPost, expired, "", strings.NewReader(`{}`))
		if response.Code != http.StatusUnauthorized || !strings.Contains(response.Body.String(), "50027") {
			t.Fatalf("attempt %d got %d %q", attempt, response.Code, response.Body.String())
		}
		if attempt > 0 && response.Header().Get("Generated-By-Proxy") != "true" {
			t.Fatalf("attempt %d reached Discord again", attempt)
		}
	}
	// The same token on its other routes is answered locally too.
	edit := serve(proxy, http.MethodPatch, expired+"/messages/@original", "", strings.NewReader(`{}`))
	if edit.Code != http.StatusUnauthorized || edit.Header().Get("Generated-By-Proxy") != "true" {
		t.Fatalf("editing the expired interaction's reply got %d %v", edit.Code, edit.Header())
	}
	count, _ := calls.Load(expired)
	if got := count.(*atomic.Int64).Load(); got != 1 {
		t.Fatalf("the expired token reached Discord %d times, want 1", got)
	}
	if response := serve(proxy, http.MethodPost, other, "", strings.NewReader(`{}`)); response.Header().Get("Generated-By-Proxy") != "" {
		t.Fatal("another interaction's token was answered locally")
	}
	if got := testutil.ToFloat64(InteractionShortCircuits) - before; got != 5 {
		t.Fatalf("interaction short-circuits counted %v, want 5", got)
	}
}

func TestInteractionTokenGuardIsBoundedAndExpires(t *testing.T) {
	guard := newInteractionTokenGuard()
	now := time.Now()
	for index := range maxInteractionFailures + 10 {
		guard.record(webhookFailure{key: strconv.Itoa(index), status: http.StatusUnauthorized, code: invalidWebhookTokenCode, expires: now.Add(interactionFailureTTL)})
	}
	if len(guard.entries) != maxInteractionFailures || guard.order.Len() != maxInteractionFailures {
		t.Fatalf("guard holds %d entries, want %d", len(guard.entries), maxInteractionFailures)
	}
	if _, blocked := guard.lookup("0", now); blocked {
		t.Fatal("the oldest token was kept past the bound")
	}
	newest := strconv.Itoa(maxInteractionFailures + 9)
	if _, blocked := guard.lookup(newest, now.Add(interactionFailureTTL-time.Second)); !blocked {
		t.Fatal("the newest token was forgotten before its 15 minutes")
	}
	if _, blocked := guard.lookup(newest, now.Add(interactionFailureTTL)); blocked {
		t.Fatal("the newest token outlived its 15 minutes")
	}
}

func TestSharedLimitReopensWithOneTrialRequest(t *testing.T) {
	var calls atomic.Int64
	var shared atomic.Bool
	shared.Store(true)
	config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls.Add(1)
		time.Sleep(50 * time.Millisecond)
		if shared.Load() {
			return sharedRateLimitResponse(request), nil
		}
		return testResponse(request, http.StatusNoContent, discordVia.Clone(), nil), nil
	}))
	config.MaxInFlightRequests = 256
	config.MaxRefusalsPerSecond = 1000
	proxy := newTestProxy(t, config)
	const application = "203039963636301824"
	path := followUpPath(application, 0)
	key := sharedResourceKey(http.MethodPost, MetricsPathFromBucket(GetOptimisticBucketPath(path, http.MethodPost)), path)
	expire := func() {
		proxy.sharedLimits.mu.Lock()
		defer proxy.sharedLimits.mu.Unlock()
		proxy.sharedLimits.entries[key].Value.(*sharedLimitEntry).openUntil = time.Now().Add(-time.Millisecond)
	}
	burst := func(from int) int64 {
		before := calls.Load()
		var wg sync.WaitGroup
		for interaction := range 20 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				serve(proxy, http.MethodPost, followUpPath(application, from+interaction), "", nil)
			}()
		}
		wg.Wait()
		return calls.Load() - before
	}
	for interaction := range sharedLimitThreshold {
		serve(proxy, http.MethodPost, followUpPath(application, interaction), "", nil)
	}
	if proxy.sharedLimits.retryAfter(key, time.Now()) <= 0 {
		t.Fatal("the storm did not close the resource")
	}

	// The closure ends while the resource is still limited: one trial, which closes it again.
	expire()
	if got := burst(100); got != 1 {
		t.Fatalf("%d requests reached Discord when the closure ended, want one trial", got)
	}
	if proxy.sharedLimits.retryAfter(key, time.Now()) < minSharedLimitCooldown-time.Second {
		t.Fatal("a shared 429 on the trial did not close the resource again")
	}

	// The next closure ends after the limit has: the trial succeeds and the resource opens fully.
	shared.Store(false)
	expire()
	if got := burst(200); got != 1 {
		t.Fatalf("%d requests reached Discord when the closure ended, want one trial", got)
	}
	if got := burst(300); got != 20 {
		t.Fatalf("%d of 20 requests reached Discord after a successful trial, want every one", got)
	}
}

func TestSuspicionProbeThatProvesNothingPausesProvisionally(t *testing.T) {
	output := captureLog(t)
	var probes, calls atomic.Int64
	config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path == "/api/v10/gateway" {
			probes.Add(1)
			return testResponse(request, http.StatusBadGateway, testHeaders(map[string]string{"Server": "cloudflare"}), http.NoBody), nil
		}
		calls.Add(1)
		return testResponse(request, http.StatusForbidden, nil, nil), nil
	}))
	config.CloudflareBanDetection = true
	config.StateFile = filepath.Join(t.TempDir(), "state.json")
	proxy := newTestProxy(t, config)

	serve(proxy, http.MethodGet, "/api/v10/channels/1/messages", "", nil)
	waitForEdgeProbe(t, proxy)
	pause := proxy.cloudflare.retryAfter(time.Now())
	if pause < provisionalEdgePause-time.Second || pause > provisionalEdgePause {
		t.Fatalf("a refusal whose probe proved nothing paused traffic for %v, want %v", pause, provisionalEdgePause)
	}
	for channel := 2; channel < 40; channel++ {
		serve(proxy, http.MethodGet, "/api/v10/channels/"+strconv.Itoa(channel)+"/messages", "", nil)
	}
	if calls.Load() != 1 || probes.Load() != 1 {
		t.Fatalf("after the provisional pause, %d client requests and %d probes reached the edge, want 1 and 1", calls.Load(), probes.Load())
	}
	if !strings.Contains(output.String(), "provisionally") {
		t.Fatalf("no warning for the provisional pause:\n%s", output.String())
	}
	waitForStateFile(t, config.StateFile, func(state savedState) bool { return state.EdgeBlockedUntil > time.Now().UnixMilli() })
}

func TestSuspicionProbesAreHeldOffWhateverTheirVerdict(t *testing.T) {
	var guard cloudflareGuard
	now := time.Now()
	if guard.suspect(now) != suspicionProbe {
		t.Fatal("the first refusal did not start a probe")
	}
	guard.settle(now, edgeClear, 0)
	for refusal := range edgeSuspicionRefusals - 2 {
		if action := guard.suspect(now.Add(time.Duration(refusal+1) * time.Second)); action != suspicionNone {
			t.Fatalf("refusal %d inside the hold-off returned %v", refusal+2, action)
		}
	}
	if action := guard.suspect(now.Add(edgeProbeInterval)); action != suspicionProbe {
		t.Fatalf("a refusal after the hold-off returned %v, want a probe", action)
	}
}

func TestRefusalsAfterAClearProbeStillPause(t *testing.T) {
	var probes, calls atomic.Int64
	config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path == "/api/v10/gateway" {
			probes.Add(1)
			return discordGateway(request), nil
		}
		calls.Add(1)
		return edgeRefusal(request, ""), nil
	}))
	config.CloudflareBanDetection = true
	proxy := newTestProxy(t, config)

	serve(proxy, http.MethodGet, "/api/v10/channels/1/messages", "", nil)
	waitForEdgeProbe(t, proxy)
	if proxy.cloudflare.retryAfter(time.Now()) != 0 {
		t.Fatal("a refusal whose probe Discord answered paused traffic")
	}
	for channel := 2; channel < 100; channel++ {
		serve(proxy, http.MethodGet, "/api/v10/channels/"+strconv.Itoa(channel)+"/messages", "", nil)
	}
	if pause := proxy.cloudflare.retryAfter(time.Now()); pause <= 0 {
		t.Fatalf("%d edge refusals after one clear probe never paused traffic", calls.Load())
	}
	if calls.Load() != edgeSuspicionRefusals || probes.Load() != 1 {
		t.Fatalf("%d client requests and %d probes reached the edge, want %d and 1", calls.Load(), probes.Load(), edgeSuspicionRefusals)
	}
}

func TestHungSuspicionProbeDoesNotLetTrafficFlow(t *testing.T) {
	var calls atomic.Int64
	config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path == "/api/v10/gateway" {
			<-request.Context().Done()
			return nil, request.Context().Err()
		}
		calls.Add(1)
		return edgeRefusal(request, ""), nil
	}))
	config.CloudflareBanDetection = true
	proxy := newTestProxy(t, config)
	for channel := 1; channel < 100; channel++ {
		serve(proxy, http.MethodGet, "/api/v10/channels/"+strconv.Itoa(channel)+"/messages", "", nil)
	}
	if calls.Load() != edgeSuspicionRefusals || proxy.cloudflare.retryAfter(time.Now()) <= 0 {
		t.Fatalf("%d client requests reached the edge while the probe hung, want %d and a pause", calls.Load(), edgeSuspicionRefusals)
	}
}

// waitForStateFile polls STATE_FILE until check accepts it, while the proxy that writes it keeps
// running, as a kill -9 would leave the file.
func waitForStateFile(t *testing.T, name string, check func(savedState) bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		var state savedState
		if data, err := os.ReadFile(name); err == nil && json.Unmarshal(data, &state) == nil && check(state) {
			return
		}
		if time.Now().After(deadline) {
			data, _ := os.ReadFile(name)
			t.Fatalf("STATE_FILE never reached the expected state; it holds %s", data)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func savedInvalid(state savedState) int64 {
	total := int64(0)
	for _, second := range state.InvalidRequests {
		total += second[1]
	}
	return total
}

func TestInvalidResponsesReachStateFileBeforeAKill(t *testing.T) {
	config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return jsonResponse(request, http.StatusForbidden, `{"message": "Missing Access", "code": 50001}`), nil
	}))
	config.MaxRefusalsPerSecond = 100_000
	config.MaxBucketStates = 1 << 20
	config.StateFile = filepath.Join(t.TempDir(), "state.json")
	proxy := newTestProxy(t, config)
	const sent = 3*invalidSaveStep + 10
	for request := range sent {
		serve(proxy, http.MethodPost, stormFollowUp(0, request), "", nil)
	}
	if got := proxy.invalidRequests.count(time.Now()); got != sent {
		t.Fatalf("recorded %d invalid responses, want %d", got, sent)
	}
	waitForStateFile(t, config.StateFile, func(state savedState) bool { return savedInvalid(state) >= 3*invalidSaveStep })

	// Restart from the file while the first process still runs, as after kill -9.
	restarted := newTestProxy(t, config)
	if got := restarted.invalidRequests.count(time.Now()); got < sent-invalidSaveStep {
		t.Fatalf("a restart after %d invalid responses restored %d, want at least %d", sent, got, sent-invalidSaveStep)
	}
}

func TestEdgePauseReachesStateFileAtOnce(t *testing.T) {
	config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return edgeRefusal(request, "3600"), nil
	}))
	config.CloudflareBanDetection = true
	config.StateFile = filepath.Join(t.TempDir(), "state.json")
	proxy := newTestProxy(t, config)
	serve(proxy, http.MethodGet, "/api/v10/channels/1/messages", "", nil)
	waitForEdgeProbe(t, proxy)
	waitForStateFile(t, config.StateFile, func(state savedState) bool {
		return state.EdgeBlockedUntil > time.Now().Add(59*time.Minute).UnixMilli()
	})
	restarted := newTestProxy(t, config)
	if pause := restarted.cloudflare.retryAfter(time.Now()); pause < 59*time.Minute {
		t.Fatalf("a restart right after the block restored a pause of %v", pause)
	}
}

func TestRefusalGuardTripReachesStateFile(t *testing.T) {
	config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return jsonResponse(request, http.StatusForbidden, `{"message": "Missing Access", "code": 50001}`), nil
	}))
	config.MaxRefusalsPerSecond = 5
	config.StateFile = filepath.Join(t.TempDir(), "state.json")
	proxy := newTestProxy(t, config)
	for request := range 20 {
		serve(proxy, http.MethodPost, stormFollowUp(0, request), "", nil)
	}
	waitForStateFile(t, config.StateFile, func(state savedState) bool { return state.RefusalGuardUntil > time.Now().UnixMilli() })
	restarted := newTestProxy(t, config)
	if !restarted.refusals.guarded(time.Now()) {
		t.Fatal("a restart right after the refusal guard tripped forgot its probation")
	}
}

func TestStateFileStaysReadableByEarlierReleases(t *testing.T) {
	config := testConfig(nil)
	config.CloudflareBanDetection = true
	config.StateFile = filepath.Join(t.TempDir(), "state.json")
	proxy := newTestProxy(t, config)
	now := time.Now()
	proxy.invalidRequests.reserve(now)
	proxy.invalidRequests.complete(now, true)
	proxy.cloudflare.settle(now, edgeBlocked, time.Hour)
	proxy.refusals.trip(now)
	if err := proxy.saveState(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(config.StateFile)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "refusalGuardUntil") {
		t.Fatalf("state file %s lacks the refusal guard", data)
	}
	// How 1.0.0 reads the file: its own fields, ignoring any it does not know, and version 1.
	var earlier struct {
		Version          int        `json:"version"`
		InvalidRequests  [][2]int64 `json:"invalidRequests,omitempty"`
		EdgeBlockedUntil int64      `json:"edgeBlockedUntil,omitempty"`
		Webhooks         []struct {
			Key     string `json:"key"`
			Status  int    `json:"status"`
			Code    int    `json:"code"`
			Expires int64  `json:"expires"`
		} `json:"webhooks,omitempty"`
	}
	if err := json.Unmarshal(data, &earlier); err != nil || earlier.Version != 1 || len(earlier.InvalidRequests) != 1 || earlier.EdgeBlockedUntil == 0 {
		t.Fatalf("1.0.0 would read %s as %+v (%v)", data, earlier, err)
	}
}
