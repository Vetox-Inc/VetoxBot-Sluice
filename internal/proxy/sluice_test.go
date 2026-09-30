package proxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var discordVia = testHeaders(map[string]string{"Via": "1.1 google"})

func serve(proxy *Proxy, method, path, authorization string, body io.Reader) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, body)
	if authorization != "" {
		request.Header.Set("Authorization", authorization)
	}
	response := httptest.NewRecorder()
	proxy.ServeHTTP(response, request)
	return response
}

func jsonResponse(request *http.Request, status int, body string) *http.Response {
	return testResponse(request, status, discordVia.Clone(), io.NopCloser(strings.NewReader(body)))
}

func TestWebhookFailFastOnlyForPermanentErrors(t *testing.T) {
	var calls sync.Map
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		count, _ := calls.LoadOrStore(request.URL.Path, new(atomic.Int64))
		count.(*atomic.Int64).Add(1)
		switch {
		case strings.Contains(request.URL.Path, "deleted"):
			return jsonResponse(request, http.StatusNotFound, `{"message": "Unknown Webhook", "code": 10015}`), nil
		case strings.Contains(request.URL.Path, "rotated"):
			return jsonResponse(request, http.StatusUnauthorized, `{"message": "Invalid Webhook Token", "code": 50027}`), nil
		default:
			return jsonResponse(request, http.StatusNotFound, `{"message": "Unknown Message", "code": 10008}`), nil
		}
	})
	proxy := newTestProxy(t, testConfig(transport))
	for _, test := range []struct {
		path       string
		status     int
		wantUpward int64
	}{
		{"/api/v10/webhooks/123456789012345678/deleted", http.StatusNotFound, 1},
		{"/api/v10/webhooks/123456789012345678/rotated", http.StatusUnauthorized, 1},
		{"/api/v10/webhooks/123456789012345678/live/messages/223456789012345678", http.StatusNotFound, 3},
		{"/api/v10/webhooks/123456789012345678/aW50ZXJhY3Rpb246deleted/messages/@original", http.StatusNotFound, 3},
	} {
		for range 3 {
			if response := serve(proxy, http.MethodGet, test.path, "", nil); response.Code != test.status {
				t.Fatalf("%s: status %d, want %d", test.path, response.Code, test.status)
			}
		}
		count, _ := calls.Load(test.path)
		if got := count.(*atomic.Int64).Load(); got != test.wantUpward {
			t.Errorf("%s reached Discord %d times, want %d", test.path, got, test.wantUpward)
		}
	}
	cached := serve(proxy, http.MethodPost, "/api/v10/webhooks/123456789012345678/deleted", "", nil)
	if cached.Header().Get("Generated-By-Proxy") != "true" || !strings.Contains(cached.Body.String(), "10015") {
		t.Fatalf("cached webhook response = %v %q", cached.Header(), cached.Body.String())
	}
}

func TestWebhookTokenUnauthorizedKeepsBotCredentialValid(t *testing.T) {
	var gatewayCalls atomic.Int64
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch {
		case strings.Contains(request.URL.Path, "/webhooks/"):
			return jsonResponse(request, http.StatusUnauthorized, `{"message": "Invalid Webhook Token", "code": 50027}`), nil
		case strings.HasSuffix(request.URL.Path, "/users/@me"):
			return jsonResponse(request, http.StatusUnauthorized, `{"message": "401: Unauthorized", "code": 0}`), nil
		default:
			gatewayCalls.Add(1)
			return jsonResponse(request, http.StatusOK, `{}`), nil
		}
	})
	proxy := newTestProxy(t, testConfig(transport))
	bot := "Bot " + fakeBotToken
	serve(proxy, http.MethodPost, "/api/v10/webhooks/123456789012345678/stale-token", bot, nil)
	if response := serve(proxy, http.MethodGet, "/api/v10/gateway", bot, nil); response.Code != http.StatusOK || gatewayCalls.Load() != 1 {
		t.Fatalf("after a webhook-token 401 the bot got %d with %d upstream calls, want 200/1", response.Code, gatewayCalls.Load())
	}
	serve(proxy, http.MethodGet, "/api/v10/users/@me", bot, nil)
	if response := serve(proxy, http.MethodGet, "/api/v10/gateway", bot, nil); response.Code != http.StatusUnauthorized || gatewayCalls.Load() != 1 {
		t.Fatalf("after its own 401 the bot got %d with %d upstream calls, want a local 401", response.Code, gatewayCalls.Load())
	}
}

func TestCloudflareBlockPausesOutboundTraffic(t *testing.T) {
	var blocked atomic.Bool
	var calls atomic.Int64
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls.Add(1)
		if blocked.Load() {
			return testResponse(request, http.StatusTooManyRequests, testHeaders(map[string]string{"Retry-After": "30"}), nil), nil
		}
		return testResponse(request, http.StatusTooManyRequests, testHeaders(map[string]string{
			"Via": "1.1 google", "Retry-After": "30", "X-RateLimit-Scope": "user", "X-RateLimit-Remaining": "0",
		}), nil), nil
	})
	config := testConfig(transport)
	config.CloudflareBanDetection = true
	proxy := newTestProxy(t, config)

	serve(proxy, http.MethodGet, "/api/v10/channels/1/messages", "", nil)
	if proxy.cloudflare.retryAfter(time.Now()) > 0 {
		t.Fatal("a 429 that reached Discord paused all traffic")
	}
	blocked.Store(true)
	serve(proxy, http.MethodGet, "/api/v10/channels/2/messages", "", nil)
	before := calls.Load()
	paused := serve(proxy, http.MethodGet, "/api/v10/channels/3/messages", "", nil)
	if calls.Load() != before || paused.Code != http.StatusTooManyRequests || paused.Header().Get("Via") != "" ||
		paused.Header().Get("Generated-By-Proxy") != "true" || paused.Header().Get("Retry-After") == "" {
		t.Fatalf("paused response = %d %v after %d upstream calls", paused.Code, paused.Header(), calls.Load()-before)
	}
	if health := serve(proxy, http.MethodGet, "/sluice/health/upstream", "", nil); health.Code != http.StatusServiceUnavailable {
		t.Fatalf("upstream health during a block = %d, want 503", health.Code)
	}
	if live := serve(proxy, http.MethodGet, "/sluice/healthz", "", nil); live.Code != http.StatusOK {
		t.Fatalf("liveness during a block = %d, want 200", live.Code)
	}
}

func TestProxyGeneratedResponsesCarryVia(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/api/v10/gateway", nil)
	if header := rememberedRateLimitResponse(request, time.Second, false).Header; header.Get("Via") != sluiceVia || header.Get("Generated-By-Proxy") != "true" {
		t.Fatalf("synthetic 429 headers = %v", header)
	}
	recorder := httptest.NewRecorder()
	writeUnavailable(recorder, "busy")
	if recorder.Header().Get("Via") != sluiceVia || recorder.Header().Get("Generated-By-Proxy") != "true" {
		t.Fatalf("proxy error headers = %v", recorder.Header())
	}
	if header := cloudflarePauseResponse(request, time.Second).Header; header.Get("Via") != "" {
		t.Fatalf("Cloudflare pause response carries Via: %v", header)
	}

	proxy := newTestProxy(t, testConfig(nil))
	for _, rejected := range []*http.Request{
		httptest.NewRequest(http.MethodGet, "/sluice/unknown", nil),
		httptest.NewRequest(http.MethodConnect, "/api/v10/gateway", nil),
		httptest.NewRequest(http.MethodGet, "/api/v10/channels/%2e%2e/messages", nil),
		httptest.NewRequest(http.MethodGet, "/sluice/healthz", nil),
		httptest.NewRequest(http.MethodGet, "/sluice/health/upstream", nil),
	} {
		recorder := httptest.NewRecorder()
		proxy.ServeHTTP(recorder, rejected)
		if recorder.Header().Get("Via") != sluiceVia || recorder.Header().Get("Generated-By-Proxy") != "true" {
			t.Fatalf("%s %s answered %d with headers %v", rejected.Method, rejected.URL.Path, recorder.Code, recorder.Header())
		}
	}
}

func TestRequestsReachDiscordIntact(t *testing.T) {
	type seen struct {
		requestURI string
		header     http.Header
	}
	received := make(chan seen, 1)
	discord := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		received <- seen{request.RequestURI, request.Header.Clone()}
		writer.Header().Add("Set-Cookie", "a=1")
		writer.Header().Add("Set-Cookie", "b=2")
		writer.Header().Set("Via", "1.1 google")
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer discord.Close()
	config := testConfig(nil)
	config.DiscordURL = discord.URL
	proxy := newTestProxy(t, config)

	request := httptest.NewRequest(http.MethodPut, "/v10/channels/1/messages/2/reactions/%23%EF%B8%8F%E2%83%A3/@me", nil)
	request.Header.Set("Authorization", "Bot "+fakeBotToken)
	request.Header.Set("X-Forwarded-For", "10.0.0.7")
	request.Header.Set("Forwarded", "for=10.0.0.7")
	request.Header.Set(hopHeader, "0")
	request.Header.Set("X-Audit-Log-Reason", "closed%20by%20mod")
	request.Header.Add("X-Multi", "one")
	request.Header.Add("X-Multi", "two")
	response := httptest.NewRecorder()
	proxy.ServeHTTP(response, request)

	var got seen
	select {
	case got = <-received:
	case <-time.After(2 * time.Second):
		t.Fatalf("request never reached Discord; proxy answered %d %q", response.Code, response.Body.String())
	}
	if want := "/api/v10/channels/1/messages/2/reactions/%23%EF%B8%8F%E2%83%A3/@me"; got.requestURI != want {
		t.Fatalf("Discord saw %q, want %q", got.requestURI, want)
	}
	for _, name := range []string{"X-Forwarded-For", "Forwarded", hopHeader} {
		if value := got.header.Get(name); value != "" {
			t.Errorf("%s reached Discord: %q", name, value)
		}
	}
	if got.header.Get("X-Audit-Log-Reason") != "closed%20by%20mod" || strings.Join(got.header.Values("X-Multi"), ",") != "one,two" {
		t.Errorf("end-to-end headers changed: %v", got.header)
	}
	if !strings.HasPrefix(got.header.Get("User-Agent"), "DiscordBot (https://github.com/Vetox-Inc/VetoxBot-Sluice") {
		t.Errorf("default User-Agent = %q", got.header.Get("User-Agent"))
	}
	if response.Code != http.StatusNoContent || len(response.Result().Header.Values("Set-Cookie")) != 2 {
		t.Fatalf("client got %d with cookies %v", response.Code, response.Result().Header.Values("Set-Cookie"))
	}
}

func TestPathTraversalIsRejectedBeforeRouting(t *testing.T) {
	var calls atomic.Int64
	proxy := newTestProxy(t, testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls.Add(1)
		return jsonResponse(request, http.StatusOK, `{}`), nil
	})))
	for _, path := range []string{
		"/api/v10/channels/1/../../users/@me",
		"/api/v10/channels/%2e%2e/users/@me",
		"/api/v10/channels/a%2Fb/messages",
		"/api/v10/webhooks/123456789012345678/token%3Fwait=true",
	} {
		if response := serve(proxy, http.MethodGet, path, "", nil); response.Code != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400", path, response.Code)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("%d rejected paths reached Discord", calls.Load())
	}
}

func TestRepeatedSlashesRouteLikeDiscord(t *testing.T) {
	var calls atomic.Int64
	var forwarded atomic.Value
	proxy := newTestProxy(t, testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls.Add(1)
		forwarded.Store(request.URL.EscapedPath())
		return jsonResponse(request, http.StatusNotFound, `{"message": "Unknown Webhook", "code": 10015}`), nil
	})))
	for range 2 {
		if response := serve(proxy, http.MethodPost, "/api//v10/webhooks/123456789012345678/token", "", nil); response.Code != http.StatusNotFound {
			t.Fatalf("status %d, want 404", response.Code)
		}
	}
	if got := forwarded.Load(); got != "/api/v10/webhooks/123456789012345678/token" {
		t.Fatalf("Discord was sent %v", got)
	}
	if calls.Load() != 1 {
		t.Fatalf("Discord was called %d times; the doubled slash hid the webhook from its guard", calls.Load())
	}
}

func TestQueuedRequestsToADeletedWebhookShortCircuit(t *testing.T) {
	var calls atomic.Int64
	proxy := newTestProxy(t, testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls.Add(1)
		time.Sleep(100 * time.Millisecond)
		return jsonResponse(request, http.StatusNotFound, `{"message": "Unknown Webhook", "code": 10015}`), nil
	})))
	statuses := make(chan int, 8)
	var senders sync.WaitGroup
	for range cap(statuses) {
		senders.Add(1)
		go func() {
			defer senders.Done()
			statuses <- serve(proxy, http.MethodPost, "/api/v10/webhooks/123456789012345678/token", "", nil).Code
		}()
	}
	senders.Wait()
	close(statuses)
	for status := range statuses {
		if status != http.StatusNotFound {
			t.Fatalf("status %d, want 404", status)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("Discord was called %d times for one deleted webhook", calls.Load())
	}
}

func TestCredentialStateOnlyFollowsCredentialRoutes(t *testing.T) {
	proxy := newTestProxy(t, testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if strings.HasSuffix(request.URL.Path, "/users/@me") {
			return jsonResponse(request, http.StatusUnauthorized, `{"message": "401: Unauthorized", "code": 0}`), nil
		}
		return jsonResponse(request, http.StatusOK, `{}`), nil
	})))
	webhook, authorization := "/api/v10/webhooks/123456789012345678/token", "Bot "+fakeBotToken
	state, err := proxy.client(identify(authorization))
	if err != nil {
		t.Fatal(err)
	}
	defer state.end()

	if response := serve(proxy, http.MethodPost, webhook, authorization, nil); response.Code != http.StatusOK || state.metricsLabel() != "Unverified" {
		t.Fatalf("a webhook call vouched for the bot token: status %d, label %q", response.Code, state.metricsLabel())
	}
	if response := serve(proxy, http.MethodGet, "/api/v10/users/@me", authorization, nil); response.Code != http.StatusUnauthorized {
		t.Fatalf("users/@me answered %d, want 401", response.Code)
	}
	if response := serve(proxy, http.MethodPost, webhook, authorization, nil); response.Code != http.StatusOK {
		t.Fatalf("an invalid bot token blocked a webhook call Discord authenticates by its path token: status %d", response.Code)
	}
}

func TestRouteNormalisationCollapsesNonNumericIdentifiers(t *testing.T) {
	for path, want := range map[string]string{
		"/api/v10/applications/123456789012345678/activity-instances/i-1276580072400224306-gc-1-2":           "/applications/123456789012345678/activity-instances/!",
		"/api/v10/applications/123456789012345678/users/223456789012345678/identities/provider-9/profile":    "/applications/123456789012345678/users/!/identities/!/profile",
		"/api/v10/users/223456789012345678/application-identities/123456789012345678/xbox/provider-9/delete": "/users/!/application-identities/!/!/!/delete",
		"/api/v10/guilds/123456789012345678/templates/hgM48av5Q69A":                                          "/guilds/123456789012345678/templates/!",
		"/api/v10/channels/123456789012345678/messages/pins":                                                 "/channels/123456789012345678/messages/pins",
		"/api/v10/guilds/123456789012345678/roles/member-counts":                                             "/guilds/123456789012345678/roles/member-counts",
		"/api/v10/guilds/123456789012345678/voice-states/@me":                                                "/guilds/123456789012345678/voice-states/@me",
	} {
		if got := GetOptimisticBucketPath(path, http.MethodGet); got != want {
			t.Errorf("%s -> %s, want %s", path, got, want)
		}
	}
}

func TestMetricsLabelIdentifiesAcceptedBots(t *testing.T) {
	bot := newClientState(identify("Bot "+fakeBotToken), discordGlobalLimit, 8, newResourceBudget(16))
	if got := bot.metricsLabel(); got != "Unverified" {
		t.Fatalf("unvalidated bot label = %q", got)
	}
	bot.validity.Store(clientValid)
	if got := bot.metricsLabel(); got != "123456789012345678" {
		t.Fatalf("validated bot label = %q", got)
	}
	if got := newClientState(identify(""), discordGlobalLimit, 8, newResourceBudget(16)).metricsLabel(); got != "NoAuth" {
		t.Fatalf("no-auth label = %q", got)
	}
}

func TestChannelCreationIsIndependentPerGuild(t *testing.T) {
	proxy := newTestProxy(t, testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return testResponse(request, http.StatusCreated, testHeaders(map[string]string{
			"Via": "1.1 google", "X-RateLimit-Bucket": "create-channel", "X-RateLimit-Remaining": "0", "X-RateLimit-Reset-After": "3600",
		}), nil), nil
	})))
	create := func(guild string) (int, time.Duration) {
		started := time.Now()
		response := serve(proxy, http.MethodPost, "/api/v10/guilds/"+guild+"/channels", "Bot "+fakeBotToken, strings.NewReader("{}"))
		return response.Code, time.Since(started)
	}
	if code, _ := create("111111111111111111"); code != http.StatusCreated {
		t.Fatalf("first creation = %d", code)
	}
	if code, took := create("222222222222222222"); code != http.StatusCreated || took > 500*time.Millisecond {
		t.Fatalf("another guild waited %v behind an exhausted guild (status %d)", took, code)
	}
	if code, took := create("111111111111111111"); code != http.StatusTooManyRequests || took > 500*time.Millisecond {
		t.Fatalf("the exhausted guild got %d after %v, want an immediate 429", code, took)
	}
}

func TestBurstBeyondQueueDepthNeverStrandsHandlers(t *testing.T) {
	release := make(chan struct{})
	var calls atomic.Int64
	config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			<-release
		}
		return jsonResponse(request, http.StatusOK, `{}`), nil
	}))
	config.QueueTimeout = 2 * time.Second
	proxy := newTestProxy(t, config)
	baseline := runtime.NumGoroutine()

	var wg sync.WaitGroup
	for index := range 40 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(50+index*10)*time.Millisecond)
			defer cancel()
			request := httptest.NewRequest(http.MethodPost, "/api/v10/guilds/111111111111111111/channels", nil).WithContext(ctx)
			request.Header.Set("Authorization", "Bot "+fakeBotToken)
			proxy.ServeHTTP(httptest.NewRecorder(), request)
		}()
	}
	time.Sleep(200 * time.Millisecond)
	close(release)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("request handlers were stranded")
	}
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > baseline+2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if count := runtime.NumGoroutine(); count > baseline+2 {
		t.Fatalf("%d goroutines remain, baseline %d", count, baseline)
	}
}

func TestInteractionsBypassTheGlobalPacer(t *testing.T) {
	config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return jsonResponse(request, http.StatusOK, `{}`), nil
	}))
	config.QueueTimeout = 5 * time.Second
	config.GlobalOverrides = "123456789012345678:20"
	proxy := newTestProxy(t, config)
	timed := func(path func(int) string) time.Duration {
		started := time.Now()
		var wg sync.WaitGroup
		for index := range 10 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				serve(proxy, http.MethodPost, path(index), "Bot "+fakeBotToken, nil)
			}()
		}
		wg.Wait()
		return time.Since(started)
	}
	paced := timed(func(index int) string {
		return "/api/v10/channels/" + strconv.Itoa(123456789012345670+index) + "/messages"
	})
	if paced < 400*time.Millisecond {
		t.Fatalf("10 bot requests at 20/s finished in %v; the global pacer did not space them", paced)
	}
	unpaced := timed(func(index int) string {
		return "/api/v10/interactions/" + strconv.Itoa(223456789012345670+index) + "/token/callback"
	})
	if unpaced >= 400*time.Millisecond {
		t.Fatalf("10 interaction callbacks took %v; they must bypass the global pacer", unpaced)
	}
}

func TestRetryCaptureLeavesNoTempFiles(t *testing.T) {
	temp := t.TempDir()
	t.Setenv("TMPDIR", temp)
	body := strings.Repeat("x", 2<<20)
	for _, cancelMidUpload := range []bool{false, true} {
		config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
			if cancelMidUpload {
				_, _ = io.CopyN(io.Discard, request.Body, 3<<19)
				<-request.Context().Done()
				return nil, request.Context().Err()
			}
			_, _ = io.Copy(io.Discard, request.Body)
			return jsonResponse(request, http.StatusOK, `{}`), nil
		}))
		config.MaxRetryBodyBytes = 4 << 20
		proxy := newTestProxy(t, config)
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		request := httptest.NewRequest(http.MethodPost, "/api/v10/channels/1/messages", strings.NewReader(body)).WithContext(ctx)
		proxy.ServeHTTP(httptest.NewRecorder(), request)
		cancel()
		if leftovers, _ := filepath.Glob(filepath.Join(temp, "sluice-retry-*")); len(leftovers) != 0 {
			t.Fatalf("cancelMidUpload=%v left %v", cancelMidUpload, leftovers)
		}
	}
}

func TestOversizedBodyIsForwardedOnceAndNotRetried(t *testing.T) {
	var calls, received atomic.Int64
	config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls.Add(1)
		read, _ := io.Copy(io.Discard, request.Body)
		received.Store(read)
		return testResponse(request, http.StatusTooManyRequests, testHeaders(map[string]string{
			"Via": "1.1 google", "Retry-After": "0.05", "X-RateLimit-Scope": "user",
		}), nil), nil
	}))
	config.MaxRetryBodyBytes = 1 << 20
	proxy := newTestProxy(t, config)
	response := serve(proxy, http.MethodPost, "/api/v10/channels/1/messages", "", strings.NewReader(strings.Repeat("x", 2<<20)))
	if response.Code != http.StatusTooManyRequests || calls.Load() != 1 || received.Load() != 2<<20 {
		t.Fatalf("status %d after %d attempts carrying %d bytes; want one full attempt and the 429", response.Code, calls.Load(), received.Load())
	}
}

func TestUpstreamFailureDoesNotWedgeTheBucket(t *testing.T) {
	var calls atomic.Int64
	proxy := newTestProxy(t, testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			return nil, errors.New("connection reset by peer")
		}
		return jsonResponse(request, http.StatusOK, `{}`), nil
	})))
	if response := serve(proxy, http.MethodGet, "/api/v10/channels/1/messages", "", nil); response.Code != http.StatusBadGateway {
		t.Fatalf("reset connection = %d, want 502", response.Code)
	}
	started := time.Now()
	if response := serve(proxy, http.MethodGet, "/api/v10/channels/1/messages", "", nil); response.Code != http.StatusOK || time.Since(started) > 500*time.Millisecond {
		t.Fatalf("next request = %d after %v", response.Code, time.Since(started))
	}
	if count := proxy.invalidRequests.count(time.Now()); count != 0 {
		t.Fatalf("a transport error consumed %d invalid-request slots", count)
	}
}

func TestInvalidTokenChurnCannotDisplaceALiveBot(t *testing.T) {
	valid := "Bot " + fakeBotToken
	config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Header.Get("Authorization") == valid {
			return jsonResponse(request, http.StatusOK, `{}`), nil
		}
		return jsonResponse(request, http.StatusUnauthorized, `{"message": "401: Unauthorized", "code": 0}`), nil
	}))
	proxy := newTestProxy(t, config)
	if response := serve(proxy, http.MethodGet, "/api/v10/gateway", valid, nil); response.Code != http.StatusOK {
		t.Fatalf("valid bot = %d", response.Code)
	}
	for index := range 1000 {
		serve(proxy, http.MethodGet, "/api/v10/gateway", "Bot junk-token-"+strconv.Itoa(index), nil)
	}
	proxy.clientsMu.Lock()
	states := len(proxy.bots) + len(proxy.bearers)
	proxy.clientsMu.Unlock()
	if states > config.MaxClientStates {
		t.Fatalf("%d credential states, cap %d", states, config.MaxClientStates)
	}
	started := time.Now()
	if response := serve(proxy, http.MethodGet, "/api/v10/gateway", valid, nil); response.Code != http.StatusOK || time.Since(started) > 500*time.Millisecond {
		t.Fatalf("valid bot after churn = %d in %v", response.Code, time.Since(started))
	}
}

func TestShutdownReleasesQueuedRequests(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	proxy, err := New(testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		select {
		case started <- struct{}{}:
			<-release
		default:
		}
		return jsonResponse(request, http.StatusOK, `{}`), nil
	})))
	if err != nil {
		t.Fatal(err)
	}
	defer close(release)
	go serve(proxy, http.MethodGet, "/api/v10/channels/1/messages", "", nil)
	<-started
	queued := make(chan *httptest.ResponseRecorder, 1)
	go func() { queued <- serve(proxy, http.MethodGet, "/api/v10/channels/1/messages", "", nil) }()
	time.Sleep(50 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = proxy.Close(ctx) }()
	select {
	case response := <-queued:
		if response.Code != http.StatusServiceUnavailable || response.Header().Get("Retry-After") == "" || response.Header().Get(proxyErrorHeader) != "true" {
			t.Fatalf("queued request at shutdown = %d %v", response.Code, response.Header())
		}
	case <-time.After(time.Second):
		t.Fatal("queued request was not released at shutdown")
	}
}
