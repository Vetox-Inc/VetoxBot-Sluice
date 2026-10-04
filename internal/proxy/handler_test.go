package proxy

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestProxyErrorsUseDiscordsErrorShape(t *testing.T) {
	proxy := newTestProxy(t, testConfig(nil))
	response := serve(proxy, http.MethodGet, "/api/v10/channels/%2e%2e/messages", "", nil)
	var payload struct {
		Message string `json:"message"`
		Code    *int   `json:"code"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil || payload.Code == nil || *payload.Code != 0 ||
		!strings.Contains(payload.Message, "dot segments") || response.Header().Get("Content-Type") != "application/json" ||
		response.Header().Get(proxyErrorHeader) != "true" {
		t.Fatalf("proxy error = %d %v %q, want Discord's JSON error shape", response.Code, response.Header(), response.Body.String())
	}
}

func TestCompressedDiscordErrorsStayReadable(t *testing.T) {
	var calls atomic.Int64
	var acceptEncoding atomic.Value
	discord := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		acceptEncoding.Store(request.Header.Get("Accept-Encoding"))
		writer.Header().Set("Via", "1.1 google")
		writer.Header().Set("Content-Type", "application/json")
		writer.Header().Set("Content-Encoding", "gzip")
		writer.WriteHeader(http.StatusNotFound)
		compressed := gzip.NewWriter(writer)
		_, _ = compressed.Write([]byte(`{"message": "Unknown Webhook", "code": 10015}`))
		_ = compressed.Close()
	}))
	defer discord.Close()
	config := testConfig(nil)
	config.DiscordURL = discord.URL
	proxy := newTestProxy(t, config)

	for range 2 {
		request := httptest.NewRequest(http.MethodPost, "/api/v10/webhooks/123456789012345678/token", nil)
		request.Header.Set("Accept-Encoding", "br")
		response := httptest.NewRecorder()
		proxy.ServeHTTP(response, request)
		if response.Code != http.StatusNotFound || response.Header().Get("Content-Encoding") != "" || !strings.Contains(response.Body.String(), "10015") {
			t.Fatalf("client got %d %v %q, want Discord's 404 decoded", response.Code, response.Header(), response.Body.String())
		}
	}
	if got := acceptEncoding.Load(); got != "gzip" {
		t.Fatalf("Discord was asked for %q, want gzip requested by Sluice itself", got)
	}
	if calls.Load() != 1 {
		t.Fatalf("Discord was called %d times; the compressed 10015 hid the deleted webhook", calls.Load())
	}
}

func TestEveryRequestCarriesADiscordBotUserAgent(t *testing.T) {
	var mu sync.Mutex
	var agents []string
	proxy := newTestProxy(t, testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		mu.Lock()
		agents = append(agents, request.Header.Get("User-Agent"))
		mu.Unlock()
		return jsonResponse(request, http.StatusOK, `{}`), nil
	})))
	const discordJS = "DiscordBot (https://discord.js.org, 2.6.3) Node.js/v24.1.0"
	for _, agent := range []string{"", "python-requests/2.32", discordJS} {
		request := httptest.NewRequest(http.MethodGet, "/api/v10/gateway", nil)
		if agent != "" {
			request.Header.Set("User-Agent", agent)
		}
		proxy.ServeHTTP(httptest.NewRecorder(), request)
	}
	want := []string{userAgent(), userAgent() + " python-requests/2.32", discordJS}
	if strings.Join(agents, "|") != strings.Join(want, "|") {
		t.Fatalf("Discord saw User-Agents %q, want %q", agents, want)
	}
}

func TestUnsupportedAuthorizationSchemeIsRefusedLocally(t *testing.T) {
	var calls atomic.Int64
	proxy := newTestProxy(t, testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls.Add(1)
		return jsonResponse(request, http.StatusOK, `{}`), nil
	})))
	refused := serve(proxy, http.MethodGet, "/api/v10/users/@me", "Token abc", nil)
	if refused.Code != http.StatusUnauthorized || refused.Header().Get("Generated-By-Proxy") != "true" || calls.Load() != 0 {
		t.Fatalf("unsupported scheme got %d %v after %d upstream calls, want a local 401", refused.Code, refused.Header(), calls.Load())
	}
	if webhook := serve(proxy, http.MethodPost, "/api/v10/webhooks/123456789012345678/token", "Token abc", nil); webhook.Code != http.StatusOK || calls.Load() != 1 {
		t.Fatalf("a webhook call, authenticated by its path token, got %d after %d calls", webhook.Code, calls.Load())
	}
}

func TestOutdatedAPIVersionsAreNamed(t *testing.T) {
	for path, want := range map[string]string{
		"/api/v10/gateway":                      "",
		"/api/v9/gateway":                       "",
		"/api/v8/gateway":                       "v8",
		"/api/v6/gateway":                       "v6",
		"/api/v006/gateway":                     "v6",
		"/api/v3/gateway":                       "v3",
		"/api/gateway":                          "none",
		"/api/channels/1/messages":              "none",
		"/api":                                  "none",
		"/api/v99999999999999999999999/gateway": "",
	} {
		if got := outdatedAPIVersion(path); got != want {
			t.Errorf("%s -> %q, want %q", path, got, want)
		}
	}
}

func TestOutdatedAPIVersionIsCountedAndWarnedOnce(t *testing.T) {
	var output bytes.Buffer
	previous := logger
	SetLogger(NewLogger(&output, slog.LevelInfo, "text"))
	t.Cleanup(func() { logger = previous })
	proxy := newTestProxy(t, testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return jsonResponse(request, http.StatusOK, `{}`), nil
	})))
	before := testutil.ToFloat64(DeprecatedAPIRequests.WithLabelValues("v6"))
	for _, path := range []string{"/api/v6/gateway", "/api/v6/gateway", "/api/v10/gateway"} {
		serve(proxy, http.MethodGet, path, "", nil)
	}
	if got := testutil.ToFloat64(DeprecatedAPIRequests.WithLabelValues("v6")) - before; got != 2 {
		t.Fatalf("v6 requests counted %v times, want 2", got)
	}
	if warnings := strings.Count(output.String(), "deprecated or discontinued"); warnings != 1 {
		t.Fatalf("logged %d warnings for v6, want 1:\n%s", warnings, output.String())
	}
}

func TestFollowUpsUnderAnAcceptedBotAreInteractions(t *testing.T) {
	proxy := newTestProxy(t, testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return jsonResponse(request, http.StatusOK, `{}`), nil
	})))
	followUp := "/api/v10/webhooks/123456789012345678/a-future-token-format/messages/@original"
	if proxy.applications.followUp(followUp) {
		t.Fatal("a follow-up was recognised before Discord accepted its bot")
	}
	serve(proxy, http.MethodGet, "/api/v10/users/@me", "Bot "+fakeBotToken, nil)
	if !proxy.applications.followUp(followUp) {
		t.Fatal("a follow-up under an accepted bot's application ID was not recognised")
	}
	if proxy.applications.followUp("/api/v10/webhooks/223456789012345678/token") {
		t.Fatal("an ordinary webhook was taken for an interaction follow-up")
	}
}

func TestFollowUpsAreRecognisedAfterALaterSuccess(t *testing.T) {
	proxy := newTestProxy(t, testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if strings.Contains(request.URL.Path, "/channels/") {
			return jsonResponse(request, http.StatusNotFound, `{"message": "Unknown Channel", "code": 10003}`), nil
		}
		return jsonResponse(request, http.StatusOK, `{}`), nil
	})))
	followUp := "/api/v10/webhooks/123456789012345678/a-future-token-format"
	serve(proxy, http.MethodGet, "/api/v10/channels/1", "Bot "+fakeBotToken, nil)
	serve(proxy, http.MethodGet, "/api/v10/users/@me", "Bot "+fakeBotToken, nil)
	if !proxy.applications.followUp(followUp) {
		t.Fatal("a bot validated by a 404 was never recorded, even after a 200")
	}
}

func TestServerErrorsDoNotJudgeACredential(t *testing.T) {
	var failed atomic.Bool
	proxy := newTestProxy(t, testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if failed.CompareAndSwap(false, true) {
			return jsonResponse(request, http.StatusServiceUnavailable, `{}`), nil
		}
		return jsonResponse(request, http.StatusOK, `{}`), nil
	})))
	bot := "Bot " + fakeBotToken
	state, err := proxy.client(identify(bot))
	if err != nil {
		t.Fatal(err)
	}
	defer state.end()
	serve(proxy, http.MethodGet, "/api/v10/users/@me", bot, nil)
	if state.validity.Load() != clientUnknown || proxy.applications.followUp("/api/v10/webhooks/123456789012345678/token") {
		t.Fatal("a Discord 503 vouched for the bot token")
	}
	serve(proxy, http.MethodGet, "/api/v10/users/@me", bot, nil)
	if state.validity.Load() != clientValid {
		t.Fatal("a Discord 200 did not validate the bot token")
	}
}

func TestClientAuthSecretGuardsThePublicHandler(t *testing.T) {
	var calls atomic.Int64
	var leaked atomic.Bool
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls.Add(1)
		if request.Header.Get(clientAuthHeader) != "" {
			leaked.Store(true)
		}
		return jsonResponse(request, http.StatusOK, `{}`), nil
	})
	config := testConfig(transport)
	config.ClientAuthSecret = strings.Repeat("s", 32)
	proxy := newTestProxy(t, config)
	public := proxy.PublicHandler()
	call := func(handler http.Handler, path, secret string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		if secret != "" {
			request.Header.Set(clientAuthHeader, secret)
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response
	}

	for _, secret := range []string{"", strings.Repeat("t", 32)} {
		if refused := call(public, "/api/v10/gateway", secret); refused.Code != http.StatusForbidden || refused.Header().Get(proxyErrorHeader) != "true" || calls.Load() != 0 {
			t.Fatalf("secret %q got %d after %d upstream calls, want a local 403", secret, refused.Code, calls.Load())
		}
	}
	if health := call(public, "/sluice/healthz", ""); health.Code != http.StatusOK {
		t.Fatalf("health check without the secret got %d", health.Code)
	}
	if accepted := call(public, "/api/v10/gateway", config.ClientAuthSecret); accepted.Code != http.StatusOK || calls.Load() != 1 {
		t.Fatalf("the right secret got %d after %d upstream calls", accepted.Code, calls.Load())
	}
	if peer := call(proxy, "/api/v10/gateway", ""); peer.Code != http.StatusOK {
		t.Fatalf("a cluster peer, authenticated by mutual TLS, got %d", peer.Code)
	}

	open := newTestProxy(t, testConfig(transport))
	call(open.PublicHandler(), "/api/v10/gateway", "a-secret-meant-for-another-proxy")
	if leaked.Load() {
		t.Fatal("X-Sluice-Auth reached Discord")
	}
}

func TestDrainFailsHealthButFinishesAdmittedRequests(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	proxy := newTestProxy(t, testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		once.Do(func() {
			close(started)
			<-release
		})
		return jsonResponse(request, http.StatusOK, `{}`), nil
	})))
	result := make(chan *httptest.ResponseRecorder, 1)
	go func() { result <- serve(proxy, http.MethodGet, "/api/v10/gateway", "", nil) }()
	<-started
	if err := proxy.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if health := serve(proxy, http.MethodGet, "/sluice/healthz", "", nil); health.Code != http.StatusServiceUnavailable {
		t.Fatalf("health while draining = %d, want 503", health.Code)
	}
	close(release)
	if response := <-result; response.Code != http.StatusOK {
		t.Fatalf("a request admitted before the drain got %d, want 200", response.Code)
	}
}

func TestLegacyHealthAliasWarnsOnce(t *testing.T) {
	var output bytes.Buffer
	previous := logger
	SetLogger(NewLogger(&output, slog.LevelInfo, "text"))
	t.Cleanup(func() { logger = previous })
	proxy := newTestProxy(t, testConfig(nil))
	for range 2 {
		if response := serve(proxy, http.MethodGet, "/nirn/healthz", "", nil); response.Code != http.StatusOK {
			t.Fatalf("/nirn/healthz = %d, want 200", response.Code)
		}
	}
	if warnings := strings.Count(output.String(), "/nirn/healthz is deprecated"); warnings != 1 {
		t.Fatalf("logged %d deprecation warnings, want 1", warnings)
	}
}
