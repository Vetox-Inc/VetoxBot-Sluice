package proxy

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// fakeBotToken has a bot token's shape without appearing as a literal secret scanners flag.
var fakeBotToken = strings.Join([]string{"MTIzNDU2Nzg5MDEyMzQ1Njc4", "GAbCdE", strings.Repeat("x", 38)}, ".")

func TestLoggerRedactsCredentialsEverywhere(t *testing.T) {
	const pathSecret = "short-secret"
	for _, format := range []string{"text", "json"} {
		for _, endpoint := range []string{"webhooks", "interactions"} {
			t.Run(format+"/"+endpoint, func(t *testing.T) {
				path := "/api/v10/" + endpoint + "/123456789012345678/" + pathSecret + "/callback"
				var output bytes.Buffer
				testLogger := NewLogger(&output, slog.LevelDebug, format)
				testLogger.With("route", path).WithGroup("request").Warn("request failed: "+path,
					"path", path,
					"error", errors.New("upstream "+path),
					"authorization", "Bot "+fakeBotToken,
					"detail", struct{ Token string }{fakeBotToken},
				)
				for _, secret := range []string{pathSecret, fakeBotToken} {
					if strings.Contains(output.String(), secret) {
						t.Fatalf("log output exposed %q:\n%s", secret, output.String())
					}
				}
				if !strings.Contains(output.String(), ":token") {
					t.Fatalf("log output lost the redaction marker:\n%s", output.String())
				}
			})
		}
	}
}

func TestErrorLogsIncrementErrorCounter(t *testing.T) {
	before := testutil.ToFloat64(ErrorCounter)
	testLogger := NewLogger(&bytes.Buffer{}, slog.LevelInfo, "text")
	testLogger.Warn("not an error")
	testLogger.Error("an error")
	testLogger.With("component", "test").Error("another error")
	if got := testutil.ToFloat64(ErrorCounter) - before; got != 2 {
		t.Fatalf("error counter increased by %v, want 2", got)
	}
}

func TestMetricsNamespaceNamesEveryMetric(t *testing.T) {
	suffixes := []string{
		"cloudflare_blocked", "cloudflare_blocks_total", "deprecated_api_requests_total", "edge_refusals_total", "error",
		"failures_total", "invalid_requests", "open_connections", "queue_wait_seconds", "requests", "requests_routed_error",
		"requests_routed_received", "requests_routed_sent", "webhook_short_circuits_total",
	}
	for _, namespace := range []string{"nirn_proxy", DefaultMetricsNamespace} {
		t.Run(namespace, func(t *testing.T) {
			var want []string
			for _, suffix := range suffixes {
				want = append(want, namespace+"_"+suffix)
			}
			set := newMetricSet(namespace)
			set.errors.Inc()
			set.failures.WithLabelValues("test").Inc()
			set.requests.WithLabelValues("GET", "200 OK", "/gateway", "NoAuth").Observe(0.1)
			set.openConnections.WithLabelValues("GET", "/gateway").Inc()
			set.queueWait.WithLabelValues("GET", "/gateway").Observe(0.1)
			set.deprecatedAPI.WithLabelValues("none").Inc()
			families, err := set.registry.Gather()
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, family := range families {
				name := family.GetName()
				if strings.HasPrefix(name, "go_") || strings.HasPrefix(name, "process_") {
					continue
				}
				got = append(got, name)
			}
			sort.Strings(got)
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Fatalf("metric names = %v, want %v", got, want)
			}
		})
	}
}

func TestRouteLabelLimiterBoundsConcurrentCardinality(t *testing.T) {
	const (
		limit  = 8
		routes = 128
	)
	limiter := newRouteLabelLimiter(limit)
	type result struct{ route, label string }
	results := make(chan result, routes)

	var wg sync.WaitGroup
	for i := range routes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			route := fmt.Sprintf("/route/%d", i)
			results <- result{route: route, label: limiter.label(route)}
		}()
	}
	wg.Wait()
	close(results)

	labels := make(map[string]struct{})
	accepted := ""
	for result := range results {
		labels[result.label] = struct{}{}
		if result.label != overflowMetricsRouteLabel {
			accepted = result.route
		}
	}
	if len(labels) > limit {
		t.Fatalf("route-label cardinality exceeded cap: got %d, cap %d", len(labels), limit)
	}
	if _, ok := labels[overflowMetricsRouteLabel]; !ok {
		t.Fatal("overflowing routes were not collapsed")
	}
	if accepted == "" || limiter.label(accepted) != accepted {
		t.Fatal("an accepted route label did not remain stable")
	}
}

func TestMetricsMethodLabelBoundsUntrustedMethods(t *testing.T) {
	if got := metricsMethodLabel(http.MethodPost); got != http.MethodPost {
		t.Fatalf("POST label = %q", got)
	}
	if got := metricsMethodLabel("attacker-controlled-method"); got != "OTHER" {
		t.Fatalf("unknown method label = %q", got)
	}
}

func TestRouteLabelLimiterRejectsOversizedLabel(t *testing.T) {
	limiter := newRouteLabelLimiter(8)
	if got := limiter.label("/" + string(make([]byte, maxMetricsRouteLabelBytes))); got != overflowMetricsRouteLabel {
		t.Fatalf("oversized route label = %q", got)
	}
	if len(limiter.seen) != 1 {
		t.Fatalf("oversized route was retained; labels=%d", len(limiter.seen))
	}
}

func TestDisabledMetricsSkipRouteLabelCollection(t *testing.T) {
	previous := metricsRoutes
	metricsRoutes = newRouteLabelLimiter(maxMetricsRouteLabels)
	defer func() { metricsRoutes = previous }()
	config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return testResponse(request, http.StatusOK, nil, nil), nil
	}))
	config.EnableMetrics = false
	proxy := newTestProxy(t, config)

	response := httptest.NewRecorder()
	proxy.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v10/channels/123/messages", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("response status = %d, want 200", response.Code)
	}
	metricsRoutes.mu.Lock()
	labels := len(metricsRoutes.seen)
	metricsRoutes.mu.Unlock()
	if labels != 1 {
		t.Fatalf("disabled metrics retained %d route labels, want only overflow sentinel", labels)
	}
}
