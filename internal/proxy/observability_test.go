package proxy

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
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

func TestRedactionKeepsRouteLabelsReadable(t *testing.T) {
	for value, want := range map[string]string{
		"/webhooks/!/!":                         "/webhooks/!/!",
		"/interactions/!/!/callback":            "/interactions/!/!/callback",
		"/webhooks/123/!starts-with-the-mark":   "/webhooks/123/:token",
		"/webhooks/123/path-secret/messages/45": "/webhooks/123/:token/messages/45",
	} {
		if got := redactLogSecrets(value); got != want {
			t.Errorf("redactLogSecrets(%q) = %q, want %q", value, got, want)
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

// metricSuffixes names every metric of Sluice's own, without its namespace.
var metricSuffixes = []string{
	"build_info", "cloudflare_blocked", "cloudflare_blocks_total", "deprecated_api_requests_total", "edge_refusals_total",
	"error", "failures_total", "global_limit", "invalid_requests", "invalid_requests_limit", "open_connections", "queue_wait_seconds",
	"requests",
	"requests_routed_error", "requests_routed_received", "requests_routed_sent", "resource_limit", "resource_usage",
	"warnings_total", "webhook_short_circuits_total",
}

func TestMetricsNamespaceNamesEveryMetric(t *testing.T) {
	// The resource gauges read the running proxy.
	newTestProxy(t, testConfig(nil))
	for _, namespace := range []string{"nirn_proxy", DefaultMetricsNamespace} {
		t.Run(namespace, func(t *testing.T) {
			var want []string
			for _, suffix := range metricSuffixes {
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

// gaugesByLabel gathers one gauge family and maps the value of label to each gauge's value.
func gaugesByLabel(t *testing.T, name, label string) map[string]float64 {
	t.Helper()
	families, err := metricsRegistry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	values := make(map[string]float64)
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, pair := range metric.GetLabel() {
				if pair.GetName() == label {
					values[pair.GetValue()] = metric.GetGauge().GetValue()
				}
			}
		}
	}
	return values
}

func TestResourceUseIsExportedBesideItsLimit(t *testing.T) {
	arrived, release := make(chan struct{}, 1), make(chan struct{})
	config := testConfig(holdUpstream(arrived, release))
	proxy := newTestProxy(t, config)
	done := make(chan struct{})
	go func() {
		defer close(done)
		serve(proxy, http.MethodGet, "/api/v10/channels/111111111111111111/messages", "Bot "+fakeBotToken, nil)
	}()
	awaitSignal(t, arrived, "the request to reach the upstream")

	usage := gaugesByLabel(t, "sluice_resource_usage", "resource")
	limits := gaugesByLabel(t, "sluice_resource_limit", "resource")
	for resource, want := range map[string][2]float64{
		"client_states":       {1, float64(config.MaxClientStates)},
		"bearer_count":        {0, float64(config.MaxBearerClients)},
		"bucket_states":       {1, float64(config.MaxBucketStates)},
		"in_flight_requests":  {1, float64(config.MaxInFlightRequests)},
		"retry_capture_bytes": {0, float64(config.MaxRetryCaptureBytes)},
	} {
		if got, exported := usage[resource]; !exported || got != want[0] {
			t.Errorf("usage of %s = %v (exported: %t), want %v", resource, got, exported, want[0])
		}
		if got, exported := limits[resource]; !exported || got != want[1] {
			t.Errorf("limit of %s = %v (exported: %t), want %v", resource, got, exported, want[1])
		}
	}
	if len(usage) != 5 || len(limits) != 5 {
		t.Errorf("exported %d usages and %d limits, want 5 of each: %v %v", len(usage), len(limits), usage, limits)
	}

	close(release)
	awaitSignal(t, done, "the held request to finish")
	if got := gaugesByLabel(t, "sluice_resource_usage", "resource")["in_flight_requests"]; got != 0 {
		t.Errorf("in-flight requests after the request finished = %v, want 0", got)
	}
}

func TestBuildInfoNamesTheVersion(t *testing.T) {
	versions := gaugesByLabel(t, "sluice_build_info", "version")
	if len(versions) != 1 || versions[Version] != 1 {
		t.Fatalf("build_info by version = %v, want one gauge of 1 for %q", versions, Version)
	}
	if runtimes := gaugesByLabel(t, "sluice_build_info", "goversion"); runtimes[runtime.Version()] != 1 {
		t.Fatalf("build_info by goversion = %v, want %q", runtimes, runtime.Version())
	}
}
