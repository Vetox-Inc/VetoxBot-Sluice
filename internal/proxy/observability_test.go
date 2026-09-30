package proxy

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
)

func TestGlobalHookRedactsStructuredCredentialPaths(t *testing.T) {
	for _, endpoint := range []string{"webhooks", "interactions"} {
		t.Run(endpoint, func(t *testing.T) {
			const secret = "short-secret"
			path := "/api/v10/" + endpoint + "/123456789012345678/" + secret + "/callback"
			entry := logrus.NewEntry(logrus.New())
			entry.Message = "request failed: " + path
			entry.Data = logrus.Fields{
				"path":          path,
				"route":         path,
				logrus.ErrorKey: errors.New("upstream " + path),
			}
			if err := (&GlobalHook{}).Fire(entry); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(entry.Message, secret) {
				t.Fatalf("message exposed credential: %q", entry.Message)
			}
			for field, value := range entry.Data {
				if strings.Contains(fmt.Sprint(value), secret) {
					t.Fatalf("field %q exposed credential: %v", field, value)
				}
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
