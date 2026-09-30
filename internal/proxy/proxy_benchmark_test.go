package proxy

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func benchmarkProxy(b *testing.B, enableMetrics bool) *Proxy {
	b.Helper()
	authorization := "Bot benchmark-token"
	serverProxy, err := New(Config{
		UpstreamTimeout:      time.Second,
		QueueTimeout:         time.Second,
		EnableMetrics:        enableMetrics,
		GlobalOverrides:      identify(authorization).fingerprint() + ":1000000000",
		MaxBearerClients:     16,
		MaxBucketStates:      4096,
		MaxClientStates:      64,
		MaxInFlightRequests:  4096,
		MaxQueueDepth:        1024,
		MaxRetryBodyBytes:    1 << 20,
		MaxRetryCaptureBytes: 8 << 20,
		Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			return &http.Response{
				Status:        "200 OK",
				StatusCode:    http.StatusOK,
				Header:        http.Header{"X-RateLimit-Bucket": {"messages"}, "X-RateLimit-Remaining": {"9"}},
				Body:          io.NopCloser(strings.NewReader("ok")),
				ContentLength: 2,
				Request:       request,
			}, nil
		}),
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = serverProxy.Close(b.Context()) })
	return serverProxy
}

func BenchmarkProxyHandlerHotPath(b *testing.B) {
	benchmarkProxyHandlerHotPath(b, true)
}

func BenchmarkProxyHandlerMetricsDisabled(b *testing.B) {
	benchmarkProxyHandlerHotPath(b, false)
}

func benchmarkProxyHandlerHotPath(b *testing.B, enableMetrics bool) {
	serverProxy := benchmarkProxy(b, enableMetrics)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		request := httptest.NewRequest(http.MethodGet, "/api/v10/channels/123456789012345678/messages", nil)
		request.Header.Set("Authorization", "Bot benchmark-token")
		response := httptest.NewRecorder()
		serverProxy.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			b.Fatalf("unexpected status %d", response.Code)
		}
	}
}

func BenchmarkProxyBurst(b *testing.B) {
	benchmarkProxyBurst(b, true)
}

func BenchmarkProxyBurstMetricsDisabled(b *testing.B) {
	benchmarkProxyBurst(b, false)
}

func benchmarkProxyBurst(b *testing.B, enableMetrics bool) {
	serverProxy := benchmarkProxy(b, enableMetrics)
	b.ReportAllocs()
	b.SetParallelism(4)
	b.ResetTimer()
	b.RunParallel(func(parallel *testing.PB) {
		requestNumber := 0
		for parallel.Next() {
			path := fmt.Sprintf("/api/v10/channels/%018d/messages", 123456789012345678+requestNumber%64)
			request := httptest.NewRequest(http.MethodGet, path, nil)
			request.Header.Set("Authorization", "Bot benchmark-token")
			response := httptest.NewRecorder()
			serverProxy.ServeHTTP(response, request)
			if response.Code != http.StatusOK {
				b.Errorf("unexpected status %d", response.Code)
			}
			requestNumber++
		}
	})
}

func BenchmarkClientAdmissionExistingParallel(b *testing.B) {
	serverProxy := benchmarkProxy(b, false)
	identified := identify("Bot benchmark-token")
	state, err := serverProxy.client(identified)
	if err != nil {
		b.Fatal(err)
	}
	state.end()
	b.ReportAllocs()
	b.RunParallel(func(parallel *testing.PB) {
		for parallel.Next() {
			state, err := serverProxy.client(identified)
			if err != nil {
				b.Error(err)
				continue
			}
			state.end()
		}
	})
}

func BenchmarkRouteLabelExistingParallel(b *testing.B) {
	limiter := newRouteLabelLimiter(maxMetricsRouteLabels)
	const route = "/channels/:channel_id/messages"
	limiter.label(route)
	b.ReportAllocs()
	b.RunParallel(func(parallel *testing.PB) {
		for parallel.Next() {
			if got := limiter.label(route); got != route {
				b.Errorf("label = %q", got)
			}
		}
	})
}
