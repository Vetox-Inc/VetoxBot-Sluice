package proxy

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestShutdownCancellationDoesNotBecomeImplicitSuccess(t *testing.T) {
	started := make(chan struct{})
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		close(started)
		<-request.Context().Done()
		return nil, request.Context().Err()
	})
	proxy := newTestProxy(t, testConfig(transport))
	request := httptest.NewRequest(http.MethodGet, "/api/v10/gateway", nil)
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		proxy.ServeHTTP(response, request)
		close(done)
	}()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := proxy.Close(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("request did not finish during shutdown")
	}
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("shutdown response = %d, want 503", response.Code)
	}
}

func TestRejectsConnectAndProtocolUpgrades(t *testing.T) {
	var calls atomic.Int64
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls.Add(1)
		return testResponse(request, http.StatusOK, nil, nil), nil
	})
	proxy := newTestProxy(t, testConfig(transport))

	tests := []struct {
		name    string
		method  string
		headers http.Header
	}{
		{name: "CONNECT", method: http.MethodConnect},
		{name: "Upgrade header", method: http.MethodGet, headers: http.Header{"Upgrade": {"websocket"}}},
		{name: "Connection token", method: http.MethodGet, headers: http.Header{"Connection": {"keep-alive, UpGrAdE"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(test.method, "http://proxy.invalid/api/v10/gateway", nil)
			request.Header = test.headers.Clone()
			response := httptest.NewRecorder()
			proxy.ServeHTTP(response, request)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
			}
		})
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("rejected requests reached transport %d times", got)
	}
}

func TestShutdownRejectsNewAdmission(t *testing.T) {
	var calls atomic.Int64
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls.Add(1)
		return testResponse(request, http.StatusOK, nil, nil), nil
	})
	proxy, err := New(testConfig(transport))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := proxy.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if state, err := proxy.client(identify("Bot shutdown-token")); !errors.Is(err, errProxyClosed) {
		if err == nil {
			state.end()
		}
		t.Fatalf("direct admission after shutdown error = %v, want %v", err, errProxyClosed)
	}

	request := httptest.NewRequest(http.MethodGet, "http://proxy.invalid/api/v10/gateway", nil)
	request.Header.Set("Authorization", "Bot shutdown-token")
	response := httptest.NewRecorder()
	proxy.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status after shutdown = %d, want 503", response.Code)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("requests admitted after shutdown = %d, want 0", got)
	}
}
