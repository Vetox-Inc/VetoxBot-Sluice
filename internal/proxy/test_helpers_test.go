package proxy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type blockingBody struct {
	once    sync.Once
	release chan struct{}
}

func newBlockingBody() *blockingBody {
	return &blockingBody{release: make(chan struct{})}
}

func (b *blockingBody) Read([]byte) (int, error) {
	<-b.release
	return 0, io.EOF
}

func (b *blockingBody) Close() error {
	b.once.Do(func() { close(b.release) })
	return nil
}

func testResponse(request *http.Request, status int, header http.Header, body io.ReadCloser) *http.Response {
	if header == nil {
		header = make(http.Header)
	}
	if body == nil {
		body = io.NopCloser(strings.NewReader("ok"))
	}
	return &http.Response{
		Status:     fmt.Sprintf("%d %s", status, http.StatusText(status)),
		StatusCode: status,
		Header:     header,
		Body:       body,
		Request:    request,
	}
}

func testConfig(transport http.RoundTripper) Config {
	return Config{
		UpstreamTimeout:      time.Second,
		QueueTimeout:         time.Second,
		MaxBearerClients:     8,
		MaxBucketStates:      256,
		MaxClientStates:      16,
		MaxInFlightRequests:  64,
		MaxQueueDepth:        8,
		MaxRetryBodyBytes:    1 << 20,
		MaxRetryCaptureBytes: 8 << 20,
		EnableMetrics:        true,
		Transport:            transport,
	}
}

func newTestProxy(t *testing.T, config Config) *Proxy {
	t.Helper()
	proxy, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := proxy.Close(ctx); err != nil {
			t.Errorf("close proxy: %v", err)
		}
	})
	return proxy
}

func scheduledRequest(t *testing.T, ctx context.Context, state *clientState, method, path string, body io.Reader, interaction bool) *http.Request {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, method, "https://discord.com"+path, body)
	if err != nil {
		t.Fatal(err)
	}
	bucketPath := GetOptimisticBucketPath(path, method)
	majorKey := majorParameter(path)
	metadata := &requestMetadata{
		state:         state,
		routeHash:     routeHash(method, bucketPath, majorKey),
		bucketPath:    bucketPath,
		metricsMethod: metricsMethodLabel(method),
		metricsPath:   bucketPath,
		majorKey:      majorKey,
		interaction:   interaction,
	}
	return request.WithContext(context.WithValue(request.Context(), requestMetadataContextKey, metadata))
}

func waitForGateWaiters(t *testing.T, gate *fifoGate, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		gate.mu.Lock()
		got := gate.waiters.Len()
		gate.mu.Unlock()
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("gate has %d waiters, want %d", got, want)
		}
		time.Sleep(100 * time.Microsecond)
	}
}

func mustBucket(t *testing.T, state *clientState, routeHash uint64) *bucketState {
	t.Helper()
	bucket, err := state.bucket(routeHash)
	if err != nil {
		t.Fatal(err)
	}
	return bucket
}

func testHeaders(values map[string]string) http.Header {
	header := make(http.Header, len(values))
	for name, value := range values {
		header.Set(name, value)
	}
	return header
}
