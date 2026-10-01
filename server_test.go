package main

import (
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Vetox-Inc/VetoxBot-Sluice/internal/proxy"
)

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

// startProxyServer serves a proxy with transport as its Discord, behind server timeouts of
// lifetime.
func startProxyServer(t *testing.T, transport roundTrip, lifetime time.Duration) (*proxy.Proxy, runningServer, string) {
	t.Helper()
	serverProxy, err := proxy.New(proxy.Config{
		UpstreamTimeout:     time.Second,
		QueueTimeout:        time.Second,
		MaxBearerClients:    8,
		MaxBucketStates:     64,
		MaxClientStates:     8,
		MaxInFlightRequests: 8,
		MaxQueueDepth:       8,
		Transport:           transport,
	})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	running := runningServer{name: "proxy", server: newProxyServer(listener.Addr().String(), serverProxy.PublicHandler(), lifetime), listener: listener}
	go func() { _ = running.server.Serve(listener) }()
	t.Cleanup(func() {
		_ = running.server.Close()
		_ = shutdownGracefully(serverProxy, nil, time.Second)
	})
	return serverProxy, running, "http://" + listener.Addr().String()
}

func discordOK(request *http.Request) *http.Response {
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Via": {"1.1 google"}}, Body: http.NoBody, Request: request}
}

func TestShutdownFinishesRequestsInFlight(t *testing.T) {
	started := make(chan struct{})
	var once sync.Once
	serverProxy, running, address := startProxyServer(t, func(request *http.Request) (*http.Response, error) {
		once.Do(func() { close(started) })
		time.Sleep(300 * time.Millisecond)
		return discordOK(request), nil
	}, 10*time.Second)
	status := make(chan int, 1)
	go func() {
		response, err := http.Get(address + "/api/v10/gateway")
		if err != nil {
			status <- 0
			return
		}
		_ = response.Body.Close()
		status <- response.StatusCode
	}()
	<-started
	if err := shutdownGracefully(serverProxy, []runningServer{running}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if got := <-status; got != http.StatusOK {
		t.Fatalf("a request in flight at shutdown got %d, want 200", got)
	}
}

type slowUpload struct{ remaining int }

func (u *slowUpload) Read(target []byte) (int, error) {
	if u.remaining == 0 {
		return 0, io.EOF
	}
	time.Sleep(40 * time.Millisecond)
	size := min(len(target), 64<<10, u.remaining)
	u.remaining -= size
	return size, nil
}

func TestUploadOutlastsTheServerTimeouts(t *testing.T) {
	var received atomic.Int64
	_, _, address := startProxyServer(t, func(request *http.Request) (*http.Response, error) {
		read, err := io.Copy(io.Discard, request.Body)
		if err != nil {
			return nil, err
		}
		received.Store(read)
		return discordOK(request), nil
	}, 300*time.Millisecond)
	// 2 MiB at 64 KiB per 40 ms takes about 1.3 seconds, four server timeouts.
	request, err := http.NewRequest(http.MethodPost, address+"/api/v10/channels/1/messages", &slowUpload{remaining: 2 << 20})
	if err != nil {
		t.Fatal(err)
	}
	request.ContentLength = 2 << 20
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("upload failed: %v", err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK || received.Load() != 2<<20 {
		t.Fatalf("upload got %d with %d bytes delivered, want 200 with %d", response.StatusCode, received.Load(), 2<<20)
	}
}
