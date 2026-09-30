package proxy

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	clientSweepInterval = 5 * time.Minute
	maxPeerHops         = 1
)

// Config controls request scheduling, resource bounds, transport, and metrics.
type Config struct {
	OutboundIP           string
	UpstreamTimeout      time.Duration
	QueueTimeout         time.Duration
	DisableHTTP2         bool
	Disable401Lock       bool
	EnableMetrics        bool
	GlobalOverrides      string
	MaxBearerClients     int
	MaxBucketStates      int
	MaxClientStates      int
	MaxInFlightRequests  int
	MaxQueueDepth        int
	MaxRetryBodyBytes    int64
	MaxRetryCaptureBytes int64
	InvalidRequestLimit  int
	Transport            http.RoundTripper
}

type requestContextKey uint8

const (
	requestMetadataContextKey requestContextKey = iota
	peerTargetContextKey
)

type requestMetadata struct {
	state         *clientState
	routeHash     uint64
	bucketPath    string
	metricsMethod string
	metricsPath   string
	majorKey      string
	interaction   bool
}

type peerTarget struct {
	address string
	hop     int
}

type Proxy struct {
	config Config

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	clientsMu sync.Mutex
	bots      map[[sha256.Size]byte]*clientState
	bearers   map[[sha256.Size]byte]*clientState
	noAuth    *clientState

	globalOverrides map[string]uint
	invalidRequests *invalidRequestGuard
	bucketSlots     *resourceBudget
	inFlight        *resourceBudget
	retryCapture    *resourceBudget

	transport    http.RoundTripper
	discordProxy *httputil.ReverseProxy
	peerProxy    *httputil.ReverseProxy
	discordURL   *url.URL

	clusterMu           sync.RWMutex
	clusterIndexMu      sync.Mutex
	cluster             clusterHandle
	clusterJoining      bool
	clusterJoinDone     chan struct{}
	peerTransport       *http.Transport
	maxClusterNodes     int
	clusterOverCapacity atomic.Bool
	localAddr           string
	routes              atomic.Pointer[routeTable]

	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
}

type resourceBudget struct {
	limit int64
	used  atomic.Int64
}

func newResourceBudget(limit int64) *resourceBudget {
	return &resourceBudget{limit: limit}
}

func (b *resourceBudget) reserve(size int64) bool {
	if size <= 0 {
		return true
	}
	for {
		used := b.used.Load()
		if used > b.limit || size > b.limit-used {
			return false
		}
		if b.used.CompareAndSwap(used, used+size) {
			return true
		}
	}
}

func (b *resourceBudget) release(size int64) {
	if size > 0 {
		b.used.Add(-size)
	}
}

func New(config Config) (*Proxy, error) {
	if config.UpstreamTimeout <= 0 {
		return nil, fmt.Errorf("upstream timeout must be positive")
	}
	if config.QueueTimeout <= 0 {
		return nil, fmt.Errorf("queue timeout must be positive")
	}
	if config.MaxBearerClients <= 0 {
		return nil, fmt.Errorf("max bearer clients must be positive")
	}
	if config.MaxBucketStates <= 0 {
		return nil, fmt.Errorf("max bucket states must be positive")
	}
	if config.MaxClientStates <= 0 {
		return nil, fmt.Errorf("max client states must be positive")
	}
	if config.MaxInFlightRequests <= 0 {
		return nil, fmt.Errorf("max in-flight requests must be positive")
	}
	if config.MaxQueueDepth <= 0 {
		return nil, fmt.Errorf("max queue depth must be positive")
	}
	if config.MaxRetryBodyBytes < 0 {
		return nil, fmt.Errorf("max retry body bytes cannot be negative")
	}
	if config.MaxRetryCaptureBytes < 0 {
		return nil, fmt.Errorf("max retry capture bytes cannot be negative")
	}
	if config.InvalidRequestLimit < 0 {
		return nil, fmt.Errorf("invalid request limit cannot be negative")
	}
	invalidLimit := config.InvalidRequestLimit
	if invalidLimit == 0 {
		invalidLimit = InvalidRequestSafetyLimit
	}

	overrides, err := parseGlobalOverrides(config.GlobalOverrides)
	if err != nil {
		return nil, err
	}
	transport := config.Transport
	if transport == nil {
		transport, err = newHTTPTransport(config.OutboundIP, config.DisableHTTP2)
		if err != nil {
			return nil, err
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	discordURL, _ := url.Parse("https://discord.com")
	p := &Proxy{
		config:          config,
		ctx:             ctx,
		cancel:          cancel,
		bots:            make(map[[sha256.Size]byte]*clientState),
		bearers:         make(map[[sha256.Size]byte]*clientState),
		globalOverrides: overrides,
		invalidRequests: newInvalidRequestGuard(invalidLimit, invalidRequestWindow),
		bucketSlots:     newResourceBudget(int64(config.MaxBucketStates)),
		inFlight:        newResourceBudget(int64(config.MaxInFlightRequests)),
		retryCapture:    newResourceBudget(config.MaxRetryCaptureBytes),
		transport:       transport,
		discordURL:      discordURL,
		closeDone:       make(chan struct{}),
	}
	p.noAuth = newClientState(identity{kind: authNone, label: "NoAuth"}, discordGlobalLimit, config.MaxQueueDepth, p.bucketSlots)
	p.initReverseProxies()
	p.wg.Add(1)
	go p.sweepLoop()
	return p, nil
}

func (p *Proxy) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	switch request.URL.Path {
	case "/nirn/healthz":
		select {
		case <-p.ctx.Done():
			writeUnavailable(writer, "proxy is shutting down")
		default:
			if p.clusterOverCapacity.Load() {
				writeUnavailable(writer, "cluster exceeds CLUSTER_MAX_NODES")
			} else {
				writer.WriteHeader(http.StatusOK)
			}
		}
		return
	}
	select {
	case <-p.ctx.Done():
		writeUnavailable(writer, "proxy is shutting down")
		return
	default:
	}
	if !p.inFlight.reserve(1) {
		writeUnavailable(writer, "in-flight request capacity exhausted")
		return
	}
	defer p.inFlight.release(1)
	if strings.HasPrefix(request.URL.Path, "/nirn/") {
		http.NotFound(writer, request)
		return
	}
	if request.Method == http.MethodConnect || isUpgradeRequest(request) {
		http.Error(writer, "protocol upgrades are not supported", http.StatusBadRequest)
		return
	}
	if p.clusterOverCapacity.Load() {
		writeUnavailable(writer, "cluster exceeds CLUSTER_MAX_NODES")
		return
	}

	requestContext, cancel := context.WithCancel(request.Context())
	stop := context.AfterFunc(p.ctx, cancel)
	defer func() {
		stop()
		cancel()
	}()
	request = request.WithContext(requestContext)

	bucketPath := GetOptimisticBucketPath(request.URL.Path, request.Method)
	metricsPath := MetricsPathFromBucket(bucketPath)
	if p.config.EnableMetrics {
		metricsPath = metricsRouteLabel(metricsPath)
	} else if len(metricsPath) > maxMetricsRouteLabelBytes {
		metricsPath = overflowMetricsRouteLabel
	}
	metricsMethod := metricsMethodLabel(request.Method)
	if p.config.EnableMetrics {
		openConnections := ConnectionsOpen.WithLabelValues(metricsMethod, metricsPath)
		openConnections.Inc()
		defer openConnections.Dec()
	}

	identity := identify(request.Header.Get("Authorization"))
	interaction := isInteractionEndpoint(request.URL.Path)
	routingHash := affinityHash(identity, bucketPath, interaction)
	hop := forwardedHop(request.Header.Get("X-Nirn-Hop"))
	request.Header.Del("X-Nirn-Hop")
	if hop > 0 && p.config.EnableMetrics {
		RequestsRoutedRecv.Inc()
	}
	if target := p.calculateRoute(routingHash); target != "" {
		if hop >= maxPeerHops {
			writeUnavailable(writer, "cluster routing did not converge")
			return
		}
		ctx := context.WithValue(request.Context(), peerTargetContextKey, peerTarget{address: target, hop: hop + 1})
		p.peerProxy.ServeHTTP(writer, request.WithContext(ctx))
		return
	}

	state, err := p.client(identity)
	if err != nil {
		writeUnavailable(writer, err.Error())
		return
	}
	defer state.end()

	majorKey := majorParameter(request.URL.Path)
	metadata := &requestMetadata{
		state:         state,
		routeHash:     routeHash(request.Method, bucketPath, majorKey),
		bucketPath:    bucketPath,
		metricsMethod: metricsMethod,
		metricsPath:   metricsPath,
		majorKey:      majorKey,
		interaction:   interaction,
	}
	ctx := context.WithValue(request.Context(), requestMetadataContextKey, metadata)
	p.discordProxy.ServeHTTP(writer, request.WithContext(ctx))
}

func affinityHash(identity identity, bucketPath string, interaction bool) uint64 {
	if identity.kind == authNone {
		if interaction {
			return HashCRC64(bucketPath)
		}
		return HashCRC64("no-auth-egress")
	}
	return binary.BigEndian.Uint64(identity.key[:8])
}

func forwardedHop(value string) int {
	hop, err := strconv.Atoi(value)
	if err != nil || hop < 0 {
		return 0
	}
	return hop
}

func isUpgradeRequest(request *http.Request) bool {
	if request.Header.Get("Upgrade") != "" {
		return true
	}
	for _, value := range request.Header.Values("Connection") {
		for token := range strings.SplitSeq(value, ",") {
			if strings.EqualFold(strings.TrimSpace(token), "upgrade") {
				return true
			}
		}
	}
	return false
}

func writeUnavailable(writer http.ResponseWriter, message string) {
	writer.Header().Set("Retry-After", "1")
	writeProxyError(writer, message, http.StatusServiceUnavailable)
}

func writeProxyError(writer http.ResponseWriter, message string, status int) {
	writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
	writer.Header().Set("X-Nirn-Proxy-Error", "true")
	writer.WriteHeader(status)
	_, _ = writer.Write([]byte(message + "\n"))
}

func (p *Proxy) sweepLoop() {
	defer p.wg.Done()
	ticker := time.NewTicker(clientSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			p.sweepClients(now)
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *Proxy) Close(ctx context.Context) error {
	p.closeOnce.Do(func() {
		p.cancel()
		go p.finishClose()
	})

	select {
	case <-p.closeDone:
		return p.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Proxy) finishClose() {
	p.closeErr = p.closeCluster(context.Background())
	p.clientsMu.Lock()
	p.bots = nil
	p.bearers = nil
	p.noAuth = nil
	p.clientsMu.Unlock()
	if closer, ok := p.transport.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
	p.wg.Wait()
	close(p.closeDone)
}
