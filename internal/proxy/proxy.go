package proxy

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
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

	hopHeader        = "X-Sluice-Hop"
	proxyErrorHeader = "X-Sluice-Proxy-Error"
	clientAuthHeader = "X-Sluice-Auth"
	sluiceVia        = "1.1 sluice"

	// DefaultDiscordURL is where requests go unless Config.DiscordURL points elsewhere.
	DefaultDiscordURL = "https://discord.com"
)

// Config controls request scheduling, resource bounds, transport, and metrics.
type Config struct {
	OutboundIP             string
	UpstreamTimeout        time.Duration
	QueueTimeout           time.Duration
	DisableHTTP2           bool
	Disable401Lock         bool
	EnableMetrics          bool
	GlobalOverrides        string
	MaxBearerClients       int
	MaxBucketStates        int
	MaxClientStates        int
	MaxInFlightRequests    int
	MaxQueueDepth          int
	MaxRetryBodyBytes      int64
	MaxRetryCaptureBytes   int64
	InvalidRequestLimit    int
	DiscordURL             string
	CloudflareBanDetection bool
	// ClientAuthSecret, when set, must arrive in X-Sluice-Auth on every request to PublicHandler.
	ClientAuthSecret string
	// StateFile keeps the invalid-request, webhook and Cloudflare guards across restarts; "" keeps
	// them in memory only.
	StateFile string
	// BotWideRoutes lists routes, as "GET /guilds/!/vanity-url", that Discord limits per bot
	// rather than per channel, guild or webhook.
	BotWideRoutes string
	Transport     http.RoundTripper
}

// RequestLifetime bounds a request that makes no progress: its queue deadline, one attempt and
// time to write the response.
func (c Config) RequestLifetime() time.Duration {
	return c.QueueTimeout + c.UpstreamTimeout + 5*time.Second
}

type requestContextKey uint8

const (
	requestMetadataContextKey requestContextKey = iota
	peerTargetContextKey
	// connectionProgressContextKey holds a func that renews the inbound connection's deadlines.
	connectionProgressContextKey
)

type requestMetadata struct {
	state         *clientState
	routeHash     uint64
	bucketPath    string
	metricsMethod string
	metricsPath   string
	majorKey      string
	interaction   bool
	// credentialScoped marks routes authenticated by the Authorization credential rather than
	// by a webhook or interaction token in the path.
	credentialScoped bool
	webhookKey       string
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
	botWideRoutes   map[string]bool
	invalidRequests *invalidRequestGuard
	webhooks        *webhookGuard
	cloudflare      *cloudflareGuard
	applications    *applicationSet
	bucketSlots     *resourceBudget
	inFlight        *resourceBudget
	retryCapture    *resourceBudget

	// backgroundMu orders goroutines started for requests before Close waits for them.
	backgroundMu    sync.Mutex
	draining        atomic.Bool
	versionWarnings sync.Map
	aliasWarning    sync.Once

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
	botWideRoutes, err := parseBotWideRoutes(config.BotWideRoutes)
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

	discordURL, err := ParseDiscordURL(config.DiscordURL)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &Proxy{
		config:          config,
		ctx:             ctx,
		cancel:          cancel,
		bots:            make(map[[sha256.Size]byte]*clientState),
		bearers:         make(map[[sha256.Size]byte]*clientState),
		globalOverrides: overrides,
		botWideRoutes:   botWideRoutes,
		invalidRequests: newInvalidRequestGuard(invalidLimit, invalidRequestWindow),
		webhooks:        newWebhookGuard(),
		cloudflare:      &cloudflareGuard{},
		applications:    newApplicationSet(),
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
	if config.StateFile != "" {
		p.loadState()
		p.wg.Add(1)
		go p.stateLoop()
	}
	activeProxy.Store(p)
	return p, nil
}

// ParseDiscordURL validates a Discord API base URL. An empty value means DefaultDiscordURL.
func ParseDiscordURL(value string) (*url.URL, error) {
	if value == "" {
		value = DefaultDiscordURL
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return nil, fmt.Errorf("Discord API URL must be an absolute URL without a path, such as %s", DefaultDiscordURL)
	}
	switch parsed.Scheme {
	case "https":
	case "http":
		if host := parsed.Hostname(); host != "localhost" && !net.ParseIP(host).IsLoopback() {
			return nil, fmt.Errorf("Discord API URL must use https unless it points at a loopback address")
		}
	default:
		return nil, fmt.Errorf("Discord API URL must use https")
	}
	parsed.Path = ""
	return parsed, nil
}

// upstreamProblem explains why Discord is unreachable from this node, or returns "".
func (p *Proxy) upstreamProblem(now time.Time) string {
	if p.cloudflare.retryAfter(now) > 0 {
		return "Discord's edge is blocking this IP"
	}
	if p.invalidRequests.count(now)*5 >= p.invalidRequests.limit*4 {
		return "invalid-request budget is at least 80% used"
	}
	return ""
}

// PublicHandler serves clients. With ClientAuthSecret set, it admits only requests carrying the
// secret in X-Sluice-Auth, apart from the health endpoints. Cluster peers authenticate with
// mutual TLS instead and are served by the Proxy itself.
func (p *Proxy) PublicHandler() http.Handler {
	if p.config.ClientAuthSecret == "" {
		return p
	}
	want := sha256.Sum256([]byte(p.config.ClientAuthSecret))
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		collapseSlashes(request.URL)
		got := sha256.Sum256([]byte(request.Header.Get(clientAuthHeader)))
		if subtle.ConstantTimeCompare(got[:], want[:]) != 1 && !isHealthPath(request.URL.Path) {
			ProxyFailures.WithLabelValues("client_auth").Inc()
			// 403, not 401: discord.js discards its bot token on a 401.
			writeProxyError(writer, "Sluice client authentication failed", http.StatusForbidden)
			return
		}
		p.ServeHTTP(writer, request)
	})
}

func isHealthPath(path string) bool {
	return path == "/sluice/healthz" || path == "/nirn/healthz" || path == "/sluice/health/upstream"
}

// Drain starts a graceful shutdown: health checks fail so load balancers stop sending traffic,
// and the node leaves its cluster so peers stop routing to it. Admitted requests keep running
// until Close.
func (p *Proxy) Drain(ctx context.Context) error {
	p.draining.Store(true)
	return p.closeCluster(ctx)
}

func (p *Proxy) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	collapseSlashes(request.URL)
	request.Header.Del(clientAuthHeader)
	switch request.URL.Path {
	case "/sluice/healthz", "/nirn/healthz":
		if request.URL.Path == "/nirn/healthz" {
			p.aliasWarning.Do(func() {
				logger.Warn("/nirn/healthz is deprecated and will be removed in Sluice 2.0; use /sluice/healthz")
			})
		}
		switch {
		case p.ctx.Err() != nil || p.draining.Load():
			writeUnavailable(writer, "proxy is shutting down")
		case p.clusterOverCapacity.Load():
			writeUnavailable(writer, "cluster exceeds CLUSTER_MAX_NODES")
		default:
			markGenerated(writer.Header())
			writer.WriteHeader(http.StatusOK)
		}
		return
	case "/sluice/health/upstream":
		if problem := p.upstreamProblem(time.Now()); problem != "" {
			writeUnavailable(writer, problem)
		} else {
			markGenerated(writer.Header())
			writer.WriteHeader(http.StatusOK)
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
	if strings.HasPrefix(request.URL.Path, "/sluice/") || strings.HasPrefix(request.URL.Path, "/nirn/") {
		writeProxyError(writer, "unknown proxy endpoint", http.StatusNotFound)
		return
	}
	if request.Method == http.MethodConnect || isUpgradeRequest(request) {
		writeProxyError(writer, "protocol upgrades are not supported", http.StatusBadRequest)
		return
	}
	if !isCleanDiscordPath(request.URL) {
		writeProxyError(writer, "path contains dot segments or encoded separators", http.StatusBadRequest)
		return
	}
	ensureAPIPrefix(request.URL)
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
	if request.Body != http.NoBody {
		// An upload can outlast the server's read and write timeouts; each chunk it sends renews them.
		controller := http.NewResponseController(writer)
		lifetime := p.config.RequestLifetime()
		requestContext = context.WithValue(requestContext, connectionProgressContextKey, func() {
			deadline := time.Now().Add(lifetime)
			_ = controller.SetReadDeadline(deadline)
			_ = controller.SetWriteDeadline(deadline)
		})
	}
	request = request.WithContext(requestContext)

	bucketPath := GetOptimisticBucketPath(request.URL.Path, request.Method)
	metricsPath := MetricsPathFromBucket(bucketPath)
	botWide := p.botWideRoutes[request.Method+" "+metricsPath]
	if botWide {
		bucketPath = metricsPath
	}
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

	if version := outdatedAPIVersion(request.URL.Path); version != "" {
		if p.config.EnableMetrics {
			DeprecatedAPIRequests.WithLabelValues(version).Inc()
		}
		if _, warned := p.versionWarnings.LoadOrStore(version, struct{}{}); !warned {
			logger.Warn("A client uses a deprecated or discontinued Discord API version; none means Discord's default, v6", "version", version, "route", metricsPath)
		}
	}

	identity := identify(request.Header.Get("Authorization"))
	interaction := isInteractionEndpoint(request.URL.Path) || p.applications.followUp(request.URL.Path)
	webhookKey := webhookCredentialKey(request.URL.Path, interaction)
	credentialScoped := !interaction && webhookKey == ""
	if identity.unsupported && credentialScoped {
		// Discord rejects every other scheme with a 401 that counts against this IP.
		ProxyFailures.WithLabelValues("unsupported_authorization").Inc()
		writeResponse(writer, unauthorizedResponse(request))
		return
	}
	routingHash := affinityHash(identity, bucketPath, interaction)
	hop := forwardedHop(request.Header.Get(hopHeader))
	request.Header.Del(hopHeader)
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
	if botWide {
		majorKey = ""
	}
	metadata := &requestMetadata{
		state:            state,
		routeHash:        routeHash(request.Method, bucketPath, majorKey),
		bucketPath:       bucketPath,
		metricsMethod:    metricsMethod,
		metricsPath:      metricsPath,
		majorKey:         majorKey,
		interaction:      interaction,
		credentialScoped: credentialScoped,
		webhookKey:       webhookKey,
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

// writeProxyError answers in Discord's error shape, which Discord libraries such as discord.js
// read to report the message.
func writeProxyError(writer http.ResponseWriter, message string, status int) {
	body, _ := json.Marshal(struct {
		Message string `json:"message"`
		Code    int    `json:"code"`
	}{message, 0})
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set(proxyErrorHeader, "true")
	markGenerated(writer.Header())
	writer.WriteHeader(status)
	_, _ = writer.Write(body)
}

func writeResponse(writer http.ResponseWriter, response *http.Response) {
	defer func() { _ = response.Body.Close() }()
	for name, values := range response.Header {
		writer.Header()[name] = values
	}
	writer.WriteHeader(response.StatusCode)
	_, _ = io.Copy(writer, response.Body)
}

func markGenerated(header http.Header) {
	header.Set("Generated-By-Proxy", "true")
	header.Set("Via", sluiceVia)
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

// goBackground runs fn on a goroutine Close waits for, unless the proxy is already closing.
func (p *Proxy) goBackground(fn func()) bool {
	p.backgroundMu.Lock()
	defer p.backgroundMu.Unlock()
	if p.ctx.Err() != nil {
		return false
	}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		fn()
	}()
	return true
}

func (p *Proxy) Close(ctx context.Context) error {
	p.closeOnce.Do(func() {
		p.backgroundMu.Lock()
		p.cancel()
		p.backgroundMu.Unlock()
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
	activeProxy.CompareAndSwap(p, nil)
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
	if p.config.StateFile != "" {
		if err := p.saveState(); err != nil {
			logger.Warn("Could not save STATE_FILE at shutdown", "error", err)
		}
	}
	close(p.closeDone)
}
