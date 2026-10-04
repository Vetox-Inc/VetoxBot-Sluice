package proxy

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"regexp"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// DefaultMetricsNamespace prefixes every metric unless METRICS_NAMESPACE overrides it. Metric
// names after the prefix must not change: MIGRATING.md promises a prefix that keeps old dashboards.
const DefaultMetricsNamespace = "sluice"

const (
	maxMetricsRouteLabels      = 1024
	maxMetricsRouteLabelBytes  = 512
	overflowMetricsRouteLabel  = "/unknown"
	maxMetricsClientLabels     = 1024
	overflowMetricsClientLabel = "Other"
	auxiliaryReadTimeout       = 15 * time.Second
	auxiliaryWriteTimeout      = 2 * time.Minute
	auxiliaryIdleTimeout       = 30 * time.Second
)

var (
	ErrorCounter         prometheus.Counter
	WarningCounter       prometheus.Counter
	ProxyFailures        *prometheus.CounterVec
	RequestHistogram     *prometheus.HistogramVec
	QueueWaitHistogram   *prometheus.HistogramVec
	ConnectionsOpen      *prometheus.GaugeVec
	RequestsRoutedSent   prometheus.Counter
	RequestsRoutedRecv   prometheus.Counter
	RequestsRoutedError  prometheus.Counter
	WebhookShortCircuits prometheus.Counter
	CloudflareBlocks     prometheus.Counter
	EdgeRefusals         prometheus.Counter
	// DeprecatedAPIRequests is labelled by version: v0 through v8, or none.
	DeprecatedAPIRequests *prometheus.CounterVec

	metricsRegistry *prometheus.Registry
	metricsRoutes   = newRouteLabelLimiter(maxMetricsRouteLabels)
	clientLabels    = newLabelLimiter(maxMetricsClientLabels, overflowMetricsClientLabel)

	// activeProxy feeds the gauges that read live proxy state.
	activeProxy atomic.Pointer[Proxy]

	logger = NewLogger(os.Stderr, slog.LevelInfo, "text")

	credentialPatterns = []struct {
		pattern     *regexp.Regexp
		replacement string
	}{
		// A lone "!" is the placeholder a route label carries there, and stays as it is.
		{regexp.MustCompile(`(/(?:webhooks|interactions)/[^/?\s]+/)(?:[^/?\s!][^/?\s]*|![^/?\s]+)`), "$1:token"},
		{regexp.MustCompile(`(?i)\b(Bot|Bearer|Basic)\s+[A-Za-z0-9._~+/=-]{16,}`), "$1 :token"},
		{regexp.MustCompile(`[A-Za-z0-9_-]{23,28}\.[A-Za-z0-9_-]{6,7}\.[A-Za-z0-9_-]{27,}`), ":token"},
	}
)

func init() {
	ConfigureMetrics(DefaultMetricsNamespace)
}

type metricSet struct {
	registry             *prometheus.Registry
	errors               prometheus.Counter
	warnings             prometheus.Counter
	failures             *prometheus.CounterVec
	requests             *prometheus.HistogramVec
	queueWait            *prometheus.HistogramVec
	openConnections      *prometheus.GaugeVec
	routedSent           prometheus.Counter
	routedReceived       prometheus.Counter
	routedError          prometheus.Counter
	webhookShortCircuits prometheus.Counter
	cloudflareBlocks     prometheus.Counter
	edgeRefusals         prometheus.Counter
	deprecatedAPI        *prometheus.CounterVec
}

func newMetricSet(namespace string) metricSet {
	set := metricSet{
		registry: prometheus.NewRegistry(),
		errors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "error",
			Help: "Error-level log records; a request that fails is not logged at that level, see failures_total",
		}),
		warnings: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "warnings_total",
			Help: "Warnings logged: when this moves, the log says what needs attention",
		}),
		failures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "failures_total",
			Help: "Requests Sluice answered with an error of its own, or whose client left unanswered, by reason",
		}, []string{"reason"}),
		requests: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Name: "requests",
			Help:    "Seconds until Discord's response headers, one observation for every attempt Discord answered",
			Buckets: []float64{.1, .25, 1, 2.5, 5, 20},
		}, []string{"method", "status", "route", "clientId"}),
		queueWait: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Name: "queue_wait_seconds",
			Help:    "Seconds a request waited for its bucket and the global limit before it was sent, or before Sluice answered it",
			Buckets: []float64{.005, .025, .1, .25, 1, 2.5, 5, 10, 30, 60},
		}, []string{"method", "route"}),
		openConnections: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Name: "open_connections",
			Help: "Gauge for requests currently active in the proxy handler",
		}, []string{"method", "route"}),
		routedSent: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "requests_routed_sent",
			Help: "Counter for requests routed from this node into other nodes",
		}),
		routedReceived: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "requests_routed_received",
			Help: "Counter for requests received from other nodes",
		}),
		routedError: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "requests_routed_error",
			Help: "Counter for failed requests routed from this node",
		}),
		webhookShortCircuits: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "webhook_short_circuits_total",
			Help: "Requests to deleted or invalid webhooks answered without contacting Discord",
		}),
		cloudflareBlocks: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "cloudflare_blocks_total",
			Help: "Times Discord's edge blocked this IP and outbound traffic was paused",
		}),
		edgeRefusals: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "edge_refusals_total",
			Help: "Responses from Discord's edge rather than Discord: a 429 or 403 without Via",
		}),
		deprecatedAPI: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "deprecated_api_requests_total",
			Help: "Requests naming a deprecated or discontinued Discord API version, or none (Discord's default, v6)",
		}, []string{"version"}),
	}
	for _, reason := range failureReasons {
		set.failures.WithLabelValues(reason)
	}
	set.registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		set.errors, set.warnings, set.failures, set.requests, set.queueWait, set.openConnections,
		set.routedSent, set.routedReceived, set.routedError, set.webhookShortCircuits, set.cloudflareBlocks,
		set.edgeRefusals, set.deprecatedAPI,
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: namespace, Name: "invalid_requests",
			Help: "Invalid Discord responses (401, 403, non-shared 429) in the rolling 10-minute window",
		}, func() float64 {
			if p := activeProxy.Load(); p != nil {
				return float64(p.invalidRequests.count(time.Now()))
			}
			return 0
		}),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: namespace, Name: "invalid_requests_limit",
			Help: "Invalid responses in that window at which this node stops sending: 9,500, split between the nodes of a cluster",
		}, func() float64 {
			if p := activeProxy.Load(); p != nil {
				return float64(p.invalidRequests.limit)
			}
			return 0
		}),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: namespace, Name: "cloudflare_blocked",
			Help: "1 while outbound traffic is paused because Discord's edge blocked this IP",
		}, func() float64 {
			if p := activeProxy.Load(); p != nil && p.cloudflare.retryAfter(time.Now()) > 0 {
				return 1
			}
			return 0
		}),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: namespace, Name: "build_info",
			Help:        "1, labelled with the running Sluice version and the Go version that built it",
			ConstLabels: prometheus.Labels{"version": Version, "goversion": runtime.Version()},
		}, func() float64 { return 1 }),
		newLimitCollector(namespace),
	)
	return set
}

// limitCollector reports the limits Sluice works under beside what is in use, so a limit shows
// before it starts refusing or delaying requests.
type limitCollector struct {
	usage, limit, globalLimit *prometheus.Desc
}

func newLimitCollector(namespace string) limitCollector {
	return limitCollector{
		usage: prometheus.NewDesc(prometheus.BuildFQName(namespace, "", "resource_usage"),
			"How much of a bounded resource is in use, named after the MAX_ setting that bounds it", []string{"resource"}, nil),
		limit: prometheus.NewDesc(prometheus.BuildFQName(namespace, "", "resource_limit"),
			"The limit a MAX_ setting puts on a bounded resource", []string{"resource"}, nil),
		globalLimit: prometheus.NewDesc(prometheus.BuildFQName(namespace, "", "global_limit"),
			"Requests a second Sluice paces a client at: Discord's 50, or its entry in BOT_RATELIMIT_OVERRIDES", []string{"clientId"}, nil),
	}
}

func (c limitCollector) Describe(descriptions chan<- *prometheus.Desc) {
	descriptions <- c.usage
	descriptions <- c.limit
	descriptions <- c.globalLimit
}

func (c limitCollector) Collect(metrics chan<- prometheus.Metric) {
	p := activeProxy.Load()
	if p == nil {
		return
	}
	for _, resource := range p.resources() {
		metrics <- prometheus.MustNewConstMetric(c.usage, prometheus.GaugeValue, float64(resource.used), resource.name)
		metrics <- prometheus.MustNewConstMetric(c.limit, prometheus.GaugeValue, float64(resource.limit), resource.name)
	}
	for client, limit := range p.globalLimits() {
		metrics <- prometheus.MustNewConstMetric(c.globalLimit, prometheus.GaugeValue, float64(limit), client)
	}
}

// ConfigureMetrics replaces the exported metrics with a set under namespace. Call it
// once at startup, before the proxy serves traffic.
func ConfigureMetrics(namespace string) {
	set := newMetricSet(namespace)
	metricsRegistry = set.registry
	ErrorCounter = set.errors
	WarningCounter = set.warnings
	ProxyFailures = set.failures
	RequestHistogram = set.requests
	QueueWaitHistogram = set.queueWait
	ConnectionsOpen = set.openConnections
	RequestsRoutedSent = set.routedSent
	RequestsRoutedRecv = set.routedReceived
	RequestsRoutedError = set.routedError
	WebhookShortCircuits = set.webhookShortCircuits
	CloudflareBlocks = set.cloudflareBlocks
	EdgeRefusals = set.edgeRefusals
	DeprecatedAPIRequests = set.deprecatedAPI
}

type routeLabelLimiter struct {
	mu       sync.Mutex
	limit    int
	overflow string
	seen     map[string]struct{}
}

func newRouteLabelLimiter(limit int) *routeLabelLimiter {
	return newLabelLimiter(limit, overflowMetricsRouteLabel)
}

func newLabelLimiter(limit int, overflow string) *routeLabelLimiter {
	return &routeLabelLimiter{
		limit:    limit,
		overflow: overflow,
		seen:     map[string]struct{}{overflow: {}},
	}
}

func (l *routeLabelLimiter) label(value string) string {
	if len(value) > maxMetricsRouteLabelBytes {
		return l.overflow
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.seen[value]; ok {
		return value
	}
	if len(l.seen) >= l.limit {
		return l.overflow
	}
	l.seen[value] = struct{}{}
	return value
}

// metricsRouteLabel bounds the number of request-derived Prometheus route labels.
func metricsRouteLabel(route string) string {
	return metricsRoutes.label(route)
}

func metricsMethodLabel(method string) string {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch,
		http.MethodDelete, http.MethodConnect, http.MethodOptions, http.MethodTrace:
		return method
	default:
		return "OTHER"
	}
}

// NewLogger returns a logger that redacts Discord credentials from the message and every
// attribute, and counts error-level records. format is "text" or "json".
func NewLogger(output io.Writer, level slog.Leveler, format string) *slog.Logger {
	options := &slog.HandlerOptions{Level: level, ReplaceAttr: redactAttr}
	var handler slog.Handler
	if format == "json" {
		handler = slog.NewJSONHandler(output, options)
	} else {
		handler = slog.NewTextHandler(output, options)
	}
	return slog.New(errorCountingHandler{handler})
}

func SetLogger(replacement *slog.Logger) {
	logger = replacement
}

type errorCountingHandler struct{ slog.Handler }

func (h errorCountingHandler) Handle(ctx context.Context, record slog.Record) error {
	switch {
	case record.Level >= slog.LevelError:
		ErrorCounter.Inc()
	case record.Level >= slog.LevelWarn:
		WarningCounter.Inc()
	}
	return h.Handler.Handle(ctx, record)
}

func (h errorCountingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return errorCountingHandler{h.Handler.WithAttrs(attrs)}
}

func (h errorCountingHandler) WithGroup(name string) slog.Handler {
	return errorCountingHandler{h.Handler.WithGroup(name)}
}

func redactAttr(_ []string, attr slog.Attr) slog.Attr {
	switch attr.Value.Kind() {
	case slog.KindString:
		attr.Value = slog.StringValue(redactLogSecrets(attr.Value.String()))
	case slog.KindAny:
		attr.Value = slog.StringValue(redactLogSecrets(fmt.Sprint(attr.Value.Any())))
	}
	return attr
}

func redactLogSecrets(value string) string {
	for _, credential := range credentialPatterns {
		value = credential.pattern.ReplaceAllString(value, credential.replacement)
	}
	return value
}

func NewMetricsServer(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(metricsRegistry, promhttp.HandlerOpts{}))
	return newAuxiliaryServer(addr, mux)
}

func NewProfileServer(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return newAuxiliaryServer(addr, mux)
}

func newAuxiliaryServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       auxiliaryReadTimeout,
		WriteTimeout:      auxiliaryWriteTimeout,
		IdleTimeout:       auxiliaryIdleTimeout,
	}
}
